use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use http::{HeaderMap, HeaderValue};

use crate::cache_registry::{
    prompt_to_cumulative_hashes_with_block_size, CacheRegistry, HostBitmap,
};
use crate::protocol::routing_headers::{
    CalinixMode, DECODE_POD_ID, MODE, PREFILL_POD_ID, TARGET_POD_ID,
};
use crate::routing::plan::RoutingPlan;
use crate::routing::pipeline::{RoutedRequest, RoutingPipeline};
use crate::routing::prepare::{PrepareInput, PrepareStage};
use crate::routing::RoutingError;
use crate::session::StickyStore;
use crate::upstream::{
    LoadState, PodEndpoint, PodId, PodRole, PodTable, RuntimeRegistry, UpstreamCatalog,
    UpstreamGroup,
};

// --- E2E Integration Tests ---

fn e2e_base_url() -> String {
    env::var("CALINIX_E2E_URL").unwrap_or_else(|_| "http://localhost:18080".to_string())
}

#[test]
#[ignore = "requires e2e/routing/docker-compose.yml services"]
fn exposes_openai_compatible_routes_through_http_api() {
    let base_url = e2e_base_url();
    let headers = e2e_headers();

    let chat = post_json(
        &base_url,
        "/v1/chat/completions",
        &headers,
        r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":"route this through calinix"}],"stream":true}"#,
    );
    let completions = post_json(
        &base_url,
        "/v1/completions",
        &headers,
        r#"{"model":"gpt-4o-mini","prompt":"route this through calinix"}"#,
    );
    let embeddings = post_json(
        &base_url,
        "/v1/embeddings",
        &headers,
        r#"{"model":"text-embedding-3-small","input":"route this through calinix"}"#,
    );

    println!("chat_completions_response={chat}\n");
    println!("completions_response={completions} \n");
    println!("embeddings_response={embeddings} \n");
}

#[test]
#[ignore = "requires e2e/routing/docker-compose.yml services"]
fn handles_concurrent_disaggregated_register_events() {
    const REQUESTS: usize = 100;

    let base_url = e2e_base_url();
    let handles = (0..REQUESTS)
        .map(|request_number| {
            let base_url = base_url.clone();
            thread::spawn(move || {
                let body = format!(
                    r#"{{"model":"gpt-4o-mini","messages":[{{"role":"user","content":"route concurrent request {request_number}"}}],"stream":true}}"#
                );
                let response = post_json(
                    &base_url,
                    "/v1/chat/completions",
                    &e2e_headers(),
                    &body,
                );
                assert_registered_event(&response, request_number);
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().expect("concurrent e2e request panicked");
    }
}

fn e2e_headers() -> [(&'static str, &'static str); 5] {
    [
        ("authorization", "Bearer user-token"),
        ("x-calinix-request-id", "req-e2e-routing-1"),
        ("x-calinix-mode", "disaggregated"),
        ("x-calinix-session-key", "session-e2e-routing"),
        ("x-event", "register"),
    ]
}

fn assert_registered_event(response: &str, request_number: usize) {
    let json: serde_json::Value =
        serde_json::from_str(response).expect("mock pod response is valid JSON");
    assert_eq!(
        json["headers"]["x-event"], "register",
        "request {request_number} did not forward x-event header: {response}"
    );
    assert_eq!(
        json["events"][0]["type"], "prefixCached",
        "request {request_number} did not emit register event: {response}"
    );
    assert_eq!(
        json["events"][0]["result"]["status"], 200,
        "request {request_number} event callback failed: {response}"
    );
}

fn post_json(base_url: &str, path: &str, headers: &[(&str, &str)], body: &str) -> String {
    let (host, port) = parse_base_url(base_url);
    let mut stream = TcpStream::connect((host.as_str(), port)).expect("connect to Calinix API");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .expect("set write timeout");

    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");

    stream
        .write_all(request.as_bytes())
        .and_then(|_| stream.write_all(body.as_bytes()))
        .expect("write request");

    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");

    assert!(
        response.starts_with("HTTP/1.1 200"),
        "expected 200 response, got:\n{response}"
    );
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .expect("response has body")
}

fn parse_base_url(base_url: &str) -> (String, u16) {
    let rest = base_url
        .strip_prefix("http://")
        .expect("CALINIX_E2E_URL must start with http://");
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = authority.split_once(':').unwrap_or((authority, "80"));
    (host.to_string(), port.parse().expect("valid port"))
}

// --- From routing/pipeline.rs ---

#[test]
fn single_route_filters_queries_scores_and_picks_by_cache_depth() {
    let registry = registry_with_roles(2, 0, 0);
    mark_alive(&registry, 0..2);

    let prompt = "alpha beta gamma delta";
    let hashes = prompt_to_cumulative_hashes_with_block_size(prompt, 2);
    registry.cache_registry.register_prefix(0, hashes[0]);
    registry.cache_registry.register_chain(1, &hashes);

    let routed = route_chat(&registry, HeaderMap::new(), prompt);

    match &routed.plan {
        RoutingPlan::Single {
            target_pod_id,
            cache_hit,
            cache_prefix_depth,
            ..
        } => {
            assert_eq!(*target_pod_id, 1);
            assert!(*cache_hit);
            assert_eq!(*cache_prefix_depth, hashes.len());
        }
        _ => panic!("expected single routing plan"),
    }
    assert_eq!(routed.forwarding_headers.get(TARGET_POD_ID).unwrap(), "1");
}

#[test]
fn single_route_keeps_strong_cache_match_over_load() {
    let registry = registry_with_roles(2, 0, 0);
    mark_alive(&registry, 0..2);

    let prompt = "cache wins over sticky and load";
    let hashes = prompt_to_cumulative_hashes_with_block_size(prompt, 2);
    registry.cache_registry.register_chain(0, &hashes);

    let loads = LoadState::new(registry.total_pods());
    loads.set_inflight_for_test(0, 90);
    let sticky = StickyStore::new();
    sticky.remember("session-a", 1);

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-calinix-session-key",
        HeaderValue::from_static("session-a"),
    );
    let routed = pipeline().route_openai_request(
        &registry,
        &loads,
        &sticky,
        "/v1/chat/completions",
        "POST",
        &headers,
        chat_body(prompt).as_bytes(),
    );

    let routed = routed.expect("request routes");
    match &routed.plan {
        RoutingPlan::Single { target_pod_id, .. } => assert_eq!(*target_pod_id, 0),
        _ => panic!("expected single routing plan"),
    }
}

#[test]
fn single_route_filters_cached_pod_when_over_capacity() {
    let registry = registry_with_roles(2, 0, 0);
    mark_alive(&registry, 0..2);

    let prompt = "cached pod cannot accept more traffic";
    let hashes = prompt_to_cumulative_hashes_with_block_size(prompt, 2);
    registry.cache_registry.register_chain(0, &hashes);

    let loads = LoadState::new(registry.total_pods());
    loads.set_inflight_for_test(0, 100);
    loads.set_inflight_for_test(1, 1);

    let routed = pipeline()
        .route_openai_request(
            &registry,
            &loads,
            &StickyStore::new(),
            "/v1/chat/completions",
            "POST",
            &HeaderMap::new(),
            chat_body(prompt).as_bytes(),
        )
        .expect("request routes");

    match &routed.plan {
        RoutingPlan::Single {
            target_pod_id,
            cache_hit,
            ..
        } => {
            assert_eq!(*target_pod_id, 1);
            assert!(!*cache_hit);
        }
        _ => panic!("expected single routing plan"),
    }
}

#[test]
fn single_route_uses_load_when_cache_scores_tie() {
    let registry = registry_with_roles(2, 0, 0);
    mark_alive(&registry, 0..2);

    let loads = LoadState::new(registry.total_pods());
    loads.set_inflight_for_test(0, 10_000);

    let routed = pipeline()
        .route_openai_request(
            &registry,
            &loads,
            &StickyStore::new(),
            "/v1/chat/completions",
            "POST",
            &HeaderMap::new(),
            chat_body("cold prompt without cache").as_bytes(),
        )
        .expect("request routes");

    match &routed.plan {
        RoutingPlan::Single { target_pod_id, .. } => assert_eq!(*target_pod_id, 1),
        _ => panic!("expected single routing plan"),
    }
}

#[test]
fn disaggregated_route_uses_prefix_query_score_and_pick_for_each_role() {
    let registry = registry_with_roles(0, 2, 2);
    mark_alive(&registry, 0..4);

    let prompt = "prefill and decode both use cache";
    let hashes = prompt_to_cumulative_hashes_with_block_size(prompt, 2);
    registry.cache_registry.register_prefix(0, hashes[0]);
    registry.cache_registry.register_chain(1, &hashes);
    registry.cache_registry.register_prefix(2, hashes[0]);
    registry.cache_registry.register_chain(3, &hashes);

    let mut headers = HeaderMap::new();
    headers.insert(MODE, HeaderValue::from_static("disaggregated"));
    let routed = route_chat(&registry, headers, prompt);

    match &routed.plan {
        RoutingPlan::Disaggregated {
            prefill_pod_id,
            decode_pod_id,
            cache_hit,
            cache_prefix_depth,
            ..
        } => {
            assert_eq!(*prefill_pod_id, 1);
            assert_eq!(*decode_pod_id, 3);
            assert!(*cache_hit);
            assert_eq!(*cache_prefix_depth, hashes.len());
        }
        _ => panic!("expected disaggregated routing plan"),
    }
    assert_eq!(routed.forwarding_headers.get(PREFILL_POD_ID).unwrap(), "1");
    assert_eq!(routed.forwarding_headers.get(DECODE_POD_ID).unwrap(), "3");
}

#[test]
fn healthy_filter_excludes_pods_not_marked_alive() {
    let registry = registry_with_roles(1, 0, 0);
    let err = match pipeline().route_openai_request(
        &registry,
        &LoadState::new(registry.total_pods()),
        &StickyStore::new(),
        "/v1/chat/completions",
        "POST",
        &HeaderMap::new(),
        chat_body("no alive pods").as_bytes(),
    ) {
        Ok(_) => panic!("no alive pods should be filtered out"),
        Err(err) => err,
    };

    assert!(matches!(err, RoutingError::NoCandidates));
}

fn route_chat(registry: &RuntimeRegistry, headers: HeaderMap, prompt: &str) -> RoutedRequest {
    pipeline()
        .route_openai_request(
            registry,
            &LoadState::new(registry.total_pods()),
            &StickyStore::new(),
            "/v1/chat/completions",
            "POST",
            &headers,
            chat_body(prompt).as_bytes(),
        )
        .expect("request routes")
}

fn pipeline() -> RoutingPipeline {
    RoutingPipeline {
        default_mode: CalinixMode::Single,
        block_size: 2,
        ..RoutingPipeline::default()
    }
}

fn chat_body(prompt: &str) -> String {
    format!(r#"{{"model":"test-model","messages":[{{"role":"user","content":"{prompt}"}}]}}"#)
}

fn mark_alive(registry: &RuntimeRegistry, pod_ids: impl IntoIterator<Item = PodId>) {
    for pod_id in pod_ids {
        registry.cache_registry.mark_pod_alive(pod_id as usize);
    }
}

fn registry_with_roles(
    single_count: usize,
    prefill_count: usize,
    decode_count: usize,
) -> RuntimeRegistry {
    let mut pods = Vec::new();
    let mut by_external_id = HashMap::new();
    let mut single_pods = HostBitmap::empty();
    let mut prefill_pods = HostBitmap::empty();
    let mut decode_pods = HostBitmap::empty();

    push_role(
        "single",
        single_count,
        &mut pods,
        &mut by_external_id,
        &mut single_pods,
    );
    push_role(
        "prefill",
        prefill_count,
        &mut pods,
        &mut by_external_id,
        &mut prefill_pods,
    );
    push_role(
        "decode",
        decode_count,
        &mut pods,
        &mut by_external_id,
        &mut decode_pods,
    );

    RuntimeRegistry {
        pod_table: PodTable {
            pods: pods.clone(),
            by_external_id,
        },
        upstreams: UpstreamCatalog {
            pods,
            groups: vec![
                upstream_group(1, "single", PodRole::Single, &single_pods),
                upstream_group(2, "prefill", PodRole::Prefill, &prefill_pods),
                upstream_group(3, "decode", PodRole::Decode, &decode_pods),
            ],
        },
        single_pods,
        prefill_pods,
        decode_pods,
        cache_registry: CacheRegistry::new_empty_alive(
            single_count + prefill_count + decode_count,
        ),
    }
}

fn push_role(
    prefix: &str,
    count: usize,
    pods: &mut Vec<PodEndpoint>,
    by_external_id: &mut HashMap<String, PodId>,
    role_bitmap: &mut HostBitmap,
) {
    for index in 0..count {
        let pod_id = pods.len() as PodId;
        let external_id = format!("{prefix}-{index}");
        by_external_id.insert(external_id.clone(), pod_id);
        role_bitmap.set(pod_id as usize);
        pods.push(PodEndpoint {
            id: pod_id,
            pod_id,
            address: format!("http://{external_id}:8000"),
            healthy: true,
            draining: false,
            max_conns: 100,
            capabilities: match prefix {
                "single" => PodRole::Single.into(),
                "prefill" => PodRole::Prefill.into(),
                "decode" => PodRole::Decode.into(),
                _ => unreachable!("test role prefix is known"),
            },
        });
    }
}

fn upstream_group(id: u16, name: &str, role: PodRole, pods: &HostBitmap) -> UpstreamGroup {
    UpstreamGroup {
        id,
        name: name.to_string(),
        role,
        pods: pods
            .iter_set_bits()
            .into_iter()
            .filter_map(|pod_id| PodId::try_from(pod_id).ok())
            .collect(),
        pod_bitmap: pods.clone(),
    }
}

// --- From routing/prepare.rs ---

#[test]
fn prepare_builds_routing_context_without_body() {
    let stage = PrepareStage {
        default_mode: CalinixMode::Single,
        block_size: 2,
    };
    let body = br#"{"model":"m","prompt":"one two three four five"}"#;

    let prepared = stage
        .run(PrepareInput {
            path: "/v1/completions",
            method: "POST",
            headers: &HeaderMap::new(),
            body,
        })
        .unwrap();

    assert_eq!(prepared.ctx.cumulative_hashes.len(), 3);
    assert_eq!(
        prepared.ctx.cache_namespace,
        "openai:m:whitespace-v1:block-2"
    );
}
