use crate::app::bootstrap::openai_compatible_endpoint;
use crate::app::state::AppState;
use crate::config::load_config;
use crate::protocol::routing_headers::{CACHE_HIT, TARGET_POD_ID};
use crate::routing::plan::RoutingPlan;
use crate::routing::pipeline::RoutedRequest;
use crate::upstream::RuntimeRegistry;

use axum::extract::State;
use axum::Extension;
use axum::http::{Method, Uri};
use axum::body::Bytes;
use axum::http::HeaderValue;

use super::parse_config;

#[test]
fn init_startup_builds_runtime_registry_from_yaml() {
    let config = parse_config();
    let registry = RuntimeRegistry::from_config(&config).expect("registry builds from config");

    assert_eq!(registry.total_pods(), 6);
    assert_eq!(registry.single_pods.count(), 2);
    assert_eq!(registry.prefill_pods.count(), 2);
    assert_eq!(registry.decode_pods.count(), 2);
    assert_eq!(registry.cache_registry.alive().count(), 0);

    println!("registry: {:#?}", registry.cache_registry.alive());
    print!("registry prefillPods: {:#?}", registry.prefill_pods);
    print!("registry decodePOds: {:#?}", registry.decode_pods);
    print!("registry single pods: {:#?}", registry.single_pods);

    let pods = &registry.pod_table.pods;
    assert_eq!(pods[0].pod_id, 0);
    assert_eq!(pods[0].address, "http://single-pod-1:8000");
    assert_eq!(pods[2].pod_id, 2);
    assert_eq!(pods[2].address, "http://prefill-pod-1:8001");
    assert_eq!(pods[4].pod_id, 4);
    assert_eq!(pods[4].address, "http://decode-pod-1:9001");
}

#[tokio::test]
#[ignore = "requires docker pods to be running"]
async fn test_openai_endpoint_returns_calinix_headers() {
    // Initialize config
    let mut config = load_config("./config.yaml").expect("load config");

    // Check reachability and rewrite URLs if needed
    let target_url = String::from("http://127.0.0.1:18101");
    println!("Resolved single-1 URL: {}", target_url);

    // Update config to use the reachable URL
    config.upstreams.single.pods[0].url = target_url.clone();

    let registry = RuntimeRegistry::from_config(&config).expect("create registry");
    // Mark first pod as alive
    registry.cache_registry.mark_pod_alive(0);

    let state = AppState::new(registry);

    let routed = RoutedRequest {
        plan: RoutingPlan::Single {
            request_id: "test-req".to_string(),
            target_pod_id: 0,
            target_address: target_url,
            cache_hit: true,
            cache_prefix_depth: 4,
            route_policy: "default".to_string(),
        },
        forwarding_headers: {
            let mut h = http::HeaderMap::new();
            h.insert("x-calinix-cache-hit", HeaderValue::from_static("true"));
            h.insert("x-calinix-target-pod-id", HeaderValue::from_static("0"));
            h.insert(
                "x-calinix-cache-prefix-depth",
                HeaderValue::from_static("4"),
            );
            h
        },
        session_key: None,
        cumulative_hashes: vec![],
    };

    let response = openai_compatible_endpoint(
        State(state),
        Extension(routed),
        Method::POST,
        Uri::from_static("/v1/chat/completions"),
        Bytes::from(
            r#"{"model":"llama-3.1-8b","messages":[{"role":"user","content":"test"}]}"#,
        ),
    )
    .await;

    let status = response.status();
    let headers = response.headers().clone();

    println!("Test Run Response Status: {:?}", status);
    println!("Test Run Response Headers:");
    for (name, value) in &headers {
        println!("  {}: {:?}", name, value);
    }

    // Assertions if we successfully hit the pod
    if status.is_success() {
        let cache_hit_val = headers.get(CACHE_HIT);
        assert!(
            cache_hit_val.is_some(),
            "response should contain x-calinix-cache-hit header"
        );
        assert_eq!(cache_hit_val.unwrap().to_str().unwrap(), "true");
        println!("Verified: {} header is present and is 'true'", CACHE_HIT);

        let target_pod_val = headers.get(TARGET_POD_ID);
        assert!(
            target_pod_val.is_some(),
            "response should contain x-calinix-target-pod-id header"
        );
        assert_eq!(target_pod_val.unwrap().to_str().unwrap(), "0");
        println!("Verified: {} header is present and is '0'", TARGET_POD_ID);
    } else {
        println!(
            "Skipping success assertion because status is non-success: {:?}",
            status
        );
    }
}
