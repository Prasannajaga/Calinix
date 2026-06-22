use std::collections::HashMap;

use crate::cache_registry::{CacheRegistry, HostBitmap};
use crate::upstream::health::{
    apply_health_result, health_uri, join_paths, parse_http_url, HealthResult, PodHealthState,
};
use crate::upstream::{
    LoadState, PodEndpoint, PodTable, RuntimeRegistry, UpstreamCatalog,
};

// --- From upstream/load.rs ---

#[test]
fn guard_increments_and_decrements_inflight() {
    let loads = LoadState::new(2);
    assert_eq!(loads.inflight(1), 0);
    {
        let _guard = loads.track(1).expect("pod exists");
        assert_eq!(loads.inflight(1), 1);
    }
    assert_eq!(loads.inflight(1), 0);
}

#[test]
fn guard_drop_never_underflows() {
    let loads = LoadState::new(1);
    loads.set_inflight_for_test(0, 0);
    loads.decrement(0);
    assert_eq!(loads.inflight(0), 0);
}

// --- From upstream/health.rs ---

#[test]
fn joins_base_path_and_health_endpoint() {
    assert_eq!(join_paths("/", "/health"), "/health");
    assert_eq!(join_paths("/vllm", "/health"), "/vllm/health");
    assert_eq!(join_paths("/vllm/", "health"), "/vllm/health");
}

#[test]
fn parses_http_url_with_default_port() {
    assert_eq!(
        parse_http_url("http://prefill-1").unwrap(),
        ("prefill-1".to_string(), 80, "/".to_string())
    );
    assert_eq!(
        parse_http_url("http://prefill-1:8000/api").unwrap(),
        ("prefill-1".to_string(), 8000, "/api".to_string())
    );
}

#[test]
fn builds_health_uri_from_upstream_url_and_endpoint() {
    assert_eq!(
        health_uri("http://prefill-1:8000", "/health")
            .unwrap()
            .to_string(),
        "http://prefill-1:8000/health"
    );
    assert_eq!(
        health_uri("http://prefill-1:8000/vllm", "/health")
            .unwrap()
            .to_string(),
        "http://prefill-1:8000/vllm/health"
    );
}

#[test]
fn thresholds_emit_only_on_state_changes() {
    let mut state = PodHealthState::new();
    assert_eq!(state.observe(HealthResult::Healthy, 2, 2), None);
    assert_eq!(state.observe(HealthResult::Healthy, 2, 2), Some(true));
    assert_eq!(state.observe(HealthResult::Healthy, 2, 2), None);
    assert_eq!(state.observe(HealthResult::Unhealthy, 2, 2), None);
    assert_eq!(state.observe(HealthResult::Unhealthy, 2, 2), Some(false));
}

#[test]
fn health_result_thresholds_update_registry_alive_state() {
    let registry = RuntimeRegistry {
        pod_table: PodTable {
            pods: vec![PodEndpoint {
                id: 0,
                pod_id: 0,
                address: "http://pod-0:8000".to_string(),
                healthy: true,
                draining: false,
                max_conns: 100,
                capabilities: crate::upstream::PodCapabilities::single(),
            }],
            by_external_id: HashMap::new(),
        },
        upstreams: UpstreamCatalog::default(),
        single_pods: HostBitmap::full_for_count(1),
        prefill_pods: HostBitmap::empty(),
        decode_pods: HostBitmap::empty(),
        cache_registry: CacheRegistry::new_empty_alive(1),
    };
    let mut states = vec![PodHealthState::new()];

    apply_health_result(
        &registry,
        &mut states,
        0,
        "http://pod-0:8000".to_string(),
        HealthResult::Healthy,
        1,
        1,
    );
    assert!(registry.cache_registry.alive().contains(0));

    apply_health_result(
        &registry,
        &mut states,
        0,
        "http://pod-0:8000".to_string(),
        HealthResult::Unhealthy,
        1,
        1,
    );
    assert!(!registry.cache_registry.alive().contains(0));
}
