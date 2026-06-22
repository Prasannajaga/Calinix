use http::{HeaderMap, HeaderValue};

use crate::proxy::forward::headers_for_attempt;
use crate::protocol::routing_headers::{DECODE_POD_ID, PREFILL_POD_ID, TARGET_POD_ID};
use crate::routing::plan::RoutingPlan;

// --- From proxy/forward.rs ---

#[test]
fn single_fallback_rewrites_target_pod_header() {
    let mut headers = HeaderMap::new();
    headers.insert(TARGET_POD_ID, HeaderValue::from_static("1"));
    let plan = RoutingPlan::Single {
        request_id: "req".to_string(),
        target_pod_id: 1,
        target_address: "http://single-1:8000".to_string(),
        cache_hit: false,
        cache_prefix_depth: 0,
        route_policy: "default".to_string(),
    };

    let rewritten = headers_for_attempt(&headers, &plan, 7).unwrap();

    assert_eq!(rewritten.get(TARGET_POD_ID).unwrap(), "7");
}

#[test]
fn disaggregated_fallback_rewrites_prefill_and_preserves_decode() {
    let mut headers = HeaderMap::new();
    headers.insert(PREFILL_POD_ID, HeaderValue::from_static("2"));
    headers.insert(DECODE_POD_ID, HeaderValue::from_static("5"));
    let plan = RoutingPlan::Disaggregated {
        request_id: "req".to_string(),
        coordinator_address: "http://prefill-1:8000".to_string(),
        prefill_pod_id: 2,
        decode_pod_id: 5,
        cache_hit: false,
        cache_prefix_depth: 0,
        route_policy: "default".to_string(),
    };

    let rewritten = headers_for_attempt(&headers, &plan, 3).unwrap();

    assert_eq!(rewritten.get(PREFILL_POD_ID).unwrap(), "3");
    assert_eq!(rewritten.get(DECODE_POD_ID).unwrap(), "5");
}
