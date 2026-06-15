use crate::protocol::routing_headers::{CalinixMode, RoutingHeaderValues};
use crate::upstream::PodId;

#[derive(Clone, Debug)]
pub enum RoutingPlan {
    Single {
        request_id: String,
        target_pod_id: PodId,
        target_address: String,
        cache_hit: bool,
        cache_prefix_depth: usize,
        route_policy: String,
    },

    Disaggregated {
        request_id: String,
        coordinator_address: String,
        prefill_pod_id: PodId,
        decode_pod_id: PodId,
        cache_hit: bool,
        cache_prefix_depth: usize,
        route_policy: String,
    },
}

impl RoutingPlan {
    pub fn routing_headers(&self) -> RoutingHeaderValues {
        match self {
            Self::Single {
                request_id,
                target_pod_id,
                cache_hit,
                cache_prefix_depth,
                route_policy,
                ..
            } => RoutingHeaderValues {
                request_id: request_id.clone(),
                mode: CalinixMode::Single,
                target_pod_id: Some(target_pod_id.to_string()),
                prefill_pod_id: None,
                decode_pod_id: None,
                cache_hit: *cache_hit,
                cache_prefix_depth: *cache_prefix_depth,
                cache_namespace: None,
                route_policy: route_policy.clone(),
            },
            Self::Disaggregated {
                request_id,
                prefill_pod_id,
                decode_pod_id,
                cache_hit,
                cache_prefix_depth,
                route_policy,
                ..
            } => RoutingHeaderValues {
                request_id: request_id.clone(),
                mode: CalinixMode::Disaggregated,
                target_pod_id: None,
                prefill_pod_id: Some(prefill_pod_id.to_string()),
                decode_pod_id: Some(decode_pod_id.to_string()),
                cache_hit: *cache_hit,
                cache_prefix_depth: *cache_prefix_depth,
                cache_namespace: None,
                route_policy: route_policy.clone(),
            },
        }
    }

    pub fn target_address(&self) -> &str {
        match self {
            Self::Single { target_address, .. } => target_address,
            Self::Disaggregated {
                coordinator_address,
                ..
            } => coordinator_address,
        }
    }
}
