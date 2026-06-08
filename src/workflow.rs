use crate::types::{FailurePolicy, PodId, RoutingPlan, RoutingStep, StepRole};

pub fn build_single_plan(pod_id: PodId) -> RoutingPlan {
    RoutingPlan {
        steps: vec![RoutingStep {
            role: StepRole::Single,
            pod_id,
            failure_policy: FailurePolicy::FailFast,
            cache_hint: None,
        }],
    }
}

pub fn build_disaggregated_plan(prefill_pod: PodId, decode_pod: PodId) -> RoutingPlan {
    RoutingPlan {
        steps: vec![
            RoutingStep {
                role: StepRole::Prefill,
                pod_id: prefill_pod,
                failure_policy: FailurePolicy::FailFast,
                cache_hint: None,
            },
            RoutingStep {
                role: StepRole::Decode,
                pod_id: decode_pod,
                failure_policy: FailurePolicy::FailFast,
                cache_hint: Some("from-prefill".to_string()),
            },
        ],
    }
}
