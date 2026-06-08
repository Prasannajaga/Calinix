use std::sync::atomic::{AtomicUsize, Ordering};

use crate::bitmap::HostBitmap;
use crate::types::{Pod, PodRole, StepRole};

pub fn filter_candidates(
    pods: &[Pod],
    role: StepRole,
    alive: HostBitmap,
    inflight: &[AtomicUsize; 256],
) -> HostBitmap {
    let mut candidates = HostBitmap::empty();
    for pod in pods {
        if !alive.contains(pod.id) || !pod.healthy {
            continue;
        }
        if !role_matches(&pod.role, &role) {
            continue;
        }
        let current = inflight[pod.id].load(Ordering::Relaxed);
        if current >= pod.max_concurrency {
            continue;
        }
        candidates.set(pod.id);
    }
    candidates
}

fn role_matches(pod_role: &PodRole, step_role: &StepRole) -> bool {
    match step_role {
        StepRole::Single => matches!(pod_role, PodRole::Both),
        StepRole::Prefill => matches!(pod_role, PodRole::Prefill | PodRole::Both),
        StepRole::Decode => matches!(pod_role, PodRole::Decode | PodRole::Both),
    }
}
