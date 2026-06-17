use std::sync::atomic::{AtomicUsize, Ordering};

use crate::upstream::pod::{PodEndpoint, PodId};

#[derive(Debug)]
pub struct LoadState {
    inflight: Vec<AtomicUsize>,
}

impl LoadState {
    pub fn new(pod_count: usize) -> Self {
        Self {
            inflight: (0..pod_count).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    pub fn inflight(&self, pod_id: PodId) -> usize {
        self.inflight
            .get(pod_id as usize)
            .map(|value| value.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn can_accept(&self, pod: &PodEndpoint) -> bool {
        self.inflight.get(pod.id as usize).is_some()
    }

    pub fn score(&self, pod: &PodEndpoint) -> f64 {
        100.0 / (self.inflight(pod.id) + 1) as f64
    }

    pub fn set_inflight_for_test(&self, pod_id: PodId, value: usize) {
        if let Some(inflight) = self.inflight.get(pod_id as usize) {
            inflight.store(value, Ordering::Relaxed);
        }
    }
}
