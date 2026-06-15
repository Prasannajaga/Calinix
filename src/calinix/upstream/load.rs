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
        pod.healthy && pod.max_conns > 0 && self.inflight(pod.id) < pod.max_conns
    }

    pub fn score(&self, pod: &PodEndpoint) -> f64 {
        if pod.max_conns == 0 {
            return 0.0;
        }

        let usage = self.inflight(pod.id) as f64 / pod.max_conns as f64;
        ((1.0 - usage).clamp(0.0, 1.0)) * 100.0
    }

    pub fn set_inflight_for_test(&self, pod_id: PodId, value: usize) {
        if let Some(inflight) = self.inflight.get(pod_id as usize) {
            inflight.store(value, Ordering::Relaxed);
        }
    }
}
