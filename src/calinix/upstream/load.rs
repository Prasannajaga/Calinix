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

    pub fn track(&self, pod_id: PodId) -> Option<InflightGuard<'_>> {
        let inflight = self.inflight.get(pod_id as usize)?;
        inflight.fetch_add(1, Ordering::Relaxed);
        Some(InflightGuard {
            load_state: self,
            pod_id,
        })
    }

    pub fn set_inflight_for_test(&self, pod_id: PodId, value: usize) {
        if let Some(inflight) = self.inflight.get(pod_id as usize) {
            inflight.store(value, Ordering::Relaxed);
        }
    }

    fn decrement(&self, pod_id: PodId) {
        if let Some(inflight) = self.inflight.get(pod_id as usize) {
            inflight
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_sub(1))
                })
                .ok();
        }
    }
}

pub struct InflightGuard<'a> {
    load_state: &'a LoadState,
    pod_id: PodId,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.load_state.decrement(self.pod_id);
    }
}

#[cfg(test)]
mod tests {
    use super::LoadState;

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
}
