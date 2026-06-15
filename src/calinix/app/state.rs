use std::sync::Arc;

use serde::Serialize;

use crate::upstream::{PodEndpoint, RuntimeRegistry};

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<RuntimeRegistry>,
}

impl AppState {
    pub fn new(registry: RuntimeRegistry) -> Self {
        Self {
            registry: Arc::new(registry),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegistrySummary {
    pub total_pods: usize,
    pub single_pod_count: usize,
    pub prefill_pod_count: usize,
    pub decode_pod_count: usize,
    pub alive_pod_count: usize,
    pub pods: Vec<PodEndpoint>,
}

impl From<&RuntimeRegistry> for RegistrySummary {
    fn from(registry: &RuntimeRegistry) -> Self {
        Self {
            total_pods: registry.total_pods(),
            single_pod_count: registry.single_pods.count(),
            prefill_pod_count: registry.prefill_pods.count(),
            decode_pod_count: registry.decode_pods.count(),
            alive_pod_count: registry.cache_registry.alive().count(),
            pods: registry.pod_table.pods.clone(),
        }
    }
}
