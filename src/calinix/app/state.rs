use serde::Serialize;
use std::sync::Arc;

use crate::cache_registry::BlockHash;
use crate::proxy::forward::HttpForwarder;
use crate::session::StickyStore;
use crate::upstream::{LoadState, PodEndpoint, RuntimeRegistry};

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<RuntimeRegistry>,
    pub forwarder: HttpForwarder,
    pub loads: Arc<LoadState>,
    pub sticky: Arc<StickyStore>,
}

impl AppState {
    pub fn new(registry: RuntimeRegistry) -> Self {
        let pod_count = registry.total_pods();
        Self {
            registry: Arc::new(registry),
            forwarder: HttpForwarder::new(),
            loads: Arc::new(LoadState::new(pod_count)),
            sticky: Arc::new(StickyStore::new()),
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
    pub block_owners: Vec<BlockOwnerSummary>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockOwnerSummary {
    pub block_hash: BlockHash,
    pub pods: Vec<usize>,
}

impl From<&RuntimeRegistry> for RegistrySummary {
    fn from(registry: &RuntimeRegistry) -> Self {
        let block_owners = registry
            .cache_registry
            .block_owners()
            .into_iter()
            .map(|(block_hash, owners)| BlockOwnerSummary {
                block_hash,
                pods: owners.iter_set_bits(),
            })
            .collect();

        Self {
            total_pods: registry.total_pods(),
            single_pod_count: registry.single_pods.count(),
            prefill_pod_count: registry.prefill_pods.count(),
            decode_pod_count: registry.decode_pods.count(),
            alive_pod_count: registry.cache_registry.alive().count(),
            pods: registry.pod_table.pods.clone(),
            block_owners,
        }
    }
}
