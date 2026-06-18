use std::collections::HashMap;

use serde::Serialize;

use crate::cache_registry::{CacheRegistry, HostBitmap};
use crate::config::{CalinixConfig, PodConfig};

use super::pool::{UpstreamCatalog, UpstreamGroup};
use super::roles::{PodCapabilities, PodRole};

pub type PodId = u16;
pub type UpstreamId = u16;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodEndpoint {
    pub id: PodId,
    pub pod_id: PodId,
    pub address: String,
    pub healthy: bool,
    pub draining: bool,
    pub max_conns: usize,
    pub capabilities: PodCapabilities,
}

#[derive(Debug)]
pub struct PodTable {
    pub pods: Vec<PodEndpoint>,
    pub by_external_id: HashMap<String, PodId>,
}

#[derive(Debug)]
pub struct RuntimeRegistry {
    pub pod_table: PodTable,
    pub upstreams: UpstreamCatalog,
    pub single_pods: HostBitmap,
    pub prefill_pods: HostBitmap,
    pub decode_pods: HostBitmap,
    pub cache_registry: CacheRegistry,
}

impl RuntimeRegistry {
    pub fn from_config(config: &CalinixConfig) -> Result<Self, String> {
        let total_pods = config.upstreams.single.pods.len()
            + config.upstreams.dispatch.prefill.pods.len()
            + config.upstreams.dispatch.decode.pods.len();

        let mut pods = Vec::with_capacity(total_pods);
        let mut by_external_id = HashMap::with_capacity(total_pods);
        let mut single_pods = HostBitmap::empty();
        let mut prefill_pods = HostBitmap::empty();
        let mut decode_pods = HostBitmap::empty();

        push_pods(
            &config.upstreams.single.pods,
            PodRole::Single,
            &mut pods,
            &mut by_external_id,
            &mut single_pods,
        )?;
        push_pods(
            &config.upstreams.dispatch.prefill.pods,
            PodRole::Prefill,
            &mut pods,
            &mut by_external_id,
            &mut prefill_pods,
        )?;
        push_pods(
            &config.upstreams.dispatch.decode.pods,
            PodRole::Decode,
            &mut pods,
            &mut by_external_id,
            &mut decode_pods,
        )?;

        let upstreams = catalog_from_parts(&pods, &single_pods, &prefill_pods, &decode_pods);

        Ok(Self {
            pod_table: PodTable {
                pods,
                by_external_id,
            },
            upstreams,
            single_pods,
            prefill_pods,
            decode_pods,
            cache_registry: CacheRegistry::with_shards_empty_alive(
                total_pods,
                config.cache_registry.shards_count,
            ),
        })
    }

    pub fn total_pods(&self) -> usize {
        self.pod_table.pods.len()
    }
}

fn catalog_from_parts(
    pods: &[PodEndpoint],
    single_pods: &HostBitmap,
    prefill_pods: &HostBitmap,
    decode_pods: &HostBitmap,
) -> UpstreamCatalog {
    UpstreamCatalog {
        pods: pods.to_vec(),
        groups: vec![
            UpstreamGroup {
                id: 1,
                name: "single".to_string(),
                role: PodRole::Single,
                pods: pod_ids_from_bitmap(single_pods),
                pod_bitmap: single_pods.clone(),
            },
            UpstreamGroup {
                id: 2,
                name: "prefill".to_string(),
                role: PodRole::Prefill,
                pods: pod_ids_from_bitmap(prefill_pods),
                pod_bitmap: prefill_pods.clone(),
            },
            UpstreamGroup {
                id: 3,
                name: "decode".to_string(),
                role: PodRole::Decode,
                pods: pod_ids_from_bitmap(decode_pods),
                pod_bitmap: decode_pods.clone(),
            },
        ],
    }
}

fn pod_ids_from_bitmap(bitmap: &HostBitmap) -> Vec<PodId> {
    let mut pod_ids = Vec::new();
    bitmap.for_each_set_bit(|pod_id| {
        if let Ok(pod_id) = PodId::try_from(pod_id) {
            pod_ids.push(pod_id);
        }
    });
    pod_ids
}

fn push_pods(
    configured: &[PodConfig],
    role: PodRole,
    pods: &mut Vec<PodEndpoint>,
    by_external_id: &mut HashMap<String, PodId>,
    role_bitmap: &mut HostBitmap,
) -> Result<(), String> {
    for pod in configured {
        let pod_id = u16::try_from(pods.len())
            .map_err(|_| "configured pod count exceeds u16 PodId capacity".to_string())?;
        by_external_id.insert(pod.id.clone(), pod_id);
        role_bitmap.set(pod_id as usize);
        let capabilities = pod.capabilities.unwrap_or_else(|| role.into());
        if !capabilities.supports(role) {
            return Err(format!(
                "pod '{}' capabilities must support its upstream role {:?}",
                pod.id, role
            ));
        }

        pods.push(PodEndpoint {
            id: pod_id,
            pod_id,
            address: pod.url.clone(),
            healthy: pod.healthy,
            draining: pod.draining,
            max_conns: pod.max_conns,
            capabilities,
        });
    }
    Ok(())
}
