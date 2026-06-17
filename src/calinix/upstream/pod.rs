use std::collections::HashMap;

use serde::Serialize;

use crate::cache_registry::{CacheRegistry, HostBitmap};
use crate::config::{CalinixConfig, PodConfig};

pub type PodId = u16;
pub type UpstreamId = u16;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodEndpoint {
    pub id: PodId,
    pub pod_id: PodId,
    pub address: String,
}

#[derive(Debug)]
pub struct PodTable {
    pub pods: Vec<PodEndpoint>,
    pub by_external_id: HashMap<String, PodId>,
}

#[derive(Debug)]
pub struct RuntimeRegistry {
    pub pod_table: PodTable,
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
            &mut pods,
            &mut by_external_id,
            &mut single_pods,
        )?;
        push_pods(
            &config.upstreams.dispatch.prefill.pods,
            &mut pods,
            &mut by_external_id,
            &mut prefill_pods,
        )?;
        push_pods(
            &config.upstreams.dispatch.decode.pods,
            &mut pods,
            &mut by_external_id,
            &mut decode_pods,
        )?;

        Ok(Self {
            pod_table: PodTable {
                pods,
                by_external_id,
            },
            single_pods,
            prefill_pods,
            decode_pods,
            cache_registry: CacheRegistry::new_empty_alive(total_pods),
        })
    }

    pub fn total_pods(&self) -> usize {
        self.pod_table.pods.len()
    }
}

fn push_pods(
    configured: &[PodConfig],
    pods: &mut Vec<PodEndpoint>,
    by_external_id: &mut HashMap<String, PodId>,
    role_bitmap: &mut HostBitmap,
) -> Result<(), String> {
    for pod in configured {
        let pod_id = u16::try_from(pods.len())
            .map_err(|_| "configured pod count exceeds u16 PodId capacity".to_string())?;
        by_external_id.insert(pod.id.clone(), pod_id);
        role_bitmap.set(pod_id as usize);
        pods.push(PodEndpoint {
            id: pod_id,
            pod_id,
            address: pod.url.clone(),
        });
    }
    Ok(())
}
