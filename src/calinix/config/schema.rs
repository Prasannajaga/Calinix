use serde::Deserialize;

use crate::cache_registry::DEFAULT_SHARD_COUNT;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CalinixConfig {
    pub version: u32,
    pub gateway: GatewayConfig,
    pub health: HealthConfig,
    pub cache_registry: CacheRegistryConfig,
    pub upstreams: UpstreamsConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayConfig {
    pub port: u16,
    pub strategy: Strategy,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Strategy {
    CacheAware,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthConfig {
    pub endpoint: String,
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub healthy_threshold: u8,
    pub unhealthy_threshold: u8,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheRegistryConfig {
    pub enabled: bool,
    pub max_pods: usize,
    #[serde(default = "default_shards_count")]
    pub shards_count: usize,
    pub stale_ttl_ms: u64,
}

fn default_shards_count() -> usize {
    DEFAULT_SHARD_COUNT
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamsConfig {
    pub single: SingleUpstreamsConfig,
    pub dispatch: DispatchUpstreamsConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SingleUpstreamsConfig {
    pub mode: UpstreamMode,
    pub pods: Vec<PodConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DispatchUpstreamsConfig {
    pub mode: UpstreamMode,
    pub prefill: PodGroupConfig,
    pub decode: PodGroupConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodGroupConfig {
    pub pods: Vec<PodConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PodConfig {
    pub id: String,
    pub url: String,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum UpstreamMode {
    Single,
    Dispatch,
}
