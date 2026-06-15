pub type PrefixDepth = usize;
pub type ShardId = usize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefixDepthByPod {
    pub pod_id: usize,
    pub depth: PrefixDepth,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheRegistryStats {
    pub pod_count: usize,
    pub alive_pods: usize,
    pub total_entries: usize,
    pub non_empty_shards: usize,
}
