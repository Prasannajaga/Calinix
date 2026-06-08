#![allow(dead_code)]

pub type PodId = usize;
pub type BlockHash = u64;
pub type SessionId = String;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PodRole {
    Prefill,
    Decode,
    Both,
}

#[derive(Clone, Debug)]
pub struct Pod {
    pub id: PodId,
    pub role: PodRole,
    pub node: String,
    pub addr: String,
    pub healthy: bool,
    pub max_concurrency: usize,
}

#[derive(Clone, Debug)]
pub enum CacheEvent {
    Registered {
        pod_id: PodId,
        block_hash: BlockHash,
    },
    Evicted {
        pod_id: PodId,
        block_hash: BlockHash,
    },
    Shutdown {
        pod_id: PodId,
    },
}

#[derive(Clone, Debug)]
pub struct RequestContext {
    pub session_id: SessionId,
    pub prompt: String,
    pub tokens: Vec<String>,
    pub block_hashes: Vec<BlockHash>,
    pub cumulative_hashes: Vec<BlockHash>,
    pub mode: RoutingMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoutingMode {
    Single,
    Disaggregated,
}

#[derive(Clone, Debug)]
pub struct CandidateScore {
    pub pod_id: PodId,
    pub cache_prefix_len: usize,
    pub cache_score: f64,
    pub load_score: f64,
    pub locality_score: f64,
    pub sticky_score: f64,
    pub final_score: f64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepRole {
    Single,
    Prefill,
    Decode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FailurePolicy {
    FailFast,
    RetryNextBest,
}

#[derive(Clone, Debug)]
pub struct RoutingStep {
    pub role: StepRole,
    pub pod_id: PodId,
    pub failure_policy: FailurePolicy,
    pub cache_hint: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RoutingPlan {
    pub steps: Vec<RoutingStep>,
}
