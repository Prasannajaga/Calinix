#![allow(dead_code)]

use crate::bitmap::HostBitmap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Single,
    Disaggregated,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub mode: Mode,
    pub prompt: String,
    pub hits: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PodRole {
    Prefill,
    Decode,
    Both,
}

#[derive(Clone, Debug)]
pub struct Pod {
    pub id: usize,
    pub role: PodRole,
    pub node: &'static str,
}

impl Pod {
    pub fn new(id: usize, role: PodRole, node: &'static str) -> Self {
        Self { id, role, node }
    }
}

#[derive(Clone, Debug)]
pub struct PreparedRequest {
    pub prompt: String,
    pub cumulative_hashes: Vec<u64>,
}

#[derive(Clone, Debug)]
pub struct FilteredCandidates {
    pub role: PodRole,
    pub candidate_pods: HostBitmap,
}

#[derive(Clone, Debug)]
pub struct ScoredCandidate {
    pub pod_id: usize,
    pub prefix_len: usize,
    pub score: usize,
}

#[derive(Clone, Debug)]
pub struct PickedPod {
    pub pod_id: usize,
}

#[derive(Clone, Debug)]
pub enum ExecutionPlan {
    Single {
        pod_id: usize,
    },
    Disaggregated {
        prefill_pod: usize,
        decode_pod: usize,
    },
}

#[derive(Clone, Debug, Default)]
pub struct RouteResult {
    pub prefill_pod: Option<usize>,
    pub decode_pod: Option<usize>,
    pub response_pod: Option<usize>,
    pub text: String,
}
