use crate::cache_registry::{BlockHash, HostBitmap};
use crate::protocol::openai::OpenAiRoutingView;
use crate::protocol::routing_headers::CalinixMode;

#[derive(Clone, Debug)]
pub struct RoutingContext {
    pub request_id: String,
    pub path: String,
    pub method: String,
    pub mode: CalinixMode,
    pub openai: OpenAiRoutingView,
    pub tokens: Vec<String>,
    pub cumulative_hashes: Vec<BlockHash>,
    pub cache_namespace: String,
    pub candidate_single: HostBitmap,
    pub candidate_prefill: HostBitmap,
    pub candidate_decode: HostBitmap,
}

#[derive(Clone, Debug)]
pub struct PreparedRequest {
    pub ctx: RoutingContext,
}
