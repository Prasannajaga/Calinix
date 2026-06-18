use crate::cache_registry::{CacheRegistry, HostBitmap};
use crate::routing::context::RoutingContext;
use crate::routing::filter::RequiredRole;
use crate::routing::RoutingError;
use crate::session::StickyStore;
use crate::upstream::{LoadState, PodId, UpstreamCatalog};

#[derive(Clone, Debug, PartialEq)]
pub struct CandidateScore {
    pub pod_id: PodId,
    pub cache_prefix_depth: usize,
    pub cache_score: f64,
    pub load_score: f64,
    pub sticky_score: f64,
    pub locality_score: f64,
    pub final_score: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PickedCandidate {
    pub pod_id: PodId,
    pub cache_prefix_depth: usize,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScoreWeights {
    pub cache: f64,
    pub load: f64,
    pub sticky: f64,
    pub locality: f64,
}

#[derive(Clone, Debug)]
pub struct ScoreStage {
    pub single_weights: ScoreWeights,
    pub prefill_weights: ScoreWeights,
    pub decode_weights: ScoreWeights,
}

impl ScoreStage {
    pub fn pick_best_cache_candidate(
        &self,
        ctx: &RoutingContext,
        candidates: HostBitmap,
        cache_registry: &CacheRegistry,
        upstreams: &UpstreamCatalog,
        loads: &LoadState,
    ) -> Option<PickedCandidate> {
        let cache_depths =
            cache_registry.longest_prefix_lengths(&ctx.cumulative_hashes, candidates.clone());
        self.pick_best_candidate(
            ctx,
            RequiredRole::Single,
            candidates,
            &cache_depths,
            upstreams,
            loads,
            &StickyStore::new(),
            None,
        )
        .ok()
        .map(|score| PickedCandidate {
            pod_id: score.pod_id,
            cache_prefix_depth: score.cache_prefix_depth,
        })
    }

    pub fn pick_first_available(
        &self,
        candidates: HostBitmap,
        upstreams: &UpstreamCatalog,
        loads: &LoadState,
    ) -> Option<PodId> {
        let cache_depths = vec![0; upstreams.pods.len()];
        self.pick_best_candidate(
            &RoutingContext {
                request_id: String::new(),
                path: String::new(),
                method: String::new(),
                mode: crate::protocol::routing_headers::CalinixMode::Single,
                openai: crate::protocol::openai::OpenAiRoutingView {
                    kind: crate::protocol::openai::OpenAiRequestKind::Unknown,
                    model: None,
                    prompt_text: String::new(),
                    session_key: None,
                    stream: false,
                },
                tokens: Vec::new(),
                cumulative_hashes: Vec::new(),
                cache_namespace: String::new(),
                candidate_single: HostBitmap::empty(),
                candidate_prefill: HostBitmap::empty(),
                candidate_decode: HostBitmap::empty(),
            },
            RequiredRole::Single,
            candidates,
            &cache_depths,
            upstreams,
            loads,
            &StickyStore::new(),
            None,
        )
        .ok()
        .map(|score| score.pod_id)
    }

    pub fn score_candidates(
        &self,
        ctx: &RoutingContext,
        role: RequiredRole,
        candidates: HostBitmap,
        cache_depths: &[usize],
        upstreams: &UpstreamCatalog,
        loads: &LoadState,
        sticky: &StickyStore,
        selected_prefill: Option<PodId>,
    ) -> Vec<CandidateScore> {
        let weights = self.weights_for(role);
        let max_depth = ctx.cumulative_hashes.len();
        let mut scores = Vec::new();
        candidates.for_each_set_bit(|pod_id| {
            if let Some(score) = self.score_candidate(
                ctx,
                pod_id,
                weights,
                max_depth,
                cache_depths,
                upstreams,
                loads,
                sticky,
                selected_prefill,
            ) {
                scores.push(score);
            }
        });

        scores.sort_by(better_candidate_order);
        scores
    }

    pub fn pick_best_candidate(
        &self,
        ctx: &RoutingContext,
        role: RequiredRole,
        candidates: HostBitmap,
        cache_depths: &[usize],
        upstreams: &UpstreamCatalog,
        loads: &LoadState,
        sticky: &StickyStore,
        selected_prefill: Option<PodId>,
    ) -> Result<CandidateScore, RoutingError> {
        self.score_candidates(
            ctx,
            role,
            candidates,
            cache_depths,
            upstreams,
            loads,
            sticky,
            selected_prefill,
        )
        .into_iter()
        .next()
        .ok_or(RoutingError::NoCandidates)
    }

    fn score_candidate(
        &self,
        ctx: &RoutingContext,
        pod_id: usize,
        weights: ScoreWeights,
        max_prefix_depth: usize,
        cache_depths: &[usize],
        upstreams: &UpstreamCatalog,
        loads: &LoadState,
        sticky: &StickyStore,
        selected_prefill: Option<PodId>,
    ) -> Option<CandidateScore> {
        let pod_id = PodId::try_from(pod_id).ok()?;
        let pod = upstreams.pod(pod_id)?;
        let cache_prefix_depth = cache_depths.get(pod_id as usize).copied().unwrap_or(0);
        let cache_score = if max_prefix_depth == 0 {
            0.0
        } else {
            100.0 * cache_prefix_depth as f64 / max_prefix_depth as f64
        };
        let load_score = loads.score(pod);
        let sticky_score = ctx
            .openai
            .session_key
            .as_deref()
            .and_then(|session_key| sticky.previous_pod(session_key))
            .map_or(
                0.0,
                |sticky_pod| {
                    if sticky_pod == pod_id {
                        100.0
                    } else {
                        0.0
                    }
                },
            );
        let locality_score =
            selected_prefill.map_or(
                0.0,
                |prefill_pod_id| {
                    if prefill_pod_id == pod_id {
                        100.0
                    } else {
                        0.0
                    }
                },
            );
        let final_score = cache_score * weights.cache
            + load_score * weights.load
            + sticky_score * weights.sticky
            + locality_score * weights.locality;

        Some(CandidateScore {
            pod_id,
            cache_prefix_depth,
            cache_score,
            load_score,
            sticky_score,
            locality_score,
            final_score,
        })
    }

    fn weights_for(&self, role: RequiredRole) -> ScoreWeights {
        match role {
            RequiredRole::Single => self.single_weights,
            RequiredRole::Prefill => self.prefill_weights,
            RequiredRole::Decode => self.decode_weights,
        }
    }
}

fn better_candidate_order(left: &CandidateScore, right: &CandidateScore) -> std::cmp::Ordering {
    right
        .final_score
        .total_cmp(&left.final_score)
        .then_with(|| left.pod_id.cmp(&right.pod_id))
}
