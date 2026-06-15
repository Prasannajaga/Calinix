use crate::cache_registry::HostBitmap;
use crate::routing::context::RoutingContext;
use crate::routing::filter::RequiredRole;
use crate::session::StickyStore;
use crate::upstream::{LoadState, PodEndpoint, PodId, UpstreamCatalog};

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
        let max_depth = ctx.cumulative_hashes.len().max(1) as f64;
        let sticky_pod = ctx
            .openai
            .session_key
            .as_deref()
            .and_then(|session_key| sticky.previous_pod(session_key));
        let prefill = selected_prefill.and_then(|pod_id| upstreams.pod(pod_id));

        let mut scores = Vec::new();
        candidates.for_each_set_bit(|pod_id| {
            let Ok(pod_id) = u16::try_from(pod_id) else {
                return;
            };
            let Some(pod) = upstreams.pod(pod_id) else {
                return;
            };
            if !loads.can_accept(pod) {
                return;
            }

            let cache_prefix_depth = cache_depths.get(pod_id as usize).copied().unwrap_or(0);
            let cache_score = ((cache_prefix_depth as f64 / max_depth).clamp(0.0, 1.0)) * 100.0;
            let load_score = loads.score(pod);
            let sticky_score = if sticky_pod == Some(pod_id) {
                100.0
            } else {
                0.0
            };
            let locality_score = locality_score(role, pod, prefill);
            let final_score = cache_score * weights.cache
                + load_score * weights.load
                + sticky_score * weights.sticky
                + locality_score * weights.locality;

            scores.push(CandidateScore {
                pod_id,
                cache_prefix_depth,
                cache_score,
                load_score,
                sticky_score,
                locality_score,
                final_score,
            });
        });

        scores.sort_by(|left, right| {
            right
                .final_score
                .total_cmp(&left.final_score)
                .then_with(|| left.pod_id.cmp(&right.pod_id))
        });
        scores
    }

    fn weights_for(&self, role: RequiredRole) -> ScoreWeights {
        match role {
            RequiredRole::Single => self.single_weights,
            RequiredRole::Prefill => self.prefill_weights,
            RequiredRole::Decode => self.decode_weights,
        }
    }
}

fn locality_score(role: RequiredRole, pod: &PodEndpoint, prefill: Option<&PodEndpoint>) -> f64 {
    if role != RequiredRole::Decode {
        return 0.0;
    }

    let Some(prefill) = prefill else {
        return 0.0;
    };

    if pod.node.is_some() && pod.node == prefill.node {
        100.0
    } else if pod.zone.is_some() && pod.zone == prefill.zone {
        60.0
    } else {
        0.0
    }
}
