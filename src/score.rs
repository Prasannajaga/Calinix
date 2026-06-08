use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use crate::bitmap::HostBitmap;
use crate::indexer::{longest_prefix_lengths_for_candidates, ShardedBlockIndexer};
use crate::types::{CandidateScore, Pod, PodId, RequestContext, SessionId, StepRole};

pub fn score_candidates(
    indexer: &ShardedBlockIndexer,
    ctx: &RequestContext,
    pods: &[Pod],
    valid_candidates: HostBitmap,
    inflight: &[AtomicUsize; 256],
    session_map: &Mutex<HashMap<SessionId, PodId>>,
    role: StepRole,
    selected_prefill: Option<PodId>,
) -> Vec<CandidateScore> {
    let prefix_lengths =
        longest_prefix_lengths_for_candidates(indexer, &ctx.cumulative_hashes, valid_candidates);
    let sticky_pod = session_map
        .lock()
        .expect("session map poisoned")
        .get(&ctx.session_id)
        .copied();
    let max_prefix = ctx.cumulative_hashes.len().max(1) as f64;
    let weights = if role == StepRole::Decode && selected_prefill.is_some() {
        (0.20, 0.30, 0.20, 0.30)
    } else {
        (0.50, 0.25, 0.20, 0.05)
    };

    let mut scores = valid_candidates
        .iter_set_bits()
        .into_iter()
        .filter_map(|pod_id| {
            pods.iter()
                .find(|pod| pod.id == pod_id)
                .map(|pod| (pod_id, pod))
        })
        .map(|(pod_id, pod)| {
            let cache_prefix_len = prefix_lengths[pod_id];
            let cache_score = if ctx.cumulative_hashes.is_empty() {
                0.0
            } else {
                100.0 * cache_prefix_len as f64 / max_prefix
            };
            let current = inflight[pod_id].load(Ordering::Relaxed) as f64;
            let max_concurrency = pod.max_concurrency.max(1) as f64;
            let load_score = (100.0 * (1.0 - current / max_concurrency)).clamp(0.0, 100.0);
            let sticky_score = if sticky_pod == Some(pod_id) && valid_candidates.contains(pod_id) {
                100.0
            } else {
                0.0
            };
            let locality_score = locality_score(pods, pod_id, role.clone(), selected_prefill);
            let final_score = weights.0 * cache_score
                + weights.1 * load_score
                + weights.2 * sticky_score
                + weights.3 * locality_score;

            CandidateScore {
                pod_id,
                cache_prefix_len,
                cache_score,
                load_score,
                locality_score,
                sticky_score,
                final_score,
            }
        })
        .collect::<Vec<_>>();

    scores.sort_by(|a, b| {
        b.final_score
            .partial_cmp(&a.final_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.pod_id.cmp(&b.pod_id))
    });
    scores
}

fn locality_score(
    pods: &[Pod],
    pod_id: PodId,
    role: StepRole,
    selected_prefill: Option<PodId>,
) -> f64 {
    if role != StepRole::Decode {
        return 50.0;
    }

    let Some(prefill_id) = selected_prefill else {
        return 50.0;
    };
    let Some(prefill) = pods.iter().find(|pod| pod.id == prefill_id) else {
        return 30.0;
    };
    let Some(decode) = pods.iter().find(|pod| pod.id == pod_id) else {
        return 30.0;
    };

    if prefill.node == decode.node {
        100.0
    } else {
        30.0
    }
}
