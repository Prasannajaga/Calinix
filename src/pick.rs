use std::collections::HashMap;
use std::sync::Mutex;

use crate::bitmap::HostBitmap;
use crate::types::{CandidateScore, PodId, SessionId};

const STICKY_NOT_TERRIBLE_THRESHOLD: f64 = 20.0;

pub fn pick_one(
    session_id: &str,
    scores: &[CandidateScore],
    session_map: &Mutex<HashMap<SessionId, PodId>>,
    valid_candidates: HostBitmap,
) -> Option<PodId> {
    if scores.is_empty() {
        return None;
    }

    let sticky = session_map
        .lock()
        .expect("session map poisoned")
        .get(session_id)
        .copied();

    let picked = sticky
        .and_then(|pod_id| {
            scores
                .iter()
                .find(|score| {
                    score.pod_id == pod_id
                        && valid_candidates.contains(pod_id)
                        && score.final_score >= STICKY_NOT_TERRIBLE_THRESHOLD
                })
                .map(|score| score.pod_id)
        })
        .or_else(|| {
            scores
                .iter()
                .max_by(|a, b| {
                    a.final_score
                        .partial_cmp(&b.final_score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| b.pod_id.cmp(&a.pod_id))
                })
                .map(|score| score.pod_id)
        });

    if let Some(pod_id) = picked {
        session_map
            .lock()
            .expect("session map poisoned")
            .insert(session_id.to_string(), pod_id);
    }

    picked
}
