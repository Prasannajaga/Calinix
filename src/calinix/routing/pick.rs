use crate::routing::score::CandidateScore;
use crate::routing::RoutingError;
use crate::upstream::PodId;

pub struct PickStage;

impl PickStage {
    pub fn pick_one(&self, scores: &[CandidateScore]) -> Result<PodId, RoutingError> {
        scores
            .iter()
            .max_by(|left, right| {
                left.final_score
                    .total_cmp(&right.final_score)
                    .then_with(|| right.pod_id.cmp(&left.pod_id))
            })
            .map(|score| score.pod_id)
            .ok_or(RoutingError::NoCandidates)
    }
}
