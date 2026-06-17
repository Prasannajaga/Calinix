use http::HeaderMap;

use crate::cache_registry::HostBitmap;
use crate::protocol::routing_headers::{inject_routing_headers, CalinixMode};
use crate::routing::context::RoutingContext;
use crate::routing::filter::{FilterStage, RequiredRole, RoutePolicy};
use crate::routing::pick::PickStage;
use crate::routing::plan::RoutingPlan;
use crate::routing::prepare::{PrepareInput, PrepareStage};
use crate::routing::score::{ScoreStage, ScoreWeights};
use crate::routing::RoutingError;
use crate::session::StickyStore;
use crate::upstream::{LoadState, PodRole, RuntimeRegistry, UpstreamCatalog, UpstreamGroup};

#[derive(Clone)]
pub struct RoutedRequest {
    pub plan: RoutingPlan,
    pub forwarding_headers: HeaderMap,
}

pub struct RoutingPipeline {
    pub default_mode: CalinixMode,
    pub block_size: usize,
    pub route_policy: RoutePolicy,
    pub score_stage: ScoreStage,
}

impl Default for RoutingPipeline {
    fn default() -> Self {
        Self {
            default_mode: CalinixMode::Single,
            block_size: 4,
            route_policy: RoutePolicy {
                name: "default".to_string(),
                single_upstream: "single".to_string(),
                prefill_upstream: "prefill".to_string(),
                decode_upstream: "decode".to_string(),
                require_healthy: true,
            },
            score_stage: ScoreStage {
                single_weights: ScoreWeights {
                    cache: 0.70,
                    load: 0.25,
                    sticky: 0.05,
                    locality: 0.0,
                },
                prefill_weights: ScoreWeights {
                    cache: 0.80,
                    load: 0.15,
                    sticky: 0.05,
                    locality: 0.0,
                },
                decode_weights: ScoreWeights {
                    cache: 0.0,
                    load: 0.35,
                    sticky: 0.0,
                    locality: 0.65,
                },
            },
        }
    }
}

impl RoutingPipeline {
    pub fn route_openai_request(
        &self,
        registry: &RuntimeRegistry,
        path: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<RoutedRequest, RoutingError> {
        let prepared = PrepareStage {
            default_mode: self.default_mode.clone(),
            block_size: self.block_size,
        }
        .run(PrepareInput {
            path,
            method: "POST",
            headers,
            body,
        })?;
        let ctx = prepared.ctx;
        let upstreams = catalog_from_registry(registry);
        let available = available_pods(registry, self.route_policy.require_healthy);
        let loads = LoadState::new(registry.total_pods());
        let sticky = StickyStore::new();

        let plan = match ctx.mode {
            CalinixMode::Single => build_single_plan(
                registry,
                &upstreams,
                &self.route_policy,
                &loads,
                &sticky,
                &self.score_stage,
                available,
                &ctx,
            )?,
            CalinixMode::Disaggregated => build_disaggregated_plan(
                registry,
                &upstreams,
                &self.route_policy,
                &loads,
                &sticky,
                &self.score_stage,
                available,
                &ctx,
            )?,
        };

        let mut forwarding_headers = headers.clone();
        let mut routing_headers = plan.routing_headers();
        routing_headers.cache_namespace = Some(ctx.cache_namespace);
        inject_routing_headers(&mut forwarding_headers, &routing_headers)
            .map_err(|err| RoutingError::InvalidMode(err.to_string()))?;

        Ok(RoutedRequest {
            plan,
            forwarding_headers,
        })
    }
}

fn build_single_plan(
    registry: &RuntimeRegistry,
    upstreams: &UpstreamCatalog,
    route_policy: &RoutePolicy,
    loads: &LoadState,
    sticky: &StickyStore,
    score: &ScoreStage,
    alive: HostBitmap,
    ctx: &RoutingContext,
) -> Result<RoutingPlan, RoutingError> {
    let filter = FilterStage;
    let candidates =
        filter.candidates_for_role(upstreams, RequiredRole::Single, route_policy, alive);
    let cache_depths = registry
        .cache_registry
        .longest_prefix_lengths(&ctx.cumulative_hashes, candidates.clone());
    let scores = score.score_candidates(
        ctx,
        RequiredRole::Single,
        candidates,
        &cache_depths,
        upstreams,
        loads,
        sticky,
        None,
    );
    let pod_id = PickStage.pick_one(&scores)?;
    let pod = upstreams
        .pod(pod_id)
        .ok_or(RoutingError::MissingPod(pod_id))?;
    let cache_prefix_depth = scores
        .iter()
        .find(|score| score.pod_id == pod_id)
        .map(|score| score.cache_prefix_depth)
        .unwrap_or(0);

    Ok(RoutingPlan::Single {
        request_id: ctx.request_id.clone(),
        target_pod_id: pod_id,
        target_address: pod.address.clone(),
        cache_hit: cache_prefix_depth > 0,
        cache_prefix_depth,
        route_policy: route_policy.name.clone(),
    })
}

fn build_disaggregated_plan(
    registry: &RuntimeRegistry,
    upstreams: &UpstreamCatalog,
    route_policy: &RoutePolicy,
    loads: &LoadState,
    sticky: &StickyStore,
    score: &ScoreStage,
    alive: HostBitmap,
    ctx: &RoutingContext,
) -> Result<RoutingPlan, RoutingError> {
    let filter = FilterStage;
    let prefill_candidates = filter.candidates_for_role(
        upstreams,
        RequiredRole::Prefill,
        route_policy,
        alive.clone(),
    );
    let prefill_cache_depths = registry
        .cache_registry
        .longest_prefix_lengths(&ctx.cumulative_hashes, prefill_candidates.clone());
    let prefill_scores = score.score_candidates(
        ctx,
        RequiredRole::Prefill,
        prefill_candidates,
        &prefill_cache_depths,
        upstreams,
        loads,
        sticky,
        None,
    );
    let prefill_pod_id = PickStage.pick_one(&prefill_scores)?;
    let prefill_pod = upstreams
        .pod(prefill_pod_id)
        .ok_or(RoutingError::MissingPod(prefill_pod_id))?;
    let cache_prefix_depth = prefill_scores
        .iter()
        .find(|score| score.pod_id == prefill_pod_id)
        .map(|score| score.cache_prefix_depth)
        .unwrap_or(0);

    let decode_candidates =
        filter.candidates_for_role(upstreams, RequiredRole::Decode, route_policy, alive);
    let decode_scores = score.score_candidates(
        ctx,
        RequiredRole::Decode,
        decode_candidates,
        &[],
        upstreams,
        loads,
        sticky,
        Some(prefill_pod_id),
    );
    let decode_pod_id = PickStage.pick_one(&decode_scores)?;

    Ok(RoutingPlan::Disaggregated {
        request_id: ctx.request_id.clone(),
        coordinator_address: prefill_pod.address.clone(),
        prefill_pod_id,
        decode_pod_id,
        cache_hit: cache_prefix_depth > 0,
        cache_prefix_depth,
        route_policy: route_policy.name.clone(),
    })
}

fn catalog_from_registry(registry: &RuntimeRegistry) -> UpstreamCatalog {
    UpstreamCatalog {
        pods: registry.pod_table.pods.clone(),
        groups: vec![
            UpstreamGroup {
                id: 1,
                name: "single".to_string(),
                role: PodRole::Single,
                pods: pod_ids_from_bitmap(&registry.single_pods),
            },
            UpstreamGroup {
                id: 2,
                name: "prefill".to_string(),
                role: PodRole::Prefill,
                pods: pod_ids_from_bitmap(&registry.prefill_pods),
            },
            UpstreamGroup {
                id: 3,
                name: "decode".to_string(),
                role: PodRole::Decode,
                pods: pod_ids_from_bitmap(&registry.decode_pods),
            },
        ],
    }
}

fn pod_ids_from_bitmap(bitmap: &HostBitmap) -> Vec<u16> {
    let mut pod_ids = Vec::new();
    bitmap.for_each_set_bit(|pod_id| {
        if let Ok(pod_id) = u16::try_from(pod_id) {
            pod_ids.push(pod_id);
        }
    });
    pod_ids
}

fn available_pods(registry: &RuntimeRegistry, require_healthy: bool) -> HostBitmap {
    if !require_healthy {
        return all_configured_pods(registry);
    }

    let alive = registry.cache_registry.alive();
    if !alive.is_empty() {
        return alive;
    }

    all_configured_pods(registry)
}

fn all_configured_pods(registry: &RuntimeRegistry) -> HostBitmap {
    let mut configured = HostBitmap::empty();
    for pod in &registry.pod_table.pods {
        configured.set(pod.id as usize);
    }
    configured
}
