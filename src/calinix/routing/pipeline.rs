use http::HeaderMap;

use crate::cache_registry::{BlockHash, HostBitmap};
use crate::protocol::routing_headers::{inject_routing_headers, CalinixMode};
use crate::routing::context::RoutingContext;
use crate::routing::filter::{FilterStage, RequiredRole, RoutePolicy};
use crate::routing::plan::RoutingPlan;
use crate::routing::prepare::{PrepareInput, PrepareStage};
use crate::routing::score::{ScoreStage, ScoreWeights};
use crate::routing::RoutingError;
use crate::session::StickyStore;
use crate::upstream::{LoadState, RuntimeRegistry, UpstreamCatalog};

const INLINE_CACHE_DEPTHS: usize = 32;
const MAX_CACHE_DEPTHS: usize = 256;

enum CacheDepthStorage {
    Inline {
        data: [usize; INLINE_CACHE_DEPTHS],
        len: usize,
    },
    Heap(Vec<usize>),
}

impl CacheDepthStorage {
    fn new(len: usize) -> Self {
        if len <= INLINE_CACHE_DEPTHS {
            Self::Inline {
                data: [0; INLINE_CACHE_DEPTHS],
                len,
            }
        } else {
            Self::Heap(vec![0; len])
        }
    }

    fn as_mut_slice(&mut self) -> &mut [usize] {
        match self {
            Self::Inline { data, len } => &mut data[..*len],
            Self::Heap(data) => data.as_mut_slice(),
        }
    }
}

#[derive(Clone)]
pub struct RoutedRequest {
    pub plan: RoutingPlan,
    pub forwarding_headers: HeaderMap,
    pub session_key: Option<String>,
    pub cumulative_hashes: Vec<BlockHash>,
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
                    cache: 0.60,
                    load: 0.30,
                    sticky: 0.10,
                    locality: 0.0,
                },
                prefill_weights: ScoreWeights {
                    cache: 0.65,
                    load: 0.25,
                    sticky: 0.10,
                    locality: 0.0,
                },
                decode_weights: ScoreWeights {
                    cache: 0.10,
                    load: 0.55,
                    sticky: 0.10,
                    locality: 0.25,
                },
            },
        }
    }
}

impl RoutingPipeline {
    pub fn route_openai_request(
        &self,
        registry: &RuntimeRegistry,
        loads: &LoadState,
        sticky: &StickyStore,
        path: &str,
        method: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<RoutedRequest, RoutingError> {
        // prepare stage
        let prepared = PrepareStage {
            default_mode: self.default_mode.clone(),
            block_size: self.block_size,
        }
        .run(PrepareInput {
            path,
            method,
            headers,
            body,
        })?;

        let ctx = prepared.ctx;
        let upstreams = &registry.upstreams;
        let available = available_pods(registry, self.route_policy.require_healthy);

        // single and disaggregated dispatch
        let plan = match ctx.mode {
            CalinixMode::Single => build_single_plan(
                registry,
                upstreams,
                &self.route_policy,
                loads,
                sticky,
                &self.score_stage,
                available,
                &ctx,
            )?,
            CalinixMode::Disaggregated => build_disaggregated_plan(
                registry,
                upstreams,
                &self.route_policy,
                loads,
                sticky,
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
            session_key: ctx.openai.session_key,
            cumulative_hashes: ctx.cumulative_hashes,
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
        filter.candidates_for_role(upstreams, loads, RequiredRole::Single, route_policy, alive);
    let mut cache_depths = CacheDepthStorage::new(registry.total_pods().min(MAX_CACHE_DEPTHS));
    let depths_slice = cache_depths.as_mut_slice();
    registry.cache_registry.longest_prefix_lengths_into(
        &ctx.cumulative_hashes,
        candidates.clone(),
        depths_slice,
    );
    let picked = score.best_candidate(
        ctx,
        RequiredRole::Single,
        candidates,
        depths_slice,
        upstreams,
        loads,
        sticky,
        None,
    )?;
    let pod_id = picked.pod_id;
    let pod = upstreams
        .pod(pod_id)
        .ok_or(RoutingError::MissingPod(pod_id))?;
    let cache_prefix_depth = picked.cache_prefix_depth;

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
        loads,
        RequiredRole::Prefill,
        route_policy,
        alive.clone(),
    );
    let mut prefill_cache_depths =
        CacheDepthStorage::new(registry.total_pods().min(MAX_CACHE_DEPTHS));
    let prefill_depths_slice = prefill_cache_depths.as_mut_slice();
    registry.cache_registry.longest_prefix_lengths_into(
        &ctx.cumulative_hashes,
        prefill_candidates.clone(),
        prefill_depths_slice,
    );
    let picked_prefill = score.best_candidate(
        ctx,
        RequiredRole::Prefill,
        prefill_candidates,
        prefill_depths_slice,
        upstreams,
        loads,
        sticky,
        None,
    )?;
    let prefill_pod_id = picked_prefill.pod_id;
    let prefill_pod = upstreams
        .pod(prefill_pod_id)
        .ok_or(RoutingError::MissingPod(prefill_pod_id))?;
    let cache_prefix_depth = picked_prefill.cache_prefix_depth;

    let decode_candidates =
        filter.candidates_for_role(upstreams, loads, RequiredRole::Decode, route_policy, alive);
    let mut decode_cache_depths =
        CacheDepthStorage::new(registry.total_pods().min(MAX_CACHE_DEPTHS));
    let decode_depths_slice = decode_cache_depths.as_mut_slice();
    registry.cache_registry.longest_prefix_lengths_into(
        &ctx.cumulative_hashes,
        decode_candidates.clone(),
        decode_depths_slice,
    );
    let picked_decode = score.best_candidate(
        ctx,
        RequiredRole::Decode,
        decode_candidates,
        decode_depths_slice,
        upstreams,
        loads,
        sticky,
        Some(prefill_pod_id),
    )?;
    let decode_pod_id = picked_decode.pod_id;

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

fn available_pods(registry: &RuntimeRegistry, require_healthy: bool) -> HostBitmap {
    if !require_healthy {
        return all_configured_pods(registry);
    }

    registry.cache_registry.alive()
}

fn all_configured_pods(registry: &RuntimeRegistry) -> HostBitmap {
    let mut configured = HostBitmap::empty();
    for pod in &registry.pod_table.pods {
        configured.set(pod.id as usize);
    }
    configured
}


