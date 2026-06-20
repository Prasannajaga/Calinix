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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use http::{HeaderMap, HeaderValue};

    use super::{RoutedRequest, RoutingPipeline};
    use crate::cache_registry::{
        prompt_to_cumulative_hashes_with_block_size, CacheRegistry, HostBitmap,
    };
    use crate::protocol::routing_headers::{
        CalinixMode, DECODE_POD_ID, MODE, PREFILL_POD_ID, TARGET_POD_ID,
    };
    use crate::routing::plan::RoutingPlan;
    use crate::routing::RoutingError;
    use crate::session::StickyStore;
    use crate::upstream::{
        LoadState, PodEndpoint, PodId, PodRole, PodTable, RuntimeRegistry, UpstreamCatalog,
        UpstreamGroup,
    };

    #[test]
    fn single_route_filters_queries_scores_and_picks_by_cache_depth() {
        let registry = registry_with_roles(2, 0, 0);
        mark_alive(&registry, 0..2);

        let prompt = "alpha beta gamma delta";
        let hashes = prompt_to_cumulative_hashes_with_block_size(prompt, 2);
        registry.cache_registry.register_prefix(0, hashes[0]);
        registry.cache_registry.register_chain(1, &hashes);

        let routed = route_chat(&registry, HeaderMap::new(), prompt);

        match &routed.plan {
            RoutingPlan::Single {
                target_pod_id,
                cache_hit,
                cache_prefix_depth,
                ..
            } => {
                assert_eq!(*target_pod_id, 1);
                assert!(*cache_hit);
                assert_eq!(*cache_prefix_depth, hashes.len());
            }
            _ => panic!("expected single routing plan"),
        }
        assert_eq!(routed.forwarding_headers.get(TARGET_POD_ID).unwrap(), "1");
    }

    #[test]
    fn single_route_keeps_strong_cache_match_over_load() {
        let registry = registry_with_roles(2, 0, 0);
        mark_alive(&registry, 0..2);

        let prompt = "cache wins over sticky and load";
        let hashes = prompt_to_cumulative_hashes_with_block_size(prompt, 2);
        registry.cache_registry.register_chain(0, &hashes);

        let loads = LoadState::new(registry.total_pods());
        loads.set_inflight_for_test(0, 90);
        let sticky = StickyStore::new();
        sticky.remember("session-a", 1);

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-calinix-session-key",
            HeaderValue::from_static("session-a"),
        );
        let routed = pipeline().route_openai_request(
            &registry,
            &loads,
            &sticky,
            "/v1/chat/completions",
            "POST",
            &headers,
            chat_body(prompt).as_bytes(),
        );

        let routed = routed.expect("request routes");
        match &routed.plan {
            RoutingPlan::Single { target_pod_id, .. } => assert_eq!(*target_pod_id, 0),
            _ => panic!("expected single routing plan"),
        }
    }

    #[test]
    fn single_route_filters_cached_pod_when_over_capacity() {
        let registry = registry_with_roles(2, 0, 0);
        mark_alive(&registry, 0..2);

        let prompt = "cached pod cannot accept more traffic";
        let hashes = prompt_to_cumulative_hashes_with_block_size(prompt, 2);
        registry.cache_registry.register_chain(0, &hashes);

        let loads = LoadState::new(registry.total_pods());
        loads.set_inflight_for_test(0, 100);
        loads.set_inflight_for_test(1, 1);

        let routed = pipeline()
            .route_openai_request(
                &registry,
                &loads,
                &StickyStore::new(),
                "/v1/chat/completions",
                "POST",
                &HeaderMap::new(),
                chat_body(prompt).as_bytes(),
            )
            .expect("request routes");

        match &routed.plan {
            RoutingPlan::Single {
                target_pod_id,
                cache_hit,
                ..
            } => {
                assert_eq!(*target_pod_id, 1);
                assert!(!*cache_hit);
            }
            _ => panic!("expected single routing plan"),
        }
    }

    #[test]
    fn single_route_uses_load_when_cache_scores_tie() {
        let registry = registry_with_roles(2, 0, 0);
        mark_alive(&registry, 0..2);

        let loads = LoadState::new(registry.total_pods());
        loads.set_inflight_for_test(0, 10_000);

        let routed = pipeline()
            .route_openai_request(
                &registry,
                &loads,
                &StickyStore::new(),
                "/v1/chat/completions",
                "POST",
                &HeaderMap::new(),
                chat_body("cold prompt without cache").as_bytes(),
            )
            .expect("request routes");

        match &routed.plan {
            RoutingPlan::Single { target_pod_id, .. } => assert_eq!(*target_pod_id, 1),
            _ => panic!("expected single routing plan"),
        }
    }

    #[test]
    fn disaggregated_route_uses_prefix_query_score_and_pick_for_each_role() {
        let registry = registry_with_roles(0, 2, 2);
        mark_alive(&registry, 0..4);

        let prompt = "prefill and decode both use cache";
        let hashes = prompt_to_cumulative_hashes_with_block_size(prompt, 2);
        registry.cache_registry.register_prefix(0, hashes[0]);
        registry.cache_registry.register_chain(1, &hashes);
        registry.cache_registry.register_prefix(2, hashes[0]);
        registry.cache_registry.register_chain(3, &hashes);

        let mut headers = HeaderMap::new();
        headers.insert(MODE, HeaderValue::from_static("disaggregated"));
        let routed = route_chat(&registry, headers, prompt);

        match &routed.plan {
            RoutingPlan::Disaggregated {
                prefill_pod_id,
                decode_pod_id,
                cache_hit,
                cache_prefix_depth,
                ..
            } => {
                assert_eq!(*prefill_pod_id, 1);
                assert_eq!(*decode_pod_id, 3);
                assert!(*cache_hit);
                assert_eq!(*cache_prefix_depth, hashes.len());
            }
            _ => panic!("expected disaggregated routing plan"),
        }
        assert_eq!(routed.forwarding_headers.get(PREFILL_POD_ID).unwrap(), "1");
        assert_eq!(routed.forwarding_headers.get(DECODE_POD_ID).unwrap(), "3");
    }

    #[test]
    fn healthy_filter_excludes_pods_not_marked_alive() {
        let registry = registry_with_roles(1, 0, 0);
        let err = match pipeline().route_openai_request(
            &registry,
            &LoadState::new(registry.total_pods()),
            &StickyStore::new(),
            "/v1/chat/completions",
            "POST",
            &HeaderMap::new(),
            chat_body("no alive pods").as_bytes(),
        ) {
            Ok(_) => panic!("no alive pods should be filtered out"),
            Err(err) => err,
        };

        assert!(matches!(err, RoutingError::NoCandidates));
    }

    fn route_chat(registry: &RuntimeRegistry, headers: HeaderMap, prompt: &str) -> RoutedRequest {
        pipeline()
            .route_openai_request(
                registry,
                &LoadState::new(registry.total_pods()),
                &StickyStore::new(),
                "/v1/chat/completions",
                "POST",
                &headers,
                chat_body(prompt).as_bytes(),
            )
            .expect("request routes")
    }

    fn pipeline() -> RoutingPipeline {
        RoutingPipeline {
            default_mode: CalinixMode::Single,
            block_size: 2,
            ..RoutingPipeline::default()
        }
    }

    fn chat_body(prompt: &str) -> String {
        format!(r#"{{"model":"test-model","messages":[{{"role":"user","content":"{prompt}"}}]}}"#)
    }

    fn mark_alive(registry: &RuntimeRegistry, pod_ids: impl IntoIterator<Item = PodId>) {
        for pod_id in pod_ids {
            registry.cache_registry.mark_pod_alive(pod_id as usize);
        }
    }

    fn registry_with_roles(
        single_count: usize,
        prefill_count: usize,
        decode_count: usize,
    ) -> RuntimeRegistry {
        let mut pods = Vec::new();
        let mut by_external_id = HashMap::new();
        let mut single_pods = HostBitmap::empty();
        let mut prefill_pods = HostBitmap::empty();
        let mut decode_pods = HostBitmap::empty();

        push_role(
            "single",
            single_count,
            &mut pods,
            &mut by_external_id,
            &mut single_pods,
        );
        push_role(
            "prefill",
            prefill_count,
            &mut pods,
            &mut by_external_id,
            &mut prefill_pods,
        );
        push_role(
            "decode",
            decode_count,
            &mut pods,
            &mut by_external_id,
            &mut decode_pods,
        );

        RuntimeRegistry {
            pod_table: PodTable {
                pods: pods.clone(),
                by_external_id,
            },
            upstreams: UpstreamCatalog {
                pods,
                groups: vec![
                    upstream_group(1, "single", PodRole::Single, &single_pods),
                    upstream_group(2, "prefill", PodRole::Prefill, &prefill_pods),
                    upstream_group(3, "decode", PodRole::Decode, &decode_pods),
                ],
            },
            single_pods,
            prefill_pods,
            decode_pods,
            cache_registry: CacheRegistry::new_empty_alive(
                single_count + prefill_count + decode_count,
            ),
        }
    }

    fn push_role(
        prefix: &str,
        count: usize,
        pods: &mut Vec<PodEndpoint>,
        by_external_id: &mut HashMap<String, PodId>,
        role_bitmap: &mut HostBitmap,
    ) {
        for index in 0..count {
            let pod_id = pods.len() as PodId;
            let external_id = format!("{prefix}-{index}");
            by_external_id.insert(external_id.clone(), pod_id);
            role_bitmap.set(pod_id as usize);
            pods.push(PodEndpoint {
                id: pod_id,
                pod_id,
                address: format!("http://{external_id}:8000"),
                healthy: true,
                draining: false,
                max_conns: 100,
                capabilities: match prefix {
                    "single" => PodRole::Single.into(),
                    "prefill" => PodRole::Prefill.into(),
                    "decode" => PodRole::Decode.into(),
                    _ => unreachable!("test role prefix is known"),
                },
            });
        }
    }

    fn upstream_group(id: u16, name: &str, role: PodRole, pods: &HostBitmap) -> UpstreamGroup {
        UpstreamGroup {
            id,
            name: name.to_string(),
            role,
            pods: pods
                .iter_set_bits()
                .into_iter()
                .filter_map(|pod_id| PodId::try_from(pod_id).ok())
                .collect(),
            pod_bitmap: pods.clone(),
        }
    }
}
