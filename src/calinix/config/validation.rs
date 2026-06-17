use std::collections::HashSet;

use super::schema::{CalinixConfig, Strategy, UpstreamMode};

const MAX_CONFIG_PODS: usize = 256;

pub fn validate_config(config: &CalinixConfig) -> Result<(), String> {
    if config.gateway.port == 0 {
        return Err("gateway.port must be non-zero".to_string());
    }

    if config.gateway.strategy != Strategy::CacheAware {
        return Err("gateway.strategy must be cacheAware".to_string());
    }

    if config.cache_registry.max_pods == 0 {
        return Err("cacheRegistry.maxPods must be non-zero".to_string());
    }

    if config.cache_registry.shards_count == 0 {
        return Err("cacheRegistry.shardsCount must be non-zero".to_string());
    }

    if config.health.endpoint.trim().is_empty() {
        return Err("health.endpoint must be non-empty".to_string());
    }

    if config.health.interval_ms == 0 {
        return Err("health.intervalMs must be non-zero".to_string());
    }

    if config.health.timeout_ms == 0 {
        return Err("health.timeoutMs must be non-zero".to_string());
    }

    if config.health.healthy_threshold == 0 {
        return Err("health.healthyThreshold must be non-zero".to_string());
    }

    if config.health.unhealthy_threshold == 0 {
        return Err("health.unhealthyThreshold must be non-zero".to_string());
    }

    if config.cache_registry.max_pods > MAX_CONFIG_PODS {
        return Err(format!(
            "cacheRegistry.maxPods must be <= {MAX_CONFIG_PODS}"
        ));
    }

    if config.upstreams.single.mode != UpstreamMode::Single {
        return Err("upstreams.single.mode must be single".to_string());
    }

    if config.upstreams.dispatch.mode != UpstreamMode::Dispatch {
        return Err("upstreams.dispatch.mode must be dispatch".to_string());
    }

    if config.upstreams.single.pods.is_empty() {
        return Err("single mode must have at least one pod".to_string());
    }

    if config.upstreams.dispatch.prefill.pods.is_empty() {
        return Err("dispatch mode must have at least one prefill pod".to_string());
    }

    if config.upstreams.dispatch.decode.pods.is_empty() {
        return Err("dispatch mode must have at least one decode pod".to_string());
    }

    let mut pod_ids = HashSet::new();
    let mut total_pods = 0;
    for pod in config
        .upstreams
        .single
        .pods
        .iter()
        .chain(config.upstreams.dispatch.prefill.pods.iter())
        .chain(config.upstreams.dispatch.decode.pods.iter())
    {
        total_pods += 1;

        if pod.id.trim().is_empty() {
            return Err("all pod IDs must be non-empty".to_string());
        }

        if !pod_ids.insert(pod.id.as_str()) {
            return Err(format!("pod ID '{}' is duplicated", pod.id));
        }

        if pod.url.trim().is_empty() {
            return Err(format!("pod '{}' must have a non-empty URL", pod.id));
        }
    }

    if total_pods > config.cache_registry.max_pods {
        return Err(format!(
            "configured pod count {total_pods} exceeds cacheRegistry.maxPods {}",
            config.cache_registry.max_pods
        ));
    }

    if total_pods > u16::MAX as usize {
        return Err("configured pod count exceeds u16 PodId capacity".to_string());
    }

    Ok(())
}
