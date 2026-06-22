use std::sync::Arc;
use std::time::Duration;

use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::Uri;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{info, warn};

use crate::config::HealthConfig;
use crate::upstream::{PodEndpoint, PodId, RuntimeRegistry};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HealthResult {
    Healthy,
    Unhealthy,
}

#[derive(Clone, Debug)]
pub(crate) struct PodHealthState {
    consecutive_successes: u8,
    consecutive_failures: u8,
    marked_alive: bool,
}

impl PodHealthState {
    pub(crate) fn new() -> Self {
        Self {
            consecutive_successes: 0,
            consecutive_failures: 0,
            marked_alive: false,
        }
    }

    pub(crate) fn observe(
        &mut self,
        result: HealthResult,
        healthy_threshold: u8,
        unhealthy_threshold: u8,
    ) -> Option<bool> {
        match result {
            HealthResult::Healthy => {
                self.consecutive_successes = self.consecutive_successes.saturating_add(1);
                self.consecutive_failures = 0;
                if !self.marked_alive && self.consecutive_successes >= healthy_threshold {
                    self.marked_alive = true;
                    return Some(true);
                }
            }
            HealthResult::Unhealthy => {
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                self.consecutive_successes = 0;
                if self.marked_alive && self.consecutive_failures >= unhealthy_threshold {
                    self.marked_alive = false;
                    return Some(false);
                }
            }
        }

        None
    }
}

pub fn start_health_poller(registry: Arc<RuntimeRegistry>, config: HealthConfig) -> JoinHandle<()> {
    tokio::spawn(async move {
        let client: HealthClient = Client::builder(TokioExecutor::new()).build_http();
        let mut interval = tokio::time::interval(Duration::from_millis(config.interval_ms));
        let timeout = Duration::from_millis(config.timeout_ms);
        let healthy_threshold = config.healthy_threshold.max(1);
        let unhealthy_threshold = config.unhealthy_threshold.max(1);
        let mut states = vec![PodHealthState::new(); registry.total_pods()];

        loop {
            interval.tick().await;

            let mut checks = JoinSet::new();
            for pod in &registry.pod_table.pods {
                let client = client.clone();
                let pod = pod.clone();
                let endpoint = config.endpoint.clone();
                checks.spawn(async move {
                    let result = if check_pod_health(&client, &pod, &endpoint, timeout).await {
                        HealthResult::Healthy
                    } else {
                        HealthResult::Unhealthy
                    };
                    (pod.id, pod.address, result)
                });
            }

            while let Some(join_result) = checks.join_next().await {
                match join_result {
                    Ok((pod_id, address, result)) => apply_health_result(
                        registry.as_ref(),
                        &mut states,
                        pod_id,
                        address,
                        result,
                        healthy_threshold,
                        unhealthy_threshold,
                    ),
                    Err(err) => warn!(%err, "health check task failed"),
                }
            }
        }
    })
}

type HealthClient = Client<HttpConnector, Empty<Bytes>>;

pub(crate) fn apply_health_result(
    registry: &RuntimeRegistry,
    states: &mut [PodHealthState],
    pod_id: PodId,
    address: String,
    result: HealthResult,
    healthy_threshold: u8,
    unhealthy_threshold: u8,
) {
    let Some(state) = states.get_mut(pod_id as usize) else {
        warn!(pod_id, "health result for unknown pod");
        return;
    };

    if let Some(alive) = state.observe(result, healthy_threshold, unhealthy_threshold) {
        if alive {
            registry.cache_registry.mark_pod_alive(pod_id as usize);
            info!(
                pod_id,
                address = %address,
                "pod marked alive by health poller"
            );
        } else {
            registry.cache_registry.shutdown_pod(pod_id as usize);
            warn!(
                pod_id,
                address = %address,
                "pod marked unhealthy by health poller"
            );
        }
    }
}

async fn check_pod_health(
    client: &HealthClient,
    pod: &PodEndpoint,
    endpoint: &str,
    timeout: Duration,
) -> bool {
    get_health_status(client, &pod.address, endpoint, timeout)
        .await
        .map(|status| (200..300).contains(&status))
        .unwrap_or(false)
}

async fn get_health_status(
    client: &HealthClient,
    url: &str,
    endpoint: &str,
    timeout: Duration,
) -> Result<u16, String> {
    let uri = health_uri(url, endpoint)?;
    let response = tokio::time::timeout(timeout, client.get(uri))
        .await
        .map_err(|_| format!("health request to {url} timed out after {timeout:?}"))?
        .map_err(|err| format!("health request to {url} failed: {err}"))?;

    Ok(response.status().as_u16())
}

pub(crate) fn health_uri(url: &str, endpoint: &str) -> Result<Uri, String> {
    let (host, port, base_path) = parse_http_url(url)?;
    let request_path = join_paths(&base_path, endpoint);
    format!("http://{host}:{port}{request_path}")
        .parse::<Uri>()
        .map_err(|err| format!("invalid health URI for {url}: {err}"))
}

pub(crate) fn parse_http_url(url: &str) -> Result<(String, u16, String), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("only http:// upstream URLs are supported: {url}"))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = if let Some((host, port)) = authority.split_once(':') {
        let port = port
            .parse::<u16>()
            .map_err(|err| format!("invalid upstream port in {url}: {err}"))?;
        (host.to_string(), port)
    } else {
        (authority.to_string(), 80)
    };
    Ok((host, port, format!("/{path}")))
}

pub(crate) fn join_paths(base_path: &str, endpoint: &str) -> String {
    let base_path = normalize_path(base_path);
    let endpoint = normalize_path(endpoint);
    if base_path == "/" {
        endpoint
    } else if endpoint == "/" {
        base_path
    } else {
        format!("{}{}", base_path.trim_end_matches('/'), endpoint)
    }
}

fn normalize_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}


