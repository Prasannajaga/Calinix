use std::sync::Arc;
use std::time::Duration;

use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::Uri;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::HealthConfig;
use crate::upstream::{PodEndpoint, RuntimeRegistry};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HealthResult {
    Healthy,
    Unhealthy,
}

#[derive(Clone, Debug)]
struct PodHealthState {
    consecutive_successes: u8,
    consecutive_failures: u8,
    marked_alive: bool,
}

impl PodHealthState {
    fn new() -> Self {
        Self {
            consecutive_successes: 0,
            consecutive_failures: 0,
            marked_alive: false,
        }
    }

    fn observe(
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

            for pod in &registry.pod_table.pods {
                let result = if check_pod_health(&client, pod, &config.endpoint, timeout).await {
                    HealthResult::Healthy
                } else {
                    HealthResult::Unhealthy
                };

                let state = &mut states[pod.id as usize];
                if let Some(alive) = state.observe(result, healthy_threshold, unhealthy_threshold) {
                    if alive {
                        registry.cache_registry.mark_pod_alive(pod.id as usize);
                        info!(
                            pod_id = pod.id,
                            address = %pod.address,
                            "pod marked alive by health poller"
                        );
                    } else {
                        registry.cache_registry.shutdown_pod(pod.id as usize);
                        warn!(
                            pod_id = pod.id,
                            address = %pod.address,
                            "pod marked unhealthy by health poller"
                        );
                    }
                }
            }
        }
    })
}

type HealthClient = Client<HttpConnector, Empty<Bytes>>;

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

fn health_uri(url: &str, endpoint: &str) -> Result<Uri, String> {
    let (host, port, base_path) = parse_http_url(url)?;
    let request_path = join_paths(&base_path, endpoint);
    format!("http://{host}:{port}{request_path}")
        .parse::<Uri>()
        .map_err(|err| format!("invalid health URI for {url}: {err}"))
}

fn parse_http_url(url: &str) -> Result<(String, u16, String), String> {
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

fn join_paths(base_path: &str, endpoint: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::{health_uri, join_paths, parse_http_url, PodHealthState};
    use crate::upstream::health::HealthResult;

    #[test]
    fn joins_base_path_and_health_endpoint() {
        assert_eq!(join_paths("/", "/health"), "/health");
        assert_eq!(join_paths("/vllm", "/health"), "/vllm/health");
        assert_eq!(join_paths("/vllm/", "health"), "/vllm/health");
    }

    #[test]
    fn parses_http_url_with_default_port() {
        assert_eq!(
            parse_http_url("http://prefill-1").unwrap(),
            ("prefill-1".to_string(), 80, "/".to_string())
        );
        assert_eq!(
            parse_http_url("http://prefill-1:8000/api").unwrap(),
            ("prefill-1".to_string(), 8000, "/api".to_string())
        );
    }

    #[test]
    fn builds_health_uri_from_upstream_url_and_endpoint() {
        assert_eq!(
            health_uri("http://prefill-1:8000", "/health")
                .unwrap()
                .to_string(),
            "http://prefill-1:8000/health"
        );
        assert_eq!(
            health_uri("http://prefill-1:8000/vllm", "/health")
                .unwrap()
                .to_string(),
            "http://prefill-1:8000/vllm/health"
        );
    }

    #[test]
    fn thresholds_emit_only_on_state_changes() {
        let mut state = PodHealthState::new();
        assert_eq!(state.observe(HealthResult::Healthy, 2, 2), None);
        assert_eq!(state.observe(HealthResult::Healthy, 2, 2), Some(true));
        assert_eq!(state.observe(HealthResult::Healthy, 2, 2), None);
        assert_eq!(state.observe(HealthResult::Unhealthy, 2, 2), None);
        assert_eq!(state.observe(HealthResult::Unhealthy, 2, 2), Some(false));
    }
}
