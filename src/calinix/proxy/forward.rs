use std::sync::Arc;
use std::time::Duration;

use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::protocol::routing_headers::{PREFILL_POD_ID, TARGET_POD_ID};
use crate::routing::plan::RoutingPlan;
use crate::upstream::{LoadState, PodId, RuntimeRegistry};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_ATTEMPTS: usize = 1;

type ProxyHttpClient = Client<HttpConnector, Full<Bytes>>;

#[derive(Clone)]
pub struct HttpForwarder {
    client: Arc<ProxyHttpClient>,
    timeout: Duration,
    max_attempts: usize,
}

pub struct UpstreamResponse {
    pub pod_id: PodId,
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ForwardTarget {
    pod_id: PodId,
    address: String,
}

impl Default for HttpForwarder {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpForwarder {
    pub fn new() -> Self {
        Self::with_policy(DEFAULT_TIMEOUT, DEFAULT_MAX_ATTEMPTS)
    }

    pub fn with_policy(timeout: Duration, max_attempts: usize) -> Self {
        Self {
            client: Arc::new(Client::builder(TokioExecutor::new()).build_http()),
            timeout,
            max_attempts: max_attempts.max(1),
        }
    }

    pub async fn forward_with_fallback(
        &self,
        method: Method,
        plan: &RoutingPlan,
        registry: &RuntimeRegistry,
        loads: &LoadState,
        path: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<UpstreamResponse, String> {
        let targets = fallback_targets(plan, registry);
        let mut errors = Vec::new();

        for (attempt, target) in targets.iter().take(self.max_attempts).enumerate() {
            let attempt_result = tokio::time::timeout(
                self.timeout,
                self.forward_once(
                    attempt + 1,
                    method.clone(),
                    plan,
                    target,
                    loads,
                    path,
                    headers,
                    body,
                ),
            )
            .await;

            match attempt_result {
                Ok(Ok(upstream)) => {
                    if is_retryable_status(upstream.status) {
                        let err_msg = format!(
                            "target {} returned status {}",
                            target.address, upstream.status
                        );
                        tracing::warn!(
                            %err_msg,
                            "upstream returned retryable status, falling back"
                        );
                        errors.push(err_msg);
                        continue;
                    }

                    return Ok(upstream);
                }
                Ok(Err(err)) => {
                    tracing::warn!(%err, "upstream communication error, falling back");
                    errors.push(err);
                }
                Err(_) => {
                    let err_msg = format!("upstream request to {} timed out", target.address);
                    tracing::warn!(%err_msg, "upstream timed out, falling back");
                    errors.push(err_msg);
                }
            }
        }

        Err(format!(
            "all upstream targets failed. Errors: [{}]",
            errors.join("; ")
        ))
    }

    async fn forward_once(
        &self,
        attempt: usize,
        method: Method,
        plan: &RoutingPlan,
        target: &ForwardTarget,
        loads: &LoadState,
        path: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<UpstreamResponse, String> {
        let _inflight = loads
            .track(target.pod_id)
            .ok_or_else(|| format!("unknown upstream pod id {}", target.pod_id))?;
        let (host, port, request_path) = upstream_target(&target.address, path)?;
        let uri = format!("http://{host}:{port}{request_path}")
            .parse()
            .map_err(|err| format!("invalid upstream URI for {}: {err}", target.address))?;
        let attempt_headers = headers_for_attempt(headers, plan, target.pod_id)?;
        let request = build_request(method.clone(), uri, &host, port, &attempt_headers, body)?;

        tracing::info!(
            attempt,
            method = %method,
            pod_id = target.pod_id,
            target = %target.address,
            path = %request_path,
            "forwarding request to upstream"
        );

        let response =
            self.client.request(request).await.map_err(|err| {
                format!("upstream connection failed for {}: {err}", target.address)
            })?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|err| {
                format!(
                    "failed reading response body from {}: {err}",
                    target.address
                )
            })?
            .to_bytes()
            .to_vec();

        Ok(UpstreamResponse {
            pod_id: target.pod_id,
            status,
            headers,
            body,
        })
    }
}

fn fallback_targets(plan: &RoutingPlan, registry: &RuntimeRegistry) -> Vec<ForwardTarget> {
    let (primary_pod_id, role_bitmap) = match plan {
        RoutingPlan::Single { target_pod_id, .. } => (*target_pod_id, &registry.single_pods),
        RoutingPlan::Disaggregated { prefill_pod_id, .. } => {
            (*prefill_pod_id, &registry.prefill_pods)
        }
    };

    let mut targets = vec![ForwardTarget {
        pod_id: primary_pod_id,
        address: plan.target_address().to_string(),
    }];
    let alive = registry.cache_registry.alive();
    let mut alive_fallbacks = Vec::new();
    let mut other_fallbacks = Vec::new();

    role_bitmap.for_each_set_bit(|id| {
        if id == primary_pod_id as usize {
            return;
        }

        if let Some(pod) = registry.pod_table.pods.get(id) {
            let target = ForwardTarget {
                pod_id: pod.id,
                address: pod.address.clone(),
            };
            if alive.contains(id) {
                alive_fallbacks.push(target);
            } else {
                other_fallbacks.push(target);
            }
        }
    });

    targets.extend(alive_fallbacks);
    targets.extend(other_fallbacks);
    targets
}

fn headers_for_attempt(
    headers: &HeaderMap,
    plan: &RoutingPlan,
    pod_id: PodId,
) -> Result<HeaderMap, String> {
    let mut headers = headers.clone();
    let header_name = match plan {
        RoutingPlan::Single { .. } => HeaderName::from_static(TARGET_POD_ID),
        RoutingPlan::Disaggregated { .. } => HeaderName::from_static(PREFILL_POD_ID),
    };
    let header_value = HeaderValue::from_str(&pod_id.to_string())
        .map_err(|err| format!("invalid fallback pod id header: {err}"))?;
    headers.insert(header_name, header_value);
    Ok(headers)
}

fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
}

fn build_request(
    method: Method,
    uri: hyper::Uri,
    host: &str,
    port: u16,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Request<Full<Bytes>>, String> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Full::from(Bytes::copy_from_slice(body)))
        .map_err(|err| format!("failed to build upstream request: {err}"))?;

    let request_headers = request.headers_mut();
    request_headers.insert(
        http::header::HOST,
        HeaderValue::from_str(&format!("{host}:{port}"))
            .map_err(|err| format!("invalid upstream host header: {err}"))?,
    );
    request_headers.insert(
        http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&body.len().to_string())
            .map_err(|err| format!("invalid content length header: {err}"))?,
    );
    request_headers.insert(http::header::CONNECTION, HeaderValue::from_static("close"));

    for (name, value) in headers {
        if name == http::header::HOST
            || name == http::header::CONTENT_LENGTH
            || name == http::header::CONNECTION
        {
            continue;
        }
        request_headers.insert(name.clone(), value.clone());
    }

    Ok(request)
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

fn upstream_target(target_address: &str, path: &str) -> Result<(String, u16, String), String> {
    let (host, port, base_path) = parse_http_url(target_address)?;
    let request_path = if base_path == "/" {
        normalize_path(path)
    } else {
        format!(
            "{}{}",
            base_path.trim_end_matches('/'),
            normalize_path(path)
        )
    };
    Ok((host, port, request_path))
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
    use http::{HeaderMap, HeaderValue};

    use super::headers_for_attempt;
    use crate::protocol::routing_headers::{DECODE_POD_ID, PREFILL_POD_ID, TARGET_POD_ID};
    use crate::routing::plan::RoutingPlan;

    #[test]
    fn single_fallback_rewrites_target_pod_header() {
        let mut headers = HeaderMap::new();
        headers.insert(TARGET_POD_ID, HeaderValue::from_static("1"));
        let plan = RoutingPlan::Single {
            request_id: "req".to_string(),
            target_pod_id: 1,
            target_address: "http://single-1:8000".to_string(),
            cache_hit: false,
            cache_prefix_depth: 0,
            route_policy: "default".to_string(),
        };

        let rewritten = headers_for_attempt(&headers, &plan, 7).unwrap();

        assert_eq!(rewritten.get(TARGET_POD_ID).unwrap(), "7");
    }

    #[test]
    fn disaggregated_fallback_rewrites_prefill_and_preserves_decode() {
        let mut headers = HeaderMap::new();
        headers.insert(PREFILL_POD_ID, HeaderValue::from_static("2"));
        headers.insert(DECODE_POD_ID, HeaderValue::from_static("5"));
        let plan = RoutingPlan::Disaggregated {
            request_id: "req".to_string(),
            coordinator_address: "http://prefill-1:8000".to_string(),
            prefill_pod_id: 2,
            decode_pod_id: 5,
            cache_hit: false,
            cache_prefix_depth: 0,
            route_policy: "default".to_string(),
        };

        let rewritten = headers_for_attempt(&headers, &plan, 3).unwrap();

        assert_eq!(rewritten.get(PREFILL_POD_ID).unwrap(), "3");
        assert_eq!(rewritten.get(DECODE_POD_ID).unwrap(), "5");
    }
}
