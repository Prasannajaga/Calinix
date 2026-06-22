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

type ProxyHttpClient = Client<HttpConnector, Full<Bytes>>;

#[derive(Clone)]
pub struct HttpForwarder {
    client: Arc<ProxyHttpClient>,
    timeout: Duration,
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
        Self::with_timeout(DEFAULT_TIMEOUT)
    }

    pub fn with_policy(timeout: Duration, _max_attempts: usize) -> Self {
        Self::with_timeout(timeout)
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            client: Arc::new(Client::builder(TokioExecutor::new()).build_http()),
            timeout,
        }
    }

    pub async fn forward_with_fallback(
        &self,
        method: Method,
        plan: &RoutingPlan,
        _registry: &RuntimeRegistry,
        loads: &LoadState,
        path: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<UpstreamResponse, String> {
        let target = ForwardTarget {
            pod_id: plan.primary_pod_id(),
            address: plan.target_address().to_string(),
        };
        let attempt_result = tokio::time::timeout(
            self.timeout,
            self.forward_once(1, method, plan, &target, loads, path, headers, body),
        )
        .await;

        match attempt_result {
            Ok(Ok(upstream)) => {
                if is_retryable_status(upstream.status) {
                    tracing::warn!(
                        status = %upstream.status,
                        target = %target.address,
                        "upstream returned retryable status"
                    );
                }
                Ok(upstream)
            }
            Ok(Err(err)) => {
                tracing::warn!(%err, "upstream communication error");
                Err(err)
            }
            Err(_) => {
                let err_msg = format!("upstream request to {} timed out", target.address);
                tracing::warn!(%err_msg, "upstream timed out");
                Err(err_msg)
            }
        }
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

pub(crate) fn headers_for_attempt(
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


