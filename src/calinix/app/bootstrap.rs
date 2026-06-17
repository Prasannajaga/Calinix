use std::env;
use std::net::SocketAddr;
use std::time::Instant;

use axum::body::{to_bytes, Body, Bytes};
use axum::extract::{Extension, State};
use axum::http::{Method, Request, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{info, Level};

use crate::app::state::{AppState, RegistrySummary};
use crate::cache_registry::BlockHash;
use crate::config::{load_config, CalinixConfig};
use crate::routing::pipeline::{RoutedRequest, RoutingPipeline};
use crate::upstream::{start_health_poller, PodId, RuntimeRegistry};

const DEFAULT_CONFIG_PATH: &str = "./config.yaml";

pub async fn run_from_cli() -> Result<(), String> {
    init_tracing();

    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_CONFIG_PATH.to_string());

    let config = load_config(&config_path)?;
    run(config).await
}

pub async fn run(config: CalinixConfig) -> Result<(), String> {
    let registry: RuntimeRegistry = RuntimeRegistry::from_config(&config)?;
    log_registry_startup(&registry);

    let port = config.gateway.port;
    let health_endpoint = config.health.endpoint.clone();
    let state = AppState::new(registry);
    let _health_poller = start_health_poller(state.registry.clone(), config.health.clone());
    let app = router(state, &health_endpoint);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| format!("failed to bind {addr}: {err}"))?;

    info!(%addr, "calinix gateway listening");
    axum::serve(listener, app)
        .await
        .map_err(|err| format!("gateway server failed: {err}"))
}

fn router(state: AppState, health_endpoint: &str) -> Router {
    let proxy_routes = Router::new()
        .route("/*path", any(openai_compatible_endpoint))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            route_openai_request,
        ));

    let mut app = Router::new()
        .route("/registry", get(debug_registry))
        .route("/events/register", post(register_event))
        .route("/events/evict", post(evict_event))
        .route("/events/shutdown", post(shutdown_event))
        .merge(proxy_routes);

    app = app.route(normalize_route_path(health_endpoint).as_str(), get(health));
    if health_endpoint != "/health" {
        app = app.route("/health", get(health));
    }

    app.layer(middleware::from_fn(log_request))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    "ok"
}

async fn debug_registry(State(state): State<AppState>) -> Json<RegistrySummary> {
    Json(RegistrySummary::from(state.registry.as_ref()))
}

async fn register_event(
    State(state): State<AppState>,
    Json(payload): Json<CacheEventRequest>,
) -> Response {
    let pod_id = match resolve_event_pod_id(state.registry.as_ref(), &payload) {
        Ok(pod_id) => pod_id,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let Some(cumulative_hash) = payload.cumulative_hash else {
        return (
            StatusCode::BAD_REQUEST,
            "register event requires cumulativeHash",
        )
            .into_response();
    };

    state
        .registry
        .cache_registry
        .mark_pod_alive(pod_id as usize);
    let registered = state
        .registry
        .cache_registry
        .register_prefix(pod_id as usize, cumulative_hash);
    if registered {
        info!(pod_id, cumulative_hash, "cache prefix registered");
    } else {
        tracing::debug!(
            pod_id,
            cumulative_hash,
            "cache prefix register skipped; already present"
        );
    }

    Json(CacheEventResponse::registered(pod_id, cumulative_hash)).into_response()
}

async fn evict_event(
    State(state): State<AppState>,
    Json(payload): Json<CacheEventRequest>,
) -> Response {
    let pod_id = match resolve_event_pod_id(state.registry.as_ref(), &payload) {
        Ok(pod_id) => pod_id,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let Some(cumulative_hash) = payload.cumulative_hash else {
        return (
            StatusCode::BAD_REQUEST,
            "evict event requires cumulativeHash",
        )
            .into_response();
    };

    state
        .registry
        .cache_registry
        .evict_prefix(pod_id as usize, cumulative_hash);
    info!(pod_id, cumulative_hash, "cache prefix evicted");

    Json(CacheEventResponse::evicted(pod_id, cumulative_hash)).into_response()
}

async fn shutdown_event(
    State(state): State<AppState>,
    Json(payload): Json<CacheEventRequest>,
) -> Response {
    let pod_id = match resolve_event_pod_id(state.registry.as_ref(), &payload) {
        Ok(pod_id) => pod_id,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    state.registry.cache_registry.shutdown_pod(pod_id as usize);
    info!(pod_id, "cache pod shutdown");

    Json(CacheEventResponse::shutdown(pod_id)).into_response()
}

async fn log_request(req: Request<Body>, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let started = Instant::now();

    let response = next.run(req).await;
    let status = response.status();
    let elapsed_ms = started.elapsed().as_millis();

    info!(
        %method,
        %path,
        status = status.as_u16(),
        elapsed_ms,
        "request completed"
    );

    response
}

async fn route_openai_request(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let (mut parts, body) = req.into_parts();
    let body = match to_bytes(body, usize::MAX).await {
        Ok(body) => body,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("failed to read request body: {err}"),
            )
                .into_response();
        }
    };

    match RoutingPipeline::default().route_openai_request(
        state.registry.as_ref(),
        state.loads.as_ref(),
        state.sticky.as_ref(),
        parts.uri.path(),
        parts.method.as_str(),
        &parts.headers,
        &body,
    ) {
        Ok(routed) => {
            parts.extensions.insert(routed);
            next.run(Request::from_parts(parts, Body::from(body))).await
        }
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}

async fn openai_compatible_endpoint(
    State(state): State<AppState>,
    Extension(routed): Extension<RoutedRequest>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let path = uri
        .path_and_query()
        .map(|path_and_query| path_and_query.as_str())
        .unwrap_or_else(|| uri.path())
        .to_string();
    let headers = routed.forwarding_headers.clone();
    let body = body.to_vec();

    let upstream = state
        .forwarder
        .forward_with_fallback(
            method,
            &routed.plan,
            state.registry.as_ref(),
            state.loads.as_ref(),
            &path,
            &headers,
            &body,
        )
        .await;

    match upstream {
        Ok(upstream) => {
            if upstream.status.is_success() {
                state
                    .registry
                    .cache_registry
                    .mark_pod_alive(upstream.pod_id as usize);
                state
                    .registry
                    .cache_registry
                    .register_chain(upstream.pod_id as usize, &routed.cumulative_hashes);
                if let Some(session_key) = routed.session_key {
                    state.sticky.remember(session_key, upstream.pod_id);
                }
            }
            let mut response = Response::builder().status(upstream.status);
            for (name, value) in &upstream.headers {
                if is_hop_by_hop_response_header(name) {
                    continue;
                }
                response = response.header(name, value);
            }
            response
                .body(upstream.body.into())
                .unwrap_or_else(|err| (StatusCode::BAD_GATEWAY, err.to_string()).into_response())
        }
        Err(err) => (StatusCode::BAD_GATEWAY, err).into_response(),
    }
}

fn is_hop_by_hop_response_header(name: &http::HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
    )
}

fn log_registry_startup(registry: &RuntimeRegistry) {
    for pod in &registry.pod_table.pods {
        info!(
            pod_id = pod.pod_id,
            address = %pod.address,
            "loaded upstream pod"
        );
    }

    info!(
        total_pods = registry.total_pods(),
        single_pods = registry.single_pods.count(),
        prefill_pods = registry.prefill_pods.count(),
        decode_pods = registry.decode_pods.count(),
        alive_pods = registry.cache_registry.alive().count(),
        "runtime registry initialized"
    );
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();
}

fn normalize_route_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CacheEventRequest {
    #[serde(default, alias = "pod_id")]
    pod_id: Option<PodRef>,
    #[serde(default)]
    pod: Option<String>,
    #[serde(default, alias = "cumulative_hash")]
    cumulative_hash: Option<BlockHash>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PodRef {
    Id(PodId),
    ExternalId(String),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CacheEventResponse {
    event: &'static str,
    pod_id: PodId,
    cumulative_hash: Option<BlockHash>,
}

impl CacheEventResponse {
    fn registered(pod_id: PodId, cumulative_hash: BlockHash) -> Self {
        Self {
            event: "prefixCached",
            pod_id,
            cumulative_hash: Some(cumulative_hash),
        }
    }

    fn evicted(pod_id: PodId, cumulative_hash: BlockHash) -> Self {
        Self {
            event: "prefixEvicted",
            pod_id,
            cumulative_hash: Some(cumulative_hash),
        }
    }

    fn shutdown(pod_id: PodId) -> Self {
        Self {
            event: "podShutdown",
            pod_id,
            cumulative_hash: None,
        }
    }
}

fn resolve_event_pod_id(
    registry: &RuntimeRegistry,
    payload: &CacheEventRequest,
) -> Result<PodId, String> {
    match (&payload.pod_id, &payload.pod) {
        (Some(PodRef::Id(pod_id)), _) => Ok(*pod_id),
        (Some(PodRef::ExternalId(external_id)), _) | (None, Some(external_id)) => registry
            .pod_table
            .by_external_id
            .get(external_id)
            .copied()
            .ok_or_else(|| format!("unknown pod external id: {external_id}")),
        (None, None) => Err("event requires podId or pod".to_string()),
    }
}
