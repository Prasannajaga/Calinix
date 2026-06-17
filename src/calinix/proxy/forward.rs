use std::time::Duration;

use http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

type ProxyClient = Client<HttpConnector, Full<Bytes>>;

pub struct UpstreamResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

pub async fn forward_http_post(
    target_address: &str,
    path: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<UpstreamResponse, String> {
    let (host, port, request_path) = upstream_target(target_address, path)?;
    let uri = format!("http://{host}:{port}{request_path}")
        .parse()
        .map_err(|err| format!("invalid upstream URI for {target_address}: {err}"))?;
    let client: ProxyClient = Client::builder(TokioExecutor::new()).build_http();
    let request = build_post_request(uri, &host, port, headers, body)?;

    tokio::time::timeout(Duration::from_secs(10), async {
        let response = client
            .request(request)
            .await
            .map_err(|err| format!("upstream request to {target_address} failed: {err}"))?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|err| format!("failed reading upstream response body: {err}"))?
            .to_bytes()
            .to_vec();

        Ok(UpstreamResponse {
            status,
            headers,
            body,
        })
    })
    .await
    .map_err(|_| format!("upstream request to {target_address} timed out"))?
}

fn build_post_request(
    uri: hyper::Uri,
    host: &str,
    port: u16,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Request<Full<Bytes>>, String> {
    let mut request = Request::builder()
        .method(Method::POST)
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
