use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[test]
#[ignore = "requires e2e/routing/docker-compose.yml services"]
fn routes_chat_completion_through_http_api_and_forwards_calinix_headers() {
    let base_url =
        env::var("CALINIX_E2E_URL").unwrap_or_else(|_| "http://127.0.0.1:18080".to_string());
    let body = r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":"route this through calinix"}],"stream":true}"#;

    let response = post_json(
        &base_url,
        "/v1/chat/completions",
        &[
            ("authorization", "Bearer user-token"),
            ("x-calinix-request-id", "req-e2e-routing-1"),
            ("x-calinix-mode", "disaggregated"),
            ("x-calinix-session-key", "session-e2e-routing"),
        ],
        body,
    );

    assert!(response.contains("\"service\":\"prefill-1\""), "{response}");
    assert!(
        response.contains("\"x-calinix-request-id\":\"req-e2e-routing-1\""),
        "{response}"
    );
    assert!(
        response.contains("\"x-calinix-mode\":\"disaggregated\""),
        "{response}"
    );
    assert!(
        response.contains("\"x-calinix-prefill-pod-id\":\"2\""),
        "{response}"
    );
    assert!(
        response.contains("\"x-calinix-decode-pod-id\":\"4\""),
        "{response}"
    );
    assert!(
        response.contains("\"authorization\":\"Bearer user-token\""),
        "{response}"
    );
}

fn post_json(base_url: &str, path: &str, headers: &[(&str, &str)], body: &str) -> String {
    let (host, port) = parse_base_url(base_url);
    let mut stream = TcpStream::connect((host.as_str(), port)).expect("connect to Calinix API");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .expect("set write timeout");

    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");

    stream
        .write_all(request.as_bytes())
        .and_then(|_| stream.write_all(body.as_bytes()))
        .expect("write request");

    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");

    assert!(
        response.starts_with("HTTP/1.1 200"),
        "expected 200 response, got:\n{response}"
    );
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .expect("response has body")
}

fn parse_base_url(base_url: &str) -> (String, u16) {
    let rest = base_url
        .strip_prefix("http://")
        .expect("CALINIX_E2E_URL must start with http://");
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = authority.split_once(':').unwrap_or((authority, "80"));
    (host.to_string(), port.parse().expect("valid port"))
}
