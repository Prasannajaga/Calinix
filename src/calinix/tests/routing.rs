use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

fn e2e_base_url() -> String {
    env::var("CALINIX_E2E_URL").unwrap_or_else(|_| "http://localhost:18080".to_string())
}

#[test]
#[ignore = "requires e2e/routing/docker-compose.yml services"]
fn exposes_openai_compatible_routes_through_http_api() {
    let base_url = e2e_base_url();
    let headers = e2e_headers();

    let chat = post_json(
        &base_url,
        "/v1/chat/completions",
        &headers,
        r#"{"model":"gpt-4o-mini","messages":[{"role":"user","content":"route this through calinix"}],"stream":true}"#,
    );
    let completions = post_json(
        &base_url,
        "/v1/completions",
        &headers,
        r#"{"model":"gpt-4o-mini","prompt":"route this through calinix"}"#,
    );
    let embeddings = post_json(
        &base_url,
        "/v1/embeddings",
        &headers,
        r#"{"model":"text-embedding-3-small","input":"route this through calinix"}"#,
    );

    println!("chat_completions_response={chat}\n");
    println!("completions_response={completions} \n");
    println!("embeddings_response={embeddings} \n");
}

#[test]
#[ignore = "requires e2e/routing/docker-compose.yml services"]
fn handles_concurrent_disaggregated_register_events() {
    const REQUESTS: usize = 100;

    let base_url = e2e_base_url();
    let handles = (0..REQUESTS)
        .map(|request_number| {
            let base_url = base_url.clone();
            thread::spawn(move || {
                let body = format!(
                    r#"{{"model":"gpt-4o-mini","messages":[{{"role":"user","content":"route concurrent request {request_number}"}}],"stream":true}}"#
                );
                let response = post_json(
                    &base_url,
                    "/v1/chat/completions",
                    &e2e_headers(),
                    &body,
                );
                assert_registered_event(&response, request_number);
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().expect("concurrent e2e request panicked");
    }
}

fn e2e_headers() -> [(&'static str, &'static str); 5] {
    [
        ("authorization", "Bearer user-token"),
        ("x-calinix-request-id", "req-e2e-routing-1"),
        ("x-calinix-mode", "disaggregated"),
        ("x-calinix-session-key", "session-e2e-routing"),
        ("x-event", "register"),
    ]
}

fn assert_registered_event(response: &str, request_number: usize) {
    let json: serde_json::Value =
        serde_json::from_str(response).expect("mock pod response is valid JSON");
    assert_eq!(
        json["headers"]["x-event"], "register",
        "request {request_number} did not forward x-event header: {response}"
    );
    assert_eq!(
        json["events"][0]["type"], "prefixCached",
        "request {request_number} did not emit register event: {response}"
    );
    assert_eq!(
        json["events"][0]["result"]["status"], 200,
        "request {request_number} event callback failed: {response}"
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
