use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use crate::protocol::get_field;
use crate::types::{Pod, PodRole};

pub fn run_mock_pod_server(pod_id: usize, role: PodRole, port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    println!("mock pod {pod_id} role={role:?} listening on 127.0.0.1:{port}");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let role = role.clone();
                thread::spawn(move || handle_connection(stream, pod_id, role));
            }
            Err(err) => eprintln!("mock pod {pod_id} accept error: {err}"),
        }
    }
    Ok(())
}

pub fn spawn_mock_pods(pods: &[Pod]) {
    for pod in pods.iter().cloned() {
        let port = pod
            .addr
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u16>().ok())
            .expect("static pod addr must include a port");
        thread::spawn(move || {
            if let Err(err) = run_mock_pod_server(pod.id, pod.role, port) {
                eprintln!("mock pod {} failed: {err}", pod.id);
            }
        });
    }
    thread::sleep(Duration::from_millis(50));
}

fn handle_connection(mut stream: std::net::TcpStream, pod_id: usize, _role: PodRole) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        return;
    }
    let line = line.trim_end().to_string();
    let prompt = get_field(&line, "prompt").unwrap_or_default();
    let request_id = get_field(&line, "request_id").unwrap_or_else(|| "0".to_string());

    if prompt.contains("SLOW") {
        thread::sleep(Duration::from_millis(120));
    }

    if line.starts_with("SINGLE ") {
        thread::sleep(Duration::from_millis(20));
        let _ = writeln!(
            stream,
            "SINGLE_OK pod={} response=\"mock response from pod {}\"",
            pod_id, pod_id
        );
    } else if line.starts_with("PREFILL ") {
        thread::sleep(Duration::from_millis(30));
        if prompt.contains("FAIL_PREFILL") {
            let _ = writeln!(stream, "ERROR message=\"prefill failed\"");
        } else {
            let _ = writeln!(
                stream,
                "PREFILL_OK pod={} cache_transfer_id={}-{}",
                pod_id, request_id, pod_id
            );
        }
    } else if line.starts_with("DECODE ") {
        thread::sleep(Duration::from_millis(10));
        if prompt.contains("FAIL_DECODE") {
            let _ = writeln!(stream, "ERROR message=\"decode failed\"");
        } else {
            let _ = writeln!(stream, "TOKEN pod={} text=\"hello\"", pod_id);
            let _ = writeln!(stream, "TOKEN pod={} text=\"world\"", pod_id);
            let _ = writeln!(stream, "DONE pod={}", pod_id);
        }
    } else {
        let _ = writeln!(stream, "ERROR message=\"unknown command\"");
    }
}
