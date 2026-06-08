use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

pub fn quote_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

pub fn get_field(line: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    let start = line.find(&prefix)? + prefix.len();
    let bytes = line.as_bytes();
    if bytes.get(start) == Some(&b'"') {
        let mut out = String::new();
        let mut escaped = false;
        for ch in line[start + 1..].chars() {
            if escaped {
                out.push(ch);
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                return Some(out);
            } else {
                out.push(ch);
            }
        }
        None
    } else {
        let end = line[start..]
            .find(' ')
            .map(|offset| start + offset)
            .unwrap_or(line.len());
        Some(line[start..end].to_string())
    }
}

pub fn send_line(addr: &str, line: &str) -> Result<Vec<String>, String> {
    let mut stream =
        TcpStream::connect(addr).map_err(|err| format!("connect {addr} failed: {err}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|err| format!("set read timeout failed: {err}"))?;
    writeln!(stream, "{line}").map_err(|err| format!("write failed: {err}"))?;
    stream
        .flush()
        .map_err(|err| format!("flush failed: {err}"))?;

    let mut reader = BufReader::new(stream);
    let mut lines = Vec::new();
    loop {
        let mut response = String::new();
        let bytes = reader
            .read_line(&mut response)
            .map_err(|err| format!("read failed: {err}"))?;
        if bytes == 0 {
            break;
        }
        let trimmed = response.trim_end().to_string();
        let done = trimmed.starts_with("DONE")
            || trimmed.starts_with("SINGLE_OK")
            || trimmed.starts_with("PREFILL_OK")
            || trimmed.starts_with("ERROR")
            || trimmed.starts_with("OK")
            || trimmed.starts_with("ROUTER_OK")
            || trimmed.starts_with("ROUTER_ERROR");
        lines.push(trimmed);
        if done {
            break;
        }
    }
    Ok(lines)
}
