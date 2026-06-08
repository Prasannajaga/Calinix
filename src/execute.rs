use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::protocol::{get_field, quote_value, send_line};
use crate::types::{FailurePolicy, Pod, PodId, RoutingPlan, StepRole};

#[derive(Clone, Debug)]
pub struct ExecutionContext {
    pub request_id: u64,
    pub session_id: String,
    pub prompt: String,
    pub cache_transfer_id: Option<String>,
    pub last_prefill_pod: Option<PodId>,
}

struct InflightGuard<'a> {
    counter: &'a AtomicUsize,
}

impl<'a> InflightGuard<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter }
    }
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

pub fn execute_plan(
    plan: RoutingPlan,
    mut ctx: ExecutionContext,
    pods: Arc<Vec<Pod>>,
    inflight: Arc<[AtomicUsize; 256]>,
) -> Result<String, String> {
    for step in plan.steps {
        let pod = pods
            .iter()
            .find(|pod| pod.id == step.pod_id)
            .ok_or_else(|| format!("pod {} not found", step.pod_id))?;
        let _guard = InflightGuard::new(&inflight[pod.id]);
        let result = match step.role {
            StepRole::Single => dispatch_single(pod, &ctx),
            StepRole::Prefill => {
                let cache_transfer_id = dispatch_prefill(pod, &ctx)?;
                ctx.cache_transfer_id = Some(cache_transfer_id);
                ctx.last_prefill_pod = Some(pod.id);
                Ok(String::new())
            }
            StepRole::Decode => dispatch_decode(pod, &ctx),
        };

        match result {
            Ok(response) if step.role == StepRole::Single || step.role == StepRole::Decode => {
                return Ok(response);
            }
            Ok(_) => {}
            Err(err) => match step.failure_policy {
                FailurePolicy::FailFast => return Err(err),
                FailurePolicy::RetryNextBest => {
                    return Err(format!("RetryNextBest not implemented yet: {err}"));
                }
            },
        }
    }

    Err("routing plan completed without a response step".to_string())
}

fn dispatch_single(pod: &Pod, ctx: &ExecutionContext) -> Result<String, String> {
    let line = format!(
        "SINGLE request_id={} session={} prompt=\"{}\"",
        ctx.request_id,
        ctx.session_id,
        quote_value(&ctx.prompt)
    );
    let lines = send_line(&pod.addr, &line)?;
    let first = lines
        .first()
        .ok_or_else(|| "empty single response".to_string())?;
    if first.starts_with("SINGLE_OK") {
        Ok(first.clone())
    } else {
        Err(first.clone())
    }
}

fn dispatch_prefill(pod: &Pod, ctx: &ExecutionContext) -> Result<String, String> {
    let line = format!(
        "PREFILL request_id={} session={} prompt=\"{}\"",
        ctx.request_id,
        ctx.session_id,
        quote_value(&ctx.prompt)
    );
    let lines = send_line(&pod.addr, &line)?;
    let first = lines
        .first()
        .ok_or_else(|| "empty prefill response".to_string())?;
    if first.starts_with("PREFILL_OK") {
        get_field(first, "cache_transfer_id")
            .ok_or_else(|| format!("prefill response missing cache_transfer_id: {first}"))
    } else {
        Err(first.clone())
    }
}

fn dispatch_decode(pod: &Pod, ctx: &ExecutionContext) -> Result<String, String> {
    let cache_transfer_id = ctx
        .cache_transfer_id
        .clone()
        .ok_or_else(|| "decode step missing cache_transfer_id".to_string())?;
    let line = format!(
        "DECODE request_id={} session={} cache_transfer_id={} prompt=\"{}\"",
        ctx.request_id,
        ctx.session_id,
        cache_transfer_id,
        quote_value(&ctx.prompt)
    );
    let lines = send_line(&pod.addr, &line)?;
    if lines.iter().any(|line| line.starts_with("ERROR")) {
        return Err(lines.join(" | "));
    }
    if lines.iter().any(|line| line.starts_with("DONE")) {
        Ok(lines.join(" "))
    } else {
        Err(format!(
            "decode response missing DONE: {}",
            lines.join(" | ")
        ))
    }
}
