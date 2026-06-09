#[derive(Clone, Debug)]
pub struct LatencyStats {
    pub count: usize,
    pub min_ns: u128,
    pub max_ns: u128,
    pub avg_ns: u128,
    pub p50_ns: u128,
    pub p95_ns: u128,
    pub p99_ns: u128,
}

pub fn summarize_ns(samples: &mut [u128]) -> LatencyStats {
    if samples.is_empty() {
        return LatencyStats {
            count: 0,
            min_ns: 0,
            max_ns: 0,
            avg_ns: 0,
            p50_ns: 0,
            p95_ns: 0,
            p99_ns: 0,
        };
    }

    samples.sort_unstable();
    let sum = samples.iter().sum::<u128>();
    LatencyStats {
        count: samples.len(),
        min_ns: samples[0],
        max_ns: samples[samples.len() - 1],
        avg_ns: sum / samples.len() as u128,
        p50_ns: percentile_ns(samples, 50),
        p95_ns: percentile_ns(samples, 95),
        p99_ns: percentile_ns(samples, 99),
    }
}

#[allow(dead_code)]
pub fn print_ns_line(name: &str, stats: &LatencyStats) {
    println!(
        "{name:<32} count={:<10} min={:<10} avg={:<10} p50={:<10} p95={:<10} p99={:<10} max={:<10} ns",
        stats.count,
        stats.min_ns,
        stats.avg_ns,
        stats.p50_ns,
        stats.p95_ns,
        stats.p99_ns,
        stats.max_ns
    );
}

#[allow(dead_code)]
pub fn print_us_line(name: &str, stats: &LatencyStats) {
    println!(
        "{name:<32} count={:<10} min={:<10.3} avg={:<10.3} p50={:<10.3} p95={:<10.3} p99={:<10.3} max={:<10.3} us",
        stats.count,
        ns_to_us(stats.min_ns),
        ns_to_us(stats.avg_ns),
        ns_to_us(stats.p50_ns),
        ns_to_us(stats.p95_ns),
        ns_to_us(stats.p99_ns),
        ns_to_us(stats.max_ns)
    );
}

fn percentile_ns(samples: &[u128], percentile: usize) -> u128 {
    let index = (samples.len() * percentile / 100).min(samples.len() - 1);
    samples[index]
}

fn ns_to_us(ns: u128) -> f64 {
    ns as f64 / 1_000.0
}
