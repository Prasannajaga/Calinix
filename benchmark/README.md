# Calinix Benchmark CLI & Arguments Reference

This guide provides the direct CLI usage and argument documentation for the Calinix benchmarking suite.

---

## 1. Policy Benchmark (`calinix-policy-bench`)

Measures the load-balancer hot path without backend or network noise.

### Binary Usage
```bash
cargo run --bin calinix-policy-bench -- [options]
```

### Wrapper Script Usage
Runs the policy benchmark and generates plots automatically:
```bash
benchmark/run_policy.sh [options]
```
*(Pass `--no-plot` to skip plot generation).*

### CLI Options Reference
| Option | Description |
| :--- | :--- |
| `--name <string>` | Name of the benchmark run (determines output directory). |
| `--pods <int>` | Number of synthetic pods in the cluster. |
| `--shards <int>` | Number of independent index shards. |
| `--requests <int>` | Total number of requests to execute. |
| `--concurrency-sweep <list>` | Comma-separated list of concurrency levels to test (e.g., `1,8,32,64,128`). |
| `--block-size <int>` | Token block size (default: `32`). |
| `--prompt-blocks <int>` | Number of blocks per prompt. |
| `--shared-prefix-blocks <int>` | Number of blocks shared in the prefix context. |
| `--skew-percent <int>` | Percentage of traffic targeting the hot prefix (default: `90`). |
| `--write-ratio-percent <int>` | Percentage of write tasks relative to reads (default: `25`). |
| `--lag-sweep-ms <list>` | Comma-separated list of cache state update delays to test (e.g., `0,100,500,1000`). |
| `--qps <int>` | Target queries per second. |

---

## 2. Request & Sweep Benchmark (`calinix-url-bench`)

Measures the load balancer by sending actual requests to the HTTP gateway endpoint.

### Binary Usage
```bash
cargo run --bin calinix-url-bench -- [options]
```

### Wrapper Script Usage
Runs the benchmark across short, mixed, and huge payload datasets:
```bash
benchmark/run.sh [options]
```

### CLI Options Reference
| Option | Description |
| :--- | :--- |
| `--name <string>` | Name of the benchmark run. |
| `--url <string>` | The HTTP gateway endpoint URL (e.g., `http://127.0.0.1:18080/v1/chat/completions`). |
| `--concurrency <int>` | Fixed concurrency level for a single run. |
| `--concurrency-sweep <list>` | Comma-separated sweep levels (e.g., `1,10,50,100,200`). |
| `--threads <int>` | Number of Tokio worker threads for the client (default: `4`). |
| `--payload file <path>` | Path to payload JSON file (e.g., `benchmark/data/mixed_payloads.json`). |
| `--block-size <int>` | Token block size. |
| `--requests <int>` | Fixed request count. |
| `--timeout-ms <int>` | Duration timeout in milliseconds (runs in duration mode when requests are omitted). |
| `--output <string>` | Output CSV filename (e.g., `url_bench.csv`). |
| `--mode <single\|disaggregated>` | Forces Calinix mode header (`x-calinix-mode`). |

---

## 3. Runner Wrapper Options (`benchmark/run.sh`)

Controls the automation runner script that sweeps across multiple profiles.

### Wrapper Options Reference
| Option | Description |
| :--- | :--- |
| `--bench-mode <sweep\|single>` | Use concurrency sweep or run a single fixed concurrency. |
| `--concurrency <int>` | Fixed concurrency for `--bench-mode single`. |
| `--concurrency-sweep <list>` | Comma-separated concurrency sweep values. |
| `--requests <int>` | Fixed-request count (overrides duration mode). |
| `--timeout-ms <int>` | Duration-mode timeout in milliseconds (if requests are disabled). |
| `--run-prefix <string>` | Directory prefix name for the output results. |
| `--mode <single\|disaggregated>` | Calinix routing mode header. |
| `--url <string>` | Gateway endpoint URL. |
| `--threads <int>` | Client Tokio worker threads. |
| `--block-size <int>` | Cache block size. |
| `--interval-secs <int>` | Sleep time in seconds between profiles. |
| `--no-plot` | Skip plot generation. |
| `--reset-cmd <cmd>` | Shell command to run before each profile (e.g., to clear/restart the server). |
| `--reset-sleep-secs <int>` | Sleep time in seconds after executing the reset command. |

### Reset Hook usage
To benchmark cold-cache scenarios between payload profiles, pass the reset hook variables:
```bash
RESET_CMD='docker compose -f e2e/routing/docker-compose.yml restart' \
RESET_SLEEP_SECS=20 \
benchmark/run.sh
```

---

## 4. Plotting Tool (`plot_url_bench.py` & `plot_policy_bench.py`)

Visualizes the benchmark results from output CSV files.

### Request/Sweep Plots
```bash
python3 benchmark/plot_url_bench.py --input <path_to_csv>
```

### Policy Plots
```bash
python3 benchmark/plot_policy_bench.py --input <path_to_csv>
```
