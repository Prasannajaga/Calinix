import json
from pathlib import Path


DATA_DIR = Path("benchmark/data")
SHORT_PAYLOAD_FILE = DATA_DIR / "short_payloads.json"
MIXED_PAYLOAD_FILE = DATA_DIR / "mixed_payloads.json"
HUGE_PAYLOAD_FILE = DATA_DIR / "huge_payloads.json"
DEFAULT_PAYLOAD_FILE = DATA_DIR / "example_payloads.json"

MODEL = "llama-3.1-8b"
MAX_TOKENS = 128


def word_count(text: str) -> int:
    return len(text.split())


def repeat_to_words(text: str, target_words: int) -> str:
    words = text.split()
    if not words:
        return ""

    repeats = target_words // len(words)
    remainder = target_words % len(words)
    expanded = words * repeats + words[:remainder]
    return " ".join(expanded)


def chat_payload(system: str, user: str) -> dict:
    return {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.2,
        "max_tokens": MAX_TOKENS,
        "stream": False,
    }


def scenario(prefix: str, question: str, target_words: int) -> str:
    prefix_words = max(1, target_words - word_count(question))
    return f"{repeat_to_words(prefix, prefix_words)} {question}".strip()


CACHE_ROUTING_PREFIX = (
    "Calinix cache aware routing benchmark notes. The gateway receives OpenAI chat "
    "completion requests, extracts the prompt text, tokenizes it into fixed size "
    "blocks, builds a cumulative prefix hash chain, and records which inference pod "
    "owns each cached prefix. When a new request arrives, the load balancer should "
    "prefer the pod with the longest matching prefix so prefill work is avoided. "
    "The experiment should expose throughput, latency, cache hit rate, prefix depth, "
    "and pod distribution under increasing concurrency."
)

CACHE_QUESTIONS = [
    "Explain why longer matching prefixes reduce prefill latency.",
    "Describe how the load balancer should choose a pod during a cache hit.",
    "List the metrics that prove cache aware routing is working.",
    "Identify failure modes when prefix ownership is stale.",
    "Summarize the benchmark outcome for an engineering report.",
]

DOCUMENT_PREFIX = (
    "Incident report archive. At 09:10 UTC request latency increased after a deployment "
    "changed routing weights. The first symptom was a rise in time to first token for "
    "long document questions. Cache hit headers were present but several requests landed "
    "on pods with shorter prefixes than expected. The team compared selected pod id, "
    "actual prefix depth, best prefix depth, and per pod request counts. The mitigation "
    "was to refresh routing metadata more frequently and temporarily reduce concurrency."
)

DOCUMENT_QUESTIONS = [
    "What was the earliest visible symptom?",
    "Which headers should the benchmark inspect?",
    "What mitigation reduced the incident impact?",
    "Write a concise postmortem action list.",
]

CODE_PREFIX = (
    "Rust service review context. The benchmark client creates a Tokio runtime, builds a "
    "reqwest client, spawns workers equal to the requested concurrency, cycles through a "
    "payload list, injects a user session id, sends POST requests to the load balancer, "
    "and records response headers. The CSV writer stores request status, latency, prompt "
    "tokens, cache prefix depth, best prefix depth, selected pod ids, and errors."
)

CODE_QUESTIONS = [
    "Review the timeout behavior and point out risks.",
    "Explain why request ids are assigned with an atomic counter.",
    "Suggest one test for CSV field correctness.",
    "Explain how sweep mode differs from request mode.",
]

COLD_PROMPTS = [
    (
        "You are a database tuning expert.",
        "Compare B-Tree and LSM-Tree indexes for write-heavy workloads.",
    ),
    (
        "You are a Rust tutor.",
        "Explain ownership, borrowing, and lifetimes using a small vector example.",
    ),
    (
        "You are a Kubernetes operator.",
        "Describe how readiness probes and horizontal pod autoscaling interact.",
    ),
    (
        "You are a distributed systems teacher.",
        "Explain quorum reads and writes with a concrete replica example.",
    ),
    (
        "You are an API designer.",
        "Design response headers for a load balancer that exposes routing decisions.",
    ),
]


def add_family(payloads: list[dict], system: str, prefix: str, questions: list[str], sizes: tuple[int, ...]) -> None:
    for target_words in sizes:
        for question in questions:
            payloads.append(chat_payload(system, scenario(prefix, question, target_words)))


def add_cold_prompts(payloads: list[dict]) -> None:
    for system, user in COLD_PROMPTS:
        payloads.append(chat_payload(system, user))


def short_payloads() -> list[dict]:
    payloads: list[dict] = []
    add_family(
        payloads,
        "You are a systems performance engineer.",
        CACHE_ROUTING_PREFIX,
        CACHE_QUESTIONS,
        (1000, 5000, 10000),
    )
    add_family(
        payloads,
        "You answer questions using the supplied incident report.",
        DOCUMENT_PREFIX,
        DOCUMENT_QUESTIONS,
        (1000, 5000, 10000),
    )
    add_family(
        payloads,
        "You are a careful Rust code reviewer.",
        CODE_PREFIX,
        CODE_QUESTIONS,
        (1000, 5000, 10000),
    )
    add_cold_prompts(payloads)
    return payloads


def mixed_payloads() -> list[dict]:
    payloads: list[dict] = []
    add_family(
        payloads,
        "You are a systems performance engineer.",
        CACHE_ROUTING_PREFIX,
        CACHE_QUESTIONS,
        (1000, 5000, 10000, 50000, 90000),
    )
    add_family(
        payloads,
        "You answer questions using the supplied incident report.",
        DOCUMENT_PREFIX,
        DOCUMENT_QUESTIONS,
        (1000, 5000, 10000, 50000, 90000),
    )
    add_family(
        payloads,
        "You are a careful Rust code reviewer.",
        CODE_PREFIX,
        CODE_QUESTIONS,
        (1000, 5000, 10000, 50000, 90000),
    )
    add_cold_prompts(payloads)
    return payloads


def huge_payloads() -> list[dict]:
    payloads: list[dict] = []
    add_family(
        payloads,
        "You are a systems performance engineer.",
        CACHE_ROUTING_PREFIX,
        CACHE_QUESTIONS[:4],
        (30000, 50000, 100000, 200000, 500000),
    )
    add_family(
        payloads,
        "You answer questions using the supplied incident report.",
        DOCUMENT_PREFIX,
        DOCUMENT_QUESTIONS[:2],
        (30000, 50000, 100000, 200000, 500000),
    )
    add_family(
        payloads,
        "You are a careful Rust code reviewer.",
        CODE_PREFIX,
        CODE_QUESTIONS[:2],
        (30000, 50000, 100000, 200000, 500000),
    )
    return payloads


def write_payload_file(path: Path, payloads: list[dict]) -> None:
    path.write_text(json.dumps(payloads, indent=2) + "\n")


def describe(name: str, path: Path, payloads: list[dict]) -> None:
    lengths = [word_count(payload["messages"][-1]["content"]) for payload in payloads]
    print(
        f"{name}: wrote {len(payloads)} payloads to {path} "
        f"(words min={min(lengths)} max={max(lengths)} unique={sorted(set(lengths))})"
    )


def main() -> None:
    datasets = [
        ("short", SHORT_PAYLOAD_FILE, short_payloads()),
        ("mixed", MIXED_PAYLOAD_FILE, mixed_payloads()),
        ("huge", HUGE_PAYLOAD_FILE, huge_payloads()),
    ]

    for name, path, payloads in datasets:
        write_payload_file(path, payloads)
        describe(name, path, payloads)

    write_payload_file(DEFAULT_PAYLOAD_FILE, datasets[1][2])
    print(f"default: refreshed {DEFAULT_PAYLOAD_FILE} from mixed payloads")


if __name__ == "__main__":
    main()
