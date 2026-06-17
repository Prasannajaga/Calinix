import json
import os
import urllib.error
import urllib.request
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse


SERVICE_NAME = os.getenv("SERVICE_NAME", "mock-pod")
POD_ID = os.getenv("POD_ID")
CALINIX_EVENT_URL = os.getenv(
    "CALINIX_EVENT_URL",
    os.getenv("CALINIX_EVENTS_URL", "http://calinix:8080/events"),
).rstrip("/")

app = FastAPI(title=f"{SERVICE_NAME} mock OpenAI pod")

next_hash = 1


@app.get("/health", response_class=PlainTextResponse)
async def health() -> str:
    return "ok" + SERVICE_NAME


@app.post("/v1/chat/completions")
async def chat_completions(request: Request) -> dict[str, Any]:
    return await echo_openai_request(request)


@app.post("/v1/completions")
async def completions(request: Request) -> dict[str, Any]:
    return await echo_openai_request(request)


@app.post("/v1/embeddings")
async def embeddings(request: Request) -> dict[str, Any]:
    return await echo_openai_request(request)


async def echo_openai_request(request: Request) -> dict[str, Any]:
    headers = {
        key.lower(): value
        for key, value in request.headers.items()
        if key.lower().startswith("x-calinix-")
        or key.lower() in {"authorization", "x-event"}
    }
    body = await request.json()
    emitted_events = emit_event_from_header(request.headers.get("x-event"))

    return {
        "service": SERVICE_NAME,
        "path": request.url.path,
        "headers": headers,
        "body": body,
        "events": emitted_events,
    }


def default_pod_id() -> int | str:
    if POD_ID is not None:
        return int(POD_ID)
    return SERVICE_NAME


def emit_event_from_header(event_name: str | None) -> list[dict[str, Any]]:
    if not event_name:
        return []

    normalized = event_name.lower()
    if normalized in {"register", "prefixcached", "prefix_cached"}:
        event = build_prefix_event("prefixCached")
    elif normalized in {"evict", "prefixevicted", "prefix_evicted"}:
        event = build_prefix_event("prefixEvicted")
    elif normalized in {"shutdown", "podshutdown", "pod_shutdown"}:
        event = build_shutdown_event()
    else:
        raise HTTPException(status_code=400, detail=f"unsupported mock event: {event_name}")

    event["result"] = post_event(event)
    return [event]


def build_prefix_event(event_type: str) -> dict[str, Any]:
    return {
        "type": event_type,
        "podId": default_pod_id(),
        "pod": SERVICE_NAME,
        "cumulativeHash": next_cumulative_hash(),
    }


def build_shutdown_event() -> dict[str, Any]:
    return {"type": "podShutdown", "podId": default_pod_id(), "pod": SERVICE_NAME}


def post_event(event: dict[str, Any]) -> dict[str, Any]:
    endpoint = event_endpoint(event)
    body = json.dumps(event).encode("utf-8")
    request = urllib.request.Request(
        f"{CALINIX_EVENT_URL}/{endpoint}",
        data=body,
        headers={"content-type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=2) as response:
            response_body = response.read().decode("utf-8")
            return {
                "status": response.status,
                "body": json.loads(response_body) if response_body else None,
            }
    except urllib.error.HTTPError as err:
        detail = err.read().decode("utf-8")
        raise HTTPException(
            status_code=502,
            detail=f"calinix event API rejected {event['type']}: {detail}",
        ) from err
    except urllib.error.URLError as err:
        raise HTTPException(
            status_code=502,
            detail=f"failed to call calinix event API at {CALINIX_EVENT_URL}: {err}",
        ) from err


def event_endpoint(event: dict[str, Any]) -> str:
    if event["type"] == "prefixCached":
        return "register"
    if event["type"] == "prefixEvicted":
        return "evict"
    if event["type"] == "podShutdown":
        return "shutdown"
    raise HTTPException(status_code=400, detail=f"unsupported mock event: {event['type']}")


def next_cumulative_hash() -> int:
    global next_hash
    value = next_hash
    next_hash += 1
    return value
