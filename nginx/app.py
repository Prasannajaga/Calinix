import os

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse


SERVICE_NAME = os.getenv("SERVICE_NAME", "user-service")

app = FastAPI(title=f"{SERVICE_NAME} API")


@app.get("/metrics", response_class=PlainTextResponse)
def metrics() -> str:
    return f"Metrics from the {SERVICE_NAME}"


@app.get("/metrics/{message}", response_class=PlainTextResponse)
def metrics_message(message: str) -> str:
    print(message, flush=True)
    return f"Metrics from the {SERVICE_NAME} {message}"
