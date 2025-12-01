"""FastAPI web server wrapper for Jules Orchestrator.

Provides REST API endpoints for:
- Health checks
- Running orchestration tasks
- Viewing task status
"""
import os
from typing import Any, Dict, List, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import structlog

from jules_orchestrator import (
    JulesOrchestrator,
    OrchestratorConfig,
    TaskConfig,
    TaskResult,
    ErrorPolicy,
)

logger = structlog.get_logger(__name__)

app = FastAPI(
    title="Jules Orchestrator API",
    description="Production-ready parallel task orchestrator service",
    version="1.0.0",
)

# In-memory store for demonstration
run_history: List[Dict[str, Any]] = []


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    service: str
    version: str


class TaskDefinition(BaseModel):
    id: str
    deps: List[str] = []
    timeout_s: Optional[float] = None
    retry_attempts: int = 0
    concurrency_label: Optional[str] = None


class RunRequest(BaseModel):
    tasks: List[TaskDefinition]
    initial_context: Dict[str, Any] = {}
    max_concurrency: int = 4
    error_policy: str = "fail_fast"


@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint for load balancers and monitoring."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat() + "Z",
        service="jules-orchestrator",
        version="1.0.0",
    )


@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "service": "jules-orchestrator",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/v1/health",
    }


@app.get("/api/v1/runs")
async def list_runs():
    """List recent orchestration runs."""
    return {"runs": run_history[-10:]}


@app.post("/api/v1/demo")
async def run_demo():
    """Run a demonstration DAG with sample tasks."""
    import anyio
    import random

    async def sample_work(ctx: Dict[str, Any]) -> int:
        await anyio.sleep(random.uniform(0.05, 0.2))
        return sum(1 for _ in ctx)

    tasks = [
        TaskConfig(id="a", fn=sample_work),
        TaskConfig(id="b", fn=sample_work),
        TaskConfig(id="c", fn=sample_work, deps=["a", "b"], retry_attempts=3, timeout_s=2.0),
    ]

    cfg = OrchestratorConfig(max_concurrency=4, error_policy=ErrorPolicy.CONTINUE)
    orch = JulesOrchestrator(cfg)

    results = await orch.run(tasks, initial_ctx={"seed": 1})
    
    run_record = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "type": "demo",
        "results": {
            k: {"success": v.success, "result": v.result, "error": v.error}
            for k, v in results.items()
        },
    }
    run_history.append(run_record)

    return run_record


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
