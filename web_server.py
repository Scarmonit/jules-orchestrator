"""FastAPI web server for Jules Orchestrator.

Provides REST API endpoints for:
- Health checks
- Running demo orchestration
"""
import os
import random
import time
from typing import Any, Dict, List
from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel
import anyio
import structlog

logger = structlog.get_logger(__name__)

app = FastAPI(
    title="Jules Orchestrator API",
    description="Production-ready parallel task orchestrator service",
    version="1.0.0",
)

run_history: List[Dict[str, Any]] = []


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    service: str
    version: str


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
    """Run a demonstration with sample async tasks."""
    results = {}
    start = time.time()
    
    async def task_a():
        await anyio.sleep(random.uniform(0.05, 0.15))
        return {"task": "a", "value": random.randint(1, 100)}
    
    async def task_b():
        await anyio.sleep(random.uniform(0.05, 0.15))
        return {"task": "b", "value": random.randint(1, 100)}
    
    async def task_c(deps):
        await anyio.sleep(random.uniform(0.05, 0.15))
        total = sum(d["value"] for d in deps)
        return {"task": "c", "value": total, "deps": ["a", "b"]}
    
    # Run a and b in parallel
    async with anyio.create_task_group() as tg:
        async def run_a():
            results["a"] = await task_a()
        async def run_b():
            results["b"] = await task_b()
        tg.start_soon(run_a)
        tg.start_soon(run_b)
    
    # Run c after a and b complete
    results["c"] = await task_c([results["a"], results["b"]])
    
    run_record = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "type": "demo",
        "duration_ms": round((time.time() - start) * 1000, 2),
        "results": results,
    }
    run_history.append(run_record)
    
    return run_record


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
