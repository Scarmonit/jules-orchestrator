# jules-orchestrator

A production-ready parallel execution orchestrator for Jules workflows.

Features:
- Async DAG execution with topological scheduling
- Per-task timeouts, retries with jitter, and cancellation
- Global and per-label concurrency limits with backpressure
- Structured logging and metrics hooks
- Typed config validation (Pydantic v2)
- Graceful shutdown and partial results capture
- Pluggable error policy (fail-fast or continue)

Quick start
1) Install requirements: pip install -r requirements.txt
2) Add tasks using TaskConfig and run via JulesOrchestrator.
3) Optionally run demo: python jules_orchestrator.py demo

Requirements
- Python 3.9+
- anyio, pydantic>=2, tenacity, structlog, typer

CI/CD
- Use GitHub Actions to run tests and lint on push.

License
MIT
