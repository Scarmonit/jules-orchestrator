"""Jules Orchestrator - Production-ready parallel execution orchestrator."""

__version__ = "0.1.0"
__author__ = "Scarmonit"

# Re-export from root module for package imports
import sys
import os

# Add parent directory to path to import from jules_orchestrator.py
_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _parent not in sys.path:
    sys.path.insert(0, _parent)

# Import from the root jules_orchestrator.py module
from jules_orchestrator import (
    JulesOrchestrator,
    OrchestratorConfig,
    OrchestratorError,
    TaskConfig,
    TaskResult,
    TaskFailed,
    DAGCycleError,
    ErrorPolicy,
)

__all__ = [
    "__version__",
    "__author__",
    "JulesOrchestrator",
    "OrchestratorConfig",
    "OrchestratorError",
    "TaskConfig",
    "TaskResult",
    "TaskFailed",
    "DAGCycleError",
    "ErrorPolicy",
]
