"""Basic tests for jules_orchestrator."""

import jules_orchestrator


def test_version() -> None:
    """Test that version is defined."""
    assert hasattr(jules_orchestrator, "__version__")
    assert isinstance(jules_orchestrator.__version__, str)


def test_imports() -> None:
    """Test that package can be imported."""
    assert jules_orchestrator is not None
