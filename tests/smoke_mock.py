#!/usr/bin/env python3
"""Smoke test to verify basic imports and module structure."""

import sys
from pathlib import Path


def test_imports():
    """Test that all required packages can be imported."""
    try:
        import aiohttp  # noqa: F401
        import yaml  # noqa: F401

        print("✓ All imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False


def test_requirements():
    """Test that requirements files exist."""
    repo_root = Path(__file__).parent.parent
    requirements = repo_root / "requirements.txt"
    requirements_dev = repo_root / "requirements-dev.txt"

    if not requirements.exists():
        print("✗ Missing requirements.txt")
        return False

    if not requirements_dev.exists():
        print("✗ Missing requirements-dev.txt")
        return False

    print("✓ All requirement files exist")
    return True
