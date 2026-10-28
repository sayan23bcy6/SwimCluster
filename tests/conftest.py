"""
conftest.py -- Shared pytest configuration and markers.
"""

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: real-socket integration tests (may take several seconds)",
    )
