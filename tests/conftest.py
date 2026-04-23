"""Test compatibility shims for local pytest invocation."""

from __future__ import annotations

from importlib.util import find_spec


if find_spec("pytest_cov") is None:
    def pytest_addoption(parser):
        """Accept pytest-cov CLI flags when the plugin is unavailable."""
        parser.addoption("--cov", action="append", default=[], help="Compatibility shim")
        parser.addoption(
            "--cov-report",
            action="append",
            default=[],
            help="Compatibility shim",
        )
