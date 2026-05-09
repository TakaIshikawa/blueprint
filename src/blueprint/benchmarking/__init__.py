"""Benchmarking system with industry standards."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.benchmarking.benchmark_data import (
        BenchmarkDataPoint,
        BenchmarkDataset,
    )
    from blueprint.benchmarking.benchmark_engine import (
        BenchmarkDataStore,
        BenchmarkEngine,
        BenchmarkInsight,
        BenchmarkReport,
        BenchmarkResult,
        Outlier,
        Percentile,
    )

_DATA_MODULE = "blueprint.benchmarking.benchmark_data"
_ENGINE_MODULE = "blueprint.benchmarking.benchmark_engine"

_EXPORTS = {
    "BenchmarkDataPoint": _DATA_MODULE,
    "BenchmarkDataset": _DATA_MODULE,
    "BenchmarkDataStore": _ENGINE_MODULE,
    "BenchmarkEngine": _ENGINE_MODULE,
    "BenchmarkInsight": _ENGINE_MODULE,
    "BenchmarkReport": _ENGINE_MODULE,
    "BenchmarkResult": _ENGINE_MODULE,
    "Outlier": _ENGINE_MODULE,
    "Percentile": _ENGINE_MODULE,
}


def __getattr__(name: str) -> Any:
    """Load benchmarking classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
