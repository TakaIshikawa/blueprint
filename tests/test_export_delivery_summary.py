"""Tests for export delivery summaries."""

from datetime import UTC, datetime
from types import SimpleNamespace

from blueprint.export import build_export_delivery_summary


def test_delivery_summary_counts_statuses_and_retryable_results():
    results = [
        {"destination": "s3://exports/tasks.json", "status": "delivered", "delivered_at": "2026-01-01T00:00:00Z"},
        {"destination": "webhook:partner-b", "status": "failed", "retryable": True},
        {"destination": "email:audit@example.com", "status": "queued"},
        {"destination": "warehouse", "status": "success", "completed_at": datetime(2026, 1, 2, tzinfo=UTC)},
    ]

    summary = build_export_delivery_summary(results)

    assert summary["total"] == 4
    assert summary["delivered"] == 2
    assert summary["failed"] == 1
    assert summary["pending"] == 1
    assert summary["retryable"] == 1
    assert summary["status_counts"] == {"delivered": 2, "failed": 1, "pending": 1}
    assert summary["latest_delivered_at"] == "2026-01-02T00:00:00+00:00"


def test_delivery_summary_returns_failed_destinations_in_deterministic_order():
    results = [
        SimpleNamespace(destination_id="zeta", status="failed", retryable=True),
        {"name": "alpha", "status": "error"},
        {"path": "/tmp/export.json", "status": "delivered"},
    ]

    summary = build_export_delivery_summary(results)

    assert summary["failed_destinations"] == ["alpha", "zeta"]
    assert summary["retryable"] == 1


def test_delivery_summary_handles_missing_timestamps_and_empty_inputs():
    assert build_export_delivery_summary([]) == {
        "total": 0,
        "delivered": 0,
        "failed": 0,
        "pending": 0,
        "retryable": 0,
        "status_counts": {},
        "failed_destinations": [],
        "latest_delivered_at": None,
    }

    summary = build_export_delivery_summary([{"destination": "s3", "status": "delivered"}])

    assert summary["latest_delivered_at"] is None
    assert summary["failed_destinations"] == []
