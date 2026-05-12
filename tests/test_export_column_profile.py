"""Tests for export column profile reports."""

from blueprint.export import build_export_column_profile


def test_column_profile_reports_observed_columns_in_deterministic_order():
    rows = [
        {"title": "Build", "id": "task-1"},
        {"owner": "Data", "id": "task-2"},
    ]

    profile = build_export_column_profile(rows)

    assert profile["row_count"] == 2
    assert profile["observed_columns"] == ["id", "owner", "title"]
    assert profile["extra_columns"] == []
    assert profile["unexpected_columns"] == []


def test_column_profile_reports_missing_required_and_unexpected_columns():
    rows = [
        {"id": "task-1", "title": "Build", "legacy": "old"},
        {"id": "task-2", "title": "Ship", "owner": "Ops"},
    ]

    profile = build_export_column_profile(
        rows,
        expected_columns=["id", "title", "status"],
    )

    assert profile["missing_required_columns"] == ["status"]
    assert profile["extra_columns"] == ["legacy", "owner"]
    assert profile["unexpected_columns"] == ["legacy", "owner"]


def test_column_profile_computes_null_counts_and_fill_rates():
    rows = [
        {"id": "task-1", "owner": "Data"},
        {"id": "task-2", "owner": None},
        {"id": "task-3"},
    ]

    profile = build_export_column_profile(rows)

    assert profile["columns"]["id"] == {"null_count": 0, "fill_rate": 1.0}
    assert profile["columns"]["owner"] == {"null_count": 2, "fill_rate": 1 / 3}


def test_column_profile_handles_empty_rows_predictably():
    profile = build_export_column_profile(
        [],
        expected_columns=["id", "title"],
        required_columns=["id"],
    )

    assert profile == {
        "row_count": 0,
        "observed_columns": [],
        "missing_required_columns": ["id"],
        "extra_columns": [],
        "unexpected_columns": [],
        "columns": {},
    }
