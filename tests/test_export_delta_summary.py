import json

from blueprint.export.data_exporter import (
    DataExportFormat,
    ExportManifest,
    ExportResult,
    ExportScope,
    summarize_delta,
)


def test_summarize_delta_reports_counts_per_section_for_decoded_dicts():
    before = {
        "plans": {
            "plan-1": {"id": "plan-1", "title": "Launch"},
            "plan-2": {"id": "plan-2", "title": "Operate"},
        },
        "tasks": [
            {"id": "task-1", "title": "Build"},
            {"id": "task-2", "title": "Ship"},
        ],
    }
    after = {
        "plans": {
            "plan-1": {"id": "plan-1", "title": "Launch v2"},
            "plan-3": {"id": "plan-3", "title": "Measure"},
        },
        "tasks": [
            {"id": "task-1", "title": "Build"},
            {"id": "task-3", "title": "Review"},
        ],
    }

    assert summarize_delta(before, after) == {
        "plans": {"added": 1, "removed": 1, "unchanged": 0, "changed": 1},
        "tasks": {"added": 1, "removed": 1, "unchanged": 1, "changed": 0},
    }


def test_summarize_delta_unwraps_json_export_payload_bytes():
    before = json.dumps(
        {
            "schema_version": "1.0.0",
            "data": {"plans": [{"id": "plan-1", "title": "Launch"}]},
        }
    ).encode("utf-8")
    after = json.dumps(
        {
            "schema_version": "1.0.0",
            "data": {
                "plans": [
                    {"id": "plan-1", "title": "Launch"},
                    {"id": "plan-2", "title": "Operate"},
                ]
            },
        }
    ).encode("utf-8")

    assert summarize_delta(before, after) == {
        "plans": {"added": 1, "removed": 0, "unchanged": 1, "changed": 0}
    }


def test_summarize_delta_accepts_export_result_json_data_bytes():
    before = _result({"data": {"users": {"user-1": {"name": "Alice"}}}})
    after = _result({"data": {"users": {"user-1": {"name": "Alice Smith"}}}})

    assert summarize_delta(before, after) == {
        "users": {"added": 0, "removed": 0, "unchanged": 0, "changed": 1}
    }


def _result(payload: dict) -> ExportResult:
    data = json.dumps(payload).encode("utf-8")
    return ExportResult(
        export_id="exp-test",
        format=DataExportFormat.JSON.value,
        scope=ExportScope.ALL.value,
        data=data,
        manifest=ExportManifest(
            export_id="exp-test",
            format=DataExportFormat.JSON.value,
            scope=ExportScope.ALL.value,
            timestamp="2026-05-12T00:00:00+00:00",
        ),
        record_count=1,
        created_at="2026-05-12T00:00:00+00:00",
    )
