import json

from blueprint.importers.execution_event_jsonl_importer import (
    ExecutionEventJsonlImporter,
    ExecutionEventRecord,
    execution_event_jsonl_import_to_dict,
)


def test_execution_event_jsonl_imports_valid_events_grouped_by_plan_and_task(tmp_path):
    jsonl_path = tmp_path / "execution_events.jsonl"
    _write_jsonl(
        jsonl_path,
        [
            _event("task_started", "plan-a", "task-api", timestamp="2026-05-01T09:00:00+09:00"),
            _event("task_completed", "plan-a", "task-api", timestamp="2026-05-01T00:05:00Z"),
            {
                "event_type": "branch_created",
                "plan_id": "plan-a",
                "branch_name": "bp/plan-a/task-api",
                "timestamp": "2026-05-01T00:01:00Z",
            },
            _event("task_failed", "plan-b", "task-worker", timestamp="2026-05-01T00:02:00Z"),
        ],
    )

    result = ExecutionEventJsonlImporter().import_file(str(jsonl_path))

    assert result.error_count == 0
    assert result.valid_count == 4
    assert result.plan_ids == ("plan-a", "plan-b")
    assert isinstance(result.records[0], ExecutionEventRecord)
    assert result.records[0].timestamp == "2026-05-01T00:00:00Z"
    assert [group.plan_id for group in result.plan_groups] == ["plan-a", "plan-b"]
    assert [group.task_id for group in result.plan_groups[0].task_groups] == ["task-api"]
    assert [event.event_type for event in result.plan_groups[0].task_groups[0].events] == [
        "task_started",
        "task_completed",
    ]
    assert result.plan_groups[0].events[2].event_type == "branch_created"
    assert result.plan_groups[0].events[2].task_id is None


def test_execution_event_jsonl_reports_malformed_lines_without_aborting_when_configured(tmp_path):
    jsonl_path = tmp_path / "execution_events.jsonl"
    jsonl_path.write_text(
        json.dumps(_event("task_started", "plan-a", "task-api"))
        + "\n"
        + '{"event_type": "task_completed"\n'
        + json.dumps(_event("task_completed", "plan-a", "task-api")) 
        + "\n",
        encoding="utf-8",
    )

    result = ExecutionEventJsonlImporter().import_file(
        str(jsonl_path),
        continue_on_error=True,
    )

    assert result.valid_count == 2
    assert result.error_count == 1
    assert result.errors[0].line_number == 2
    assert "invalid JSON" in result.errors[0].message
    assert [event.event_type for event in result.records] == ["task_started", "task_completed"]


def test_execution_event_jsonl_stops_on_malformed_line_by_default(tmp_path):
    jsonl_path = tmp_path / "execution_events.jsonl"
    jsonl_path.write_text(
        json.dumps(_event("task_started", "plan-a", "task-api"))
        + "\n"
        + '{"event_type": "task_completed"\n'
        + json.dumps(_event("task_completed", "plan-a", "task-api")) 
        + "\n",
        encoding="utf-8",
    )

    result = ExecutionEventJsonlImporter().import_file(str(jsonl_path))

    assert result.valid_count == 0
    assert result.error_count == 1
    assert result.errors[0].line_number == 2
    assert result.records == ()
    assert result.plan_groups == ()


def test_execution_event_jsonl_unknown_event_type_produces_deterministic_error(tmp_path):
    jsonl_path = tmp_path / "execution_events.jsonl"
    _write_jsonl(
        jsonl_path,
        [
            {
                "event_type": "deployment_started",
                "plan_id": "plan-a",
                "task_id": "task-api",
                "timestamp": "2026-05-01T00:00:00Z",
            }
        ],
    )

    result = ExecutionEventJsonlImporter().import_file(str(jsonl_path))

    assert result.valid_count == 0
    assert result.error_count == 1
    assert result.errors[0].line_number == 1
    assert result.errors[0].message == (
        "validation failed: unknown event_type: deployment_started; expected one of: "
        "task_started, task_completed, task_failed, verification_failed, branch_created, "
        "artifact_exported"
    )


def test_execution_event_jsonl_timestamp_normalization_is_consistent(tmp_path):
    jsonl_path = tmp_path / "execution_events.jsonl"
    _write_jsonl(
        jsonl_path,
        [
            _event("task_started", "plan-a", "task-api", timestamp="2026-05-01T12:30:05+09:00"),
            _event("task_completed", "plan-a", "task-api", timestamp="2026-05-01T03:30:05Z"),
            _event("verification_failed", "plan-a", "task-api", timestamp="2026-05-01T03:30:05"),
        ],
    )

    result = ExecutionEventJsonlImporter().import_file(str(jsonl_path))
    payload = execution_event_jsonl_import_to_dict(result)

    assert [event.timestamp for event in result.records] == [
        "2026-05-01T03:30:05Z",
        "2026-05-01T03:30:05Z",
        "2026-05-01T03:30:05Z",
    ]
    assert payload["records"][0]["payload"]["timestamp"] == "2026-05-01T03:30:05Z"
    assert json.loads(json.dumps(payload)) == payload


def test_execution_event_jsonl_validates_required_fields_by_event_type(tmp_path):
    jsonl_path = tmp_path / "execution_events.jsonl"
    _write_jsonl(
        jsonl_path,
        [
            {
                "event_type": "artifact_exported",
                "plan_id": "plan-a",
                "timestamp": "2026-05-01T00:00:00Z",
            }
        ],
    )

    result = ExecutionEventJsonlImporter().import_file(str(jsonl_path))

    assert result.valid_count == 0
    assert result.errors[0].message == "validation failed: missing required field(s): artifact_path"


def _write_jsonl(path, records):
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )


def _event(event_type, plan_id, task_id, *, timestamp="2026-05-01T00:00:00Z"):
    return {
        "event_type": event_type,
        "plan_id": plan_id,
        "task_id": task_id,
        "timestamp": timestamp,
        "engine": "codex",
    }
