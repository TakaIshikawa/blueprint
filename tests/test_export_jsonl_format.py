import json

from blueprint.export.data_exporter import DataExporter, DataExportFormat, ExportFilters, InMemoryDataStore


def _store() -> InMemoryDataStore:
    store = InMemoryDataStore()
    store.plans["plan-1"] = {
        "id": "plan-1",
        "title": "Launch",
        "status": "active",
        "tags": ["release"],
        "tasks": [{"id": "task-1"}],
    }
    store.plans["plan-2"] = {
        "id": "plan-2",
        "title": "Backlog",
        "status": "draft",
        "tags": ["internal"],
        "tasks": [{"id": "task-2"}],
    }
    store.tasks["task-1"] = {
        "id": "task-1",
        "title": "Ship",
        "execution_plan_id": "plan-1",
    }
    store.tasks["task-2"] = {
        "id": "task-2",
        "title": "Wait",
        "execution_plan_id": "plan-2",
    }
    store.users["user-1"] = {"id": "user-1", "email": "owner@example.com"}
    return store


def _jsonl_records(data: bytes) -> list[dict[str, object]]:
    return [json.loads(line) for line in data.decode("utf-8").splitlines()]


def test_all_data_jsonl_outputs_one_json_object_per_record():
    exporter = DataExporter(store=_store())

    result = exporter.export_all_data(fmt=DataExportFormat.JSONL)

    records = _jsonl_records(result.data)
    assert result.format == "jsonl"
    assert result.manifest.format == "jsonl"
    assert result.manifest.record_counts == {
        "workspaces": 0,
        "plans": 2,
        "tasks": 2,
        "users": 1,
        "settings": 0,
        "events": 0,
    }
    assert len(records) == result.record_count == 5
    assert {record["section"] for record in records} == {"plans", "tasks", "users"}
    assert all(record["schema_version"] == result.manifest.schema_version for record in records)
    assert {record["id"] for record in records} == {
        "plan-1",
        "plan-2",
        "task-1",
        "task-2",
        "user-1",
    }
    assert all("data" in record for record in records)


def test_filtered_jsonl_output_only_emits_matching_records():
    exporter = DataExporter(store=_store())
    filters = ExportFilters(status=["active"])

    result = exporter.export_all_data(fmt=DataExportFormat.JSONL, filters=filters)

    records = _jsonl_records(result.data)
    assert result.scope == "filtered"
    assert result.manifest.record_counts == {"plans": 1, "tasks": 1}
    assert result.manifest.filters_applied == {"status": ["active"]}
    assert [(record["section"], record["id"]) for record in records] == [
        ("plans", "plan-1"),
        ("tasks", "task-1"),
    ]
