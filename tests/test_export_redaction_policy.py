import hashlib
import json

from blueprint.export.data_exporter import (
    DataExporter,
    DataExportFormat,
    ExportOptions,
    ExportRedactionPolicy,
    InMemoryDataStore,
)


def _store() -> InMemoryDataStore:
    store = InMemoryDataStore()
    store.workspaces["ws-1"] = {
        "id": "ws-1",
        "name": "Launch Team",
        "plan_ids": ["plan-1"],
    }
    store.plans["plan-1"] = {
        "id": "plan-1",
        "title": "Launch",
        "client_secret": "plan-secret",
        "tasks": [
            {
                "id": "task-1",
                "title": "Prepare rollout",
                "client_secret": "nested-secret",
            }
        ],
    }
    store.tasks["task-1"] = {
        "id": "task-1",
        "title": "Prepare rollout",
        "client_secret": "task-secret",
        "owner_id": "user-1",
    }
    store.users["user-1"] = {
        "id": "user-1",
        "name": "Alice",
        "email": "alice@example.com",
        "api_key": "key-123",
    }
    return store


def test_export_redaction_policy_redacts_custom_fields_recursively_in_json():
    exporter = DataExporter(_store())
    options = ExportOptions(
        redaction_policy=ExportRedactionPolicy(
            "partner-safe",
            {"client_secret", "api_key"},
            replacement_text="[redacted]",
        )
    )

    result = exporter.export_all_data(fmt=DataExportFormat.JSON, options=options)

    payload = json.loads(result.data)
    assert payload["data"]["plans"]["plan-1"]["client_secret"] == "[redacted]"
    assert payload["data"]["plans"]["plan-1"]["tasks"][0]["client_secret"] == "[redacted]"
    assert payload["data"]["tasks"]["task-1"]["client_secret"] == "[redacted]"
    assert payload["data"]["users"]["user-1"]["api_key"] == "[redacted]"
    assert payload["data"]["users"]["user-1"]["email"] == "alice@example.com"


def test_export_redaction_policy_uses_deterministic_hashed_replacements():
    exporter = DataExporter(_store())
    options = ExportOptions(
        redaction_policy=ExportRedactionPolicy(
            "hashed",
            {"api_key"},
            replacement_text="hash:",
            hash_replacements=True,
        )
    )

    first = json.loads(exporter.export_all_data(options=options).data)
    second = json.loads(exporter.export_all_data(options=options).data)
    expected = "hash:" + hashlib.sha256(b"key-123").hexdigest()

    assert first["data"]["users"]["user-1"]["api_key"] == expected
    assert second["data"]["users"]["user-1"]["api_key"] == expected


def test_existing_anonymized_exports_still_hash_default_pii_fields():
    exporter = DataExporter(_store())

    result = exporter.export_all_data(options=ExportOptions(anonymize=True))

    user = json.loads(result.data)["data"]["users"]["user-1"]
    assert user["name"].startswith("anon-")
    assert user["email"].startswith("anon-")
    assert user["api_key"] == "key-123"


def test_jsonl_export_reflects_configured_redaction_policy():
    exporter = DataExporter(_store())
    options = ExportOptions(
        redaction_policy=ExportRedactionPolicy(
            "jsonl",
            ["client_secret"],
            replacement_text="MASKED",
        )
    )

    result = exporter.export_all_data(fmt=DataExportFormat.JSONL, options=options)
    rows = [json.loads(line) for line in result.data.decode("utf-8").splitlines()]

    plan_row = next(row for row in rows if row["section"] == "plans" and row["id"] == "plan-1")
    task_row = next(row for row in rows if row["section"] == "tasks" and row["id"] == "task-1")
    assert plan_row["data"]["client_secret"] == "MASKED"
    assert plan_row["data"]["tasks"][0]["client_secret"] == "MASKED"
    assert task_row["data"]["client_secret"] == "MASKED"
