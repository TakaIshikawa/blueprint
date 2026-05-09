"""Tests for the comprehensive data export API."""

import json
import struct
from datetime import datetime, timezone

from blueprint.export.data_exporter import (
    DataExporter,
    DataExportFormat,
    ExportFilters,
    ExportJobStatus,
    ExportManifest,
    ExportOptions,
    ExportResult,
    ExportScheduleConfig,
    ExportScope,
    InMemoryDataStore,
    UserDataExport,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_store() -> InMemoryDataStore:
    """Build a store pre-populated with realistic test data."""
    now = _now()
    store = InMemoryDataStore()

    store.workspaces["ws-1"] = {
        "id": "ws-1",
        "name": "Acme Corp",
        "plan_ids": ["plan-1", "plan-2"],
        "created_at": now,
    }
    store.workspaces["ws-2"] = {
        "id": "ws-2",
        "name": "Beta Inc",
        "plan_ids": ["plan-3"],
        "created_at": now,
    }

    store.plans["plan-1"] = {
        "id": "plan-1",
        "title": "Auth System",
        "status": "in_progress",
        "tags": ["backend", "security"],
        "user_ids": ["user-1"],
        "tasks": [
            {"id": "task-1", "title": "Login endpoint", "depends_on": []},
            {"id": "task-2", "title": "Token refresh", "depends_on": ["task-1"]},
        ],
        "created_at": now,
        "updated_at": now,
    }
    store.plans["plan-2"] = {
        "id": "plan-2",
        "title": "Dashboard UI",
        "status": "draft",
        "tags": ["frontend"],
        "user_ids": ["user-1", "user-2"],
        "tasks": [
            {"id": "task-3", "title": "Chart component", "depends_on": []},
        ],
        "created_at": now,
        "updated_at": now,
    }
    store.plans["plan-3"] = {
        "id": "plan-3",
        "title": "Billing Integration",
        "status": "completed",
        "tags": ["backend", "billing"],
        "user_ids": ["user-2"],
        "tasks": [
            {"id": "task-4", "title": "Stripe setup", "depends_on": []},
        ],
        "created_at": now,
        "updated_at": now,
    }

    store.tasks["task-1"] = {
        "id": "task-1",
        "title": "Login endpoint",
        "owner_id": "user-1",
        "execution_plan_id": "plan-1",
        "depends_on": [],
        "updated_at": now,
    }
    store.tasks["task-2"] = {
        "id": "task-2",
        "title": "Token refresh",
        "owner_id": "user-1",
        "execution_plan_id": "plan-1",
        "depends_on": ["task-1"],
        "updated_at": now,
    }
    store.tasks["task-3"] = {
        "id": "task-3",
        "title": "Chart component",
        "owner_id": "user-2",
        "execution_plan_id": "plan-2",
        "depends_on": [],
        "updated_at": now,
    }
    store.tasks["task-4"] = {
        "id": "task-4",
        "title": "Stripe setup",
        "owner_id": "user-2",
        "execution_plan_id": "plan-3",
        "depends_on": [],
        "updated_at": now,
    }

    store.users["user-1"] = {
        "id": "user-1",
        "name": "Alice Johnson",
        "email": "alice@example.com",
    }
    store.users["user-2"] = {
        "id": "user-2",
        "name": "Bob Smith",
        "email": "bob@example.com",
    }

    store.settings = {"theme": "dark", "notifications": True}

    store.events.append({
        "id": "evt-1",
        "user_id": "user-1",
        "action": "plan_created",
        "entity_id": "plan-1",
        "created_at": now,
    })
    store.events.append({
        "id": "evt-2",
        "user_id": "user-2",
        "action": "task_completed",
        "entity_id": "task-4",
        "created_at": now,
    })

    return store


def _exporter(store: InMemoryDataStore | None = None) -> DataExporter:
    return DataExporter(store=store or _make_store())


# ---------------------------------------------------------------------------
# JSON format tests
# ---------------------------------------------------------------------------


class TestJsonExport:
    def test_export_all_data_json(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.JSON)

        assert isinstance(result, ExportResult)
        assert result.format == "json"
        assert result.scope == "all"
        assert result.record_count > 0
        assert len(result.data) > 0

        payload = json.loads(result.data)
        assert payload["schema_version"] == "1.0.0"
        assert "data" in payload
        assert "plans" in payload["data"]
        assert "tasks" in payload["data"]
        assert "users" in payload["data"]

    def test_json_contains_all_plans(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.JSON)
        payload = json.loads(result.data)
        plans = payload["data"]["plans"]
        assert len(plans) == 3

    def test_json_manifest_generated(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.JSON)
        manifest = result.manifest

        assert isinstance(manifest, ExportManifest)
        assert manifest.schema_version == "1.0.0"
        assert manifest.format == "json"
        assert manifest.scope == "all"
        assert "sha256" in manifest.checksums
        assert len(manifest.checksums["sha256"]) == 64
        assert manifest.record_counts["plans"] == 3
        assert manifest.record_counts["tasks"] == 4
        assert manifest.record_counts["users"] == 2

    def test_json_checksum_matches_data(self):
        import hashlib
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.JSON)
        expected = hashlib.sha256(result.data).hexdigest()
        assert result.manifest.checksums["sha256"] == expected


# ---------------------------------------------------------------------------
# CSV format tests
# ---------------------------------------------------------------------------


class TestCsvExport:
    def test_export_all_data_csv(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.CSV)

        assert result.format == "csv"
        csv_text = result.data.decode("utf-8")
        assert "# plans" in csv_text
        assert "# tasks" in csv_text

    def test_csv_contains_plan_fields(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.CSV)
        csv_text = result.data.decode("utf-8")
        assert "Auth System" in csv_text
        assert "Dashboard UI" in csv_text
        assert "Billing Integration" in csv_text

    def test_csv_workspace_export(self):
        exp = _exporter()
        data = exp.export_workspace("ws-1", fmt=DataExportFormat.CSV)
        csv_text = data.decode("utf-8")
        assert "Auth System" in csv_text or "plan-1" in csv_text


# ---------------------------------------------------------------------------
# SQL format tests
# ---------------------------------------------------------------------------


class TestSqlExport:
    def test_export_all_data_sql(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.SQL)

        assert result.format == "sql"
        sql_text = result.data.decode("utf-8")
        assert "INSERT INTO" in sql_text
        assert "-- Blueprint Data Export" in sql_text

    def test_sql_contains_insert_for_plans(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.SQL)
        sql_text = result.data.decode("utf-8")
        assert "INSERT INTO plans" in sql_text
        assert "Auth System" in sql_text

    def test_sql_escapes_single_quotes(self):
        store = _make_store()
        store.plans["plan-q"] = {
            "id": "plan-q",
            "title": "It's a test",
            "status": "draft",
            "tags": [],
            "user_ids": [],
            "tasks": [],
            "created_at": _now(),
            "updated_at": _now(),
        }
        exp = DataExporter(store=store)
        result = exp.export_all_data(fmt=DataExportFormat.SQL)
        sql_text = result.data.decode("utf-8")
        assert "It''s a test" in sql_text


# ---------------------------------------------------------------------------
# Parquet format tests
# ---------------------------------------------------------------------------


class TestParquetExport:
    def test_export_all_data_parquet(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.PARQUET)

        assert result.format == "parquet"
        assert len(result.data) > 0

    def test_parquet_has_header(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.PARQUET)
        # First 4 bytes = header length (big-endian unsigned int)
        header_len = struct.unpack(">I", result.data[:4])[0]
        header = json.loads(result.data[4 : 4 + header_len])
        assert header["format"] == "blueprint-columnar-v1"
        assert header["schema_version"] == "1.0.0"
        assert "plans" in header["tables"]

    def test_parquet_column_offsets(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.PARQUET)
        header_len = struct.unpack(">I", result.data[:4])[0]
        header = json.loads(result.data[4 : 4 + header_len])
        plans_table = header["tables"]["plans"]
        assert plans_table["row_count"] == 3
        assert len(plans_table["columns"]) > 0
        for col in plans_table["columns"]:
            assert col in plans_table["column_offsets"]


# ---------------------------------------------------------------------------
# Protobuf format tests
# ---------------------------------------------------------------------------


class TestProtobufExport:
    def test_export_all_data_protobuf(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.PROTOBUF)

        assert result.format == "protobuf"
        assert result.data[:4] == b"BPEX"

    def test_protobuf_contains_schema_version(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.PROTOBUF)
        data = result.data
        # After magic, read schema version
        sv_len = struct.unpack(">H", data[4:6])[0]
        sv = data[6 : 6 + sv_len].decode("utf-8")
        assert sv == "1.0.0"

    def test_protobuf_round_trip_rows(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.PROTOBUF)
        data = result.data
        pos = 4  # skip magic
        sv_len = struct.unpack(">H", data[pos : pos + 2])[0]
        pos += 2 + sv_len

        tables_found: dict[str, list[dict]] = {}
        while pos < len(data):
            tn_len = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2
            table_name = data[pos : pos + tn_len].decode("utf-8")
            pos += tn_len
            row_count = struct.unpack(">I", data[pos : pos + 4])[0]
            pos += 4
            rows = []
            for _ in range(row_count):
                row_len = struct.unpack(">I", data[pos : pos + 4])[0]
                pos += 4
                row = json.loads(data[pos : pos + row_len])
                rows.append(row)
                pos += row_len
            tables_found[table_name] = rows

        assert "plans" in tables_found
        assert len(tables_found["plans"]) == 3


# ---------------------------------------------------------------------------
# Export scopes
# ---------------------------------------------------------------------------


class TestExportScopes:
    def test_scope_all(self):
        exp = _exporter()
        result = exp.export_all_data()
        assert result.scope == "all"
        payload = json.loads(result.data)
        assert len(payload["data"]["plans"]) == 3

    def test_scope_workspace(self):
        exp = _exporter()
        data = exp.export_workspace("ws-1")
        payload = json.loads(data)
        ws_data = payload["data"]
        assert "plans" in ws_data
        # ws-1 has plan-1, plan-2
        assert len(ws_data["plans"]) == 2

    def test_scope_workspace_excludes_other(self):
        exp = _exporter()
        data = exp.export_workspace("ws-2")
        payload = json.loads(data)
        ws_data = payload["data"]
        # ws-2 has only plan-3
        assert len(ws_data["plans"]) == 1

    def test_scope_plan_tree(self):
        exp = _exporter()
        result = exp.export_plan_with_dependencies("plan-1")
        assert result["scope"] == "plan_tree"
        assert result["plan_id"] == "plan-1"
        assert "plan-1" in result["plans"]

    def test_scope_plan_tree_depth_zero(self):
        exp = _exporter()
        result = exp.export_plan_with_dependencies("plan-1", depth=0)
        assert "plan-1" in result["plans"]

    def test_scope_filtered_by_status(self):
        exp = _exporter()
        filters = ExportFilters(status=["completed"])
        result = exp.export_all_data(filters=filters)
        assert result.scope == "filtered"
        payload = json.loads(result.data)
        plans = payload["data"]["plans"]
        assert len(plans) == 1

    def test_scope_filtered_by_tags(self):
        exp = _exporter()
        filters = ExportFilters(tags=["frontend"])
        result = exp.export_all_data(filters=filters)
        payload = json.loads(result.data)
        plans = payload["data"]["plans"]
        assert len(plans) == 1

    def test_scope_filtered_by_plan_ids(self):
        exp = _exporter()
        filters = ExportFilters(plan_ids=["plan-1", "plan-3"])
        result = exp.export_all_data(filters=filters)
        payload = json.loads(result.data)
        assert len(payload["data"]["plans"]) == 2

    def test_scope_filtered_empty(self):
        exp = _exporter()
        filters = ExportFilters(status=["nonexistent"])
        result = exp.export_all_data(filters=filters)
        payload = json.loads(result.data)
        assert len(payload["data"]["plans"]) == 0


# ---------------------------------------------------------------------------
# Incremental exports
# ---------------------------------------------------------------------------


class TestIncrementalExports:
    def test_changes_since(self):
        exp = _exporter()
        past = datetime(2020, 1, 1, tzinfo=timezone.utc)
        result = exp.export_changes_since(past)
        payload = json.loads(result.data)
        # All data was created "now", so everything should be included
        assert len(payload["data"]["plans"]) == 3

    def test_changes_since_future(self):
        exp = _exporter()
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        result = exp.export_changes_since(future)
        payload = json.loads(result.data)
        assert len(payload["data"]["plans"]) == 0

    def test_delta_between(self):
        exp = _exporter()
        past = datetime(2020, 1, 1, tzinfo=timezone.utc)
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        result = exp.export_delta(past, future)
        payload = json.loads(result.data)
        assert len(payload["data"]["plans"]) == 3

    def test_delta_narrow_window(self):
        exp = _exporter()
        t1 = datetime(2020, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2020, 1, 2, tzinfo=timezone.utc)
        result = exp.export_delta(t1, t2)
        payload = json.loads(result.data)
        assert len(payload["data"]["plans"]) == 0

    def test_incremental_filters_recorded(self):
        exp = _exporter()
        since = datetime(2020, 6, 1, tzinfo=timezone.utc)
        result = exp.export_changes_since(since)
        assert "date_from" in result.manifest.filters_applied


# ---------------------------------------------------------------------------
# Streaming and chunked exports
# ---------------------------------------------------------------------------


class TestStreamingExport:
    def test_stream_yields_chunks(self):
        exp = _exporter()
        chunks = list(exp.stream_export())
        assert len(chunks) > 0
        for chunk in chunks:
            assert isinstance(chunk, bytes)
            assert len(chunk) > 0

    def test_stream_with_small_chunk_size(self):
        exp = _exporter()
        chunks = list(exp.stream_export(chunk_size=1))
        # With chunk_size=1 and multiple records, we get more chunks
        assert len(chunks) >= 3

    def test_stream_csv_format(self):
        exp = _exporter()
        chunks = list(exp.stream_export(fmt=DataExportFormat.CSV))
        assert len(chunks) > 0
        # Each chunk should be valid CSV-ish bytes
        for chunk in chunks:
            text = chunk.decode("utf-8")
            assert len(text) > 0

    def test_stream_progress_tracking(self):
        exp = _exporter()
        chunks = list(exp.stream_export(chunk_size=2))
        # After consuming all chunks, there should be a completed progress
        assert len(chunks) > 0
        # Progress entries are stored internally; verify at least one exists
        assert len(exp._progress) > 0
        # The last progress should be completed
        last_progress = list(exp._progress.values())[-1]
        assert last_progress.status == ExportJobStatus.COMPLETED.value

    def test_stream_filtered(self):
        exp = _exporter()
        filters = ExportFilters(status=["completed"])
        chunks = list(exp.stream_export(filters=filters))
        assert len(chunks) > 0

    def test_stream_sql_format(self):
        exp = _exporter()
        chunks = list(exp.stream_export(fmt=DataExportFormat.SQL))
        combined = b"".join(chunks).decode("utf-8")
        assert "INSERT INTO" in combined


# ---------------------------------------------------------------------------
# Export options
# ---------------------------------------------------------------------------


class TestExportOptions:
    def test_include_metadata_default(self):
        exp = _exporter()
        result = exp.export_all_data(fmt=DataExportFormat.JSON)
        payload = json.loads(result.data)
        plans = payload["data"]["plans"]
        # Default includes metadata
        first_plan = list(plans.values())[0] if isinstance(plans, dict) else plans[0]
        assert "created_at" in first_plan

    def test_exclude_metadata(self):
        exp = _exporter()
        opts = ExportOptions(include_metadata=False)
        result = exp.export_all_data(fmt=DataExportFormat.JSON, options=opts)
        payload = json.loads(result.data)
        plans = payload["data"]["plans"]
        first_plan = list(plans.values())[0] if isinstance(plans, dict) else plans[0]
        assert "created_at" not in first_plan
        assert "updated_at" not in first_plan

    def test_exclude_relationships(self):
        exp = _exporter()
        opts = ExportOptions(include_relationships=False)
        result = exp.export_all_data(fmt=DataExportFormat.JSON, options=opts)
        payload = json.loads(result.data)
        plans = payload["data"]["plans"]
        first_plan = list(plans.values())[0] if isinstance(plans, dict) else plans[0]
        assert "user_ids" not in first_plan

    def test_anonymize_pii(self):
        exp = _exporter()
        opts = ExportOptions(anonymize=True)
        result = exp.export_all_data(fmt=DataExportFormat.JSON, options=opts)
        payload = json.loads(result.data)
        users = payload["data"]["users"]
        first_user = list(users.values())[0] if isinstance(users, dict) else users[0]
        assert first_user["name"].startswith("anon-")
        assert first_user["email"].startswith("anon-")

    def test_anonymize_preserves_non_pii(self):
        exp = _exporter()
        opts = ExportOptions(anonymize=True)
        result = exp.export_all_data(fmt=DataExportFormat.JSON, options=opts)
        payload = json.loads(result.data)
        plans = payload["data"]["plans"]
        first_plan = list(plans.values())[0] if isinstance(plans, dict) else plans[0]
        # Title is not PII — it should be preserved
        assert not first_plan["title"].startswith("anon-")


# ---------------------------------------------------------------------------
# User data export (GDPR)
# ---------------------------------------------------------------------------


class TestUserDataExport:
    def test_export_user_data(self):
        exp = _exporter()
        result = exp.export_user_data("user-1")

        assert isinstance(result, UserDataExport)
        assert result.user_id == "user-1"
        assert len(result.plans) >= 1
        assert len(result.tasks) >= 1
        assert len(result.activity) >= 1
        assert result.anonymized is False

    def test_export_user_data_anonymized(self):
        exp = _exporter()
        result = exp.export_user_data("user-1", anonymize=True)
        assert result.anonymized is True

    def test_export_nonexistent_user(self):
        exp = _exporter()
        result = exp.export_user_data("nonexistent")
        assert result.user_id == "nonexistent"
        assert len(result.plans) == 0
        assert len(result.tasks) == 0


# ---------------------------------------------------------------------------
# Scheduled export jobs
# ---------------------------------------------------------------------------


class TestScheduledExports:
    def test_schedule_export(self):
        exp = _exporter()
        job = exp.schedule_export("daily")
        assert job.schedule == "daily"
        assert job.status == ExportJobStatus.PENDING.value
        assert job.job_id.startswith("exp-")

    def test_schedule_with_config(self):
        exp = _exporter()
        config = ExportScheduleConfig(
            format=DataExportFormat.CSV,
            scope=ExportScope.WORKSPACE,
            destination="/tmp/exports",
        )
        job = exp.schedule_export("0 2 * * *", config=config)
        assert job.config["format"] == "csv"
        assert job.config["scope"] == "workspace"
        assert job.config["destination"] == "/tmp/exports"

    def test_get_job(self):
        exp = _exporter()
        job = exp.schedule_export("weekly")
        retrieved = exp.get_job(job.job_id)
        assert retrieved is not None
        assert retrieved.job_id == job.job_id

    def test_cancel_job(self):
        exp = _exporter()
        job = exp.schedule_export("monthly")
        assert exp.cancel_job(job.job_id) is True
        cancelled = exp.get_job(job.job_id)
        assert cancelled is not None
        assert cancelled.status == ExportJobStatus.CANCELLED.value

    def test_cancel_nonexistent_job(self):
        exp = _exporter()
        assert exp.cancel_job("fake-id") is False


# ---------------------------------------------------------------------------
# Manifest and checksums
# ---------------------------------------------------------------------------


class TestManifest:
    def test_manifest_record_counts(self):
        exp = _exporter()
        result = exp.export_all_data()
        assert result.manifest.record_counts["plans"] == 3
        assert result.manifest.record_counts["tasks"] == 4
        assert result.manifest.record_counts["users"] == 2
        assert result.manifest.record_counts["events"] == 2

    def test_manifest_scope(self):
        exp = _exporter()
        result = exp.export_all_data()
        assert result.manifest.scope == "all"

    def test_manifest_filters_empty_for_all(self):
        exp = _exporter()
        result = exp.export_all_data()
        assert result.manifest.filters_applied == {}

    def test_manifest_filters_recorded(self):
        exp = _exporter()
        filters = ExportFilters(status=["draft"], tags=["frontend"])
        result = exp.export_all_data(filters=filters)
        applied = result.manifest.filters_applied
        assert applied["status"] == ["draft"]
        assert applied["tags"] == ["frontend"]


# ---------------------------------------------------------------------------
# Plan tree / dependency resolution
# ---------------------------------------------------------------------------


class TestPlanTree:
    def test_plan_tree_basic(self):
        exp = _exporter()
        result = exp.export_plan_with_dependencies("plan-1")
        assert "plan-1" in result["plans"]
        assert result["schema_version"] == "1.0.0"

    def test_plan_tree_nonexistent(self):
        exp = _exporter()
        result = exp.export_plan_with_dependencies("nonexistent")
        assert len(result["plans"]) == 0

    def test_plan_tree_with_cross_plan_deps(self):
        store = _make_store()
        # Make task-3 (plan-2) depend on task-1 (plan-1)
        store.tasks["task-3"]["depends_on"] = ["task-1"]
        store.plans["plan-2"]["tasks"][0]["depends_on"] = ["task-1"]
        exp = DataExporter(store=store)
        result = exp.export_plan_with_dependencies("plan-2")
        # Should include plan-2 and transitively plan-1
        assert "plan-2" in result["plans"]
        assert "plan-1" in result["plans"]

    def test_plan_tree_depth_limits(self):
        store = _make_store()
        store.tasks["task-3"]["depends_on"] = ["task-1"]
        store.plans["plan-2"]["tasks"][0]["depends_on"] = ["task-1"]
        exp = DataExporter(store=store)
        # depth=0 should only include the root plan
        result = exp.export_plan_with_dependencies("plan-2", depth=0)
        assert "plan-2" in result["plans"]
        assert "plan-1" not in result["plans"]


# ---------------------------------------------------------------------------
# Empty store
# ---------------------------------------------------------------------------


class TestEmptyStore:
    def test_export_empty_json(self):
        exp = DataExporter(store=InMemoryDataStore())
        result = exp.export_all_data(fmt=DataExportFormat.JSON)
        payload = json.loads(result.data)
        assert payload["data"]["plans"] == {}
        assert result.record_count == 0

    def test_export_empty_csv(self):
        exp = DataExporter(store=InMemoryDataStore())
        result = exp.export_all_data(fmt=DataExportFormat.CSV)
        # Empty store produces empty CSV (no rows to write)
        assert isinstance(result.data, bytes)
        assert result.record_count == 0

    def test_export_empty_sql(self):
        exp = DataExporter(store=InMemoryDataStore())
        result = exp.export_all_data(fmt=DataExportFormat.SQL)
        sql_text = result.data.decode("utf-8")
        assert "-- Blueprint Data Export" in sql_text

    def test_stream_empty(self):
        exp = DataExporter(store=InMemoryDataStore())
        chunks = list(exp.stream_export())
        # Empty store may yield no data chunks or minimal ones
        # Just verify it doesn't crash
        assert isinstance(chunks, list)


# ---------------------------------------------------------------------------
# Large dataset handling
# ---------------------------------------------------------------------------


class TestLargeDataset:
    def test_export_many_plans(self):
        store = InMemoryDataStore()
        now = _now()
        for i in range(100):
            pid = f"plan-{i}"
            store.plans[pid] = {
                "id": pid,
                "title": f"Plan {i}",
                "status": "draft",
                "tags": [],
                "user_ids": [],
                "tasks": [{"id": f"task-{i}", "title": f"Task {i}", "depends_on": []}],
                "created_at": now,
                "updated_at": now,
            }
            store.tasks[f"task-{i}"] = {
                "id": f"task-{i}",
                "title": f"Task {i}",
                "owner_id": "user-1",
                "execution_plan_id": pid,
                "depends_on": [],
                "updated_at": now,
            }

        exp = DataExporter(store=store)
        result = exp.export_all_data(fmt=DataExportFormat.JSON)
        payload = json.loads(result.data)
        assert len(payload["data"]["plans"]) == 100
        assert result.manifest.record_counts["plans"] == 100

    def test_stream_large_dataset_chunked(self):
        store = InMemoryDataStore()
        now = _now()
        for i in range(50):
            store.plans[f"p-{i}"] = {
                "id": f"p-{i}",
                "title": f"Plan {i}",
                "status": "draft",
                "tags": [],
                "user_ids": [],
                "tasks": [],
                "created_at": now,
                "updated_at": now,
            }

        exp = DataExporter(store=store)
        chunks = list(exp.stream_export(chunk_size=10))
        # 50 plans / 10 per chunk = 5 plan chunks (empty other sections skip)
        assert len(chunks) >= 5


# ---------------------------------------------------------------------------
# Format-specific edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_all_formats_produce_bytes(self):
        exp = _exporter()
        for fmt in DataExportFormat:
            result = exp.export_all_data(fmt=fmt)
            assert isinstance(result.data, bytes)
            assert len(result.data) > 0

    def test_workspace_all_formats(self):
        exp = _exporter()
        for fmt in DataExportFormat:
            data = exp.export_workspace("ws-1", fmt=fmt)
            assert isinstance(data, bytes)
            assert len(data) > 0

    def test_export_id_uniqueness(self):
        exp = _exporter()
        ids = {exp.export_all_data().export_id for _ in range(10)}
        assert len(ids) == 10

    def test_options_with_all_flags(self):
        exp = _exporter()
        opts = ExportOptions(
            include_metadata=False,
            include_relationships=False,
            include_attachments=False,
            anonymize=True,
        )
        result = exp.export_all_data(fmt=DataExportFormat.JSON, options=opts)
        payload = json.loads(result.data)
        plans = payload["data"]["plans"]
        first_plan = list(plans.values())[0]
        assert "created_at" not in first_plan
        assert "user_ids" not in first_plan
