import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_database_backup_restore_readiness import (
    TaskDatabaseBackupRestoreReadinessPlan,
    TaskDatabaseBackupRestoreReadinessRecord,
    analyze_task_database_backup_restore_readiness,
    build_task_database_backup_restore_readiness_plan,
    extract_task_database_backup_restore_readiness,
    generate_task_database_backup_restore_readiness,
    summarize_task_database_backup_restore_readiness,
    task_database_backup_restore_readiness_plan_to_dict,
    task_database_backup_restore_readiness_plan_to_markdown,
)


def test_migration_file_requires_backup_restore_and_recovery_safeguards():
    result = build_task_database_backup_restore_readiness_plan(
        _plan(
            [
                _task(
                    "task-migration",
                    title="Add nullable customer status column",
                    description="Run a PostgreSQL schema migration and alter table customer.",
                    files_or_modules=["db/migrations/20260502_add_customer_status.sql"],
                    acceptance_criteria=["Migration applies in staging."],
                )
            ]
        )
    )

    assert isinstance(result, TaskDatabaseBackupRestoreReadinessPlan)
    assert result.database_task_ids == ("task-migration",)
    record = result.records[0]
    assert isinstance(record, TaskDatabaseBackupRestoreReadinessRecord)
    assert record.affected_data_stores == ("postgresql", "database")
    assert record.detected_signals == ("database", "schema_migration", "ddl")
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "backup_verification",
        "restore_rehearsal",
        "point_in_time_recovery",
        "rollback_data_snapshot",
        "owner_evidence",
    )
    assert record.risk_level == "high"
    assert "files_or_modules: db/migrations/20260502_add_customer_status.sql" in record.evidence
    assert result.summary["task_count"] == 1
    assert result.summary["database_task_count"] == 1
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}
    assert result.summary["missing_safeguard_counts"]["restore_rehearsal"] == 1


def test_storage_metadata_and_tags_detect_data_stores_and_partial_safeguards():
    result = analyze_task_database_backup_restore_readiness(
        _plan(
            [
                _task(
                    "task-storage",
                    title="Move generated invoices to object storage",
                    description="Persist invoice PDFs in the upload storage layer.",
                    files_or_modules=["src/storage/invoice_uploads.py"],
                    tags=["s3 bucket", "persistence"],
                    metadata={
                        "storage": {"provider": "s3", "bucket": "invoice-archive"},
                        "backup_verification": "validated snapshot exists before deploy",
                        "data_owner": "billing platform",
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.affected_data_stores == ("s3", "object_storage", "application_database")
    assert record.detected_signals == ("persistence_layer", "storage")
    assert record.present_safeguards == ("backup_verification", "owner_evidence")
    assert record.missing_safeguards == (
        "restore_rehearsal",
        "point_in_time_recovery",
        "rollback_data_snapshot",
    )
    assert record.risk_level == "medium"
    assert any("metadata.storage" in item for item in record.evidence)
    assert result.summary["data_store_counts"]["s3"] == 1


def test_destructive_changes_are_high_risk_without_rehearsed_restore():
    result = build_task_database_backup_restore_readiness_plan(
        _plan(
            [
                _task(
                    "task-purge",
                    title="Purge old sessions",
                    description="Bulk delete stale Redis session records and hard delete expired database rows.",
                    files_or_modules=["src/repositories/session_cleanup.py"],
                    acceptance_criteria=[
                        "A pre-change snapshot is captured for rollback.",
                        "Service owner approval is attached.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.affected_data_stores == ("redis", "application_database", "database")
    assert record.detected_signals == (
        "database",
        "persistence_layer",
        "storage",
        "destructive_data_flow",
    )
    assert record.present_safeguards == ("rollback_data_snapshot", "owner_evidence")
    assert "backup_verification" in record.missing_safeguards
    assert "restore_rehearsal" in record.missing_safeguards
    assert record.risk_level == "high"


def test_fully_covered_backup_restore_safeguards_are_low_risk():
    result = build_task_database_backup_restore_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Contract legacy account columns",
                    description="Drop column after a PostgreSQL migration contract phase.",
                    files_or_modules=["db/migrations/20260502_drop_legacy_account.sql"],
                    acceptance_criteria=[
                        "Backup verification checks current backup integrity.",
                        "Restore rehearsal completes with a test restore in staging.",
                        "Point-in-time recovery is confirmed through WAL archive retention.",
                        "Pre-change snapshot is captured for rollback before execution.",
                        "Database owner approval and DBA sign-off are attached.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.present_safeguards == (
        "backup_verification",
        "restore_rehearsal",
        "point_in_time_recovery",
        "rollback_data_snapshot",
        "owner_evidence",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "low"
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}
    assert result.summary["missing_safeguard_count"] == 0


def test_unrelated_ui_tasks_return_empty_recommendations_and_stable_summary():
    result = build_task_database_backup_restore_readiness_plan(
        _plan(
            [
                _task(
                    "task-ui",
                    title="Update settings button copy",
                    description="Adjust profile UI labels and loading text.",
                    files_or_modules=["src/ui/settings_panel.tsx"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.database_task_ids == ()
    assert result.not_applicable_task_ids == ("task-ui",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "database_task_count": 0,
        "not_applicable_task_ids": ["task-ui"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_safeguard_counts": {
            "backup_verification": 0,
            "restore_rehearsal": 0,
            "point_in_time_recovery": 0,
            "rollback_data_snapshot": 0,
            "owner_evidence": 0,
        },
        "data_store_counts": {},
    }
    assert "No database backup or restore readiness records" in result.to_markdown()


def test_deterministic_serialization_markdown_aliases_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Database migration | restore ready",
                description="Run database migration with backup verification and restore rehearsal.",
                acceptance_criteria=[
                    "PITR target is confirmed.",
                    "Pre-change snapshot is captured.",
                    "Data owner sign-off is attached.",
                ],
            ),
            _task(
                "task-a",
                title="Drop audit table",
                description="Drop table audit_log.",
            ),
            _task("task-copy", title="Update copy", description="Change UI copy."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_database_backup_restore_readiness(plan)
    payload = task_database_backup_restore_readiness_plan_to_dict(result)
    markdown = task_database_backup_restore_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert extract_task_database_backup_restore_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_database_backup_restore_readiness(plan).to_dict() == result.to_dict()
    assert result.database_task_ids == ("task-a", "task-z")
    assert result.not_applicable_task_ids == ("task-copy",)
    assert list(payload) == [
        "plan_id",
        "records",
        "database_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "affected_data_stores",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "recommended_readiness_steps",
    ]
    assert [record.risk_level for record in result.records] == ["high", "low"]
    assert markdown.startswith("# Task Database Backup Restore Readiness: plan-db")
    assert "Database migration \\| restore ready" in markdown


def test_execution_plan_input_is_supported():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Update Prisma model and migration",
                    description="Prisma schema migration for PostgreSQL includes backup verification and restore rehearsal.",
                    files_or_modules=["prisma/migrations/20260502_init/migration.sql", "prisma/schema.prisma"],
                    acceptance_criteria=[
                        "PITR is enabled.",
                        "Rollback snapshot is stored.",
                        "Service owner approval is attached.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )

    result = build_task_database_backup_restore_readiness_plan(model)

    assert result.plan_id == "plan-model"
    assert result.records[0].task_id == "task-model"
    assert result.records[0].risk_level == "low"
    assert "schema_migration" in result.records[0].detected_signals
    assert "orm_model" in result.records[0].detected_signals


def _plan(tasks, plan_id="plan-db"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-db",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-db",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        payload["tags"] = tags
    return payload
