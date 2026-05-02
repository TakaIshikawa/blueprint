import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_backup_restore_readiness import (
    BackupRestoreReadinessTask,
    TaskBackupRestoreReadinessPlan,
    TaskBackupRestoreReadinessRecord,
    analyze_task_backup_restore_readiness,
    build_task_backup_restore_readiness_plan,
    derive_task_backup_restore_readiness,
    extract_task_backup_restore_readiness,
    generate_task_backup_restore_readiness,
    recommend_task_backup_restore_readiness,
    summarize_task_backup_restore_readiness,
    task_backup_restore_readiness_plan_to_dict,
    task_backup_restore_readiness_plan_to_dicts,
    task_backup_restore_readiness_plan_to_markdown,
    task_backup_restore_readiness_to_dicts,
)


def test_complete_backup_restore_expectations_generate_execution_ready_checklist():
    result = build_task_backup_restore_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Launch customer export storage",
                    description=(
                        "Store customer export files in an S3 bucket with daily backup cadence, restore drill, "
                        "RPO of 15 minutes, RTO of 2 hours, checksum data integrity verification, least "
                        "privilege restore access controls, and an operational runbook."
                    ),
                    files_or_modules=["infra/storage/customer_exports_backup_restore.tf"],
                    validation_commands={
                        "restore": ["poetry run pytest tests/infra/test_customer_export_restore.py"]
                    },
                    metadata={"runbook": "On-call restore procedure includes escalation path and audit log review."},
                )
            ]
        )
    )

    assert isinstance(result, TaskBackupRestoreReadinessPlan)
    assert result.backup_restore_task_ids == ("task-ready",)
    assert result.impacted_task_ids == result.backup_restore_task_ids
    record = result.records[0]
    assert isinstance(record, TaskBackupRestoreReadinessRecord)
    assert record.task_id == "task-ready"
    assert record.affected_data_stores == ("s3", "object_storage")
    assert record.readiness == "ready"
    assert record.missing_expectations == ()
    assert record.present_expectations == (
        "backup_cadence",
        "restore_validation",
        "recovery_point_objective",
        "recovery_time_objective",
        "data_integrity_verification",
        "access_controls",
        "operational_runbook",
    )
    assert record.detected_signals == (
        "backup_restore",
        "data_store",
        "backup_cadence",
        "restore_drill",
        "recovery_point_objective",
        "recovery_time_objective",
        "data_integrity_verification",
        "access_controls",
        "operational_runbook",
    )
    assert _categories(record) == record.present_expectations
    assert all(isinstance(task, BackupRestoreReadinessTask) for task in record.generated_tasks)
    assert all(len(task.acceptance_criteria) == 3 for task in record.generated_tasks)
    assert any("validation_commands: poetry run pytest tests/infra/test_customer_export_restore.py" in item for item in record.evidence)
    assert any("Rationale: detected backup/restore signals" in task.description for task in record.generated_tasks)
    assert result.summary["readiness_counts"] == {"needs_planning": 0, "partial": 0, "ready": 1}
    assert result.summary["generated_task_category_counts"]["operational_runbook"] == 1


def test_partial_storage_plan_identifies_missing_recovery_objectives_validation_and_runbook():
    result = analyze_task_backup_restore_readiness(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Move invoice PDFs to managed storage",
                    description=(
                        "Persist invoice PDFs in object storage. Backups run nightly and backup integrity "
                        "checks compare object counts."
                    ),
                    files_or_modules=["src/storage/invoice_uploads.py"],
                    metadata={"storage": {"provider": "gcs", "bucket": "invoice-archive"}},
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "partial"
    assert record.affected_data_stores == ("gcs", "object_storage")
    assert record.present_expectations == ("backup_cadence", "data_integrity_verification")
    assert record.missing_expectations == (
        "restore_validation",
        "recovery_point_objective",
        "recovery_time_objective",
        "access_controls",
        "operational_runbook",
    )
    by_category = {task.category: task for task in record.generated_tasks}
    assert "restore_validation" in by_category
    assert "recovery_point_objective" in by_category
    assert "recovery_time_objective" in by_category
    assert "access_controls" in by_category
    assert "operational_runbook" in by_category
    assert any("restore drill" in item.lower() for item in by_category["restore_validation"].acceptance_criteria)
    assert any("RPO target" in item for item in by_category["recovery_point_objective"].acceptance_criteria)
    assert any("RTO target" in item for item in by_category["recovery_time_objective"].acceptance_criteria)
    assert any("least-privilege" in item for item in by_category["access_controls"].acceptance_criteria)
    assert any("runbook" in item.lower() for item in by_category["operational_runbook"].acceptance_criteria)
    assert result.summary["missing_expectation_counts"]["restore_validation"] == 1
    assert result.summary["data_store_counts"]["gcs"] == 1


def test_destructive_data_change_without_expectations_needs_complete_restore_planning():
    result = build_task_backup_restore_readiness_plan(
        _plan(
            [
                _task(
                    "task-destructive",
                    title="Purge old workspace records",
                    description="Bulk delete stale PostgreSQL workspace records and purge orphaned uploads.",
                    files_or_modules=["db/migrations/20260503_purge_workspace_records.sql"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "needs_planning"
    assert "destructive_data_change" in record.detected_signals
    assert record.present_expectations == ()
    assert record.missing_expectations == (
        "backup_cadence",
        "restore_validation",
        "recovery_point_objective",
        "recovery_time_objective",
        "data_integrity_verification",
        "access_controls",
        "operational_runbook",
    )


def test_no_relevant_signals_empty_invalid_and_object_model_inputs_are_supported():
    no_match = build_task_backup_restore_readiness_plan(
        _plan(
            [
                _task("task-copy", title="Update onboarding copy", description="Static text only."),
                _task(
                    "task-explicit-none",
                    title="Settings panel polish",
                    description="No backup or restore requirements are in scope for this copy-only UI change.",
                ),
            ]
        )
    )
    empty = build_task_backup_restore_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_backup_restore_readiness_plan(13)
    object_task = SimpleNamespace(
        id="task-object",
        title="Redis restore runbook",
        description="Redis session storage needs a restore runbook and RTO target.",
        files_or_modules=["ops/runbooks/redis_restore.md"],
        acceptance_criteria=["Done."],
        status="pending",
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Backup cadence for profile storage",
            description="Profile storage backups require an hourly backup cadence and restore drill.",
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan([model_task.model_dump(mode="python")], plan_id="plan-model")
    )

    object_result = build_task_backup_restore_readiness_plan([object_task])
    model_result = build_task_backup_restore_readiness_plan(plan_model)

    assert no_match.records == ()
    assert no_match.no_impact_task_ids == ("task-copy", "task-explicit-none")
    assert "No task backup restore readiness records were inferred." in no_match.to_markdown()
    assert "No-impact tasks: task-copy, task-explicit-none" in no_match.to_markdown()
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert invalid.records == ()
    assert object_result.records[0].task_id == "task-object"
    assert "redis" in object_result.records[0].affected_data_stores
    assert model_result.plan_id == "plan-model"
    assert model_result.records[0].task_id == "task-model"


def test_serialization_markdown_aliases_and_no_mutation_are_stable():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Database backup restore ready | accounts",
                description=(
                    "Account database backups have backup cadence, restore drill, RPO, RTO, checksums, "
                    "access controls, and runbook."
                ),
            ),
            _task(
                "task-a",
                title="Storage backups need RPO",
                description="Object storage backups have backup cadence but no recovery point objective yet.",
            ),
            _task("task-copy", title="Update copy", description="Change UI copy."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_backup_restore_readiness(plan)
    payload = task_backup_restore_readiness_plan_to_dict(result)
    markdown = task_backup_restore_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_backup_restore_readiness_plan_to_dicts(result) == payload["records"]
    assert task_backup_restore_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_backup_restore_readiness_to_dicts(result.records) == payload["records"]
    assert extract_task_backup_restore_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_backup_restore_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_backup_restore_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_backup_restore_readiness(plan).to_dict() == result.to_dict()
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.backup_restore_task_ids == ("task-a", "task-z")
    assert result.no_impact_task_ids == ("task-copy",)
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "backup_restore_task_ids",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "affected_data_stores",
        "detected_signals",
        "present_expectations",
        "missing_expectations",
        "generated_tasks",
        "readiness",
        "evidence",
    ]
    assert [record.readiness for record in result.records] == ["partial", "ready"]
    assert markdown.startswith("# Task Backup Restore Readiness: plan-backup-restore")
    assert "Database backup restore ready \\| accounts" in markdown


def _categories(record):
    return tuple(task.category for task in record.generated_tasks)


def _plan(tasks, plan_id="plan-backup-restore"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-backup-restore",
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
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
    validation_commands=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-backup-restore",
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
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
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    return payload
