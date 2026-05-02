import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_soft_delete_recovery import (
    TaskSoftDeleteRecoveryPlan,
    TaskSoftDeleteRecoveryRecord,
    analyze_task_soft_delete_recovery,
    build_task_soft_delete_recovery_plan,
    extract_task_soft_delete_recovery,
    generate_task_soft_delete_recovery,
    recommend_task_soft_delete_recovery,
    summarize_task_soft_delete_recovery,
    task_soft_delete_recovery_plan_to_dict,
    task_soft_delete_recovery_plan_to_dicts,
    task_soft_delete_recovery_plan_to_markdown,
)


def test_detects_soft_delete_restore_signals_and_missing_safeguards_from_task_fields():
    result = build_task_soft_delete_recovery_plan(
        _plan(
            [
                _task(
                    "task-trash",
                    title="Add trash restore flow",
                    description=(
                        "Soft delete projects into trash, allow restore and undelete, "
                        "and keep tombstone records for the retention window."
                    ),
                    files_or_modules=[
                        "src/tasks/soft_delete.py",
                        "src/tasks/trash_restore.py",
                    ],
                    acceptance_criteria=[
                        "Archived projects can be restored from the recycle bin.",
                        "Search results exclude deleted records by default.",
                    ],
                    validation_plan="Run API tests for trash restore and list filtering.",
                )
            ]
        )
    )

    assert isinstance(result, TaskSoftDeleteRecoveryPlan)
    assert result.soft_delete_task_ids == ("task-trash",)
    record = result.records[0]
    assert isinstance(record, TaskSoftDeleteRecoveryRecord)
    assert {
        "soft_delete",
        "archive",
        "restore",
        "trash",
        "undelete",
        "tombstone",
        "retention_window",
        "filtering",
    } <= set(record.matched_deletion_signals)
    assert record.present_safeguards == ("search_list_filtering_semantics",)
    assert record.missing_safeguards == (
        "restore_path_validation",
        "uniqueness_with_deleted_records",
        "restore_permanent_delete_authorization",
        "retention_expiry_behavior",
        "audit_trail_coverage",
    )
    assert record.readiness_level == "not_ready"
    assert "Validate restore and undelete paths" in record.recommended_checks[0]
    assert any("description:" in item and "Soft delete projects" in item for item in record.evidence)
    assert "files_or_modules: src/tasks/soft_delete.py" in record.evidence
    assert any("validation_plan:" in item and "list filtering" in item for item in record.evidence)
    assert result.summary["soft_delete_task_count"] == 1
    assert result.summary["signal_counts"]["restore"] == 1
    assert result.summary["missing_safeguard_counts"]["restore_path_validation"] == 1


def test_metadata_acceptance_criteria_and_validation_plan_detect_ready_safeguards():
    result = analyze_task_soft_delete_recovery(
        _plan(
            [
                _task(
                    "task-retention",
                    title="Retention window hard delete cleanup",
                    description="Purge expired soft-deleted tasks after the retention window.",
                    metadata={
                        "soft_delete": {
                            "restore": "Restore path validation covers conflict and already-restored cases.",
                            "authorization": "Admin RBAC permission is required to restore or permanently delete.",
                            "retention_expiry_behavior": "Cleanup job documents retention expiry behavior.",
                            "audit": "Audit trail records deletion event, restore event, and permanent delete event.",
                        }
                    },
                    acceptance_criteria=[
                        "Partial unique index prevents slug conflict with deleted records.",
                        "Search/list filtering semantics exclude deleted records unless include deleted is requested.",
                    ],
                    validation_plan="Validate unique constraint behavior with deleted records and retention expiry.",
                )
            ]
        )
    )

    record = result.records[0]
    assert {"soft_delete", "retention_window", "permanent_delete", "restore", "filtering"} <= set(
        record.matched_deletion_signals
    )
    assert record.present_safeguards == (
        "restore_path_validation",
        "uniqueness_with_deleted_records",
        "restore_permanent_delete_authorization",
        "retention_expiry_behavior",
        "audit_trail_coverage",
        "search_list_filtering_semantics",
    )
    assert record.missing_safeguards == ()
    assert record.readiness_level == "ready"
    assert any("metadata.soft_delete.restore" in item for item in record.evidence)
    assert any("validation_plan:" in item for item in record.evidence)
    assert result.summary["readiness_counts"] == {"not_ready": 0, "needs_review": 0, "ready": 1}


def test_execution_plan_execution_task_single_task_and_non_applicable_handling():
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Archive closed tickets",
            description="Archive closed tickets and hide archived rows from list views.",
            acceptance_criteria=[
                "List filtering semantics exclude archived rows.",
                "Audit log records archive events.",
                "Retention expiry behavior is documented.",
            ],
        )
    )
    single_task = build_task_soft_delete_recovery_plan(model_task)
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-undel",
                    title="Undelete user note",
                    description="Undelete a trashed note with restore path validation.",
                ),
                _task("task-copy", title="Update copy", description="Adjust empty state wording."),
            ]
        )
    )
    plan_result = generate_task_soft_delete_recovery(plan)
    empty = build_task_soft_delete_recovery_plan([])
    noop = build_task_soft_delete_recovery_plan(
        _plan([_task("task-copy", title="Update copy", description="Static text.")])
    )

    assert single_task.plan_id is None
    assert single_task.soft_delete_task_ids == ("task-model",)
    assert single_task.records[0].readiness_level == "needs_review"
    assert plan_result.plan_id == "plan-soft-delete"
    assert plan_result.soft_delete_task_ids == ("task-undel",)
    assert plan_result.not_applicable_task_ids == ("task-copy",)
    assert empty.records == ()
    assert empty.not_applicable_task_ids == ()
    assert noop.records == ()
    assert noop.soft_delete_task_ids == ()
    assert noop.not_applicable_task_ids == ("task-copy",)
    assert noop.summary == {
        "task_count": 1,
        "soft_delete_task_count": 0,
        "not_applicable_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "readiness_counts": {"not_ready": 0, "needs_review": 0, "ready": 0},
        "signal_counts": {
            "soft_delete": 0,
            "archive": 0,
            "restore": 0,
            "trash": 0,
            "undelete": 0,
            "tombstone": 0,
            "retention_window": 0,
            "permanent_delete": 0,
            "filtering": 0,
        },
        "missing_safeguard_counts": {
            "restore_path_validation": 0,
            "uniqueness_with_deleted_records": 0,
            "restore_permanent_delete_authorization": 0,
            "retention_expiry_behavior": 0,
            "audit_trail_coverage": 0,
            "search_list_filtering_semantics": 0,
        },
        "present_safeguard_counts": {
            "restore_path_validation": 0,
            "uniqueness_with_deleted_records": 0,
            "restore_permanent_delete_authorization": 0,
            "retention_expiry_behavior": 0,
            "audit_trail_coverage": 0,
            "search_list_filtering_semantics": 0,
        },
        "soft_delete_task_ids": [],
    }
    assert "No soft-delete recovery readiness records" in noop.to_markdown()
    assert "Not-applicable tasks: task-copy" in noop.to_markdown()


def test_deterministic_serialization_markdown_aliases_sorting_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Restore deleted profile | ready",
                description="Restore a soft-deleted profile.",
                acceptance_criteria=[
                    "Restore path validation covers conflicts.",
                    "Unique constraint handles deleted records.",
                    "Admin RBAC authorizes restore and permanently delete.",
                    "Retention expiry behavior is tested.",
                    "Audit trail records delete and restore events.",
                    "Search/list filtering semantics are verified.",
                ],
            ),
            _task(
                "task-a",
                title="Purge expired records",
                description="Hard delete expired tombstone records after retention.",
            ),
            _task(
                "task-m",
                title="Archive dashboard items",
                description="Archive dashboard items.",
                acceptance_criteria=[
                    "Audit log records archive events.",
                    "List filtering semantics hide archived rows.",
                    "Retention expiry behavior is documented.",
                ],
            ),
            _task("task-copy", title="Copy update", description="Update helper text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_soft_delete_recovery(plan)
    payload = task_soft_delete_recovery_plan_to_dict(result)
    markdown = task_soft_delete_recovery_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_soft_delete_recovery_plan_to_dicts(result) == payload["records"]
    assert task_soft_delete_recovery_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_soft_delete_recovery(plan).to_dict() == result.to_dict()
    assert recommend_task_soft_delete_recovery(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "recommendations",
        "soft_delete_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "matched_deletion_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness_level",
        "recommended_checks",
        "evidence",
    ]
    assert result.soft_delete_task_ids == ("task-a", "task-m", "task-z")
    assert [record.readiness_level for record in result.records] == ["not_ready", "needs_review", "ready"]
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.summary["readiness_counts"] == {"not_ready": 1, "needs_review": 1, "ready": 1}
    assert markdown.startswith("# Task Soft Delete Recovery Readiness: plan-soft-delete")
    assert "Restore deleted profile \\| ready" in markdown
    assert (
        "| Task | Title | Readiness | Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |"
        in markdown
    )


def _plan(tasks, plan_id="plan-soft-delete"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-soft-delete",
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
    validation_plan=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-soft-delete",
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
    if validation_plan is not None:
        payload["validation_plan"] = validation_plan
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
