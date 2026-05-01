import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_data_retention_impact_matrix import (
    PlanDataRetentionImpactMatrix,
    PlanDataRetentionImpactMatrixRow,
    build_plan_data_retention_impact_matrix,
    plan_data_retention_impact_matrix_to_dict,
    plan_data_retention_impact_matrix_to_markdown,
    summarize_plan_data_retention_impact_matrix,
)


def test_lifecycle_domains_and_data_classes_are_detected():
    result = build_plan_data_retention_impact_matrix(
        _plan(
            [
                _task(
                    "task-delete",
                    title="Implement account deletion and purge job",
                    description=(
                        "Delete user data and PII, then run a hard delete purge job after the "
                        "retention period expires."
                    ),
                    files_or_modules=[
                        "src/users/deletion/account_delete.py",
                        "src/db/purge/expired_user_records.py",
                    ],
                    acceptance_criteria=[
                        "Deletion is verified across primary database records and exports."
                    ],
                ),
                _task(
                    "task-archive-backup",
                    title="Archive reports and backup restore handling",
                    description="Move old customer report exports to cold archive and test backup restore windows.",
                    files_or_modules=[
                        "src/reports/archive/customer_exports.py",
                        "ops/backups/restore_drill.py",
                    ],
                ),
                _task(
                    "task-audit-analytics-legal",
                    title="Audit log analytics history legal hold",
                    description=(
                        "Retain audit logs, anonymize analytics history for deleted users, "
                        "and exclude legal hold records from purge."
                    ),
                    metadata={
                        "legal_hold": "Litigation hold release must preserve audit evidence."
                    },
                ),
                _task(
                    "task-copy",
                    title="Update help copy",
                    description="Clarify dashboard labels.",
                ),
            ]
        )
    )

    assert isinstance(result, PlanDataRetentionImpactMatrix)
    assert result.plan_id == "plan-retention"
    assert result.impacted_task_ids == (
        "task-delete",
        "task-audit-analytics-legal",
        "task-archive-backup",
    )
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary["task_count"] == 4
    assert result.summary["impacted_task_count"] == 3
    assert result.summary["severity_counts"] == {"high": 7, "medium": 4, "low": 0}

    domains = {row.retention_domain for row in result.rows}
    assert domains >= {
        "deletion",
        "purge",
        "archive",
        "backup",
        "audit_log",
        "analytics_history",
        "legal_hold",
        "retention_period",
        "user_data_lifecycle",
    }
    assert result.summary["retention_domain_counts"]["deletion"] >= 1
    assert result.summary["retention_domain_counts"]["purge"] >= 1
    assert result.summary["retention_domain_counts"]["legal_hold"] == 1
    assert result.summary["retention_domain_counts"]["analytics_history"] == 1

    delete = next(row for row in result.rows if row.retention_domain == "deletion")
    assert isinstance(delete, PlanDataRetentionImpactMatrixRow)
    assert delete.task_id == "task-delete"
    assert delete.impacted_data_class == "personal_data"
    assert delete.severity == "high"
    assert any("soft-delete, hard-delete" in item for item in delete.required_decisions)
    assert any("search indexes" in item for item in delete.validation_recommendations)
    assert any("title: Implement account deletion" in item for item in delete.evidence)
    assert any(
        "files_or_modules: src/users/deletion/account_delete.py" == item for item in delete.evidence
    )


def test_serialization_markdown_alias_model_input_and_deterministic_ordering():
    plan = _plan(
        [
            _task(
                "task-lowish",
                title="Audit log | export cleanup",
                description="Add audit log retention for exported report files.",
                files_or_modules=["src/audit/export_retention.py", "src/audit/export_retention.py"],
            ),
            _task(
                "task-high",
                title="Purge user account",
                description="Purge deleted user account records and PII after retention period.",
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_plan_data_retention_impact_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_data_retention_impact_matrix_to_dict(result)
    markdown = plan_data_retention_impact_matrix_to_markdown(result)

    assert plan == original
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert [row.task_id for row in result.rows][:3] == ["task-high", "task-high", "task-high"]
    assert [row.severity for row in result.rows][:3] == ["high", "high", "high"]
    assert list(payload) == [
        "plan_id",
        "rows",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "impacted_data_class",
        "retention_domain",
        "required_decisions",
        "validation_recommendations",
        "severity",
        "evidence",
    ]
    assert len(result.rows[0].evidence) == len(set(result.rows[0].evidence))
    assert markdown.startswith("# Plan Data Retention Impact Matrix: plan-retention")
    assert "Summary: " in markdown
    assert "Audit log \\| export cleanup" in markdown


def test_no_impact_empty_invalid_execution_task_and_object_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Configure analytics history retention",
        description="Set event history TTL and anonymize user data analytics after account deletion.",
        files_or_modules=["src/analytics/history_retention.py"],
        metadata={"policy": "Retain for 90 days."},
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Backup deletion restore drill",
            description="Verify backups do not reintroduce deleted customer records.",
            files_or_modules=["ops/backup_restore.py"],
        )
    )

    object_result = build_plan_data_retention_impact_matrix([object_task])
    task_result = build_plan_data_retention_impact_matrix(task_model)
    no_impact = build_plan_data_retention_impact_matrix(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Document onboarding",
                    description="Clarify support handoff text.",
                )
            ]
        )
    )
    empty = build_plan_data_retention_impact_matrix({"id": "empty-plan", "tasks": []})
    invalid = build_plan_data_retention_impact_matrix(19)

    assert object_result.impacted_task_ids == ("task-object",)
    assert {"analytics_history", "retention_period", "user_data_lifecycle"} <= {
        row.retention_domain for row in object_result.rows
    }
    assert task_result.plan_id is None
    assert task_result.rows[0].task_id == "task-model"
    assert task_result.summary["severity_counts"]["high"] >= 1
    assert no_impact.rows == ()
    assert no_impact.no_impact_task_ids == ("task-docs",)
    assert "No data retention lifecycle impacts were detected." in no_impact.to_markdown()
    assert "No-impact tasks: task-docs" in no_impact.to_markdown()
    assert empty.plan_id == "empty-plan"
    assert empty.summary["task_count"] == 0
    assert invalid.plan_id is None
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-retention"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-retention",
        "milestones": [],
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
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
