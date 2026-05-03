import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_production_access_review_matrix import (
    PlanProductionAccessReviewMatrix,
    PlanProductionAccessReviewRow,
    build_plan_production_access_review_matrix,
    derive_plan_production_access_review_matrix,
    extract_plan_production_access_review_matrix,
    generate_plan_production_access_review_matrix,
    plan_production_access_review_matrix_to_dict,
    plan_production_access_review_matrix_to_dicts,
    plan_production_access_review_matrix_to_markdown,
    summarize_plan_production_access_review_matrix,
)


def test_production_access_tasks_create_review_rows_and_control_classification():
    result = build_plan_production_access_review_matrix(
        _plan(
            [
                _task(
                    "task-admin",
                    title="Grant admin console permissions",
                    description=(
                        "Grant admin console permissions for production access to resolve tenant "
                        "configuration issues."
                    ),
                    acceptance_criteria=[
                        "Reviewer: Priya Shah validates least privilege role scope.",
                        "Approval from CAB-123 is recorded before access.",
                        "Audit log evidence is available in CloudTrail.",
                        "Revoke access after rollout and quarterly access review is scheduled.",
                    ],
                ),
                _task(
                    "task-break-glass",
                    title="Enable break-glass database admin role",
                    description=(
                        "Create break-glass privileged role for prod access during incidents."
                    ),
                    acceptance_criteria=["Audit logs are sent to SIEM."],
                ),
                _task(
                    "task-docs",
                    title="Refresh onboarding guide",
                    description="Update internal docs for support onboarding.",
                ),
            ]
        )
    )

    assert isinstance(result, PlanProductionAccessReviewMatrix)
    assert all(isinstance(row, PlanProductionAccessReviewRow) for row in result.rows)
    assert result.plan_id == "plan-access"
    assert result.not_applicable_task_ids == ("task-docs",)
    assert [row.task_id for row in result.rows] == ["task-break-glass", "task-admin"]

    break_glass = _row(result, "task-break-glass")
    assert break_glass.access_signals == (
        "production_access",
        "privileged_role",
        "break_glass_access",
    )
    assert break_glass.present_controls == ("audit_log_evidence",)
    assert "approval_evidence" in break_glass.missing_controls
    assert "break_glass_handling" in break_glass.missing_controls
    assert "revocation_path" in break_glass.missing_controls
    assert break_glass.audit_log_evidence == ("acceptance_criteria[0]: Audit logs are sent to SIEM.",)
    assert break_glass.risk_level == "high"

    admin = _row(result, "task-admin")
    assert admin.access_signals == ("production_access", "admin_console_permission")
    assert admin.present_controls == (
        "reviewer_named",
        "privileged_role_scope",
        "approval_evidence",
        "audit_log_evidence",
        "revocation_path",
        "review_frequency",
    )
    assert admin.missing_controls == ("break_glass_handling",)
    assert admin.reviewer_evidence == (
        "acceptance_criteria[0]: Reviewer: Priya Shah validates least privilege role scope.",
    )
    assert admin.approval_evidence == (
        "acceptance_criteria[1]: Approval from CAB-123 is recorded before access.",
    )
    assert admin.review_frequency == "quarterly"
    assert admin.risk_level == "medium"


def test_full_control_privileged_access_is_low_risk_and_summary_counts_are_stable():
    result = build_plan_production_access_review_matrix(
        _plan(
            [
                _task(
                    "task-owner",
                    title="Provision production owner role",
                    description=(
                        "Provision a privileged role for production account access with break-glass "
                        "runbook handling."
                    ),
                    metadata={
                        "access_reviewer": "Mina Rao",
                        "privileged_role_scope": "Scoped role limited to billing reconciliation.",
                        "approval_ticket": "SEC-991 approved by Jorge Kim",
                        "audit_log": "CloudTrail audit log evidence retained.",
                        "revocation_path": "Revoke access through IAM group removal.",
                        "review_cadence": "Monthly access review",
                    },
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.present_controls == (
        "reviewer_named",
        "privileged_role_scope",
        "approval_evidence",
        "break_glass_handling",
        "audit_log_evidence",
        "revocation_path",
        "review_frequency",
    )
    assert row.missing_controls == ()
    assert row.review_frequency == "monthly"
    assert row.risk_level == "low"
    assert result.summary == {
        "task_count": 1,
        "access_task_count": 1,
        "not_applicable_task_count": 0,
        "signal_counts": {
            "production_access": 1,
            "privileged_role": 1,
            "break_glass_access": 1,
            "admin_console_permission": 0,
        },
        "risk_counts": {"high": 0, "medium": 0, "low": 1},
        "missing_control_counts": {
            "reviewer_named": 0,
            "privileged_role_scope": 0,
            "approval_evidence": 0,
            "break_glass_handling": 0,
            "audit_log_evidence": 0,
            "revocation_path": 0,
            "review_frequency": 0,
        },
    }


def test_unrelated_invalid_and_empty_inputs_render_deterministic_empty_outputs():
    empty = build_plan_production_access_review_matrix({"id": "empty-access", "tasks": []})
    invalid = build_plan_production_access_review_matrix(17)
    no_signal = build_plan_production_access_review_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    title="Optimize search pagination",
                    description="Tune backend query limits for account search.",
                )
            ],
            plan_id="no-access",
        )
    )

    assert empty.to_dict() == {
        "plan_id": "empty-access",
        "rows": [],
        "records": [],
        "not_applicable_task_ids": [],
        "summary": {
            "task_count": 0,
            "access_task_count": 0,
            "not_applicable_task_count": 0,
            "signal_counts": {
                "production_access": 0,
                "privileged_role": 0,
                "break_glass_access": 0,
                "admin_console_permission": 0,
            },
            "risk_counts": {"high": 0, "medium": 0, "low": 0},
            "missing_control_counts": {
                "reviewer_named": 0,
                "privileged_role_scope": 0,
                "approval_evidence": 0,
                "break_glass_handling": 0,
                "audit_log_evidence": 0,
                "revocation_path": 0,
                "review_frequency": 0,
            },
        },
    }
    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Production Access Review Matrix: empty-access",
            "",
            "Summary: 0 of 0 tasks require production access review (high: 0, medium: 0, low: 0).",
            "",
            "No production access review rows were inferred.",
        ]
    )
    assert invalid.plan_id is None
    assert invalid.rows == ()
    assert invalid.summary["access_task_count"] == 0
    assert no_signal.rows == ()
    assert no_signal.not_applicable_task_ids == ("task-api",)
    assert "Not applicable tasks: task-api" in no_signal.to_markdown()


def test_serialization_aliases_model_input_markdown_escaping_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-admin | console",
                title="Admin console | permissions",
                description="Admin console access for production support.",
                metadata={
                    "access_reviewer": "Security Team",
                    "approval_ticket": "SEC-1",
                    "audit_log": "Audit trail in SIEM",
                    "revocation_path": "Remove role after 24 hours",
                },
            ),
            _task(
                "task-role",
                title="Privileged database admin",
                description="Privileged role for prod access.",
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_production_access_review_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_production_access_review_matrix_to_dict(result)
    markdown = plan_production_access_review_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_production_access_review_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_production_access_review_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_production_access_review_matrix(plan).to_dict() == result.to_dict()
    assert summarize_plan_production_access_review_matrix(result) == result.summary
    assert plan_production_access_review_matrix_to_dicts(result) == payload["rows"]
    assert plan_production_access_review_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "records", "not_applicable_task_ids", "summary"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "task_title",
        "access_signals",
        "present_controls",
        "missing_controls",
        "reviewer_evidence",
        "approval_evidence",
        "audit_log_evidence",
        "revocation_evidence",
        "review_frequency",
        "risk_level",
        "access_evidence",
    ]
    assert [(row.task_id, row.risk_level) for row in result.rows] == [
        ("task-role", "high"),
        ("task-admin | console", "high"),
    ]
    assert markdown.startswith("# Plan Production Access Review Matrix: plan-access")
    assert (
        "| Task | Title | Signals | Present Controls | Missing Controls | Reviewer | Approval | "
        "Audit Logs | Revocation | Cadence | Risk | Evidence |"
    ) in markdown
    assert "`task-admin \\| console`" in markdown
    assert "Admin console \\| permissions" in markdown
    assert plan_production_access_review_matrix_to_markdown(result) == result.to_markdown()


def _row(result, task_id):
    return next(row for row in result.rows if row.task_id == task_id)


def _plan(tasks, *, plan_id="plan-access"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-access",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
