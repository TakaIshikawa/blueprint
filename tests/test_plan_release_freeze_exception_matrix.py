import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_release_freeze_exception_matrix import (
    PlanReleaseFreezeExceptionMatrix,
    PlanReleaseFreezeExceptionRow,
    build_plan_release_freeze_exception_matrix,
    extract_plan_release_freeze_exception_matrix,
    plan_release_freeze_exception_matrix_to_dict,
    plan_release_freeze_exception_matrix_to_dicts,
    plan_release_freeze_exception_matrix_to_markdown,
    summarize_plan_release_freeze_exception_matrix,
)


def test_freeze_language_in_plan_and_task_fields_creates_exception_rows():
    result = build_plan_release_freeze_exception_matrix(
        _plan(
            [
                _task(
                    "task-security",
                    title="Ship auth bypass patch",
                    description="Must ship during the release freeze for a CVE security fix.",
                    acceptance_criteria=[
                        "Rollback plan restores the old auth policy.",
                        "Blast radius is limited to admin login and smoke test passes.",
                    ],
                    metadata={"approved_by": "Rina Patel"},
                ),
                _task(
                    "task-incident",
                    title="Deploy queue hotfix",
                    description="Emergency change for a production incident and customer outage.",
                    acceptance_criteria=["Notify support and status page owners."],
                ),
                _task(
                    "task-copy",
                    title="Refresh admin copy",
                    description="Update labels for the admin dashboard.",
                ),
            ],
            release_plan="Holiday freeze is active through the week.",
        )
    )

    assert isinstance(result, PlanReleaseFreezeExceptionMatrix)
    assert all(isinstance(row, PlanReleaseFreezeExceptionRow) for row in result.rows)
    assert result.plan_id == "plan-freeze"
    assert result.not_applicable_task_ids == ("task-copy",)
    assert [row.task_id for row in result.rows] == ["task-incident", "task-security"]

    incident = _row(result, "task-incident")
    assert incident.exception_reasons == ("production_incident",)
    assert incident.present_controls == ("communication_plan",)
    assert incident.risk_level == "high"
    assert "approver_named" in incident.missing_controls
    assert "rollback_plan" in incident.missing_controls

    security = _row(result, "task-security")
    assert security.exception_reasons == ("security_fix",)
    assert security.present_controls == (
        "approver_named",
        "rollback_plan",
        "blast_radius",
        "validation_command",
    )
    assert security.approver_evidence == ("metadata.approved_by: Rina Patel",)
    assert security.risk_level == "medium"
    assert any("Holiday freeze" in evidence for evidence in security.exception_evidence)


def test_full_control_exception_is_low_risk_and_summarized():
    result = build_plan_release_freeze_exception_matrix(
        _plan(
            [
                _task(
                    "task-data",
                    title="Release reconciliation fix",
                    description=(
                        "Freeze exception to release during change moratorium for data integrity "
                        "reconciliation before the compliance deadline."
                    ),
                    test_command="poetry run pytest tests/test_reconcile.py",
                    metadata={"freeze_exception_approver": "CAB - Mei Tan"},
                    acceptance_criteria=[
                        "Rollback plan restores the previous reconciliation job.",
                        "Blast radius limited to 5% of accounts.",
                        "Communication plan notifies support and customer success.",
                        "Timebox within 2 hours.",
                        "Post-release review is scheduled next business day.",
                    ],
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.exception_reasons == ("compliance_deadline", "data_integrity")
    assert row.present_controls == (
        "approver_named",
        "rollback_plan",
        "blast_radius",
        "validation_command",
        "communication_plan",
        "timebox",
        "post_release_review",
    )
    assert row.missing_controls == ()
    assert row.risk_level == "low"
    assert result.summary == {
        "task_count": 1,
        "exception_count": 1,
        "reason_counts": {
            "security_fix": 0,
            "production_incident": 0,
            "compliance_deadline": 1,
            "customer_commitment": 0,
            "data_integrity": 1,
            "operational_unblock": 0,
        },
        "risk_counts": {"high": 0, "medium": 0, "low": 1},
        "missing_control_counts": {
            "approver_named": 0,
            "rollback_plan": 0,
            "blast_radius": 0,
            "validation_command": 0,
            "communication_plan": 0,
            "timebox": 0,
            "post_release_review": 0,
        },
    }


def test_unrelated_tasks_are_not_applicable_and_empty_output_is_stable():
    result = build_plan_release_freeze_exception_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    title="Improve account search",
                    description="Tune pagination and cache behavior.",
                )
            ],
            plan_id="empty-freeze",
        )
    )
    invalid = build_plan_release_freeze_exception_matrix(17)

    assert result.rows == ()
    assert result.not_applicable_task_ids == ("task-api",)
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Release Freeze Exception Matrix: empty-freeze",
            "",
            "Summary: 0 of 1 tasks require freeze exceptions (high: 0, medium: 0, low: 0).",
            "",
            "No release-freeze exception tasks were inferred.",
            "",
            "Not applicable tasks: task-api",
        ]
    )
    assert invalid.plan_id is None
    assert invalid.rows == ()
    assert invalid.summary["exception_count"] == 0


def test_serialization_aliases_markdown_escaping_sorted_output_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-zeta | ops",
                title="Deploy ops unblock | waiver",
                description="Must deploy during release freeze to unblock support escalation.",
                metadata={"approver": "Sam Rivera"},
                acceptance_criteria=["Rollback plan is documented."],
            ),
            _task(
                "task-alpha",
                title="Ship customer contract hotfix",
                description=(
                    "Freeze exception to ship for a customer commitment and contractual deadline."
                ),
            ),
        ],
        metadata={"release_plan": "Release freeze remains active."},
    )
    original = copy.deepcopy(plan)

    result = build_plan_release_freeze_exception_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_release_freeze_exception_matrix_to_dict(result)
    markdown = plan_release_freeze_exception_matrix_to_markdown(result)

    assert plan == original
    assert extract_plan_release_freeze_exception_matrix(plan).to_dict() == result.to_dict()
    assert summarize_plan_release_freeze_exception_matrix(result) == result.summary
    assert plan_release_freeze_exception_matrix_to_dicts(result) == payload["rows"]
    assert plan_release_freeze_exception_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "records", "not_applicable_task_ids", "summary"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "task_title",
        "exception_reasons",
        "present_controls",
        "missing_controls",
        "approver_evidence",
        "risk_level",
        "exception_evidence",
    ]
    assert [(row.task_id, row.risk_level) for row in result.rows] == [
        ("task-alpha", "high"),
        ("task-zeta | ops", "medium"),
    ]
    assert markdown.startswith("# Plan Release Freeze Exception Matrix: plan-freeze")
    assert "| Task | Title | Reasons | Present Controls | Missing Controls | Approver Evidence | Risk | Evidence |" in markdown
    assert "`task-zeta \\| ops`" in markdown
    assert "Deploy ops unblock \\| waiver" in markdown


def _row(result, task_id):
    return next(row for row in result.rows if row.task_id == task_id)


def _plan(tasks, *, plan_id="plan-freeze", release_plan=None, metadata=None):
    plan = {
        "id": plan_id,
        "implementation_brief_id": "brief-freeze",
        "milestones": [],
        "tasks": tasks,
    }
    if release_plan is not None:
        plan["release_plan"] = release_plan
    if metadata is not None:
        plan["metadata"] = metadata
    return plan


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    metadata=None,
    test_command=None,
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
    if test_command is not None:
        task["test_command"] = test_command
    return task
