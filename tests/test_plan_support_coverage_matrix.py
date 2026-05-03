import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_support_coverage_matrix import (
    PlanSupportCoverageMatrix,
    PlanSupportCoverageMatrixRow,
    build_plan_support_coverage_matrix,
    derive_plan_support_coverage_matrix,
    generate_plan_support_coverage_matrix,
    plan_support_coverage_matrix_to_dict,
    plan_support_coverage_matrix_to_markdown,
    summarize_plan_support_coverage_matrix,
)


def test_support_artifacts_create_coverage_rows():
    result = build_plan_support_coverage_matrix(
        _plan(
            [
                _task(
                    "task-support",
                    title="Launch customer-facing billing dashboard",
                    description=(
                        "Customers get a billing dashboard. Support tooling in the agent "
                        "console and ticket routing are ready."
                    ),
                    acceptance_criteria=[
                        "Help center support docs are published.",
                        "Support macros are ready for invoice questions.",
                        "Internal FAQ is documented for support agents.",
                        "Triage owner is Support Billing Queue.",
                        "Launch staffing and watch window are staffed.",
                        "Known issues and workarounds are documented.",
                        "Customer communication handoff and release notes are ready.",
                    ],
                    metadata={"support_docs_owner": "Content Ops"},
                )
            ]
        )
    )

    assert isinstance(result, PlanSupportCoverageMatrix)
    assert result.plan_id == "plan-support-coverage"
    assert [row.coverage_area for row in result.rows] == [
        "support_docs",
        "support_macros",
        "internal_faq",
        "support_tooling",
        "triage_ownership",
        "launch_staffing",
        "known_issues",
        "customer_communication_handoff",
    ]
    assert all(isinstance(row, PlanSupportCoverageMatrixRow) for row in result.rows)
    assert all(row.readiness_status == "ready" for row in result.rows)
    assert result.summary["ready_count"] == 8
    assert result.summary["partial_count"] == 0
    assert result.summary["missing_count"] == 0
    assert result.summary["customer_impacting_task_ids"] == ["task-support"]
    assert _row(result, "support_docs").recommended_owner == "Content Ops"
    assert any(
        "Help center support docs are published" in evidence
        for evidence in _row(result, "support_docs").detected_evidence
    )


def test_customer_impacting_plan_without_support_artifacts_recommends_missing_coverage():
    result = build_plan_support_coverage_matrix(
        _plan(
            [
                _task(
                    "task-profile",
                    title="Release customer-facing profile notification workflow",
                    description="Customers receive a new email notification after profile updates.",
                    acceptance_criteria=["Profile notification sends successfully."],
                )
            ]
        )
    )

    assert [row.coverage_area for row in result.rows] == [
        "support_docs",
        "support_macros",
        "internal_faq",
        "triage_ownership",
        "launch_staffing",
        "known_issues",
        "customer_communication_handoff",
    ]
    assert all(row.readiness_status == "missing" for row in result.rows)
    assert _row(result, "support_docs").missing_coverage_items == (
        "Add help center, support guide, or troubleshooting documentation before launch.",
    )
    assert _row(result, "triage_ownership").recommended_owner == "Support lead"
    assert _row(result, "customer_communication_handoff").affected_task_ids == ("task-profile",)
    assert result.summary["ready_count"] == 0
    assert result.summary["partial_count"] == 0
    assert result.summary["missing_count"] == 7
    assert result.summary["missing_coverage_item_count"] == 7


def test_partial_support_artifacts_are_counted_separately():
    result = build_plan_support_coverage_matrix(
        _plan(
            [
                _task(
                    "task-admin",
                    title="Admin settings rollout",
                    description="Admins receive a new customer-visible settings workflow.",
                    acceptance_criteria=[
                        "Draft support docs for the settings workflow.",
                        "Support macro outline covers likely permission questions.",
                        "Known issues list started for support agents.",
                    ],
                )
            ]
        )
    )

    assert _row(result, "support_docs").readiness_status == "partial"
    assert _row(result, "support_macros").readiness_status == "partial"
    assert _row(result, "known_issues").readiness_status == "partial"
    assert result.summary["partial_count"] == 3
    assert result.summary["missing_count"] == 4
    assert result.summary["ready_count"] == 0


def test_plan_context_milestones_risks_and_acceptance_criteria_are_inspected():
    result = build_plan_support_coverage_matrix(
        _plan(
            [
                _task(
                    "task-rollout",
                    title="Customer settings launch",
                    description="Release settings workflow to customers.",
                )
            ],
            milestones=[
                {
                    "name": "Support prep",
                    "acceptance_criteria": [
                        "Support macros are ready.",
                        "Launch staffing coverage window is staffed.",
                    ],
                }
            ],
            risks=["Known issue workarounds are documented before release."],
            acceptance_criteria=["Customer communication handoff is ready."],
        )
    )

    assert _row(result, "support_macros").readiness_status == "ready"
    assert _row(result, "launch_staffing").readiness_status == "ready"
    assert _row(result, "known_issues").readiness_status == "ready"
    assert _row(result, "customer_communication_handoff").readiness_status == "ready"


def test_markdown_dict_serialization_model_inputs_and_aliases_are_stable():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Customer | support docs rollout",
                    description="Customers can export account data after launch.",
                    acceptance_criteria=["Support docs are ready."],
                )
            ],
            plan_id="plan-pipes",
        )
    )
    task = ExecutionTask.model_validate(
        _task(
            "task-object",
            title="Admin permission notification",
            description="Admins receive customer-visible permission notifications.",
        )
    )

    result = build_plan_support_coverage_matrix(plan)
    generated = generate_plan_support_coverage_matrix(plan)
    derived = derive_plan_support_coverage_matrix(result)
    summarized = summarize_plan_support_coverage_matrix(task)
    payload = plan_support_coverage_matrix_to_dict(result)
    markdown = plan_support_coverage_matrix_to_markdown(result)

    assert generated.to_dict() == result.to_dict()
    assert derived is result
    assert summarized.summary["customer_impacting_task_count"] == 1
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "rows"]
    assert list(payload["rows"][0]) == [
        "coverage_area",
        "detected_evidence",
        "readiness_status",
        "missing_coverage_items",
        "recommended_owner",
        "affected_task_ids",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Plan Support Coverage Matrix: plan-pipes")
    assert "Customer \\| support docs rollout" in markdown


def test_empty_state_and_invalid_tasks_do_not_mutate_input():
    plan = _plan(
        [
            _task(
                "task-internal",
                title="Refactor parser internals",
                description="Simplify internal token handling.",
                files_or_modules=["src/blueprint/parser.py"],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_support_coverage_matrix(plan)
    invalid = build_plan_support_coverage_matrix({"id": "plan-invalid", "tasks": "not a list"})

    assert plan == original
    assert result.rows == ()
    assert result.summary == {
        "total_task_count": 1,
        "customer_impacting_task_count": 0,
        "coverage_area_count": 0,
        "ready_count": 0,
        "partial_count": 0,
        "missing_count": 0,
        "status_counts": {"ready": 0, "partial": 0, "missing": 0},
        "missing_coverage_item_count": 0,
        "customer_impacting_task_ids": [],
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Support Coverage Matrix: plan-support-coverage",
            "",
            "## Summary",
            "",
            "- Total tasks: 1",
            "- Customer-impacting tasks: 0",
            "- Coverage areas: 0",
            "- Ready areas: 0",
            "- Partial areas: 0",
            "- Missing areas: 0",
            "",
            "No support coverage needs were inferred.",
        ]
    )
    assert invalid.rows == ()
    assert invalid.summary["total_task_count"] == 0


def _row(result, area):
    return next(row for row in result.rows if row.coverage_area == area)


def _plan(
    tasks,
    *,
    plan_id="plan-support-coverage",
    metadata=None,
    milestones=None,
    risks=None,
    acceptance_criteria=None,
):
    plan = {
        "id": plan_id,
        "implementation_brief_id": "brief-support-coverage",
        "metadata": {} if metadata is None else metadata,
        "tasks": tasks,
    }
    if milestones is not None:
        plan["milestones"] = milestones
    else:
        plan["milestones"] = []
    if risks is not None:
        plan["risks"] = risks
    if acceptance_criteria is not None:
        plan["acceptance_criteria"] = acceptance_criteria
    return plan


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
    risks=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    if risks is not None:
        task["risks"] = risks
    return task
