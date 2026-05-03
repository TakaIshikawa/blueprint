import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_operational_handoff_readiness_matrix import (
    PlanOperationalHandoffReadinessMatrix,
    PlanOperationalHandoffReadinessRow,
    analyze_plan_operational_handoff_readiness_matrix,
    build_plan_operational_handoff_readiness_matrix,
    derive_plan_operational_handoff_readiness_matrix,
    extract_plan_operational_handoff_readiness_matrix,
    generate_plan_operational_handoff_readiness_matrix,
    plan_operational_handoff_readiness_matrix_to_dict,
    plan_operational_handoff_readiness_matrix_to_dicts,
    plan_operational_handoff_readiness_matrix_to_markdown,
    summarize_plan_operational_handoff_readiness_matrix,
)


def test_handoff_signal_tasks_create_rows_and_artifact_gaps():
    result = build_plan_operational_handoff_readiness_matrix(
        _plan(
            [
                _task(
                    "task-support",
                    title="Launch support handoff for customer escalation",
                    description=(
                        "Transition rollout questions to support and customer success after launch."
                    ),
                    acceptance_criteria=[
                        "Support playbook covers likely tickets.",
                        "Escalation path routes issues to Support Ops.",
                    ],
                    metadata={"handoff_owner": "Support Ops"},
                ),
                _task(
                    "task-oncall",
                    title="Prepare on-call rollback runbook",
                    description="Operations need launch watch and incident response ownership.",
                    acceptance_criteria=["On-call runbook covers rollback steps."],
                    validation_commands=["pytest tests/test_incidents.py"],
                ),
                _task(
                    "task-internal",
                    title="Refactor parser internals",
                    description="Simplify token handling.",
                ),
            ]
        )
    )

    assert isinstance(result, PlanOperationalHandoffReadinessMatrix)
    assert all(isinstance(row, PlanOperationalHandoffReadinessRow) for row in result.rows)
    assert result.plan_id == "plan-handoff"
    assert result.no_handoff_task_ids == ("task-internal",)
    assert [row.task_id for row in result.rows] == ["task-oncall", "task-support"]

    oncall = _row(result, "task-oncall")
    assert oncall.handoff_domains == ("oncall_runbook",)
    assert oncall.required_artifacts == (
        "oncall_runbook",
        "escalation_path",
        "operational_owner",
    )
    assert oncall.present_artifacts == ("oncall_runbook",)
    assert oncall.missing_artifacts == ("escalation_path", "operational_owner")
    assert oncall.readiness_level == "blocked"
    assert oncall.handoff_timing == "before production launch"
    assert "Operations on-call" in oncall.owner_suggestions
    assert "title: Prepare on-call rollback runbook" in oncall.evidence
    assert "acceptance_criteria[0]: On-call runbook covers rollback steps." in oncall.evidence

    support = _row(result, "task-support")
    assert support.handoff_domains == (
        "support_playbook",
        "customer_success_briefing",
    )
    assert support.owner_suggestions[:1] == ("Support Ops",)
    assert support.missing_artifacts == ("customer_success_briefing",)
    assert support.readiness_level == "partial"


def test_finance_security_data_and_vendor_domains_are_detected_from_task_surfaces():
    result = build_plan_operational_handoff_readiness_matrix(
        _plan(
            [
                _task(
                    "task-ops",
                    title="Vendor billing migration handoff",
                    description=(
                        "Coordinate Stripe vendor cutover, invoice reconciliation, security audit, "
                        "and data operations backfill."
                    ),
                    files_or_modules=[
                        "src/billing/invoice_reconciliation.py",
                        "docs/vendors/stripe_handoff.md",
                        "src/security/audit.py",
                        "src/data/backfill_pipeline.py",
                    ],
                    acceptance_criteria=[
                        "Finance operations runbook is complete.",
                        "Security operations runbook is complete.",
                        "Data operations runbook is complete.",
                        "Vendor handoff names the Stripe contact.",
                        "Escalation path and operational owner are documented.",
                    ],
                    metadata={"owner": "Revenue Ops", "handoff_timing": "before Stripe cutover"},
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.handoff_domains == (
        "finance_operations",
        "security_operations",
        "data_operations",
        "vendor_operations",
    )
    assert row.present_artifacts == row.required_artifacts
    assert row.missing_artifacts == ()
    assert row.readiness_level == "ready"
    assert row.handoff_timing == "before Stripe cutover"
    assert row.owner_suggestions == (
        "Revenue Ops",
        "Finance Operations",
        "Security Operations",
        "Data Operations",
        "Vendor Operations",
    )


def test_dependencies_plan_metadata_and_model_input_drive_deterministic_timing_and_owners():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-cs",
                    title="Customer success rollout briefing",
                    description="Brief CSMs on customer-facing release questions.",
                    depends_on=["task-release"],
                    acceptance_criteria=[
                        "Customer success briefing is ready.",
                        "Escalation path routes to Launch Desk.",
                        "Operational owner: Launch Desk.",
                    ],
                )
            ],
            metadata={"support_owner": "Global Support Queue"},
        )
    )

    result = build_plan_operational_handoff_readiness_matrix(plan)
    row = result.rows[0]

    assert row.handoff_domains == (
        "support_playbook",
        "customer_success_briefing",
    )
    assert row.handoff_timing == "after dependencies: task-release"
    assert row.owner_suggestions == (
        "Launch Desk",
        "Global Support Queue",
        "Support operations",
        "Customer Success",
    )
    assert row.readiness_level == "partial"
    assert "support_playbook" in row.missing_artifacts


def test_empty_invalid_serialization_aliases_markdown_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-finance | ops",
                title="Finance | operations handoff",
                description="Finance operations month-end billing reconciliation handoff.",
                acceptance_criteria=[
                    "Finance operations runbook is ready.",
                    "Escalation path is documented.",
                    "Operational owner is Finance Ops.",
                ],
            ),
            _task("task-docs", title="Refresh docs", description="Update README copy."),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_operational_handoff_readiness_matrix(plan)
    payload = plan_operational_handoff_readiness_matrix_to_dict(result)
    markdown = plan_operational_handoff_readiness_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_operational_handoff_readiness_matrix(plan).to_dict() == result.to_dict()
    assert analyze_plan_operational_handoff_readiness_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_operational_handoff_readiness_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_operational_handoff_readiness_matrix(plan).to_dict() == result.to_dict()
    assert summarize_plan_operational_handoff_readiness_matrix(result) == result.summary
    assert plan_operational_handoff_readiness_matrix_to_dicts(result) == payload["rows"]
    assert plan_operational_handoff_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "records", "no_handoff_task_ids", "summary"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "handoff_domains",
        "required_artifacts",
        "present_artifacts",
        "owner_suggestions",
        "missing_artifacts",
        "readiness_level",
        "handoff_timing",
        "evidence",
    ]
    assert markdown.startswith("# Plan Operational Handoff Readiness Matrix: plan-handoff")
    assert (
        "| Task | Title | Domains | Required Artifacts | Present Artifacts | Missing Artifacts | "
        "Owners | Readiness | Timing | Evidence |"
    ) in markdown
    assert "`task-finance \\| ops`" in markdown
    assert "Finance \\| operations handoff" in markdown
    assert "No handoff signals: task-docs" in markdown

    empty = build_plan_operational_handoff_readiness_matrix({"id": "empty-handoff", "tasks": []})
    invalid = build_plan_operational_handoff_readiness_matrix(17)
    assert empty.to_dict() == {
        "plan_id": "empty-handoff",
        "rows": [],
        "records": [],
        "no_handoff_task_ids": [],
        "summary": {
            "task_count": 0,
            "handoff_task_count": 0,
            "no_handoff_task_count": 0,
            "domain_counts": {
                "support_playbook": 0,
                "oncall_runbook": 0,
                "customer_success_briefing": 0,
                "finance_operations": 0,
                "security_operations": 0,
                "data_operations": 0,
                "vendor_operations": 0,
            },
            "readiness_counts": {"blocked": 0, "partial": 0, "ready": 0},
            "missing_artifact_counts": {
                "support_playbook": 0,
                "oncall_runbook": 0,
                "customer_success_briefing": 0,
                "finance_operations_runbook": 0,
                "security_operations_runbook": 0,
                "data_operations_runbook": 0,
                "vendor_operations_handoff": 0,
                "escalation_path": 0,
                "operational_owner": 0,
            },
        },
    }
    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Operational Handoff Readiness Matrix: empty-handoff",
            "",
            "Summary: 0 of 0 tasks require operational handoff (blocked: 0, partial: 0, ready: 0).",
            "",
            "No operational handoff readiness rows were inferred.",
        ]
    )
    assert invalid.plan_id is None
    assert invalid.rows == ()
    assert invalid.summary["handoff_task_count"] == 0


def _row(result, task_id):
    return next(row for row in result.rows if row.task_id == task_id)


def _plan(tasks, *, plan_id="plan-handoff", metadata=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-handoff",
        "milestones": [],
        "metadata": {} if metadata is None else metadata,
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    depends_on=None,
    files_or_modules=None,
    acceptance_criteria=None,
    validation_commands=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [] if depends_on is None else depends_on,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if validation_commands is not None:
        task["validation_commands"] = validation_commands
    if metadata is not None:
        task["metadata"] = metadata
    return task
