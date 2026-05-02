import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_manual_fallback_operations_matrix import (
    PlanManualFallbackOperationsMatrix,
    PlanManualFallbackOperationsRow,
    analyze_plan_manual_fallback_operations_matrix,
    build_plan_manual_fallback_operations_matrix,
    plan_manual_fallback_operations_matrix_to_dict,
    plan_manual_fallback_operations_matrix_to_markdown,
    summarize_plan_manual_fallback_operations_matrix,
)


def test_complete_manual_fallback_row_extracts_operational_fields():
    result = build_plan_manual_fallback_operations_matrix(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Manual order fallback",
                    description=(
                        "When payment automation fails, support follows the manual procedure runbook "
                        "to process orders from the manual queue."
                    ),
                    files_or_modules=["runbooks/manual/order_fallback.md"],
                    acceptance_criteria=[
                        "Operations DRI owns execution during the incident.",
                        "Required tools include admin console access and the CRM ticket queue.",
                        "After recovery, reconcile manual orders against the ledger audit trail.",
                        "Customer communication uses a status page update and support macro.",
                    ],
                    metadata={"owner": "operations DRI"},
                )
            ]
        )
    )

    assert isinstance(result, PlanManualFallbackOperationsMatrix)
    assert result.fallback_task_ids == ("task-complete",)
    row = result.rows[0]
    assert isinstance(row, PlanManualFallbackOperationsRow)
    assert row.fallback_signals == (
        "manual_processing",
        "manual_reconciliation",
        "customer_communication",
    )
    assert row.fallback_trigger.startswith("When payment automation fails")
    assert row.manual_procedure.startswith("When payment automation fails")
    assert row.responsible_owner == "operations DRI"
    assert row.required_tools_or_access.startswith("Required tools include admin console access")
    assert row.reconciliation_step.startswith("After recovery, reconcile manual orders")
    assert row.customer_communication.startswith("Customer communication uses a status page")
    assert row.missing_fields == ()
    assert row.recommendations == ()
    assert row.readiness_level == "ready"
    assert "files_or_modules: runbooks/manual/order_fallback.md" in row.evidence
    assert result.summary["readiness_counts"] == {"incomplete": 0, "partial": 0, "ready": 1}


def test_missing_owner_procedure_access_and_reconciliation_generate_recommendations():
    result = build_plan_manual_fallback_operations_matrix(
        _plan(
            [
                _task(
                    "task-missing",
                    title="Support-assisted override",
                    description="If the eligibility integration fails, support-assisted override is available.",
                    acceptance_criteria=["Notify customers by email when the override delays activation."],
                )
            ]
        )
    )

    row = result.rows[0]

    assert row.fallback_signals == ("support_override", "customer_communication")
    assert row.fallback_trigger.startswith("If the eligibility integration fails")
    assert row.customer_communication.startswith("Notify customers by email")
    assert row.missing_fields == (
        "manual_procedure",
        "responsible_owner",
        "required_tools_or_access",
        "reconciliation_step",
    )
    assert row.recommendations == (
        "Document the manual fallback procedure as ordered operator steps or a runbook.",
        "Assign a responsible owner or DRI for executing the manual fallback.",
        "List the tools, queues, credentials, roles, or break-glass access needed.",
        "Define how manually processed work is reconciled after automation recovers.",
    )
    assert row.readiness_level == "incomplete"
    assert result.summary["missing_field_counts"]["responsible_owner"] == 1
    assert result.summary["missing_field_counts"]["manual_procedure"] == 1
    assert result.summary["missing_field_counts"]["required_tools_or_access"] == 1
    assert result.summary["missing_field_counts"]["reconciliation_step"] == 1


def test_dependencies_risks_and_metadata_fields_are_scanned_without_mutation():
    plan = _plan(
        [
            _task(
                "task-meta",
                title="Rollback to manual fulfillment | warehouse",
                description="Switch to manual fulfillment when automation fails.",
                depends_on=["Break-glass access request is approved."],
                risks=["Offline spreadsheet can drift from inventory."],
                metadata={
                    "fallback_trigger": "Automation error rate exceeds 5 percent for 10 minutes.",
                    "manual_procedure": "Warehouse lead follows offline CSV import checklist.",
                    "responsible_owner": "warehouse ops lead",
                    "required_tools_or_access": "Break-glass admin role and shared spreadsheet.",
                    "reconciliation_step": "True-up inventory and replay accepted orders after recovery.",
                    "customer_communication": "Support macro explains delayed shipment updates.",
                },
            ),
            _task(
                "task-copy",
                title="Update warehouse copy",
                description="Adjust labels only.",
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_plan_manual_fallback_operations_matrix(plan)
    payload = plan_manual_fallback_operations_matrix_to_dict(result)
    model_plan = copy.deepcopy(plan)
    model_plan["tasks"][0].pop("risks")
    model = ExecutionPlan.model_validate(model_plan)

    assert plan == original
    assert result.fallback_task_ids == ("task-meta",)
    assert result.non_fallback_task_ids == ("task-copy",)
    row = result.rows[0]
    assert row.fallback_signals == (
        "offline_procedure",
        "break_glass_access",
        "manual_reconciliation",
        "customer_communication",
        "manual_rollback",
    )
    assert row.fallback_trigger == "Automation error rate exceeds 5 percent for 10 minutes."
    assert row.manual_procedure == "Warehouse lead follows offline CSV import checklist."
    assert row.required_tools_or_access == "Break-glass admin role and shared spreadsheet."
    assert any("depends_on[0]" in item for item in row.evidence)
    assert any("risks[0]" in item for item in row.evidence)
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["rows"]
    assert analyze_plan_manual_fallback_operations_matrix(plan).to_dict() == summarize_plan_manual_fallback_operations_matrix(plan).to_dict()
    assert summarize_plan_manual_fallback_operations_matrix(model).plan_id == "plan-fallback"


def test_no_signal_plan_returns_empty_deterministic_matrix():
    result = build_plan_manual_fallback_operations_matrix(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Adjust labels and helper text.",
                )
            ]
        )
    )

    assert result.rows == ()
    assert result.fallback_task_ids == ()
    assert result.non_fallback_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "fallback_task_count": 0,
        "fallback_task_ids": [],
        "non_fallback_task_ids": ["task-copy"],
        "missing_field_count": 0,
        "readiness_counts": {"incomplete": 0, "partial": 0, "ready": 0},
        "missing_field_counts": {
            "fallback_trigger": 0,
            "manual_procedure": 0,
            "responsible_owner": 0,
            "required_tools_or_access": 0,
            "reconciliation_step": 0,
            "customer_communication": 0,
        },
        "signal_counts": {
            "manual_processing": 0,
            "support_override": 0,
            "offline_procedure": 0,
            "break_glass_access": 0,
            "manual_reconciliation": 0,
            "customer_communication": 0,
            "manual_rollback": 0,
        },
    }
    assert result.to_markdown() == (
        "# Plan Manual Fallback Operations Matrix: plan-fallback\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Fallback task count: 0\n"
        "- Missing field count: 0\n"
        "- Readiness counts: incomplete 0, partial 0, ready 0\n"
        "- Fallback task ids: none\n"
        "- Non-fallback task ids: task-copy\n"
        "\n"
        "No manual fallback operations tasks were detected."
    )


def test_markdown_escapes_pipes_and_dict_key_order_is_stable():
    result = build_plan_manual_fallback_operations_matrix(
        _plan(
            [
                _task(
                    "task-pipe",
                    title="Manual fallback | billing",
                    description="When billing automation fails, rollback to manual processing.",
                    metadata={
                        "owner": "ops | billing",
                        "manual_procedure": "Run billing SOP.",
                        "required_tools_or_access": "Admin | billing console.",
                        "reconciliation_step": "Reconcile invoices after recovery.",
                        "customer_communication": "Customer notice banner.",
                    },
                )
            ]
        )
    )

    payload = result.to_dict()
    markdown = plan_manual_fallback_operations_matrix_to_markdown(result)

    assert list(payload) == [
        "plan_id",
        "rows",
        "fallback_task_ids",
        "non_fallback_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "fallback_signals",
        "fallback_trigger",
        "manual_procedure",
        "responsible_owner",
        "required_tools_or_access",
        "reconciliation_step",
        "customer_communication",
        "missing_fields",
        "recommendations",
        "readiness_level",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert "Manual fallback \\| billing" in markdown
    assert "ops \\| billing" in markdown
    assert "Admin \\| billing console." in markdown


def _plan(tasks):
    return {
        "id": "plan-fallback",
        "implementation_brief_id": "brief-fallback",
        "milestones": [],
        "tasks": tasks,
        "metadata": {},
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    depends_on=None,
    risks=None,
    metadata=None,
):
    payload = {
        "id": task_id,
        "title": title,
        "description": description,
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "depends_on": depends_on or [],
        "risk_level": "medium",
        "status": "pending",
        "metadata": metadata or {},
    }
    if risks is not None:
        payload["risks"] = risks
    return payload
