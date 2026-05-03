import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_billing_reconciliation_readiness_matrix import (
    PlanBillingReconciliationReadinessMatrix,
    PlanBillingReconciliationReadinessRow,
    analyze_plan_billing_reconciliation_readiness_matrix,
    build_plan_billing_reconciliation_readiness_matrix,
    derive_plan_billing_reconciliation_readiness_matrix,
    extract_plan_billing_reconciliation_readiness_matrix,
    generate_plan_billing_reconciliation_readiness_matrix,
    plan_billing_reconciliation_readiness_matrix_to_dict,
    plan_billing_reconciliation_readiness_matrix_to_dicts,
    plan_billing_reconciliation_readiness_matrix_to_markdown,
    summarize_plan_billing_reconciliation_readiness_matrix,
)


def test_groups_related_billing_reconciliation_tasks_by_source_flow_and_owner():
    result = build_plan_billing_reconciliation_readiness_matrix(
        _plan(
            [
                _task(
                    "task-stripe-recon",
                    title="Reconcile Stripe payment processor balances",
                    description=(
                        "Compare Stripe payment processor balances with ledger settlement postings "
                        "for finance operations."
                    ),
                    acceptance_criteria=[
                        "Validation report shows matched balances, variance report, and reviewer signoff.",
                    ],
                    metadata={"finance_owner": "Payments Accounting"},
                ),
                _task(
                    "task-stripe-recon-tests",
                    title="Validate Stripe reconciliation totals",
                    description="Add balance checks for payment processor balances before close.",
                    depends_on=["task-stripe-recon"],
                    acceptance_criteria=["Audit evidence stores settlement report tie-out."],
                    metadata={"finance_owner": "Payments Accounting"},
                ),
                _task("task-copy", title="Update checkout copy", description="Adjust button text."),
            ]
        )
    )

    assert isinstance(result, PlanBillingReconciliationReadinessMatrix)
    assert all(isinstance(row, PlanBillingReconciliationReadinessRow) for row in result.rows)
    assert result.plan_id == "plan-billing"
    assert result.billing_reconciliation_task_ids == (
        "task-stripe-recon",
        "task-stripe-recon-tests",
    )
    assert result.no_billing_reconciliation_task_ids == ("task-copy",)
    assert len(result.rows) == 1

    row = result.rows[0]
    assert row.reconciliation_source == "payment_processor"
    assert row.financial_flow == "payment_processor_balances"
    assert row.owner_or_consumer == "payments_accounting"
    assert row.task_ids == ("task-stripe-recon", "task-stripe-recon-tests")
    assert row.readiness == "ready"
    assert row.severity == "low"
    assert row.gaps == ()
    assert any("Stripe payment processor balances" in item for item in row.source_evidence)
    assert any("matched balances" in item for item in row.validation_evidence)
    assert result.summary["severity_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_missing_source_or_owner_blocks_with_high_severity_and_validation_gap_is_partial():
    result = build_plan_billing_reconciliation_readiness_matrix(
        _plan(
            [
                _task(
                    "task-missing-source",
                    title="Reconcile revenue reporting totals",
                    description="Reconcile monthly revenue reporting totals for finance operations; source unclear.",
                    acceptance_criteria=["Audit evidence includes reviewer signoff."],
                ),
                _task(
                    "task-missing-owner",
                    title="Reconcile invoice totals",
                    description="Compare invoice totals from billing records against ledger settlement.",
                    acceptance_criteria=["Validation compares invoice totals to ledger postings."],
                ),
                _task(
                    "task-no-validation",
                    title="Tax total reconciliation",
                    description="Reconcile tax totals from VAT and GST invoices for the tax team.",
                    metadata={"tax_owner": "Tax Ops"},
                ),
            ]
        )
    )

    missing_source = _row(result, "task-missing-source")
    missing_owner = _row(result, "task-missing-owner")
    partial = _row(result, "task-no-validation")

    assert missing_source.reconciliation_source == "missing_source"
    assert missing_source.owner_or_consumer == "finance_operations"
    assert missing_source.readiness == "blocked"
    assert missing_source.severity == "high"
    assert "task-missing-source" in result.missing_source_task_ids
    assert any("Missing reconciliation source" in gap for gap in missing_source.gaps)

    assert missing_owner.reconciliation_source == "invoice_system"
    assert missing_owner.owner_or_consumer == "missing_owner_or_consumer"
    assert missing_owner.readiness == "blocked"
    assert missing_owner.severity == "high"
    assert "task-missing-owner" in result.missing_owner_task_ids

    assert partial.reconciliation_source == "invoice_system"
    assert partial.financial_flow == "tax_total_reconciliation"
    assert partial.owner_or_consumer == "tax_ops"
    assert partial.readiness == "partial"
    assert partial.severity == "medium"
    assert any("Missing validation evidence" in gap for gap in partial.gaps)
    assert result.summary["readiness_counts"] == {"blocked": 2, "partial": 1, "ready": 0}
    assert result.summary["missing_source_task_count"] == 1
    assert result.summary["missing_owner_task_count"] == 1


def test_serialization_aliases_model_input_markdown_invalid_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-refund | 1",
                title="Refund | chargeback reconciliation",
                description="Reconcile refunds and chargebacks from billing records for accounting.",
                acceptance_criteria=[
                    "Validation report compares refunds, reversals, and chargebacks."
                ],
                metadata={"accounting_owner": "Revenue Accounting"},
            ),
            _task("task-docs", title="Docs", description="Refresh help center copy."),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_plan_billing_reconciliation_readiness_matrix(model)
    payload = plan_billing_reconciliation_readiness_matrix_to_dict(result)
    markdown = plan_billing_reconciliation_readiness_matrix_to_markdown(result)

    assert plan == original
    assert (
        generate_plan_billing_reconciliation_readiness_matrix(model).to_dict() == result.to_dict()
    )
    assert analyze_plan_billing_reconciliation_readiness_matrix(result) is result
    assert derive_plan_billing_reconciliation_readiness_matrix(model).to_dict() == result.to_dict()
    assert extract_plan_billing_reconciliation_readiness_matrix(model).to_dict() == result.to_dict()
    assert summarize_plan_billing_reconciliation_readiness_matrix(result) == result.summary
    assert plan_billing_reconciliation_readiness_matrix_to_dicts(result) == payload["rows"]
    assert (
        plan_billing_reconciliation_readiness_matrix_to_dicts(result.records) == payload["records"]
    )
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "billing_reconciliation_task_ids",
        "missing_source_task_ids",
        "missing_owner_task_ids",
        "no_billing_reconciliation_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "reconciliation_source",
        "financial_flow",
        "owner_or_consumer",
        "task_ids",
        "titles",
        "validation_evidence",
        "gaps",
        "readiness",
        "severity",
        "source_evidence",
        "evidence",
    ]
    assert markdown.startswith("# Plan Billing Reconciliation Readiness Matrix: plan-billing")
    assert "Refund \\| chargeback reconciliation" in markdown
    assert "task-refund \\| 1" in markdown
    assert "No billing reconciliation signals: task-docs" in markdown

    empty = build_plan_billing_reconciliation_readiness_matrix({"id": "empty-billing", "tasks": []})
    invalid = build_plan_billing_reconciliation_readiness_matrix(object())
    object_result = build_plan_billing_reconciliation_readiness_matrix(
        SimpleNamespace(
            id="task-object",
            title="Ledger settlement validation",
            description="Compare ledger settlement with settlement reports for accounting.",
            acceptance_criteria=["Audit evidence includes balance checks."],
            metadata={"owner": "Finance Controls"},
        )
    )

    assert empty.to_dict() == {
        "plan_id": "empty-billing",
        "rows": [],
        "records": [],
        "billing_reconciliation_task_ids": [],
        "missing_source_task_ids": [],
        "missing_owner_task_ids": [],
        "no_billing_reconciliation_task_ids": [],
        "summary": {
            "task_count": 0,
            "row_count": 0,
            "billing_reconciliation_task_count": 0,
            "missing_source_task_count": 0,
            "missing_owner_task_count": 0,
            "no_billing_reconciliation_task_count": 0,
            "readiness_counts": {"blocked": 0, "partial": 0, "ready": 0},
            "severity_counts": {"high": 0, "medium": 0, "low": 0},
            "source_counts": {},
            "owner_or_consumer_counts": {},
        },
    }
    assert "No billing reconciliation readiness rows were inferred." in empty.to_markdown()
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0
    assert object_result.rows[0].reconciliation_source == "ledger"
    assert object_result.rows[0].owner_or_consumer == "finance_controls"


def _row(result, task_id):
    return next(row for row in result.rows if task_id in row.task_ids)


def _plan(tasks, *, plan_id="plan-billing"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-billing",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    depends_on=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "depends_on": [] if depends_on is None else depends_on,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
