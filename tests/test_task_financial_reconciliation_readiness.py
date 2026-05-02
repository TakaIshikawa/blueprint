import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_financial_reconciliation_readiness import (
    TaskFinancialReconciliationReadinessPlan,
    TaskFinancialReconciliationReadinessRecord,
    build_task_financial_reconciliation_readiness_plan,
    extract_task_financial_reconciliation_readiness,
    generate_task_financial_reconciliation_readiness,
    summarize_task_financial_reconciliation_readiness,
    task_financial_reconciliation_readiness_plan_to_dict,
    task_financial_reconciliation_readiness_plan_to_markdown,
)


def test_detects_payment_reconciliation_task_with_missing_safeguards():
    result = build_task_financial_reconciliation_readiness_plan(
        _plan(
            [
                _task(
                    "task-payment",
                    title="Add Stripe payment reconciliation job",
                    description="Match checkout payments against settlement reports.",
                    files_or_modules=[
                        "src/payments/reconciliation.py",
                        "src/ledger/postings.py",
                    ],
                    metadata={"finance_owner": "payments accounting"},
                )
            ]
        )
    )

    record = result.records[0]

    assert isinstance(result, TaskFinancialReconciliationReadinessPlan)
    assert isinstance(record, TaskFinancialReconciliationReadinessRecord)
    assert result.financial_task_ids == ("task-payment",)
    assert result.not_applicable_task_ids == ()
    assert record.financial_signals == ("payment", "ledger", "reconciliation")
    assert record.financial_surfaces == (
        "payment_operations",
        "ledger_postings",
        "reconciliation_reporting",
    )
    assert record.detected_safeguards == ("reconciliation_reports",)
    assert record.readiness == "partial"
    assert record.missing_safeguards == (
        "double_entry_ledger_checks",
        "idempotent_payment_operations",
        "audit_evidence",
        "rounding_currency_tests",
        "refund_edge_cases",
        "rollback_manual_adjustments",
    )
    assert record.owner_assumptions == (
        "payments accounting owns financial reconciliation sign-off.",
    )
    assert "title: Add Stripe payment reconciliation job" in record.evidence
    assert "files_or_modules: src/payments/reconciliation.py" in record.evidence


def test_all_financial_signals_and_ready_safeguards_are_detected():
    result = build_task_financial_reconciliation_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Reconcile invoice tax credits refunds and payouts",
                    description=(
                        "Post double-entry ledger journal entries for invoice credits, "
                        "refunds, taxes, and payout settlements."
                    ),
                    files_or_modules=[
                        "src/billing/invoices.py",
                        "src/refunds/refund_processor.py",
                        "src/payouts/settlement.py",
                        "src/tax/calculator.py",
                    ],
                    acceptance_criteria=[
                        "Double-entry ledger checks prove debits equal credits.",
                        "Idempotency keys prevent duplicate charges and duplicate refunds.",
                        "Reconciliation reports compare internal records to settlement reports.",
                        "Audit trail captures approvals and immutable logs for adjustments.",
                        "Rounding, currency, minor-unit, and tax precision tests pass.",
                        "Partial refunds, chargebacks, reversals, and negative balances are covered.",
                        "Rollback runbook documents compensating entries and manual adjustments.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.financial_signals == (
        "payment",
        "invoice",
        "ledger",
        "refund",
        "payout",
        "credit",
        "tax",
        "reconciliation",
    )
    assert record.detected_safeguards == (
        "double_entry_ledger_checks",
        "idempotent_payment_operations",
        "reconciliation_reports",
        "audit_evidence",
        "rounding_currency_tests",
        "refund_edge_cases",
        "rollback_manual_adjustments",
    )
    assert record.missing_safeguards == ()
    assert record.readiness == "ready"
    assert result.summary["readiness_counts"] == {
        "ready": 1,
        "partial": 0,
        "missing": 0,
    }


def test_missing_reconciliation_audit_and_currency_evidence_is_not_ready():
    result = build_task_financial_reconciliation_readiness_plan(
        _plan(
            [
                _task(
                    "task-missing",
                    title="Implement invoice credit ledger posting",
                    description="Create accounting entries for invoice account credits.",
                    acceptance_criteria=["Entries are stored."],
                ),
                _task(
                    "task-partial",
                    title="Add payout reconciliation report",
                    description="Generate payout settlement variance report.",
                    acceptance_criteria=[
                        "Audit log captures reviewer approval.",
                        "Rollback is a manual journal adjustment.",
                    ],
                ),
            ]
        )
    )

    missing = _record(result, "task-missing")
    partial = _record(result, "task-partial")

    assert missing.readiness == "missing"
    assert set(("reconciliation_reports", "audit_evidence", "rounding_currency_tests")).issubset(
        missing.missing_safeguards
    )
    assert partial.readiness == "partial"
    assert "rounding_currency_tests" in partial.missing_safeguards
    assert result.summary["readiness_counts"] == {
        "ready": 0,
        "partial": 1,
        "missing": 1,
    }


def test_non_financial_tasks_and_malformed_sources_return_empty_valid_plans():
    result = build_task_financial_reconciliation_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update profile settings copy",
                    description="Clarify labels in account settings.",
                    files_or_modules=["src/ui/settings.py"],
                )
            ]
        )
    )
    malformed = build_task_financial_reconciliation_readiness_plan({"id": "bad", "tasks": "oops"})
    invalid = build_task_financial_reconciliation_readiness_plan(object())

    assert result.records == ()
    assert result.financial_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.summary == {
        "financial_task_count": 0,
        "not_applicable_task_ids": ["task-copy"],
        "readiness_counts": {"ready": 0, "partial": 0, "missing": 0},
        "signal_counts": {
            "payment": 0,
            "invoice": 0,
            "ledger": 0,
            "refund": 0,
            "payout": 0,
            "credit": 0,
            "tax": 0,
            "reconciliation": 0,
        },
    }
    assert malformed.records == ()
    assert malformed.summary["financial_task_count"] == 0
    assert invalid.records == ()
    assert invalid.to_dicts() == []


def test_model_dict_and_markdown_representations_are_deterministic():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Ready payment reconciliation",
                description="Reconcile payment ledger entries.",
                acceptance_criteria=[
                    "Double-entry ledger checks prove debits equal credits.",
                    "Idempotency keys prevent duplicate charges.",
                    "Reconciliation reports compare settlement reports.",
                    "Audit trail captures approvals.",
                    "Currency and rounding tests cover minor units.",
                    "Partial refund edge cases are covered.",
                    "Rollback uses manual adjustments.",
                ],
            ),
            _task(
                "task-a",
                title="Payment capture",
                description="Capture checkout payment.",
            ),
            _task("task-docs", title="Update docs", description="Document checkout copy."),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_task_financial_reconciliation_readiness(model)
    payload = task_financial_reconciliation_readiness_plan_to_dict(result)
    markdown = task_financial_reconciliation_readiness_plan_to_markdown(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert extract_task_financial_reconciliation_readiness(model).to_dict() == result.to_dict()
    assert generate_task_financial_reconciliation_readiness(model).to_dict() == result.to_dict()
    assert result.financial_task_ids == ("task-a", "task-z")
    assert result.not_applicable_task_ids == ("task-docs",)
    assert list(payload) == [
        "plan_id",
        "records",
        "financial_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "financial_signals",
        "financial_surfaces",
        "detected_safeguards",
        "missing_safeguards",
        "readiness",
        "required_readiness_steps",
        "owner_assumptions",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert markdown.startswith("# Task Financial Reconciliation Readiness: plan-financial")
    assert "| `task-a` | payment | payment_operations | missing |" in markdown


def test_single_task_model_input_is_supported_without_mutation():
    task = _task(
        "task-single",
        title="Add tax reconciliation audit evidence",
        description="Add tax reconciliation report.",
        acceptance_criteria=[
            "Reconciliation reports compare taxable invoice totals.",
            "Audit evidence is exported.",
            "Currency rounding tests cover VAT minor units.",
        ],
    )
    model = ExecutionTask.model_validate(task)
    before = copy.deepcopy(model.model_dump(mode="python"))

    result = build_task_financial_reconciliation_readiness_plan(model)

    assert model.model_dump(mode="python") == before
    assert result.plan_id is None
    assert result.financial_task_ids == ("task-single",)
    assert result.records[0].readiness == "partial"
    assert result.records[0].owner_assumptions == (
        "Finance or tax owner signs off on rounding, currency, and audit evidence.",
    )


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks):
    return {
        "id": "plan-financial",
        "implementation_brief_id": "brief-financial",
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
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
