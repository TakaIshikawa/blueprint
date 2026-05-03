import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_regional_pricing_rollout_matrix import (
    PlanRegionalPricingRolloutMatrix,
    PlanRegionalPricingRolloutMatrixRow,
    analyze_plan_regional_pricing_rollout_matrix,
    build_plan_regional_pricing_rollout_matrix,
    derive_plan_regional_pricing_rollout_matrix,
    extract_plan_regional_pricing_rollout_matrix,
    generate_plan_regional_pricing_rollout_matrix,
    plan_regional_pricing_rollout_matrix_to_dict,
    plan_regional_pricing_rollout_matrix_to_dicts,
    plan_regional_pricing_rollout_matrix_to_markdown,
    summarize_plan_regional_pricing_rollout_matrix,
)


def test_multi_task_grouping_detects_pricing_currency_tax_and_controls():
    result = build_plan_regional_pricing_rollout_matrix(
        _plan(
            [
                _task(
                    "task-pricebook",
                    title="Publish EU EUR localized price book",
                    description="Localized price book sets EUR regional pricing for EU launch.",
                    acceptance_criteria=[
                        "Price book approval and finance approval are complete.",
                        "Localization review covers price presentation.",
                        "Rollout gate and monitoring dashboard are ready.",
                    ],
                    metadata={"pricing_owner": "Revenue Ops"},
                ),
                _task(
                    "task-tax",
                    title="Validate EU EUR tax-inclusive checkout",
                    description="Tax-inclusive price display uses EUR for EU checkout.",
                    acceptance_criteria=[
                        "Tax validation covers VAT included totals through the tax engine.",
                        "Billing reconciliation checks invoices after launch.",
                        "Feature flag controls the rollout.",
                    ],
                    metadata={"tax_owner": "Tax Ops"},
                ),
                _task("task-copy", title="Update copy", description="Polish settings labels."),
            ]
        )
    )

    assert isinstance(result, PlanRegionalPricingRolloutMatrix)
    assert all(isinstance(row, PlanRegionalPricingRolloutMatrixRow) for row in result.rows)
    assert result.plan_id == "plan-regional-pricing"
    assert result.affected_task_ids == ("task-pricebook", "task-tax")
    assert result.not_applicable_task_ids == ("task-copy",)

    pricebook = _row(result, ("task-pricebook",))
    tax = _row(result, ("task-tax",))

    assert pricebook.regions_or_currencies == ("EU", "EUR")
    assert pricebook.detected_signals == (
        "regional_pricing",
        "currency",
        "localized_price_book",
    )
    assert pricebook.present_controls == (
        "rollout_gate",
        "price_book_approval",
        "localization_review",
        "monitoring",
    )
    assert pricebook.missing_decisions == (
        "Define billing, invoice, refund, and revenue reconciliation checks.",
    )
    assert pricebook.risk_level == "medium"
    assert pricebook.owners[:1] == ("Revenue Ops",)

    assert tax.detected_signals == ("currency", "tax_inclusive_price")
    assert tax.present_controls == (
        "feature_flag",
        "tax_validation",
        "billing_reconciliation",
    )
    assert tax.missing_decisions == (
        "Approve localized price book, currency conversion, and rounding rules.",
    )
    assert tax.risk_level == "medium"
    assert result.summary["signal_counts"]["tax_inclusive_price"] == 1
    assert result.summary["control_counts"]["tax_validation"] == 1


def test_related_tasks_with_same_scope_and_signals_are_grouped_stably():
    result = build_plan_regional_pricing_rollout_matrix(
        _plan(
            [
                _task(
                    "task-b",
                    title="Add GBP price book",
                    description="UK GBP localized price book for regional pricing.",
                    metadata={"owner": "Pricing Ops"},
                ),
                _task(
                    "task-a",
                    title="Backfill UK GBP regional pricing",
                    description="Regional pricing uses the UK GBP localized price book.",
                    metadata={"billing_owner": "Billing Ops"},
                ),
            ]
        )
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.task_ids == ("task-a", "task-b")
    assert row.regions_or_currencies == ("UK", "GBP")
    assert row.detected_signals == (
        "regional_pricing",
        "currency",
        "localized_price_book",
    )
    assert row.risk_level == "high"
    assert row.owners[:2] == ("Billing Ops", "Pricing Ops")
    assert row.missing_decisions == (
        "Define regional rollout gate, cohort, or kill-switch decision.",
        "Approve localized price book, currency conversion, and rounding rules.",
        "Define billing, invoice, refund, and revenue reconciliation checks.",
        "Complete localization review for price presentation and market copy.",
    )


def test_missing_controls_for_region_availability_and_tax_raise_risk():
    result = build_plan_regional_pricing_rollout_matrix(
        _plan(
            [
                _task(
                    "task-market",
                    title="Launch Canada CAD tax-inclusive regional availability",
                    description=(
                        "Available in Canada with CAD tax-inclusive regional pricing and "
                        "country availability changes."
                    ),
                    files_or_modules=["src/billing/price_books/cad.py"],
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.regions_or_currencies == ("CA", "CAD")
    assert row.detected_signals == (
        "regional_pricing",
        "currency",
        "localized_price_book",
        "tax_inclusive_price",
        "region_availability",
    )
    assert row.present_controls == ()
    assert row.risk_level == "high"
    assert "Validate tax-inclusive display and tax engine calculations per market." in row.missing_decisions
    assert "Decide region availability eligibility, exclusions, and launch sequencing." in row.missing_decisions
    assert "Run tax-inclusive price validation against billing and tax providers." in row.recommended_steps


def test_no_signal_empty_invalid_and_markdown_output_are_deterministic():
    no_signal = build_plan_regional_pricing_rollout_matrix(
        _plan(
            [_task("task-docs", title="Refresh help text", description="Update static help text.")],
            plan_id="no-pricing",
        )
    )
    empty = build_plan_regional_pricing_rollout_matrix({"id": "empty-pricing", "tasks": []})
    invalid = build_plan_regional_pricing_rollout_matrix(17)

    assert no_signal.rows == ()
    assert no_signal.affected_task_ids == ()
    assert no_signal.not_applicable_task_ids == ("task-docs",)
    assert no_signal.to_markdown() == "\n".join(
        [
            "# Plan Regional Pricing Rollout Matrix: no-pricing",
            "",
            "Summary: 0 of 1 tasks affect regional pricing (high: 0, medium: 0, low: 0).",
            "",
            "No regional pricing rollout rows were inferred.",
            "",
            "Not applicable: task-docs",
        ]
    )
    assert empty.to_dict() == {
        "plan_id": "empty-pricing",
        "rows": [],
        "records": [],
        "affected_task_ids": [],
        "not_applicable_task_ids": [],
        "summary": {
            "task_count": 0,
            "row_count": 0,
            "affected_task_count": 0,
            "not_applicable_task_count": 0,
            "risk_counts": {"high": 0, "medium": 0, "low": 0},
            "signal_counts": {
                "regional_pricing": 0,
                "currency": 0,
                "localized_price_book": 0,
                "tax_inclusive_price": 0,
                "region_availability": 0,
            },
            "control_counts": {
                "rollout_gate": 0,
                "feature_flag": 0,
                "price_book_approval": 0,
                "tax_validation": 0,
                "billing_reconciliation": 0,
                "localization_review": 0,
                "availability_gate": 0,
                "monitoring": 0,
            },
            "missing_decision_count": 0,
            "affected_task_ids": [],
            "not_applicable_task_ids": [],
        },
    }
    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Regional Pricing Rollout Matrix: empty-pricing",
            "",
            "Summary: 0 of 0 tasks affect regional pricing (high: 0, medium: 0, low: 0).",
            "",
            "No regional pricing rollout rows were inferred.",
        ]
    )
    assert invalid.plan_id is None
    assert invalid.rows == ()
    assert invalid.summary["row_count"] == 0


def test_serialization_aliases_model_list_object_input_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-price | eu",
                title="EU | EUR localized price book",
                description="EU EUR localized price book with regional pricing.",
                acceptance_criteria=["Price book approval and rollout gate are complete."],
                metadata={"owner": "Revenue Ops"},
            ),
            _task("task-copy", title="Copy refresh", description="Update labels."),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_regional_pricing_rollout_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_regional_pricing_rollout_matrix_to_dict(result)
    markdown = plan_regional_pricing_rollout_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_regional_pricing_rollout_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_regional_pricing_rollout_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_regional_pricing_rollout_matrix(result) is result
    assert analyze_plan_regional_pricing_rollout_matrix(result) is result
    assert summarize_plan_regional_pricing_rollout_matrix(result) == result.summary
    assert isinstance(summarize_plan_regional_pricing_rollout_matrix(plan), PlanRegionalPricingRolloutMatrix)
    assert plan_regional_pricing_rollout_matrix_to_dicts(result) == payload["rows"]
    assert plan_regional_pricing_rollout_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "affected_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "task_ids",
        "regions_or_currencies",
        "detected_signals",
        "present_controls",
        "missing_decisions",
        "risk_level",
        "owners",
        "evidence",
        "recommended_steps",
    ]
    assert "`task-price \\| eu`" in markdown
    assert "EU \\| EUR localized price book" in markdown
    assert (
        "| Tasks | Regions/Currencies | Signals | Controls | Missing Decisions | Risk | "
        "Owners | Recommended Steps | Evidence |"
    ) in markdown

    object_task = SimpleNamespace(
        id="task-object",
        title="APAC SGD regional availability",
        description="Region availability and SGD currency rollout use availability gate monitoring.",
        acceptance_criteria=["Done"],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Brazil BRL tax-inclusive prices",
            description="BR BRL tax-inclusive price validation uses tax engine and billing reconciliation.",
        )
    )
    object_result = build_plan_regional_pricing_rollout_matrix([object_task, model_task])

    assert object_result.plan_id is None
    assert object_result.affected_task_ids == ("task-object", "task-model")
    assert _row(object_result, ("task-object",)).present_controls == ("availability_gate", "monitoring")
    assert _row(object_result, ("task-model",)).present_controls == (
        "tax_validation",
        "billing_reconciliation",
    )


def _row(result, task_ids):
    return next(row for row in result.rows if row.task_ids == task_ids)


def _plan(tasks, *, plan_id="plan-regional-pricing"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-regional-pricing",
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
    metadata=None,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-regional-pricing",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "medium",
        "estimated_hours": 2.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
