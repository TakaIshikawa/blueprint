import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_payment_flow_risk import (
    TaskPaymentFlowRiskFinding,
    TaskPaymentFlowRiskPlan,
    build_task_payment_flow_risk_plan,
    summarize_task_payment_flow_risk,
    task_payment_flow_risk_plan_to_dict,
    task_payment_flow_risk_plan_to_markdown,
)


def test_payment_checkout_provider_refunds_tax_and_subscriptions_are_detected():
    result = build_task_payment_flow_risk_plan(
        _plan(
            [
                _task(
                    "task-checkout",
                    title="Add Stripe checkout payment flow",
                    description=(
                        "Create checkout session, authorize card payment, capture charges, "
                        "verify webhook signature, and store idempotency keys."
                    ),
                    files_or_modules=[
                        "src/payments/stripe_checkout.py",
                        "src/billing/reconciliation.py",
                    ],
                    tags=["payment-provider"],
                    metadata={
                        "refunds": "Validate partial refund and dispute handling.",
                        "tax": {"fixtures": "VAT and sales tax calculation cases"},
                    },
                    validation_commands={
                        "test": ["poetry run pytest tests/payments/test_stripe_checkout.py"]
                    },
                ),
                _task(
                    "task-subscription",
                    title="Update subscription invoice renewal flow",
                    description="Handle recurring billing, proration, invoice emails, and failed renewals.",
                    files_or_modules=["src/billing/subscriptions.py"],
                ),
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="Adjust labels and helper text.",
                    files_or_modules=["src/blueprint/ui/dashboard_copy.py"],
                ),
            ]
        )
    )

    assert isinstance(result, TaskPaymentFlowRiskPlan)
    assert result.plan_id == "plan-payment"
    assert result.payment_impacted_task_ids == ("task-checkout", "task-subscription")
    assert result.low_risk_task_ids == ("task-copy",)
    assert result.summary["risk_counts"] == {"high": 2, "medium": 0, "low": 1}
    assert result.summary["signal_counts"]["provider_integration"] == 1
    assert result.summary["signal_counts"]["subscriptions"] == 1

    checkout = result.task_risks[0]
    assert isinstance(checkout, TaskPaymentFlowRiskFinding)
    assert checkout.task_id == "task-checkout"
    assert checkout.risk_level == "high"
    assert checkout.detected_signals == (
        "payment",
        "checkout",
        "refunds",
        "tax",
        "provider_integration",
        "idempotency",
        "reconciliation",
    )
    assert any("sandbox payment-provider tests" in value for value in checkout.recommended_safeguards)
    assert any("Idempotency" in value or "idempotency" in value for value in checkout.recommended_safeguards)
    assert any("reconciliation checks" in value for value in checkout.recommended_safeguards)
    assert any("tax calculation fixtures" in value for value in checkout.recommended_safeguards)
    assert any("files_or_modules: src/payments/stripe_checkout.py" == item for item in checkout.evidence)
    assert any("metadata.refunds" in item for item in checkout.evidence)
    assert any("validation_commands: poetry run pytest tests/payments/test_stripe_checkout.py" in item for item in checkout.evidence)


def test_medium_and_low_risk_tasks_include_explanatory_rationales():
    result = build_task_payment_flow_risk_plan(
        _plan(
            [
                _task(
                    "task-invoice-admin",
                    title="Add internal invoicing dashboard",
                    description="Show invoice status for finance operations.",
                    risk_level="medium",
                ),
                _task(
                    "task-docs",
                    title="Document onboarding checklist",
                    description="Clarify support handoff text.",
                ),
            ]
        )
    )

    by_id = {risk.task_id: risk for risk in result.task_risks}

    assert result.payment_impacted_task_ids == ("task-invoice-admin",)
    assert result.low_risk_task_ids == ("task-docs",)
    assert by_id["task-invoice-admin"].risk_level == "medium"
    assert by_id["task-invoice-admin"].detected_signals == ("invoicing",)
    assert "payment-adjacent implementation signals" in by_id["task-invoice-admin"].rationale
    assert by_id["task-docs"].risk_level == "low"
    assert by_id["task-docs"].detected_signals == ()
    assert "No payment, checkout" in by_id["task-docs"].rationale
    assert any("does not alter payment" in value for value in by_id["task-docs"].recommended_safeguards)


def test_serialization_markdown_alias_and_deterministic_ordering():
    plan = _plan(
        [
            _task(
                "task-low",
                title="Update profile copy",
                description="Text-only profile page changes.",
            ),
            _task(
                "task-high",
                title="Refund path | PayPal",
                description="Validate PayPal refund webhook replay and idempotency key behavior.",
                files_or_modules={"main": "src/payments/paypal_refunds.py", "duplicate": "src/payments/paypal_refunds.py"},
            ),
            _task(
                "task-medium",
                title="Invoice list",
                description="Render invoice status for finance users.",
                risk_level="medium",
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_payment_flow_risk(plan)
    payload = task_payment_flow_risk_plan_to_dict(result)
    markdown = task_payment_flow_risk_plan_to_markdown(result)

    assert plan == original
    assert result.to_dicts() == payload["task_risks"]
    assert json.loads(json.dumps(payload)) == payload
    assert [risk.task_id for risk in result.task_risks] == ["task-high", "task-medium", "task-low"]
    assert [risk.risk_level for risk in result.task_risks] == ["high", "medium", "low"]
    assert list(payload) == [
        "plan_id",
        "task_risks",
        "payment_impacted_task_ids",
        "low_risk_task_ids",
        "summary",
    ]
    assert list(payload["task_risks"][0]) == [
        "task_id",
        "title",
        "risk_level",
        "detected_signals",
        "recommended_safeguards",
        "rationale",
        "evidence",
    ]
    assert len(result.task_risks[0].evidence) == len(set(result.task_risks[0].evidence))
    assert result.task_risks[0].evidence.count("files_or_modules: src/payments/paypal_refunds.py") == 1
    assert markdown.startswith("# Task Payment Flow Risk Plan: plan-payment")
    assert "Refund path \\| PayPal" in markdown
    assert "Low-risk tasks: task-low" in markdown


def test_empty_invalid_execution_plan_execution_task_and_object_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add tax fixture coverage",
        description="Run VAT and GST tax calculation fixtures through the sandbox provider tests.",
        files_or_modules=["tests/payments/test_tax_calculation.py"],
        metadata={"validation_commands": {"test": ["poetry run pytest tests/payments/test_tax_calculation.py"]}},
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Add checkout reconciliation audit logging",
            description="Track checkout payment ledger reconciliation and audit log entries.",
            files_or_modules=["src/payments/audit_log.py"],
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan([task_model.model_dump(mode="python")], plan_id="plan-model")
    )

    first = build_task_payment_flow_risk_plan([object_task])
    second = build_task_payment_flow_risk_plan(plan_model)
    single = build_task_payment_flow_risk_plan(task_model)
    empty = build_task_payment_flow_risk_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_payment_flow_risk_plan(19)

    assert first.task_risks[0].task_id == "task-object"
    assert first.task_risks[0].risk_level == "high"
    assert "tax" in first.task_risks[0].detected_signals
    assert "validation_commands: poetry run pytest tests/payments/test_tax_calculation.py" in first.task_risks[0].evidence
    assert second.plan_id == "plan-model"
    assert second.task_risks[0].task_id == "task-model"
    assert single.plan_id is None
    assert single.task_risks[0].task_id == "task-model"
    assert empty.plan_id == "empty-plan"
    assert empty.task_risks == ()
    assert empty.summary["task_count"] == 0
    assert "No tasks were available" in empty.to_markdown()
    assert invalid.plan_id is None
    assert invalid.task_risks == ()
    assert invalid.summary["task_count"] == 0


def _plan(tasks, plan_id="plan-payment"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-payment",
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
    risk_level=None,
    validation_commands=None,
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
    if risk_level is not None:
        task["risk_level"] = risk_level
    if validation_commands is not None:
        task["validation_commands"] = validation_commands
    return task
