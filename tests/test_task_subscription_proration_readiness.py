import json

from blueprint.task_subscription_proration_readiness import (
    analyze_task_subscription_proration_readiness,
    build_task_subscription_proration_readiness_plan,
    task_subscription_proration_readiness_plan_to_dict,
    task_subscription_proration_readiness_plan_to_markdown,
)


def test_complete_subscription_proration_task_is_ready():
    result = build_task_subscription_proration_readiness_plan(
        _plan(
            [
                _task(
                    "proration-ready",
                    "Subscription proration for plan changes",
                    (
                        "Handle mid-cycle upgrade and downgrade proration with invoice preview before commit. "
                        "Credit calculation tests cover unused time credit and partial period rounding. "
                        "Tax handling covers VAT and sales tax rounding. Provider reconciliation checks Stripe sync. "
                        "Customer notification sends billing email and receipt. Rollback uses compensating credit."
                    ),
                    ["src/billing/subscriptions/proration.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert {"subscription_proration", "plan_change", "mid_cycle_change", "invoice_preview", "credit_calculation"} <= set(
        record.detected_signals
    )
    assert record.missing_safeguards == ()


def test_partial_proration_task_reports_required_missing_safeguards_and_summary_counts():
    result = analyze_task_subscription_proration_readiness(
        _plan(
            [
                _task(
                    "proration-partial",
                    "Preview mid-cycle subscription upgrade invoices",
                    "Create invoice preview for upgrade and downgrade plan changes.",
                    ["src/billing/invoice_preview.py"],
                ),
                _task("copy", "Docs", "Update unrelated docs.", []),
            ]
        )
    )

    record = result.records[0]
    assert result.impacted_task_ids == ("proration-partial",)
    assert result.ignored_task_ids == ("copy",)
    assert record.present_safeguards == ("invoice_preview",)
    assert {"tax_handling", "provider_reconciliation", "customer_communication", "rollback"} <= set(record.missing_safeguards)
    assert result.summary["readiness_counts"]["partial"] == 1
    assert result.summary["impact_counts"]["high"] == 1
    assert result.summary["signal_counts"]["plan_change"] == 1
    assert result.summary["missing_safeguard_counts"]["provider_reconciliation"] == 1


def test_serialization_and_markdown_are_stable():
    result = build_task_subscription_proration_readiness_plan(
        _plan([_task("proration-weak", "Add prorated billing", "Apply proration for seat changes.", [])])
    )
    payload = task_subscription_proration_readiness_plan_to_dict(result)

    assert result.records[0].readiness == "missing"
    assert json.loads(json.dumps(payload)) == payload
    assert "| `proration-weak` | Add prorated billing |" in task_subscription_proration_readiness_plan_to_markdown(result)


def _plan(tasks):
    return {"id": "plan-proration", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
