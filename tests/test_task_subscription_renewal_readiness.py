import json

from blueprint.task_subscription_renewal_readiness import (
    analyze_task_subscription_renewal_readiness,
    build_task_subscription_renewal_readiness_plan,
    recommend_task_subscription_renewal_readiness,
    task_subscription_renewal_readiness_plan_to_dict,
    task_subscription_renewal_readiness_plan_to_dicts,
    task_subscription_renewal_readiness_plan_to_markdown,
)


def test_complete_subscription_renewal_task_is_ready():
    result = build_task_subscription_renewal_readiness_plan(
        {
            "id": "plan-renewal",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Subscription renewal auto-renewal flow",
                    "description": "Renewal reminders and renewal invoices support recurring renewal.",
                    "acceptance_criteria": [
                        "Renewal trigger uses the term end renewal date and renewal schedule.",
                        "Customer notice timing sends advance notice and reminder cadence 30 days before renewal.",
                        "Renewal invoice generation documents billing behavior, renewal charge, and tax calculation.",
                        "Payment failure handling covers dunning, retry schedule, card decline, and past due states.",
                        "Cancellation window lets customers opt-out and turn off auto-renew before renewal.",
                        "Entitlement continuity keeps subscription access through the grace period.",
                        "Audit trail exposes renewal history in the support dashboard and event log.",
                        "Tests include renewal tests, billing tests, and invoice tests.",
                    ],
                    "files_or_modules": ["src/billing/subscriptions/renewal_invoices.py"],
                }
            ],
        }
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "renewal_trigger",
        "customer_notice_timing",
        "billing_invoice_behavior",
        "payment_failure_handling",
        "cancellation_opt_out_window",
        "entitlement_continuity",
        "audit_support_visibility",
        "tests",
    )


def test_partial_subscription_renewal_reports_gaps_and_ignores_no_impact():
    result = analyze_task_subscription_renewal_readiness(
        [
            {
                "id": "task-partial",
                "title": "Add subscription renewal lifecycle",
                "description": "Subscription renewal has a renewal trigger and support visibility.",
                "metadata": {"billing": {"invoice": "Renewal invoice generation is included."}},
                "validation_commands": ["python -m pytest tests/billing/test_renewal_invoices.py"],
            },
            {
                "id": "task-copy",
                "title": "Billing copy cleanup",
                "description": "No subscription renewal, auto-renewal, renewal reminders, or renewal invoices changes are planned.",
            },
        ]
    )

    record = result.records[0]
    assert result.ignored_task_ids == ("task-copy",)
    assert record.readiness == "partial"
    assert record.present_criteria == ("renewal_trigger", "billing_invoice_behavior", "audit_support_visibility", "tests")
    assert record.missing_criteria == (
        "customer_notice_timing",
        "payment_failure_handling",
        "cancellation_opt_out_window",
        "entitlement_continuity",
    )
    assert any("metadata.billing.invoice" in item for item in record.evidence)
    assert any("validation_commands[0]" in item for item in record.evidence)


def test_subscription_renewal_path_hints_serialization_and_markdown_are_stable():
    result = build_task_subscription_renewal_readiness_plan(
        {
            "id": "plan-path",
            "tasks": [{"id": "task-path", "title": "Refactor billing", "files_or_modules": ["src/billing/subscription_renewals/grace_period.py"]}],
        }
    )
    payload = task_subscription_renewal_readiness_plan_to_dict(result)

    assert result.records[0].detected_signals == ("subscription_renewal", "renewal_billing")
    assert recommend_task_subscription_renewal_readiness(result) == result.records
    assert task_subscription_renewal_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-path"
    assert task_subscription_renewal_readiness_plan_to_markdown(result).startswith("# Task Subscription Renewal Readiness: plan-path")
