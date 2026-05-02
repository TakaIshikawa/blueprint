import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_payment_failure_readiness import (
    TaskPaymentFailureReadinessPlan,
    TaskPaymentFailureReadinessRecord,
    analyze_task_payment_failure_readiness,
    build_task_payment_failure_readiness_plan,
    derive_task_payment_failure_readiness,
    extract_task_payment_failure_readiness,
    generate_task_payment_failure_readiness,
    recommend_task_payment_failure_readiness,
    summarize_task_payment_failure_readiness,
    task_payment_failure_readiness_plan_to_dict,
    task_payment_failure_readiness_plan_to_dicts,
    task_payment_failure_readiness_plan_to_markdown,
    task_payment_failure_readiness_to_dicts,
)


def test_detects_payment_failure_signals_from_task_fields_paths_tags_and_metadata():
    result = build_task_payment_failure_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Add payment failure rollout checks",
                    description="Handle failed charge, card decline, and dunning states.",
                    files_or_modules=[
                        "src/billing/payment_retry_schedule.py",
                        "src/billing/unpaid_invoice_collection.py",
                        "src/billing/grace_period_access.py",
                        "src/billing/subscription_suspension.py",
                        "src/billing/payment_method_update.py",
                        "src/billing/stripe_webhook_idempotency.py",
                    ],
                    tags=["billing-webhook", "past-due"],
                    acceptance_criteria=[
                        "Customer notification sends payment failure email before access changes.",
                        "Support dashboard shows billing status.",
                    ],
                    metadata={"audit_trail": "Billing event history is timestamped."},
                ),
                _task(
                    "task-metadata",
                    title="Protect invoice collection",
                    description="Invoice payment can fail after an issuer decline.",
                    metadata={
                        "payment_method_update": "Billing portal supports recovery path.",
                        "entitlement_state": "Subscription status and customer access are handled.",
                    },
                ),
            ]
        )
    )

    assert isinstance(result, TaskPaymentFailureReadinessPlan)
    by_id = {record.task_id: record for record in result.records}
    assert set(by_id) == {"task-paths", "task-metadata"}
    assert set(by_id["task-paths"].detected_signals) == {
        "failed_charge",
        "card_decline",
        "retry_schedule",
        "dunning",
        "invoice_collection",
        "grace_period",
        "subscription_suspension",
        "payment_method_update",
        "billing_webhook",
    }
    assert {"invoice_collection", "card_decline", "payment_method_update"} <= set(
        by_id["task-metadata"].detected_signals
    )
    assert {"customer_notification", "webhook_idempotency", "support_visibility", "audit_trail"} <= set(
        by_id["task-paths"].present_safeguards
    )
    assert {"entitlement_state_handling", "recovery_path"} <= set(by_id["task-metadata"].present_safeguards)
    assert any("files_or_modules:" in item for item in by_id["task-paths"].evidence)
    assert any("tags[0]:" in item for item in by_id["task-paths"].evidence)
    assert any("metadata.payment_method_update" in item for item in by_id["task-metadata"].evidence)


def test_weak_partial_and_strong_readiness_and_recommended_checks_are_inferred():
    result = analyze_task_payment_failure_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Complete dunning workflow",
                    description="Dunning handles failed payment, retry schedule, grace period, and subscription suspension.",
                    acceptance_criteria=[
                        "Retry policy defines cadence, retry limit, and hard decline no retry behavior.",
                        "Customer notification sends dunning email and in-app notice.",
                        "Entitlement state handling covers grace, suspension, and restored access.",
                        "Webhook idempotency deduplicates duplicate webhook events.",
                        "Support visibility exposes billing status in the admin dashboard.",
                        "Audit trail records billing event history.",
                        "Recovery path lets users update payment method and settle invoice.",
                    ],
                ),
                _task(
                    "task-partial",
                    title="Invoice payment failure flow",
                    description="Unpaid invoice collection with payment retries and card declines.",
                    acceptance_criteria=[
                        "Retry policy includes backoff.",
                        "Customer notification sends payment failure email.",
                        "Support dashboard shows retry state.",
                        "Audit log captures status history.",
                    ],
                ),
                _task(
                    "task-weak",
                    title="Billing webhook for failed charge",
                    description="Process invoice.payment_failed webhook events.",
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert by_id["task-strong"].readiness == "strong"
    assert by_id["task-strong"].missing_safeguards == ()
    assert by_id["task-strong"].recommended_checks == ()
    assert by_id["task-partial"].readiness == "partial"
    assert by_id["task-partial"].missing_safeguards == (
        "entitlement_state_handling",
        "webhook_idempotency",
        "recovery_path",
    )
    assert by_id["task-weak"].readiness == "weak"
    assert by_id["task-weak"].missing_safeguards == (
        "retry_policy",
        "customer_notification",
        "entitlement_state_handling",
        "webhook_idempotency",
        "support_visibility",
        "audit_trail",
        "recovery_path",
    )
    assert by_id["task-weak"].recommended_checks[0].startswith("Define retry cadence")
    assert by_id["task-weak"].recommendations == by_id["task-weak"].recommended_checks
    assert by_id["task-weak"].recommended_actions == by_id["task-weak"].recommended_checks
    assert result.payment_failure_task_ids == ("task-weak", "task-partial", "task-strong")
    assert result.impacted_task_ids == result.payment_failure_task_ids
    assert result.summary["readiness_counts"] == {"weak": 1, "partial": 1, "strong": 1}
    assert result.summary["missing_safeguard_counts"]["webhook_idempotency"] == 2


def test_no_impact_empty_invalid_markdown_and_summary_are_stable():
    result = build_task_payment_failure_readiness_plan(
        _plan(
            [
                _task("task-copy", title="Update pricing copy", description="Text only."),
                _task(
                    "task-billing",
                    title="Plan payment failure emails",
                    description="Failed payment dunning sends customer notification.",
                ),
            ]
        )
    )
    empty = build_task_payment_failure_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_payment_failure_readiness_plan(13)
    no_signal = build_task_payment_failure_readiness_plan(
        _plan([_task("task-copy", title="Update helper copy", description="Static text only.")])
    )

    assert result.payment_failure_task_ids == ("task-billing",)
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary["no_impact_task_ids"] == ["task-copy"]
    assert empty.records == ()
    assert invalid.records == ()
    assert no_signal.records == ()
    assert no_signal.no_impact_task_ids == ("task-copy",)
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Payment Failure Readiness: empty-plan",
            "",
            "## Summary",
            "",
            "- Task count: 0",
            "- Payment-failure task count: 0",
            "- Missing safeguard count: 0",
            "- Readiness counts: weak 0, partial 0, strong 0",
            (
                "- Signal counts: failed_charge 0, card_decline 0, retry_schedule 0, dunning 0, "
                "invoice_collection 0, grace_period 0, subscription_suspension 0, "
                "payment_method_update 0, billing_webhook 0"
            ),
            "",
            "No task payment failure readiness records were inferred.",
        ]
    )
    assert "No-impact tasks: task-copy" in no_signal.to_markdown()


def test_model_objects_serialization_markdown_aliases_and_no_source_mutation_are_stable():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add card decline support view",
        description="Card decline support visibility is needed for dunning agents.",
        files_or_modules=["src/billing/card_decline_support_dashboard.py"],
        acceptance_criteria=["Audit trail records billing event history."],
        metadata={"recovery_path": "Billing portal lets customers update card."},
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Billing webhook | failed payment",
            description="Stripe webhook handles invoice.payment_failed with idempotent webhook processing.",
            acceptance_criteria=["Retry policy defines next retry."],
        )
    )
    plan = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-a",
                title="Dunning grace period",
                description="Past due customers get a grace period before subscription suspension.",
            ),
            _task("task-copy", title="Copy update", description="Update helper text."),
        ],
        plan_id="plan-serialization",
    )
    original = copy.deepcopy(plan)

    result = summarize_task_payment_failure_readiness(plan)
    object_result = build_task_payment_failure_readiness_plan([object_task])
    model_result = generate_task_payment_failure_readiness(ExecutionPlan.model_validate(plan))
    payload = task_payment_failure_readiness_plan_to_dict(result)
    markdown = task_payment_failure_readiness_plan_to_markdown(result)

    assert plan == original
    assert isinstance(result.records[0], TaskPaymentFailureReadinessRecord)
    assert object_result.records[0].task_id == "task-object"
    assert model_result.plan_id == "plan-serialization"
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_payment_failure_readiness_plan_to_dicts(result) == payload["records"]
    assert task_payment_failure_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_payment_failure_readiness_to_dicts(result.records) == payload["records"]
    assert extract_task_payment_failure_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_payment_failure_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_payment_failure_readiness(plan).to_dict() == result.to_dict()
    assert analyze_task_payment_failure_readiness(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "payment_failure_task_ids",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness",
        "recommended_checks",
        "evidence",
    ]
    assert result.payment_failure_task_ids == ("task-a", "task-model")
    assert result.no_impact_task_ids == ("task-copy",)
    assert markdown.startswith("# Task Payment Failure Readiness: plan-serialization")
    assert "Billing webhook \\| failed payment" in markdown
    assert (
        "| Task | Title | Readiness | Detected Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |"
        in markdown
    )


def _plan(tasks, plan_id="plan-payment-failure"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-payment-failure",
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
    tags=None,
    metadata=None,
    validation_commands=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-payment-failure",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        payload["tags"] = tags
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    return payload
