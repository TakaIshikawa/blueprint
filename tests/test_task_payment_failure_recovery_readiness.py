import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_payment_failure_recovery_readiness import (
    TaskPaymentFailureRecoveryReadinessPlan,
    TaskPaymentFailureRecoveryReadinessRecord,
    analyze_task_payment_failure_recovery_readiness,
    build_task_payment_failure_recovery_readiness_plan,
    derive_task_payment_failure_recovery_readiness,
    extract_task_payment_failure_recovery_readiness,
    generate_task_payment_failure_recovery_readiness,
    recommend_task_payment_failure_recovery_readiness,
    summarize_task_payment_failure_recovery_readiness,
    task_payment_failure_recovery_readiness_plan_to_dict,
    task_payment_failure_recovery_readiness_plan_to_dicts,
    task_payment_failure_recovery_readiness_plan_to_markdown,
)


def test_detects_payment_signals_from_text_paths_tags_metadata_and_validation_commands():
    result = build_task_payment_failure_recovery_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Harden payment recovery paths",
                    description="Move implementation files.",
                    files_or_modules=[
                        "src/payments/payment_collection.py",
                        "src/billing/billing_retry_schedule.py",
                        "src/subscriptions/subscription_renewal_invoice.py",
                        "src/payments/charge_capture_refund.py",
                        "src/webhooks/stripe_payment_provider_webhook.py",
                    ],
                    tags=["failed-card", "dunning"],
                    validation_commands={
                        "test": ["pytest tests/billing/test_idempotency_key_reconciliation.py"]
                    },
                ),
                _task(
                    "task-metadata",
                    title="Recover failed card invoices",
                    description="Failed payment and past-due invoice recovery for subscription renewal.",
                    metadata={
                        "customer_notification": "Payment failure email includes card update link.",
                        "manual_recovery_path": "Support playbook documents manual retry.",
                    },
                ),
            ]
        )
    )

    assert isinstance(result, TaskPaymentFailureRecoveryReadinessPlan)
    assert result.plan_id == "plan-payment-recovery"
    by_id = {record.task_id: record for record in result.records}
    assert set(by_id) == {"task-paths", "task-metadata"}
    assert set(by_id["task-paths"].detected_signals) == {
        "payment_collection",
        "billing_retry",
        "subscription_renewal",
        "invoicing",
        "charge_capture",
        "refund",
        "dunning",
        "failed_card",
        "provider_webhook",
    }
    assert {"failed_card", "subscription_renewal", "invoicing"} <= set(
        by_id["task-metadata"].detected_signals
    )
    assert "customer_notification" in by_id["task-metadata"].present_safeguards
    assert "manual_recovery_path" in by_id["task-metadata"].present_safeguards
    assert "idempotent_charge_handling" in by_id["task-paths"].present_safeguards
    assert "ledger_reconciliation" in by_id["task-paths"].present_safeguards
    assert any("files_or_modules:" in item for item in by_id["task-paths"].evidence)
    assert any("tags[0]:" in item for item in by_id["task-paths"].evidence)
    assert any("metadata.customer_notification" in item for item in by_id["task-metadata"].evidence)
    assert any("validation_commands:" in item for item in by_id["task-paths"].evidence)


def test_high_medium_low_risk_and_recommended_actions_are_inferred():
    result = analyze_task_payment_failure_recovery_readiness(
        _plan(
            [
                _task(
                    "task-low",
                    title="Ready failed card recovery",
                    description="Payment failure and failed card recovery for subscription renewal invoices.",
                    acceptance_criteria=[
                        "Retry backoff policy uses bounded retries with exponential backoff.",
                        "Customer notification sends payment failure email and card update link.",
                        "Idempotency key prevents duplicate charge capture and refund retries.",
                        "Ledger reconciliation compares invoice, settlement, and provider events.",
                        "Provider webhook handling verifies signature, dedupes events, and supports replay.",
                        "Manual recovery runbook gives support a manual retry and override path.",
                    ],
                ),
                _task(
                    "task-medium",
                    title="Billing retry notifications",
                    description="Billing retry flow for past-due invoice dunning.",
                    acceptance_criteria=[
                        "Retry schedule includes bounded retries.",
                        "Customer notification sends dunning email.",
                        "Idempotency key prevents duplicate payment attempts.",
                    ],
                ),
                _task(
                    "task-high",
                    title="Capture failed card webhook",
                    description="Charge capture consumes provider webhook events when card payment failed.",
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert by_id["task-low"].risk_level == "low"
    assert by_id["task-low"].missing_safeguards == ()
    assert by_id["task-low"].recommended_actions == ()
    assert by_id["task-medium"].risk_level == "medium"
    assert by_id["task-medium"].missing_safeguards == (
        "ledger_reconciliation",
        "provider_webhook_handling",
        "manual_recovery_path",
    )
    assert by_id["task-high"].risk_level == "high"
    assert by_id["task-high"].missing_safeguards == (
        "retry_backoff_policy",
        "customer_notification",
        "idempotent_charge_handling",
        "ledger_reconciliation",
        "provider_webhook_handling",
        "manual_recovery_path",
    )
    assert by_id["task-high"].recommended_actions[0].startswith("Define bounded payment retry")
    assert by_id["task-high"].recommendations == by_id["task-high"].recommended_actions
    assert by_id["task-high"].recommended_checks == by_id["task-high"].recommended_actions
    assert result.impacted_task_ids == ("task-high", "task-medium", "task-low")
    assert result.summary["risk_counts"] == {"high": 1, "medium": 1, "low": 1}
    assert result.summary["missing_safeguard_counts"]["idempotent_charge_handling"] == 1


def test_no_impact_invalid_and_empty_inputs_have_stable_markdown():
    result = build_task_payment_failure_recovery_readiness_plan(
        _plan(
            [
                _task("task-copy", title="Update dashboard copy", description="Text only."),
                _task(
                    "task-payment",
                    title="Add failed payment guard",
                    description="Payment failure recovery uses idempotency key handling.",
                ),
            ]
        )
    )
    empty = build_task_payment_failure_recovery_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_payment_failure_recovery_readiness_plan(13)
    invalid_tasks = build_task_payment_failure_recovery_readiness_plan(
        {"id": "bad-plan", "tasks": {"not": "a list"}}
    )
    no_signal = build_task_payment_failure_recovery_readiness_plan(
        _plan([_task("task-copy", title="Update helper copy", description="Static text only.")])
    )

    assert result.impacted_task_ids == ("task-payment",)
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary["no_impact_task_ids"] == ["task-copy"]
    assert empty.records == ()
    assert invalid.records == ()
    assert invalid_tasks.records == ()
    assert no_signal.records == ()
    assert no_signal.no_impact_task_ids == ("task-copy",)
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Payment Failure Recovery Readiness: empty-plan",
            "",
            "## Summary",
            "",
            "- Task count: 0",
            "- Impacted task count: 0",
            "- Missing safeguard count: 0",
            "- Risk counts: high 0, medium 0, low 0",
            (
                "- Signal counts: payment_collection 0, billing_retry 0, subscription_renewal 0, "
                "invoicing 0, charge_capture 0, refund 0, dunning 0, failed_card 0, provider_webhook 0"
            ),
            "",
            "No task payment failure recovery-readiness records were inferred.",
        ]
    )
    assert "No-impact tasks: task-copy" in no_signal.to_markdown()


def test_model_objects_serialization_markdown_aliases_and_no_mutation_are_stable():
    object_task = SimpleNamespace(
        id="task-object",
        title="Recover payment webhook",
        description="Payment provider webhook handles failed payment recovery.",
        files_or_modules=["src/webhooks/provider_webhook.py"],
        acceptance_criteria=["Webhook signature verification and replay behavior are covered."],
        metadata={"manual_recovery_path": "Operator recovery runbook documents replay tool."},
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Set billing retry guard | renewal",
            description="Billing retry for subscription renewal invoice.",
            acceptance_criteria=["Retry backoff and customer notification are configured."],
        )
    )
    plan = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-a",
                title="Failed card capture guard",
                description="Failed card charge capture uses idempotency key.",
            ),
            _task("task-copy", title="Copy update", description="Update helper text."),
        ],
        plan_id="plan-serialization",
    )
    original = copy.deepcopy(plan)

    result = summarize_task_payment_failure_recovery_readiness(plan)
    object_result = build_task_payment_failure_recovery_readiness_plan([object_task])
    model_result = generate_task_payment_failure_recovery_readiness(ExecutionPlan.model_validate(plan))
    payload = task_payment_failure_recovery_readiness_plan_to_dict(result)
    markdown = task_payment_failure_recovery_readiness_plan_to_markdown(result)

    assert plan == original
    assert isinstance(result.records[0], TaskPaymentFailureRecoveryReadinessRecord)
    assert object_result.records[0].task_id == "task-object"
    assert model_result.plan_id == "plan-serialization"
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_payment_failure_recovery_readiness_plan_to_dicts(result) == payload["records"]
    assert task_payment_failure_recovery_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_payment_failure_recovery_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_payment_failure_recovery_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_payment_failure_recovery_readiness(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
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
        "risk_level",
        "recommended_actions",
        "evidence",
    ]
    assert result.impacted_task_ids == ("task-a", "task-model")
    assert result.no_impact_task_ids == ("task-copy",)
    assert analyze_task_payment_failure_recovery_readiness(plan).to_dict() == result.to_dict()
    assert markdown.startswith("# Task Payment Failure Recovery Readiness: plan-serialization")
    assert "Set billing retry guard \\| renewal" in markdown
    assert (
        "| Task | Title | Risk | Detected Signals | Present Safeguards | Missing Safeguards | Recommended Actions | Evidence |"
        in markdown
    )


def _plan(tasks, plan_id="plan-payment-recovery"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-payment-recovery",
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
        "execution_plan_id": "plan-payment-recovery",
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
