import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_idempotency_readiness import (
    TaskIdempotencyReadinessFinding,
    TaskIdempotencyReadinessPlan,
    analyze_task_idempotency_readiness,
    build_task_idempotency_readiness_plan,
    extract_task_idempotency_readiness,
    generate_task_idempotency_readiness,
    summarize_task_idempotency_readiness,
    task_idempotency_readiness_plan_to_dict,
    task_idempotency_readiness_plan_to_dicts,
    task_idempotency_readiness_plan_to_markdown,
)


def test_webhook_retry_flags_missing_replay_and_duplicate_safeguards_with_evidence():
    result = build_task_idempotency_readiness_plan(
        _plan(
            [
                _task(
                    "task-webhook-retry",
                    title="Handle Stripe webhook retries",
                    description="Provider webhook retries can redeliver payment events after transient failures.",
                    files_or_modules=["src/integrations/stripe/webhooks/retry_handler.py"],
                    acceptance_criteria=["Persist the event payload before acknowledging the callback."],
                )
            ]
        )
    )

    assert isinstance(result, TaskIdempotencyReadinessPlan)
    assert result.plan_id == "plan-idempotency"
    assert result.idempotency_task_ids == ("task-webhook-retry",)
    finding = result.findings[0]
    assert isinstance(finding, TaskIdempotencyReadinessFinding)
    assert finding.risk_level == "high"
    assert finding.detected_signals == (
        "webhook_retry",
        "payment_mutation",
        "external_api_call",
        "mutation_endpoint",
    )
    assert "replay_handling" in finding.missing_safeguards
    assert "duplicate_suppression" in finding.missing_safeguards
    assert "Define replay handling for repeated, delayed, and out-of-order delivery." in finding.actionable_remediations
    assert any("webhook retries" in item for item in finding.evidence)
    assert any("files_or_modules: src/integrations/stripe/webhooks/retry_handler.py" in item for item in finding.evidence)


def test_payment_mutation_with_all_safeguards_is_low_risk():
    result = analyze_task_idempotency_readiness(
        _plan(
            [
                _task(
                    "task-payment-create",
                    title="Create payment mutation endpoint",
                    description=(
                        "Create a Stripe payment endpoint that requires an idempotency key, handles replay-safe "
                        "retry delivery, uses duplicate suppression with a unique index, and persists through an "
                        "atomic transaction."
                    ),
                    acceptance_criteria=[
                        "Idempotency tests retry the payment create request and assert only one durable charge.",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.risk_level == "low"
    assert finding.missing_safeguards == ()
    assert finding.actionable_remediations == ()
    assert finding.present_safeguards == (
        "idempotency_key",
        "replay_handling",
        "duplicate_suppression",
        "retry_safe_persistence",
        "idempotency_tests",
    )
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_bulk_import_and_background_job_report_retry_safe_persistence_and_test_expectations():
    result = build_task_idempotency_readiness_plan(
        _plan(
            [
                _task(
                    "task-import",
                    title="Bulk import customers from CSV",
                    description="Bulk import customer rows and reprocess failed import batches.",
                    metadata={"dedupe": "Use a natural key to dedupe imported customer rows."},
                ),
                _task(
                    "task-worker",
                    title="Background job retries invoice sync",
                    description="Queue worker retries invoice sync jobs against an external API after 5xx responses.",
                    acceptance_criteria=["Use an idempotency key for each sync operation."],
                ),
            ]
        )
    )

    by_id = {finding.task_id: finding for finding in result.findings}

    assert by_id["task-import"].detected_signals == ("bulk_import", "mutation_endpoint")
    assert by_id["task-import"].present_safeguards == ("duplicate_suppression",)
    assert "retry_safe_persistence" in by_id["task-import"].missing_safeguards
    assert "idempotency_tests" in by_id["task-import"].missing_safeguards
    assert any("metadata.dedupe" in item for item in by_id["task-import"].evidence)

    assert by_id["task-worker"].detected_signals == (
        "background_job",
        "external_api_call",
        "mutation_endpoint",
    )
    assert by_id["task-worker"].present_safeguards == ("idempotency_key",)
    assert by_id["task-worker"].risk_level == "high"
    assert result.summary["missing_safeguard_counts"]["idempotency_tests"] == 2


def test_read_only_and_explicit_out_of_scope_tasks_do_not_emit_high_severity_findings():
    result = build_task_idempotency_readiness_plan(
        _plan(
            [
                _task(
                    "task-readonly-provider",
                    title="Read-only external API report",
                    description="Fetch and list external API usage in a read-only dashboard with no writes.",
                    files_or_modules=["src/integrations/github/read_only_report.py"],
                ),
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="No idempotency or retry workflow changes are in scope.",
                ),
            ]
        )
    )

    assert result.findings == ()
    assert result.records == ()
    assert result.idempotency_task_ids == ()
    assert result.not_applicable_task_ids == ("task-readonly-provider", "task-copy")
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 0}


def test_serialization_aliases_markdown_model_object_input_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z | retry",
                title="Webhook | retry readiness",
                description="Webhook retry handler uses an idempotency key and replay handling.",
                acceptance_criteria=["Duplicate suppression is tracked with a processed event table."],
            ),
            _task(
                "task-a",
                title="Mutation endpoint idempotency tests",
                description=(
                    "Create endpoint persists writes in a transaction, has idempotency tests, and documents "
                    "retry-safe persistence for partial failure."
                ),
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model_plan = ExecutionPlan.model_validate(plan)

    result = summarize_task_idempotency_readiness(model_plan)
    payload = task_idempotency_readiness_plan_to_dict(result)
    markdown = task_idempotency_readiness_plan_to_markdown(result)
    task_result = build_task_idempotency_readiness_plan(ExecutionTask.model_validate(plan["tasks"][0]))
    object_result = build_task_idempotency_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="Background job replay handling",
            description="Background job retry includes replay handling and duplicate suppression.",
        )
    )
    invalid = build_task_idempotency_readiness_plan(42)

    assert plan == original
    assert result.records == result.findings
    assert task_result.findings[0].task_id == "task-z | retry"
    assert object_result.findings[0].task_id == "task-object"
    assert invalid.findings == ()
    assert extract_task_idempotency_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_idempotency_readiness(plan).to_dict() == result.to_dict()
    assert task_idempotency_readiness_plan_to_dicts(result) == payload["findings"]
    assert task_idempotency_readiness_plan_to_dicts(result.findings) == payload["findings"]
    assert result.to_dicts() == payload["findings"]
    assert json.loads(json.dumps(payload)) == payload
    assert "Webhook \\| retry readiness" in markdown
    assert list(payload) == [
        "plan_id",
        "findings",
        "idempotency_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["findings"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "actionable_remediations",
    ]


def _plan(tasks, plan_id="plan-idempotency"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-idempotency",
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
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
