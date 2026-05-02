import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_idempotency_readiness import (
    TaskIdempotencyReadinessPlan,
    TaskIdempotencyReadinessRecommendation,
    build_task_idempotency_readiness_plan,
    extract_task_idempotency_readiness_recommendations,
    task_idempotency_readiness_plan_to_dict,
)


def test_detects_retryable_webhook_payment_queue_import_and_migration_surfaces():
    result = build_task_idempotency_readiness_plan(
        _plan(
            [
                _task(
                    "task-webhook",
                    title="Handle Stripe webhook callbacks",
                    description="Persist incoming webhook callback events for checkout payments.",
                    files_or_modules=["src/integrations/stripe/webhooks.py"],
                    acceptance_criteria=[
                        "Signature validation rejects invalid payloads.",
                        "Audit log records accepted delivery ids.",
                    ],
                ),
                _task(
                    "task-queue",
                    title="Retry fulfillment queue consumer",
                    description="Consumer worker retries transient queue message failures.",
                    files_or_modules=["src/workers/fulfillment_consumer.py"],
                    acceptance_criteria=["Tests cover ordinary success and failure paths."],
                ),
                _task(
                    "task-import",
                    title="Import merchant catalog feed",
                    description="Bulk import partner feed rows from CSV uploads.",
                    files_or_modules=["src/imports/catalog.py"],
                    acceptance_criteria=["Validation reports malformed rows."],
                ),
                _task(
                    "task-migration",
                    title="Run account data migration",
                    description="Backfill account billing records with a schema migration.",
                    files_or_modules=["migrations/20260502_accounts.py"],
                    acceptance_criteria=["Dry run reports row counts before writes."],
                ),
            ]
        )
    )

    assert isinstance(result, TaskIdempotencyReadinessPlan)
    by_id = {item.task_id: item for item in result.recommendations}
    assert by_id["task-webhook"].idempotency_surfaces == (
        "webhook_receiver",
        "payment_flow",
        "external_callback",
    )
    assert by_id["task-webhook"].risk_level == "high"
    assert by_id["task-queue"].idempotency_surfaces == (
        "retryable_workflow",
        "queue_consumer",
    )
    assert by_id["task-import"].idempotency_surfaces == ("import_job",)
    assert by_id["task-migration"].idempotency_surfaces == (
        "payment_flow",
        "migration",
    )
    assert "idempotency_key_or_dedupe_key" in by_id["task-webhook"].missing_acceptance_criteria
    assert "safe_rerun_or_compensation" not in by_id["task-migration"].missing_acceptance_criteria
    assert "files_or_modules: src/integrations/stripe/webhooks.py" in by_id["task-webhook"].evidence
    assert result.summary["sensitive_task_count"] == 4
    assert result.summary["surface_counts"]["payment_flow"] == 2


def test_model_input_extract_helper_and_missing_acceptance_criteria_detection():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Receive partner webhook events",
                    description="Build incoming webhook receiver for partner status events.",
                    acceptance_criteria=[
                        "Idempotency key handling stores event_id.",
                        "Duplicate delivery tests cover replayed events.",
                        "Replay-safe processing is accepted for retries.",
                        "Side effects create each status record only once.",
                        "Duplicate observability metrics are emitted.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )

    result = build_task_idempotency_readiness_plan(plan)
    extracted = extract_task_idempotency_readiness_recommendations(plan)

    assert result.plan_id == "plan-model"
    assert extracted == result.recommendations
    assert result.records == result.recommendations
    assert isinstance(result.recommendations[0], TaskIdempotencyReadinessRecommendation)
    assert result.recommendations[0].missing_acceptance_criteria == ("safe_rerun_or_compensation",)
    assert result.recommendations[0].risk_level == "low"


def test_non_idempotency_and_noisy_non_matches_are_suppressed():
    result = build_task_idempotency_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update empty state copy",
                    description="Clarify onboarding copy and analytics labels.",
                    files_or_modules=["src/ui/settings.py"],
                    acceptance_criteria=["Copy review is complete."],
                ),
                _task(
                    "task-try",
                    title="Try alternate dashboard layout",
                    description="Try three visual arrangements for the dashboard.",
                    acceptance_criteria=["Design review approves one layout."],
                ),
            ]
        )
    )

    assert result.recommendations == ()
    assert result.sensitive_task_ids == ()
    assert result.summary == {
        "task_count": 2,
        "sensitive_task_count": 0,
        "recommendation_count": 0,
        "high_risk_count": 0,
        "medium_risk_count": 0,
        "low_risk_count": 0,
        "missing_acceptance_criteria_count": 0,
        "surface_counts": {
            "retryable_workflow": 0,
            "webhook_receiver": 0,
            "queue_consumer": 0,
            "payment_flow": 0,
            "import_job": 0,
            "migration": 0,
            "external_callback": 0,
        },
        "sensitive_task_ids": [],
    }


def test_sorting_stability_no_mutation_and_serialization():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Retry sync worker",
                description="Retry transient external feed sync failures.",
                acceptance_criteria=[
                    "Idempotency key handling stores request ids.",
                    "Duplicate delivery tests cover replay.",
                    "Safe to rerun from checkpoint.",
                    "Side effects create records only once.",
                    "Duplicate observability alerts fire.",
                ],
            ),
            _task(
                "task-a",
                title="PayPal payment callback",
                description="Handle external callback after PayPal payment approval.",
                acceptance_criteria=["Callback validates provider status."],
            ),
            _task(
                "task-m",
                title="Catalog import job",
                description="Import supplier catalog CSV files.",
                acceptance_criteria=["Idempotency key handling uses supplier sku."],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_idempotency_readiness_plan(plan)
    payload = task_idempotency_readiness_plan_to_dict(result)

    assert plan == original
    assert result.sensitive_task_ids == ("task-a", "task-m", "task-z")
    assert [item.risk_level for item in result.recommendations] == ["high", "medium", "low"]
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["recommendations"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "recommendations", "sensitive_task_ids", "summary"]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "idempotency_surfaces",
        "missing_acceptance_criteria",
        "risk_level",
        "evidence",
    ]


def test_single_task_mapping_input_is_supported():
    result = build_task_idempotency_readiness_plan(
        _task(
            "task-single",
            title="Queue consumer duplicate handling",
            description="Message consumer handles at-least-once delivery from the job queue.",
            acceptance_criteria=["Duplicate delivery tests cover redelivery."],
        )
    )

    assert result.plan_id is None
    assert result.sensitive_task_ids == ("task-single",)
    assert result.recommendations[0].idempotency_surfaces == (
        "retryable_workflow",
        "queue_consumer",
    )


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
