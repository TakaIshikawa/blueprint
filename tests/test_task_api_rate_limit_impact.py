import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_rate_limit_impact import (
    TaskApiRateLimitImpactPlan,
    TaskApiRateLimitImpactRecord,
    build_task_api_rate_limit_impact_plan,
    derive_task_api_rate_limit_impact_plan,
    summarize_task_api_rate_limit_impact,
    summarize_task_api_rate_limit_impacts,
    task_api_rate_limit_impact_plan_to_dict,
    task_api_rate_limit_impact_plan_to_markdown,
)


def test_api_rate_limit_signals_create_separate_categorized_records_and_summary():
    result = build_task_api_rate_limit_impact_plan(
        _plan(
            [
                _task(
                    "task-inbound",
                    title="Throttle auth API endpoint",
                    description="Add rate limiting to the login API endpoint for auth token requests.",
                    files_or_modules=["src/api/routes/auth_login.py"],
                ),
                _task(
                    "task-outbound",
                    title="Stripe payment client quota",
                    description="Call the Stripe provider API for checkout payment capture under vendor quota.",
                    files_or_modules=["src/integrations/stripe/payment_client.py"],
                ),
                _task(
                    "task-batch",
                    title="Scheduled bulk account sync",
                    description="Nightly scheduled bulk job fans out API calls and must avoid quota burst.",
                    files_or_modules=["jobs/cron/bulk_account_sync.py"],
                ),
                _task(
                    "task-retry",
                    title="Retry failed API calls",
                    description="Handle 429 and transient failure retries with exponential backoff and jitter.",
                    files_or_modules=["src/api/retry_policy.py"],
                ),
                _task(
                    "task-pagination",
                    title="Cursor pagination for audit API",
                    description="Add cursor pagination, page size limits, and Retry-After backoff handling.",
                ),
                _task(
                    "task-quota-copy",
                    title="Customer quota exceeded message",
                    description="Show customer-facing quota exceeded copy with retry timing and upgrade prompt.",
                ),
                _task("task-copy", title="Update settings copy", description="Adjust profile labels."),
            ]
        )
    )

    assert isinstance(result, TaskApiRateLimitImpactPlan)
    assert result.plan_id == "plan-rate-limits"
    assert result.impacted_task_ids == (
        "task-inbound",
        "task-outbound",
        "task-batch",
        "task-retry",
        "task-pagination",
        "task-quota-copy",
    )
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary == {
        "task_count": 7,
        "record_count": 6,
        "impacted_task_count": 6,
        "no_impact_task_count": 1,
        "category_counts": {
            "inbound_endpoint": 1,
            "outbound_vendor": 1,
            "batch_burst": 1,
            "retry_amplification": 1,
            "pagination_backoff": 1,
            "customer_quota_messaging": 1,
        },
        "severity_counts": {"high": 4, "medium": 2, "low": 0},
        "impacted_task_ids": [
            "task-inbound",
            "task-outbound",
            "task-batch",
            "task-retry",
            "task-pagination",
            "task-quota-copy",
        ],
        "no_impact_task_ids": ["task-copy"],
    }

    inbound = _record(result, "task-inbound", "inbound_endpoint")
    assert inbound.severity == "high"
    assert inbound.context_signals == ("auth",)

    outbound = _record(result, "task-outbound", "outbound_vendor")
    assert outbound.severity == "high"
    assert outbound.provider_name == "Stripe"
    assert "payment" in outbound.context_signals

    batch = _record(result, "task-batch", "batch_burst")
    assert batch.severity == "high"
    assert batch.context_signals == ("scheduled_bulk",)

    retry = _record(result, "task-retry", "retry_amplification")
    assert retry.severity == "medium"
    assert any("Retry budget" in item for item in retry.mitigation_artifacts)

    pagination = _record(result, "task-pagination", "pagination_backoff")
    assert pagination.severity == "medium"
    assert any("Cursor or page-token" in item for item in pagination.mitigation_artifacts)

    quota_copy = _record(result, "task-quota-copy", "customer_quota_messaging")
    assert quota_copy.severity == "high"
    assert "customer_facing" in quota_copy.context_signals


def test_metadata_can_override_and_enrich_owner_limit_provider_and_artifacts_without_mutation():
    task = _task(
        "task-metadata",
        title="Vendor sync guardrails",
        description="Build integration sync guardrails.",
        metadata={
            "rate_limit_categories": ["outbound_vendor", "retry_amplification"],
            "quota_owner": "platform DRI",
            "limit_value": "100 requests per minute",
            "provider_name": "Salesforce",
            "mitigation_artifacts": ["Quota dashboard | escalation runbook"],
            "validation_commands": {"test": ["poetry run pytest tests/integrations/test_salesforce_quota.py"]},
        },
    )
    original = copy.deepcopy(task)

    result = derive_task_api_rate_limit_impact_plan(_plan([task]))

    assert task == original
    assert {record.category for record in result.records} == {"outbound_vendor", "retry_amplification"}
    for record in result.records:
        assert record.quota_owner == "platform DRI"
        assert record.limit_value == "100 requests per minute"
        assert record.provider_name == "Salesforce"
        assert record.mitigation_artifacts[0] == "Quota dashboard | escalation runbook"
        assert "metadata.rate_limit_categories: " in "; ".join(record.evidence)
    assert any(
        "validation_commands: poetry run pytest tests/integrations/test_salesforce_quota.py" in item
        for record in result.records
        for item in record.evidence
    )


def test_text_only_detection_deduplicates_evidence_and_serializes_stably():
    result = build_task_api_rate_limit_impact_plan(
        {
            "id": "task-text",
            "title": "Public API endpoint | throttle",
            "description": "Public API endpoint needs rate limit policy and 60 requests per minute.",
            "files_or_modules": {
                "first": "src/api/routes/public.py",
                "duplicate": "src/api/routes/public.py",
            },
            "acceptance_criteria": [
                "Public API endpoint returns 429 with Retry-After when the rate limit is exceeded."
            ],
            "status": "pending",
        }
    )
    payload = task_api_rate_limit_impact_plan_to_dict(result)
    markdown = task_api_rate_limit_impact_plan_to_markdown(result)
    empty = build_task_api_rate_limit_impact_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_api_rate_limit_impact_plan(13)
    no_impact = build_task_api_rate_limit_impact_plan(
        _plan([_task("task-ui", title="Profile UI", description="Render profile settings.")])
    )

    record = _record(result, "task-text", "inbound_endpoint")

    assert isinstance(record, TaskApiRateLimitImpactRecord)
    assert record.limit_value == "60 requests per minute"
    assert record.evidence.count("files_or_modules: src/api/routes/public.py") == 1
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert result.findings == result.records
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "records",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "category",
        "severity",
        "quota_owner",
        "limit_value",
        "provider_name",
        "mitigation_artifacts",
        "context_signals",
        "evidence",
    ]
    assert markdown.startswith("# Task API Rate Limit Impact Plan")
    assert "Public API endpoint \\| throttle" in markdown
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert invalid.records == ()
    assert no_impact.records == ()
    assert no_impact.no_impact_task_ids == ("task-ui",)
    assert "No API rate limit impact records were inferred." in no_impact.to_markdown()


def test_execution_plan_execution_task_iterable_and_object_like_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Webhook receiver throttle",
        description="Add rate limiting to incoming webhook receiver route.",
        files_or_modules=["src/api/webhooks/receiver.py"],
        acceptance_criteria=["429 returned when webhook rate limit is exceeded."],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="OpenAI client quota",
            description="Throttle outbound OpenAI provider API requests to stay inside quota.",
            files_or_modules=["src/integrations/openai/client.py"],
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")], plan_id="plan-model"))

    iterable_result = build_task_api_rate_limit_impact_plan([object_task])
    task_result = summarize_task_api_rate_limit_impact(task_model)
    plural_result = summarize_task_api_rate_limit_impacts(task_model)
    plan_result = build_task_api_rate_limit_impact_plan(plan_model)

    assert _record(iterable_result, "task-object", "inbound_endpoint").task_id == "task-object"
    assert _record(task_result, "task-model", "outbound_vendor").provider_name == "OpenAI"
    assert _record(plural_result, "task-model", "outbound_vendor").provider_name == "OpenAI"
    assert plan_result.plan_id == "plan-model"
    assert _record(plan_result, "task-model", "outbound_vendor").provider_name == "OpenAI"


def _record(result, task_id, category):
    return next(record for record in result.records if record.task_id == task_id and record.category == category)


def _plan(tasks, plan_id="plan-rate-limits"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-rate-limits",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    depends_on=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    test_command=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [] if depends_on is None else depends_on,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if test_command is not None:
        task["test_command"] = test_command
    return task
