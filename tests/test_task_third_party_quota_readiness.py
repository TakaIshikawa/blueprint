import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_third_party_quota_readiness import (
    TaskThirdPartyQuotaReadinessPlan,
    TaskThirdPartyQuotaReadinessRecord,
    analyze_task_third_party_quota_readiness,
    build_task_third_party_quota_readiness_plan,
    extract_task_third_party_quota_readiness,
    generate_task_third_party_quota_readiness,
    recommend_task_third_party_quota_readiness,
    task_third_party_quota_readiness_plan_to_dict,
    task_third_party_quota_readiness_plan_to_dicts,
    task_third_party_quota_readiness_plan_to_markdown,
)


def test_detects_quota_signals_from_task_fields_paths_metadata_and_validation_commands():
    result = build_task_third_party_quota_readiness_plan(
        _plan(
            [
                _task(
                    "task-provider-quota",
                    title="Add OpenAI provider quota planner",
                    description="External provider quota exhaustion can stop enrichment requests.",
                    files_or_modules=["src/integrations/openai/provider_quota.py"],
                ),
                _task(
                    "task-burst",
                    title="Handle Slack burst limits",
                    description="Slack provider has 50 requests per minute burst limit and 429 throttling.",
                    validation_command="poetry run pytest tests/integrations/test_slack_provider_limit_tests.py",
                ),
                _task(
                    "task-metadata",
                    title="Track billing tier and monthly cap",
                    description="Build usage controls for an integration client.",
                    metadata={
                        "provider_name": "SendGrid",
                        "quota": {
                            "monthly_cap": "Monthly cap is 100k emails per month.",
                            "paid_tier": "Paid tier upgrade is required above the free tier.",
                            "quota_alert": "Usage alert fires at 80 percent of quota.",
                        },
                    },
                ),
                _task("task-copy", title="Update settings copy", description="Adjust labels."),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert isinstance(result, TaskThirdPartyQuotaReadinessPlan)
    assert all(isinstance(record, TaskThirdPartyQuotaReadinessRecord) for record in result.records)
    assert by_id["task-provider-quota"].matched_signals == ("provider_quota",)
    assert by_id["task-provider-quota"].provider_name == "OpenAI"
    assert by_id["task-burst"].matched_signals == (
        "provider_quota",
        "burst_limit",
        "provider_throttling",
    )
    assert by_id["task-metadata"].matched_signals == (
        "provider_quota",
        "monthly_cap",
        "paid_tier",
        "quota_alert",
    )
    assert by_id["task-metadata"].provider_name == "SendGrid"
    assert "files_or_modules: src/integrations/openai/provider_quota.py" in by_id["task-provider-quota"].evidence
    assert any("validation_commands: poetry run pytest" in item for item in by_id["task-burst"].evidence)
    assert any("metadata.quota.monthly_cap" in item for item in by_id["task-metadata"].evidence)
    assert result.impacted_task_ids == ("task-burst", "task-metadata", "task-provider-quota")
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary["signal_counts"]["paid_tier"] == 1
    assert result.summary["signal_counts"]["quota_alert"] == 1


def test_high_medium_and_low_impact_classification_and_missing_safeguard_recommendations():
    result = analyze_task_third_party_quota_readiness(
        _plan(
            [
                _task(
                    "task-high",
                    title="Protect paid provider monthly cap",
                    description="OpenAI paid tier has a monthly cap and quota exhaustion stops requests.",
                ),
                _task(
                    "task-medium",
                    title="Smooth provider burst limit",
                    description="Mapbox provider has a burst limit for requests per minute.",
                    acceptance_criteria=[
                        "Budget alarm notifies the owning team.",
                        "Fallback behavior queues work for later.",
                        "Cache strategy serves cached responses.",
                        "Provider-limit tests simulate quota exhaustion.",
                    ],
                ),
                _task(
                    "task-low",
                    title="Ready quota alerts",
                    description="GitHub provider quota alert supports external API quota planning.",
                    acceptance_criteria=[
                        "Budget alerts fire before usage cap is reached.",
                        "Retry backoff uses exponential backoff and jitter.",
                        "Graceful degradation behavior is documented.",
                        "Cache strategy dedupes requests.",
                        "Provider-limit tests cover 429 and quota exhaustion.",
                    ],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert by_id["task-high"].impact_level == "high"
    assert by_id["task-high"].missing_safeguards == (
        "budget_alarms",
        "retry_backoff",
        "degradation_behavior",
        "cache_strategy",
        "provider_limit_tests",
    )
    assert by_id["task-high"].recommended_checks == (
        "Add budget or quota alarms before provider usage reaches billing or service caps.",
        "Define retry, backoff, jitter, and client-side throttling behavior for provider throttles.",
        "Specify degradation or fallback behavior when the provider quota is exhausted.",
        "Use caching, request deduplication, or reuse of provider responses to reduce quota burn.",
        "Add provider-limit tests or simulations for quota exhaustion, 429s, and billing caps.",
    )
    assert by_id["task-medium"].impact_level == "medium"
    assert by_id["task-medium"].missing_safeguards == ("retry_backoff",)
    assert by_id["task-low"].impact_level == "low"
    assert by_id["task-low"].missing_safeguards == ()
    assert by_id["task-low"].recommended_checks == ()
    assert result.summary["impact_counts"] == {"high": 1, "medium": 1, "low": 1}


def test_metadata_evidence_validation_command_evidence_and_no_source_mutation():
    task = _task(
        "task-evidence",
        title="Salesforce quota readiness",
        description="Build third-party quota readiness for Salesforce sync.",
        metadata={
            "provider": "Salesforce",
            "budget_alarm": "Budget alarm and quota alert page the integration owner.",
            "retry_policy": {"retry_backoff": "Retry backoff honors Retry-After for 429 throttling."},
            "validation_commands": {
                "quota": ["poetry run pytest tests/integrations/test_salesforce_quota_exhaustion.py"]
            },
        },
    )
    original = copy.deepcopy(task)

    result = recommend_task_third_party_quota_readiness(_plan([task]))
    record = result.records[0]

    assert task == original
    assert record.provider_name == "Salesforce"
    assert record.present_safeguards == ("budget_alarms", "retry_backoff", "provider_limit_tests")
    assert any("metadata.budget_alarm" in item for item in record.evidence)
    assert any("metadata.retry_policy.retry_backoff" in item for item in record.evidence)
    assert any(
        "metadata.validation_commands.quota[0]: poetry run pytest tests/integrations/test_salesforce_quota_exhaustion.py"
        in item
        for item in record.evidence
    )


def test_empty_no_impact_invalid_inputs_and_serialization_are_stable():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Quota readiness | OpenAI",
                description="OpenAI provider quota alert covers paid tier usage.",
            ),
            _task(
                "task-a",
                title="Monthly provider cap",
                description="Monthly cap for SendGrid has budget alarms and provider-limit tests.",
            ),
            _task(
                "task-copy",
                title="Profile UI copy",
                description="No third-party provider quota changes are in scope.",
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_third_party_quota_readiness_plan(plan)
    payload = task_third_party_quota_readiness_plan_to_dict(result)
    markdown = task_third_party_quota_readiness_plan_to_markdown(result)
    invalid = build_task_third_party_quota_readiness_plan(42)
    empty = build_task_third_party_quota_readiness_plan({"id": "empty-plan", "tasks": []})

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_third_party_quota_readiness_plan_to_dicts(result) == payload["records"]
    assert task_third_party_quota_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_third_party_quota_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_third_party_quota_readiness(plan).to_dict() == result.to_dict()
    assert result.recommendations == result.records
    assert result.findings == result.records
    assert result.impacted_task_ids == ("task-a", "task-z")
    assert result.no_impact_task_ids == ("task-copy",)
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
        "impact_level",
        "matched_signals",
        "provider_name",
        "present_safeguards",
        "missing_safeguards",
        "recommended_checks",
        "evidence",
    ]
    assert markdown.startswith("# Task Third-Party Quota Readiness: plan-third-party-quota")
    assert "Quota readiness \\| OpenAI" in markdown
    assert invalid.records == ()
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert "No third-party quota readiness records were inferred." in invalid.to_markdown()


def test_execution_plan_execution_task_iterable_and_object_like_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Twilio monthly usage cap",
        description="Twilio provider monthly cap needs quota alert behavior.",
        files_or_modules=["src/integrations/twilio/monthly_cap.py"],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Anthropic provider throttling fallback",
            description="Anthropic provider throttling uses fallback behavior and retry backoff.",
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")], plan_id="plan-model"))

    iterable_result = build_task_third_party_quota_readiness_plan([object_task])
    task_result = build_task_third_party_quota_readiness_plan(task_model)
    plan_result = build_task_third_party_quota_readiness_plan(plan_model)

    assert iterable_result.records[0].task_id == "task-object"
    assert iterable_result.records[0].provider_name == "Twilio"
    assert task_result.records[0].provider_name == "Anthropic"
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-model"


def _plan(tasks, plan_id="plan-third-party-quota"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-third-party-quota",
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
    validation_command=None,
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
    if validation_command is not None:
        task["validation_command"] = validation_command
    return task
