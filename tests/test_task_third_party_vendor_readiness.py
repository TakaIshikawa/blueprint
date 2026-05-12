import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_third_party_vendor_readiness import (
    build_task_third_party_vendor_readiness_plan,
    recommend_task_third_party_vendor_readiness,
    summarize_task_third_party_vendor_readiness,
    task_third_party_vendor_readiness_plan_to_dict,
    task_third_party_vendor_readiness_plan_to_dicts,
    task_third_party_vendor_readiness_plan_to_markdown,
)


def test_detects_vendor_api_task_and_all_required_safeguards():
    result = build_task_third_party_vendor_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Integrate Stripe vendor API",
                    description="Add third-party Stripe API client for billing.",
                    acceptance_criteria=[
                        "Contract owner and SLA are recorded.",
                        "Credentials use vault secret storage with API key rotation.",
                        "Rate limits, retry-after, throttling, and backoff are handled.",
                        "Sandbox testing uses Stripe test mode and integration tests.",
                        "Failure fallback uses timeout, retry, and circuit breaker degraded mode.",
                        "Support escalation includes vendor support, status page, and runbook.",
                    ],
                    files_or_modules=["src/integrations/stripe_api_client.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == ("vendor_integration", "external_api", "named_provider")
    assert record.present_criteria == (
        "contract_ownership",
        "credential_handling",
        "rate_limits",
        "sandbox_testing",
        "failure_fallback",
        "support_escalation",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_metadata_dependencies_and_paths_report_missing_vendor_safeguards():
    source = _plan(
        [
            _task(
                "task-provider",
                title="Add SendGrid provider webhook",
                description="Implement provider webhook integration.",
                depends_on=["vendor API credentials"],
                metadata={"provider": {"quota": "rate limit is 100 requests per minute"}},
                files_or_modules=["src/third_party/sendgrid_webhook.py"],
            ),
            _task("task-docs", title="Docs", description="No vendor API changes are in scope."),
        ]
    )

    result = build_task_third_party_vendor_readiness_plan(ExecutionPlan.model_validate(source))

    assert result.impacted_task_ids == ("task-provider",)
    assert result.ignored_task_ids == ("task-docs",)
    record = result.records[0]
    assert record.present_criteria == ("credential_handling", "rate_limits")
    assert record.missing_criteria == (
        "contract_ownership",
        "sandbox_testing",
        "failure_fallback",
        "support_escalation",
    )
    assert any("depends_on" in item for item in record.evidence)
    assert any("files_or_modules" in item for item in record.evidence)


def test_aliases_serialization_sorting_and_invalid_inputs_are_stable():
    source = _plan(
        [
            _task("task-missing", title="Partner API integration", description="Add external API integration."),
            _task("task-partial", title="Slack provider", description="Slack integration with sandbox tests."),
        ],
        plan_id="plan-vendor-sort",
    )

    result = summarize_task_third_party_vendor_readiness(source)
    payload = task_third_party_vendor_readiness_plan_to_dict(result)
    markdown = task_third_party_vendor_readiness_plan_to_markdown(result)

    assert [record.task_id for record in result.records] == ["task-missing", "task-partial"]
    assert recommend_task_third_party_vendor_readiness(source) == result.records
    assert task_third_party_vendor_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert markdown.startswith("# Task Third-Party Vendor Readiness: plan-vendor-sort")
    assert build_task_third_party_vendor_readiness_plan(42).records == ()
    assert build_task_third_party_vendor_readiness_plan({"tasks": "bad"}).records == ()


def _plan(tasks, *, plan_id="plan-vendor"):
    return {"id": plan_id, "implementation_brief_id": "brief-vendor", "milestones": [], "tasks": tasks}


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    depends_on=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if depends_on is not None:
        task["depends_on"] = depends_on
    if metadata is not None:
        task["metadata"] = metadata
    return task
