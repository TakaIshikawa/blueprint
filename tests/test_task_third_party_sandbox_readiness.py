import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_third_party_sandbox_readiness import (
    TaskThirdPartySandboxReadinessPlan,
    TaskThirdPartySandboxReadinessRecord,
    build_task_third_party_sandbox_readiness_plan,
    summarize_task_third_party_sandbox_readiness,
    task_third_party_sandbox_readiness_plan_to_dict,
    task_third_party_sandbox_readiness_plan_to_markdown,
)


def test_detects_known_vendor_and_reports_missing_readiness_criteria():
    result = build_task_third_party_sandbox_readiness_plan(
        _task(
            "task-stripe",
            title="Integrate Stripe checkout",
            description="Use the Stripe SDK and payment webhooks for checkout confirmation.",
            files_or_modules=["src/integrations/stripe_checkout.py"],
            acceptance_criteria=[
                "A Stripe sandbox account is available.",
                "Use test cards as fixture test data.",
                "Sandbox API key is stored separately from production credentials.",
            ],
        )
    )

    assert isinstance(result, TaskThirdPartySandboxReadinessPlan)
    assert result.plan_id is None
    assert result.third_party_task_ids == ("task-stripe",)
    record = result.records[0]
    assert isinstance(record, TaskThirdPartySandboxReadinessRecord)
    assert record.service_names == ("Stripe",)
    assert record.sandbox_needs == (
        "test_account_availability",
        "fixture_test_data_setup",
        "credential_isolation",
        "webhook_endpoint_setup",
        "rate_limit_safe_validation",
        "fallback_manual_test_steps",
    )
    assert record.missing_acceptance_criteria == (
        "Webhook or callback validation uses a non-production endpoint with signature verification and replay steps.",
        "Validation uses throttling, quotas, or bounded test calls to avoid vendor rate-limit impact.",
        "Manual fallback test steps are documented for vendor-dashboard or console verification.",
    )
    assert record.risk_level == "high"
    assert "title: Integrate Stripe checkout" in record.evidence
    assert "files_or_modules: src/integrations/stripe_checkout.py" in record.evidence


def test_detects_generic_external_service_language_without_vendor_name():
    result = summarize_task_third_party_sandbox_readiness(
        [
            _task(
                "task-external",
                title="Build external service client",
                description="Add an integration client for the partner API and OAuth app.",
                acceptance_criteria=[
                    "Dedicated test account exists.",
                    "Fixture test data covers success and error payloads.",
                    "Separate secret is configured for non-production.",
                    "Callback URL is configured for validation.",
                    "Rate limit validation uses throttling.",
                    "Manual verification checklist covers the vendor dashboard.",
                ],
            )
        ]
    )

    record = result.records[0]
    assert record.service_names == ("External service",)
    assert record.risk_level == "low"
    assert record.missing_acceptance_criteria == ()
    assert (
        "description: Add an integration client for the partner API and OAuth app."
        in record.evidence
    )


def test_metadata_tags_and_paths_are_detection_sources_without_mutation():
    plan = _plan(
        [
            _task(
                "task-crm",
                title="Sync lead status",
                description="Map lead stages.",
                files_or_modules=["src/providers/crm/client.py"],
                tags=["salesforce", "integration"],
                metadata={
                    "vendor": "HubSpot",
                    "setup": "Sandbox account, fixtures, env vars, webhook endpoint, quota-safe validation, and manual QA runbook are ready.",
                },
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_third_party_sandbox_readiness_plan(plan)

    assert plan == original
    assert result.plan_id == "plan-third-party"
    record = result.records[0]
    assert record.service_names == ("HubSpot", "Salesforce")
    assert record.risk_level == "low"
    assert record.missing_acceptance_criteria == ()
    assert "metadata.vendor: HubSpot" in record.evidence
    assert "tags[0]: salesforce" in record.evidence


def test_missing_core_acceptance_criteria_raise_risk_level_and_summary_counts():
    result = build_task_third_party_sandbox_readiness_plan(
        _plan(
            [
                _task(
                    "task-email",
                    title="SendGrid transactional email",
                    description="Use SendGrid webhooks for bounced email events.",
                    acceptance_criteria=["Done when emails send successfully."],
                ),
                _task(
                    "task-slack",
                    title="Slack alert publisher",
                    description="Post incident alerts to Slack using the Slack SDK.",
                    acceptance_criteria=[
                        "Test workspace exists.",
                        "Seed data covers success payloads.",
                        "Non-production secret is configured.",
                        "Manual console verification is documented.",
                    ],
                ),
            ]
        )
    )

    assert result.third_party_task_ids == ("task-email", "task-slack")
    assert [record.risk_level for record in result.records] == ["high", "medium"]
    assert result.summary == {
        "task_count": 2,
        "third_party_task_count": 2,
        "missing_acceptance_criterion_count": 8,
        "risk_counts": {"high": 1, "medium": 1, "low": 0},
        "service_counts": {"SendGrid": 1, "Slack": 1},
    }


def test_non_external_task_returns_empty_plan_and_markdown():
    result = build_task_third_party_sandbox_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Adjust profile empty state",
                    description="Update local UI copy.",
                    files_or_modules=["src/ui/profile_copy.py"],
                    metadata={"surface": "profile"},
                )
            ]
        )
    )

    assert result.records == ()
    assert result.third_party_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "third_party_task_count": 0,
        "missing_acceptance_criterion_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "service_counts": {},
    }
    assert result.to_markdown() == (
        "# Task Third-Party Sandbox Readiness Plan: plan-third-party\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Third-party task count: 0\n"
        "- Missing acceptance criterion count: 0\n"
        "- Risk counts: high 0, medium 0, low 0\n"
        "\n"
        "No third-party integration tasks were detected."
    )


def test_execution_plan_model_task_model_and_serialization_are_stable():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="PayPal billing sync",
                    description="Call PayPal APIs.",
                    acceptance_criteria=[
                        "Sandbox account exists.",
                        "Test data fixtures exist.",
                        "Sandbox credential is isolated.",
                        "Webhook endpoint is ready.",
                        "Rate limit tests are throttled.",
                        "Manual test runbook is complete.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )
    task = ExecutionTask.model_validate(
        _task(
            "task-object",
            title="Webhook provider client",
            description="Implement external API webhook client.",
        )
    )

    plan_result = build_task_third_party_sandbox_readiness_plan(plan)
    task_result = build_task_third_party_sandbox_readiness_plan(task)
    payload = task_third_party_sandbox_readiness_plan_to_dict(plan_result)
    markdown = task_third_party_sandbox_readiness_plan_to_markdown(plan_result)

    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].risk_level == "low"
    assert task_result.third_party_task_ids == ("task-object",)
    assert payload == plan_result.to_dict()
    assert plan_result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "third_party_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "risk_level",
        "service_names",
        "sandbox_needs",
        "missing_acceptance_criteria",
        "credential_setup_assumptions",
        "evidence",
    ]
    assert markdown == plan_result.to_markdown()
    assert markdown.startswith("# Task Third-Party Sandbox Readiness Plan: plan-model")
    assert "| `task-model` |" in markdown


def _plan(tasks, plan_id="plan-third-party"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-third-party",
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
    risks=None,
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
    if risks is not None:
        task["risks"] = risks
    return task
