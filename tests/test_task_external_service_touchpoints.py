import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_external_service_touchpoints import (
    TaskExternalServiceEvidence,
    plan_task_external_service_touchpoints,
    task_external_service_touchpoints_to_dict,
)


def test_detects_webhook_and_payment_provider_touchpoints_from_task_fields():
    report = plan_task_external_service_touchpoints(
        _plan(
            [
                _task(
                    "task-stripe",
                    description="Handle Stripe webhook retries and signature verification.",
                    acceptance_criteria=[
                        "Payment webhook duplicate delivery is idempotent and declined payments are handled."
                    ],
                    test_command="pytest tests/integrations/test_stripe_webhook.py --allow-net",
                    files=["src/integrations/stripe_webhook.py"],
                    metadata={"webhook_secret": "STRIPE_WEBHOOK_SECRET"},
                )
            ]
        )
    )

    task = report.tasks[0]
    services = {touchpoint.service for touchpoint in task.touchpoints}

    assert {"Stripe", "Webhook"}.issubset(services)
    stripe = _touchpoint(task, "Stripe")
    webhook = _touchpoint(task, "Webhook")
    assert stripe.category == "payments"
    assert "Stripe secret key" in stripe.credential_hint
    assert "idempotency" in stripe.failure_mode
    assert "idempotency keys" in stripe.retry_or_fallback_guidance
    assert "decline" in stripe.validation_evidence
    assert "metadata.webhook_secret" in {item.source for item in webhook.evidence}
    assert "test_command" in {item.source for item in stripe.evidence}


def test_detects_saas_apis_and_uses_plan_integration_points_as_context():
    report = plan_task_external_service_touchpoints(
        _plan(
            [
                _task(
                    "task-sync",
                    description="Build issue sync client and notification digest.",
                    acceptance_criteria=[
                        "External project updates create tickets and post chat notifications."
                    ],
                    files=["src/integrations/issue_sync.py", "src/notifications/slack_digest.py"],
                    metadata={"target": "Linear workspace"},
                ),
                _task(
                    "task-parser",
                    description="Refactor local parser only.",
                    acceptance_criteria=["Parser returns deterministic local errors."],
                    files=["src/parser.py"],
                ),
            ],
            integration_points=["GitHub issues API", "Slack notifications", "Jira import"],
        )
    )

    assert [task.task_id for task in report.tasks] == ["task-sync", "task-parser"]
    assert [touchpoint.service for touchpoint in report.tasks[0].touchpoints] == [
        "GitHub",
        "Slack",
        "Jira",
        "Linear",
    ]
    assert report.tasks[1].touchpoints == ()

    github = _touchpoint(report.tasks[0], "GitHub")
    slack = _touchpoint(report.tasks[0], "Slack")

    assert github.category == "source_control"
    assert github.evidence == (
        TaskExternalServiceEvidence(
            source="integration_points",
            detail="integration_points references GitHub: GitHub issues API",
        ),
    )
    assert slack.category == "chat"
    assert "Slack bot token" in slack.credential_hint
    assert "rate-limit" in slack.validation_evidence
    assert report.summary["service_counts"] == {
        "GitHub": 1,
        "Jira": 1,
        "Linear": 1,
        "Slack": 1,
    }


def test_detects_llm_providers_email_storage_and_databases_from_multiple_sources():
    report = plan_task_external_service_touchpoints(
        _plan(
            [
                _task(
                    "task-ai-email",
                    description="Call OpenAI and Anthropic models before sending an email summary.",
                    acceptance_criteria=[
                        "LLM timeout falls back to cached copy and SendGrid rejection is reported."
                    ],
                    test_command="pytest tests/test_ai_email.py",
                    files=["src/ai/openai_client.py", "src/mail/sendgrid_sender.py"],
                    metadata={"provider": "claude", "fallback_model": "gpt-4.1-mini"},
                ),
                _task(
                    "task-storage-db",
                    description="Persist export metadata and upload artifacts to S3.",
                    acceptance_criteria=[
                        "Postgres transaction rollback and object storage permission failures are covered."
                    ],
                    files=["src/db/export_repository.py", "src/storage/s3_artifacts.py"],
                    metadata={"database_url": "POSTGRES_DSN", "bucket": "exports"},
                ),
            ]
        )
    )

    assert [touchpoint.service for touchpoint in report.tasks[0].touchpoints] == [
        "OpenAI",
        "Anthropic",
        "Email",
    ]
    assert [touchpoint.service for touchpoint in report.tasks[1].touchpoints] == [
        "Object storage",
        "Database",
    ]

    openai = _touchpoint(report.tasks[0], "OpenAI")
    anthropic = _touchpoint(report.tasks[0], "Anthropic")
    email = _touchpoint(report.tasks[0], "Email")
    storage = _touchpoint(report.tasks[1], "Object storage")
    database = _touchpoint(report.tasks[1], "Database")

    assert openai.category == "llm_provider"
    assert "quota" in openai.failure_mode
    assert anthropic.category == "llm_provider"
    assert "Anthropic API key" in anthropic.credential_hint
    assert email.category == "email"
    assert "bounced" in email.validation_evidence
    assert storage.category == "object_storage"
    assert "bucket" in storage.credential_hint
    assert database.category == "database"
    assert "disposable state" in database.validation_evidence
    assert report.summary["category_counts"] == {
        "database": 1,
        "email": 1,
        "llm_provider": 2,
        "object_storage": 1,
    }


def test_supports_execution_plan_model_input_and_serializes_deterministically():
    plan = _plan(
        [
            _task(
                "task-model",
                description="Implement API client for the configured external provider.",
                acceptance_criteria=["Provider timeout has fallback behavior."],
                files=["src/integrations/provider_client.py"],
            )
        ],
        metadata={"integration_points": ["OpenAI responses API", "AWS S3 artifact bucket"]},
    )

    report = plan_task_external_service_touchpoints(ExecutionPlan.model_validate(plan))
    payload = task_external_service_touchpoints_to_dict(report)

    assert [touchpoint.service for touchpoint in report.tasks[0].touchpoints] == [
        "OpenAI",
        "Object storage",
    ]
    assert report.summary == {
        "task_count": 1,
        "tasks_with_touchpoints": 1,
        "touchpoint_count": 2,
        "service_counts": {"Object storage": 1, "OpenAI": 1},
        "category_counts": {"llm_provider": 1, "object_storage": 1},
    }
    assert json.loads(json.dumps(payload)) == payload


def test_no_touchpoint_task_has_empty_guidance_and_summary_counts():
    report = plan_task_external_service_touchpoints(
        _plan(
            [
                _task(
                    "task-unit",
                    description="Refactor deterministic CLI parser.",
                    acceptance_criteria=["Quoted local arguments parse correctly."],
                    test_command="pytest tests/test_cli_parser.py",
                    files=["src/cli_parser.py", "tests/test_cli_parser.py"],
                )
            ]
        )
    )

    assert report.tasks[0].touchpoints == ()
    assert task_external_service_touchpoints_to_dict(report)["summary"] == {
        "task_count": 1,
        "tasks_with_touchpoints": 0,
        "touchpoint_count": 0,
        "service_counts": {},
        "category_counts": {},
    }


def _touchpoint(task, service):
    for touchpoint in task.touchpoints:
        if touchpoint.service == service:
            return touchpoint
    raise AssertionError(f"missing touchpoint for {service}")


def _plan(tasks, *, plan_id="plan-test", integration_points=None, metadata=None):
    plan = {
        "id": plan_id,
        "implementation_brief_id": "brief-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Build", "description": "Build the feature"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "metadata": metadata or {},
        "tasks": tasks,
    }
    if integration_points is not None:
        plan["integration_points"] = integration_points
    return plan


def _task(
    task_id,
    *,
    description=None,
    files=None,
    acceptance_criteria=None,
    test_command=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "description": description or f"Implement {task_id}",
        "milestone": "Build",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files or ["src/app.py"],
        "acceptance_criteria": acceptance_criteria or [f"{task_id} works"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
