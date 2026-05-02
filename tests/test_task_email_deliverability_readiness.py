import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_email_deliverability_readiness import (
    TaskEmailDeliverabilityReadinessPlan,
    TaskEmailDeliverabilityReadinessRecord,
    analyze_task_email_deliverability_readiness,
    build_task_email_deliverability_readiness_plan,
    extract_task_email_deliverability_readiness,
    generate_task_email_deliverability_readiness,
    recommend_task_email_deliverability_readiness,
    summarize_task_email_deliverability_readiness,
    task_email_deliverability_readiness_plan_to_dict,
    task_email_deliverability_readiness_plan_to_dicts,
    task_email_deliverability_readiness_plan_to_markdown,
)


def test_missing_readiness_detects_email_flows_and_safeguard_gaps():
    result = build_task_email_deliverability_readiness_plan(
        _plan(
            [
                _task(
                    "task-email",
                    title="Add transactional email notifications",
                    description=(
                        "Send outbound email for order confirmation and notification emails "
                        "through the email provider."
                    ),
                    files_or_modules=[
                        "src/mail/transactional_email.py",
                        "templates/email/order_confirmation.mjml",
                    ],
                    acceptance_criteria=["Users receive the receipt email after checkout."],
                )
            ]
        )
    )

    assert isinstance(result, TaskEmailDeliverabilityReadinessPlan)
    assert result.email_task_ids == ("task-email",)
    record = result.records[0]
    assert isinstance(record, TaskEmailDeliverabilityReadinessRecord)
    assert {
        "outbound_email",
        "notification_email",
        "transactional_email",
        "email_template",
        "email_provider",
    } <= set(record.detected_signals)
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "spf",
        "dkim",
        "dmarc",
        "unsubscribe_preferences",
        "bounce_complaint_handling",
        "rate_limiting",
        "template_rendering_tests",
        "localization_accessibility",
        "fallback_support_visibility",
    )
    assert record.readiness_level == "weak"
    assert "files_or_modules: src/mail/transactional_email.py" in record.evidence
    assert any("SPF" in check for check in record.recommended_checks)
    assert result.summary["email_task_count"] == 1
    assert result.summary["signal_counts"]["transactional_email"] == 1


def test_strong_readiness_has_no_missing_safeguards():
    result = build_task_email_deliverability_readiness_plan(
        _plan(
            [
                _task(
                    "task-digest",
                    title="Launch weekly digest email",
                    description=(
                        "Weekly digest email via Postmark uses SPF, DKIM, and DMARC alignment. "
                        "Add unsubscribe and email preference handling, bounce handling and complaint "
                        "webhooks, rate limiting with backoff, template rendering tests, localization "
                        "and accessibility review, plus support visibility with delivery logs and resend fallback."
                    ),
                    files_or_modules=["src/emails/digest/templates/weekly_digest.mjml"],
                )
            ]
        )
    )

    record = result.records[0]
    assert {"digest_email", "email_provider", "email_template"} <= set(record.detected_signals)
    assert record.present_safeguards == (
        "spf",
        "dkim",
        "dmarc",
        "unsubscribe_preferences",
        "bounce_complaint_handling",
        "rate_limiting",
        "template_rendering_tests",
        "localization_accessibility",
        "fallback_support_visibility",
    )
    assert record.missing_safeguards == ()
    assert record.recommended_checks == ()
    assert record.readiness_level == "strong"
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "strong": 1}


def test_partial_readiness_and_recommended_checks_are_actionable():
    result = build_task_email_deliverability_readiness_plan(
        _plan(
            [
                _task(
                    "task-reset",
                    title="Add password reset email",
                    description=(
                        "Forgot password email uses AWS SES with SPF, DKIM, and DMARC. "
                        "Template rendering tests cover reset links and plain text fallback."
                    ),
                    acceptance_criteria=["Rate limit password reset email requests."],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == (
        "outbound_email",
        "password_reset_email",
        "email_template",
        "email_provider",
    )
    assert record.present_safeguards == ("spf", "dkim", "dmarc", "rate_limiting", "template_rendering_tests")
    assert record.missing_safeguards == (
        "bounce_complaint_handling",
        "localization_accessibility",
        "fallback_support_visibility",
    )
    assert record.readiness_level == "partial"
    assert record.recommended_checks == (
        "Wire bounce and complaint webhooks into suppression handling and operational alerts.",
        "Review email content for localization coverage, accessible markup, alt text, and readable contrast.",
        "Document fallback/resend behavior and expose delivery state or logs to support.",
    )


def test_signals_are_detected_from_title_description_files_and_metadata():
    result = analyze_task_email_deliverability_readiness(
        _plan(
            [
                _task(
                    "task-invites",
                    title="Invite email rollout",
                    description="Send team invitation emails with magic link through SendGrid.",
                    files_or_modules=["app/notifications/email_invites/sendgrid_provider.py"],
                    metadata={
                        "email_deliverability": {
                            "authentication": ["spf", "dkim", "dmarc"],
                            "bounce_complaint_handling": "Use complaint webhooks and suppression list.",
                            "support_visibility": "Expose message logs for support.",
                        }
                    },
                    tags=["rate limiting", "accessibility"],
                )
            ]
        )
    )

    record = result.records[0]
    assert {"outbound_email", "invite_email", "email_provider"} <= set(record.detected_signals)
    assert record.present_safeguards == (
        "spf",
        "dkim",
        "dmarc",
        "bounce_complaint_handling",
        "rate_limiting",
        "localization_accessibility",
        "fallback_support_visibility",
    )
    assert "unsubscribe_preferences" not in record.missing_safeguards
    assert "template_rendering_tests" in record.missing_safeguards
    assert any("metadata.email_deliverability.authentication" in item for item in record.evidence)
    assert any("tags[0]" in item for item in record.evidence)


def test_unrelated_tasks_empty_and_malformed_inputs_are_stable():
    no_op = build_task_email_deliverability_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard empty state",
                    description="Adjust labels and helper text.",
                    files_or_modules=["src/blueprint/ui/dashboard_copy.py"],
                )
            ]
        )
    )
    malformed = build_task_email_deliverability_readiness_plan({"tasks": "not-a-list"})

    assert build_task_email_deliverability_readiness_plan(None).to_dicts() == []
    assert build_task_email_deliverability_readiness_plan("not a task").summary["task_count"] == 0
    assert malformed.records == ()
    assert malformed.summary["task_count"] == 0
    assert no_op.records == ()
    assert no_op.email_task_ids == ()
    assert no_op.no_signal_task_ids == ("task-copy",)
    assert no_op.summary == {
        "task_count": 1,
        "email_task_count": 0,
        "no_signal_task_count": 1,
        "missing_safeguard_count": 0,
        "readiness_counts": {"weak": 0, "partial": 0, "strong": 0},
        "signal_counts": {
            "outbound_email": 0,
            "notification_email": 0,
            "invite_email": 0,
            "password_reset_email": 0,
            "digest_email": 0,
            "transactional_email": 0,
            "email_template": 0,
            "email_provider": 0,
        },
        "present_safeguard_counts": {
            "spf": 0,
            "dkim": 0,
            "dmarc": 0,
            "unsubscribe_preferences": 0,
            "bounce_complaint_handling": 0,
            "rate_limiting": 0,
            "template_rendering_tests": 0,
            "localization_accessibility": 0,
            "fallback_support_visibility": 0,
        },
        "missing_safeguard_counts": {
            "spf": 0,
            "dkim": 0,
            "dmarc": 0,
            "unsubscribe_preferences": 0,
            "bounce_complaint_handling": 0,
            "rate_limiting": 0,
            "template_rendering_tests": 0,
            "localization_accessibility": 0,
            "fallback_support_visibility": 0,
        },
    }
    assert "No outbound or transactional email tasks" in no_op.to_markdown()


def test_serialization_markdown_aliases_order_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Notification email | support",
                description="Notification emails include SPF and DKIM only.",
            ),
            _task(
                "task-a",
                title="Digest email",
                description=(
                    "Digest email has SPF, DKIM, DMARC, unsubscribe preferences, bounce handling, "
                    "rate limiting, render tests, localization, accessibility, and delivery logs."
                ),
            ),
            _task("task-copy", title="Update copy", description="Adjust static wording."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_email_deliverability_readiness(plan)
    payload = task_email_deliverability_readiness_plan_to_dict(result)
    markdown = task_email_deliverability_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_email_deliverability_readiness_plan_to_dicts(result) == payload["records"]
    assert task_email_deliverability_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_email_deliverability_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_email_deliverability_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_email_deliverability_readiness(plan).to_dict() == result.to_dict()
    assert result.recommendations == result.records
    assert result.email_task_ids == ("task-z", "task-a")
    assert result.no_signal_task_ids == ("task-copy",)
    assert list(payload) == [
        "plan_id",
        "records",
        "recommendations",
        "email_task_ids",
        "no_signal_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "readiness_level",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "evidence",
        "recommended_checks",
    ]
    assert [record.readiness_level for record in result.records] == ["weak", "strong"]
    assert markdown.startswith("# Task Email Deliverability Readiness Plan: plan-email")
    assert "Notification email \\| support" in markdown


def test_execution_plan_execution_task_and_object_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add notification emails",
        description="Email notifications through Mailgun include SPF, DKIM, DMARC, and bounce webhooks.",
        files_or_modules=["src/notifications/email_sender.py"],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Password reset email",
            description="Password reset email through SMTP includes SPF and DKIM.",
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([model_task.model_dump(mode="python")], plan_id="plan-model"))

    first = build_task_email_deliverability_readiness_plan([object_task])
    second = build_task_email_deliverability_readiness_plan(plan_model)

    assert first.records[0].task_id == "task-object"
    assert "notification_email" in first.records[0].detected_signals
    assert "bounce_complaint_handling" in first.records[0].present_safeguards
    assert second.plan_id == "plan-model"
    assert second.records[0].task_id == "task-model"


def _plan(tasks, plan_id="plan-email"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-email",
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
    metadata=None,
    tags=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-email",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "medium",
        "estimated_hours": 2.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        payload["tags"] = tags
    return payload
