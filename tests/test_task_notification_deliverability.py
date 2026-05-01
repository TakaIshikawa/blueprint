import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_notification_deliverability import (
    TaskNotificationDeliverabilityPlan,
    TaskNotificationDeliverabilityRecord,
    build_task_notification_deliverability_plan,
    derive_task_notification_deliverability_plan,
    summarize_task_notification_deliverability,
    task_notification_deliverability_plan_to_dict,
    task_notification_deliverability_plan_to_markdown,
)


def test_notification_channels_are_detected_and_summarized_by_category_and_severity():
    result = build_task_notification_deliverability_plan(
        _plan(
            [
                _task(
                    "task-email",
                    title="Receipt email delivery",
                    description=(
                        "Send receipt email through SendGrid with template review, retry backoff, "
                        "provider sandbox validation, unsubscribe preferences, bounce tracking, "
                        "delivery monitoring, rate limiting, and customer communication checks."
                    ),
                    files_or_modules=["src/notifications/email/receipt_template.py"],
                ),
                _task(
                    "task-sms",
                    title="SMS reminder",
                    description="Send SMS text message reminders through Twilio with retry backoff and delivery metrics.",
                    files_or_modules=["src/notifications/sms/twilio_reminders.py"],
                ),
                _task(
                    "task-push",
                    title="Mobile push delivery",
                    description=(
                        "Deliver push notifications with APNs and FCM using device token lifecycle, "
                        "retry handling, provider sandbox checks, rate limits, and delivery dashboard metrics."
                    ),
                    files_or_modules=["mobile/push/device_tokens.ts"],
                ),
                _task(
                    "task-webhook",
                    title="Partner webhook events",
                    description="Deliver webhooks to partner callback URLs with HMAC signing and retry schedule.",
                    files_or_modules=["src/webhooks/partner_delivery.py"],
                ),
                _task(
                    "task-in-app",
                    title="In-app notification inbox",
                    description="Add in-app notification center read and unread state.",
                    files_or_modules=["src/notifications/in_app/inbox.tsx"],
                ),
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Clarify labels on account settings.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskNotificationDeliverabilityPlan)
    assert result.plan_id == "plan-notifications"
    assert result.notification_task_ids == ("task-sms", "task-webhook", "task-in-app", "task-email", "task-push")
    assert result.no_signal_task_ids == ("task-copy",)
    assert result.summary == {
        "task_count": 6,
        "notification_task_count": 5,
        "no_signal_task_count": 1,
        "channel_counts": {"email": 1, "sms": 1, "push": 1, "webhook": 1, "in_app": 1},
        "severity_counts": {"high": 2, "medium": 1, "low": 2},
    }

    email = _record(result, "task-email")
    assert isinstance(email, TaskNotificationDeliverabilityRecord)
    assert email.severity == "low"
    assert email.channels == ("email",)
    assert email.deliverability_categories == ("email_delivery",)
    assert email.providers == ("SendGrid",)
    assert any("unsubscribe" in item.lower() for item in email.unsubscribe_requirements)
    assert any("bounce" in item.lower() for item in email.monitoring_signals)
    assert any("Bounce, complaint" in item for item in email.recommended_artifacts)

    sms = _record(result, "task-sms")
    assert sms.severity == "high"
    assert sms.channels == ("sms",)
    assert sms.deliverability_categories == ("sms_delivery",)
    assert sms.providers == ("Twilio",)
    assert any("Unsubscribe, opt-out" in item for item in sms.recommended_artifacts)

    push = _record(result, "task-push")
    assert push.severity == "low"
    assert push.channels == ("push",)
    assert push.deliverability_categories == ("push_delivery",)
    assert push.providers == ("APNs", "FCM")
    assert any("Device-token lifecycle" in item for item in push.recommended_artifacts)

    webhook = _record(result, "task-webhook")
    assert webhook.severity == "high"
    assert webhook.channels == ("webhook",)
    assert webhook.deliverability_categories == ("webhook_delivery",)
    assert any("Webhook signing" in item for item in webhook.recommended_artifacts)

    in_app = _record(result, "task-in-app")
    assert in_app.severity == "medium"
    assert in_app.channels == ("in_app",)
    assert in_app.deliverability_categories == ("in_app_delivery",)
    assert any("Notification center" in item for item in in_app.recommended_artifacts)


def test_metadata_can_explicitly_provide_delivery_controls_and_evidence_is_deduped():
    result = derive_task_notification_deliverability_plan(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Account alert delivery",
                    description="Implement account alert notifications.",
                    files_or_modules={
                        "first": "src/notifications/email/account_delivery.py",
                        "duplicate": "src/notifications/email/account_delivery.py",
                    },
                    acceptance_criteria={
                        "delivery": "Postmark sandbox receives account alert email delivery receipt."
                    },
                    metadata={
                        "channels": ["email"],
                        "providers": ["Postmark"],
                        "retry_strategy": "3 retries with exponential backoff and DLQ after final failure.",
                        "unsubscribe_requirements": "Respect account notification preferences and suppression list.",
                        "monitoring_signals": ["delivery rate", "bounce rate", "provider latency alert"],
                        "validation_commands": {"test": ["poetry run pytest tests/alerts/test_postmark_delivery.py"]},
                    },
                )
            ]
        )
    )

    record = result.records[0]

    assert record.task_id == "task-metadata"
    assert record.channels == ("email",)
    assert record.providers == ("Postmark",)
    assert record.severity == "low"
    assert any("exponential backoff" in item for item in record.retry_strategy)
    assert any("notification preferences" in item for item in record.unsubscribe_requirements)
    assert any("delivery rate" in item for item in record.monitoring_signals)
    assert record.evidence.count("files_or_modules: src/notifications/email/account_delivery.py") == 1
    assert "metadata.channels[0]: email" in record.evidence
    assert "metadata.providers[0]: Postmark" in record.evidence
    assert "validation_commands: poetry run pytest tests/alerts/test_postmark_delivery.py" in record.evidence


def test_empty_invalid_no_signal_serialization_markdown_and_escaping_are_stable():
    task_dict = _task(
        "task-email | pipe",
        title="Email alert | template",
        description="Send email alert with provider monitoring and retry backoff.",
        files_or_modules=["src/notifications/email/alert_template.py"],
    )
    original = copy.deepcopy(task_dict)

    result = build_task_notification_deliverability_plan(_plan([task_dict]))
    payload = task_notification_deliverability_plan_to_dict(result)
    markdown = task_notification_deliverability_plan_to_markdown(result)
    empty = build_task_notification_deliverability_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_notification_deliverability_plan(13)
    no_signal = build_task_notification_deliverability_plan(
        _plan([_task("task-ui", title="Add profile UI", description="Render profile settings.")])
    )

    assert task_dict == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert result.findings == result.records
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "records",
        "notification_task_ids",
        "no_signal_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "severity",
        "channels",
        "deliverability_categories",
        "providers",
        "retry_strategy",
        "unsubscribe_requirements",
        "monitoring_signals",
        "recommended_artifacts",
        "evidence",
    ]
    assert markdown.startswith("# Task Notification Deliverability Plan: plan-notifications")
    assert "Summary: 1 notification tasks" in markdown
    assert "Email alert \\| template" in markdown
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert empty.summary["task_count"] == 0
    assert invalid.plan_id is None
    assert invalid.records == ()
    assert no_signal.records == ()
    assert no_signal.no_signal_task_ids == ("task-ui",)
    assert "No notification deliverability records were inferred." in no_signal.to_markdown()
    assert "No-signal tasks: task-ui" in no_signal.to_markdown()


def test_execution_plan_execution_task_iterable_and_object_like_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Webhook delivery receipts",
        description="Monitor webhook delivery receipts with retry and DLQ handling.",
        files_or_modules=["src/webhooks/delivery_receipts.py"],
        acceptance_criteria=["Webhook dashboard shows delivery failure rate."],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="SMS opt-out handling",
            description="Twilio SMS STOP keyword opt-out handling with retry monitoring.",
            files_or_modules=["src/notifications/sms/opt_out.py"],
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")], plan_id="plan-model"))

    iterable_result = build_task_notification_deliverability_plan([object_task])
    task_result = summarize_task_notification_deliverability(task_model)
    plan_result = build_task_notification_deliverability_plan(plan_model)

    assert iterable_result.records[0].task_id == "task-object"
    assert iterable_result.records[0].channels == ("webhook",)
    assert task_result.records[0].task_id == "task-model"
    assert task_result.records[0].channels == ("sms",)
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-model"


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks, plan_id="plan-notifications"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-notifications",
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
