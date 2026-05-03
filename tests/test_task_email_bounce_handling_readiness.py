import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_email_bounce_handling_readiness import (
    TaskEmailBounceHandlingReadinessPlan,
    TaskEmailBounceHandlingReadinessRecord,
    analyze_task_email_bounce_handling_readiness,
    build_task_email_bounce_handling_readiness_plan,
    derive_task_email_bounce_handling_readiness,
    extract_task_email_bounce_handling_readiness,
    generate_task_email_bounce_handling_readiness,
    recommend_task_email_bounce_handling_readiness,
    summarize_task_email_bounce_handling_readiness,
    task_email_bounce_handling_readiness_plan_to_dict,
    task_email_bounce_handling_readiness_plan_to_dicts,
    task_email_bounce_handling_readiness_plan_to_markdown,
)


def test_detects_bounce_work_from_text_paths_tags_metadata_and_validation_commands():
    result = build_task_email_bounce_handling_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Wire provider bounce webhooks",
                    description="Move implementation files.",
                    files_or_modules=[
                        "src/email/bounce_processor.py",
                        "src/email/hard_bounce_suppression.py",
                        "src/email/soft_bounce_retry_policy.py",
                        "src/email/complaint_feedback_loop.py",
                        "src/email/unsubscribe_interaction.py",
                        "src/email/delivery_status_store.py",
                        "src/email/reactivation_policy.py",
                    ],
                    tags=["suppression-list"],
                    validation_commands={
                        "webhook": "pytest tests/email/test_sendgrid_webhook_signature.py",
                    },
                ),
                _task(
                    "task-text",
                    title="Handle email bounces and complaints",
                    description=(
                        "Bounce handling covers hard bounce suppression, soft bounce retry policy, "
                        "complaint handling, suppression list updates, unsubscribe interaction, provider webhook "
                        "verification, delivery status persistence, dashboards and alerts, "
                        "and reactivation policy."
                    ),
                    metadata={
                        "bounce_handling": {
                            "provider_webhook_verification": "Verify webhook signature before processing.",
                        }
                    },
                ),
            ]
        )
    )

    assert isinstance(result, TaskEmailBounceHandlingReadinessPlan)
    assert result.plan_id == "plan-email-bounce"
    by_id = {record.task_id: record for record in result.records}
    assert set(by_id) == {"task-paths", "task-text"}
    assert set(by_id["task-paths"].bounce_signals) == {
        "bounce_handling",
        "hard_bounce",
        "soft_bounce",
        "complaint_handling",
        "suppression_list",
        "unsubscribe_interaction",
        "provider_webhook",
        "delivery_status",
        "reactivation",
    }
    assert by_id["task-text"].present_safeguards == (
        "hard_bounce_suppression",
        "soft_bounce_retry_policy",
        "complaint_handling",
        "unsubscribe_interaction",
        "provider_webhook_verification",
        "delivery_status_persistence",
        "dashboard_alerting",
        "reactivation_policy",
    )
    assert any("files_or_modules:" in item for item in by_id["task-paths"].evidence)
    assert any("tags[0]:" in item for item in by_id["task-paths"].evidence)
    assert any("validation_commands" in item for item in by_id["task-paths"].evidence)
    assert any("metadata.bounce_handling.provider_webhook_verification" in item for item in by_id["task-text"].evidence)


def test_recommends_missing_safeguards_and_marks_strong_when_all_present():
    result = analyze_task_email_bounce_handling_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Harden bounce handling rollout",
                    description=(
                        "Email bounce handling suppresses hard bounces, uses a soft bounce retry policy "
                        "with backoff, processes spam complaints, defines unsubscribe interaction, "
                        "requires webhook signature verification, persists delivery status, adds "
                        "bounce rate dashboard alerts, and documents reactivation policy."
                    ),
                ),
                _task(
                    "task-weak",
                    title="Receive bounce events",
                    description="Process provider webhook bounce events from SendGrid.",
                    acceptance_criteria=["Bounce rate dashboard is visible."],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    strong = by_id["task-strong"]
    weak = by_id["task-weak"]

    assert strong.missing_safeguards == ()
    assert strong.recommendations == ()
    assert strong.readiness == "strong"
    assert weak.readiness == "weak"
    assert weak.missing_safeguards == (
        "hard_bounce_suppression",
        "soft_bounce_retry_policy",
        "complaint_handling",
        "unsubscribe_interaction",
        "provider_webhook_verification",
        "delivery_status_persistence",
        "reactivation_policy",
    )
    assert weak.recommendations[0] == "Suppress hard-bounced recipients before any future send attempts."
    assert result.bounce_task_ids == ("task-weak", "task-strong")
    assert result.summary["readiness_counts"] == {"weak": 1, "moderate": 0, "strong": 1}
    assert result.summary["missing_safeguard_counts"]["hard_bounce_suppression"] == 1


def test_moderate_readiness_when_core_safeguards_exist_but_optional_safeguards_are_missing():
    result = build_task_email_bounce_handling_readiness_plan(
        _plan(
            [
                _task(
                    "task-moderate",
                    title="Build bounce handling worker",
                    description=(
                        "Bounce handling suppresses hard bounces, uses soft bounce retry policy, "
                        "verifies provider webhook signature, persists delivery status for recipients, "
                        "and emits bounce metrics."
                    ),
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "moderate"
    assert record.risk_level == record.readiness
    assert record.present_safeguards == (
        "hard_bounce_suppression",
        "soft_bounce_retry_policy",
        "provider_webhook_verification",
        "delivery_status_persistence",
        "dashboard_alerting",
    )
    assert record.missing_safeguards == (
        "complaint_handling",
        "unsubscribe_interaction",
        "reactivation_policy",
    )
    assert record.recommendations == (
        "Process spam or abuse complaints and add complained recipients to suppression handling.",
        "Specify how unsubscribes, preference changes, bounces, and complaints interact.",
        "Document when and how suppressed recipients can be reviewed, reactivated, or resubscribed.",
    )


def test_unrelated_and_documentation_only_inputs_are_suppressed_with_deterministic_summary():
    result = build_task_email_bounce_handling_readiness_plan(
        _plan(
            [
                _task("task-copy", title="Update dashboard copy", description="Text only."),
                _task(
                    "task-docs",
                    title="Document bounce handling runbook",
                    description="Documentation for bounce handling and suppression list behavior.",
                    files_or_modules=["docs/email/bounce-handling.md"],
                ),
            ]
        )
    )
    empty = build_task_email_bounce_handling_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_email_bounce_handling_readiness_plan(13)

    assert result.records == ()
    assert result.bounce_task_ids == ()
    assert result.suppressed_task_ids == ("task-copy", "task-docs")
    assert result.summary["bounce_task_count"] == 0
    assert result.summary["suppressed_task_count"] == 2
    assert empty.records == ()
    assert invalid.records == ()
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Email Bounce Handling Readiness: empty-plan",
            "",
            "## Summary",
            "",
            "- Task count: 0",
            "- Bounce task count: 0",
            "- Suppressed task count: 0",
            "- Missing safeguard count: 0",
            "- Readiness counts: weak 0, moderate 0, strong 0",
            (
                "- Signal counts: bounce_handling 0, hard_bounce 0, soft_bounce 0, "
                "complaint_handling 0, suppression_list 0, unsubscribe_interaction 0, "
                "provider_webhook 0, delivery_status 0, reactivation 0"
            ),
            "",
            "No email bounce handling readiness records were inferred.",
        ]
    )


def test_model_object_serialization_markdown_aliases_and_no_mutation_are_stable():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add bounce processor",
        description="Bounce handling stores delivery status and verifies provider webhook signature.",
        files_or_modules=["src/email/bounce_processor.py"],
        acceptance_criteria=["Hard bounce suppression is applied."],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Bounce handling | scoped",
            description="Hard bounce suppression and soft bounce retry policy are implemented.",
        )
    )
    plan = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-a",
                title="Complaint webhook",
                description="Spam complaint handling suppresses recipients after provider webhook verification.",
            ),
        ],
        plan_id="plan-serialization",
    )
    original = copy.deepcopy(plan)

    result = summarize_task_email_bounce_handling_readiness(plan)
    object_result = build_task_email_bounce_handling_readiness_plan([object_task])
    model_result = generate_task_email_bounce_handling_readiness(ExecutionPlan.model_validate(plan))
    payload = task_email_bounce_handling_readiness_plan_to_dict(result)
    markdown = task_email_bounce_handling_readiness_plan_to_markdown(result)

    assert plan == original
    assert isinstance(result.records[0], TaskEmailBounceHandlingReadinessRecord)
    assert object_result.records[0].task_id == "task-object"
    assert "provider_webhook_verification" in object_result.records[0].present_safeguards
    assert model_result.plan_id == "plan-serialization"
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_email_bounce_handling_readiness_plan_to_dicts(result) == payload["records"]
    assert task_email_bounce_handling_readiness_plan_to_dicts(result.records) == payload["records"]
    assert analyze_task_email_bounce_handling_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_email_bounce_handling_readiness(plan).to_dict() == result.to_dict()
    assert extract_task_email_bounce_handling_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_email_bounce_handling_readiness(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "bounce_task_ids",
        "suppressed_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "bounce_signals",
        "matched_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness",
        "risk_level",
        "evidence",
        "recommendations",
    ]
    assert markdown.startswith("# Task Email Bounce Handling Readiness: plan-serialization")
    assert "Bounce handling \\| scoped" in markdown


def _plan(tasks, plan_id="plan-email-bounce"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-email-bounce",
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
    validation_commands=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-email-bounce",
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
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    return payload
