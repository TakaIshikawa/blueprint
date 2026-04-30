import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_security_review import (
    TaskSecurityReviewTrigger,
    detect_task_security_review_triggers,
    task_security_review_triggers_to_dict,
    task_security_review_triggers_to_markdown,
)


def test_detects_security_triggers_from_core_task_fields():
    result = detect_task_security_review_triggers(
        _plan(
            [
                _task(
                    "task-auth-payments",
                    title="Harden auth checkout session",
                    description="Rotate tokens and verify Stripe webhook signatures.",
                    files_or_modules=[
                        "src/blueprint/auth/tokens.py",
                        "src/blueprint/billing/webhooks.py",
                    ],
                    acceptance_criteria=[
                        "Expired tokens cannot be reused.",
                        "Billing webhook replay attempts are rejected.",
                    ],
                    test_command="poetry run pytest tests/test_billing_webhook.py",
                )
            ]
        )
    )

    triggers = {trigger.trigger_type: trigger for trigger in result.triggers}

    assert result.plan_id == "plan-security"
    assert isinstance(triggers["auth"], TaskSecurityReviewTrigger)
    assert triggers["auth"].task_id == "task-auth-payments"
    assert triggers["auth"].severity == "high"
    assert triggers["auth"].blocking is True
    assert triggers["auth"].recommended_reviewers == ("security", "identity")
    assert triggers["auth"].evidence == (
        "files_or_modules: src/blueprint/auth/tokens.py",
        "title: Harden auth checkout session",
        "description: Rotate tokens and verify Stripe webhook signatures.",
        "acceptance_criteria[0]: Expired tokens cannot be reused.",
    )
    assert triggers["payments"].evidence == (
        "files_or_modules: src/blueprint/billing/webhooks.py",
        "title: Harden auth checkout session",
        "description: Rotate tokens and verify Stripe webhook signatures.",
        "acceptance_criteria[1]: Billing webhook replay attempts are rejected.",
    )
    assert triggers["webhooks"].evidence == (
        "files_or_modules: src/blueprint/billing/webhooks.py",
        "description: Rotate tokens and verify Stripe webhook signatures.",
        "acceptance_criteria[1]: Billing webhook replay attempts are rejected.",
        "test_command: poetry run pytest tests/test_billing_webhook.py",
    )
    assert result.blocking_task_ids == ("task-auth-payments",)


def test_detects_all_requested_sensitive_trigger_categories():
    result = detect_task_security_review_triggers(
        [
            _task(
                "task-admin",
                title="Add admin permission policy",
                description="Expose administrator role controls in the support console.",
                files_or_modules=["src/admin/permissions.py"],
            ),
            _task(
                "task-privacy-export",
                title="Export customer data report",
                description="Download CSV rows containing email address and phone number.",
                files_or_modules=["src/reports/exports/customer_csv.py"],
            ),
            _task(
                "task-network-deps",
                title="Add external API client",
                description="Bump dependency package and allow outbound HTTPS requests.",
                files_or_modules=[
                    "src/integrations/http/client.py",
                    "poetry.lock",
                    ".github/workflows/release.yml",
                ],
            ),
            _task(
                "task-secrets",
                title="Move service credentials",
                description="Store API key values in Vault.",
                files_or_modules=["infra/secrets/service.yaml"],
            ),
        ]
    )

    trigger_types = {trigger.trigger_type for trigger in result.triggers}

    assert trigger_types == {
        "admin_surface",
        "authorization",
        "data_export",
        "dependency_change",
        "network_access",
        "permission_sensitive_file",
        "pii",
        "secrets",
    }
    assert _trigger(result, "task-privacy-export", "data_export").severity == "medium"
    assert _trigger(result, "task-network-deps", "network_access").blocking is False
    assert _trigger(result, "task-network-deps", "dependency_change").blocking is False
    assert _trigger(result, "task-secrets", "permission_sensitive_file").blocking is True


def test_metadata_and_test_commands_are_detected_and_duplicates_are_collapsed():
    task_model = ExecutionTask.model_validate(
        _task(
            "task-metadata",
            title="Review authorization matrix",
            description="Tighten RBAC checks for admin tokens.",
            files_or_modules=["src/security/permissions.py"],
            acceptance_criteria=["Permission replay is covered by tests."],
            test_command="poetry run pytest tests/test_permissions.py",
            metadata={
                "security_review": "authorization",
                "permission_notes": ["Manual authorization signoff required."],
                "test_commands": ["poetry run pytest tests/test_admin_permissions.py"],
            },
        )
    )

    result = detect_task_security_review_triggers(task_model)
    authorization = _trigger(result, "task-metadata", "authorization")

    assert [trigger.trigger_type for trigger in result.triggers].count("authorization") == 1
    assert authorization.severity == "high"
    assert authorization.evidence == (
        "files_or_modules: src/security/permissions.py",
        "title: Review authorization matrix",
        "description: Tighten RBAC checks for admin tokens.",
        "acceptance_criteria[0]: Permission replay is covered by tests.",
        "metadata.permission_notes[0]: Manual authorization signoff required.",
        "metadata.security_review: authorization",
    )
    assert _trigger(result, "task-metadata", "admin_surface").evidence == (
        "description: Tighten RBAC checks for admin tokens.",
        "metadata.test_commands[0]: poetry run pytest tests/test_admin_permissions.py",
    )


def test_execution_plan_models_serialization_and_markdown_are_stable():
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-payments",
                    title="Update billing refund flow",
                    description="Payment refunds preserve invoice audit records.",
                    files_or_modules=["src/payments/refunds.py"],
                    risk_level="high",
                ),
                _task(
                    "task-safe",
                    title="Refresh empty state copy",
                    description="Update non-sensitive UI copy.",
                    files_or_modules=["src/ui/empty_state.py"],
                ),
            ]
        )
    )

    result = detect_task_security_review_triggers(plan_model)
    payload = task_security_review_triggers_to_dict(result)
    markdown = task_security_review_triggers_to_markdown(result)

    assert payload == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "triggers",
        "trigger_counts_by_type",
        "blocking_task_ids",
    ]
    assert list(payload["triggers"][0]) == [
        "task_id",
        "trigger_type",
        "severity",
        "evidence",
        "recommended_reviewers",
        "blocking",
    ]
    assert result.trigger_counts_by_type == {"payments": 1}
    assert result.blocking_task_ids == ("task-payments",)
    assert json.loads(json.dumps(payload)) == payload
    assert markdown.startswith("## Security Review Triggers")
    assert "| `task-payments` | payments | high | yes | security, payments |" in markdown
    assert "task-safe" not in markdown


def test_no_triggers_returns_empty_summary():
    result = detect_task_security_review_triggers(
        [_task("task-copy", title="Update copy", description="Clarify button label.")]
    )

    assert result.triggers == ()
    assert result.trigger_counts_by_type == {}
    assert result.blocking_task_ids == ()
    assert result.to_markdown() == "## Security Review Triggers\n\nNo security review triggers detected."


def _trigger(result, task_id, trigger_type):
    return next(
        trigger
        for trigger in result.triggers
        if trigger.task_id == task_id and trigger.trigger_type == trigger_type
    )


def _plan(tasks):
    return {
        "id": "plan-security",
        "implementation_brief_id": "brief-security",
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
    test_command=None,
    risk_level=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if test_command is not None:
        task["test_command"] = test_command
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
