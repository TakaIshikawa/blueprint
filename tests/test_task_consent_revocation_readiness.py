from blueprint.task_consent_revocation_readiness import (
    analyze_task_consent_revocation_readiness,
    task_consent_revocation_readiness_plan_to_dict,
    task_consent_revocation_readiness_plan_to_markdown,
)


def test_ready_consent_revocation_task_covers_required_criteria():
    plan = analyze_task_consent_revocation_readiness(
        [{
            "id": "task-ready",
            "title": "Implement consent revocation and opt-out propagation",
            "description": (
                "Revocation trigger is an unsubscribe link and preference change API request. "
                "Affected data actions stop email processing for the marketing purpose. "
                "Propagation targets include downstream CRM, ESP, warehouse, webhooks, and queues. "
                "User confirmation sends a receipt. Audit trail records actor timestamp and outcome. "
                "Retries use backoff and DLQ failure handling. Privacy copy includes GDPR withdrawal language. "
                "Integration tests verify propagation and failures."
            ),
            "files_or_modules": ["src/privacy/consent_revocation_worker.py"],
        }]
    )

    record = plan.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "revocation_trigger",
        "affected_data_actions",
        "propagation_targets",
        "user_confirmation",
        "audit_trail",
        "retries_failure_handling",
        "privacy_copy",
        "tests",
    )


def test_partial_consent_revocation_task_returns_actionable_followups():
    plan = analyze_task_consent_revocation_readiness(
        [{"id": "task-partial", "title": "Add unsubscribe opt-out", "description": "Opt-out trigger updates consent scope and sends confirmation."}]
    )

    record = plan.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("revocation_trigger", "affected_data_actions", "user_confirmation")
    assert "Name downstream systems" in record.recommended_follow_up_actions[0]


def test_sparse_consent_revocation_task_needs_planning():
    plan = analyze_task_consent_revocation_readiness("Revoke consent from marketing messages")

    assert plan.records[0].readiness == "needs_planning"
    assert len(plan.records[0].missing_criteria) == 8


def test_generic_consent_capture_without_revocation_is_ignored():
    plan = analyze_task_consent_revocation_readiness(
        [{"id": "task-capture", "title": "Capture privacy consent", "description": "Add checkbox for marketing consent during signup."}]
    )

    assert plan.records == ()
    assert plan.ignored_task_ids == ("task-capture",)


def test_consent_revocation_serialization_and_markdown_are_deterministic():
    plan = analyze_task_consent_revocation_readiness(
        [{"id": "task-revoke", "title": "Consent revocation", "description": "Revocation trigger is preference change."}]
    )

    payload = task_consent_revocation_readiness_plan_to_dict(plan)
    assert payload["impacted_task_ids"] == ["task-revoke"]
    assert payload["summary"]["missing_criterion_count"] == 7
    assert task_consent_revocation_readiness_plan_to_markdown(plan) == plan.to_markdown()
