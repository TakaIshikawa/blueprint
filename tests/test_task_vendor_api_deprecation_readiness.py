from blueprint.task_vendor_api_deprecation_readiness import (
    analyze_task_vendor_api_deprecation_readiness,
    task_vendor_api_deprecation_readiness_plan_to_dict,
    task_vendor_api_deprecation_readiness_plan_to_markdown,
)


def test_ready_vendor_api_deprecation_task_covers_expected_criteria():
    plan = analyze_task_vendor_api_deprecation_readiness(
        [{
            "id": "task-ready",
            "title": "Migrate Stripe SDK before vendor API deprecation",
            "description": (
                "Vendor version v1 is retiring. Impacted integration paths include endpoints, webhooks, clients, "
                "and call sites. Replacement API is v2 with a new SDK. Migration sequencing uses phases, canary, "
                "and cutover timeline. Compatibility tests include contract tests and sandbox tests. Monitoring "
                "dashboard alerts on vendor errors and latency. Rollback uses a feature flag fallback to legacy path. "
                "Customer communication, support communication, release notes, and runbook cover impact."
            ),
            "files_or_modules": ["src/integrations/stripe/deprecation_migration.py"],
        }]
    )

    record = plan.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert "vendor_deprecation" in record.detected_signals


def test_partial_vendor_api_deprecation_task_returns_followups():
    plan = analyze_task_vendor_api_deprecation_readiness(
        [{"id": "task-partial", "title": "Vendor API deprecation", "description": "Vendor version v1 is deprecated and call sites are listed."}]
    )

    record = plan.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("vendor_version", "impacted_integration_paths")
    assert "Document the replacement API" in record.recommended_follow_up_actions[0]


def test_sparse_vendor_api_deprecation_task_needs_planning():
    plan = analyze_task_vendor_api_deprecation_readiness("Third-party SDK retirement")

    assert plan.records[0].readiness == "needs_planning"
    assert len(plan.records[0].missing_criteria) == 8


def test_internal_api_versioning_without_vendor_signal_is_ignored():
    plan = analyze_task_vendor_api_deprecation_readiness(
        [{"id": "task-internal", "title": "Sunset internal API v1", "description": "Deprecate our internal endpoint version and migrate clients."}]
    )

    assert plan.records == ()
    assert plan.ignored_task_ids == ("task-internal",)


def test_vendor_api_deprecation_serialization_and_markdown_are_deterministic():
    plan = analyze_task_vendor_api_deprecation_readiness(
        [{"id": "task-vendor", "title": "Vendor API deprecation", "description": "Vendor version v1 is deprecated."}]
    )

    payload = task_vendor_api_deprecation_readiness_plan_to_dict(plan)
    assert payload["summary"]["missing_criterion_count"] == 7
    assert task_vendor_api_deprecation_readiness_plan_to_markdown(plan) == plan.to_markdown()
