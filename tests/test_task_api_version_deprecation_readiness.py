from blueprint.task_api_version_deprecation_readiness import (
    analyze_task_api_version_deprecation_readiness,
    task_api_version_deprecation_readiness_plan_to_dict,
    task_api_version_deprecation_readiness_plan_to_markdown,
)


def test_complete_api_deprecation_detects_all_signals_and_steps():
    plan = analyze_task_api_version_deprecation_readiness(
        {
            "id": "plan-api-deprecation",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "API version deprecation for v1 endpoint sunset",
                    "description": (
                        "Deprecate API v1 with endpoint sunset, field removal, SDK breaking change, "
                        "compatibility window, and migration deadline. Usage inventory covers endpoint "
                        "usage and call volume. Migration guide maps replacement endpoint and SDK guide. "
                        "Customer notice includes changelog and release notes. Compatibility fallback keeps "
                        "legacy mode during the grace period. Telemetry dashboard monitors remaining traffic "
                        "and error rate. API owner is integrations on-call. Removal criteria require zero traffic."
                    ),
                    "files_or_modules": ["src/api/v1_deprecation/endpoint_sunset.py"],
                }
            ],
        }
    )

    record = plan.records[0]
    assert record.detected_signals == (
        "api_version_deprecation",
        "endpoint_sunset",
        "field_removal",
        "sdk_breaking_change",
        "compatibility_window",
        "migration_deadline",
    )
    assert record.present_criteria == (
        "usage_inventory",
        "migration_guide",
        "customer_notice",
        "compatibility_fallback",
        "telemetry",
        "owner",
        "removal_criteria",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"
    assert any("files_or_modules: src/api/v1_deprecation/endpoint_sunset.py" in item for item in record.evidence)


def test_partial_api_deprecation_returns_required_followups():
    plan = analyze_task_api_version_deprecation_readiness(
        [
            {
                "id": "task-partial",
                "title": "Sunset legacy endpoint",
                "description": "Endpoint sunset has usage inventory, customer notice, telemetry alerts, and an API owner.",
            }
        ]
    )

    record = plan.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("usage_inventory", "customer_notice", "telemetry", "owner")
    assert record.missing_criteria == ("migration_guide", "compatibility_fallback", "removal_criteria")
    assert record.recommended_follow_up_actions[0].startswith("Publish migration")


def test_absent_api_deprecation_is_ignored():
    plan = analyze_task_api_version_deprecation_readiness(
        [{"id": "task-ui", "title": "Refresh dashboard", "description": "Update chart labels."}]
    )

    assert plan.records == ()
    assert plan.ignored_task_ids == ("task-ui",)


def test_metadata_driven_api_deprecation_detects_nested_content():
    plan = analyze_task_api_version_deprecation_readiness(
        [
            {
                "id": "task-metadata",
                "title": "SDK contract breaking change",
                "metadata": {
                    "deprecation": {
                        "deadline": "Migration deadline is 2026-09-30.",
                        "fallback": "Dual support preserves compatibility fallback.",
                        "criteria": "Removal criteria require migration complete.",
                    }
                },
                "notes": ["Developer notice and upgrade guide are drafted."],
                "validation_commands": ["poetry run api-usage telemetry --remaining-traffic"],
            }
        ]
    )

    record = plan.records[0]
    assert "sdk_breaking_change" in record.detected_signals
    assert record.present_criteria == (
        "migration_guide",
        "customer_notice",
        "compatibility_fallback",
        "telemetry",
        "removal_criteria",
    )
    assert any("metadata.deprecation.deadline:" in item for item in record.evidence)
    assert any("validation_commands[0]:" in item for item in record.evidence)


def test_serialization_and_markdown_are_stable():
    plan = analyze_task_api_version_deprecation_readiness(
        [{"id": "task-api", "title": "Deprecate API v2", "description": "API version deprecation has owner."}]
    )

    payload = task_api_version_deprecation_readiness_plan_to_dict(plan)
    markdown = task_api_version_deprecation_readiness_plan_to_markdown(plan)

    assert list(payload) == ["plan_id", "records", "findings", "recommendations", "impacted_task_ids", "ignored_task_ids", "summary"]
    assert payload["summary"]["missing_criterion_count"] == 6
    assert markdown == plan.to_markdown()
    assert "# Task API Version Deprecation Readiness" in markdown
