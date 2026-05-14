import json

from blueprint.task_api_version_sunset_readiness import (
    analyze_task_api_version_sunset_readiness,
    build_task_api_version_sunset_readiness_plan,
    recommend_task_api_version_sunset_readiness,
    task_api_version_sunset_readiness_plan_to_dict,
    task_api_version_sunset_readiness_plan_to_dicts,
    task_api_version_sunset_readiness_plan_to_markdown,
)


def test_complete_api_version_sunset_task_is_ready():
    result = build_task_api_version_sunset_readiness_plan(
        {
            "id": "plan-sunset",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "API version sunset for v1 endpoints",
                    "description": "Sunset execution retires API version v1.",
                    "acceptance_criteria": [
                        "Migration guide documents replacement endpoint and SDK guide.",
                        "Customer communication includes developer notice, changelog, and release notes.",
                        "Sunset timeline sets migration deadline, notice period, and removal date.",
                        "Compatibility tests cover contract tests and dual support regression tests.",
                        "Metrics dashboard tracks usage tracking, remaining traffic, and client adoption alerts.",
                        "Rollback and extension criteria can pause removal with a fallback.",
                        "Documentation updates cover API docs, OpenAPI docs, SDK docs, and runbook.",
                    ],
                    "files_or_modules": ["openapi/v1_sunset.yaml", "sdk/version_migration.md"],
                }
            ],
        }
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "migration_guidance",
        "customer_communication",
        "sunset_timeline",
        "compatibility_tests",
        "metrics_usage_tracking",
        "rollback_extension_criteria",
        "documentation_updates",
    )
    assert any("openapi/v1_sunset.yaml" in item for item in record.evidence)


def test_partial_api_version_sunset_reports_actionable_gaps_and_ignores_no_impact():
    result = analyze_task_api_version_sunset_readiness(
        [
            {
                "id": "task-partial",
                "title": "Deprecate API v2",
                "description": "API version deprecation has migration guide and metrics dashboard.",
            },
            {
                "id": "task-docs",
                "title": "Docs cleanup",
                "description": "No API version sunset, deprecation, removal, or endpoint impact is planned.",
            },
        ]
    )

    record = result.records[0]
    assert result.ignored_task_ids == ("task-docs",)
    assert record.readiness == "partial"
    assert record.present_criteria == ("migration_guidance", "metrics_usage_tracking")
    assert record.missing_criteria == (
        "customer_communication",
        "sunset_timeline",
        "compatibility_tests",
        "rollback_extension_criteria",
        "documentation_updates",
    )
    assert record.recommended_follow_up_actions[0].startswith("Prepare customer")


def test_path_hints_serialization_and_markdown_are_stable():
    result = build_task_api_version_sunset_readiness_plan(
        {"id": "plan-path", "tasks": [{"id": "task-path", "title": "Refactor route", "files_or_modules": ["src/api/v3/routes/sunset.py"]}]}
    )
    payload = task_api_version_sunset_readiness_plan_to_dict(result)

    assert result.records[0].detected_signals == ("api_version_sunset", "versioned_endpoint", "sunset_execution")
    assert recommend_task_api_version_sunset_readiness(result) == result.records
    assert task_api_version_sunset_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-path"
    assert task_api_version_sunset_readiness_plan_to_markdown(result).startswith("# Task API Version Sunset Readiness: plan-path")
