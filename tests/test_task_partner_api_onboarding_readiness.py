import json

from blueprint.task_partner_api_onboarding_readiness import (
    analyze_task_partner_api_onboarding_readiness,
    build_task_partner_api_onboarding_readiness_plan,
    recommend_task_partner_api_onboarding_readiness,
    task_partner_api_onboarding_readiness_plan_to_dict,
    task_partner_api_onboarding_readiness_plan_to_dicts,
    task_partner_api_onboarding_readiness_plan_to_markdown,
)


def test_complete_partner_api_onboarding_task_is_ready():
    result = build_task_partner_api_onboarding_readiness_plan(
        {
            "id": "plan-partner",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Partner API onboarding for external partner enablement",
                    "description": "Developer portal onboarding issues partner credentials for a partner API.",
                    "acceptance_criteria": [
                        "Partner identity maps partner ID, partner account, tenant, and app identity.",
                        "Credential issuance provisions client ID, client secret, API key, and rotation.",
                        "Partner sandbox setup includes sandbox credentials, test data, and test account.",
                        "Access scopes and permissions use least privilege entitlements.",
                        "Rate limits and quotas document throttling, RPM, and usage cap policy.",
                        "Documentation includes developer guide, API docs, OpenAPI, quickstart, and runbook.",
                        "Support escalation path names partner support channel, on-call, and contact queue.",
                        "Tests include integration tests, contract tests, sandbox tests, and credential tests.",
                    ],
                    "files_or_modules": ["src/partners/onboarding/partner_api_credentials.py"],
                }
            ],
        }
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "partner_identity",
        "credential_issuance",
        "sandbox_test_data",
        "access_scopes",
        "rate_limits_quotas",
        "documentation",
        "support_escalation_path",
        "tests",
    )
    assert any("src/partners/onboarding/partner_api_credentials.py" in item for item in record.evidence)


def test_partial_partner_onboarding_reports_actionable_gaps_and_ignores_no_impact():
    result = analyze_task_partner_api_onboarding_readiness(
        [
            {
                "id": "task-partial",
                "title": "Developer portal onboarding for partner sandbox setup",
                "description": "Partner onboarding defines partner identity and sandbox test data.",
                "metadata": {"portal": {"docs": "Developer guide and API docs are drafted."}},
                "validation_commands": ["python -m pytest tests/partners/test_partner_onboarding.py"],
            },
            {
                "id": "task-copy",
                "title": "Portal copy cleanup",
                "description": "No partner API onboarding, partner credentials, developer portal, or partner sandbox changes are planned.",
            },
        ]
    )

    record = result.records[0]
    assert result.ignored_task_ids == ("task-copy",)
    assert record.readiness == "partial"
    assert record.present_criteria == ("partner_identity", "sandbox_test_data", "documentation", "tests")
    assert record.missing_criteria == (
        "credential_issuance",
        "access_scopes",
        "rate_limits_quotas",
        "support_escalation_path",
    )
    assert record.recommended_follow_up_actions[0].startswith("Specify credential")
    assert any("metadata.portal.docs" in item for item in record.evidence)
    assert any("validation_commands[0]" in item for item in record.evidence)


def test_partner_onboarding_path_hints_serialization_and_markdown_are_stable():
    result = build_task_partner_api_onboarding_readiness_plan(
        {
            "id": "plan-path",
            "tasks": [
                {
                    "id": "task-path",
                    "title": "Refactor integration config",
                    "files_or_modules": ["docs/developer_portal/partner_sandbox_quickstart.md"],
                }
            ],
        }
    )
    payload = task_partner_api_onboarding_readiness_plan_to_dict(result)

    assert result.records[0].detected_signals == (
        "developer_portal_onboarding",
        "partner_sandbox_setup",
        "external_partner_integration",
    )
    assert recommend_task_partner_api_onboarding_readiness(result) == result.records
    assert task_partner_api_onboarding_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-path"
    assert task_partner_api_onboarding_readiness_plan_to_markdown(result).startswith("# Task Partner API Onboarding Readiness: plan-path")
