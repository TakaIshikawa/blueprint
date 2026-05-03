import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_oauth_token_refresh_readiness import (
    TaskOAuthTokenRefreshReadinessFinding,
    TaskOAuthTokenRefreshReadinessPlan,
    analyze_task_oauth_token_refresh_readiness,
    build_task_oauth_token_refresh_readiness_plan,
    extract_task_oauth_token_refresh_readiness,
    generate_task_oauth_token_refresh_readiness,
    summarize_task_oauth_token_refresh_readiness,
    task_oauth_token_refresh_readiness_plan_to_dict,
    task_oauth_token_refresh_readiness_plan_to_dicts,
)


def test_ready_oauth_refresh_task_has_no_actionable_gaps():
    result = analyze_task_oauth_token_refresh_readiness(
        _plan(
            [
                _task(
                    "task-google-refresh",
                    title="Implement Google OAuth token refresh",
                    description=(
                        "Refresh expired access tokens with the Google OAuth provider. Store refresh tokens "
                        "encrypted in KMS-backed secret storage, handle token rotation and revocation including "
                        "invalid_grant, retry one 401 after refresh, map provider error codes and rate limits, "
                        "and emit observability metrics, logs, alerts, and dashboards."
                    ),
                    files_or_modules=["src/integrations/oauth/google_token_refresh.py"],
                    acceptance_criteria=[
                        "Integration tests cover refresh token success, expired token retry, provider errors, rotation, and revocation.",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskOAuthTokenRefreshReadinessPlan)
    assert result.plan_id == "plan-token-refresh"
    assert result.refresh_task_ids == ("task-google-refresh",)
    finding = result.findings[0]
    assert isinstance(finding, TaskOAuthTokenRefreshReadinessFinding)
    assert finding.detected_signals == (
        "oauth_token_refresh",
        "refresh_token",
        "access_token_expiry",
        "provider_oauth",
    )
    assert finding.present_requirements == (
        "refresh_token_storage",
        "rotation_revocation_handling",
        "expired_token_retry",
        "provider_error_handling",
        "observability",
        "integration_tests",
    )
    assert finding.missing_requirements == ()
    assert finding.actionable_gaps == ()
    assert finding.risk_level == "low"
    assert "files_or_modules: src/integrations/oauth/google_token_refresh.py" in finding.evidence
    assert result.summary["refresh_task_count"] == 1
    assert result.summary["missing_requirement_count"] == 0
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_partial_oauth_refresh_task_reports_actionable_missing_lifecycle_requirements():
    result = build_task_oauth_token_refresh_readiness_plan(
        _plan(
            [
                _task(
                    "task-slack-refresh",
                    title="Add Slack refresh token support",
                    description=(
                        "Use OAuth refresh tokens and offline_access so Slack access tokens can be renewed "
                        "after expiration. Persist refresh tokens in the encrypted credential store."
                    ),
                    metadata={"provider": "Slack", "token_endpoint": "OAuth token refresh endpoint"},
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-slack-refresh"
    assert finding.present_requirements == ("refresh_token_storage",)
    assert finding.missing_requirements == (
        "rotation_revocation_handling",
        "expired_token_retry",
        "provider_error_handling",
        "observability",
        "integration_tests",
    )
    assert finding.risk_level == "high"
    assert finding.actionable_gaps == (
        "Handle refresh-token rotation, provider revocation, disconnects, and invalid_grant outcomes.",
        "Define expired access-token detection, bounded refresh-and-retry behavior, and retry limits.",
        "Map provider token endpoint errors, rate limits, and outages to user-visible and retry behavior.",
        "Add metrics, logs, alerts, and dashboards for refresh success, failure, latency, and provider errors.",
        "Cover token refresh, expired-token retries, provider errors, revocation, and rotation in integration tests.",
    )
    assert result.summary["missing_requirement_counts"]["expired_token_retry"] == 1
    assert result.summary["present_requirement_counts"]["refresh_token_storage"] == 1


def test_unrelated_or_explicitly_out_of_scope_tasks_are_not_applicable():
    result = build_task_oauth_token_refresh_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings page copy",
                    description="No OAuth token refresh changes are required for this release.",
                    files_or_modules=["src/blueprint/ui/settings_copy.py"],
                ),
                _task(
                    "task-login",
                    title="Polish login form",
                    description="Adjust username field validation and remember-me labels.",
                ),
            ]
        )
    )

    assert result.findings == ()
    assert result.records == ()
    assert result.refresh_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy", "task-login")
    assert result.to_dicts() == []
    assert result.summary == {
        "total_task_count": 2,
        "refresh_task_count": 0,
        "not_applicable_task_ids": ["task-copy", "task-login"],
        "missing_requirement_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "oauth_token_refresh": 0,
            "refresh_token": 0,
            "access_token_expiry": 0,
            "offline_access": 0,
            "provider_oauth": 0,
        },
        "present_requirement_counts": {
            "refresh_token_storage": 0,
            "rotation_revocation_handling": 0,
            "expired_token_retry": 0,
            "provider_error_handling": 0,
            "observability": 0,
            "integration_tests": 0,
        },
        "missing_requirement_counts": {
            "refresh_token_storage": 0,
            "rotation_revocation_handling": 0,
            "expired_token_retry": 0,
            "provider_error_handling": 0,
            "observability": 0,
            "integration_tests": 0,
        },
    }


def test_model_object_aliases_serialization_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="OAuth refresh observability",
                description="Add OAuth token refresh metrics and logs for refresh failure rate.",
            ),
            _task(
                "task-a",
                title="Refresh expired Microsoft access tokens",
                description=(
                    "When Microsoft access tokens expire, refresh and retry once, handle provider "
                    "temporarily_unavailable errors, and cover the token endpoint with integration tests."
                ),
                metadata={
                    "storage": "Refresh token storage uses encrypted vault secrets.",
                    "rotation": "Rotated refresh tokens and revoked grants are handled.",
                },
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_task_oauth_token_refresh_readiness(model)
    payload = task_oauth_token_refresh_readiness_plan_to_dict(result)
    task_result = build_task_oauth_token_refresh_readiness_plan(
        ExecutionTask.model_validate(plan["tasks"][1])
    )
    object_result = build_task_oauth_token_refresh_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="Refresh token retry",
            description="OAuth refresh token retry handles expired access token 401 responses with provider error logging.",
        )
    )

    assert plan == original
    assert result.refresh_task_ids == ("task-z", "task-a")
    assert result.records == result.findings
    assert task_result.findings[0].task_id == "task-a"
    assert object_result.findings[0].task_id == "task-object"
    assert extract_task_oauth_token_refresh_readiness(plan).to_dict() == summarize_task_oauth_token_refresh_readiness(plan).to_dict()
    assert generate_task_oauth_token_refresh_readiness(plan).to_dict() == summarize_task_oauth_token_refresh_readiness(plan).to_dict()
    assert task_oauth_token_refresh_readiness_plan_to_dicts(result) == payload["findings"]
    assert task_oauth_token_refresh_readiness_plan_to_dicts(result.findings) == payload["findings"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "findings",
        "refresh_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["findings"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_requirements",
        "missing_requirements",
        "risk_level",
        "evidence",
        "actionable_gaps",
    ]


def _plan(tasks, plan_id="plan-token-refresh"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-token-refresh",
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
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
