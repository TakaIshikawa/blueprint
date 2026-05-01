import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_oauth_scope_readiness import (
    TaskOAuthScopeReadinessPlan,
    TaskOAuthScopeReadinessRecommendation,
    build_task_oauth_scope_readiness_plan,
    generate_task_oauth_scope_readiness,
    summarize_task_oauth_scope_readiness,
    task_oauth_scope_readiness_plan_to_dict,
    task_oauth_scope_readiness_to_dicts,
)


def test_positive_detection_from_text_files_and_metadata():
    result = build_task_oauth_scope_readiness_plan(
        _plan(
            [
                _task(
                    "task-google-oauth",
                    title="Add Google OAuth scopes",
                    description=(
                        "Request OAuth read-only scopes for Google Drive using delegated permission "
                        "and update the consent screen."
                    ),
                    files_or_modules=[
                        "src/integrations/oauth/google_scopes.py",
                        "config/consent/google.yaml",
                    ],
                    acceptance_criteria=[
                        "Run least privilege scope review and consent screen review.",
                        "Document token rotation, revocation path, audit logging, and negative permission tests.",
                    ],
                    metadata={
                        "provider": "Google",
                        "requested_scopes": ["drive.readonly", "openid profile email"],
                    },
                )
            ]
        )
    )

    assert isinstance(result, TaskOAuthScopeReadinessPlan)
    assert result.plan_id == "plan-oauth-readiness"
    assert result.flagged_task_ids == ("task-google-oauth",)
    record = result.recommendations[0]
    assert isinstance(record, TaskOAuthScopeReadinessRecommendation)
    assert record.task_id == "task-google-oauth"
    assert record.title == "Add Google OAuth scopes"
    assert "oauth" in record.auth_surfaces
    assert "delegated permission" in record.auth_surfaces
    assert "consent screen" in record.auth_surfaces
    assert "scope configuration" in record.auth_surfaces
    assert record.requested_scope_signals == ("read_scope",)
    assert record.missing_controls == ()
    assert record.risk_level == "low"
    assert "files_or_modules: src/integrations/oauth/google_scopes.py" in record.evidence
    assert any("metadata.requested_scopes[0]" in item for item in record.evidence)


def test_risk_escalates_for_admin_write_wildcard_offline_and_impersonation_scopes():
    result = build_task_oauth_scope_readiness_plan(
        _plan(
            [
                _task(
                    "task-admin",
                    title="Grant admin OAuth app access",
                    description=(
                        "Connected app requests admin and read/write permissions with all scopes, "
                        "offline_access refresh token, and domain-wide delegation to impersonate users."
                    ),
                    files_or_modules=["config/oauth/admin_permissions.yaml"],
                ),
                _task(
                    "task-read",
                    title="Add read only profile scope",
                    description="OAuth app requests read-only profile.read scope.",
                    acceptance_criteria=[
                        "Least privilege scope review is complete.",
                        "Consent screen review is complete.",
                        "Token rotation plan, revocation path, audit logging, and negative permission tests exist.",
                    ],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.recommendations}
    assert by_id["task-admin"].risk_level == "high"
    assert {
        "write_scope",
        "admin_scope",
        "wildcard_scope",
        "offline_access",
        "impersonation",
    } <= set(by_id["task-admin"].requested_scope_signals)
    assert by_id["task-read"].risk_level == "low"
    assert by_id["task-read"].requested_scope_signals == ("read_scope",)


def test_missing_controls_are_reported_and_summary_counts_unrelated_tasks():
    result = build_task_oauth_scope_readiness_plan(
        _plan(
            [
                _task(
                    "task-token",
                    title="Add API token integration",
                    description="Use an API token and bearer token for partner read permissions.",
                    files_or_modules=["src/integrations/tokens/partner.py"],
                ),
                _task(
                    "task-docs",
                    title="Update settings docs",
                    description="Document UI settings only.",
                    files_or_modules=["docs/settings.md"],
                ),
            ]
        )
    )

    assert result.flagged_task_ids == ("task-token",)
    record = result.recommendations[0]
    assert record.risk_level == "medium"
    assert record.missing_controls == (
        "least_privilege_scope_review",
        "consent_screen_review",
        "token_rotation_plan",
        "revocation_path",
        "audit_logging",
        "negative_permission_tests",
    )
    assert result.summary["total_task_count"] == 2
    assert result.summary["flagged_task_count"] == 1
    assert result.summary["unrelated_task_count"] == 1
    assert result.summary["risk_counts"] == {"low": 0, "medium": 1, "high": 0}
    assert result.summary["missing_control_counts"]["token_rotation_plan"] == 1


def test_model_input_serializes_stably_without_mutation_and_aliases_match():
    plan = _plan(
        [
            _task(
                "task-z",
                title="OAuth write calendar scope",
                description="Request calendar.write scope and add audit logs.",
            ),
            _task(
                "task-a",
                title="Service account read scope",
                description="Service account requests readonly report scope.",
                metadata={
                    "controls": {
                        "least_privilege_scope_review": "complete",
                        "consent_screen_review": "reviewed for admin consent",
                        "token_rotation_plan": "rotate token every 90 days",
                        "revocation_path": "revoke access from admin console",
                        "audit_logging": "audit logs include actor and scope",
                        "negative_permission_tests": "403 tests cover missing scope",
                    }
                },
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_task_oauth_scope_readiness_plan(model)
    alias_result = summarize_task_oauth_scope_readiness(plan)
    records = generate_task_oauth_scope_readiness(model)
    payload = task_oauth_scope_readiness_plan_to_dict(result)

    assert plan == original
    assert result.flagged_task_ids == ("task-a", "task-z")
    assert records == result.recommendations
    assert alias_result.to_dict() == result.to_dict()
    assert result.to_dicts() == payload["recommendations"]
    assert task_oauth_scope_readiness_to_dicts(records) == payload["recommendations"]
    assert task_oauth_scope_readiness_to_dicts(result) == payload["recommendations"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "recommendations", "flagged_task_ids", "summary"]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "auth_surfaces",
        "requested_scope_signals",
        "missing_controls",
        "risk_level",
        "evidence",
    ]


def test_empty_partial_or_non_model_sources_do_not_raise():
    empty = build_task_oauth_scope_readiness_plan(_plan([_task("task-general", title="General work")]))
    assert empty.recommendations == ()
    assert empty.flagged_task_ids == ()
    assert empty.summary["flagged_task_count"] == 0
    assert empty.summary["unrelated_task_count"] == 1
    assert generate_task_oauth_scope_readiness({"tasks": "not a list"}) == ()
    assert generate_task_oauth_scope_readiness("not a plan") == ()
    assert generate_task_oauth_scope_readiness(None) == ()
    assert build_task_oauth_scope_readiness_plan({"tasks": "not a list"}).summary == {
        "total_task_count": 0,
        "flagged_task_count": 0,
        "unrelated_task_count": 0,
        "risk_counts": {"low": 0, "medium": 0, "high": 0},
        "missing_control_counts": {
            "least_privilege_scope_review": 0,
            "consent_screen_review": 0,
            "token_rotation_plan": 0,
            "revocation_path": 0,
            "audit_logging": 0,
            "negative_permission_tests": 0,
        },
        "auth_surface_counts": {
            "oauth": 0,
            "api token": 0,
            "service account": 0,
            "delegated permission": 0,
            "third-party app": 0,
            "consent screen": 0,
            "scope configuration": 0,
        },
    }


def _plan(tasks):
    return {
        "id": "plan-oauth-readiness",
        "implementation_brief_id": "brief-oauth-readiness",
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
    return {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {title or task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "metadata": metadata or {},
    }
