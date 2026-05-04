import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_oauth_token_revocation_readiness import (
    TaskOAuthTokenRevocationReadinessFinding,
    TaskOAuthTokenRevocationReadinessPlan,
    plan_task_oauth_token_revocation_readiness,
    task_oauth_token_revocation_readiness_to_dict,
    task_oauth_token_revocation_readiness_to_markdown,
    analyze_task_oauth_token_revocation_readiness,
    extract_task_oauth_token_revocation_readiness,
    generate_task_oauth_token_revocation_readiness,
    recommend_task_oauth_token_revocation_readiness,
)


def test_strong_oauth_revocation_task_has_all_safeguards():
    result = plan_task_oauth_token_revocation_readiness(
        _plan(
            [
                _task(
                    "task-oauth-revoke",
                    title="Implement OAuth token revocation endpoint",
                    prompt=(
                        "Implement token invalidation mechanism with token blacklist and database update. "
                        "Ensure refresh token cleanup by revoking refresh tokens when access tokens are invalidated. "
                        "Implement active session termination to clear server-side session state after revocation. "
                        "Add audit trail capture to log revocation events with user ID, token ID, timestamp, and IP address. "
                        "Implement cascade revocation to invalidate related tokens including refresh and session tokens. "
                        "Add revocation verification with regression tests covering token invalidation and session cleanup."
                    ),
                    outputs=["src/oauth/revocation.py", "tests/test_revocation.py", "db/audit_log.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskOAuthTokenRevocationReadinessPlan)
    assert result.plan_id == "plan-oauth-revoke"
    assert result.oauth_revocation_task_ids == ("task-oauth-revoke",)
    finding = result.findings[0]
    assert isinstance(finding, TaskOAuthTokenRevocationReadinessFinding)
    assert "token_revocation" in finding.detected_signals
    assert "refresh_token" in finding.detected_signals
    assert "session_cleanup" in finding.detected_signals
    assert "audit_logging" in finding.detected_signals
    assert finding.present_safeguards == (
        "token_invalidation_mechanism",
        "refresh_token_cleanup",
        "active_session_termination",
        "audit_trail_capture",
        "cascade_revocation",
        "revocation_verification",
    )
    assert finding.missing_safeguards == ()
    assert finding.actionable_remediations == ()
    assert finding.readiness == "strong"
    assert any("token invalidation" in ev.lower() for ev in finding.evidence)
    assert result.summary["oauth_revocation_task_count"] == 1
    assert result.summary["overall_readiness"] == "strong"


def test_partial_oauth_revocation_task_reports_missing_safeguards():
    result = plan_task_oauth_token_revocation_readiness(
        _plan(
            [
                _task(
                    "task-basic-revoke",
                    title="Add token removal endpoint",
                    prompt=(
                        "Implement token blacklist mechanism. "
                        "Add audit trail capture for token events. "
                        "Ensure refresh token cleanup."
                    ),
                    outputs=["src/auth/remove_tokens.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-basic-revoke"
    assert "token_revocation" in finding.detected_signals
    assert "audit_logging" in finding.detected_signals
    assert "refresh_token" in finding.detected_signals
    assert "token_invalidation_mechanism" in finding.present_safeguards
    assert "audit_trail_capture" in finding.present_safeguards
    assert "refresh_token_cleanup" in finding.present_safeguards
    assert "active_session_termination" in finding.missing_safeguards
    assert "cascade_revocation" in finding.missing_safeguards
    assert "revocation_verification" in finding.missing_safeguards
    assert finding.readiness in {"weak", "partial"}
    assert len(finding.actionable_remediations) >= 2
    assert any("session" in remediation.lower() for remediation in finding.actionable_remediations)


def test_path_hints_contribute_to_oauth_revocation_detection():
    result = plan_task_oauth_token_revocation_readiness(
        _plan(
            [
                _task(
                    "task-paths",
                    title="OAuth revocation infrastructure",
                    prompt="Set up token revocation with audit logging and session cleanup.",
                    outputs=[
                        "src/oauth/token_revocation.py",
                        "src/oauth/refresh_token_cleanup.py",
                        "src/session/session_termination.py",
                        "src/audit/revocation_log.py",
                        "tests/test_revocation_verification.py",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert {"token_revocation", "refresh_token", "session_cleanup", "audit_logging"} <= set(finding.detected_signals)
    assert "token_invalidation_mechanism" in finding.present_safeguards
    assert "audit_trail_capture" in finding.present_safeguards


def test_unrelated_and_explicit_no_impact_tasks_are_not_applicable():
    result = plan_task_oauth_token_revocation_readiness(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update API documentation",
                    prompt="Improve API endpoint documentation.",
                    outputs=["docs/api.md"],
                ),
                _task(
                    "task-no-revoke",
                    title="Add feature flag",
                    prompt="This task has no token revocation requirements and no changes are involved.",
                    outputs=["src/features/flag.py"],
                ),
            ]
        )
    )

    assert result.oauth_revocation_task_ids == ()
    assert result.not_applicable_task_ids == ("task-docs", "task-no-revoke")
    assert result.findings == ()
    assert result.summary["oauth_revocation_task_count"] == 0
    assert result.summary["not_applicable_task_count"] == 2


def test_logout_and_connected_apps_signals():
    result = plan_task_oauth_token_revocation_readiness(
        _plan(
            [
                _task(
                    "task-logout-apps",
                    title="Implement logout and connected apps disconnect",
                    prompt=(
                        "Implement logout flow with session termination and token revocation. "
                        "Add connected app disconnect functionality to revoke authorized app tokens."
                    ),
                    outputs=["src/auth/logout.py", "src/oauth/connected_apps.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "logout_flow" in finding.detected_signals
    assert "connected_apps" in finding.detected_signals
    assert "token_revocation" in finding.detected_signals


def test_refresh_token_rotation_task():
    result = plan_task_oauth_token_revocation_readiness(
        _plan(
            [
                _task(
                    "task-refresh-rotation",
                    title="Implement refresh token rotation",
                    prompt=(
                        "Implement refresh token rotation to invalidate old refresh tokens when new ones are issued. "
                        "Add token invalidation mechanism for expired refresh tokens. "
                        "Implement revocation verification with regression tests."
                    ),
                    outputs=["src/oauth/token_rotation.py", "tests/test_rotation.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "refresh_token" in finding.detected_signals
    assert "token_revocation" in finding.detected_signals
    assert "refresh_token_cleanup" in finding.present_safeguards
    assert "token_invalidation_mechanism" in finding.present_safeguards
    assert "revocation_verification" in finding.present_safeguards


def test_session_cleanup_and_audit_logging_signals():
    result = plan_task_oauth_token_revocation_readiness(
        _plan(
            [
                _task(
                    "task-session-audit",
                    title="Add session cleanup and audit logging",
                    prompt=(
                        "Implement active session termination to clear session state on logout. "
                        "Add audit trail capture for logging revocation events with timestamp and user ID."
                    ),
                    outputs=["src/session/cleanup.py", "src/audit/events.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "session_cleanup" in finding.detected_signals
    assert "audit_logging" in finding.detected_signals
    assert "active_session_termination" in finding.present_safeguards
    assert "audit_trail_capture" in finding.present_safeguards


def test_serialization_and_compatibility_views():
    plan = _plan(
        [
            _task(
                "task-revoke-001",
                title="Implement OAuth token revocation",
                prompt="Add token invalidation mechanism with audit trail capture and revocation verification.",
                outputs=["src/oauth/revoke.py"],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = plan_task_oauth_token_revocation_readiness(plan)
    serialized = task_oauth_token_revocation_readiness_to_dict(result)

    assert plan == original
    assert result.records == result.findings
    assert serialized["oauth_revocation_task_ids"] == ["task-revoke-001"]
    assert "findings" in serialized
    assert "summary" in serialized
    assert list(serialized) == ["plan_id", "findings", "oauth_revocation_task_ids", "not_applicable_task_ids", "summary", "records"]
    assert json.loads(json.dumps(serialized)) == serialized
    finding = result.findings[0]
    assert finding.actionable_gaps == finding.actionable_remediations


def test_multiple_tasks_with_different_readiness_levels():
    result = plan_task_oauth_token_revocation_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Comprehensive OAuth revocation",
                    prompt=(
                        "Implement token invalidation mechanism, refresh token cleanup, active session termination, "
                        "audit trail capture, cascade revocation, and revocation verification with regression tests."
                    ),
                    outputs=["src/oauth/full_revoke.py"],
                ),
                _task(
                    "task-partial",
                    title="Basic token revocation",
                    prompt="Add token invalidation mechanism with audit trail capture.",
                    outputs=["src/oauth/basic_revoke.py"],
                ),
                _task(
                    "task-weak",
                    title="Token expiry",
                    prompt="Configure token expiry for access tokens.",
                    outputs=["config/oauth.yaml"],
                ),
            ]
        )
    )

    assert len(result.findings) == 3
    assert result.oauth_revocation_task_ids == ("task-strong", "task-partial", "task-weak")
    assert result.summary["readiness_distribution"]["strong"] == 1
    assert result.summary["readiness_distribution"]["weak"] >= 1
    assert result.summary["overall_readiness"] in {"weak", "partial"}


def test_object_and_dict_inputs_are_handled():
    plan_dict = {
        "id": "plan-dict",
        "tasks": [
            {
                "task_id": "task-001",
                "title": "OAuth token revocation",
                "prompt": "Implement token invalidation mechanism with audit trail capture and revocation verification.",
            }
        ],
    }
    plan_obj = SimpleNamespace(
        id="plan-obj",
        tasks=[
            SimpleNamespace(
                task_id="task-002",
                title="Token revocation setup",
                prompt="Add refresh token cleanup with session termination and cascade revocation.",
            )
        ],
    )

    result_dict = plan_task_oauth_token_revocation_readiness(plan_dict)
    result_obj = plan_task_oauth_token_revocation_readiness(plan_obj)

    assert result_dict.plan_id == "plan-dict"
    assert result_dict.oauth_revocation_task_ids == ("task-001",)
    assert result_obj.plan_id == "plan-obj"
    assert result_obj.oauth_revocation_task_ids == ("task-002",)


def test_empty_plan_produces_empty_findings():
    result = plan_task_oauth_token_revocation_readiness(_plan([]))

    assert result.oauth_revocation_task_ids == ()
    assert result.not_applicable_task_ids == ()
    assert result.findings == ()
    assert result.summary["oauth_revocation_task_count"] == 0
    assert result.summary["overall_readiness"] == "weak"


def test_sparse_task_with_minimal_signals():
    result = plan_task_oauth_token_revocation_readiness(
        _plan(
            [
                _task(
                    "task-sparse",
                    title="Update logout button",
                    prompt="Update logout button styling.",
                    outputs=["src/components/logout_button.tsx"],
                )
            ]
        )
    )

    assert len(result.findings) == 1
    finding = result.findings[0]
    assert "logout_flow" in finding.detected_signals
    assert finding.readiness == "weak"
    assert len(finding.missing_safeguards) > 0


def test_alias_consistency():
    plan = _plan(
        [
            _task(
                "task-alias",
                title="OAuth revocation",
                prompt="Implement token invalidation mechanism with audit trail capture.",
            )
        ]
    )

    result_plan = plan_task_oauth_token_revocation_readiness(plan)
    result_analyze = analyze_task_oauth_token_revocation_readiness(plan)
    result_extract = extract_task_oauth_token_revocation_readiness(plan)
    result_generate = generate_task_oauth_token_revocation_readiness(plan)
    result_recommend = recommend_task_oauth_token_revocation_readiness(plan)

    assert result_plan == result_analyze
    assert result_plan == result_extract
    assert result_plan == result_generate
    assert result_plan == result_recommend


def test_markdown_output():
    result = plan_task_oauth_token_revocation_readiness(
        _plan(
            [
                _task(
                    "task-md",
                    title="OAuth token revocation",
                    prompt="Implement token invalidation mechanism with audit trail capture.",
                    outputs=["src/oauth/revoke.py"],
                )
            ]
        )
    )

    markdown = task_oauth_token_revocation_readiness_to_markdown(result)

    assert "# Task OAuth Token Revocation Readiness Plan: plan-oauth-revoke" in markdown
    assert "| Task | Readiness | Detected Signals | Present Safeguards | Missing Safeguards | Actionable Remediations |" in markdown
    assert "| `task-md` |" in markdown
    assert "token_revocation" in markdown


def test_empty_plan_markdown_output():
    result = plan_task_oauth_token_revocation_readiness(_plan([]))
    markdown = task_oauth_token_revocation_readiness_to_markdown(result)

    assert "# Task OAuth Token Revocation Readiness Plan" in markdown
    assert "No OAuth token revocation readiness findings were derived." in markdown


def _plan(tasks):
    return {
        "id": "plan-oauth-revoke",
        "tasks": tasks,
    }


def _task(task_id, *, title="", prompt="", outputs=None, scope=None):
    return {
        "task_id": task_id,
        "title": title,
        "prompt": prompt,
        "outputs": outputs or [],
        "scope": scope or [],
    }
