import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_session_management_impact import (
    TaskSessionManagementImpactPlan,
    TaskSessionManagementImpactRecommendation,
    build_task_session_management_impact_plan,
    generate_task_session_management_impact,
    summarize_task_session_management_impact,
    task_session_management_impact_plan_to_dict,
    task_session_management_impact_plan_to_markdown,
    task_session_management_impact_to_dicts,
)


def test_detects_session_surfaces_from_text_files_and_metadata():
    result = build_task_session_management_impact_plan(
        _plan(
            [
                _task(
                    "task-session",
                    title="Update login session cookie and remember-me behavior",
                    description=(
                        "Change authenticated session state, secure cookie attributes, "
                        "remember-me persistent sessions, and refresh token handling."
                    ),
                    files_or_modules=[
                        "src/auth/sessions/cookies.py",
                        "src/auth/refresh_tokens/service.py",
                        "src/devices/device_sessions.py",
                    ],
                    acceptance_criteria=[
                        "Logout invalidation clears the server session.",
                        "Audit logs capture session changes.",
                    ],
                    metadata={
                        "session_invalidation": "revoke sessions after password change",
                        "device_sessions": ["show active devices"],
                    },
                )
            ]
        )
    )

    assert isinstance(result, TaskSessionManagementImpactPlan)
    assert result.plan_id == "plan-session-impact"
    assert result.flagged_task_ids == ("task-session",)
    record = result.recommendations[0]
    assert isinstance(record, TaskSessionManagementImpactRecommendation)
    assert record.task_id == "task-session"
    assert record.session_surfaces == (
        "login_session",
        "session_cookie",
        "refresh_token",
        "remember_me",
        "session_invalidation",
        "device_session",
        "logout",
    )
    assert "cookie_attributes" not in record.missing_controls
    assert "logout_invalidation" not in record.missing_controls
    assert "audit_logging" not in record.missing_controls
    assert record.risk_level == "high"
    assert "files_or_modules: src/auth/sessions/cookies.py" in record.evidence
    assert any("metadata.session_invalidation" in item for item in record.evidence)


def test_high_risk_user_facing_token_cookie_changes_sort_before_internal_sessions():
    result = build_task_session_management_impact_plan(
        _plan(
            [
                _task(
                    "task-z-internal",
                    title="Refactor session store internals",
                    description="Update login session store implementation for authenticated session state.",
                    files_or_modules=["src/auth/session_store.py"],
                ),
                _task(
                    "task-a-cookie",
                    title="Change auth cookie refresh token",
                    description="Change session cookie and refresh token behavior for users.",
                    files_or_modules=["src/auth/cookies.py"],
                ),
            ]
        )
    )

    assert [record.task_id for record in result.recommendations] == [
        "task-a-cookie",
        "task-z-internal",
    ]
    assert result.recommendations[0].risk_level == "high"
    assert result.recommendations[1].risk_level == "medium"


def test_complete_acceptance_criteria_reduce_missing_controls_and_lower_risk():
    guarded = build_task_session_management_impact_plan(
        _plan(
            [
                _task(
                    "task-guarded",
                    title="Change refresh token session cookie behavior",
                    description="Update refresh token and session cookie handling.",
                    acceptance_criteria=[
                        "Cookie attributes include Secure, HttpOnly, SameSite, domain, path, and max-age.",
                        "Refresh token rotation and refresh token reuse detection are covered.",
                        "Logout invalidation clears the server session.",
                        "Device revocation removes trusted devices.",
                        "Idle timeout and absolute timeout are enforced.",
                        "CSRF and replay protection use nonce checks.",
                        "Audit logs capture login, logout, and revocation events.",
                    ],
                )
            ]
        )
    )
    unguarded = build_task_session_management_impact_plan(
        _plan(
            [
                _task(
                    "task-unguarded",
                    title="Change refresh token session cookie behavior",
                    description="Update refresh token and session cookie handling.",
                )
            ]
        )
    )

    guarded_record = guarded.recommendations[0]
    unguarded_record = unguarded.recommendations[0]
    assert guarded_record.missing_controls == ()
    assert guarded_record.risk_level == "low"
    assert len(unguarded_record.missing_controls) == 7
    assert unguarded_record.risk_level == "high"


def test_model_input_serializes_stably_without_mutation_and_aliases_match():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Logout semantics",
                description="Logout invalidation and audit logging for sign out.",
            ),
            _task(
                "task-a",
                title="Device sessions",
                description="Add device sessions and trusted device revocation.",
                metadata={"csrf_or_replay_guard": "CSRF tests cover device session mutations"},
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_task_session_management_impact_plan(model)
    alias_result = summarize_task_session_management_impact(plan)
    records = generate_task_session_management_impact(model)
    payload = task_session_management_impact_plan_to_dict(result)

    assert plan == original
    assert records == result.recommendations
    assert result.records == result.recommendations
    assert alias_result.to_dict() == result.to_dict()
    assert result.to_dicts() == payload["recommendations"]
    assert task_session_management_impact_to_dicts(records) == payload["recommendations"]
    assert task_session_management_impact_to_dicts(result) == payload["recommendations"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "recommendations", "flagged_task_ids", "summary"]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "session_surfaces",
        "missing_controls",
        "risk_level",
        "evidence",
    ]
    assert task_session_management_impact_plan_to_markdown(result) == result.to_markdown()


def test_empty_unrelated_or_malformed_inputs_return_stable_empty_outputs():
    empty = build_task_session_management_impact_plan(
        _plan([_task("task-general", title="Update dashboard labels", description="Copy-only UI work.")])
    )

    assert empty.recommendations == ()
    assert empty.flagged_task_ids == ()
    assert empty.summary == {
        "total_task_count": 1,
        "flagged_task_count": 0,
        "unrelated_task_count": 1,
        "missing_control_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_control_counts": {
            "cookie_attributes": 0,
            "token_rotation": 0,
            "logout_invalidation": 0,
            "device_revocation": 0,
            "idle_or_absolute_timeout": 0,
            "csrf_or_replay_guard": 0,
            "audit_logging": 0,
        },
        "session_surface_counts": {
            "login_session": 0,
            "session_cookie": 0,
            "refresh_token": 0,
            "remember_me": 0,
            "session_invalidation": 0,
            "device_session": 0,
            "logout": 0,
        },
    }
    assert "No session management impact" in empty.to_markdown()
    assert generate_task_session_management_impact({"tasks": "not a list"}) == ()
    assert generate_task_session_management_impact("not a plan") == ()
    assert generate_task_session_management_impact(None) == ()


def _plan(tasks):
    return {
        "id": "plan-session-impact",
        "implementation_brief_id": "brief-session-impact",
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
