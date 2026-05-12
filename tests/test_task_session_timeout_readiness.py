import copy
import json

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord
from blueprint.domain.models import ExecutionPlan
from blueprint.task_session_timeout_readiness import (
    TaskSessionTimeoutReadinessPlan,
    analyze_task_session_timeout_readiness,
    build_task_session_timeout_readiness_plan,
    recommend_task_session_timeout_readiness,
    summarize_task_session_timeout_readiness,
    summarize_task_session_timeout_readiness_plan,
    task_session_timeout_readiness_plan_to_dict,
    task_session_timeout_readiness_plan_to_dicts,
    task_session_timeout_readiness_plan_to_markdown,
)


def test_complete_session_timeout_task_is_ready():
    result = build_task_session_timeout_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Enforce session timeout and idle expiration",
                    description="Change auth session timeout, idle timeout, and absolute timeout behavior.",
                    acceptance_criteria=[
                        "Idle timeout policy sets a 30 minute inactivity timeout with a 2 minute grace period.",
                        "Absolute timeout policy limits session lifetime to 12 hours.",
                        "Remember-me persistent sessions use a separate timeout policy and can be disabled.",
                        "Session renewal uses sliding expiration and refresh session behavior before reauthentication.",
                        "User warning messaging shows a countdown modal before expiration.",
                        "Admin override supports tenant policy exceptions for privileged accounts.",
                        "Audit logs emit timeout events, expiration events, renewal events, and override outcomes.",
                        "Rollout uses a feature flag and canary with rollback through a kill switch.",
                        "Support impact is covered with help desk guidance, FAQ updates, and escalation paths.",
                    ],
                    files_or_modules=["src/auth/session_timeout_policy.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskSessionTimeoutReadinessPlan)
    assert isinstance(result, SimpleReadinessPlan)
    record = result.records[0]
    assert isinstance(record, SimpleReadinessRecord)
    assert record.readiness == "ready"
    assert record.detected_signals == ("session_timeout", "idle_timeout", "absolute_timeout")
    assert record.present_criteria == (
        "idle_timeout_policy",
        "absolute_timeout_policy",
        "remember_me_behavior",
        "renewal_behavior",
        "user_messaging",
        "admin_override",
        "audit_logging",
        "rollout_rollback",
        "support_impact",
    )
    assert record.missing_criteria == ()


def test_detects_timeout_tasks_from_metadata_tags_and_paths_with_gaps():
    source = _plan(
        [
            _task(
                "task-partial",
                title="Add idle session expiry",
                description="Expire inactive sessions after the configured idle timeout.",
                acceptance_criteria=["Warn users with a toast before session expiration and audit timeout events."],
                metadata={"tags": ["auth", "session-timeout"], "runbook": "Rollout behind a feature flag."},
                files_or_modules=["src/auth/session_expiration.py"],
            ),
            _task(
                "task-path-only",
                title="Move auth config",
                description="Refactor configuration module.",
                files_or_modules=["config/auth/session_timeout.yaml"],
            ),
            _task(
                "task-remember-copy",
                title="Polish remember-me label",
                description="Adjust remember-me copy on the login page without session expiration changes.",
                files_or_modules=["src/auth/login_form.py"],
            ),
        ]
    )
    original = copy.deepcopy(source)

    result = analyze_task_session_timeout_readiness(ExecutionPlan.model_validate(source))

    assert source == original
    by_id = {record.task_id: record for record in result.records}
    assert result.impacted_task_ids == ("task-path-only", "task-partial")
    assert result.ignored_task_ids == ("task-remember-copy",)
    assert by_id["task-partial"].readiness == "partial"
    assert by_id["task-partial"].present_criteria == ("idle_timeout_policy", "user_messaging", "audit_logging", "rollout_rollback")
    assert by_id["task-path-only"].readiness == "needs_planning"
    assert by_id["task-path-only"].detected_signals == ("session_timeout",)
    assert any("metadata.tags" in item for item in by_id["task-partial"].evidence)
    assert any("files_or_modules" in item for item in by_id["task-path-only"].evidence)


def test_unrelated_auth_tasks_and_explicit_no_impact_tasks_are_ignored():
    result = build_task_session_timeout_readiness_plan(
        _plan(
            [
                _task(
                    "task-auth-copy",
                    title="Update authentication page copy",
                    description="Adjust remember-me labels and login field validation.",
                    files_or_modules=["src/auth/login.py"],
                ),
                _task(
                    "task-no-timeout",
                    title="Update account settings",
                    description="No session timeout or idle timeout changes are required.",
                ),
            ]
        )
    )

    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.ignored_task_ids == ("task-auth-copy", "task-no-timeout")


def test_aliases_serialization_and_markdown_are_stable():
    source = _plan(
        [
            _task(
                "task-alias",
                title="Configure absolute session timeout",
                description="Absolute timeout policy limits session lifetime to 8 hours with audit logs.",
            )
        ],
        plan_id="plan-session-timeout-alias",
    )

    result = summarize_task_session_timeout_readiness(source)
    payload = task_session_timeout_readiness_plan_to_dict(result)
    markdown = task_session_timeout_readiness_plan_to_markdown(result)

    assert summarize_task_session_timeout_readiness_plan(result) is result
    assert recommend_task_session_timeout_readiness(source) == result.records
    assert build_task_session_timeout_readiness_plan(result) is result
    assert task_session_timeout_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-session-timeout-alias"
    assert markdown.startswith("# Task Session Timeout Readiness: plan-session-timeout-alias")
    assert "| Task | Title | Readiness |" in markdown


def test_invalid_inputs_return_empty_simple_plan():
    assert build_task_session_timeout_readiness_plan(42).records == ()
    assert build_task_session_timeout_readiness_plan({"id": "bad", "tasks": "not a list"}).records == ()
    assert build_task_session_timeout_readiness_plan({"tasks": []}).summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-session-timeout"):
    return {"id": plan_id, "implementation_brief_id": "brief-session-timeout", "milestones": [], "tasks": tasks}


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
