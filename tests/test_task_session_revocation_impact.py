import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_session_revocation_impact import (
    TaskSessionRevocationImpactFinding,
    TaskSessionRevocationImpactPlan,
    analyze_task_session_revocation_impact,
    build_task_session_revocation_impact_plan,
    summarize_task_session_revocation_impact,
    summarize_task_session_revocation_impacts,
    task_session_revocation_impact_plan_to_dict,
    task_session_revocation_impact_plan_to_markdown,
)


def test_session_tasks_produce_deterministic_high_medium_low_findings():
    result = build_task_session_revocation_impact_plan(
        _plan(
            [
                _task(
                    "task-low",
                    title="Harden logout session revocation",
                    description=(
                        "Logout revokes sessions with server-side revocation, cookie invalidation, "
                        "refresh-token rotation, audit log events, replay protection, and cross-device logout."
                    ),
                ),
                _task(
                    "task-high",
                    title="Add support impersonation",
                    description="Support agents can impersonate customers through a privileged admin session.",
                ),
                _task(
                    "task-medium",
                    title="Remember trusted device",
                    description="Trusted device login stores a device session.",
                    acceptance_criteria=["Audit log records device trust changes."],
                ),
            ]
        )
    )

    assert isinstance(result, TaskSessionRevocationImpactPlan)
    assert result.impacted_task_ids == ("task-high", "task-medium", "task-low")
    by_id = {finding.task_id: finding for finding in result.findings}
    assert by_id["task-high"].impact_level == "high"
    assert {"impersonation", "admin_session"} <= set(by_id["task-high"].impacted_surfaces)
    assert by_id["task-medium"].impact_level == "medium"
    assert "audit_logging" not in by_id["task-medium"].missing_safeguards
    assert by_id["task-low"].impact_level == "low"
    assert by_id["task-low"].missing_safeguards == ()
    assert result.summary["impact_counts"] == {"high": 1, "medium": 1, "low": 1}


def test_mapping_and_execution_plan_inputs_serialize_equivalently_without_mutation():
    source = _plan(
        [
            _task(
                "task-reset",
                title="Password reset session cleanup",
                description="Password reset invalidates all sessions and clears the auth cookie.",
                metadata={
                    "safeguards": {
                        "server_side_revocation": "server-side revocation through session store",
                        "cookie_invalidation": "clear cookie on reset",
                        "refresh_token_rotation": "refresh-token rotation and reuse detection",
                        "audit_logging": "audit event records reset",
                        "replay_protection": "one-time token with nonce",
                        "cross_device_logout": "logout all devices",
                    }
                },
            ),
            _task("task-ui", title="Polish dashboard", description="Adjust filters and spacing."),
        ]
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    mapping_result = build_task_session_revocation_impact_plan(source)
    model_result = build_task_session_revocation_impact_plan(model)
    alias_result = summarize_task_session_revocation_impact(source)
    plural_alias_result = summarize_task_session_revocation_impacts(model)
    findings = analyze_task_session_revocation_impact(model)
    payload = task_session_revocation_impact_plan_to_dict(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert alias_result.to_dict() == mapping_result.to_dict()
    assert plural_alias_result.to_dict() == model_result.to_dict()
    assert findings == model_result.findings
    assert model_result.records == model_result.findings
    assert model_result.to_dicts() == payload["findings"]
    assert isinstance(model_result.findings[0], TaskSessionRevocationImpactFinding)
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "findings",
        "impacted_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["findings"][0]) == [
        "task_id",
        "title",
        "impacted_surfaces",
        "missing_safeguards",
        "impact_level",
        "evidence",
    ]
    assert payload["ignored_task_ids"] == ["task-ui"]


def test_object_like_tasks_and_iterables_are_supported():
    object_task = _ObjectTask(
        id="task-object",
        title="Refresh token flow",
        description="Token refresh issues a new access token.",
        files_or_modules=["src/auth/token_refresh.py"],
        acceptance_criteria=["Refresh-token rotation is required."],
    )

    result = build_task_session_revocation_impact_plan([object_task])

    assert result.records[0].task_id == "task-object"
    assert result.records[0].impacted_surfaces == ("token_refresh",)
    assert "refresh_token_rotation" not in result.records[0].missing_safeguards
    assert any("files_or_modules" in value for value in result.records[0].evidence)


def test_malformed_nested_metadata_and_evidence_deduplication_are_stable():
    result = build_task_session_revocation_impact_plan(
        _plan(
            [
                _task(
                    None,
                    title="Logout all devices",
                    description="Logout all devices",
                    metadata={
                        "auth": [
                            {"session": "Logout all devices"},
                            {"cookie": "Cookie invalidation clears the session cookie."},
                            object(),
                        ],
                        "safeguards": {"audit_logging": "audit log"},
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.task_id == "task-1"
    assert record.impacted_surfaces == ("logout", "session_cookie")
    assert record.evidence.count("description: Logout all devices") == 1
    assert any("metadata.auth[1].cookie" in value for value in record.evidence)


def test_markdown_escaping_empty_plans_and_non_security_tasks():
    result = build_task_session_revocation_impact_plan(
        _plan(
            [
                _task(
                    "task-pipe",
                    title="Login | password reset",
                    description="Login and password reset update the session cookie.",
                ),
                _task("task-copy", title="Copy tweaks", description="Update empty state copy."),
            ]
        )
    )

    markdown = task_session_revocation_impact_plan_to_markdown(result)

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Session Revocation Impact Plan: plan-session-revocation")
    assert "Login \\| password reset" in markdown
    assert result.ignored_task_ids == ("task-copy",)
    assert "Ignored tasks: task-copy" in markdown

    empty = build_task_session_revocation_impact_plan([])
    assert empty.records == ()
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Session Revocation Impact Plan",
            "",
            "No session revocation impact findings were inferred.",
        ]
    )

    non_security = build_task_session_revocation_impact_plan(
        _plan([_task("task-docs", title="Docs update", description="Refresh onboarding docs.")])
    )
    assert non_security.records == ()
    assert non_security.ignored_task_ids == ("task-docs",)


def _plan(tasks):
    return {
        "id": "plan-session-revocation",
        "implementation_brief_id": "brief-session-revocation",
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
    tags=None,
    metadata=None,
):
    task = {
        "title": title or task_id or "Untitled task",
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if task_id is not None:
        task["id"] = task_id
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if tags is not None:
        task["tags"] = tags
    if metadata is not None:
        task["metadata"] = metadata
    return task


class _ObjectTask:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
