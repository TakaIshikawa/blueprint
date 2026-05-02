import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_admin_impersonation_safety import (
    TaskAdminImpersonationSafetyPlan,
    TaskAdminImpersonationSafetyRecommendation,
    build_task_admin_impersonation_safety_plan,
    extract_task_admin_impersonation_safety_recommendations,
    summarize_task_admin_impersonation_safety,
    task_admin_impersonation_safety_plan_to_dict,
    task_admin_impersonation_safety_plan_to_dicts,
    task_admin_impersonation_safety_plan_to_markdown,
)


def test_detects_impersonation_from_text_paths_and_metadata():
    result = build_task_admin_impersonation_safety_plan(
        _plan(
            [
                _task(
                    "task-login-as",
                    title="Add login-as-user route",
                    description="Admins can login-as-user from the internal console.",
                    files_or_modules=["src/admin/impersonation/session_controller.py"],
                ),
                _task(
                    "task-delegated",
                    title="Delegated account access",
                    metadata={"support_access": {"mode": "Support agents may view customer accounts."}},
                ),
                _task(
                    "task-dashboard",
                    title="Admin dashboard reporting",
                    description="Add admin dashboard filters and CSV labels.",
                ),
            ],
            metadata={"audit": "User-visible audit trail is required for impersonation launch."},
        )
    )

    by_id = {recommendation.task_id: recommendation for recommendation in result.recommendations}

    assert set(by_id) == {"task-login-as", "task-delegated"}
    assert by_id["task-login-as"].access_surfaces == (
        "admin_console",
        "authentication_session",
        "api",
    )
    assert "user_visible_audit_trail" not in by_id["task-login-as"].missing_safeguards
    assert any("files_or_modules[0]" in item for item in by_id["task-login-as"].evidence)
    assert any("metadata.support_access.mode" in item for item in by_id["task-delegated"].evidence)
    assert result.summary["task_count"] == 3
    assert result.summary["impersonation_task_count"] == 2
    assert result.summary["access_surface_counts"]["support_tooling"] == 1


def test_existing_safeguards_reduce_risk_and_leave_only_missing_controls():
    result = build_task_admin_impersonation_safety_plan(
        _plan(
            [
                _task(
                    "task-safe",
                    title="Support impersonation session guardrails",
                    description=(
                        "Support impersonation uses scoped permissions and requires ticket id reason capture. "
                        "The user-visible audit trail records every access log entry. "
                        "Session time limit auto-expires after 15 minutes. "
                        "Approval workflow is required for broad access. "
                        "Block sensitive actions including billing, password, export, and delete. "
                        "Post-session review sends completed sessions to a supervisor review queue."
                    ),
                    files_or_modules=["src/support/impersonation/session.py"],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert isinstance(recommendation, TaskAdminImpersonationSafetyRecommendation)
    assert recommendation.missing_safeguards == ()
    assert recommendation.risk_level == "low"
    assert recommendation.recommended_acceptance_criteria == ()
    assert result.summary["missing_safeguard_count"] == 0
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_high_risk_support_tooling_recommends_required_safeguards():
    result = build_task_admin_impersonation_safety_plan(
        _plan(
            [
                _task(
                    "task-support",
                    title="Build customer support account access",
                    description="Support agents can access customer accounts to debug billing issues.",
                    files_or_modules=["src/support/customer_accounts.py"],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.risk_level == "high"
    assert recommendation.access_surfaces == (
        "support_tooling",
        "customer_account",
        "data_export",
    )
    assert recommendation.missing_safeguards == (
        "scoped_permissions",
        "explicit_reason_capture",
        "user_visible_audit_trail",
        "session_time_limit",
        "approval_workflow",
        "sensitive_action_blocking",
        "post_session_review",
    )
    assert "Block sensitive changes" in recommendation.recommended_acceptance_criteria[5]
    assert result.summary["missing_safeguard_counts"]["sensitive_action_blocking"] == 1


def test_sensitive_action_blocking_and_audit_evidence_reduce_incomplete_task_to_medium():
    result = build_task_admin_impersonation_safety_plan(
        _plan(
            [
                _task(
                    "task-reduced",
                    title="Login as user for support",
                    description=(
                        "Support login lets agents inspect customer accounts. "
                        "Customer-visible audit log captures access evidence. "
                        "Block sensitive actions including billing, password changes, and exports."
                    ),
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.risk_level == "medium"
    assert "user_visible_audit_trail" not in recommendation.missing_safeguards
    assert "sensitive_action_blocking" not in recommendation.missing_safeguards
    assert "scoped_permissions" in recommendation.missing_safeguards


def test_unrelated_admin_dashboard_work_is_not_flagged_and_serializes_empty_summary():
    plan = _plan(
        [
            _task(
                "task-admin",
                title="Admin dashboard reporting",
                description="Add admin filters, account status columns, and CSV label cleanup.",
                files_or_modules=["src/admin/dashboard/reports.py"],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_admin_impersonation_safety_plan(plan)

    assert plan == original
    assert isinstance(result, TaskAdminImpersonationSafetyPlan)
    assert result.recommendations == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.to_dict() == {
        "plan_id": "plan-impersonation",
        "recommendations": [],
        "summary": {
            "task_count": 1,
            "impersonation_task_count": 0,
            "missing_safeguard_count": 0,
            "at_risk_task_count": 0,
            "risk_counts": {"high": 0, "medium": 0, "low": 0},
            "access_surface_counts": {
                "admin_console": 0,
                "support_tooling": 0,
                "customer_account": 0,
                "authentication_session": 0,
                "api": 0,
                "data_export": 0,
            },
            "missing_safeguard_counts": {
                "scoped_permissions": 0,
                "explicit_reason_capture": 0,
                "user_visible_audit_trail": 0,
                "session_time_limit": 0,
                "approval_workflow": 0,
                "sensitive_action_blocking": 0,
                "post_session_review": 0,
            },
        },
    }
    assert "No admin impersonation" in result.to_markdown()


def test_deterministic_serialization_markdown_and_alias_helpers():
    result = build_task_admin_impersonation_safety_plan(
        _plan(
            [
                _task(
                    "task-md",
                    title="Break-glass access | support",
                    description=(
                        "Break-glass support impersonation. Approval workflow and session time limit "
                        "are required before launch."
                    ),
                )
            ]
        )
    )
    payload = task_admin_impersonation_safety_plan_to_dict(result)
    markdown = task_admin_impersonation_safety_plan_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "recommendations", "summary"]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "access_surfaces",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "recommended_acceptance_criteria",
    ]
    assert markdown == result.to_markdown()
    assert "| `task-md` | Break-glass access \\| support | high |" in markdown
    assert task_admin_impersonation_safety_plan_to_dicts(result) == payload["recommendations"]
    assert task_admin_impersonation_safety_plan_to_dicts(result.recommendations) == payload["recommendations"]
    assert extract_task_admin_impersonation_safety_recommendations(result.recommendations) == result.recommendations
    assert summarize_task_admin_impersonation_safety(result) is result


def test_execution_plan_input_matches_mapping_input():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Customer support account access",
                description=(
                    "Support agents can access customer accounts. "
                    "Audit trail and sensitive-action blocking are required."
                ),
                acceptance_criteria=["Reason capture records the support ticket id."],
            )
        ]
    )

    model = ExecutionPlan.model_validate(plan)

    assert build_task_admin_impersonation_safety_plan(model).to_dict() == (
        build_task_admin_impersonation_safety_plan(plan).to_dict()
    )


def _plan(tasks, *, metadata=None):
    return {
        "id": "plan-impersonation",
        "implementation_brief_id": "brief-impersonation",
        "milestones": [],
        "tasks": tasks,
        "metadata": metadata or {},
    }


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
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
