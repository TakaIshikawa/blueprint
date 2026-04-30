import json

from blueprint.domain.models import ExecutionTask
from blueprint.task_reviewer_assignment import (
    TaskReviewerAssignment,
    TaskReviewerRecommendation,
    build_task_reviewer_assignment,
    task_reviewer_assignment_to_dict,
)


def test_security_sensitive_files_get_security_and_code_owner_reviewers():
    result = build_task_reviewer_assignment(
        _plan(
            [
                _task(
                    "task-auth",
                    title="Harden token rotation",
                    description="Update auth middleware and sanitize token logging.",
                    files_or_modules=["src/blueprint/auth/tokens.py"],
                    acceptance_criteria=["Expired tokens cannot be reused."],
                    owner_type="backend",
                    suggested_engine="codex",
                )
            ]
        )
    )

    assignment = result.assignments[0]

    assert isinstance(assignment, TaskReviewerAssignment)
    assert assignment.task_id == "task-auth"
    assert _recommendation(assignment, "security") == TaskReviewerRecommendation(
        role="security",
        priority="high",
        reason=(
            "Elevated security review is recommended for auth, secrets, "
            "or permission-sensitive signals."
        ),
        evidence=(
            "files_or_modules: src/blueprint/auth/tokens.py",
            "title: Harden token rotation",
            "description: Update auth middleware and sanitize token logging.",
            "acceptance_criteria[0]: Expired tokens cannot be reused.",
        ),
    )
    assert _recommendation(assignment, "code_owner").priority == "medium"
    assert result.reviewer_demand_by_role == {
        "code_owner": 1,
        "security": 1,
    }


def test_database_tasks_get_data_reviewer_from_paths_and_acceptance_criteria():
    result = build_task_reviewer_assignment(
        _plan(
            [
                _task(
                    "task-db",
                    title="Add account preference table",
                    description="Create an Alembic migration and SQLAlchemy model.",
                    files_or_modules=[
                        "migrations/versions/20260501_account_preferences.py",
                        "src/blueprint/store/models.py",
                    ],
                    acceptance_criteria=[
                        "Run alembic upgrade head.",
                        "Model tests verify persisted defaults.",
                    ],
                )
            ]
        )
    )

    assignment = result.assignments[0]

    assert _recommendation(assignment, "data").priority == "medium"
    assert _recommendation(assignment, "data").evidence == (
        "files_or_modules: migrations/versions/20260501_account_preferences.py",
        "files_or_modules: src/blueprint/store/models.py",
        "title: Add account preference table",
        "description: Create an Alembic migration and SQLAlchemy model.",
        "acceptance_criteria[0]: Run alembic upgrade head.",
        "acceptance_criteria[1]: Model tests verify persisted defaults.",
    )
    assert _recommendation(assignment, "release").evidence == (
        "files_or_modules: migrations/versions/20260501_account_preferences.py",
        "acceptance_criteria[0]: Run alembic upgrade head.",
    )


def test_documentation_only_tasks_get_docs_reviewer_without_code_owner():
    result = build_task_reviewer_assignment(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update rollout runbook",
                    description="Document support handoff steps.",
                    files_or_modules=["docs/runbooks/release.md"],
                    acceptance_criteria=["Docs explain rollback owners."],
                )
            ]
        )
    )

    assignment = result.assignments[0]

    assert _recommendation(assignment, "docs").evidence == (
        "files_or_modules: docs/runbooks/release.md",
        "title: Update rollout runbook",
        "description: Document support handoff steps.",
        "acceptance_criteria[0]: Docs explain rollback owners.",
    )
    assert _recommendation(assignment, "release").priority == "medium"
    assert _roles(assignment) == ("release", "docs")


def test_high_risk_release_tasks_receive_elevated_priority_recommendation():
    result = build_task_reviewer_assignment(
        _plan(
            [
                _task(
                    "task-release",
                    title="Deploy feature flag rollout",
                    description="Coordinate production launch and rollback plan.",
                    files_or_modules=["deploy/helm/values.yaml"],
                    acceptance_criteria=["Release owner signs off before production rollout."],
                    milestone="Launch",
                    risk_level="high",
                    metadata={"release_notes": "Coordinate staged deployment."},
                )
            ]
        )
    )

    assignment = result.assignments[0]

    assert _recommendation(assignment, "release").priority == "high"
    assert "risk_level: high" in _recommendation(assignment, "release").evidence
    assert any(recommendation.priority == "high" for recommendation in assignment.recommendations)


def test_duplicate_reviewer_roles_merge_with_combined_evidence_and_stable_serialization():
    task_model = ExecutionTask.model_validate(
        _task(
            "task-metadata-security",
            title="Add permission audit checks",
            description="Review authorization behavior for admin tokens.",
            files_or_modules=["src/security/permissions.py"],
            acceptance_criteria=["Security tests cover token replay."],
            metadata={
                "reviewer_role": "security",
                "security_notes": ["Permission matrix needs manual review."],
            },
        )
    )

    result = build_task_reviewer_assignment(task_model)
    payload = task_reviewer_assignment_to_dict(result)
    assignment = result.assignments[0]
    security = _recommendation(assignment, "security")

    assert payload == result.to_dict()
    assert list(payload) == ["plan_id", "assignments", "reviewer_demand_by_role"]
    assert list(payload["assignments"][0]) == ["task_id", "title", "recommendations"]
    assert list(payload["assignments"][0]["recommendations"][0]) == [
        "role",
        "priority",
        "reason",
        "evidence",
    ]
    assert _roles(assignment).count("security") == 1
    assert security.priority == "high"
    assert security.evidence == (
        "files_or_modules: src/security/permissions.py",
        "title: Add permission audit checks",
        "description: Review authorization behavior for admin tokens.",
        "acceptance_criteria[0]: Security tests cover token replay.",
        "metadata.reviewer_role: security",
        "metadata.security_notes[0]: Permission matrix needs manual review.",
    )
    assert result.reviewer_demand_by_role["security"] == 1
    assert json.loads(json.dumps(payload)) == payload


def _recommendation(assignment, role):
    return next(recommendation for recommendation in assignment.recommendations if recommendation.role == role)


def _roles(assignment):
    return tuple(recommendation.role for recommendation in assignment.recommendations)


def _plan(tasks):
    return {
        "id": "plan-reviewers",
        "implementation_brief_id": "brief-reviewers",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    milestone=None,
    owner_type=None,
    suggested_engine=None,
    files_or_modules=None,
    acceptance_criteria=None,
    risk_level=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if milestone is not None:
        task["milestone"] = milestone
    if owner_type is not None:
        task["owner_type"] = owner_type
    if suggested_engine is not None:
        task["suggested_engine"] = suggested_engine
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
