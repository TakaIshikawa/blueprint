import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_dependency_owner_escalation import (
    PlanDependencyOwnerEscalationMatrix,
    PlanDependencyOwnerEscalationRow,
    build_plan_dependency_owner_escalation_matrix,
    plan_dependency_owner_escalation_matrix_to_dict,
    plan_dependency_owner_escalation_matrix_to_markdown,
    summarize_plan_dependency_owner_escalation,
)


def test_owned_dependency_has_no_missing_owner_fields():
    result = build_plan_dependency_owner_escalation_matrix(
        _plan(
            [
                _task(
                    "task-launch",
                    depends_on=[
                        {
                            "name": "Security review",
                            "owner": "Security",
                            "escalation_path": "#security-oncall",
                            "target_response_date": "2026-05-06",
                            "decision_needed": "Approve threat model exceptions.",
                        }
                    ],
                )
            ]
        )
    )

    assert isinstance(result, PlanDependencyOwnerEscalationMatrix)
    row = result.rows[0]
    assert isinstance(row, PlanDependencyOwnerEscalationRow)
    assert row.dependency_name == "Security review"
    assert row.blocked_task_ids == ("task-launch",)
    assert row.current_owner == "Security"
    assert row.missing_owner_fields == ()
    assert row.escalation_path == "#security-oncall"
    assert row.target_response_date == "2026-05-06"
    assert row.decision_needed == "Approve threat model exceptions."
    assert result.summary == {
        "dependency_count": 1,
        "dependencies_missing_owners": 0,
        "dependencies_missing_escalation_paths": 0,
        "blocked_task_count": 1,
    }


def test_missing_owner_escalation_from_blockers_risks_metadata_and_descriptions():
    result = summarize_plan_dependency_owner_escalation(
        _plan(
            [
                _task(
                    "task-copy",
                    description="Blocked by Legal approval before publishing consent copy.",
                    blocked_reason="Waiting on Legal approval for launch copy.",
                    risks=["External dependency on Privacy decision could block release."],
                    metadata={"dependency": "blocked by Legal approval", "response_by": "2026-05-07"},
                )
            ]
        )
    )

    assert [row.dependency_name for row in result.rows] == [
        "Legal approval",
        "Privacy decision could block release",
    ]
    legal = result.rows[0]
    assert legal.blocked_task_ids == ("task-copy",)
    assert legal.missing_owner_fields == ("current_owner", "escalation_path")
    assert legal.target_response_date == "2026-05-07"
    assert legal.escalation_recommendation == (
        "Assign owner, escalation path for Legal approval before dependent work continues."
    )
    assert "approval" in legal.decision_needed.casefold()
    assert result.summary["dependencies_missing_owners"] == 2
    assert result.summary["dependencies_missing_escalation_paths"] == 2
    assert result.summary["blocked_task_count"] == 1


def test_duplicate_dependency_normalization_groups_blocked_tasks():
    result = build_plan_dependency_owner_escalation_matrix(
        _plan(
            [
                _task("task-a", depends_on=["Legal Approval"]),
                _task(
                    "task-b",
                    depends_on=[{"name": "legal approval", "owner": "Legal", "escalate_to": "GC"}],
                ),
            ]
        )
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.dependency_name == "Legal Approval"
    assert row.blocked_task_ids == ("task-a", "task-b")
    assert row.current_owner == "Legal"
    assert row.escalation_path == "GC"
    assert row.missing_owner_fields == ("target_response_date",)
    assert result.summary["blocked_task_count"] == 2


def test_no_signal_plans_return_empty_matrix():
    result = build_plan_dependency_owner_escalation_matrix(
        _plan([_task("task-docs", description="Update local docs.", depends_on=[])], plan_id="plan-empty")
    )

    assert result.plan_id == "plan-empty"
    assert result.rows == ()
    assert result.summary == {
        "dependency_count": 0,
        "dependencies_missing_owners": 0,
        "dependencies_missing_escalation_paths": 0,
        "blocked_task_count": 0,
    }
    assert "No dependency-owner escalation signals were detected." in result.to_markdown()


def test_dict_output_is_stable_json_safe_and_supports_model_input():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    metadata={
                        "external_dependencies": [
                            {
                                "name": "Partner API approval",
                                "team": "Partnerships",
                                "backup_owner": "Partner Ops",
                                "respond_by": "2026-05-08",
                            }
                        ]
                    },
                )
            ],
            plan_id="plan-model",
        )
    )

    result = build_plan_dependency_owner_escalation_matrix(plan)
    payload = plan_dependency_owner_escalation_matrix_to_dict(result)

    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "summary"]
    assert list(payload["rows"][0]) == [
        "dependency_name",
        "blocked_task_ids",
        "current_owner",
        "missing_owner_fields",
        "escalation_recommendation",
        "decision_needed",
        "escalation_path",
        "target_response_date",
        "evidence",
    ]
    assert payload["rows"][0]["current_owner"] == "Partnerships"


def test_markdown_output_escapes_tables_and_uses_helper():
    result = build_plan_dependency_owner_escalation_matrix(
        _plan(
            [
                _task(
                    "task-pipe",
                    depends_on=[
                        {
                            "name": "Design | UX approval",
                            "owner": "Design | UX",
                            "escalation_path": "#design | #ux",
                            "target_response_date": "2026-05-09",
                            "decision_needed": "Approve checkout | settings copy.",
                        }
                    ],
                )
            ],
            plan_id="plan-md",
        )
    )

    markdown = plan_dependency_owner_escalation_matrix_to_markdown(result)

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Plan Dependency Owner Escalation Matrix: plan-md")
    assert "Design \\| UX approval" in markdown
    assert "#design \\| #ux" in markdown
    assert "Approve checkout \\| settings copy." in markdown


def _plan(tasks, *, plan_id="plan-owner-escalation"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-owner-escalation",
        "milestones": [{"name": "Launch"}],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    depends_on=None,
    risks=None,
    blocked_reason=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or f"Task {task_id}",
        "description": description or f"Implement {task_id}.",
        "milestone": "Launch",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{task_id} works"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": None,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": blocked_reason,
    }
    if risks is not None:
        task["risks"] = risks
    return task
