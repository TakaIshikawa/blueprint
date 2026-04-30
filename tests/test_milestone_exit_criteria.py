import json

from blueprint.domain.models import ExecutionPlan
from blueprint.milestone_exit_criteria import (
    MilestoneExitCriteria,
    build_milestone_exit_criteria,
    milestone_exit_criteria_to_dict,
)


def test_groups_tasks_by_milestone_and_marks_completed_criteria_satisfied():
    criteria = build_milestone_exit_criteria(
        _plan(
            [
                _task(
                    "task-setup",
                    "Setup project",
                    milestone="Foundation",
                    acceptance_criteria=["Project installs"],
                    status="completed",
                    test_command="poetry run pytest tests/test_setup.py",
                ),
                _task(
                    "task-api",
                    "Build API",
                    milestone="Foundation",
                    acceptance_criteria=["API returns data"],
                    depends_on=["task-setup"],
                    status="pending",
                    test_command="poetry run pytest tests/test_api.py",
                ),
                _task(
                    "task-ui",
                    "Build UI",
                    milestone="Interface",
                    acceptance_criteria=["UI shows data"],
                    status="blocked",
                    risk_level="high",
                ),
            ]
        )
    )

    assert criteria == (
        MilestoneExitCriteria(
            milestone="Foundation",
            criteria=(
                "Satisfied: `task-setup` Project installs",
                "Validate `task-setup` with `poetry run pytest tests/test_setup.py`.",
                "Complete `task-api` Build API: API returns data",
                "Validate `task-api` with `poetry run pytest tests/test_api.py`.",
                "Dependency `task-setup` is completed for `task-api`.",
            ),
            blocking_task_ids=("task-api",),
            required_validation_commands=(
                "poetry run pytest tests/test_setup.py",
                "poetry run pytest tests/test_api.py",
            ),
            ready_to_exit=False,
        ),
        MilestoneExitCriteria(
            milestone="Interface",
            criteria=(
                "Complete `task-ui` Build UI: UI shows data",
                "Review high-risk controls for `task-ui` before exit.",
            ),
            blocking_task_ids=("task-ui",),
            required_validation_commands=(),
            ready_to_exit=False,
        ),
    )


def test_missing_milestones_are_grouped_stably_under_ungrouped():
    criteria = build_milestone_exit_criteria(
        _plan(
            [
                _task(
                    "task-a",
                    "Known milestone task",
                    milestone="Foundation",
                    acceptance_criteria=["Known task passes"],
                    status="completed",
                ),
                _task(
                    "task-b",
                    "Blank milestone task",
                    milestone=" ",
                    acceptance_criteria=["Blank task passes"],
                    status="completed",
                ),
                _task(
                    "task-c",
                    "Missing milestone task",
                    milestone=None,
                    acceptance_criteria=["Missing task passes"],
                    status="completed",
                ),
            ],
            milestones=[{"name": "Foundation"}, {"name": "Interface"}],
        )
    )

    assert [item.milestone for item in criteria] == ["Foundation", "Ungrouped"]
    assert criteria[1].criteria == (
        "Satisfied: `task-b` Blank task passes",
        "Satisfied: `task-c` Missing task passes",
    )
    assert criteria[1].ready_to_exit is True


def test_validation_commands_are_deduplicated_in_first_seen_order():
    criteria = build_milestone_exit_criteria(
        _plan(
            [
                _task(
                    "task-a",
                    "First task",
                    acceptance_criteria=["First passes"],
                    status="completed",
                    test_command="poetry run pytest tests/test_shared.py",
                ),
                _task(
                    "task-b",
                    "Second task",
                    acceptance_criteria=["Second passes"],
                    status="completed",
                    test_command="poetry run pytest tests/test_shared.py",
                ),
                _task(
                    "task-c",
                    "Third task",
                    acceptance_criteria=["Third passes"],
                    status="completed",
                    test_command="poetry run pytest tests/test_other.py",
                ),
            ]
        )
    )

    assert criteria[0].required_validation_commands == (
        "poetry run pytest tests/test_shared.py",
        "poetry run pytest tests/test_other.py",
    )
    assert criteria[0].ready_to_exit is True


def test_incomplete_or_unknown_dependencies_block_milestone_exit():
    criteria = build_milestone_exit_criteria(
        _plan(
            [
                _task(
                    "task-api",
                    "Build API",
                    acceptance_criteria=["API returns data"],
                    status="pending",
                ),
                _task(
                    "task-ui",
                    "Build UI",
                    acceptance_criteria=["UI shows data"],
                    depends_on=["task-api", "task-missing"],
                    status="completed",
                ),
            ]
        )
    )

    assert criteria[0].blocking_task_ids == ("task-api", "task-ui")
    assert "Complete dependency `task-api` before exiting `task-ui`." in criteria[0].criteria
    assert (
        "Complete dependency `task-missing` before exiting `task-ui`."
        in criteria[0].criteria
    )
    assert criteria[0].ready_to_exit is False


def test_accepts_execution_plan_models_and_serializes_stably():
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-ready",
                    "Ready task",
                    acceptance_criteria=["Ready behavior passes"],
                    status="completed",
                )
            ]
        )
    )

    criteria = build_milestone_exit_criteria(plan_model)
    payload = milestone_exit_criteria_to_dict(criteria)

    assert payload == [item.to_dict() for item in criteria]
    assert list(payload[0]) == [
        "milestone",
        "criteria",
        "blocking_task_ids",
        "required_validation_commands",
        "ready_to_exit",
    ]
    assert payload[0]["ready_to_exit"] is True
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks, *, milestones=None):
    return {
        "id": "plan-exit-criteria",
        "implementation_brief_id": "brief-exit-criteria",
        "target_repo": "example/repo",
        "milestones": milestones
        if milestones is not None
        else [
            {"name": "Foundation", "description": "Build the base"},
            {"name": "Interface", "description": "Expose the flow"},
        ],
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    acceptance_criteria,
    milestone="Foundation",
    depends_on=None,
    risk_level="low",
    status="pending",
    test_command=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": [],
        "acceptance_criteria": acceptance_criteria,
        "risk_level": risk_level,
        "status": status,
    }
    if test_command is not None:
        task["test_command"] = test_command
    return task
