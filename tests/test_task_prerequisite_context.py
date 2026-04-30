import json

import pytest

from blueprint.domain.models import ExecutionPlan
from blueprint.task_prerequisite_context import (
    TaskPrerequisiteContext,
    TaskPrerequisiteSummary,
    build_task_prerequisite_context,
    task_prerequisite_context_to_dict,
)


def test_direct_dependencies_include_task_summary_fields():
    context = build_task_prerequisite_context(
        _plan(
            tasks=[
                _task(
                    "task-api",
                    "Build API",
                    status="completed",
                    files=["src/blueprint/api.py"],
                    acceptance=["API returns prerequisite data"],
                    test_command="poetry run pytest tests/test_api.py",
                    metadata={
                        "completion_evidence": {
                            "commit": "abc123",
                            "test_output": "tests passed",
                        }
                    },
                ),
                _task("task-ui", "Build UI", depends_on=["task-api"]),
            ],
        ),
        "task-ui",
    )

    assert isinstance(context, TaskPrerequisiteContext)
    assert isinstance(context.direct_dependencies[0], TaskPrerequisiteSummary)
    assert context.transitive_dependencies == ()
    assert context.unresolved_dependency_ids == ()
    assert context.direct_dependencies[0].to_dict() == {
        "task_id": "task-api",
        "title": "Build API",
        "status": "completed",
        "files_or_modules": ["src/blueprint/api.py"],
        "acceptance_criteria": ["API returns prerequisite data"],
        "test_command": "poetry run pytest tests/test_api.py",
        "evidence": {
            "commit": "abc123",
            "test_output": "tests passed",
        },
    }


def test_transitive_dependencies_are_separate_and_topological():
    context = build_task_prerequisite_context(
        _plan(
            tasks=[
                _task("task-schema", "Schema", status="completed"),
                _task("task-api", "API", depends_on=["task-schema"], status="completed"),
                _task("task-copy", "Copy", status="completed"),
                _task(
                    "task-ui",
                    "UI",
                    depends_on=["task-api", "task-copy"],
                ),
            ],
        ),
        "task-ui",
    )

    assert [task.task_id for task in context.direct_dependencies] == [
        "task-api",
        "task-copy",
    ]
    assert [task.task_id for task in context.transitive_dependencies] == ["task-schema"]


def test_completed_and_pending_status_summaries_are_preserved():
    context = build_task_prerequisite_context(
        _plan(
            tasks=[
                _task("task-ready", "Ready", status="completed"),
                _task("task-waiting", "Waiting", status="pending"),
                _task(
                    "task-selected",
                    "Selected",
                    depends_on=["task-ready", "task-waiting"],
                ),
            ],
        ),
        "task-selected",
    )

    assert [
        (dependency.task_id, dependency.status) for dependency in context.direct_dependencies
    ] == [
        ("task-ready", "completed"),
        ("task-waiting", "pending"),
    ]


def test_unknown_task_id_raises_clear_value_error():
    with pytest.raises(ValueError, match="Unknown task_id: 'task-missing'"):
        build_task_prerequisite_context(
            _plan(tasks=[_task("task-known", "Known")]),
            "task-missing",
        )


def test_missing_dependency_ids_are_reported():
    context = build_task_prerequisite_context(
        _plan(
            tasks=[
                _task("task-setup", "Setup", depends_on=["task-missing-transitive"]),
                _task(
                    "task-selected",
                    "Selected",
                    depends_on=["task-setup", "task-missing-direct"],
                ),
            ],
        ),
        "task-selected",
    )

    assert [task.task_id for task in context.direct_dependencies] == ["task-setup"]
    assert context.transitive_dependencies == ()
    assert context.unresolved_dependency_ids == (
        "task-missing-transitive",
        "task-missing-direct",
    )


def test_accepts_execution_plan_models_and_serializes_stably():
    model = ExecutionPlan.model_validate(
        _plan(
            tasks=[
                _task(
                    "task-a",
                    "Task A",
                    status="completed",
                    metadata={"evidence": "done in prior branch"},
                ),
                _task("task-b", "Task B", depends_on=["task-a"]),
            ],
        )
    )

    context = build_task_prerequisite_context(model, "task-b")
    payload = task_prerequisite_context_to_dict(context)

    assert payload == context.to_dict()
    assert list(payload) == [
        "plan_id",
        "task_id",
        "title",
        "direct_dependencies",
        "transitive_dependencies",
        "unresolved_dependency_ids",
    ]
    assert payload["plan_id"] == "plan-prerequisites"
    assert payload["direct_dependencies"][0]["status"] == "completed"
    assert payload["direct_dependencies"][0]["evidence"] == {"evidence": "done in prior branch"}
    assert json.loads(json.dumps(payload)) == payload


def _plan(*, tasks):
    return {
        "id": "plan-prerequisites",
        "implementation_brief_id": "brief-prerequisites",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    status="pending",
    depends_on=None,
    files=None,
    acceptance=None,
    test_command=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "depends_on": depends_on or [],
        "files_or_modules": files or [f"src/blueprint/{task_id}.py"],
        "acceptance_criteria": acceptance or [f"{title} is complete"],
        "status": status,
        "metadata": metadata or {},
    }
    if test_command is not None:
        task["test_command"] = test_command
    return task
