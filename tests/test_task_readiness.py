import json

from blueprint.task_readiness import score_task_readiness


def test_ready_roots_and_completed_dependencies_are_ready():
    result = score_task_readiness(
        _plan(
            [
                _task("task-root", "Root"),
                _task("task-setup", "Setup", status="completed"),
                _task("task-api", "API", depends_on=["task-setup"]),
            ]
        )
    )

    assert [task.task_id for task in result.ready_tasks] == ["task-root", "task-api"]
    assert result.tasks[0].to_dict() == {
        "task_id": "task-root",
        "title": "Root",
        "task_status": "pending",
        "readiness": "ready",
        "ready": True,
        "dependency_ids": [],
        "satisfied_dependency_ids": [],
        "waiting_dependency_ids": [],
        "blocked_dependency_ids": [],
        "invalid_dependency_ids": [],
        "blocker_dependency_ids": [],
    }
    assert result.tasks[2].satisfied_dependency_ids == ["task-setup"]


def test_pending_and_in_progress_dependencies_make_tasks_waiting():
    result = score_task_readiness(
        _plan(
            [
                _task("task-setup", "Setup", status="pending"),
                _task("task-api", "API", status="in_progress"),
                _task("task-ui", "UI", depends_on=["task-setup", "task-api"]),
            ]
        )
    )

    task = _by_id(result, "task-ui")
    assert task.readiness == "waiting"
    assert task.waiting_dependency_ids == ["task-setup", "task-api"]
    assert task.blocker_dependency_ids == ["task-setup", "task-api"]
    assert result.to_dict()["waiting_task_ids"] == ["task-ui"]


def test_blocked_dependencies_make_tasks_blocked():
    result = score_task_readiness(
        _plan(
            [
                _task("task-api", "API", status="blocked"),
                _task("task-ui", "UI", depends_on=["task-api"]),
            ]
        )
    )

    task = _by_id(result, "task-ui")
    assert task.readiness == "blocked"
    assert task.blocked_dependency_ids == ["task-api"]
    assert task.blocker_dependency_ids == ["task-api"]
    assert result.to_dict()["blocked_task_ids"] == ["task-api", "task-ui"]


def test_skipped_dependencies_satisfy_readiness_but_skipped_tasks_are_not_ready():
    result = score_task_readiness(
        _plan(
            [
                _task("task-legacy", "Legacy", status="skipped"),
                _task("task-replacement", "Replacement", depends_on=["task-legacy"]),
            ]
        )
    )

    skipped = _by_id(result, "task-legacy")
    replacement = _by_id(result, "task-replacement")
    assert skipped.readiness == "skipped"
    assert skipped.ready is False
    assert replacement.readiness == "ready"
    assert replacement.satisfied_dependency_ids == ["task-legacy"]
    assert result.to_dict()["summary"]["skipped"] == 1


def test_unknown_dependencies_are_invalid_blockers_without_crashing():
    result = score_task_readiness(
        _plan([_task("task-ui", "UI", depends_on=["task-api", "task-missing"])])
    )

    task = result.tasks[0]
    assert task.readiness == "blocked"
    assert task.invalid_dependency_ids == ["task-api", "task-missing"]
    assert task.blocker_dependency_ids == ["task-api", "task-missing"]
    assert result.invalid_tasks == [task]
    assert result.to_dict()["invalid_task_ids"] == ["task-ui"]


def test_empty_plan_serializes_with_stable_keys_and_ordering():
    result = score_task_readiness({"id": "plan-empty", "tasks": []})

    assert result.to_dict() == {
        "plan_id": "plan-empty",
        "summary": {
            "ready": 0,
            "waiting": 0,
            "blocked": 0,
            "completed": 0,
            "in_progress": 0,
            "skipped": 0,
            "invalid": 0,
            "tasks": 0,
        },
        "ready_task_ids": [],
        "waiting_task_ids": [],
        "blocked_task_ids": [],
        "invalid_task_ids": [],
        "tasks": [],
    }


def test_to_dict_is_json_serializable_and_preserves_input_task_order():
    result = score_task_readiness(
        _plan(
            [
                _task("task-z", "Z"),
                _task("task-a", "A"),
                _task("task-m", "M", depends_on=["task-z"]),
            ]
        )
    )
    payload = result.to_dict()

    assert json.loads(json.dumps(payload, sort_keys=True)) == payload
    assert [task["task_id"] for task in payload["tasks"]] == [
        "task-z",
        "task-a",
        "task-m",
    ]
    assert list(payload) == [
        "plan_id",
        "summary",
        "ready_task_ids",
        "waiting_task_ids",
        "blocked_task_ids",
        "invalid_task_ids",
        "tasks",
    ]


def _by_id(result, task_id):
    return next(task for task in result.tasks if task.task_id == task_id)


def _plan(tasks):
    return {
        "id": "plan-readiness",
        "status": "planned",
        "tasks": tasks,
    }


def _task(task_id, title, *, status="pending", depends_on=None):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}",
        "status": status,
        "depends_on": depends_on or [],
    }
