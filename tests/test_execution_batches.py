from blueprint.execution_batches import ExecutionBatch, build_execution_batches


def test_build_execution_batches_handles_linear_chains():
    plan = _plan(
        [
            _task("task-a"),
            _task("task-b", depends_on=["task-a"]),
            _task("task-c", depends_on=["task-b"]),
        ]
    )

    assert build_execution_batches(plan) == [
        ExecutionBatch(batch_index=1, task_ids=["task-a"]),
        ExecutionBatch(batch_index=2, task_ids=["task-b"]),
        ExecutionBatch(batch_index=3, task_ids=["task-c"]),
    ]


def test_build_execution_batches_handles_fan_out_and_fan_in_graphs():
    plan = _plan(
        [
            _task("task-root"),
            _task("task-left", depends_on=["task-root"]),
            _task("task-right", depends_on=["task-root"]),
            _task("task-join", depends_on=["task-left", "task-right"]),
        ]
    )

    assert [batch.task_ids for batch in build_execution_batches(plan)] == [
        ["task-root"],
        ["task-left", "task-right"],
        ["task-join"],
    ]


def test_build_execution_batches_excludes_completed_tasks_by_default():
    plan = _plan(
        [
            _task("task-done", status="completed"),
            _task("task-api", depends_on=["task-done"]),
            _task("task-ui", depends_on=["task-api"]),
        ]
    )

    assert build_execution_batches(plan) == [
        ExecutionBatch(batch_index=1, task_ids=["task-api"]),
        ExecutionBatch(batch_index=2, task_ids=["task-ui"]),
    ]


def test_build_execution_batches_can_include_completed_tasks():
    plan = _plan(
        [
            _task("task-done", status="completed"),
            _task("task-api", depends_on=["task-done"]),
        ]
    )

    assert build_execution_batches(plan, include_completed=True) == [
        ExecutionBatch(batch_index=1, task_ids=["task-done"]),
        ExecutionBatch(batch_index=2, task_ids=["task-api"]),
    ]


def test_build_execution_batches_reports_unknown_dependencies_as_blocked():
    plan = _plan(
        [
            _task("task-root"),
            _task("task-api", depends_on=["task-missing"]),
            _task("task-ui", depends_on=["task-api"]),
        ]
    )

    assert build_execution_batches(plan) == [
        ExecutionBatch(batch_index=1, task_ids=["task-root"]),
        ExecutionBatch(
            batch_index=2,
            blocked_task_ids=["task-api", "task-ui"],
            unresolved_dependency_ids=["task-missing", "task-api"],
        ),
    ]


def test_build_execution_batches_reports_cycles_as_blocked():
    plan = _plan(
        [
            _task("task-root"),
            _task("task-a", depends_on=["task-c"]),
            _task("task-b", depends_on=["task-a"]),
            _task("task-c", depends_on=["task-b"]),
        ]
    )

    assert build_execution_batches(plan) == [
        ExecutionBatch(batch_index=1, task_ids=["task-root"]),
        ExecutionBatch(
            batch_index=2,
            blocked_task_ids=["task-a", "task-b", "task-c"],
            unresolved_dependency_ids=["task-c", "task-a", "task-b"],
        ),
    ]


def test_build_execution_batches_preserves_plan_order_within_batches():
    plan = _plan(
        [
            _task("task-z"),
            _task("task-a"),
            _task("task-m", depends_on=["task-z"]),
            _task("task-b", depends_on=["task-z"]),
            _task("task-final", depends_on=["task-m", "task-b"]),
        ]
    )

    assert [batch.task_ids for batch in build_execution_batches(plan)] == [
        ["task-z", "task-a"],
        ["task-m", "task-b"],
        ["task-final"],
    ]


def test_execution_batch_to_dict_is_json_ready():
    batch = ExecutionBatch(
        batch_index=1,
        task_ids=["task-a"],
        blocked_task_ids=["task-b"],
        unresolved_dependency_ids=["task-missing"],
    )

    assert batch.to_dict() == {
        "batch_index": 1,
        "task_ids": ["task-a"],
        "blocked_task_ids": ["task-b"],
        "unresolved_dependency_ids": ["task-missing"],
    }


def _plan(tasks):
    return {"id": "plan-test", "tasks": tasks}


def _task(task_id, *, depends_on=None, status="pending"):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "depends_on": depends_on or [],
        "status": status,
    }
