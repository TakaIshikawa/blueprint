from blueprint.execution_capacity import (
    CapacityLimitedBatch,
    plan_capacity_limited_batches,
    serialize_capacity_limited_batches,
)


def test_capacity_limited_batches_preserve_dependency_ordering():
    plan = _plan(
        [
            _task("task-a", estimated_hours=1),
            _task("task-b", depends_on=["task-a"], estimated_hours=1),
            _task("task-c", depends_on=["task-b"], estimated_hours=1),
        ]
    )

    assert plan_capacity_limited_batches(plan, default_capacity=10) == [
        CapacityLimitedBatch(
            batch_index=1,
            scheduled_task_ids=["task-a"],
            capacity_by_lane={"codex": 10},
            used_capacity_by_lane={"codex": 1},
        ),
        CapacityLimitedBatch(
            batch_index=2,
            scheduled_task_ids=["task-b"],
            capacity_by_lane={"codex": 10},
            used_capacity_by_lane={"codex": 1},
        ),
        CapacityLimitedBatch(
            batch_index=3,
            scheduled_task_ids=["task-c"],
            capacity_by_lane={"codex": 10},
            used_capacity_by_lane={"codex": 1},
        ),
    ]


def test_lane_specific_limits_defer_ready_tasks_without_overfilling_lane():
    plan = _plan(
        [
            _task("task-api", suggested_engine="codex", estimated_hours=2),
            _task("task-ui", suggested_engine="codex", estimated_hours=2),
            _task("task-review", suggested_engine="manual", estimated_hours=1),
        ]
    )

    batches = plan_capacity_limited_batches(
        plan,
        capacities={"codex": 3, "manual": 1},
    )

    assert batches == [
        CapacityLimitedBatch(
            batch_index=1,
            scheduled_task_ids=["task-api", "task-review"],
            deferred_task_ids=["task-ui"],
            capacity_by_lane={"codex": 3, "manual": 1},
            used_capacity_by_lane={"codex": 2, "manual": 1},
        ),
        CapacityLimitedBatch(
            batch_index=2,
            scheduled_task_ids=["task-ui"],
            capacity_by_lane={"codex": 3},
            used_capacity_by_lane={"codex": 2},
        ),
    ]


def test_default_capacity_applies_to_lanes_without_specific_limits():
    plan = _plan(
        [
            _task("task-human", suggested_engine=None, owner_type="human", estimated_hours=1),
            _task("task-agent-a", suggested_engine="codex", estimated_hours=1),
            _task("task-agent-b", suggested_engine="codex", estimated_hours=1),
        ]
    )

    batches = plan_capacity_limited_batches(
        plan,
        capacities={"human": 2},
        default_capacity=1,
    )

    assert batches == [
        CapacityLimitedBatch(
            batch_index=1,
            scheduled_task_ids=["task-human", "task-agent-a"],
            deferred_task_ids=["task-agent-b"],
            capacity_by_lane={"codex": 1, "human": 2},
            used_capacity_by_lane={"codex": 1, "human": 1},
        ),
        CapacityLimitedBatch(
            batch_index=2,
            scheduled_task_ids=["task-agent-b"],
            capacity_by_lane={"codex": 1},
            used_capacity_by_lane={"codex": 1},
        ),
    ]


def test_completed_dependencies_are_satisfied_but_not_scheduled():
    plan = _plan(
        [
            _task("task-done", status="completed", estimated_hours=3),
            _task("task-api", depends_on=["task-done"], estimated_complexity="low"),
        ]
    )

    assert plan_capacity_limited_batches(plan, default_capacity=1) == [
        CapacityLimitedBatch(
            batch_index=1,
            scheduled_task_ids=["task-api"],
            capacity_by_lane={"codex": 1},
            used_capacity_by_lane={"codex": 1},
        )
    ]


def test_impossible_oversized_tasks_get_their_own_batch_deterministically():
    plan = _plan(
        [
            _task("task-small", estimated_hours=1),
            _task("task-large", estimated_hours=5),
            _task("task-after", estimated_hours=1),
        ]
    )

    batches = plan_capacity_limited_batches(plan, default_capacity=2)

    assert batches == [
        CapacityLimitedBatch(
            batch_index=1,
            scheduled_task_ids=["task-small"],
            deferred_task_ids=["task-large", "task-after"],
            capacity_by_lane={"codex": 2},
            used_capacity_by_lane={"codex": 1},
        ),
        CapacityLimitedBatch(
            batch_index=2,
            scheduled_task_ids=["task-large"],
            deferred_task_ids=["task-after"],
            capacity_by_lane={"codex": 2},
            used_capacity_by_lane={"codex": 5},
        ),
        CapacityLimitedBatch(
            batch_index=3,
            scheduled_task_ids=["task-after"],
            capacity_by_lane={"codex": 2},
            used_capacity_by_lane={"codex": 1},
        ),
    ]


def test_unknown_dependencies_are_reported_as_blocked():
    plan = _plan(
        [
            _task("task-root", estimated_hours=1),
            _task("task-api", depends_on=["task-missing"], estimated_hours=1),
            _task("task-ui", depends_on=["task-api"], estimated_hours=1),
        ]
    )

    assert plan_capacity_limited_batches(plan, default_capacity=2) == [
        CapacityLimitedBatch(
            batch_index=1,
            scheduled_task_ids=["task-root"],
            capacity_by_lane={"codex": 2},
            used_capacity_by_lane={"codex": 1},
        ),
        CapacityLimitedBatch(
            batch_index=2,
            capacity_by_lane={"codex": 2},
            blocked_task_ids=["task-api", "task-ui"],
        ),
    ]


def test_serializer_returns_json_ready_payloads():
    batch = CapacityLimitedBatch(
        batch_index=1,
        scheduled_task_ids=["task-a"],
        deferred_task_ids=["task-b"],
        capacity_by_lane={"codex": 2},
        used_capacity_by_lane={"codex": 1},
        blocked_task_ids=["task-c"],
    )

    assert serialize_capacity_limited_batches([batch]) == [
        {
            "batch_index": 1,
            "scheduled_task_ids": ["task-a"],
            "deferred_task_ids": ["task-b"],
            "capacity_by_lane": {"codex": 2},
            "used_capacity_by_lane": {"codex": 1},
            "blocked_task_ids": ["task-c"],
        }
    ]


def _plan(tasks):
    return {"id": "plan-test", "tasks": tasks}


def _task(
    task_id,
    *,
    depends_on=None,
    status="pending",
    suggested_engine="codex",
    owner_type="agent",
    estimated_complexity="medium",
    estimated_hours=None,
):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "owner_type": owner_type,
        "suggested_engine": suggested_engine,
        "depends_on": depends_on or [],
        "status": status,
        "estimated_complexity": estimated_complexity,
        "estimated_hours": estimated_hours,
    }
