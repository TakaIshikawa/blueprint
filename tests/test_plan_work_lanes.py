import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_work_lanes import (
    BlockedPlanWorkTask,
    PlanWorkLane,
    PlanWorkLaneAssignment,
    build_plan_work_lanes,
    plan_work_lanes_to_dict,
)


def test_independent_tasks_share_a_lane():
    result = build_plan_work_lanes(
        _plan(
            [
                _task("task-api", title="Build API", files_or_modules=["src/api.py"]),
                _task("task-ui", title="Build UI", files_or_modules=["src/ui.py"]),
            ]
        )
    )

    assert result.lanes == (
        PlanWorkLane(
            lane_index=1,
            dependency_level=1,
            assignments=(
                PlanWorkLaneAssignment(
                    task_id="task-api",
                    title="Build API",
                    dependency_level=1,
                    files_or_modules=("src/api.py",),
                ),
                PlanWorkLaneAssignment(
                    task_id="task-ui",
                    title="Build UI",
                    dependency_level=1,
                    files_or_modules=("src/ui.py",),
                ),
            ),
        ),
    )
    assert result.blocked_tasks == ()
    assert result.unresolved_dependency_ids == ()


def test_dependency_chains_create_later_lanes():
    result = build_plan_work_lanes(
        _plan(
            [
                _task("task-schema", title="Schema"),
                _task("task-service", title="Service", depends_on=["task-schema"]),
                _task("task-client", title="Client", depends_on=["task-service"]),
            ]
        )
    )

    assert [(lane.dependency_level, lane.task_ids) for lane in result.lanes] == [
        (1, ("task-schema",)),
        (2, ("task-service",)),
        (3, ("task-client",)),
    ]


def test_file_contention_splits_otherwise_eligible_tasks():
    result = build_plan_work_lanes(
        _plan(
            [
                _task("task-a", title="First edit", files_or_modules=["src/shared.py"]),
                _task("task-b", title="Second edit", files_or_modules=["src/shared.py"]),
                _task("task-c", title="Independent edit", files_or_modules=["src/other.py"]),
            ]
        )
    )

    assert [(lane.dependency_level, lane.task_ids) for lane in result.lanes] == [
        (1, ("task-a", "task-c")),
        (1, ("task-b",)),
    ]
    assert result.lanes[1].assignments[0].conflict_reasons == (
        "Separated from task-a due to overlapping files_or_modules: src/shared.py",
    )


def test_missing_dependencies_are_reported_without_crashing():
    result = build_plan_work_lanes(
        _plan(
            [
                _task("task-root", title="Root"),
                _task("task-api", title="API", depends_on=["task-missing"]),
                _task("task-ui", title="UI", depends_on=["task-api"]),
            ]
        )
    )

    assert [lane.task_ids for lane in result.lanes] == [("task-root",)]
    assert result.unresolved_dependency_ids == ("task-missing",)
    assert result.blocked_tasks == (
        BlockedPlanWorkTask(
            task_id="task-api",
            title="API",
            depends_on=("task-missing",),
            unresolved_dependency_ids=("task-missing",),
            block_reasons=("depends_on references unknown task 'task-missing'",),
        ),
        BlockedPlanWorkTask(
            task_id="task-ui",
            title="UI",
            depends_on=("task-api",),
            block_reasons=("depends_on references unscheduled task 'task-api'",),
        ),
    )


def test_accepts_execution_plan_models_and_serializes_stably():
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task("task-foundation", title="Foundation"),
                _task(
                    "task-feature",
                    title="Feature",
                    depends_on=["task-foundation"],
                    files_or_modules=["src/feature.py"],
                ),
            ]
        )
    )

    result = build_plan_work_lanes(plan_model)
    payload = plan_work_lanes_to_dict(result)

    assert payload == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "lanes",
        "blocked_tasks",
        "unresolved_dependency_ids",
    ]
    assert list(payload["lanes"][0]) == [
        "lane_index",
        "dependency_level",
        "task_ids",
        "assignments",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_identical_payloads_produce_deterministic_lane_ordering():
    plan = _plan(
        [
            _task("task-z", title="Zed", files_or_modules=["src/shared.py"]),
            _task("task-a", title="Alpha", files_or_modules=["src/shared.py"]),
            _task("task-m", title="Middle", files_or_modules=["src/middle.py"]),
            _task("task-final", title="Final", depends_on=["task-z", "task-a"]),
        ]
    )

    first = build_plan_work_lanes(plan).to_dict()
    second = build_plan_work_lanes(plan).to_dict()

    assert first == second
    assert [
        (lane["dependency_level"], lane["task_ids"])
        for lane in first["lanes"]
    ] == [
        (1, ["task-z", "task-m"]),
        (1, ["task-a"]),
        (2, ["task-final"]),
    ]


def _plan(tasks):
    return {
        "id": "plan-work-lanes",
        "implementation_brief_id": "brief-work-lanes",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    depends_on=None,
    files_or_modules=None,
):
    return {
        "id": task_id,
        "title": title or task_id,
        "description": f"Implement {task_id}.",
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": ["Done"],
        "status": "pending",
    }
