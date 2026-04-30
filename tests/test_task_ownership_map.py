import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_ownership_map import (
    CrossOwnerDependencyEdge,
    TaskOwnershipGroup,
    TaskOwnershipMap,
    build_task_ownership_map,
    task_ownership_map_to_dict,
)


def test_groups_tasks_by_owner_and_engine_in_first_appearance_order():
    ownership_map = build_task_ownership_map(
        _plan(
            [
                _task(
                    "task-api",
                    "Build API",
                    owner_type="agent",
                    suggested_engine="codex",
                    estimated_hours=2,
                    risk_level="high",
                ),
                _task(
                    "task-docs",
                    "Write docs",
                    owner_type="human",
                    suggested_engine="manual",
                    estimated_hours=1.5,
                ),
                _task(
                    "task-ui",
                    "Build UI",
                    owner_type="agent",
                    suggested_engine="codex",
                    estimated_hours="3",
                    risk_level="low",
                ),
                _task(
                    "task-review",
                    "Review release",
                    owner_type="human",
                    suggested_engine="manual",
                    estimated_hours=None,
                    risk_level="critical",
                ),
            ]
        )
    )

    assert isinstance(ownership_map, TaskOwnershipMap)
    assert ownership_map.plan_id == "plan-ownership"
    assert ownership_map.task_count == 4
    assert ownership_map.unassigned_task_ids == ()
    assert ownership_map.owner_groups == (
        TaskOwnershipGroup(
            owner_type="agent",
            suggested_engine="codex",
            task_ids=("task-api", "task-ui"),
            total_estimated_hours=2.0,
            high_risk_task_ids=("task-api",),
        ),
        TaskOwnershipGroup(
            owner_type="human",
            suggested_engine="manual",
            task_ids=("task-docs", "task-review"),
            total_estimated_hours=1.5,
            high_risk_task_ids=("task-review",),
        ),
    )


def test_unassigned_tasks_require_missing_owner_and_engine():
    ownership_map = build_task_ownership_map(
        _plan(
            [
                _task(
                    "task-none",
                    "Needs routing",
                    owner_type=None,
                    suggested_engine=None,
                ),
                _task(
                    "task-owner-only",
                    "Has owner",
                    owner_type="agent",
                    suggested_engine=None,
                ),
                _task(
                    "task-engine-only",
                    "Has engine",
                    owner_type=None,
                    suggested_engine="codex",
                ),
            ]
        )
    )

    assert ownership_map.unassigned_task_ids == ("task-none",)
    assert [group.to_dict() for group in ownership_map.owner_groups] == [
        {
            "owner_type": None,
            "suggested_engine": None,
            "task_count": 1,
            "task_ids": ["task-none"],
            "total_estimated_hours": None,
            "high_risk_task_ids": [],
        },
        {
            "owner_type": "agent",
            "suggested_engine": None,
            "task_count": 1,
            "task_ids": ["task-owner-only"],
            "total_estimated_hours": None,
            "high_risk_task_ids": [],
        },
        {
            "owner_type": None,
            "suggested_engine": "codex",
            "task_count": 1,
            "task_ids": ["task-engine-only"],
            "total_estimated_hours": None,
            "high_risk_task_ids": [],
        },
    ]


def test_cross_owner_dependency_edges_include_source_and_target_groups():
    ownership_map = build_task_ownership_map(
        _plan(
            [
                _task(
                    "task-setup",
                    "Setup schema",
                    owner_type="agent",
                    suggested_engine="codex",
                ),
                _task(
                    "task-api",
                    "Build API",
                    owner_type="agent",
                    suggested_engine="codex",
                    depends_on=["task-setup"],
                ),
                _task(
                    "task-docs",
                    "Document API",
                    owner_type="human",
                    suggested_engine="manual",
                    depends_on=["task-api", "task-missing"],
                ),
                _task(
                    "task-smoothie",
                    "Run alternate engine",
                    owner_type="agent",
                    suggested_engine="smoothie",
                    depends_on=["task-api"],
                ),
            ]
        )
    )

    assert ownership_map.cross_owner_dependency_edges == (
        CrossOwnerDependencyEdge(
            source_task_id="task-api",
            target_task_id="task-docs",
            source_owner_type="agent",
            source_suggested_engine="codex",
            target_owner_type="human",
            target_suggested_engine="manual",
        ),
        CrossOwnerDependencyEdge(
            source_task_id="task-api",
            target_task_id="task-smoothie",
            source_owner_type="agent",
            source_suggested_engine="codex",
            target_owner_type="agent",
            target_suggested_engine="smoothie",
        ),
    )


def test_accepts_execution_plan_models_and_serializes_stably():
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-api",
                    "Build API",
                    owner_type="agent",
                    suggested_engine="codex",
                    estimated_hours=2.25,
                )
            ]
        )
    )

    ownership_map = build_task_ownership_map(plan_model)
    payload = task_ownership_map_to_dict(ownership_map)

    assert payload == ownership_map.to_dict()
    assert list(payload) == [
        "plan_id",
        "task_count",
        "owner_groups",
        "unassigned_task_ids",
        "cross_owner_dependency_edges",
    ]
    assert list(payload["owner_groups"][0]) == [
        "owner_type",
        "suggested_engine",
        "task_count",
        "task_ids",
        "total_estimated_hours",
        "high_risk_task_ids",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks):
    return {
        "id": "plan-ownership",
        "implementation_brief_id": "brief-ownership",
        "target_repo": "example/repo",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    owner_type,
    suggested_engine,
    depends_on=None,
    estimated_hours=None,
    risk_level="low",
):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "owner_type": owner_type,
        "suggested_engine": suggested_engine,
        "depends_on": depends_on or [],
        "files_or_modules": [],
        "acceptance_criteria": [f"{title} is complete"],
        "risk_level": risk_level,
        "estimated_hours": estimated_hours,
        "status": "pending",
    }
