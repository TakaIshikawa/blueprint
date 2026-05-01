import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_owner_load_balancer import (
    PlanOwnerLoadBalance,
    build_plan_owner_load_balance,
    plan_owner_load_balance_to_dict,
    plan_owner_load_balance_to_markdown,
    recommend_plan_owner_load_balance,
)


def test_balanced_plan_has_load_totals_without_recommendations():
    result = build_plan_owner_load_balance(
        _plan(
            [
                _task("task-api", owner="api-owner", estimated_hours=3, risk_level="low"),
                _task("task-ui", owner="ui-owner", estimated_hours=4, risk_level="medium"),
                _task("task-docs", owner="api-owner", estimated_hours=1, risk_level="low"),
                _task("task-copy", owner="ui-owner", estimated_hours=1, risk_level="low"),
            ]
        )
    )

    loads = {load.owner: load for load in result.owner_loads}

    assert result.plan_id == "plan-load"
    assert result.overloaded_owners == ()
    assert result.unowned_tasks == ()
    assert result.recommendations == ()
    assert loads["api-owner"].task_count == 2
    assert loads["api-owner"].estimated_effort == 4.0
    assert loads["ui-owner"].task_count == 2
    assert loads["ui-owner"].estimated_effort == 5.0
    assert result.summary == {
        "task_count": 4,
        "owned_task_count": 4,
        "unowned_task_count": 0,
        "owner_count": 2,
        "overloaded_owner_count": 0,
        "recommendation_count": 0,
        "estimated_effort": 9.0,
        "high_risk_task_count": 0,
    }


def test_overloaded_owner_gets_reassignment_candidates_without_mutating_plan():
    plan = _plan(
        [
            _task("task-api-1", owner="api-owner", estimated_hours=6, risk_level="medium"),
            _task("task-api-2", owner="api-owner", estimated_hours=5, risk_level="low"),
            _task("task-api-3", owner="api-owner", estimated_hours=4, risk_level="low"),
            _task("task-api-4", owner="api-owner", estimated_hours=3, risk_level="low"),
            _task("task-ui", owner="ui-owner", estimated_hours=2, risk_level="low"),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_owner_load_balance(plan)

    assert plan == original
    assert result.overloaded_owners == ("api-owner",)
    assert result.owner_loads[0].owner == "api-owner"
    assert result.owner_loads[0].overload_reasons == (
        "task_count_above_plan_average",
        "estimated_effort_above_plan_average",
    )
    assert [item.task_id for item in result.recommendations] == [
        "task-api-1",
        "task-api-2",
    ]
    assert {item.to_owner for item in result.recommendations} == {"ui-owner"}
    assert result.summary["recommendation_count"] == 2


def test_unowned_tasks_are_reported_separately_from_overloaded_owners():
    result = build_plan_owner_load_balance(
        _plan(
            [
                _task("task-owned-1", owner="platform", estimated_hours=4),
                _task("task-owned-2", owner="platform", estimated_hours=4),
                _task("task-owned-3", owner="platform", estimated_hours=4),
                _task("task-owned-4", owner="platform", estimated_hours=4),
                _task("task-light", owner="qa", estimated_hours=1),
                _task("task-unowned", owner=None, estimated_hours=2, risk_level="high"),
                _task("task-assignee", owner=None, estimated_hours=1, metadata={"assignee": "qa"}),
            ]
        )
    )

    assert result.overloaded_owners == ("platform",)
    assert [task.task_id for task in result.unowned_tasks] == ["task-unowned"]
    assert result.unowned_tasks[0].estimated_effort == 2.0
    assert result.unowned_tasks[0].risk_level == "high"
    assert all(item.from_owner != "unowned" for item in result.recommendations)
    assert result.summary["owned_task_count"] == 6
    assert result.summary["unowned_task_count"] == 1


def test_high_risk_concentration_recommends_moving_high_risk_candidate():
    result = build_plan_owner_load_balance(
        _plan(
            [
                _task("task-payment", owner="backend", estimated_hours=3, risk_level="high"),
                _task("task-migration", owner="backend", estimated_hours=2, risk_level="critical"),
                _task("task-copy", owner="backend", estimated_hours=1, risk_level="low"),
                _task("task-ui", owner="frontend", estimated_hours=4, risk_level="low"),
            ]
        )
    )

    backend = next(load for load in result.owner_loads if load.owner == "backend")

    assert backend.high_risk_task_count == 2
    assert backend.high_risk_task_ids == ("task-payment", "task-migration")
    assert backend.overload_reasons == ("high_risk_concentration",)
    assert result.overloaded_owners == ("backend",)
    assert [item.task_id for item in result.recommendations] == [
        "task-payment",
        "task-migration",
    ]
    assert all(item.reason == "high_risk_concentration" for item in result.recommendations)
    assert all(item.risk_level in {"high", "critical"} for item in result.recommendations)


def test_complexity_fallback_model_iterable_and_serialization_are_stable():
    plan = _plan(
        [
            _task(
                "task-large",
                owner="backend",
                estimated_hours=None,
                estimated_complexity="high",
            ),
            _task(
                "task-small",
                owner="frontend",
                estimated_hours=None,
                estimated_complexity="low",
            ),
        ]
    )
    result = build_plan_owner_load_balance(ExecutionPlan.model_validate(plan))
    payload = plan_owner_load_balance_to_dict(result)
    task = recommend_plan_owner_load_balance(
        ExecutionTask.model_validate(_task("task-single", owner="agent", estimated_hours=1))
    )
    iterable = build_plan_owner_load_balance(
        [_task("task-assigned", owner=None, metadata={"owner": "ops"}, estimated_hours="2")]
    )

    assert isinstance(result, PlanOwnerLoadBalance)
    assert result.summary["estimated_effort"] == 10.0
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["owner_loads"]
    assert list(payload) == [
        "plan_id",
        "owner_loads",
        "overloaded_owners",
        "unowned_tasks",
        "recommendations",
        "summary",
    ]
    assert list(payload["owner_loads"][0]) == [
        "owner",
        "task_count",
        "estimated_effort",
        "missing_estimate_count",
        "high_risk_task_count",
        "high_risk_task_ids",
        "task_ids",
        "overload_reasons",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert task.owner_loads[0].owner == "agent"
    assert iterable.owner_loads[0].owner == "ops"


def test_markdown_and_empty_plan_rendering():
    result = build_plan_owner_load_balance(
        _plan([_task("task-api", owner="api-owner", estimated_hours=2)])
    )
    empty = build_plan_owner_load_balance({"id": "plan-empty", "tasks": []})

    assert plan_owner_load_balance_to_markdown(result) == result.to_markdown()
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Owner Load Balance: plan-load",
            "",
            "| Owner | Tasks | Estimated Effort | High-risk Tasks | Overload Reasons |",
            "| --- | --- | --- | --- | --- |",
            "| api-owner | 1 | 2 | 0 | none |",
        ]
    )
    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Owner Load Balance: plan-empty",
            "",
            "No tasks were available for owner load analysis.",
        ]
    )


def _plan(tasks):
    return {
        "id": "plan-load",
        "implementation_brief_id": "brief-load",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    owner,
    estimated_hours=1,
    estimated_complexity="medium",
    risk_level="low",
    metadata=None,
):
    return {
        "id": task_id,
        "title": task_id.replace("-", " ").title(),
        "description": f"Implement {task_id}.",
        "owner_type": owner,
        "acceptance_criteria": ["Tests pass."],
        "estimated_hours": estimated_hours,
        "estimated_complexity": estimated_complexity,
        "risk_level": risk_level,
        "metadata": metadata or {},
    }
