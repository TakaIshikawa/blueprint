import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_cost_rollup import (
    PlanCostBucket,
    PlanCostRollup,
    build_plan_cost_rollup,
    plan_cost_rollup_to_dict,
)


def test_plan_cost_rollup_aggregates_hours_by_required_dimensions():
    rollup = build_plan_cost_rollup(_plan())

    assert isinstance(rollup, PlanCostRollup)
    assert isinstance(rollup.total, PlanCostBucket)
    assert rollup.total == PlanCostBucket(
        key="total",
        task_count=5,
        estimated_hours=13.5,
        missing_estimate_count=1,
    )
    assert [
        (bucket.key, bucket.estimated_hours, bucket.missing_estimate_count)
        for bucket in rollup.by_milestone
    ] == [
        ("Foundation", 6.0, 0),
        ("Delivery", 7.5, 0),
        ("Ungrouped", 0.0, 1),
    ]
    assert [(bucket.key, bucket.estimated_hours) for bucket in rollup.by_owner_type] == [
        ("agent", 9.5),
        ("human", 4.0),
        ("unassigned", 0.0),
    ]
    assert [(bucket.key, bucket.estimated_hours) for bucket in rollup.by_suggested_engine] == [
        ("codex", 9.5),
        ("human", 4.0),
        ("unassigned", 0.0),
    ]
    assert [(bucket.key, bucket.estimated_hours) for bucket in rollup.by_risk_level] == [
        ("medium", 6.0),
        ("high", 3.5),
        ("low", 4.0),
        ("unassigned", 0.0),
    ]


def test_hourly_rates_add_costs_for_matching_owner_or_engine_keys_only():
    rollup = build_plan_cost_rollup(
        _plan(),
        hourly_rates={
            "agent": 100,
            "human": 125,
            "unassigned": 0,
        },
    )

    assert rollup.total.estimated_cost == 1450.0
    assert [(bucket.key, bucket.estimated_cost) for bucket in rollup.by_milestone] == [
        ("Foundation", 600.0),
        ("Delivery", 850.0),
        ("Ungrouped", None),
    ]
    assert [(bucket.key, bucket.estimated_cost) for bucket in rollup.by_owner_type] == [
        ("agent", 950.0),
        ("human", 500.0),
        ("unassigned", None),
    ]


def test_engine_rates_are_used_when_owner_rate_is_missing():
    rollup = build_plan_cost_rollup(
        _plan(
            tasks=[
                _task(
                    "task-api",
                    "Build API",
                    milestone="Foundation",
                    owner_type="specialist",
                    suggested_engine="codex",
                    risk_level="medium",
                    estimated_hours=2,
                ),
                _task(
                    "task-copy",
                    "Review copy",
                    milestone="Foundation",
                    owner_type="human",
                    suggested_engine="manual",
                    risk_level="low",
                    estimated_hours=1.5,
                ),
            ]
        ),
        hourly_rates={"codex": 80},
    )

    assert rollup.total.estimated_cost == 160.0
    assert rollup.by_owner_type == (
        PlanCostBucket(
            key="specialist",
            task_count=1,
            estimated_hours=2.0,
            missing_estimate_count=0,
            estimated_cost=160.0,
        ),
        PlanCostBucket(
            key="human",
            task_count=1,
            estimated_hours=1.5,
            missing_estimate_count=0,
        ),
    )


def test_missing_estimated_hours_are_counted_without_breaking_totals():
    rollup = build_plan_cost_rollup(
        _plan(
            tasks=[
                _task(
                    "task-known",
                    "Known estimate",
                    milestone="Foundation",
                    estimated_hours=2.5,
                ),
                _task(
                    "task-missing",
                    "Missing estimate",
                    milestone="Foundation",
                    estimated_hours=None,
                ),
            ]
        )
    )

    assert rollup.total.task_count == 2
    assert rollup.total.estimated_hours == 2.5
    assert rollup.total.missing_estimate_count == 1
    assert rollup.by_milestone == (
        PlanCostBucket(
            key="Foundation",
            task_count=2,
            estimated_hours=2.5,
            missing_estimate_count=1,
        ),
    )


def test_accepts_execution_plan_models_and_serializes_stably():
    model = ExecutionPlan.model_validate(_plan())

    rollup = build_plan_cost_rollup(model, hourly_rates={"codex": 90})
    payload = plan_cost_rollup_to_dict(rollup)

    assert payload == rollup.to_dict()
    assert list(payload) == [
        "plan_id",
        "total",
        "by_milestone",
        "by_owner_type",
        "by_suggested_engine",
        "by_risk_level",
    ]
    assert list(payload["total"]) == [
        "key",
        "task_count",
        "estimated_hours",
        "missing_estimate_count",
        "estimated_cost",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(*, tasks=None):
    return {
        "id": "plan-cost",
        "implementation_brief_id": "brief-cost",
        "target_repo": "example/repo",
        "milestones": [
            {"name": "Foundation", "description": "Build the base"},
            {"name": "Delivery", "description": "Ship the flow"},
        ],
        "test_strategy": "Run focused validation",
        "metadata": {},
        "tasks": tasks
        if tasks is not None
        else [
            _task(
                "task-api",
                "Build API",
                milestone="Foundation",
                owner_type="agent",
                suggested_engine="codex",
                risk_level="medium",
                estimated_hours=3.5,
            ),
            _task(
                "task-worker",
                "Build worker",
                milestone="Foundation",
                owner_type="agent",
                suggested_engine="codex",
                risk_level="medium",
                estimated_hours=2.5,
            ),
            _task(
                "task-ui",
                "Build UI",
                milestone="Delivery",
                owner_type="agent",
                suggested_engine="codex",
                risk_level="high",
                estimated_hours=3.5,
            ),
            _task(
                "task-copy",
                "Write copy",
                milestone="Delivery",
                owner_type="human",
                suggested_engine="human",
                risk_level="low",
                estimated_hours=4,
            ),
            _task(
                "task-research",
                "Research rollout",
                milestone=None,
                owner_type=None,
                suggested_engine=None,
                risk_level=None,
                estimated_hours=None,
            ),
        ],
    }


def _task(
    task_id,
    title,
    *,
    estimated_hours,
    milestone="Foundation",
    owner_type="agent",
    suggested_engine="codex",
    risk_level="medium",
):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "milestone": milestone,
        "owner_type": owner_type,
        "suggested_engine": suggested_engine,
        "depends_on": [],
        "files_or_modules": ["src/blueprint/example.py"],
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": "medium",
        "estimated_hours": estimated_hours,
        "risk_level": risk_level,
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": {},
    }
