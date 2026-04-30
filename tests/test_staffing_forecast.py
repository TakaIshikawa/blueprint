import json

from blueprint.domain.models import ExecutionPlan
from blueprint.staffing_forecast import (
    MilestoneStaffingRecommendation,
    StaffingForecast,
    StaffingForecastBucket,
    build_staffing_forecast,
    staffing_forecast_to_dict,
)


def test_staffing_forecast_aggregates_hours_by_required_dimensions():
    forecast = build_staffing_forecast(_plan(), hours_per_parallel_slot=4)

    assert isinstance(forecast, StaffingForecast)
    assert isinstance(forecast.total, StaffingForecastBucket)
    assert forecast.total == StaffingForecastBucket(
        key="total",
        task_count=7,
        estimated_hours=17.0,
        missing_estimate_count=1,
        zero_hour_task_count=1,
    )
    assert [
        (
            bucket.key,
            bucket.task_count,
            bucket.estimated_hours,
            bucket.missing_estimate_count,
            bucket.zero_hour_task_count,
        )
        for bucket in forecast.by_milestone
    ] == [
        ("Foundation", 3, 6.0, 0, 1),
        ("Delivery", 3, 11.0, 0, 0),
        ("Ungrouped", 1, 0.0, 1, 0),
    ]
    assert [(bucket.key, bucket.estimated_hours) for bucket in forecast.by_owner_type] == [
        ("agent", 13.0),
        ("human", 4.0),
        ("unassigned", 0.0),
    ]
    assert [(bucket.key, bucket.estimated_hours) for bucket in forecast.by_suggested_engine] == [
        ("codex", 13.0),
        ("human", 4.0),
        ("unassigned", 0.0),
    ]
    assert [(bucket.key, bucket.estimated_hours) for bucket in forecast.by_risk_level] == [
        ("medium", 6.0),
        ("high", 7.0),
        ("low", 4.0),
        ("unassigned", 0.0),
    ]


def test_missing_estimated_hours_are_counted_separately_from_zero_hour_tasks():
    forecast = build_staffing_forecast(
        _plan(
            tasks=[
                _task("task-zero", "Zero", estimated_hours=0),
                _task("task-missing", "Missing", estimated_hours=None),
                _task("task-invalid", "Invalid", estimated_hours="unknown"),
                _task("task-known", "Known", estimated_hours="2.5"),
            ]
        )
    )

    assert forecast.total == StaffingForecastBucket(
        key="total",
        task_count=4,
        estimated_hours=2.5,
        missing_estimate_count=2,
        zero_hour_task_count=1,
    )


def test_parallel_slot_recommendations_account_for_dependency_blocked_tasks():
    forecast = build_staffing_forecast(_plan(), hours_per_parallel_slot=4)

    assert list(forecast.milestone_recommendations) == [
        MilestoneStaffingRecommendation(
            milestone="Foundation",
            task_count=3,
            estimated_hours=6.0,
            missing_estimate_count=0,
            zero_hour_task_count=1,
            dependency_ready_task_count=2,
            dependency_blocked_task_count=0,
            recommended_min_parallel_slots=2,
            dependency_ready_task_ids=("task-api", "task-worker"),
            dependency_blocked_task_ids=(),
        ),
        MilestoneStaffingRecommendation(
            milestone="Delivery",
            task_count=3,
            estimated_hours=11.0,
            missing_estimate_count=0,
            zero_hour_task_count=0,
            dependency_ready_task_count=1,
            dependency_blocked_task_count=2,
            recommended_min_parallel_slots=1,
            dependency_ready_task_ids=("task-copy",),
            dependency_blocked_task_ids=("task-ui", "task-release"),
        ),
        MilestoneStaffingRecommendation(
            milestone="Ungrouped",
            task_count=1,
            estimated_hours=0.0,
            missing_estimate_count=1,
            zero_hour_task_count=0,
            dependency_ready_task_count=1,
            dependency_blocked_task_count=0,
            recommended_min_parallel_slots=1,
            dependency_ready_task_ids=("task-research",),
            dependency_blocked_task_ids=(),
        ),
    ]


def test_completed_dependencies_unlock_ready_tasks_and_completed_tasks_do_not_need_slots():
    forecast = build_staffing_forecast(
        _plan(
            tasks=[
                _task("task-done", "Done", estimated_hours=2, status="completed"),
                _task(
                    "task-next",
                    "Next",
                    estimated_hours=6,
                    depends_on=["task-done"],
                ),
                _task(
                    "task-later",
                    "Later",
                    estimated_hours=6,
                    depends_on=["task-next"],
                ),
                _task(
                    "task-missing-dep",
                    "Missing dependency",
                    estimated_hours=6,
                    depends_on=["task-unknown"],
                ),
            ]
        ),
        hours_per_parallel_slot=4,
    )

    recommendation = forecast.milestone_recommendations[0]

    assert recommendation.dependency_ready_task_ids == ("task-next",)
    assert recommendation.dependency_blocked_task_ids == ("task-later", "task-missing-dep")
    assert recommendation.recommended_min_parallel_slots == 1


def test_accepts_execution_plan_models_and_serializes_stably():
    model = ExecutionPlan.model_validate(_plan())

    first = build_staffing_forecast(model, hours_per_parallel_slot=4)
    second = build_staffing_forecast(model, hours_per_parallel_slot=4)
    payload = staffing_forecast_to_dict(first)

    assert payload == staffing_forecast_to_dict(second)
    assert payload == first.to_dict()
    assert list(payload) == [
        "plan_id",
        "hours_per_parallel_slot",
        "total",
        "by_milestone",
        "by_owner_type",
        "by_suggested_engine",
        "by_risk_level",
        "milestone_recommendations",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(*, tasks=None):
    return {
        "id": "plan-staffing",
        "implementation_brief_id": "brief-staffing",
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
                "task-migration",
                "Add no-op migration",
                milestone="Foundation",
                owner_type="agent",
                suggested_engine="codex",
                risk_level="medium",
                estimated_hours=0,
                status="completed",
            ),
            _task(
                "task-ui",
                "Build UI",
                milestone="Delivery",
                owner_type="agent",
                suggested_engine="codex",
                risk_level="high",
                estimated_hours=3.5,
                depends_on=["task-api"],
            ),
            _task(
                "task-release",
                "Release flow",
                milestone="Delivery",
                owner_type="agent",
                suggested_engine="codex",
                risk_level="high",
                estimated_hours=3.5,
                depends_on=["task-ui"],
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
    depends_on=None,
    status="pending",
):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}.",
        "milestone": milestone,
        "owner_type": owner_type,
        "suggested_engine": suggested_engine,
        "depends_on": depends_on or [],
        "files_or_modules": ["src/blueprint/example.py"],
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": "medium",
        "estimated_hours": estimated_hours,
        "risk_level": risk_level,
        "test_command": "pytest",
        "status": status,
        "metadata": {},
    }
