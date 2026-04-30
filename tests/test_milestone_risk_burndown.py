import json

from blueprint.domain.models import ExecutionPlan
from blueprint.milestone_risk_burndown import (
    MilestoneRiskBurndownRecord,
    build_milestone_risk_burndown,
    milestone_risk_burndown_to_dict,
)


def test_builds_mixed_status_burndown_with_fallback_and_blocked_tasks():
    records = build_milestone_risk_burndown(_plan())
    by_milestone = {record.milestone: record for record in records}

    assert by_milestone["Foundation"] == MilestoneRiskBurndownRecord(
        milestone="Foundation",
        total_tasks=2,
        completed_tasks=1,
        remaining_tasks=1,
        total_risk_points=5,
        remaining_risk_points=4,
        high_risk_task_ids=(),
        blocked_task_ids=(),
        completion_percent=50.0,
    )
    assert by_milestone["Interface"] == MilestoneRiskBurndownRecord(
        milestone="Interface",
        total_tasks=2,
        completed_tasks=0,
        remaining_tasks=1,
        total_risk_points=8,
        remaining_risk_points=7,
        high_risk_task_ids=("task-ui",),
        blocked_task_ids=("task-ui",),
        completion_percent=0.0,
    )
    assert by_milestone["Unassigned"].total_tasks == 1
    assert by_milestone["Unassigned"].remaining_risk_points == 4


def test_records_are_ordered_by_plan_milestones_then_name_then_unassigned():
    records = build_milestone_risk_burndown(_plan())

    assert [record.milestone for record in records] == [
        "Foundation",
        "Interface",
        "Release",
        "Follow-up",
        "Unassigned",
    ]
    assert records[2] == MilestoneRiskBurndownRecord(
        milestone="Release",
        total_tasks=0,
        completed_tasks=0,
        remaining_tasks=0,
        total_risk_points=0,
        remaining_risk_points=0,
        high_risk_task_ids=(),
        blocked_task_ids=(),
        completion_percent=0.0,
    )


def test_risk_weighting_uses_conservative_defaults_and_metadata_signals():
    plan = _plan(
        tasks=[
            _task(
                "task-missing",
                "Missing risk fields",
                milestone="Foundation",
                risk_level=None,
                estimated_complexity=None,
            ),
            _task(
                "task-unknown",
                "Unknown risk fields",
                milestone="Foundation",
                risk_level="surprising",
                estimated_complexity="unclear",
            ),
            _task(
                "task-metadata-level",
                "Metadata risk level",
                milestone="Foundation",
                risk_level=None,
                estimated_complexity=None,
                metadata={"riskLevel": "critical", "complexity": "high"},
            ),
            _task(
                "task-metadata-points",
                "Metadata risk points",
                milestone="Foundation",
                risk_level="low",
                estimated_complexity="low",
                metadata={"risk_points": "9"},
            ),
        ]
    )

    record = {
        record.milestone: record for record in build_milestone_risk_burndown(plan)
    }["Foundation"]

    assert record.total_tasks == 4
    assert record.total_risk_points == 27
    assert record.remaining_risk_points == 27
    assert record.high_risk_task_ids == (
        "task-metadata-level",
        "task-metadata-points",
    )


def test_accepts_execution_plan_model_and_serializes_json_compatible_records():
    model = ExecutionPlan.model_validate(_plan())

    first = build_milestone_risk_burndown(model)
    second = build_milestone_risk_burndown(model)
    payload = milestone_risk_burndown_to_dict(first)

    assert payload == milestone_risk_burndown_to_dict(second)
    assert payload[0] == {
        "milestone": "Foundation",
        "total_tasks": 2,
        "completed_tasks": 1,
        "remaining_tasks": 1,
        "total_risk_points": 5,
        "remaining_risk_points": 4,
        "high_risk_task_ids": [],
        "blocked_task_ids": [],
        "completion_percent": 50.0,
    }
    assert list(payload[0]) == [
        "milestone",
        "total_tasks",
        "completed_tasks",
        "remaining_tasks",
        "total_risk_points",
        "remaining_risk_points",
        "high_risk_task_ids",
        "blocked_task_ids",
        "completion_percent",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks=None):
    return {
        "id": "plan-risk-burndown",
        "implementation_brief_id": "brief-risk-burndown",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [
            {"name": "Foundation", "description": "Build the base service"},
            {"name": "Interface", "description": "Expose the user flow"},
            {"name": "Release", "description": "Ship the change"},
        ],
        "test_strategy": "Run focused validation",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "metadata": {},
        "tasks": _tasks() if tasks is None else tasks,
    }


def _tasks():
    return [
        _task(
            "task-setup",
            "Setup project",
            milestone="Foundation",
            risk_level="low",
            estimated_complexity="low",
            status="completed",
        ),
        _task(
            "task-api",
            "Build API",
            milestone="Foundation",
            risk_level="medium",
            estimated_complexity="medium",
            status="in_progress",
        ),
        _task(
            "task-ui",
            "Build UI",
            milestone="Interface",
            risk_level="high",
            estimated_complexity="high",
            status="blocked",
            blocked_reason="Waiting for API contract",
        ),
        _task(
            "task-copy",
            "Write copy",
            milestone="Interface",
            risk_level="low",
            estimated_complexity="low",
            status="skipped",
        ),
        _task(
            "task-follow-up",
            "Follow-up task",
            milestone="Follow-up",
            risk_level="medium",
            estimated_complexity="low",
            status="completed",
        ),
        _task(
            "task-unassigned",
            "Unassigned task",
            milestone=None,
            risk_level=None,
            estimated_complexity=None,
            status="pending",
        ),
    ]


def _task(
    task_id,
    title,
    *,
    milestone,
    risk_level="low",
    estimated_complexity="low",
    status="pending",
    blocked_reason=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "execution_plan_id": "plan-risk-burndown",
        "title": title,
        "description": f"Implement {title}",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": ["src/example.py"],
        "acceptance_criteria": [f"{title} is observable"],
        "estimated_complexity": estimated_complexity,
        "risk_level": risk_level,
        "test_command": "pytest tests/test_milestone_risk_burndown.py",
        "status": status,
        "metadata": metadata or {},
    }
    if blocked_reason is not None:
        task["blocked_reason"] = blocked_reason
    return task
