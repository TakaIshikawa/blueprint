from blueprint.milestone_capacity import analyze_milestone_capacity


def test_analyze_milestone_capacity_accepts_balanced_milestones():
    result = analyze_milestone_capacity(
        _plan(
            tasks=[
                _task("task-api", "Foundation", estimated_hours=2, risk_level="low"),
                _task("task-ui", "Delivery", story_points=3, risk_level="high"),
                _task("task-docs", "Delivery", size="S"),
            ]
        ),
        max_tasks_per_milestone=2,
        max_effort_per_milestone=6,
    )

    assert result.ok is True
    assert result.overload_findings == []
    assert result.suggested_moves == []
    assert [summary.to_dict() for summary in result.summaries] == [
        {
            "milestone": "Foundation",
            "task_count": 1,
            "effort": 2.0,
            "high_risk_task_count": 0,
            "dependency_pressure": 0,
            "incoming_dependency_count": 0,
            "outgoing_dependency_count": 0,
            "task_ids": ["task-api"],
        },
        {
            "milestone": "Delivery",
            "task_count": 2,
            "effort": 5.0,
            "high_risk_task_count": 1,
            "dependency_pressure": 0,
            "incoming_dependency_count": 0,
            "outgoing_dependency_count": 0,
            "task_ids": ["task-ui", "task-docs"],
        },
    ]


def test_analyze_milestone_capacity_flags_task_and_effort_overloads():
    result = analyze_milestone_capacity(
        _plan(
            tasks=[
                _task("task-a", "Foundation", estimate="3"),
                _task("task-b", "Foundation", estimated_hours=4),
                _task("task-c", "Foundation", size="L"),
                _task("task-d", "Delivery", size="XS"),
            ]
        ),
        max_tasks_per_milestone=2,
        max_effort_per_milestone=10,
    )

    assert [finding.to_dict() for finding in result.overload_findings] == [
        {
            "code": "effort_overload",
            "milestone": "Foundation",
            "metric": "effort",
            "value": 12.0,
            "threshold": 10.0,
            "task_ids": ["task-a", "task-b", "task-c"],
        },
        {
            "code": "task_count_overload",
            "milestone": "Foundation",
            "metric": "task_count",
            "value": 3.0,
            "threshold": 2.0,
            "task_ids": ["task-a", "task-b", "task-c"],
        },
    ]
    assert [move.task_id for move in result.suggested_moves] == [
        "task-c",
        "task-b",
        "task-a",
    ]
    assert {move.to_milestone for move in result.suggested_moves} == {"Delivery"}


def test_analyze_milestone_capacity_groups_missing_milestone_values():
    result = analyze_milestone_capacity(
        _plan(
            tasks=[
                _task("task-known", "Foundation", estimated_hours=1),
                _task("task-none", None, estimated_hours=2),
                _task("task-blank", " ", metadata={"size": "M"}),
            ]
        )
    )

    assert [summary.milestone for summary in result.summaries] == [
        "Foundation",
        "Delivery",
        "Unassigned",
    ]
    assert result.summaries[2].to_dict() == {
        "milestone": "Unassigned",
        "task_count": 2,
        "effort": 5.0,
        "high_risk_task_count": 0,
        "dependency_pressure": 0,
        "incoming_dependency_count": 0,
        "outgoing_dependency_count": 0,
        "task_ids": ["task-none", "task-blank"],
    }


def test_suggested_moves_skip_tasks_with_cross_milestone_dependency_constraints():
    result = analyze_milestone_capacity(
        _plan(
            tasks=[
                _task("task-setup", "Foundation"),
                _task("task-cross-dependent", "Foundation", depends_on=["task-api"]),
                _task("task-cross-dependency", "Foundation"),
                _task("task-free", "Foundation", size="M"),
                _task("task-api", "Delivery", depends_on=["task-cross-dependency"]),
                _task("task-release", "Release"),
            ],
            milestones=["Foundation", "Delivery", "Release"],
        ),
        max_tasks_per_milestone=2,
    )

    foundation = result.summaries[0]
    assert foundation.dependency_pressure == 2
    assert foundation.incoming_dependency_count == 1
    assert foundation.outgoing_dependency_count == 1
    assert [move.task_id for move in result.suggested_moves] == [
        "task-free",
        "task-setup",
    ]


def test_analyze_milestone_capacity_returns_deterministic_ordering():
    result = analyze_milestone_capacity(
        _plan(
            tasks=[
                _task("task-z", "Zeta", estimate=1),
                _task("task-b", "Foundation", estimate=1),
                _task("task-a", "Alpha", estimate=1),
                _task("task-c", "Foundation", estimate=1),
                _task("task-d", "Foundation", estimate=1),
            ]
        ),
        max_tasks_per_milestone=2,
    )

    assert [summary.milestone for summary in result.summaries] == [
        "Foundation",
        "Delivery",
        "Alpha",
        "Zeta",
    ]
    assert [finding.to_dict() for finding in result.overload_findings] == [
        {
            "code": "task_count_overload",
            "milestone": "Foundation",
            "metric": "task_count",
            "value": 3.0,
            "threshold": 2.0,
            "task_ids": ["task-b", "task-c", "task-d"],
        }
    ]
    assert [move.to_dict() for move in result.suggested_moves] == [
        {
            "task_id": "task-b",
            "from_milestone": "Foundation",
            "to_milestone": "Delivery",
            "effort": 1.0,
            "reason": "Task has no cross-milestone dependency constraints.",
        },
        {
            "task_id": "task-c",
            "from_milestone": "Foundation",
            "to_milestone": "Delivery",
            "effort": 1.0,
            "reason": "Task has no cross-milestone dependency constraints.",
        },
        {
            "task_id": "task-d",
            "from_milestone": "Foundation",
            "to_milestone": "Delivery",
            "effort": 1.0,
            "reason": "Task has no cross-milestone dependency constraints.",
        },
    ]


def _plan(
    *,
    tasks: list[dict],
    milestones: list[str] | None = None,
) -> dict:
    return {
        "id": "plan-capacity",
        "milestones": [
            {"name": milestone} for milestone in (milestones or ["Foundation", "Delivery"])
        ],
        "tasks": tasks,
    }


def _task(
    task_id: str,
    milestone: str | None,
    **overrides,
) -> dict:
    task = {
        "id": task_id,
        "title": task_id,
        "description": task_id,
        "milestone": milestone,
        "depends_on": [],
        "acceptance_criteria": [f"{task_id} works"],
    }
    task.update(overrides)
    return task
