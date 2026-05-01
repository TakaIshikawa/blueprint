import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_launch_window_risk import (
    LaunchWindowBlocker,
    LaunchWindowPhase,
    LaunchWindowTaskRisk,
    PlanLaunchWindowRiskMap,
    build_plan_launch_window_risk_map,
    derive_plan_launch_window_risk_map,
    plan_launch_window_risk_map_to_dict,
    plan_launch_window_risk_map_to_markdown,
)


def test_dependency_and_release_metadata_order_launch_phases_deterministically():
    result = build_plan_launch_window_risk_map(
        _plan(
            [
                _task(
                    "task-release",
                    milestone="Launch",
                    depends_on=["task-api"],
                    metadata={"release_window": "Window 2"},
                    test_command="make smoke",
                ),
                _task(
                    "task-foundation",
                    milestone="Foundation",
                    metadata={"release_window": "Window 1"},
                    test_command="make unit",
                ),
                _task(
                    "task-api",
                    milestone="Foundation",
                    depends_on=["task-foundation"],
                    metadata={"release_window": "Window 1"},
                    test_command="make api-test",
                ),
            ],
            milestones=[
                {"name": "Foundation", "release_window": "Window 1"},
                {"name": "Launch", "release_window": "Window 2"},
            ],
        )
    )

    assert [phase.label for phase in result.phases] == ["Window 1", "Window 2"]
    assert [phase.task_ids for phase in result.phases] == [
        ("task-foundation", "task-api"),
        ("task-release",),
    ]
    assert result.phases[0].release_windows == ("Window 1",)
    assert result.phases[1].blockers == ("task-release: waits for task-api (pending)",)
    assert result.high_risk_task_ids == ()


def test_high_risk_detection_combines_production_migration_external_and_weak_validation_signals():
    result = build_plan_launch_window_risk_map(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update docs",
                    files_or_modules=["docs/launch.md"],
                    risk_level="low",
                    test_command="make docs-test",
                ),
                _task(
                    "task-migration",
                    title="Deploy production customer schema migration",
                    description="Run SQL backfill against production customer traffic.",
                    files_or_modules=["migrations/20260501_customer.sql"],
                    risk_level="medium",
                ),
                _task(
                    "task-integration",
                    title="Release external Stripe webhook integration",
                    description="Launch production webhook flow for Stripe partner events.",
                    risk_level="high",
                    metadata={"validation_commands": ["manual"]},
                ),
            ]
        )
    )

    assert result.high_risk_task_ids == ("task-migration", "task-integration")
    assert _risk(result, "task-migration").signals == (
        "production",
        "migration",
        "weak-validation",
    )
    assert _risk(result, "task-integration").signals == (
        "explicit-high-risk",
        "production",
        "external-dependency",
        "weak-validation",
    )
    assert [blocker.code for blocker in result.suggested_launch_blockers] == [
        "high_risk_weak_validation",
        "production_migration_without_rollback",
        "high_risk_weak_validation",
    ]


def test_missing_metadata_and_unknown_dependencies_fall_back_to_dependency_phases():
    result = build_plan_launch_window_risk_map(
        _plan(
            [
                _task("task-first", milestone=None, test_command="make test"),
                _task(
                    "task-second",
                    milestone=None,
                    depends_on=["task-first", "task-missing"],
                    status="blocked",
                    blocked_reason="Waiting on vendor approval.",
                ),
            ],
            milestones=[],
        )
    )

    assert [phase.to_dict() for phase in result.phases] == [
        {
            "phase_id": "000-dependency-phase-1",
            "label": "Dependency Phase 1",
            "order": 0,
            "source": "dependency",
            "task_ids": ["task-first"],
            "milestones": [],
            "release_windows": [],
            "estimated_hours": 8.0,
            "high_risk_task_ids": [],
            "blockers": [],
            "coordination_notes": ["task-first: owner agent, engine codex."],
        },
        {
            "phase_id": "001-dependency-phase-2",
            "label": "Dependency Phase 2",
            "order": 1,
            "source": "dependency",
            "task_ids": ["task-second"],
            "milestones": [],
            "release_windows": [],
            "estimated_hours": 8.0,
            "high_risk_task_ids": ["task-second"],
            "blockers": [
                "task-second: task status is blocked",
                "task-second: Waiting on vendor approval.",
                "task-second: high-risk launch task lacks strong validation",
                "task-second: waits for task-first (pending)",
                "task-second: missing prerequisite task-missing",
            ],
            "coordination_notes": [
                "task-second: confirm external owner availability during the window.",
                "task-second: owner agent, engine codex.",
            ],
        },
    ]
    assert [blocker.code for blocker in result.suggested_launch_blockers] == [
        "unknown_dependency",
        "high_risk_weak_validation",
        "blocked_task_in_launch_window",
    ]
    assert result.coordination_notes[0] == (
        "Some tasks lack milestone or release metadata; dependency depth was used for phase order."
    )


def test_serialization_markdown_aliases_and_model_input_are_stable_without_mutation():
    plan = _plan(
        [
            _task(
                "task-setup",
                milestone="Release Alpha",
                metadata={"launch_window": "Alpha"},
                test_command="make setup-test",
                estimated_hours=2,
            ),
            _task(
                "task-prod",
                title="Launch production API",
                description="Deploy production API with rollback runbook.",
                milestone="Release Beta",
                depends_on=["task-setup"],
                metadata={"launch_window": "Beta", "validation_gates": ["make smoke"]},
                estimated_hours=3.5,
            ),
        ],
        plan_id="plan-launch",
        milestones=[
            {"name": "Release Alpha", "launch_window": "Alpha"},
            {"name": "Release Beta", "launch_window": "Beta"},
        ],
    )
    original = copy.deepcopy(plan)

    result = build_plan_launch_window_risk_map(ExecutionPlan.model_validate(plan))
    alias_result = derive_plan_launch_window_risk_map(plan)
    payload = plan_launch_window_risk_map_to_dict(result)

    assert plan == original
    assert isinstance(result, PlanLaunchWindowRiskMap)
    assert isinstance(LaunchWindowPhase, type)
    assert isinstance(LaunchWindowTaskRisk, type)
    assert isinstance(LaunchWindowBlocker, type)
    assert payload == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert alias_result.to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "phases",
        "task_risks",
        "high_risk_task_ids",
        "suggested_launch_blockers",
        "coordination_notes",
    ]
    assert list(payload["phases"][0]) == [
        "phase_id",
        "label",
        "order",
        "source",
        "task_ids",
        "milestones",
        "release_windows",
        "estimated_hours",
        "high_risk_task_ids",
        "blockers",
        "coordination_notes",
    ]
    assert plan_launch_window_risk_map_to_markdown(result) == "\n".join(
        [
            "# Plan Launch Window Risk Map: plan-launch",
            "",
            "## Phase Summary",
            "",
            "| Phase | Source | Tasks | High Risk | Estimated Hours | Blockers | Coordination Notes |",
            "| --- | --- | --- | --- | --- | --- | --- |",
            "| Alpha | metadata | task-setup | none | 2 | none | task-setup: owner agent, engine codex. |",
            (
                "| Beta | metadata | task-prod | none | 3.5 | "
                "task-prod: waits for task-setup (pending) | "
                "task-prod: owner agent, engine codex. |"
            ),
            "",
            "## High-Risk Tasks",
            "",
            "No high-risk launch tasks detected.",
            "",
            "## Suggested Launch Blockers",
            "",
            "No launch blockers suggested.",
            "",
            "## Coordination Notes",
            "",
            "- task-setup: owner agent, engine codex.",
            "- task-prod: owner agent, engine codex.",
        ]
    )
    assert plan_launch_window_risk_map_to_markdown(result) == result.to_markdown()


def test_empty_plan_behavior_is_deterministic():
    result = build_plan_launch_window_risk_map({"id": "plan-empty", "tasks": []})

    assert result.to_dict() == {
        "plan_id": "plan-empty",
        "phases": [],
        "task_risks": [],
        "high_risk_task_ids": [],
        "suggested_launch_blockers": [],
        "coordination_notes": [],
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Launch Window Risk Map: plan-empty",
            "",
            "No launch phases were derived.",
            "",
            "## Suggested Launch Blockers",
            "",
            "No launch blockers suggested.",
            "",
            "## Coordination Notes",
            "",
            "No coordination notes.",
        ]
    )


def _risk(result, task_id):
    return next(risk for risk in result.task_risks if risk.task_id == task_id)


def _plan(tasks, *, plan_id="plan-launch", milestones=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-launch",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": (
            milestones if milestones is not None else [{"name": "Foundation"}, {"name": "Launch"}]
        ),
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    milestone="Foundation",
    owner_type="agent",
    suggested_engine="codex",
    depends_on=None,
    files_or_modules=None,
    estimated_complexity="medium",
    estimated_hours=None,
    risk_level="medium",
    test_command=None,
    validation_command=None,
    status="pending",
    blocked_reason=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or f"Task {task_id}",
        "description": description or f"Implement {task_id}.",
        "milestone": milestone,
        "owner_type": owner_type,
        "suggested_engine": suggested_engine,
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules or ["src/app.py"],
        "acceptance_criteria": [f"{task_id} works"],
        "estimated_complexity": estimated_complexity,
        "estimated_hours": estimated_hours,
        "risk_level": risk_level,
        "test_command": test_command,
        "status": status,
        "metadata": metadata or {},
        "blocked_reason": blocked_reason,
    }
    if validation_command is not None:
        task["validation_command"] = validation_command
    return task
