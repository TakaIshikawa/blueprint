import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_blast_radius import (
    PlanBlastRadius,
    TaskBlastRadius,
    estimate_plan_blast_radius,
    plan_blast_radius_to_dict,
)


def test_low_risk_isolated_tasks_have_low_blast_radius():
    summary = estimate_plan_blast_radius(
        _plan(
            tasks=[
                _task(
                    "task-widget",
                    "Build widget",
                    files_or_modules=["src/features/widget.py"],
                    risk_level="low",
                    test_command="pytest tests/test_widget.py",
                )
            ]
        )
    )

    assert isinstance(summary, PlanBlastRadius)
    assert isinstance(summary.task_summaries[0], TaskBlastRadius)
    assert summary.aggregate_score == 1
    assert summary.severity == "low"
    assert summary.reasons == ()
    assert summary.affected_task_ids == ()
    assert summary.severity_counts == {"low": 1, "medium": 0, "high": 0, "critical": 0}
    assert summary.task_summaries[0] == TaskBlastRadius(
        task_id="task-widget",
        title="Build widget",
        score=1,
        severity="low",
        reasons=(),
        affected_task_ids=(),
        file_paths=("src/features/widget.py",),
    )


def test_config_schema_and_database_paths_escalate_task_and_plan_severity():
    summary = estimate_plan_blast_radius(
        _plan(
            tasks=[
                _task(
                    "task-infra",
                    "Update shared storage",
                    files_or_modules=[
                        " config/app.yml ",
                        "src/schema/user.graphql",
                        "migrations/202604300101_add_users.sql",
                    ],
                    test_command="poetry run pytest",
                )
            ]
        )
    )

    task = summary.task_summaries[0]

    assert task.score == 14
    assert task.severity == "critical"
    assert task.reasons == (
        "touches 3 file paths",
        "config path: config/app.yml",
        "schema path: src/schema/user.graphql",
        "database path: migrations/202604300101_add_users.sql",
        "broad validation command",
    )
    assert summary.aggregate_score == 15
    assert summary.severity == "critical"
    assert summary.reasons == ("1 task(s) have high or critical blast radius",)
    assert summary.affected_task_ids == ("task-infra",)


def test_dependency_fan_in_fan_out_and_cross_milestone_reach_affect_score():
    summary = estimate_plan_blast_radius(
        _plan(
            milestones=[
                {"name": "Foundation"},
                {"name": "Delivery"},
            ],
            tasks=[
                _task(
                    "task-core",
                    "Build core API",
                    milestone="Foundation",
                    files_or_modules=["src/core/api.py"],
                ),
                _task(
                    "task-ui",
                    "Build UI",
                    milestone="Delivery",
                    depends_on=["task-core"],
                    files_or_modules=["src/ui/panel.tsx"],
                ),
                _task(
                    "task-worker",
                    "Build worker",
                    milestone="Delivery",
                    depends_on=["task-core"],
                    files_or_modules=["src/workers/sync.py"],
                ),
            ],
        )
    )

    assert [(task.task_id, task.score, task.severity) for task in summary.task_summaries] == [
        ("task-core", 9, "high"),
        ("task-ui", 3, "low"),
        ("task-worker", 3, "low"),
    ]
    assert summary.task_summaries[0].reasons == (
        "shared infrastructure path: src/core/api.py",
        "dependency fan-in from 2 task(s)",
        "cross-milestone dependency reach to 2 task(s)",
    )
    assert summary.task_summaries[0].affected_task_ids == ("task-ui", "task-worker")
    assert summary.task_summaries[1].reasons == (
        "dependency fan-out to 1 task(s)",
        "cross-milestone dependency reach to 1 task(s)",
    )
    assert summary.aggregate_score == 9
    assert summary.severity == "high"
    assert summary.reasons == ("1 task(s) have high or critical blast radius",)


def test_accepts_execution_plan_models_and_serializes_stably():
    model = ExecutionPlan.model_validate(
        _plan(
            tasks=[
                _task(
                    "task-cli-export",
                    "Wire exporter CLI",
                    files_or_modules=["src/blueprint/cli.py"],
                    risk_level="high",
                    metadata={
                        "blast_radius": "medium",
                        "expected_files": ["src/blueprint/exporters/registry.py"],
                    },
                    test_command="pytest",
                )
            ]
        )
    )

    first = estimate_plan_blast_radius(model)
    second = estimate_plan_blast_radius(model)
    payload = plan_blast_radius_to_dict(first)

    assert payload == plan_blast_radius_to_dict(second)
    assert payload == first.to_dict()
    assert (
        payload
        == [
            {
                "plan_id": "plan-blast-radius",
                "aggregate_score": 12,
                "severity": "critical",
                "reasons": ["1 task(s) have high or critical blast radius"],
                "affected_task_ids": ["task-cli-export"],
                "task_summaries": [
                    {
                        "task_id": "task-cli-export",
                        "title": "Wire exporter CLI",
                        "score": 11,
                        "severity": "high",
                        "reasons": [
                            "CLI path: src/blueprint/cli.py",
                            "exporter registry path: src/blueprint/exporters/registry.py",
                            "high risk task metadata",
                            "medium blast radius metadata",
                            "broad validation command",
                        ],
                        "affected_task_ids": [],
                        "file_paths": [
                            "src/blueprint/cli.py",
                            "src/blueprint/exporters/registry.py",
                        ],
                    }
                ],
                "severity_counts": {"low": 0, "medium": 0, "high": 1, "critical": 0},
            }
        ][0]
    )
    assert list(payload) == [
        "plan_id",
        "aggregate_score",
        "severity",
        "reasons",
        "affected_task_ids",
        "task_summaries",
        "severity_counts",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(*, tasks, milestones=None):
    return {
        "id": "plan-blast-radius",
        "implementation_brief_id": "brief-blast-radius",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": milestones if milestones is not None else [{"name": "Implementation"}],
        "test_strategy": "Run focused validation",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "metadata": {},
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    files_or_modules,
    milestone="Implementation",
    depends_on=None,
    risk_level="low",
    expectedFiles=None,
    metadata=None,
    test_command="pytest tests/test_plan_blast_radius.py",
):
    task = {
        "id": task_id,
        "execution_plan_id": "plan-blast-radius",
        "title": title,
        "description": f"Implement {title}",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": ["Behavior is covered"],
        "estimated_complexity": "medium",
        "risk_level": risk_level,
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
    }
    if expectedFiles is not None:
        task["expectedFiles"] = expectedFiles
    return task
