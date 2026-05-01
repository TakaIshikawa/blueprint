import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_release_train_alignment import (
    PlanReleaseTrainAlignment,
    PlanReleaseTrainFinding,
    build_plan_release_train_alignment,
    derive_plan_release_train_alignment,
    plan_release_train_alignment_to_dict,
    plan_release_train_alignment_to_markdown,
)


def test_tasks_are_grouped_deterministically_from_metadata_and_milestones():
    result = build_plan_release_train_alignment(
        _plan(
            [
                _task(
                    "task-schema",
                    milestone="Train Alpha",
                    metadata={"release_train": "Train Alpha"},
                    test_command="poetry run pytest tests/test_schema.py",
                ),
                _task(
                    "task-api",
                    milestone="Train Beta",
                    metadata={"release_window": "Train Beta"},
                ),
                _task(
                    "task-docs",
                    milestone="Release Candidate",
                ),
            ],
            milestones=[
                {"name": "Train Alpha"},
                {"name": "Train Beta"},
                {"name": "Release Candidate"},
            ],
        )
    )

    assert [train.train_id for train in result.trains] == [
        "train-alpha",
        "train-beta",
        "release-candidate",
    ]
    assert [train.task_ids for train in result.trains] == [
        ("task-schema",),
        ("task-api",),
        ("task-docs",),
    ]
    assert result.task_train_map == {
        "task-schema": "train-alpha",
        "task-api": "train-beta",
        "task-docs": "release-candidate",
    }
    assert result.trains[0].validation_commands == ("poetry run pytest tests/test_schema.py",)
    assert result.findings == ()


def test_later_train_dependencies_on_earlier_prerequisites_are_accepted():
    result = build_plan_release_train_alignment(
        _plan(
            [
                _task("task-foundation", metadata={"release_window": "Train 1"}),
                _task(
                    "task-rollout",
                    depends_on=["task-foundation"],
                    metadata={"release_window": "Train 2"},
                ),
            ],
            milestones=[
                {"name": "Foundation", "release_window": "Train 1"},
                {"name": "Rollout", "release_window": "Train 2"},
            ],
        )
    )

    assert [finding.code for finding in result.findings] == []


def test_prerequisites_scheduled_after_dependents_are_flagged():
    result = build_plan_release_train_alignment(
        _plan(
            [
                _task(
                    "task-api",
                    depends_on=["task-schema"],
                    metadata={"release_window": "Train 1"},
                ),
                _task("task-schema", metadata={"release_window": "Train 3"}),
            ],
            milestones=[
                {"name": "Train 1"},
                {"name": "Train 2"},
                {"name": "Train 3"},
            ],
        )
    )

    assert [finding.to_dict() for finding in result.findings] == [
        {
            "code": "dependency_release_order_violation",
            "severity": "error",
            "reason": (
                "Task task-api is scheduled in Train 1 before prerequisite "
                "task-schema in Train 3."
            ),
            "suggested_remediation": (
                "Move the prerequisite to the same or an earlier release train, "
                "or move the dependent task later."
            ),
            "task_ids": ["task-api", "task-schema"],
            "train_ids": ["train-1", "train-3"],
        }
    ]


def test_missing_conflicting_and_risky_release_windows_produce_findings():
    result = build_plan_release_train_alignment(
        _plan(
            [
                _task("task-copy", milestone="Build"),
                _task(
                    "task-conflict",
                    milestone="Train Beta",
                    metadata={
                        "release_train": "Train Alpha",
                        "release_window": "Train Beta",
                    },
                ),
                _task(
                    "task-migration",
                    title="Run customer schema migration",
                    milestone="Train Final",
                    files_or_modules=["migrations/20260501_customer.sql"],
                    risk_level="high",
                    test_command="poetry run pytest tests/test_migrations.py",
                ),
            ],
            milestones=[
                {"name": "Train Alpha"},
                {"name": "Train Beta"},
                {"name": "Train Final"},
            ],
        )
    )

    assert [finding.code for finding in result.findings] == [
        "missing_release_window",
        "explicit_release_window_required",
        "conflicting_release_assignment",
        "high_risk_task_scheduled_late",
    ]
    assert result.findings[0].task_ids == ("task-copy",)
    assert result.findings[1].task_ids == ("task-migration",)
    assert result.findings[2].severity == "error"
    assert result.findings[3].train_ids == ("train-final",)
    assert _train(result, "train-final").risk_notes == (
        "task-migration: high risk; task-migration: migration-related",
    )


def test_serialization_markdown_aliases_and_model_input_are_stable_without_mutation():
    plan = _plan(
        [
            _task("task-setup", metadata={"release_window": "Train 1"}),
            _task(
                "task-release",
                depends_on=["task-setup"],
                metadata={"release_window": "Train 2"},
                test_command="make smoke",
            ),
        ],
        plan_id="plan-release",
        milestones=[
            {"name": "Train 1"},
            {"name": "Train 2"},
        ],
    )
    original = copy.deepcopy(plan)

    result = build_plan_release_train_alignment(ExecutionPlan.model_validate(plan))
    alias_result = derive_plan_release_train_alignment(plan)
    payload = plan_release_train_alignment_to_dict(result)

    assert plan == original
    assert isinstance(result, PlanReleaseTrainAlignment)
    assert isinstance(PlanReleaseTrainFinding, type)
    assert payload == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert alias_result.task_train_map == result.task_train_map
    assert list(payload) == ["plan_id", "trains", "findings", "task_train_map"]
    assert list(payload["trains"][0]) == [
        "train_id",
        "label",
        "order",
        "source",
        "task_ids",
        "milestones",
        "validation_commands",
        "risk_notes",
    ]
    assert plan_release_train_alignment_to_markdown(result) == "\n".join(
        [
            "# Plan Release Train Alignment: plan-release",
            "",
            "## Release Trains",
            "",
            "| Train | Source | Tasks | Milestones | Validation | Risk Notes |",
            "| --- | --- | --- | --- | --- | --- |",
            "| Train 1 | metadata | task-setup | none | none | none |",
            "| Train 2 | metadata | task-release | none | make smoke | none |",
            "",
            "## Issues",
            "",
            "No issues found.",
        ]
    )


def test_empty_and_iterable_inputs_are_supported():
    empty = build_plan_release_train_alignment({"id": "plan-empty", "tasks": []})
    iterable = build_plan_release_train_alignment(
        [
            _task("task-later", metadata={"release_window": "Train B"}),
            _task("task-first", metadata={"release_window": "Train A"}),
        ]
    )

    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Release Train Alignment: plan-empty",
            "",
            "No release trains were derived.",
            "",
            "## Issues",
            "",
            "No issues found.",
        ]
    )
    assert iterable.plan_id is None
    assert [train.train_id for train in iterable.trains] == ["train-b", "train-a"]


def _plan(tasks, *, plan_id="plan-train", milestones=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-train",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": milestones or [],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _train(result, train_id):
    return next(train for train in result.trains if train.train_id == train_id)


def _task(
    task_id,
    *,
    title=None,
    description=None,
    milestone=None,
    depends_on=None,
    files_or_modules=None,
    risk_level="medium",
    test_command=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title or f"Task {task_id}",
        "description": description or f"Implement {task_id}.",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules or ["src/app.py"],
        "acceptance_criteria": [f"{task_id} works"],
        "estimated_complexity": "medium",
        "risk_level": risk_level,
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
