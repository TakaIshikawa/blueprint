import json

from blueprint.domain.models import ExecutionPlan
from blueprint.milestone_readiness_summary import (
    MilestoneReadinessSummary,
    build_milestone_readiness_summaries,
    milestone_readiness_summaries_to_dict,
)


def test_builds_summaries_in_milestone_order_with_unassigned_bucket():
    summaries = build_milestone_readiness_summaries(
        _plan(
            [
                _task("task-foundation", "Build foundation", milestone="Foundation"),
                _task("task-ui", "Build UI", milestone="Interface"),
                _task("task-floating", "Clean up copy", milestone=None),
                _task("task-unknown", "Handle unknown", milestone="Not declared"),
            ]
        )
    )

    assert [summary.milestone_name for summary in summaries] == [
        "Foundation",
        "Interface",
        "Unassigned",
    ]
    assert summaries[0] == MilestoneReadinessSummary(
        milestone_name="Foundation",
        task_count=1,
        task_ids=("task-foundation",),
        readiness_status="ready",
        acceptance_criteria_count=1,
        tasks_with_acceptance_criteria_count=1,
    )
    assert summaries[2].task_ids == ("task-floating", "task-unknown")
    assert summaries[2].readiness_status == "ready"


def test_sparse_plan_identifies_missing_metadata_and_validation():
    summaries = build_milestone_readiness_summaries(
        _plan(
            [
                {
                    "id": "task-sparse",
                    "title": "Sparse task",
                    "description": "Fill in task planning metadata",
                    "milestone": "Foundation",
                    "owner_type": "",
                    "suggested_engine": "",
                    "depends_on": [],
                    "files_or_modules": [],
                    "acceptance_criteria": [],
                    "estimated_complexity": "low",
                    "status": "pending",
                    "test_command": "",
                    "metadata": {},
                }
            ]
        )
    )

    summary = summaries[0]

    assert summary.readiness_status == "needs_attention"
    assert summary.missing_owner_task_ids == ("task-sparse",)
    assert summary.missing_agent_hint_task_ids == ("task-sparse",)
    assert summary.missing_validation_task_ids == ("task-sparse",)
    assert summary.acceptance_criteria_count == 0
    assert summary.tasks_with_acceptance_criteria_count == 0
    assert summary.tasks_missing_acceptance_criteria_count == 1
    assert summary.missing_acceptance_criteria_task_ids == ("task-sparse",)
    assert summary.blocked_task_ids == ()
    assert summary.unresolved_dependency_ids == ()


def test_blocked_tasks_and_blocked_or_missing_dependencies_block_milestone():
    summaries = build_milestone_readiness_summaries(
        _plan(
            [
                _task(
                    "task-contract",
                    "Finalize contract",
                    milestone="Foundation",
                    status="blocked",
                    blocked_reason="Waiting for schema decision",
                ),
                _task(
                    "task-client",
                    "Build client",
                    milestone="Interface",
                    depends_on=["task-contract", "task-missing"],
                ),
            ]
        )
    )

    foundation, interface = summaries

    assert foundation.readiness_status == "blocked"
    assert foundation.blocked_task_ids == ("task-contract",)
    assert foundation.unresolved_dependency_ids == ()
    assert interface.readiness_status == "blocked"
    assert interface.blocked_task_ids == ()
    assert interface.unresolved_dependency_ids == ("task-contract", "task-missing")


def test_metadata_validation_commands_count_as_coverage_and_human_owner_needs_no_agent_hint():
    summaries = build_milestone_readiness_summaries(
        _plan(
            [
                _task(
                    "task-human",
                    "Review launch copy",
                    milestone="Interface",
                    owner_type="human",
                    suggested_engine="",
                    test_command=None,
                    metadata={
                        "validation_commands": {
                            "lint": ["poetry run ruff check"],
                            "test": ["pytest tests/test_copy.py"],
                        }
                    },
                )
            ]
        )
    )

    summary = summaries[1]

    assert summary.readiness_status == "ready"
    assert summary.missing_agent_hint_task_ids == ()
    assert summary.missing_validation_task_ids == ()
    assert summary.acceptance_criteria_count == 1
    assert summary.tasks_with_acceptance_criteria_count == 1
    assert summary.tasks_missing_acceptance_criteria_count == 0


def test_accepts_execution_plan_model_and_serializes_stable_dicts():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    "Use model input",
                    milestone="Foundation",
                    depends_on=[],
                )
            ]
        )
    )

    first = build_milestone_readiness_summaries(model)
    second = build_milestone_readiness_summaries(model)
    payload = milestone_readiness_summaries_to_dict(first)

    assert payload == milestone_readiness_summaries_to_dict(second)
    assert payload == [
        {
            "milestone_name": "Foundation",
            "task_count": 1,
            "task_ids": ["task-model"],
            "readiness_status": "ready",
            "blocked_task_ids": [],
            "missing_owner_task_ids": [],
            "missing_agent_hint_task_ids": [],
            "missing_validation_task_ids": [],
            "unresolved_dependency_ids": [],
            "acceptance_criteria_count": 1,
            "tasks_with_acceptance_criteria_count": 1,
            "tasks_missing_acceptance_criteria_count": 0,
            "missing_acceptance_criteria_task_ids": [],
        },
        {
            "milestone_name": "Interface",
            "task_count": 0,
            "task_ids": [],
            "readiness_status": "needs_attention",
            "blocked_task_ids": [],
            "missing_owner_task_ids": [],
            "missing_agent_hint_task_ids": [],
            "missing_validation_task_ids": [],
            "unresolved_dependency_ids": [],
            "acceptance_criteria_count": 0,
            "tasks_with_acceptance_criteria_count": 0,
            "tasks_missing_acceptance_criteria_count": 0,
            "missing_acceptance_criteria_task_ids": [],
        },
    ]
    assert list(payload[0]) == [
        "milestone_name",
        "task_count",
        "task_ids",
        "readiness_status",
        "blocked_task_ids",
        "missing_owner_task_ids",
        "missing_agent_hint_task_ids",
        "missing_validation_task_ids",
        "unresolved_dependency_ids",
        "acceptance_criteria_count",
        "tasks_with_acceptance_criteria_count",
        "tasks_missing_acceptance_criteria_count",
        "missing_acceptance_criteria_task_ids",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks):
    return {
        "id": "plan-milestone-readiness",
        "implementation_brief_id": "brief-milestone-readiness",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [
            {"name": "Foundation", "description": "Build the base"},
            {"name": "Interface", "description": "Expose the flow"},
        ],
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
    milestone,
    owner_type="agent",
    suggested_engine="codex",
    depends_on=None,
    acceptance_criteria=None,
    status="pending",
    test_command="pytest tests/test_milestone_readiness_summary.py",
    metadata=None,
    blocked_reason=None,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-milestone-readiness",
        "title": title,
        "description": f"Implement {title}",
        "milestone": milestone,
        "owner_type": owner_type,
        "suggested_engine": suggested_engine,
        "depends_on": depends_on or [],
        "files_or_modules": ["src/example.py"],
        "acceptance_criteria": acceptance_criteria or [f"{title} is observable"],
        "estimated_complexity": "medium",
        "risk_level": "low",
        "test_command": test_command,
        "status": status,
        "metadata": metadata or {},
        "blocked_reason": blocked_reason,
    }
