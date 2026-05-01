import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_deployment_freeze_conflicts import (
    PlanDeploymentFreezeConflictRecord,
    PlanDeploymentFreezeConflictReport,
    analyze_plan_deployment_freeze_conflicts,
    build_plan_deployment_freeze_conflict_report,
    plan_deployment_freeze_conflict_report_to_dict,
    plan_deployment_freeze_conflict_report_to_markdown,
    summarize_plan_deployment_freeze_conflicts,
)


def test_metadata_freeze_blackout_business_hours_and_change_gates_create_conflicts():
    result = build_plan_deployment_freeze_conflict_report(
        _plan(
            [
                _task(
                    "task-prod-release",
                    title="Deploy production holiday release",
                    description="Release the production checkout service during the launch window.",
                    acceptance_criteria=[
                        "Production deploy is queued after release owner sign-off."
                    ],
                ),
                _task(
                    "task-docs",
                    title="Update runbook copy",
                    description="Clarify support handoff text.",
                ),
            ],
            metadata={
                "freeze_windows": [
                    {
                        "name": "Holiday production freeze",
                        "start": "2026-12-20",
                        "end": "2027-01-03",
                        "environment": "production",
                        "required_approvals": ["VP Engineering"],
                    }
                ],
                "blackout_dates": ["2026-11-27 retail blackout"],
                "business_hours": "Deployments only Tuesday-Thursday 10:00-15:00 PT",
                "change_management_gates": [{"name": "CAB approval required"}],
                "release_calendar": ["Launch date 2026-12-22"],
            },
        )
    )

    assert isinstance(result, PlanDeploymentFreezeConflictReport)
    assert result.plan_id == "plan-freeze"
    assert result.conflicted_task_ids == ("task-prod-release",)
    assert result.no_conflict_task_ids == ("task-docs",)
    assert result.summary["conflict_count"] == 5
    assert result.summary["severity_counts"] == {
        "hard_conflict": 2,
        "warning": 2,
        "informational": 1,
    }
    assert result.summary["constraint_counts"]["freeze_window"] == 1
    assert result.summary["constraint_counts"]["blackout"] == 1

    first = result.records[0]
    assert isinstance(first, PlanDeploymentFreezeConflictRecord)
    assert first.task_id == "task-prod-release"
    assert first.severity == "hard_conflict"
    assert first.constraint_type == "freeze_window"
    assert first.window_or_constraint == (
        "Holiday production freeze - 2026-12-20 to 2027-01-03 - production"
    )
    assert "VP Engineering" in first.required_approvals
    assert "Release manager approval" in first.required_approvals
    assert any("metadata.freeze_windows[0]" in item for item in first.evidence)
    assert any("title: Deploy production holiday release" == item for item in first.evidence)


def test_inferred_task_conflicts_distinguish_hard_warning_and_informational():
    result = analyze_plan_deployment_freeze_conflicts(
        _plan(
            [
                _task(
                    "task-hard",
                    title="Deploy production during blackout",
                    description="Production cutover must not run in the holiday freeze blackout.",
                ),
                _task(
                    "task-warning",
                    title="Run migration after-hours",
                    description="Deploy database migration outside business hours after CAB approval.",
                ),
                _task(
                    "task-info",
                    title="Release on launch date",
                    description="Ship the release on the holiday launch date for marketing.",
                ),
            ],
            metadata={},
        )
    )

    assert [(record.task_id, record.severity) for record in result.records] == [
        ("task-hard", "hard_conflict"),
        ("task-warning", "warning"),
        ("task-info", "informational"),
    ]
    by_id = {record.task_id: record for record in result.records}
    assert by_id["task-hard"].window_or_constraint == "Inferred freeze or blackout timing from task text"
    assert by_id["task-warning"].required_approvals == ("Change manager approval",)
    assert by_id["task-info"].required_approvals == ()
    assert result.summary["constraint_counts"]["inferred_timing"] == 3


def test_no_conflict_empty_and_invalid_inputs_are_stable():
    no_conflict = build_plan_deployment_freeze_conflict_report(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="Adjust labels and helper text.",
                    files_or_modules=["src/blueprint/ui/copy.py"],
                )
            ],
            metadata={"release_calendar": ["Launch date 2026-06-01"]},
        )
    )
    empty = build_plan_deployment_freeze_conflict_report({"id": "empty-plan", "tasks": []})
    invalid = build_plan_deployment_freeze_conflict_report(19)

    assert no_conflict.records == ()
    assert no_conflict.conflicted_task_ids == ()
    assert no_conflict.no_conflict_task_ids == ("task-copy",)
    assert no_conflict.summary["conflict_count"] == 0
    assert "Summary: 0 conflicts across 0 tasks" in no_conflict.to_markdown()
    assert "No-conflict tasks: task-copy" in no_conflict.to_markdown()
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert empty.summary["task_count"] == 0
    assert "No deployment freeze conflicts were detected." in empty.to_markdown()
    assert invalid.plan_id is None
    assert invalid.records == ()
    assert invalid.summary["task_count"] == 0


def test_serialization_markdown_alias_model_object_and_deterministic_ordering():
    plan = _plan(
        [
            _task(
                "task-z-info",
                title="Release customer launch date | email",
                description="Ship launch date messaging on the holiday calendar.",
                metadata={"notes": [{"date": "Launch date 2026-12-22"}, None, 7]},
            ),
            _task(
                "task-a-hard",
                title="Production deploy",
                description="Deploy production API in the freeze.",
            ),
        ],
        metadata={
            "freeze_windows": [{"name": "Year-end freeze", "start": "2026-12-24"}],
            "release_calendar": {"launch": "Launch date 2026-12-22"},
        },
    )
    original = copy.deepcopy(plan)

    result = build_plan_deployment_freeze_conflict_report(ExecutionPlan.model_validate(plan))
    alias = summarize_plan_deployment_freeze_conflicts(plan)
    payload = plan_deployment_freeze_conflict_report_to_dict(result)
    markdown = plan_deployment_freeze_conflict_report_to_markdown(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert alias.to_dict() == result.to_dict()
    assert [record.task_id for record in result.records] == [
        "task-a-hard",
        "task-z-info",
        "task-a-hard",
        "task-z-info",
    ]
    assert [record.severity for record in result.records] == [
        "hard_conflict",
        "hard_conflict",
        "informational",
        "informational",
    ]
    assert list(payload) == [
        "plan_id",
        "records",
        "conflicted_task_ids",
        "no_conflict_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "window_or_constraint",
        "severity",
        "recommended_scheduling_action",
        "required_approvals",
        "evidence",
        "constraint_type",
    ]
    assert len(result.records[0].evidence) == len(set(result.records[0].evidence))
    assert markdown.startswith("# Plan Deployment Freeze Conflict Report: plan-freeze")
    assert "Summary: 4 conflicts across 2 tasks" in markdown
    assert "Release customer launch date \\| email" in markdown


def test_execution_task_and_object_like_task_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Deploy production after-hours",
        description="Production deployment outside business hours requires change approval.",
        files_or_modules=["ops/deploy.yaml"],
        acceptance_criteria=["Release owner confirms timing."],
        metadata={"change_gate": "CAB approval"},
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Launch weekend release",
            description="Release the production app on a weekend launch date.",
        )
    )

    first = build_plan_deployment_freeze_conflict_report([object_task])
    second = build_plan_deployment_freeze_conflict_report(task_model)

    assert first.records[0].task_id == "task-object"
    assert first.records[0].severity == "warning"
    assert first.records[0].required_approvals == ("Change manager approval",)
    assert second.records[0].task_id == "task-model"
    assert second.records[0].severity == "informational"


def _plan(tasks, *, plan_id="plan-freeze", metadata=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-freeze",
        "milestones": [],
        "metadata": {} if metadata is None else metadata,
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
