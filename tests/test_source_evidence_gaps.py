from copy import deepcopy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.source_evidence_gaps import (
    SourceEvidenceGap,
    analyze_source_evidence_gaps,
    source_evidence_gap_analysis_to_dict,
    source_evidence_gap_analysis_to_markdown,
    source_evidence_gaps_to_dicts,
)


def test_source_evidence_gaps_ignores_tasks_with_complete_task_level_evidence():
    plan = _plan(
        [
            _task(
                "task-complete",
                source_ids=["brief-1"],
                source_links={"brief": "https://example.test/brief/1"},
                requirement_ids=["req-acceptance", "req-files", "req-risk"],
                metadata={
                    "source_evidence": {
                        "metadata": {"source_id": "brief-1", "text": "Metadata is from brief."}
                    }
                },
            )
        ]
    )

    result = analyze_source_evidence_gaps(plan)

    assert result.plan_id == "plan-test"
    assert result.task_count == 1
    assert result.gaps == ()
    assert result.to_markdown() == (
        "# Source Evidence Gaps: plan-test\n\nNo source evidence gaps detected."
    )


def test_source_evidence_gaps_reports_partial_field_evidence_deterministically():
    plan = _plan(
        [
            _task(
                "task-z",
                metadata={
                    "source_evidence": {
                        "acceptance_criteria": ["req-acceptance"],
                        "metadata": ["req-metadata"],
                    }
                },
            ),
            _task(
                "task-a",
                metadata={
                    "traceability": {
                        "files_or_modules": {"source_ids": ["brief-files"]},
                        "risks": [{"source_id": "brief-risk", "field": "risk_level"}],
                        "metadata": {"requirement_ids": ["req-metadata"]},
                    }
                },
            ),
        ]
    )

    result = analyze_source_evidence_gaps(plan)

    assert [gap.to_dict() for gap in result.gaps] == [
        {
            "task_id": "task-a",
            "title": "Task task-a",
            "gap_type": "acceptance_criteria",
            "field_path": "acceptance_criteria",
            "message": "Acceptance criteria are not backed by traceable source evidence.",
            "checked_evidence_fields": list(result.gaps[0].checked_evidence_fields),
            "observed_values": ["Verify task-a returns expected output."],
        },
        {
            "task_id": "task-z",
            "title": "Task task-z",
            "gap_type": "files_or_modules",
            "field_path": "files_or_modules",
            "message": "Files or modules are not backed by traceable source evidence.",
            "checked_evidence_fields": list(result.gaps[1].checked_evidence_fields),
            "observed_values": ["src/task-z.py"],
        },
        {
            "task_id": "task-z",
            "title": "Task task-z",
            "gap_type": "risks",
            "field_path": "risk_level/metadata.risks",
            "message": "Risk details are not backed by traceable source evidence.",
            "checked_evidence_fields": list(result.gaps[2].checked_evidence_fields),
            "observed_values": ["medium"],
        },
    ]
    assert result.task_ids_with_gaps == ("task-a", "task-z")


def test_source_evidence_gaps_reports_all_gap_types_when_task_has_no_evidence():
    result = analyze_source_evidence_gaps(_plan([_task("task-empty", metadata={"owner": "platform"})]))

    assert [gap.gap_type for gap in result.gaps] == [
        "acceptance_criteria",
        "files_or_modules",
        "risks",
        "metadata_traceability",
    ]
    assert isinstance(result.gaps[0], SourceEvidenceGap)
    assert result.gaps[3].observed_values == ("owner",)
    assert source_evidence_gaps_to_dicts(result) == result.to_dicts()
    assert json.loads(json.dumps(result.to_dict())) == result.to_dict()


def test_source_evidence_gaps_accepts_models_task_collections_and_does_not_mutate_inputs():
    plan = _plan(
        [
            _task(
                "task-model",
                metadata={
                    "source_evidence": {
                        "acceptance_criteria": ["req-1"],
                        "files_or_modules": ["req-2"],
                    }
                },
            )
        ]
    )
    original = deepcopy(plan)

    model_result = analyze_source_evidence_gaps(ExecutionPlan.model_validate(plan))
    task_result = analyze_source_evidence_gaps(
        [
            ExecutionTask.model_validate({**plan["tasks"][0], "execution_plan_id": "plan-test"}),
            _task("task-plain", evidence=[{"source_id": "brief-1", "text": "Whole task evidence"}]),
        ]
    )

    assert plan == original
    assert [gap.gap_type for gap in model_result.gaps] == ["risks", "metadata_traceability"]
    assert task_result.plan_id is None
    assert task_result.task_count == 2
    assert [gap.task_id for gap in task_result.gaps] == ["task-model", "task-model"]


def test_source_evidence_gap_markdown_and_wrapper_are_stable():
    result = analyze_source_evidence_gaps(
        _plan(
            [
                _task(
                    "task-api",
                    metadata={"source_evidence": {"acceptance_criteria": ["req-acceptance"]}},
                )
            ]
        )
    )

    markdown = source_evidence_gap_analysis_to_markdown(result)

    assert source_evidence_gap_analysis_to_dict(result) == result.to_dict()
    assert markdown == result.to_markdown()
    assert markdown == "\n".join(
        [
            "# Source Evidence Gaps: plan-test",
            "",
            "| Task | Gap Type | Field | Observed Values | Message |",
            "| --- | --- | --- | --- | --- |",
            "| task-api | files_or_modules | files_or_modules | src/task-api.py | Files or modules are not backed by traceable source evidence. |",
            "| task-api | risks | risk_level/metadata.risks | medium | Risk details are not backed by traceable source evidence. |",
            "| task-api | metadata_traceability | metadata.source_evidence/metadata.traceability | None | Task metadata is not backed by source evidence or traceability metadata. |",
        ]
    )


def test_source_evidence_gaps_handles_empty_plans():
    result = analyze_source_evidence_gaps(_plan([]))

    assert result.to_dict() == {
        "plan_id": "plan-test",
        "task_count": 0,
        "gap_count": 0,
        "task_ids_with_gaps": [],
        "gaps": [],
    }
    assert result.to_dicts() == []


def _plan(tasks, *, plan_id="plan-test"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Build", "description": "Build the feature"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    source_ids=None,
    source_links=None,
    evidence=None,
    requirement_ids=None,
    files=None,
    acceptance_criteria=None,
    risk_level="medium",
    metadata=None,
):
    task = {
        "id": task_id,
        "title": f"Task {task_id}",
        "description": f"Implement {task_id}",
        "milestone": "Build",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files or [f"src/{task_id}.py"],
        "acceptance_criteria": acceptance_criteria
        or [f"Verify {task_id} returns expected output."],
        "estimated_complexity": "medium",
        "risk_level": risk_level,
        "test_command": "pytest",
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
    if source_ids is not None:
        task["source_ids"] = source_ids
    if source_links is not None:
        task["source_links"] = source_links
    if evidence is not None:
        task["evidence"] = evidence
    if requirement_ids is not None:
        task["requirement_ids"] = requirement_ids
    return task
