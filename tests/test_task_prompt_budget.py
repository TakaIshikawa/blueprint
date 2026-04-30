from blueprint.audits import (
    TaskPromptBudgetResult,
    audit_task_prompt_budget,
    estimate_task_prompt,
)
from blueprint.domain.models import ExecutionTask


def test_task_prompt_budget_accepts_under_budget_plan():
    result = audit_task_prompt_budget(
        _plan([_task("task-small", description="Update the queue summary.")]),
        warning_threshold=80,
        error_threshold=120,
    )

    assert isinstance(result, TaskPromptBudgetResult)
    assert result.ok is True
    assert result.total_tasks_checked == 1
    assert result.warning_count == 0
    assert result.error_count == 0
    assert result.findings == []
    assert result.largest_task_ids == ["task-small"]


def test_task_prompt_budget_flags_warning_with_affected_task_context():
    task = _task(
        "task-warning",
        description=_repeated_text("Describe the importer edge case", 14),
        acceptance_criteria=["Importer handles archived issues"] * 4,
    )
    estimate = estimate_task_prompt(task)

    result = audit_task_prompt_budget(
        _plan([task]),
        warning_threshold=estimate.estimated_tokens,
        error_threshold=estimate.estimated_tokens + 20,
    )

    assert result.ok is True
    assert result.warning_count == 1
    assert result.error_count == 0
    finding = result.findings[0]
    assert finding.severity == "warning"
    assert finding.code == "task_prompt_budget_warning"
    assert finding.task_id == "task-warning"
    assert finding.estimated_tokens == estimate.estimated_tokens
    assert finding.threshold == estimate.estimated_tokens
    assert "task-warning" in finding.message
    assert finding.largest_fields[0] == "description"
    assert "Split or trim this task" in finding.remediation


def test_task_prompt_budget_flags_error_at_configured_threshold():
    task = _task(
        "task-error",
        description=_repeated_text("Implement migration and validation behavior", 20),
        metadata={"notes": _repeated_text("Preserve compatibility", 12)},
    )
    estimate = estimate_task_prompt(task)

    result = audit_task_prompt_budget(
        _plan([task]),
        warning_threshold=10,
        error_threshold=estimate.estimated_tokens,
    )

    assert result.ok is False
    assert result.warning_count == 0
    assert result.error_count == 1
    assert result.findings[0].severity == "error"
    assert result.findings[0].code == "task_prompt_budget_error"
    assert result.findings[0].task_id == "task-error"
    assert result.to_dict()["summary"]["errors"] == 1


def test_task_prompt_budget_handles_missing_optional_fields_and_execution_task():
    task = ExecutionTask(
        id="task-minimal",
        title="Minimal task",
        description="Make the focused change.",
        acceptance_criteria=["Focused change is covered"],
    )

    result = audit_task_prompt_budget(
        _plan([task]),
        warning_threshold=80,
        error_threshold=120,
    )

    assert result.findings == []
    assert result.total_tasks_checked == 1
    assert result.largest_tasks[0].task_id == "task-minimal"
    assert result.largest_tasks[0].field_token_estimates["files_or_modules"] == 0
    assert result.largest_tasks[0].field_token_estimates["test_command"] == 0


def test_task_prompt_budget_orders_largest_tasks_and_findings_by_estimated_size():
    small = _task("task-small", description="Small change.")
    tie_b = _task("task-b", description=_repeated_text("Tie sized task", 12))
    tie_a = _task("task-a", description=_repeated_text("Tie sized task", 12))
    large = _task("task-large", description=_repeated_text("Largest task body", 24))

    result = audit_task_prompt_budget(
        _plan([small, tie_b, large, tie_a]),
        warning_threshold=estimate_task_prompt(tie_a).estimated_tokens,
        error_threshold=500,
    )

    assert result.largest_task_ids == [
        "task-large",
        "task-a",
        "task-b",
        "task-small",
    ]
    assert [finding.task_id for finding in result.findings] == [
        "task-large",
        "task-a",
        "task-b",
    ]


def _plan(tasks):
    return {
        "id": "plan-budget",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    description,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
    test_command=None,
):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "description": description,
        "acceptance_criteria": acceptance_criteria or ["Task is complete"],
        "files_or_modules": files_or_modules if files_or_modules is not None else ["src/app.py"],
        "metadata": metadata or {},
        "test_command": test_command,
    }


def _repeated_text(text, count):
    return " ".join([text] * count)
