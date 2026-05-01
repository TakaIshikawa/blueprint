from copy import deepcopy
import json
import re

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_branch_isolation import (
    PlanBranchIsolationFinding,
    advise_plan_branch_isolation,
    plan_branch_isolation_to_dict,
)


def test_independent_tasks_with_distinct_files_get_separate_parallel_branches():
    advice = advise_plan_branch_isolation(
        _plan(
            [
                _task("task-api", files=["src/api.py"]),
                _task("task-ui", files=["src/ui.py"]),
                _task("task-docs", files=["docs/usage.md"]),
            ]
        )
    )

    assert [group.task_ids for group in advice.branch_groups] == [
        ("task-api",),
        ("task-ui",),
        ("task-docs",),
    ]
    assert [group.isolation_level for group in advice.branch_groups] == [
        "parallel",
        "parallel",
        "parallel",
    ]
    assert advice.parallel_branch_names == (
        "bp/plan-test/task-api",
        "bp/plan-test/task-ui",
        "bp/plan-test/task-docs",
    )
    assert advice.findings == ()


def test_shared_file_contention_is_serialized_and_flagged():
    advice = advise_plan_branch_isolation(
        _plan(
            [
                _task("task-api", files=["src/app.py", "src/routes.py"]),
                _task("task-validation", files=["src/app.py"]),
                _task("task-docs", files=["docs/usage.md"]),
            ]
        )
    )

    assert [group.task_ids for group in advice.branch_groups] == [
        ("task-api", "task-validation"),
        ("task-docs",),
    ]
    contended = advice.branch_groups[0]
    assert contended.isolation_level == "serialized"
    assert contended.branch_name == "bp/plan-test/task-api-task-validation"
    assert contended.serialization_reasons == ("shared files_or_modules: src/app.py",)

    assert [finding.to_dict() for finding in advice.findings] == [
        {
            "code": "file_contention",
            "severity": "warning",
            "reason": (
                "Tasks task-api and task-validation touch overlapping "
                "files_or_modules: src/app.py."
            ),
            "suggested_remediation": (
                "Keep the tasks on one serialized branch group or split the file ownership "
                "so autonomous agents do not edit the same paths in parallel."
            ),
            "task_ids": ["task-api", "task-validation"],
            "file_paths": ["src/app.py"],
        }
    ]


def test_dependency_linked_tasks_are_not_recommended_for_parallel_branches():
    advice = advise_plan_branch_isolation(
        _plan(
            [
                _task("task-schema", files=["src/schema.py"]),
                _task("task-api", depends_on=["task-schema"], files=["src/api.py"]),
                _task("task-ui", files=["src/ui.py"]),
            ]
        )
    )

    assert [group.task_ids for group in advice.branch_groups] == [
        ("task-schema", "task-api"),
        ("task-ui",),
    ]
    dependent_group = advice.branch_groups[0]
    assert dependent_group.isolation_level == "serialized"
    assert dependent_group.serialization_reasons == ("task-api depends on task-schema",)
    assert dependent_group.branch_name not in advice.parallel_branch_names
    assert dependent_group.branch_name in advice.serialized_branch_names

    assert len(advice.findings) == 1
    assert advice.findings[0].code == "dependency_serialization_required"
    assert advice.findings[0].task_ids == ("task-api", "task-schema")


def test_owner_and_scope_metadata_are_carried_into_branch_groups_and_findings():
    advice = advise_plan_branch_isolation(
        _plan(
            [
                _task(
                    "task-billing",
                    files=["src/billing.py"],
                    owner_type="agent",
                    metadata={"owner": "payments-team"},
                    estimated_complexity="large",
                    risk_level="high",
                )
            ]
        )
    )

    assert len(advice.branch_groups) == 1
    group = advice.branch_groups[0]
    assert group.owner == "payments-team"
    assert group.owner_type == "agent"
    assert group.isolation_level == "parallel"

    assert len(advice.findings) == 1
    assert advice.findings[0].to_dict() == {
        "code": "manual_branch_review_recommended",
        "severity": "info",
        "reason": "Task task-billing has high risk and large estimated complexity.",
        "suggested_remediation": (
            "Review branch scope before dispatch and avoid combining this task with "
            "unrelated parallel work."
        ),
        "task_ids": ["task-billing"],
        "file_paths": ["src/billing.py"],
    }


def test_branch_names_are_deterministic_slug_safe_and_inputs_serialize_without_mutation():
    plan = _plan(
        [
            _task("Task API v2!", files=["src/API Service.py"]),
            _task("Task UI++", files=["src/ui.py"]),
        ],
        plan_id="Plan With Spaces!",
    )
    original = deepcopy(plan)

    first = advise_plan_branch_isolation(ExecutionPlan.model_validate(plan))
    second = advise_plan_branch_isolation(plan)
    payload = plan_branch_isolation_to_dict(first)

    assert plan == original
    assert [group.branch_name for group in first.branch_groups] == [
        "bp/plan-with-spaces/task-api-v2",
        "bp/plan-with-spaces/task-ui",
    ]
    assert [group.branch_name for group in first.branch_groups] == [
        group.branch_name for group in second.branch_groups
    ]
    assert all(re.fullmatch(r"[a-z0-9._/-]+", group.branch_name) for group in first.branch_groups)
    assert json.loads(json.dumps(payload)) == payload
    assert isinstance(payload["findings"], list)
    assert isinstance(PlanBranchIsolationFinding, type)


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
    files=None,
    depends_on=None,
    metadata=None,
    owner_type="agent",
    estimated_complexity="medium",
    risk_level="medium",
):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "description": f"Implement {task_id}",
        "milestone": "Build",
        "owner_type": owner_type,
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": files or ["src/app.py"],
        "acceptance_criteria": [f"{task_id} works"],
        "estimated_complexity": estimated_complexity,
        "risk_level": risk_level,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
