from copy import deepcopy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_sequencing_constraints import (
    TaskSequencingFinding,
    analyze_task_sequencing_constraints,
    task_sequencing_findings_to_dicts,
)


def test_valid_acyclic_plan_has_no_findings():
    plan = _plan(
        [
            _task("task-foundation"),
            _task("task-api", depends_on=["task-foundation"]),
            _task("task-ui", depends_on=["task-api"]),
            _task("task-docs"),
        ]
    )

    assert analyze_task_sequencing_constraints(plan, max_chain_depth=3) == ()


def test_missing_dependency_references_include_actionable_remediation():
    findings = analyze_task_sequencing_constraints(
        _plan(
            [
                _task("task-api", depends_on=["task-schema"]),
                _task("task-ui", depends_on=["task-api", "task-design"]),
            ]
        )
    )

    assert [finding.to_dict() for finding in findings] == [
        {
            "task_id": "task-api",
            "severity": "error",
            "reason": "Task depends on missing prerequisite task(s): task-schema.",
            "suggested_remediation": (
                "Add the missing prerequisite task(s), correct the depends_on IDs, "
                "or remove stale dependency references before dispatch."
            ),
            "code": "missing_dependency",
            "related_task_ids": ["task-schema"],
        },
        {
            "task_id": "task-ui",
            "severity": "error",
            "reason": "Task depends on missing prerequisite task(s): task-design.",
            "suggested_remediation": (
                "Add the missing prerequisite task(s), correct the depends_on IDs, "
                "or remove stale dependency references before dispatch."
            ),
            "code": "missing_dependency",
            "related_task_ids": ["task-design"],
        },
    ]


def test_parallelization_conflict_when_dependent_tasks_claim_independent_execution():
    findings = analyze_task_sequencing_constraints(
        _plan(
            [
                _task("task-schema", metadata={"parallel_safe": True}),
                _task(
                    "task-api",
                    depends_on=["task-schema"],
                    metadata={"independently_runnable": True},
                ),
            ]
        )
    )

    assert len(findings) == 1
    finding = findings[0]
    assert finding.code == "parallel_dependency_conflict"
    assert finding.task_id == "task-api"
    assert finding.severity == "error"
    assert finding.related_task_ids == ("task-schema",)
    assert "marked independently runnable" in finding.reason
    assert "Remove independent/parallel-safe metadata" in finding.suggested_remediation


def test_unresolved_decision_blockers_are_reported():
    findings = analyze_task_sequencing_constraints(
        _plan(
            [
                _task(
                    "task-billing",
                    blocked_reason="Waiting for product decision on billing tier names.",
                    metadata={
                        "decisions": [
                            {
                                "question": "Which billing provider owns retries?",
                                "status": "unresolved",
                            },
                            {
                                "decision": "Use Stripe test mode.",
                                "status": "decided",
                            },
                        ],
                    },
                )
            ]
        )
    )

    assert len(findings) == 1
    assert findings[0].to_dict() == {
        "task_id": "task-billing",
        "severity": "error",
        "reason": (
            "Task is blocked by unresolved decision(s): "
            "Which billing provider owns retries?; "
            "Waiting for product decision on billing tier names."
        ),
        "suggested_remediation": (
            "Resolve and record the decision outcome, then clear the decision blocker "
            "before autonomous execution."
        ),
        "code": "unresolved_decision_blocker",
        "related_task_ids": [],
    }


def test_chain_depth_threshold_warnings_are_deterministic():
    findings = analyze_task_sequencing_constraints(
        _plan(
            [
                _task("task-a"),
                _task("task-b", depends_on=["task-a"]),
                _task("task-c", depends_on=["task-b"]),
                _task("task-d", depends_on=["task-c"]),
            ]
        ),
        max_chain_depth=1,
    )

    assert [(finding.task_id, finding.code, finding.severity) for finding in findings] == [
        ("task-c", "dependency_chain_depth_exceeded", "warning"),
        ("task-d", "dependency_chain_depth_exceeded", "warning"),
    ]
    assert findings[0].reason == (
        "Task has dependency chain depth 2, which exceeds the configured limit of 1."
    )
    assert findings[1].reason == (
        "Task has dependency chain depth 3, which exceeds the configured limit of 1."
    )
    assert findings[0].related_task_ids == ("task-b",)
    assert findings[1].related_task_ids == ("task-c",)


def test_model_inputs_serialize_without_mutation():
    plan = _plan(
        [
            _task("task-a"),
            _task("task-b", depends_on=["task-a", "task-missing"]),
        ]
    )
    original = deepcopy(plan)

    findings = analyze_task_sequencing_constraints(ExecutionPlan.model_validate(plan))
    payload = task_sequencing_findings_to_dicts(findings)

    assert plan == original
    assert isinstance(findings[0], TaskSequencingFinding)
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload[0]) == [
        "task_id",
        "severity",
        "reason",
        "suggested_remediation",
        "code",
        "related_task_ids",
    ]


def _plan(tasks):
    return {
        "id": "plan-test",
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
    depends_on=None,
    metadata=None,
    blocked_reason=None,
):
    return {
        "id": task_id,
        "title": f"Task {task_id}",
        "description": f"Implement {task_id}",
        "milestone": "Build",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{task_id} works"],
        "estimated_complexity": "medium",
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": blocked_reason,
    }
