import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_unresolved_blockers import (
    PlanUnresolvedBlockerSummary,
    build_plan_unresolved_blocker_summary,
    plan_unresolved_blocker_summary_to_dict,
    plan_unresolved_blocker_summary_to_markdown,
    summarize_plan_unresolved_blockers,
)


def test_empty_plan_has_no_blockers_and_continues():
    result = build_plan_unresolved_blocker_summary({"id": "plan-empty", "tasks": []})

    assert result.plan_id == "plan-empty"
    assert result.blocker_groups == ()
    assert result.blocked_task_ids == ()
    assert result.should_pause_execution is False
    assert result.recommended_next_action == "continue_execution"
    assert result.summary == {
        "task_count": 0,
        "blocker_group_count": 0,
        "blocker_count": 0,
        "affected_task_count": 0,
        "high_severity_count": 0,
        "medium_severity_count": 0,
        "low_severity_count": 0,
        "blocker_type_counts": {
            "blocked_task": 0,
            "missing_dependency_output": 0,
            "validation_blocker": 0,
            "missing_owner": 0,
            "unresolved_assumption": 0,
            "unanswered_question": 0,
        },
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Unresolved Blockers: plan-empty",
            "",
            "No unresolved blockers were found.",
        ]
    )


def test_mixed_blockers_are_grouped_with_affected_task_ids_and_pause():
    result = build_plan_unresolved_blocker_summary(
        _plan(
            [
                _task(
                    "task-design",
                    owner_type="designer",
                    status="completed",
                    outputs=["approved flow"],
                ),
                _task(
                    "task-api",
                    owner_type=None,
                    depends_on=["task-design", "task-schema"],
                    blocked_reason="Blocked waiting for partner API credentials.",
                    assumptions=[
                        {
                            "assumption": "Partner API credentials are available.",
                            "status": "unresolved",
                        },
                        {
                            "assumption": "OAuth scopes are documented.",
                            "status": "validated",
                        },
                    ],
                    questions=[
                        {"question": "Which OAuth scopes are required?", "status": "open"},
                        {"question": "Who reviews the rollout?", "answer": "Platform lead"},
                    ],
                    validation_blockers=[
                        {"description": "Need sandbox token before validation.", "resolved": False},
                    ],
                ),
            ]
        )
    )

    groups = {group.blocker_type: group for group in result.blocker_groups}

    assert [group.blocker_type for group in result.blocker_groups] == [
        "blocked_task",
        "missing_dependency_output",
        "validation_blocker",
        "missing_owner",
        "unresolved_assumption",
        "unanswered_question",
    ]
    assert groups["blocked_task"].affected_task_ids == ("task-api",)
    assert groups["missing_dependency_output"].details == (
        "missing dependency task: task-schema",
    )
    assert groups["validation_blocker"].details == (
        "Need sandbox token before validation.",
    )
    assert groups["missing_owner"].affected_task_ids == ("task-api",)
    assert groups["unresolved_assumption"].details == (
        "Partner API credentials are available.",
    )
    assert groups["unanswered_question"].details == (
        "Which OAuth scopes are required?",
    )
    assert result.blocked_task_ids == ("task-api",)
    assert result.should_pause_execution is True
    assert result.recommended_next_action == "pause_execution_resolve_high_severity_blockers"
    assert result.summary["blocker_count"] == 6
    assert result.summary["high_severity_count"] == 3
    assert result.summary["medium_severity_count"] == 2
    assert result.summary["low_severity_count"] == 1


def test_missing_owner_questions_and_assumptions_do_not_pause_without_high_severity():
    result = build_plan_unresolved_blocker_summary(
        _plan(
            [
                _task(
                    "task-copy",
                    owner_type=None,
                    assumptions=["Pending legal approval for empty-state copy."],
                    questions=["Unanswered: should admins see the draft copy?"],
                )
            ]
        )
    )

    assert [group.severity for group in result.blocker_groups] == [
        "medium",
        "medium",
        "low",
    ]
    assert result.should_pause_execution is False
    assert result.recommended_next_action == "continue_execution"


def test_dependency_outputs_are_detected_independently_from_blocked_status():
    result = build_plan_unresolved_blocker_summary(
        _plan(
            [
                _task("task-schema", owner_type="backend", status="completed"),
                _task("task-client", owner_type="frontend", depends_on=["task-schema"]),
            ]
        )
    )

    assert len(result.blocker_groups) == 1
    group = result.blocker_groups[0]
    assert group.blocker_type == "missing_dependency_output"
    assert group.severity == "high"
    assert group.affected_task_ids == ("task-client",)
    assert group.details == ("dependency task-schema is complete but has no outputs",)
    assert result.should_pause_execution is True


def test_model_and_iterable_inputs_serialize_without_mutation():
    plan = _plan(
        [
            _task(
                "task-validation",
                owner_type="qa",
                metadata={
                    "validation_blockers": ["Blocked until staging data is refreshed."],
                },
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_unresolved_blocker_summary(ExecutionPlan.model_validate(plan))
    payload = plan_unresolved_blocker_summary_to_dict(result)
    task = summarize_plan_unresolved_blockers(
        ExecutionTask.model_validate(
            _task(
                "task-single",
                owner_type=None,
                metadata={"assumptions": ["Unknown browser support matrix."]},
            )
        )
    )
    iterable = build_plan_unresolved_blocker_summary(
        [_task("task-owned", owner_type="backend")]
    )

    assert plan == original
    assert isinstance(result, PlanUnresolvedBlockerSummary)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["blocker_groups"]
    assert list(payload) == [
        "plan_id",
        "blocker_groups",
        "blocked_task_ids",
        "should_pause_execution",
        "recommended_next_action",
        "summary",
    ]
    assert list(payload["blocker_groups"][0]) == [
        "blocker_type",
        "severity",
        "affected_task_ids",
        "count",
        "details",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert task.plan_id is None
    assert [group.blocker_type for group in task.blocker_groups] == [
        "missing_owner",
        "unresolved_assumption",
    ]
    assert iterable.blocker_groups == ()


def test_markdown_and_deterministic_severity_ordering():
    result = build_plan_unresolved_blocker_summary(
        _plan(
            [
                _task(
                    "task-z",
                    owner_type=None,
                    questions=["Pending decision from support."],
                ),
                _task(
                    "task-a",
                    owner_type="backend",
                    status="blocked",
                    blocked_reason="Blocked by vendor outage.",
                    validation_blockers=["Cannot validate until vendor outage clears."],
                ),
            ]
        )
    )

    assert [(group.severity, group.blocker_type) for group in result.blocker_groups] == [
        ("high", "blocked_task"),
        ("high", "validation_blocker"),
        ("medium", "missing_owner"),
        ("low", "unanswered_question"),
    ]
    assert plan_unresolved_blocker_summary_to_markdown(result) == "\n".join(
        [
            "# Plan Unresolved Blockers: plan-blockers",
            "",
            "| Blocker Type | Severity | Count | Affected Tasks |",
            "| --- | --- | --- | --- |",
            "| blocked_task | high | 1 | task-a |",
            "| validation_blocker | high | 1 | task-a |",
            "| missing_owner | medium | 1 | task-z |",
            "| unanswered_question | low | 1 | task-z |",
        ]
    )


def _plan(tasks):
    return {
        "id": "plan-blockers",
        "implementation_brief_id": "brief-blockers",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    owner_type="worker",
    depends_on=None,
    acceptance_criteria=None,
    status="pending",
    blocked_reason=None,
    assumptions=None,
    questions=None,
    validation_blockers=None,
    outputs=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "owner_type": owner_type,
        "depends_on": depends_on or [],
        "files_or_modules": [],
        "acceptance_criteria": (
            acceptance_criteria if acceptance_criteria is not None else ["Done"]
        ),
        "status": status,
        "metadata": metadata or {},
    }
    if blocked_reason is not None:
        task["blocked_reason"] = blocked_reason
    if assumptions is not None:
        task["assumptions"] = assumptions
    if questions is not None:
        task["questions"] = questions
    if validation_blockers is not None:
        task["validation_blockers"] = validation_blockers
    if outputs is not None:
        task["outputs"] = outputs
    return task
