import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.implementation_readiness_scorecard import (
    ImplementationReadinessScorecard,
    build_implementation_readiness_scorecard,
    implementation_readiness_scorecard_to_dict,
    score_implementation_readiness,
)


def test_ready_plan_returns_full_scores_and_stable_serialization():
    result = build_implementation_readiness_scorecard(
        _plan(
            [
                _task("task-api", owner="backend", test_command="pytest tests/test_api.py"),
                _task(
                    "task-ui",
                    owner="frontend",
                    depends_on=["task-api"],
                    test_command="npm test -- task-ui",
                ),
            ],
            test_strategy="Run focused backend and frontend test commands.",
        )
    )
    payload = implementation_readiness_scorecard_to_dict(result)

    assert isinstance(result, ImplementationReadinessScorecard)
    assert result.plan_id == "plan-ready"
    assert result.status == "ready"
    assert result.overall_score == 100
    assert result.findings == ()
    assert _scores(result) == {
        "task_completeness": 100,
        "dependency_clarity": 100,
        "validation_coverage": 100,
        "acceptance_criteria_quality": 100,
        "blocker_status": 100,
        "ownership_coverage": 100,
    }
    assert payload == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "status",
        "overall_score",
        "category_scores",
        "findings",
        "summary",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_high_risk_blocked_task_forces_blocked_status():
    result = build_implementation_readiness_scorecard(
        _plan(
            [
                _task(
                    "task-payment",
                    owner="backend",
                    risk_level="high",
                    status="blocked",
                    blocked_reason="Waiting for PCI review.",
                ),
                _task("task-copy", owner="frontend"),
            ]
        )
    )

    assert result.status == "blocked"
    assert _scores(result)["blocker_status"] == 0
    assert result.summary["blocked_task_count"] == 1
    assert result.summary["severe_blocked_task_count"] == 1
    assert result.findings[0].category == "blocker_status"
    assert result.findings[0].severity == "blocking"
    assert result.findings[0].task_ids == ("task-payment",)


def test_low_risk_blocked_task_forces_needs_attention_without_blocking():
    result = build_implementation_readiness_scorecard(
        _plan(
            [
                _task("task-api", owner="backend"),
                _task(
                    "task-docs",
                    owner="docs",
                    risk_level="low",
                    status="blocked",
                    blocked_reason="Waiting for copy review.",
                ),
            ]
        )
    )

    assert result.status == "needs_attention"
    assert _scores(result)["blocker_status"] == 70
    assert not any(finding.severity == "blocking" for finding in result.findings)


def test_weak_validation_coverage_lowers_score_deterministically():
    result = build_implementation_readiness_scorecard(
        _plan(
            [
                _task("task-api", owner="backend", test_command="pytest tests/test_api.py"),
                _task("task-worker", owner="backend", test_command=None),
                _task("task-ui", owner="frontend", test_command=None),
            ],
            test_strategy=None,
        )
    )

    assert result.status == "blocked"
    assert _scores(result)["validation_coverage"] == 33
    assert result.overall_score == 87
    finding = _finding(result, "validation_coverage")
    assert finding.severity == "blocking"
    assert finding.task_ids == ("task-worker", "task-ui")


def test_missing_owners_lower_ownership_score_and_status():
    result = build_implementation_readiness_scorecard(
        _plan(
            [
                _task("task-api", owner="backend"),
                _task("task-worker", owner=None),
                _task("task-ui", owner=None),
            ]
        )
    )

    assert result.status == "blocked"
    assert _scores(result)["ownership_coverage"] == 33
    assert result.summary["owned_task_count"] == 1
    finding = _finding(result, "ownership_coverage")
    assert finding.severity == "blocking"
    assert finding.task_ids == ("task-worker", "task-ui")


def test_category_weighting_controls_overall_score():
    result = build_implementation_readiness_scorecard(
        _plan(
            [
                _task("task-api", owner="backend", test_command="pytest tests/test_api.py"),
                _task("task-worker", owner="backend", test_command=None),
            ],
            test_strategy=None,
        )
    )

    scores = _scores(result)
    weighted_scores = {
        score.category: score.weighted_score for score in result.category_scores
    }

    assert scores["validation_coverage"] == 50
    assert weighted_scores == {
        "task_completeness": 20.0,
        "dependency_clarity": 15.0,
        "validation_coverage": 10.0,
        "acceptance_criteria_quality": 15.0,
        "blocker_status": 15.0,
        "ownership_coverage": 15.0,
    }
    assert result.overall_score == 90
    assert result.summary["category_weights"]["validation_coverage"] == 0.2


def test_model_task_iterable_and_alias_are_supported():
    plan = ExecutionPlan.model_validate(
        _plan([_task("task-model", owner="backend", test_command="pytest tests/test_model.py")])
    )
    task = ExecutionTask.model_validate(
        _task("task-single", owner="agent", test_command="pytest tests/test_single.py")
    )

    plan_result = score_implementation_readiness(plan)
    iterable_result = build_implementation_readiness_scorecard([task])

    assert plan_result.status == "ready"
    assert iterable_result.plan_id is None
    assert iterable_result.status == "ready"
    assert iterable_result.summary["task_count"] == 1


def _scores(result):
    return {score.category: score.score for score in result.category_scores}


def _finding(result, category):
    return next(finding for finding in result.findings if finding.category == category)


def _plan(tasks, *, test_strategy="Run the focused test_command for each task."):
    return {
        "id": "plan-ready",
        "implementation_brief_id": "brief-ready",
        "milestones": [{"name": "Implementation"}],
        "test_strategy": test_strategy,
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    owner,
    depends_on=None,
    test_command="pytest tests/test_impl.py",
    risk_level="low",
    status="pending",
    blocked_reason=None,
):
    return {
        "id": task_id,
        "title": task_id.replace("-", " ").title(),
        "description": f"Implement {task_id} and wire it into the existing workflow.",
        "owner_type": owner,
        "depends_on": depends_on or [],
        "files_or_modules": [f"src/{task_id}.py"],
        "acceptance_criteria": [
            f"{task_id} returns the expected result for valid input.",
            f"{task_id} rejects invalid input with a clear error.",
        ],
        "estimated_hours": 2,
        "risk_level": risk_level,
        "test_command": test_command,
        "status": status,
        "blocked_reason": blocked_reason,
        "metadata": {},
    }
