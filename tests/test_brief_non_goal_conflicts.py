import copy
import json

from blueprint.brief_non_goal_conflicts import (
    brief_non_goal_conflicts_to_dict,
    detect_brief_non_goal_conflicts,
)
from blueprint.domain.models import ExecutionPlan, ImplementationBrief


def test_reports_exact_non_goal_conflicts_with_high_confidence():
    result = detect_brief_non_goal_conflicts(
        _brief(non_goals=["Do not build mobile offline mode."]),
        _plan(
            [
                _task(
                    "task-offline",
                    title="Build mobile offline mode",
                    description="Add cached order handling.",
                    acceptance_criteria=[
                        "Mobile offline mode works during network loss.",
                    ],
                    files_or_modules=["src/mobile/offline.py"],
                    tags=["mobile"],
                ),
                _task(
                    "task-api",
                    title="Update admin API",
                    description="Keep existing reporting behavior.",
                ),
            ]
        ),
    )

    assert len(result.conflicts) == 1
    conflict = result.conflicts[0]
    assert conflict.non_goal_id == "non_goal-1"
    assert conflict.non_goal == "Do not build mobile offline mode."
    assert conflict.matched_task_ids == ("task-offline",)
    assert conflict.confidence == "high"
    assert conflict.matched_terms == ("build mobile offline mode",)
    assert conflict.remediation == (
        "Review tasks task-offline against non-goal: Do not build mobile offline mode. "
        "Remove the conflicting scope or document an explicit brief change."
    )
    assert result.summary == {
        "non_goal_count": 1,
        "conflict_count": 1,
        "high_confidence_count": 1,
        "medium_confidence_count": 0,
    }


def test_reports_meaningful_token_overlap_with_medium_confidence():
    result = detect_brief_non_goal_conflicts(
        _brief(non_goals=["Avoid advanced analytics dashboard filters."]),
        _plan(
            [
                _task(
                    "task-analytics",
                    title="Improve dashboard filter controls",
                    description="Add analytics segments for finance review.",
                    acceptance_criteria=[
                        "Dashboard filters support analytics segment comparison.",
                    ],
                ),
            ]
        ),
    )

    assert len(result.conflicts) == 1
    conflict = result.conflicts[0]
    assert conflict.matched_task_ids == ("task-analytics",)
    assert conflict.confidence == "medium"
    assert conflict.matched_terms == ("analytic", "dashboard", "filter")


def test_ignores_generic_overlap_from_filler_words():
    result = detect_brief_non_goal_conflicts(
        _brief(non_goals=["Do not add new feature work."]),
        _plan(
            [
                _task(
                    "task-support",
                    title="Add support task",
                    description="Implement the planned workflow updates.",
                    acceptance_criteria=["Feature flag remains unchanged."],
                    tags=["support"],
                ),
            ]
        ),
    )

    assert result.conflicts == ()
    assert result.summary == {
        "non_goal_count": 1,
        "conflict_count": 0,
        "high_confidence_count": 0,
        "medium_confidence_count": 0,
    }


def test_empty_non_goals_return_empty_result():
    result = detect_brief_non_goal_conflicts(
        _brief(non_goals=[]),
        _plan(
            [
                _task(
                    "task-any",
                    title="Build mobile offline mode",
                    description="Add analytics dashboard filters.",
                )
            ]
        ),
    )

    assert result.conflicts == ()
    assert result.to_dict() == {
        "brief_id": "brief-non-goals",
        "plan_id": "plan-non-goals",
        "conflicts": [],
        "summary": {
            "non_goal_count": 0,
            "conflict_count": 0,
            "high_confidence_count": 0,
            "medium_confidence_count": 0,
        },
    }


def test_model_inputs_do_not_mutate_and_serialize_stably():
    brief = _brief(
        non_goals=[
            "Do not build mobile offline mode.",
            "Avoid advanced analytics dashboard filters.",
        ]
    )
    plan = _plan(
        [
            _task(
                "task-offline",
                title="Build mobile offline mode",
                description="Update mobile queue behavior.",
            ),
            _task(
                "task-analytics",
                title="Improve dashboard filter controls",
                description="Add analytics segments for finance review.",
                files_or_modules=["src/analytics/dashboard.py"],
                tags=["analytics"],
            ),
        ]
    )
    original_brief = copy.deepcopy(brief)
    original_plan = copy.deepcopy(plan)

    result = detect_brief_non_goal_conflicts(
        ImplementationBrief.model_validate(brief),
        ExecutionPlan.model_validate(plan),
    )
    payload = brief_non_goal_conflicts_to_dict(result)

    assert brief == original_brief
    assert plan == original_plan
    assert payload == result.to_dict()
    assert list(payload) == ["brief_id", "plan_id", "conflicts", "summary"]
    assert list(payload["conflicts"][0]) == [
        "non_goal_id",
        "non_goal",
        "matched_task_ids",
        "confidence",
        "matched_terms",
        "remediation",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert payload["summary"] == {
        "non_goal_count": 2,
        "conflict_count": 2,
        "high_confidence_count": 1,
        "medium_confidence_count": 1,
    }


def _brief(**overrides):
    payload = {
        "id": "brief-non-goals",
        "source_brief_id": "source-non-goals",
        "title": "Non-goal conflict plan",
        "domain": "retail",
        "target_user": "Operations manager",
        "buyer": "VP Operations",
        "workflow_context": "Daily exception review",
        "problem_statement": "The current workflow lacks task guardrails.",
        "mvp_goal": "Improve exception review",
        "product_surface": "Admin dashboard",
        "scope": ["Improve exception review"],
        "non_goals": ["Do not build mobile offline mode."],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Validate with operations manager",
        "definition_of_done": ["Plan conflicts are reported"],
    }
    payload.update(overrides)
    return payload


def _plan(tasks):
    return {
        "id": "plan-non-goals",
        "implementation_brief_id": "brief-non-goals",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    acceptance_criteria=None,
    files_or_modules=None,
    tags=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "acceptance_criteria": acceptance_criteria or ["Behavior is covered."],
        "files_or_modules": files_or_modules,
        "metadata": {"tags": tags or []},
    }
