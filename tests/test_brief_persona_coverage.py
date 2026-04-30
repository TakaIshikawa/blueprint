import copy
import json

from blueprint.brief_persona_coverage import (
    brief_persona_coverage_to_dict,
    brief_persona_coverage_to_markdown,
    evaluate_brief_persona_coverage,
)
from blueprint.domain.models import ExecutionPlan, ImplementationBrief


def test_reports_covered_weak_and_missing_persona_concerns_with_evidence():
    result = evaluate_brief_persona_coverage(
        _brief(
            target_user="Finance operations manager",
            buyer="VP Finance",
            workflow_context="Monthly close dashboard review",
            product_surface="Admin reporting dashboard",
            mvp_goal="Export close variance report",
            validation_plan="Validate export with finance operations manager",
        ),
        _plan(
            [
                _task(
                    "task-dashboard",
                    title="Update admin reporting dashboard",
                    description="Improve the monthly close dashboard review filters.",
                    acceptance_criteria=[
                        "Finance operations manager can export close variance report.",
                    ],
                ),
                _task(
                    "task-validation",
                    title="Add export validation",
                    description="Run a validation session for the export workflow.",
                    acceptance_criteria=[
                        "Validate export with finance operations manager before handoff.",
                    ],
                ),
            ]
        ),
    )

    assert [item.source_field for item in result.covered_personas] == [
        "target_user",
        "mvp_goal",
        "validation_plan",
    ]
    assert [item.source_field for item in result.weak_coverage] == [
        "workflow_context",
        "product_surface",
    ]
    assert [item.source_field for item in result.missing_coverage] == ["buyer"]
    assert result.summary == {
        "concern_count": 6,
        "covered_count": 3,
        "weak_count": 2,
        "missing_count": 1,
    }

    target_user = result.covered_personas[0]
    assert target_user.matched_task_ids == ("task-dashboard", "task-validation")
    assert target_user.evidence[0].to_dict() == {
        "task_id": "task-dashboard",
        "field": "acceptance_criteria[0]",
        "snippet": "Finance operations manager can export close variance report.",
        "strength": "strong",
    }

    weak = result.weak_coverage[0]
    assert weak.status == "weak"
    assert weak.recommendation is not None
    assert weak.recommendation.to_dict() == {
        "concern_id": "workflow_context-3",
        "recommendation_type": "add_acceptance_criterion",
        "text": "Workflow context concern is validated: Monthly close dashboard review.",
        "task_ids": ["task-dashboard"],
    }

    missing = result.missing_coverage[0]
    assert missing.recommendation is not None
    assert missing.recommendation.text == "Add plan task: Cover buyer - VP Finance."


def test_model_and_dict_inputs_are_supported_without_mutation_and_serialize_stably():
    brief = _brief(
        target_user="Store associate",
        buyer="Retail operations lead",
        workflow_context="In-store pickup exception workflow",
        product_surface="Mobile order queue",
        mvp_goal="Resolve pickup exceptions",
        validation_plan="Observe store associate resolving pickup exceptions",
    )
    plan = _plan(
        [
            _task(
                "task-pickup",
                title="Update mobile order queue",
                description="Handle in-store pickup exception workflow for store associates.",
                acceptance_criteria=[
                    "Store associate can resolve pickup exceptions from the mobile order queue.",
                    "Observe store associate resolving pickup exceptions.",
                ],
            )
        ]
    )
    original_brief = copy.deepcopy(brief)
    original_plan = copy.deepcopy(plan)

    result = evaluate_brief_persona_coverage(
        ImplementationBrief.model_validate(brief),
        ExecutionPlan.model_validate(plan),
    )
    payload = brief_persona_coverage_to_dict(result)
    markdown = brief_persona_coverage_to_markdown(result)

    assert brief == original_brief
    assert plan == original_plan
    assert payload == result.to_dict()
    assert list(payload) == [
        "brief_id",
        "plan_id",
        "covered_personas",
        "weak_coverage",
        "missing_coverage",
        "recommendations",
        "summary",
    ]
    assert list(payload["covered_personas"][0]) == [
        "concern_id",
        "source_field",
        "concern",
        "status",
        "matched_task_ids",
        "evidence",
        "recommendation",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert markdown.startswith("# Brief Persona Coverage: plan-persona")
    assert "| Target user: Store associate | covered | task-pickup |" in markdown


def test_empty_persona_fields_are_supported():
    result = evaluate_brief_persona_coverage(
        _brief(
            target_user=None,
            buyer=None,
            workflow_context=None,
            product_surface=None,
        ),
        _plan([]),
    )

    assert result.to_dict()["summary"] == {
        "concern_count": 2,
        "covered_count": 0,
        "weak_count": 0,
        "missing_count": 2,
    }
    assert [item.source_field for item in result.missing_coverage] == [
        "mvp_goal",
        "validation_plan",
    ]


def _brief(**overrides):
    payload = {
        "id": "brief-persona",
        "source_brief_id": "source-persona",
        "title": "Persona plan",
        "domain": "retail",
        "target_user": "Default user",
        "buyer": "Default buyer",
        "workflow_context": "Default workflow",
        "problem_statement": "The current workflow misses context.",
        "mvp_goal": "Ship contextual workflow support",
        "product_surface": "Default surface",
        "scope": ["Improve workflow"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Validate with target user",
        "definition_of_done": ["Persona workflow is represented"],
    }
    payload.update(overrides)
    return payload


def _plan(tasks):
    return {
        "id": "plan-persona",
        "implementation_brief_id": "brief-persona",
        "milestones": [],
        "tasks": tasks,
    }


def _task(task_id, title, description, acceptance_criteria):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "acceptance_criteria": acceptance_criteria,
    }
