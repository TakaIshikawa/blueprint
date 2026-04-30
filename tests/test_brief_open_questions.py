import json

from blueprint.brief_open_questions import (
    BriefOpenQuestion,
    extract_open_questions,
    open_questions_to_dict,
)
from blueprint.domain.models import ImplementationBrief


def test_complete_briefs_return_empty_open_questions():
    questions = extract_open_questions(_brief())

    assert questions == ()


def test_missing_integration_and_data_details_produce_actionable_questions():
    questions = extract_open_questions(
        _brief(
            integration_points=[],
            data_requirements=None,
        )
    )

    by_topic = _by_topic(questions)

    assert isinstance(questions[0], BriefOpenQuestion)
    assert by_topic["integration_points"].to_dict() == {
        "topic": "integration_points",
        "question": (
            "Which external systems, APIs, or services does the implementation depend on?"
        ),
        "evidence": ["integration_points: missing"],
        "severity": "medium",
        "suggested_owner": "technical_lead",
    }
    assert by_topic["data_requirements"].evidence == ("data_requirements: missing",)
    assert by_topic["data_requirements"].suggested_owner == "data_owner"


def test_vague_validation_language_is_flagged_for_qa_owner():
    questions = extract_open_questions(_brief(validation_plan="Test it and make sure it works."))

    assert questions == (
        BriefOpenQuestion(
            topic="validation_plan",
            question=(
                "What specific automated or manual checks will prove the implementation works?"
            ),
            evidence=(
                "validation_plan: Test it and make sure it works. " "(generic validation language)",
                "validation_plan lacks a concrete check, command, metric, or scenario",
            ),
            severity="medium",
            suggested_owner="qa_owner",
        ),
    )


def test_empty_and_vague_scope_assign_high_severity():
    questions = extract_open_questions(
        _brief(
            scope=["TBD", "Improve intake as needed"],
            definition_of_done=[],
        )
    )

    by_topic = _by_topic(questions)

    assert by_topic["scope"].severity == "high"
    assert by_topic["scope"].evidence == (
        "scope.1: TBD (placeholder)",
        "scope.1: TBD (too terse to guide task planning)",
        "scope.2: Improve intake as needed (undefined condition)",
    )
    assert by_topic["definition_of_done"].severity == "high"
    assert by_topic["definition_of_done"].suggested_owner == "delivery_lead"


def test_question_ordering_and_serialization_are_stable_for_model_inputs():
    questions = extract_open_questions(
        ImplementationBrief.model_validate(
            _brief(
                scope=[],
                assumptions=[],
                integration_points=["CRM API", "TBD"],
                architecture_notes="TBD",
                validation_plan="Run tests",
            )
        )
    )
    payload = open_questions_to_dict(questions)

    assert [question.topic for question in questions] == [
        "scope",
        "assumptions",
        "integration_points",
        "architecture_notes",
        "validation_plan",
    ]
    assert [question.severity for question in questions] == [
        "high",
        "low",
        "high",
        "high",
        "medium",
    ]
    assert payload == [question.to_dict() for question in questions]
    assert list(payload[0]) == [
        "topic",
        "question",
        "evidence",
        "severity",
        "suggested_owner",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _by_topic(questions):
    return {question.topic: question for question in questions}


def _brief(
    *,
    scope=None,
    assumptions=None,
    architecture_notes=(
        "Extend the existing intake service with a deterministic priority scorer and "
        "document the queue contract."
    ),
    data_requirements=("Persist queue item priority, owner id, SLA due date, and audit timestamp."),
    integration_points=None,
    validation_plan=(
        "Run pytest tests/test_intake_queue.py and manually verify the urgent-case "
        "routing scenario."
    ),
    definition_of_done=None,
):
    return {
        "id": "brief-open-questions",
        "source_brief_id": "source-open-questions",
        "title": "Open Questions Brief",
        "problem_statement": "Care teams need clearer intake prioritization.",
        "mvp_goal": "Ship priority-aware intake queue routing.",
        "scope": [
            "Add priority sorting to the intake queue",
            "Show owner and SLA metadata for each queue item",
        ]
        if scope is None
        else scope,
        "non_goals": [],
        "assumptions": [
            "The intake service already stores owner metadata",
            "Existing role permissions apply to queue metadata",
        ]
        if assumptions is None
        else assumptions,
        "architecture_notes": architecture_notes,
        "data_requirements": data_requirements,
        "integration_points": ["CRM API", "Notification webhook"]
        if integration_points is None
        else integration_points,
        "risks": [],
        "validation_plan": validation_plan,
        "definition_of_done": [
            "Priority queue ordering is covered by automated tests",
            "Support reviewers can inspect owner and SLA metadata in the queue",
        ]
        if definition_of_done is None
        else definition_of_done,
    }
