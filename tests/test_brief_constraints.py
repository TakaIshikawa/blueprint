import copy
import json

from blueprint.brief_constraints import (
    ImplementationConstraint,
    extract_implementation_constraints,
    implementation_constraints_to_dicts,
)
from blueprint.domain.models import ImplementationBrief


def test_constraints_are_extracted_from_supported_fields_without_mutating_input():
    brief = _brief(
        assumptions=["Use existing retry helpers"],
        non_goals=["Do not add a hosted dashboard"],
        architecture_notes="Keep the command module small.\nReuse store access patterns.",
        data_requirements="Execution plan rows",
        integration_points=["CLI export command"],
        risks=["Backfill may be slow"],
        validation_plan="Run focused pytest coverage.",
    )
    original = copy.deepcopy(brief)

    constraints = extract_implementation_constraints(brief)

    assert brief == original
    assert [
        (item.category, item.source_field, item.text, item.severity) for item in constraints
    ] == [
        ("technical", "assumptions", "Use existing retry helpers", "medium"),
        ("scope", "non_goals", "Do not add a hosted dashboard", "medium"),
        ("technical", "architecture_notes", "Keep the command module small.", "medium"),
        ("technical", "architecture_notes", "Reuse store access patterns.", "medium"),
        ("data", "data_requirements", "Execution plan rows", "medium"),
        ("integration", "integration_points", "CLI export command", "medium"),
        ("technical", "risks", "Backfill may be slow", "medium"),
        ("validation", "validation_plan", "Run focused pytest coverage.", "medium"),
    ]
    assert all(isinstance(item, ImplementationConstraint) for item in constraints)


def test_security_data_and_integration_keywords_raise_severity_above_default():
    constraints = extract_implementation_constraints(
        _brief(
            assumptions=[
                "OAuth tokens must never be logged",
                "Schema migration remains backward compatible",
                "Webhook delivery is retried by the provider",
            ],
            risks=["Permissions can expose private exports"],
        )
    )

    assert [(item.category, item.text, item.severity) for item in constraints] == [
        ("security", "OAuth tokens must never be logged", "high"),
        ("data", "Schema migration remains backward compatible", "high"),
        ("integration", "Webhook delivery is retried by the provider", "high"),
        ("security", "Permissions can expose private exports", "high"),
        ("validation", "Run the focused pytest suite.", "medium"),
    ]


def test_duplicate_constraint_text_is_deduplicated_deterministically():
    constraints = extract_implementation_constraints(
        _brief(
            assumptions=["Shared retry limit", "Unique assumption"],
            architecture_notes="shared retry limit",
            risks=["Shared retry limit", "Unique risk"],
            validation_plan="Unique risk",
        )
    )

    assert [(item.source_field, item.text) for item in constraints] == [
        ("assumptions", "Shared retry limit"),
        ("assumptions", "Unique assumption"),
        ("risks", "Unique risk"),
    ]


def test_model_inputs_and_empty_fields_serialize_cleanly():
    constraints = extract_implementation_constraints(
        ImplementationBrief.model_validate(
            _brief(
                assumptions=["  Persist audit data  "],
                non_goals=[],
                architecture_notes=None,
                data_requirements=None,
                integration_points=[],
                risks=[],
                validation_plan="Run JSON serialization checks.",
            )
        )
    )
    payload = implementation_constraints_to_dicts(constraints)

    assert payload == [item.to_dict() for item in constraints]
    assert payload == [
        {
            "category": "data",
            "source_field": "assumptions",
            "text": "Persist audit data",
            "severity": "high",
        },
        {
            "category": "validation",
            "source_field": "validation_plan",
            "text": "Run JSON serialization checks.",
            "severity": "medium",
        },
    ]
    assert list(payload[0]) == ["category", "source_field", "text", "severity"]
    assert json.loads(json.dumps(payload)) == payload


def test_plain_dicts_can_be_partial_and_non_string_values_are_ignored():
    assert extract_implementation_constraints(
        {
            "assumptions": ["Keep text", 42, None],
            "data_requirements": {"ignored": "mapping"},
            "validation_plan": "",
        }
    ) == (
        ImplementationConstraint(
            category="technical",
            source_field="assumptions",
            text="Keep text",
            severity="medium",
        ),
    )


def _brief(
    *,
    assumptions=None,
    non_goals=None,
    architecture_notes=None,
    data_requirements=None,
    integration_points=None,
    risks=None,
    validation_plan="Run the focused pytest suite.",
):
    return {
        "id": "brief-constraints",
        "source_brief_id": "source-constraints",
        "title": "Constraint Brief",
        "problem_statement": "Operators need safer execution planning.",
        "mvp_goal": "Extract constraints for planning.",
        "scope": ["Build the extractor"],
        "non_goals": [] if non_goals is None else non_goals,
        "assumptions": [] if assumptions is None else assumptions,
        "architecture_notes": architecture_notes,
        "data_requirements": data_requirements,
        "integration_points": [] if integration_points is None else integration_points,
        "risks": [] if risks is None else risks,
        "validation_plan": validation_plan,
        "definition_of_done": ["Constraints are available to execution agents"],
    }
