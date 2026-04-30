import json

from blueprint.brief_glossary import (
    BriefGlossaryEntry,
    brief_glossary_to_dict,
    extract_implementation_brief_glossary,
)
from blueprint.domain.models import ImplementationBrief


def test_explicit_definitions_produce_glossary_entries():
    glossary = extract_implementation_brief_glossary(
        _brief(
            architecture_notes=(
                "Patient Graph: The relationship model connecting patients to care teams.\n"
                "Keep reads deterministic."
            ),
            data_requirements="Care Bundle: A reusable grouping of forms and follow-ups.",
        )
    )

    by_term = _by_term(glossary)

    assert isinstance(glossary[0], BriefGlossaryEntry)
    assert by_term["Patient Graph"].to_dict() == {
        "term": "Patient Graph",
        "definition": "The relationship model connecting patients to care teams.",
        "sources": ["architecture_notes"],
        "occurrence_count": 1,
        "confidence": 1.0,
    }
    assert by_term["Care Bundle"].definition == "A reusable grouping of forms and follow-ups."


def test_acronyms_and_quoted_feature_names_are_detected_with_confidence():
    glossary = extract_implementation_brief_glossary(
        _brief(
            problem_statement="CSAT reviewers need faster triage.",
            mvp_goal='Ship "Escalation Inbox" for CSAT review queues.',
            integration_points=["CRM API"],
        )
    )

    by_term = _by_term(glossary)

    assert by_term["CSAT"].occurrence_count == 2
    assert by_term["CSAT"].confidence == 0.84
    assert by_term["Escalation Inbox"].confidence == 0.85
    assert by_term["Escalation Inbox"].sources == ("mvp_goal",)
    assert by_term["CRM"].confidence == 0.8


def test_repeated_capitalized_domain_terms_are_counted_across_brief_fields():
    glossary = extract_implementation_brief_glossary(
        _brief(
            problem_statement="Intake Queue delays hide urgent work.",
            scope=[
                "Prioritize Intake Queue items by age",
                "Surface Intake Queue owner metadata",
            ],
            validation_plan="Verify Intake Queue ordering in review.",
        )
    )

    entry = _by_term(glossary)["Intake Queue"]

    assert entry.definition is None
    assert entry.sources == (
        "problem_statement",
        "scope.1",
        "scope.2",
        "validation_plan",
    )
    assert entry.occurrence_count == 4
    assert entry.confidence == 0.8


def test_source_brief_text_enriches_sources_and_occurrence_counts():
    glossary = extract_implementation_brief_glossary(
        _brief(
            problem_statement="Route case work through Triage Hub.",
            scope=["Render Triage Hub filters"],
        ),
        source_brief={
            "id": "sb-glossary",
            "title": "Source",
            "summary": "Triage Hub appears in support planning notes.",
            "source_payload": {
                "feature": "Triage Hub",
                "notes": ["Add SLA badges to Triage Hub"],
            },
        },
    )

    entry = _by_term(glossary)["Triage Hub"]

    assert entry.sources == (
        "problem_statement",
        "scope.1",
        "source_brief.summary",
        "source_brief.source_payload.feature",
        "source_brief.source_payload.notes.1",
    )
    assert entry.occurrence_count == 5
    assert entry.confidence == 0.8


def test_duplicate_terms_are_normalized_case_insensitively():
    glossary = extract_implementation_brief_glossary(
        _brief(
            problem_statement="Audit Trail: Records who changed plan state.",
            scope=[
                "Render audit trail filters",
                "Export Audit Trail evidence",
            ],
        )
    )

    entries = [entry for entry in glossary if entry.term == "Audit Trail"]

    assert len(entries) == 1
    assert entries[0].definition == "Records who changed plan state."
    assert entries[0].occurrence_count == 3
    assert entries[0].sources == ("problem_statement", "scope.1", "scope.2")


def test_accepts_implementation_brief_models_and_serializes_stably():
    glossary = extract_implementation_brief_glossary(
        ImplementationBrief.model_validate(
            _brief(
                problem_statement="Ops KPI: A metric reviewed during operations planning.",
                mvp_goal="Expose KPI snapshots.",
            )
        )
    )
    payload = brief_glossary_to_dict(glossary)

    assert payload == [entry.to_dict() for entry in glossary]
    assert list(payload[0]) == [
        "term",
        "definition",
        "sources",
        "occurrence_count",
        "confidence",
    ]
    assert payload[0]["term"] == "Ops KPI"
    assert json.loads(json.dumps(payload)) == payload


def test_empty_or_minimal_briefs_return_empty_glossary():
    assert extract_implementation_brief_glossary({}) == ()
    assert (
        extract_implementation_brief_glossary(
            {
                "problem_statement": "make the thing work",
                "mvp_goal": "ship it",
                "scope": [],
                "risks": [],
                "validation_plan": "test it",
                "definition_of_done": [],
            }
        )
        == ()
    )


def _by_term(glossary):
    return {entry.term: entry for entry in glossary}


def _brief(
    *,
    problem_statement="Care teams need better intake review.",
    mvp_goal="Ship intake review support.",
    scope=None,
    architecture_notes=None,
    data_requirements=None,
    integration_points=None,
    risks=None,
    validation_plan="Run the focused pytest suite.",
    definition_of_done=None,
):
    return {
        "id": "brief-glossary",
        "source_brief_id": "source-glossary",
        "title": "Glossary Brief",
        "problem_statement": problem_statement,
        "mvp_goal": mvp_goal,
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": data_requirements,
        "integration_points": [] if integration_points is None else integration_points,
        "risks": [] if risks is None else risks,
        "validation_plan": validation_plan,
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
    }
