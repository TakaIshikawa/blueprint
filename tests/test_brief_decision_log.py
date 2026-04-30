import json

from blueprint.brief_decision_log import (
    BriefDecisionRecord,
    brief_decision_log_to_dicts,
    extract_brief_decision_log,
)
from blueprint.domain.models import ImplementationBrief


def test_extracts_explicit_decisions_from_brief_fields():
    decisions = extract_brief_decision_log(
        _brief(
            architecture_notes="Decision: use a repository adapter for persistence.",
            data_requirements="Must store audit events with actor_id and timestamp fields.",
            definition_of_done=["Validate rollback through a smoke test."],
        )
    )

    assert decisions[:3] == (
        BriefDecisionRecord(
            decision_id="decision-architecture-decision-use-a-repository-adapter-for-persistence",
            category="architecture",
            decision_text="Decision: use a repository adapter for persistence.",
            source_field="architecture_notes",
            evidence=("architecture_notes: Decision: use a repository adapter for persistence.",),
            confidence="high",
        ),
        BriefDecisionRecord(
            decision_id="decision-data-store-audit-events-with-actor-id-and-timestamp-fields",
            category="data",
            decision_text="Must store audit events with actor_id and timestamp fields.",
            source_field="data_requirements",
            evidence=(
                "data_requirements: Must store audit events with actor_id and timestamp fields.",
            ),
            confidence="high",
        ),
        BriefDecisionRecord(
            decision_id="decision-validation-validate-rollback-through-a-smoke-test",
            category="validation",
            decision_text="Validate rollback through a smoke test.",
            source_field="definition_of_done[0]",
            evidence=("definition_of_done[0]: Validate rollback through a smoke test.",),
            confidence="medium",
        ),
    )


def test_non_goals_produce_scope_exclusion_decisions():
    decisions = extract_brief_decision_log(
        _brief(non_goals=["Realtime collaboration", "Do not include billing changes."])
    )

    scope_decisions = [
        decision for decision in decisions if decision.source_field.startswith("non_goals")
    ]

    assert [
        (decision.category, decision.decision_text, decision.confidence)
        for decision in scope_decisions
    ] == [
        ("scope", "Exclude realtime collaboration", "high"),
        ("scope", "Do not include billing changes.", "high"),
    ]


def test_repeated_decisions_are_deduplicated_while_retaining_evidence():
    decisions = extract_brief_decision_log(
        _brief(
            architecture_notes="We will use Redis for caching.",
            assumptions=["Use Redis for caching."],
            scope=["Use Redis for caching."],
        )
    )

    redis_decisions = [
        decision
        for decision in decisions
        if decision.decision_text == "We will use Redis for caching."
    ]

    assert len(redis_decisions) == 1
    assert redis_decisions[0].source_field == "architecture_notes"
    assert redis_decisions[0].evidence == (
        "architecture_notes: We will use Redis for caching.",
        "scope[0]: Use Redis for caching.",
        "assumptions[0]: Use Redis for caching.",
    )
    assert redis_decisions[0].confidence == "high"


def test_empty_briefs_return_no_decisions():
    assert extract_brief_decision_log({}) == ()


def test_accepts_mapping_and_model_inputs_and_serializes_stably():
    mapping = _brief(
        scope=["Build import preview"],
        non_goals=[],
        assumptions=["Assume operators can retry failed imports."],
    )
    model = ImplementationBrief.model_validate(mapping)

    mapping_payload = brief_decision_log_to_dicts(extract_brief_decision_log(mapping))
    model_payload = brief_decision_log_to_dicts(extract_brief_decision_log(model))

    assert mapping_payload == model_payload
    assert mapping_payload == [
        {
            "id": "decision-scope-build-import-preview",
            "category": "scope",
            "decision_text": "Build import preview",
            "source_field": "scope[0]",
            "evidence": ["scope[0]: Build import preview"],
            "confidence": "medium",
        },
        {
            "id": "decision-operations-assume-operators-can-retry-failed-imports",
            "category": "operations",
            "decision_text": "Assume operators can retry failed imports.",
            "source_field": "assumptions[0]",
            "evidence": ["assumptions[0]: Assume operators can retry failed imports."],
            "confidence": "medium",
        },
    ]
    assert list(mapping_payload[0]) == [
        "id",
        "category",
        "decision_text",
        "source_field",
        "evidence",
        "confidence",
    ]
    assert json.loads(json.dumps(mapping_payload)) == mapping_payload


def _brief(
    *,
    architecture_notes=None,
    data_requirements=None,
    scope=None,
    non_goals=None,
    assumptions=None,
    definition_of_done=None,
):
    return {
        "id": "brief-decision-log",
        "source_brief_id": "source-decision-log",
        "title": "Decision Log Brief",
        "problem_statement": "Execution agents need settled implementation choices.",
        "mvp_goal": "Extract brief decisions for planning.",
        "scope": [] if scope is None else scope,
        "non_goals": [] if non_goals is None else non_goals,
        "assumptions": [] if assumptions is None else assumptions,
        "architecture_notes": architecture_notes,
        "data_requirements": data_requirements,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run focused tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
    }
