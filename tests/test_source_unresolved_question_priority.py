import json

from blueprint.domain.models import SourceBrief
from blueprint.source_unresolved_question_priority import (
    SourceUnresolvedQuestionPriorityRecord,
    SourceUnresolvedQuestionPriorityReport,
    build_source_unresolved_question_priority,
    derive_source_unresolved_question_priority,
    source_unresolved_question_priority_to_dict,
    source_unresolved_question_priority_to_markdown,
)


def test_structured_questions_are_ranked_by_implementation_blocking_impact():
    result = build_source_unresolved_question_priority(
        _source_brief(
            source_payload={
                "open_questions": [
                    {
                        "question": (
                            "Which auth permission model is required before rollout?"
                        )
                    },
                    "Who should review the support copy?",
                    {
                        "text": (
                            "What database migration and backfill evidence is required "
                            "for acceptance?"
                        )
                    },
                ],
                "acceptance_criteria": [
                    "Acceptance is blocked until production rollout and data migration details are known."
                ],
                "risks": ["Security approval may block launch."],
            }
        )
    )

    assert [record.question for record in result.records] == [
        "What database migration and backfill evidence is required for acceptance?",
        "Which auth permission model is required before rollout?",
        "Who should review the support copy?",
    ]
    assert [record.priority for record in result.records] == ["high", "high", "low"]
    assert result.records[0].suggested_owner_role == "data_owner"
    assert result.records[1].suggested_owner_role == "security_owner"
    assert "Concrete acceptance criteria and pass/fail examples." in result.records[0].evidence_needed
    assert "Security requirements, approval notes, and threat or permission model." in (
        result.records[1].evidence_needed
    )
    assert result.records[2].score < result.records[1].score
    assert all(record.rationale for record in result.records)
    assert all(isinstance(record, SourceUnresolvedQuestionPriorityRecord) for record in result.records)


def test_questions_embedded_in_text_are_extracted_and_deduped_deterministically():
    result = build_source_unresolved_question_priority(
        _source_brief(
            source_payload={
                "normalized": {
                    "summary": (
                        "Question: Which external API contract blocks checkout planning? "
                        "Can analytics use the existing purchase event?"
                    ),
                    "risks": [
                        "Which external API contract blocks checkout planning?",
                        "",
                        "   ",
                    ],
                    "constraints": [
                        "Can analytics use the existing purchase event?",
                    ],
                },
                "questions": [
                    "",
                    "Which external API contract blocks checkout planning?",
                ],
            }
        )
    )

    assert [record.question for record in result.records] == [
        "Which external API contract blocks checkout planning?",
        "Can analytics use the existing purchase event?",
    ]
    assert result.records[0].suggested_owner_role == "technical_lead"
    assert result.records[1].suggested_owner_role == "data_owner"
    assert result.records[0].source_fields == (
        "source_payload.questions",
        "source_payload.normalized.risks",
        "source_payload.normalized.summary",
    )
    assert result.records[1].source_fields == (
        "source_payload.normalized.constraints",
        "source_payload.normalized.summary",
    )


def test_no_question_briefs_return_empty_report_and_markdown():
    result = build_source_unresolved_question_priority(
        _source_brief(
            summary="Checkout retry flow is ready for planning.",
            source_payload={
                "acceptance_criteria": [
                    "Retry action preserves the cart and records an audit event."
                ],
                "risks": ["Low operational risk."],
            },
        )
    )

    assert result.records == ()
    assert result.to_dict() == {"records": []}
    assert result.to_markdown() == "\n".join(
        [
            "# Source Unresolved Question Priority",
            "",
            "No unresolved source questions found.",
        ]
    )


def test_model_inputs_aliases_dict_serialization_and_markdown_are_stable():
    source_brief = SourceBrief.model_validate(
        _source_brief(
            source_payload={
                "questions": [
                    "What acceptance metric proves release readiness?",
                ],
                "metadata": {
                    "rollout": "Production rollout cannot start until readiness is defined."
                },
            }
        )
    )

    result = build_source_unresolved_question_priority(source_brief)
    alias_result = derive_source_unresolved_question_priority([source_brief])
    payload = source_unresolved_question_priority_to_dict(result)

    assert isinstance(result, SourceUnresolvedQuestionPriorityReport)
    assert payload == result.to_dict()
    assert alias_result.to_dict() == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["records"]
    assert list(payload["records"][0]) == [
        "source_brief_id",
        "question",
        "priority",
        "score",
        "score_components",
        "rationale",
        "suggested_owner_role",
        "evidence_needed",
        "source_fields",
    ]
    assert payload["records"][0]["score_components"] == {
        "implementation_impact": 4,
        "blocker_likelihood": 2,
        "affected_scope": 1,
        "acceptance_ambiguity": 3,
        "dependency_risk": 1,
    }
    assert source_unresolved_question_priority_to_markdown(result) == "\n".join(
        [
            "# Source Unresolved Question Priority",
            "",
            "| Priority | Score | Question | Owner | Evidence Needed | Rationale |",
            "| --- | --- | --- | --- | --- | --- |",
            (
                "| high | 11 | What acceptance metric proves release readiness? | "
                "release_manager | Concrete acceptance criteria and pass/fail examples.; "
                "Data contract, schema, migration, retention, or metric definition.; "
                "Rollout plan, environment target, rollback path, and launch window.; "
                "Source field: source_payload.questions | Ranked from acceptance ambiguity, "
                "data impact, rollout impact; strongest score component is "
                "implementation impact. |"
            ),
        ]
    )


def _source_brief(
    *,
    source_id="sb-unresolved-priority",
    title="Checkout Retry",
    domain="payments",
    summary="Retry failed payment submissions.",
    source_payload=None,
    source_links=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }
