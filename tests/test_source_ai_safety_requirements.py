import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_ai_safety_requirements import (
    SourceAISafetyRequirement,
    SourceAISafetyRequirementsReport,
    build_source_ai_safety_requirements,
    derive_source_ai_safety_requirements,
    extract_source_ai_safety_requirements,
    generate_source_ai_safety_requirements,
    source_ai_safety_requirements_to_dict,
    source_ai_safety_requirements_to_dicts,
    source_ai_safety_requirements_to_markdown,
    summarize_source_ai_safety_requirements,
)


def test_user_facing_ai_feature_extracts_safety_categories_with_planning_notes():
    result = build_source_ai_safety_requirements(
        _source_brief(
            summary=(
                "Customer-facing AI assistant answers billing questions. Responses must cite sources, "
                "stay within billing scope, refuse legal or financial advice, and fall back to search "
                "results on low confidence or provider error."
            ),
            source_payload={
                "ai_safety": [
                    "Prompt injection defenses must treat retrieved content as untrusted and block tool instructions.",
                    "PII and customer data must be redacted before prompts are logged.",
                    "Evaluation dataset needs golden prompts, adversarial prompts, and expected answers.",
                    "AI audit logging must capture prompt version, model version, response, and refusal decision.",
                ]
            },
        )
    )

    assert isinstance(result, SourceAISafetyRequirementsReport)
    assert all(isinstance(record, SourceAISafetyRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "model_output_boundaries",
        "evaluation_dataset",
        "sensitive_data_handling",
        "prompt_injection_defense",
        "safety_refusal_policy",
        "audit_logging",
        "fallback_behavior",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["prompt_injection_defense"].suggested_owner == "security"
    assert by_category["sensitive_data_handling"].suggested_owner == "security_privacy"
    assert "untrusted-content treatment" in by_category["prompt_injection_defense"].planning_note
    assert "source_payload.ai_safety[0]" in by_category["prompt_injection_defense"].source_field_paths
    assert "prompt injection" in {term.casefold() for term in by_category["prompt_injection_defense"].matched_terms}
    assert any("low confidence" in evidence for evidence in by_category["fallback_behavior"].evidence)
    assert result.summary["requirement_count"] == 7
    assert result.summary["category_counts"]["audit_logging"] == 1
    assert result.summary["confidence_counts"]["high"] == 6
    assert result.summary["confidence_counts"]["medium"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_internal_copilot_human_review_and_structured_metadata_are_extracted():
    result = extract_source_ai_safety_requirements(
        _source_brief(
            title="Support copilot",
            summary="Internal LLM copilot drafts support replies for agents.",
            source_payload={
                "ai_controls": {
                    "human_review": "Agents must review before send and approve every model-generated reply.",
                    "output_boundaries": "The copilot should only answer from the account knowledge base.",
                    "fallbacks": "On model timeout, handoff to human support with a safe default draft.",
                }
            },
            metadata={
                "ai_safety": {
                    "audit_logging": "Usage logging records who approved each response.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}
    assert [record.category for record in result.records] == [
        "model_output_boundaries",
        "human_review",
        "audit_logging",
        "fallback_behavior",
    ]
    assert by_category["human_review"].confidence == "high"
    assert by_category["human_review"].suggested_owner == "operations"
    assert "source_payload.ai_controls.human_review" in by_category["human_review"].source_field_paths
    assert "metadata.ai_safety.audit_logging" in by_category["audit_logging"].source_field_paths
    assert "review triggers" in by_category["human_review"].suggested_planning_note


def test_generic_automation_and_negated_ai_scope_do_not_create_records():
    automation = build_source_ai_safety_requirements(
        _source_brief(
            title="Automation rules",
            summary=(
                "Automated workflow sends renewal reminders, applies template output boundaries, "
                "logs decisions, and falls back to email retry on timeout."
            ),
            source_payload={
                "requirements": [
                    "Human review is required for billing credits over 500 dollars.",
                    "Evaluation dataset covers invoice tax examples.",
                ]
            },
        )
    )
    negated = build_source_ai_safety_requirements(
        _source_brief(
            title="Profile search",
            summary="No AI, LLM, or model-generated feature is in scope for this release.",
            source_payload={
                "requirements": [
                    "Audit logs are unchanged.",
                    "Fallback behavior remains existing search pagination.",
                ]
            },
        )
    )
    malformed = build_source_ai_safety_requirements(object())

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "model_output_boundaries": 0,
            "human_review": 0,
            "evaluation_dataset": 0,
            "sensitive_data_handling": 0,
            "prompt_injection_defense": 0,
            "safety_refusal_policy": 0,
            "audit_logging": 0,
            "fallback_behavior": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "owner_counts": {
            "compliance": 0,
            "engineering": 0,
            "ml_evaluation": 0,
            "operations": 0,
            "product": 0,
            "security": 0,
            "security_privacy": 0,
            "trust_and_safety": 0,
        },
        "categories": [],
        "status": "no_ai_safety_language",
    }
    assert automation.records == ()
    assert automation.summary == expected_summary
    assert negated.records == ()
    assert negated.summary == expected_summary
    assert malformed.records == ()
    assert "No source AI safety requirements were inferred" in automation.to_markdown()


def test_model_input_serialization_aliases_and_no_mutation_are_stable():
    source = _source_brief(
        source_id="ai-safety-model",
        summary="LLM answer generator must use a refusal policy for unsafe requests.",
        source_payload={
            "requirements": [
                "Sensitive customer data must be masked before prompt construction.",
                "Prompt regression eval set must include jailbreak prompts.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_ai_safety_requirements(source)
    model_result = generate_source_ai_safety_requirements(model)
    derived = derive_source_ai_safety_requirements(model)
    payload = source_ai_safety_requirements_to_dict(model_result)
    markdown = source_ai_safety_requirements_to_markdown(model_result)

    assert source == original
    assert model_result.to_dict() == mapping_result.to_dict()
    assert derived.to_dict() == mapping_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert source_ai_safety_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_ai_safety_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_ai_safety_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "evidence",
        "source_field_paths",
        "matched_terms",
        "confidence",
        "suggested_owner",
        "suggested_planning_note",
    ]
    assert markdown.startswith("# Source AI Safety Requirements Report: ai-safety-model")


def test_multiple_sources_duplicate_merging_and_repeated_runs_have_stable_order():
    sources = [
        _source_brief(
            source_id="brief-b",
            summary="LLM copilot requires human review before publish. Human review before publish is required.",
        ),
        _source_brief(
            source_id="brief-a",
            summary="AI assistant must define prompt injection defenses and audit logging.",
        ),
    ]

    first = build_source_ai_safety_requirements(sources)
    second = build_source_ai_safety_requirements(copy.deepcopy(sources))

    assert first.to_dict() == second.to_dict()
    assert [(record.source_brief_id, record.category) for record in first.records] == [
        ("brief-a", "prompt_injection_defense"),
        ("brief-a", "audit_logging"),
        ("brief-b", "human_review"),
    ]
    human_review = next(record for record in first.records if record.category == "human_review")
    assert len(human_review.evidence) == 2
    assert human_review.evidence == tuple(sorted(human_review.evidence, key=str.casefold))
    assert first.source_id is None


def _source_brief(
    *,
    source_id="ai-safety-source",
    title="AI safety requirements",
    domain="product",
    summary="General AI safety requirements.",
    source_payload=None,
    metadata=None,
):
    payload = {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }
    if metadata is not None:
        payload["metadata"] = metadata
    return payload
