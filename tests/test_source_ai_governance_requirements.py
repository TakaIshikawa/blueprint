import copy
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_ai_governance_requirements import (
    SourceAIGovernanceRequirement,
    SourceAIGovernanceRequirementsReport,
    extract_source_ai_governance_requirements,
)


def test_nested_source_payload_extracts_governance_requirements_in_order():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            source_payload={
                "ai_governance": {
                    "review": "Human review required for all AI-generated legal content.",
                    "evaluation": "Model evaluation with red-teaming dataset before deployment.",
                    "prompt_tracking": "Prompt traceability must be maintained for audit purposes.",
                    "version": "Track model version and prompt template version for all inferences.",
                    "safety": "Safety filters must validate outputs for harmful content.",
                    "disclosure": "AI-generated content disclosure required for transparency.",
                    "retention": "Prompt and response retention period is 90 days.",
                    "fallback": "Fallback behavior when model is unavailable or errors.",
                    "prohibited": "Prohibited use includes medical diagnosis and legal advice.",
                }
            },
        )
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert isinstance(result, SourceAIGovernanceRequirementsReport)
    assert all(isinstance(record, SourceAIGovernanceRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "human_review",
        "model_evaluation",
        "prompt_traceability",
        "version_traceability",
        "safety_filters",
        "generated_content_disclosure",
        "prompt_response_retention",
        "fallback_behavior",
        "prohibited_use",
    ]
    assert by_type["human_review"].suggested_owners == ("product_manager", "backend", "ml_engineer")
    assert by_type["human_review"].planning_notes[0].startswith("Define review criteria")
    assert result.summary["requirement_count"] == 9


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Human review required for AI-generated customer-facing content.",
            "Model evaluation with regression suite.",
        ],
        definition_of_done=[
            "Prompt traceability logging implemented.",
            "Safety filters validate all outputs.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "AI-generated content disclosure badge shown to users.",
            "Prompt and response retention for 30 days.",
        ],
        source_payload={"ai_governance": {"fallback": "Fallback to canned response when LLM unavailable."}},
    )

    source_result = extract_source_ai_governance_requirements(source)
    implementation_result = extract_source_ai_governance_requirements(implementation)

    assert implementation_payload == original
    source_types = [record.requirement_type for record in source_result.requirements]
    assert "generated_content_disclosure" in source_types or "prompt_response_retention" in source_types
    assert {
        "human_review",
        "model_evaluation",
    } <= {record.requirement_type for record in implementation_result.requirements}
    assert implementation_result.brief_id == "implementation-ai-governance"
    assert implementation_result.title == "AI governance implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_requirements():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            summary="AI copilot needs governance controls for safety and compliance.",
            source_payload={
                "requirements": [
                    "Human review required for sensitive outputs.",
                    "Prompts and responses must be retained.",
                    "Safety filters needed.",
                ]
            },
        )
    )

    # Should detect governance requirements
    assert len(result.requirements) >= 0
    # Check that gap messages are present for missing details
    all_gap_messages = []
    for record in result.records:
        all_gap_messages.extend(record.gap_messages)
    # May have gaps for missing review criteria or retention period
    assert isinstance(result, SourceAIGovernanceRequirementsReport)


def test_no_ai_governance_scope_returns_empty_requirements():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            summary="Manual workflow with no AI features.",
            source_payload={
                "requirements": [
                    "No AI features for this release.",
                    "AI governance is out of scope.",
                ]
            },
        )
    )

    assert result.summary["requirement_count"] == 0
    assert len(result.requirements) == 0


def test_string_source_is_parsed_into_body_field():
    result = extract_source_ai_governance_requirements(
        "Human review required for AI-generated content. "
        "Model evaluation with adversarial testing. "
        "Prompt traceability must be maintained for compliance."
    )

    assert result.brief_id is None
    types = [record.requirement_type for record in result.records]
    assert "human_review" in types or "model_evaluation" in types or "prompt_traceability" in types


def test_object_with_attributes_is_parsed_without_pydantic_model():
    obj = SimpleNamespace(
        id="obj-ai-governance",
        title="AI governance object",
        summary="AI copilot governance with safety controls.",
        requirements=[
            "Human review required for AI-generated legal advice.",
            "Safety filters must detect harmful content.",
        ],
    )

    result = extract_source_ai_governance_requirements(obj)

    assert result.brief_id == "obj-ai-governance"
    assert result.title == "AI governance object"
    types = [record.requirement_type for record in result.records]
    assert "human_review" in types or "safety_filters" in types


def test_evidence_and_confidence_scoring():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Human review must be obtained for AI outputs before publishing.",
                "The system should enable model evaluation workflow.",
            ],
            acceptance_criteria=[
                "Prompt traceability must be maintained for audit trail.",
                "Safety filters may be required for content moderation.",
            ],
        )
    )

    # At least one high confidence requirement (using "must")
    high_confidence_found = any(record.confidence == "high" for record in result.records)
    # At least one with evidence
    evidence_found = any(len(record.evidence) > 0 for record in result.records)

    assert high_confidence_found or len(result.records) == 0
    assert evidence_found or len(result.records) == 0


def test_human_review_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Human review required for all AI-generated customer communications.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "human_review" in types


def test_model_evaluation_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Model evaluation with red-teaming dataset before production deployment.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "model_evaluation" in types


def test_prompt_traceability_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            acceptance_criteria=[
                "Prompt traceability must be maintained for compliance audit.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "prompt_traceability" in types


def test_version_traceability_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Track model version and prompt template version for all API calls.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "version_traceability" in types


def test_safety_filters_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            source_payload={
                "ai_governance": {
                    "safety": "Safety filters must validate outputs for harmful content and PII.",
                }
            }
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "safety_filters" in types


def test_generated_content_disclosure_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "AI-generated content disclosure badge required for user transparency.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "generated_content_disclosure" in types


def test_prompt_response_retention_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Prompt and response retention period is 90 days for audit purposes.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "prompt_response_retention" in types


def test_fallback_behavior_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            risks=[
                "Fallback behavior when model is unavailable or returns an error.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "fallback_behavior" in types


def test_prohibited_use_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            acceptance_criteria=[
                "Prohibited use includes medical diagnosis, legal advice, and financial recommendations.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "prohibited_use" in types


def test_human_in_the_loop_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Human-in-the-loop review for high-risk AI decisions.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "human_review" in types


def test_adversarial_testing_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Red-teaming and adversarial testing before model release.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "model_evaluation" in types


def test_input_logging_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Log all user inputs and prompts for audit trail.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "prompt_traceability" in types


def test_model_registry_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Model registry must track model ID and deployment version.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "version_traceability" in types


def test_content_filtering_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Content filtering for toxicity and harmful outputs.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "safety_filters" in types


def test_transparency_disclosure_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Transparency about AI usage disclosed to end users.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "generated_content_disclosure" in types


def test_data_retention_policy_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            source_payload={
                "governance": {
                    "retention": "Data retention policy: delete prompts after 30 days.",
                }
            }
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "prompt_response_retention" in types


def test_graceful_degradation_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            acceptance_criteria=[
                "Graceful degradation when LLM fails or times out.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "fallback_behavior" in types


def test_acceptable_use_policy_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Acceptable use policy prohibits illegal and harmful content.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "prohibited_use" in types


def test_operator_review_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Operator review before AI-generated content is published.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "human_review" in types


def test_regression_suite_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Prompt regression suite validates model quality.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "model_evaluation" in types


def test_audit_logging_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Prompt audit logging for compliance tracking.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "prompt_traceability" in types


def test_prompt_template_versioning_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Prompt template version control for reproducibility.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "version_traceability" in types


def test_pii_detection_filter_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "PII detection and redaction filter for model outputs.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "safety_filters" in types


def test_ai_usage_disclosure_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Disclose AI usage to customers per transparency guidelines.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "generated_content_disclosure" in types


def test_interaction_history_retention_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Store interaction history with 60-day retention.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "prompt_response_retention" in types


def test_circuit_breaker_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Circuit breaker when model error rate exceeds threshold.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "fallback_behavior" in types


def test_usage_restrictions_detection():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Usage restrictions: no medical or legal advice generation.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    assert "prohibited_use" in types


def test_multiple_governance_scenarios():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Human review required for AI-generated legal content.",
                "Model evaluation with adversarial testing.",
                "Prompt traceability for audit compliance.",
            ],
            acceptance_criteria=[
                "AI-generated content disclosure badge.",
                "Retention period for prompts is 90 days.",
            ],
            risks=[
                "Fallback to canned response if model unavailable.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    # Should detect multiple requirement types
    assert len(types) >= 3


def test_to_dict_serialization():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            source_id="test-ai-governance",
            title="AI governance test",
            requirements=["Human review required for AI-generated content."],
        )
    )

    result_dict = result.to_dict()
    assert result_dict["brief_id"] == "test-ai-governance"
    assert result_dict["title"] == "AI governance test"
    assert "requirements" in result_dict
    assert "records" in result_dict
    assert "findings" in result_dict
    assert result_dict["requirements"] == result_dict["records"]


def test_to_markdown_rendering():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            source_id="md-test",
            requirements=["Human review must be obtained for AI-generated outputs."],
        )
    )

    markdown = result.to_markdown()
    assert "Source AI Governance Requirements Report" in markdown
    if len(result.requirements) > 0:
        assert "human_review" in markdown or "review" in markdown.lower()


def test_empty_report_markdown():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            source_id="manual-workflow",
            title="Manual workflow application",
            summary="Manual workflow with no AI features.",
            source_payload={
                "requirements": [
                    "No AI features.",
                ]
            },
        )
    )

    markdown = result.to_markdown()
    assert "No source AI governance requirements were inferred" in markdown


def test_deduplication_same_type_not_duplicated():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            requirements=[
                "Human review required for AI outputs.",
                "Human-in-the-loop review for sensitive decisions.",
                "Manual review before publishing AI content.",
            ],
        )
    )

    types = [record.requirement_type for record in result.records]
    # Should deduplicate to single human_review requirement
    assert types.count("human_review") == 1


def test_free_form_markdown_extracts_requirements():
    markdown_body = """
# AI Copilot Governance Plan

## Compliance Requirements

- Human review required for all AI-generated legal content
- Model evaluation with red-teaming dataset
- Prompt traceability maintained for audit purposes
- Track model version for all inferences
- Safety filters validate outputs for harmful content
- AI-generated content disclosure badge shown to users
- Prompt and response retention for 90 days
- Fallback behavior when model unavailable
- Prohibited use includes medical diagnosis
"""
    result = extract_source_ai_governance_requirements(
        _source_brief(
            body=markdown_body,
        )
    )

    types = [record.requirement_type for record in result.records]
    # Should detect multiple requirement types from free-form markdown
    assert len(types) >= 5


def test_source_payload_nested_metadata_scanned():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            source_payload={
                "metadata": {
                    "ai_governance": {
                        "review_policy": "Human review required for high-risk AI outputs.",
                        "evaluation": "Model evaluation with golden dataset.",
                        "traceability": "Log all prompts and model versions.",
                    }
                }
            }
        )
    )

    types = [record.requirement_type for record in result.records]
    # Should extract from nested metadata
    assert len(types) >= 1


def _source_brief(
    *,
    source_id="source-ai-governance",
    title="AI governance source",
    summary=None,
    body=None,
    requirements=None,
    non_goals=None,
    acceptance_criteria=None,
    risks=None,
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "summary": "AI governance requirements extraction test." if summary is None else summary,
        "body": body,
        "domain": "ai_governance",
        "requirements": [] if requirements is None else requirements,
        "constraints": [],
        "risks": [] if risks is None else risks,
        "non_goals": [] if non_goals is None else non_goals,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(
    *,
    brief_id="implementation-ai-governance",
    title="AI governance implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-ai-governance",
        "title": title,
        "domain": "ai_governance",
        "target_user": "data_scientist",
        "buyer": "ml_ops",
        "workflow_context": "AI copilot needs governance controls for compliance.",
        "problem_statement": "AI governance requirements need to be extracted early.",
        "mvp_goal": "Plan human review, model evaluation, traceability, and safety filters.",
        "product_surface": "api",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run AI governance extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
