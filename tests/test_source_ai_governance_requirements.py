import copy
import json
from types import SimpleNamespace
from typing import Any

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_ai_governance_requirements import (
    SourceAIGovernanceRequirement,
    SourceAIGovernanceRequirementsReport,
    build_source_ai_governance_requirements,
    derive_source_ai_governance_requirements,
    extract_source_ai_governance_requirements,
    generate_source_ai_governance_requirements,
    source_ai_governance_requirements_to_dict,
    source_ai_governance_requirements_to_dicts,
    source_ai_governance_requirements_to_markdown,
    summarize_source_ai_governance_requirements,
)


def test_structured_source_brief_extracts_all_ai_governance_categories_in_order():
    result = build_source_ai_governance_requirements(
        _source_brief(
            source_payload={
                "ai_governance": {
                    "review": "Human review required before publishing high-risk AI answers.",
                    "disclosure": "AI-generated content disclosure badge must be shown to users.",
                    "retention": "Prompt and response retention period is 90 days.",
                    "training": "Customer prompts must be excluded from model training.",
                    "evaluation": "Model evaluation criteria include golden dataset accuracy above 95%.",
                    "safety": "Bias review and safety guardrails are required before launch.",
                    "fallback": "Fallback behavior when the LLM is unavailable returns a canned answer.",
                    "audit": "Audit trail must log prompts, outputs, and model version.",
                    "decisions": "AI must not make final loan eligibility automated decisions.",
                }
            }
        )
    )

    assert isinstance(result, SourceAIGovernanceRequirementsReport)
    assert result.brief_id == "source-ai-governance"
    assert all(isinstance(record, SourceAIGovernanceRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "human_review",
        "model_disclosure",
        "prompt_data_retention",
        "training_data_exclusion",
        "evaluation_criteria",
        "bias_safety_review",
        "fallback_behavior",
        "auditability",
        "prohibited_automated_decisions",
    ]
    assert result.summary["requirement_count"] == 9
    assert result.summary["status"] == "complete"
    assert result.summary["type_counts"]["training_data_exclusion"] == 1


def test_implementation_brief_mapping_object_and_string_inputs_are_supported_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Human-in-the-loop review is required for sensitive AI outputs.",
            "Model evaluation criteria must include regression prompts.",
        ],
        definition_of_done=[
            "AI disclosure copy is visible in the response UI.",
            "Prompt audit logs include model identifier and prompt template version.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    obj = SimpleNamespace(
        id="object-ai-governance",
        title="Object AI governance",
        summary="AI copilot governance",
        requirements=[
            "Safety review checks bias and harmful output risks.",
            "Do not train on customer prompts.",
        ],
    )

    implementation_result = extract_source_ai_governance_requirements(implementation)
    mapping_result = derive_source_ai_governance_requirements(implementation_payload)
    object_result = generate_source_ai_governance_requirements(obj)
    string_result = build_source_ai_governance_requirements(
        "AI must not make final hiring automated decisions without human approval."
    )

    assert implementation_payload == original
    assert implementation_result.to_dict() == mapping_result.to_dict()
    assert {
        "human_review",
        "model_disclosure",
        "evaluation_criteria",
        "auditability",
    } <= set(implementation_result.summary["requirement_types"])
    assert object_result.brief_id == "object-ai-governance"
    assert {"bias_safety_review", "training_data_exclusion"} <= set(object_result.summary["requirement_types"])
    assert string_result.brief_id is None
    assert string_result.summary["requirement_types"] == ["human_review", "prohibited_automated_decisions"]


def test_missing_detail_flags_status_and_gap_messages_are_stable():
    result = extract_source_ai_governance_requirements(
        _source_brief(
            source_payload={
                "requirements": [
                    "Human review required for AI-generated legal summaries.",
                    "Prompts and model data must be retained.",
                ]
            }
        )
    )

    assert result.summary["status"] == "needs_detail"
    assert result.summary["missing_detail_flags"] == [
        "missing_review_policy",
        "missing_retention_training_policy",
    ]
    assert result.summary["gap_messages"] == [
        "Specify human review policy, triggers, owners, or approval criteria.",
        "Define prompt/data retention period and whether source data is excluded from model training.",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["human_review"].gap_messages == (
        "Specify human review policy, triggers, owners, or approval criteria.",
    )
    assert by_type["prompt_data_retention"].gap_messages == (
        "Define prompt/data retention period and whether source data is excluded from model training.",
    )


def test_no_scope_generic_automation_and_out_of_scope_ai_do_not_create_false_positives():
    no_scope = extract_source_ai_governance_requirements(
        _source_brief(
            summary="Manual workflow with no AI features.",
            source_payload={"requirements": ["AI governance is out of scope for this release."]},
        )
    )
    generic_automation = extract_source_ai_governance_requirements(
        _source_brief(
            title="Automation rules",
            summary="Automate ticket routing with deterministic status transitions.",
            source_payload={"requirements": ["Add workflow automation and retry failed jobs."]},
        )
    )

    assert no_scope.requirements == ()
    assert no_scope.summary["status"] == "no_requirements"
    assert generic_automation.requirements == ()
    assert generic_automation.summary["requirement_count"] == 0


def test_serialization_markdown_aliases_evidence_deduplication_owners_and_notes_are_stable():
    source = _source_brief(
        source_id="ai-model",
        source_payload={
            "requirements": [
                "Safety review checks bias and harmful output risks.",
                "Safety review checks bias and harmful output risks.",
                "Fallback behavior when the model times out is required.",
            ]
        },
    )
    model = SourceBrief.model_validate(source)

    result = build_source_ai_governance_requirements(model)
    summary = summarize_source_ai_governance_requirements(result)
    dict_payload = source_ai_governance_requirements_to_dict(result)
    dicts_payload = source_ai_governance_requirements_to_dicts(result)
    markdown = source_ai_governance_requirements_to_markdown(result)

    assert summary["requirement_count"] == 2
    assert dict_payload["brief_id"] == "ai-model"
    assert dict_payload["source_id"] == "ai-model"
    assert dict_payload["requirements"] == dict_payload["records"] == dict_payload["findings"]
    assert len(dicts_payload) == 2
    assert len({ev for record in result.records for ev in record.evidence}) == 2
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["bias_safety_review"].suggested_owners == ("ml_engineer", "security", "compliance")
    assert by_type["fallback_behavior"].planning_notes[0].startswith("Design timeout")
    assert "# Source AI Governance Requirements Report: ai-model" in markdown
    assert "| Source Brief | Type | Confidence |" in markdown
    assert json.loads(json.dumps(dict_payload, sort_keys=True))["brief_id"] == "ai-model"


def _source_brief(
    *,
    source_id: str = "source-ai-governance",
    title: str = "AI governance source",
    summary: str = "AI governance requirements extraction test.",
    source_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": source_id,
        "title": title,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": f"{source_id}-upstream",
        "source_payload": source_payload or {},
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(
    *,
    brief_id: str = "implementation-ai-governance",
    title: str = "AI governance implementation",
    scope: list[str] | None = None,
    definition_of_done: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": brief_id,
        "source_brief_id": "source-ai-governance",
        "title": title,
        "target_user": "ml_ops",
        "buyer": "compliance",
        "workflow_context": "AI copilot needs governance controls for compliance.",
        "problem_statement": "AI governance requirements need to be extracted early.",
        "mvp_goal": "Plan human review, evaluation, disclosure, and auditability.",
        "product_surface": "api",
        "scope": scope or [],
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run AI governance extractor tests.",
        "definition_of_done": definition_of_done or [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
