import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_support_tier_requirements import (
    SourceSupportTierRequirement,
    SourceSupportTierRequirementsReport,
    build_source_support_tier_requirements,
    derive_source_support_tier_requirements,
    extract_source_support_tier_requirements,
    generate_source_support_tier_requirements,
    source_support_tier_requirements_to_dict,
    source_support_tier_requirements_to_dicts,
    source_support_tier_requirements_to_markdown,
    summarize_source_support_tier_requirements,
)


def test_extracts_all_support_tiers_with_sla_channels_and_escalation_details():
    result = build_source_support_tier_requirements(
        _source_brief(
            source_payload={
                "body": """
# Support tier requirements

- Enterprise support must provide first response within 1 hour by phone and support portal, with escalation to the named account owner and engineering on-call.
- Premium paid support package requires chat and email priority support within 4 business hours.
- Standard support should use email tickets during business hours with next business day response.
- Self-serve users receive help center only and community forum support with no live support escalation.
"""
            }
        )
    )

    assert isinstance(result, SourceSupportTierRequirementsReport)
    assert all(isinstance(record, SourceSupportTierRequirement) for record in result.records)
    assert [record.tier for record in result.records] == [
        "enterprise",
        "premium",
        "standard",
        "self_serve",
    ]
    by_tier = {record.tier: record for record in result.records}
    assert by_tier["enterprise"].sla_text == "within 1 hour"
    assert by_tier["enterprise"].support_channel == "phone, support portal"
    assert "named account owner" in by_tier["enterprise"].escalation_note
    assert by_tier["premium"].sla_text == "within 4 business hours"
    assert by_tier["premium"].support_channel == "chat, email"
    assert by_tier["standard"].sla_text == "business hours, next business day"
    assert by_tier["self_serve"].support_channel == "help center, community forum"
    assert by_tier["self_serve"].escalation_note == "no live support escalation"
    assert result.summary["requirement_count"] == 4
    assert result.summary["tier_counts"] == {
        "enterprise": 1,
        "premium": 1,
        "standard": 1,
        "self_serve": 1,
    }


def test_structured_fields_source_brief_and_implementation_brief_are_supported():
    source = _source_brief(
        source_payload={
            "support_tiers": [
                {
                    "tier": "Enterprise",
                    "sla": "first response within 30 minutes",
                    "channel": "phone and Slack",
                    "escalation": "handoff to dedicated CSM",
                    "audience": "enterprise customers",
                },
                {
                    "tier": "Premium",
                    "package": "paid support add-on",
                    "response_time": "same day",
                    "channel": "Intercom chat",
                },
            ]
        }
    )
    model_result = build_source_support_tier_requirements(SourceBrief.model_validate(source))
    implementation_result = build_source_support_tier_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=[
                    "Support entitlement checks must verify paid support package before premium support routing.",
                    "Customer success handoff is required for Enterprise accounts after launch.",
                ],
                definition_of_done=[
                    "Standard support must route tickets through Zendesk during business hours.",
                ],
            )
        )
    )

    assert [record.tier for record in model_result.records] == ["enterprise", "premium"]
    assert model_result.records[0].customer_segment == "enterprise customers"
    assert model_result.records[0].sla_text == "within 30 minutes"
    assert model_result.records[0].support_channel == "phone, Slack"
    assert "dedicated CSM" in model_result.records[0].escalation_note
    assert [record.tier for record in implementation_result.records] == [
        "enterprise",
        "premium",
        "standard",
    ]
    by_tier = {record.tier: record for record in implementation_result.records}
    assert by_tier["premium"].source_field == "scope[0]"
    assert "paid support package" in by_tier["premium"].evidence[0]
    assert by_tier["enterprise"].escalation_note == "Customer success handoff is required for Enterprise accounts after launch"
    assert by_tier["standard"].support_channel == "tickets, Zendesk"


def test_no_support_impact_language_prevents_extraction_and_empty_report_is_stable():
    empty = build_source_support_tier_requirements(
        _source_brief(
            summary="Launch copy update.",
            source_payload={"body": "No support tier, SLA, escalation, or customer success impact is required."},
        )
    )
    repeat = build_source_support_tier_requirements(
        _source_brief(
            summary="Launch copy update.",
            source_payload={"body": "No support tier, SLA, escalation, or customer success impact is required."},
        )
    )
    object_empty = build_source_support_tier_requirements(
        SimpleNamespace(id="object-empty", summary="No support impact or SLA changes are needed.")
    )
    invalid = build_source_support_tier_requirements(42)

    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "sb-support-tier"
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "tier_counts": {
            "enterprise": 0,
            "premium": 0,
            "standard": 0,
            "self_serve": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "tiers": [],
    }
    assert "No support tier requirements were found" in empty.to_markdown()
    assert object_empty.records == ()
    assert invalid.records == ()


def test_duplicate_tier_candidates_merge_with_stable_ordering_and_details():
    result = build_source_support_tier_requirements(
        {
            "id": "dupes",
            "source_payload": {
                "support": {
                    "enterprise": "Enterprise support must respond within 1 hour by phone.",
                    "same_enterprise": "Enterprise support must respond within 1 hour by phone.",
                    "standard": "Standard support must respond next business day by email ticket.",
                },
                "acceptance_criteria": [
                    "Standard support must respond next business day by email ticket.",
                    "Enterprise support must respond within 1 hour by phone.",
                ],
            },
        }
    )

    assert [record.tier for record in result.records] == ["enterprise", "standard"]
    assert result.records[0].evidence == (
        "source_payload.acceptance_criteria[1]: Enterprise support must respond within 1 hour by phone.",
    )
    assert result.records[0].support_channel == "phone"
    assert result.records[1].source_field == "source_payload.acceptance_criteria[0]"


def test_aliases_serialization_markdown_json_ordering_and_no_input_mutation():
    source = _source_brief(
        source_id="support-tier-model",
        source_payload={
            "requirements": [
                "Premium support must include paid support package routing within 2 hours via chat.",
                "Enterprise support must escape plan | account details in named account owner escalation notes.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_support_tier_requirements(source)
    model_result = generate_source_support_tier_requirements(model)
    derived = derive_source_support_tier_requirements(model)
    extracted = extract_source_support_tier_requirements(model)
    text_result = build_source_support_tier_requirements(
        "Self-serve support must be limited to knowledge base only."
    )
    object_result = build_source_support_tier_requirements(
        SimpleNamespace(id="object-support-tier", metadata={"support_tier": "Standard support requires email tickets."})
    )
    payload = source_support_tier_requirements_to_dict(model_result)
    markdown = source_support_tier_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_support_tier_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert extracted.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_support_tier_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_support_tier_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_support_tier_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "tier",
        "sla_text",
        "customer_segment",
        "support_channel",
        "escalation_note",
        "source_field",
        "evidence",
        "confidence",
        "planning_note",
    ]
    assert [record.tier for record in model_result.records] == ["enterprise", "premium"]
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Tier | SLA | Segment | Channel | Escalation | Source Field | Confidence | Planning Note | Evidence |" in markdown
    assert "plan \\| account details" in markdown
    assert text_result.records[0].tier == "self_serve"
    assert object_result.records[0].tier == "standard"


def _source_brief(
    *,
    source_id="sb-support-tier",
    title="Support tier requirements",
    domain="support",
    summary="General support tier requirements.",
    source_payload=None,
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
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "impl-support-tier",
        "source_brief_id": "source-support-tier",
        "title": "Support tier rollout",
        "domain": "support",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need support tier SLA requirements before task generation.",
        "problem_statement": "Support tier requirements need to be extracted early.",
        "mvp_goal": "Plan support tier work from source briefs.",
        "product_surface": "support",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review support routing.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
    }
