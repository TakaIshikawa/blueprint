import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_service_ownership_requirements import (
    SourceServiceOwnershipRequirement,
    SourceServiceOwnershipRequirementsReport,
    build_source_service_ownership_requirements,
    derive_source_service_ownership_requirements,
    extract_source_service_ownership_requirements,
    generate_source_service_ownership_requirements,
    source_service_ownership_requirements_to_dict,
    source_service_ownership_requirements_to_dicts,
    source_service_ownership_requirements_to_markdown,
    summarize_source_service_ownership_requirements,
)


def test_extracts_ownership_categories_from_brief_fields_and_nested_payloads():
    result = build_source_service_ownership_requirements(
        _source_brief(
            summary="Service owner is Payments Platform and post-launch owner must remain Checkout Ops.",
            source_payload={
                "acceptance_criteria": [
                    "DRI: Checkout Ops must join the on-call rotation before launch.",
                    "Escalation path must route to Tier 2 Payments via PagerDuty.",
                    "Support channel must be Slack #checkout-support for incident intake.",
                ],
                "constraints": [
                    "Maintenance owner is SRE during the weekly maintenance window.",
                    "Handoff to Support is complete when the runbook is approved.",
                ],
                "metadata": {
                    "ownership": {
                        "gap": "Ownership gap: reporting export has no clear owner.",
                    }
                },
            },
        )
    )

    assert isinstance(result, SourceServiceOwnershipRequirementsReport)
    assert result.source_id == "source-ownership"
    assert all(isinstance(record, SourceServiceOwnershipRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "service_owner",
        "operational_dri",
        "escalation_path",
        "support_channel",
        "maintenance_window",
        "handoff_requirement",
        "ownership_gap",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["service_owner"].responsible_party == "Payments Platform"
    assert by_category["support_channel"].responsible_party == "Slack"
    assert "absent_owner" in by_category["ownership_gap"].missing_detail_flags
    assert result.summary["category_counts"]["service_owner"] == 1
    assert result.summary["has_ownership_gaps"] is True
    assert any(
        "source_payload.metadata.ownership.gap" in evidence
        for evidence in by_category["ownership_gap"].evidence
    )


def test_missing_detail_flags_cover_absent_owner_escalation_channel_and_handoff_criteria():
    result = build_source_service_ownership_requirements(
        {
            "id": "missing-details",
            "requirements": [
                "Service owner must be assigned before launch.",
                "Escalation path is required for production incidents.",
                "Support channel must be defined for launch week.",
                "Handoff to operations is required after launch.",
            ],
        }
    )

    flags_by_category = {record.category: record.missing_detail_flags for record in result.records}

    assert "absent_owner" in flags_by_category["service_owner"]
    assert "absent_escalation_target" in flags_by_category["escalation_path"]
    assert "absent_support_channel" in flags_by_category["support_channel"]
    assert flags_by_category["handoff_requirement"] == (
        "absent_owner",
        "absent_handoff_criteria",
    )
    assert result.summary["missing_detail_flag_counts"] == {
        "absent_owner": 2,
        "absent_escalation_target": 1,
        "absent_support_channel": 1,
        "absent_handoff_criteria": 1,
    }


def test_sourcebrief_implementationbrief_objects_and_aliases_are_supported_without_mutation():
    source = _source_brief(
        source_id="ownership-model",
        summary="The service owner is Platform Ops.",
        source_payload={
            "operational_notes": [
                "PagerDuty service must page Platform Ops for escalation.",
                "Slack channel #platform-support is the production support channel.",
            ],
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Post-launch responsibility must hand over to Support once runbook acceptance passes.",
            ],
        )
    )
    object_result = build_source_service_ownership_requirements(
        SimpleNamespace(
            id="object-owner",
            operational_notes="Operational review must include maintenance owner SRE.",
        )
    )

    mapping_result = build_source_service_ownership_requirements(source)
    model_result = generate_source_service_ownership_requirements(model)
    derived = derive_source_service_ownership_requirements(model)
    implementation_records = extract_source_service_ownership_requirements(implementation)
    payload = source_service_ownership_requirements_to_dict(model_result)
    markdown = source_service_ownership_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_service_ownership_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_service_ownership_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_service_ownership_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_service_ownership_requirements(model_result) == model_result.summary
    assert [record.category for record in implementation_records] == ["handoff_requirement"]
    assert [record.category for record in object_result.records] == ["maintenance_window"]
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "category",
        "requirement",
        "responsible_party",
        "evidence",
        "missing_detail_flags",
        "confidence",
        "source_id",
    ]
    assert markdown == model_result.to_markdown()
    assert "| Category | Requirement | Responsible Party | Confidence | Missing Details | Source | Evidence |" in markdown


def test_generic_stakeholders_empty_invalid_and_markdown_escaping_are_stable():
    generic = build_source_service_ownership_requirements(
        {
            "id": "generic",
            "summary": "Product and Legal stakeholders must approve copy before launch.",
            "acceptance_criteria": ["Design stakeholder signs off on the settings modal."],
        }
    )
    empty = build_source_service_ownership_requirements(
        _source_brief(source_id="empty-ownership", summary="Update onboarding copy only.")
    )
    invalid = build_source_service_ownership_requirements(42)
    escaped = build_source_service_ownership_requirements(
        {
            "id": "pipes",
            "summary": "Service owner is Platform | Ops and support channel is #ops-support.",
        }
    )

    assert generic.records == ()
    assert empty.source_id == "empty-ownership"
    assert empty.records == ()
    assert empty.summary["requirement_count"] == 0
    assert "No service ownership requirements were found" in empty.to_markdown()
    assert invalid.records == ()
    assert "\\|" in escaped.to_markdown()


def _source_brief(
    *,
    source_id="source-ownership",
    title="Service ownership requirements",
    domain="platform",
    summary="General ownership requirements.",
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


def _implementation_brief(*, scope=None):
    return {
        "id": "impl-ownership",
        "source_brief_id": "source-ownership",
        "title": "Operational ownership",
        "domain": "platform",
        "problem_statement": "Operational ownership is unclear.",
        "mvp_goal": "Clarify ownership.",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "definition_of_done": [],
        "risks": [],
        "assumptions": [],
        "validation_plan": "Review operational ownership requirements.",
    }
