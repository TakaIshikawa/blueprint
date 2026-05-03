import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_data_residency_requirements import (
    SourceDataResidencyRequirement,
    SourceDataResidencyRequirementsReport,
    build_source_data_residency_requirements,
    build_source_data_residency_requirements_report,
    extract_source_data_residency_requirements,
    source_data_residency_requirements_to_dict,
    source_data_residency_requirements_to_dicts,
    source_data_residency_requirements_to_markdown,
    summarize_source_data_residency_requirements,
)


def test_structured_source_payload_extracts_all_residency_categories():
    result = build_source_data_residency_requirements(
        _source(
            source_payload={
                "data_residency": {
                    "eu": "EU-only tenants must store customer data only in the EU.",
                    "us": "US-only customers require support logs to stay within the United States.",
                    "pinning": "Analytics exports are pinned to eu-west-1 regional storage.",
                    "transfer": "Cross-border transfer outside the EU requires SCCs and transfer impact assessment.",
                    "failover": "Regional failover must remain inside the EU residency boundary.",
                    "localization": "Payment data localization requires records to remain in Germany.",
                    "tenant_routing": "Route tenants to their tenant home region via regional endpoints.",
                    "selectable": "Customers can choose their hosting region from EU, US, or Canada.",
                    "sovereignty": "Public sector accounts require sovereign cloud data sovereignty controls.",
                    "subprocessors": "Subprocessors must process customer data only in approved residency regions.",
                    "evidence": "Residency audit evidence must prove regional hosting commitments for customers.",
                }
            }
        )
    )

    assert isinstance(result, SourceDataResidencyRequirementsReport)
    assert all(isinstance(record, SourceDataResidencyRequirement) for record in result.records)
    assert [record.category for record in result.requirements] == [
        "eu_only",
        "us_only",
        "region_pinning",
        "cross_border_transfer",
        "regional_failover",
        "data_localization",
        "tenant_region_routing",
        "customer_selectable_region",
        "data_sovereignty",
        "subprocessor_residency",
        "residency_audit_evidence",
    ]
    assert result.summary["requirement_count"] == 11
    assert result.summary["category_counts"]["tenant_region_routing"] == 1
    assert result.summary["category_counts"]["regional_failover"] == 1
    assert result.summary["category_counts"]["subprocessor_residency"] == 1
    assert result.summary["confidence_counts"]["high"] >= 5
    assert "eu" in result.summary["region_signals"]
    assert "us" in result.summary["region_signals"]
    assert result.records[0].planning_note.startswith("Preserve the EU-only constraint")


def test_natural_language_markdown_and_string_scanning_are_stable():
    result = build_source_data_residency_requirements(
        """
# Data residency

- Customer data must remain in the EU only for enterprise tenants.
- Tenant-region routing should use the tenant selected region.
- Data transfers outside the region require a transfer impact assessment.
"""
    )
    markdown = source_data_residency_requirements_to_markdown(result)

    assert [record.category for record in result.records] == [
        "eu_only",
        "cross_border_transfer",
        "tenant_region_routing",
        "customer_selectable_region",
    ]
    assert result.source_id is None
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source Data Residency Requirements Report")
    assert "| Category | Confidence | Regions | Data Scope | Evidence | Planning Note |" in markdown
    assert "body: Customer data must remain in the EU only for enterprise tenants." in markdown


def test_implementation_brief_model_and_explicit_fields_contribute_evidence():
    model = ImplementationBrief.model_validate(
        _implementation(
            architecture_notes="Pin primary data store to us-east-1 and keep tenant home region routing deterministic.",
            data_requirements="EU-only customer records cannot leave the EU.",
            risks=[
                "Cross-border data transfer to subprocessors needs transfer impact assessment.",
                "Regional failover replicas must remain inside the tenant residency region.",
                "Sovereign cloud requirements apply to public sector tenants.",
            ],
            definition_of_done=[
                "Data localization checks prove payment data remains in Canada.",
            ],
        )
    )

    result = build_source_data_residency_requirements_report(model)
    by_category = {record.category: record for record in result.records}

    assert result.source_id == "impl-residency"
    assert {
        "eu_only",
        "region_pinning",
        "cross_border_transfer",
        "regional_failover",
        "data_localization",
        "tenant_region_routing",
        "data_sovereignty",
    } <= set(by_category)
    assert by_category["region_pinning"].evidence == (
        "architecture_notes: Pin primary data store to us-east-1 and keep tenant home region routing deterministic.",
    )
    assert "us-east-1" in by_category["region_pinning"].region_signals
    assert by_category["data_localization"].data_scope == "payment data"


def test_duplicate_requirements_collapse_without_mutating_source_and_serialization_is_stable():
    source = _source(
        source_id="dupe-residency",
        source_payload={
            "requirements": [
                "Customer data must remain in the EU only.",
                "customer data must remain in the EU only.",
                "Tenant region routing must use the selected region.",
            ],
            "metadata": {"eu_only": "Customer data must remain in the EU only."},
        },
    )
    original = copy.deepcopy(source)
    model_result = build_source_data_residency_requirements(SourceBrief.model_validate(source))
    mapping_result = build_source_data_residency_requirements(source)
    extracted = extract_source_data_residency_requirements(SourceBrief.model_validate(source))
    payload = source_data_residency_requirements_to_dict(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert extracted == model_result.requirements
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_data_residency_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_data_residency_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_data_residency_requirements(model_result) == model_result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "category",
        "confidence",
        "region_signals",
        "data_scope",
        "evidence",
        "planning_note",
    ]
    eu_only = next(record for record in model_result.records if record.category == "eu_only")
    assert eu_only.evidence == ("source_payload.requirements[0]: Customer data must remain in the EU only.",)
    assert [record.category for record in model_result.records] == ["eu_only", "tenant_region_routing"]


def test_empty_invalid_object_and_deterministic_ordering():
    empty = build_source_data_residency_requirements(
        _source(summary="Polish onboarding copy.", source_payload={"body": "No data residency changes are needed."})
    )
    repeat = build_source_data_residency_requirements(
        _source(summary="Polish onboarding copy.", source_payload={"body": "No data residency changes are needed."})
    )
    invalid = build_source_data_residency_requirements(17)
    object_result = build_source_data_residency_requirements(
        SimpleNamespace(
            id="object-residency",
            body="US-only support records must stay in the United States. Route tenants by customer region.",
        )
    )

    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "source-residency"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "requirement_count": 0,
        "category_counts": {
            "eu_only": 0,
            "us_only": 0,
            "region_pinning": 0,
            "cross_border_transfer": 0,
            "regional_failover": 0,
            "data_localization": 0,
            "tenant_region_routing": 0,
            "customer_selectable_region": 0,
            "data_sovereignty": 0,
            "subprocessor_residency": 0,
            "residency_audit_evidence": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
        "region_signals": [],
    }
    assert "No source data residency requirements were inferred." in empty.to_markdown()
    assert invalid.source_id is None
    assert invalid.records == ()
    assert object_result.source_id == "object-residency"
    assert [record.category for record in object_result.records] == ["us_only", "tenant_region_routing"]


def test_country_specific_customer_selectable_failover_subprocessors_and_evidence_are_separate():
    result = build_source_data_residency_requirements(
        _source(
            source_payload={
                "requirements": [
                    "Country-specific hosting requires authentication records to be processed only in Switzerland.",
                    "Customers can select EU, US, or Canada as their storage region during provisioning.",
                    "Disaster recovery replicas and regional failover must stay in the same jurisdiction.",
                    "Subprocessors must keep support logs in the customer's selected residency region.",
                    "Provide residency audit evidence and attestation reports for enterprise renewals.",
                ],
            }
        )
    )
    by_category = {record.category: record for record in result.records}

    assert {
        "data_localization",
        "customer_selectable_region",
        "regional_failover",
        "subprocessor_residency",
        "residency_audit_evidence",
    } <= set(by_category)
    assert by_category["data_localization"].data_scope == "authentication records"
    assert "switzerland" in by_category["data_localization"].region_signals
    assert {"eu", "us", "ca"} <= set(by_category["customer_selectable_region"].region_signals)
    assert by_category["regional_failover"].evidence == (
        "source_payload.requirements[2]: Disaster recovery replicas and regional failover must stay in the same jurisdiction.",
    )
    assert by_category["subprocessor_residency"].planning_note.startswith("Validate subprocessor")
    assert by_category["residency_audit_evidence"].confidence == "high"


def test_ambiguous_geographic_mentions_do_not_become_residency_requirements():
    source = _source(
        summary="Update the Germany, France, and Japan onboarding pages with regional testimonials.",
        source_payload={
            "notes": [
                "The EU campaign needs translated screenshots.",
                "No data residency, cross-border transfer, regional failover, or subprocessor changes are required.",
            ],
        },
    )
    source["title"] = "Germany launch copy"
    result = build_source_data_residency_requirements(source)

    assert result.records == ()


def _source(*, source_id="source-residency", summary="Enterprise regional storage.", source_payload=None):
    return {
        "id": source_id,
        "title": "Residency requirements",
        "domain": "compliance",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "issue",
        "source_id": source_id,
        "source_payload": source_payload or {},
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation(*, architecture_notes=None, data_requirements=None, risks=None, definition_of_done=None):
    return {
        "id": "impl-residency",
        "source_brief_id": "source-residency",
        "title": "Enterprise residency",
        "domain": "compliance",
        "target_user": "ops",
        "buyer": "enterprise",
        "workflow_context": "Preserve residency requirements before task generation.",
        "problem_statement": "Enterprise customers need regional storage controls.",
        "mvp_goal": "Capture residency constraints in the plan.",
        "product_surface": "data platform",
        "scope": ["Regional storage"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": data_requirements,
        "integration_points": [],
        "risks": risks or [],
        "validation_plan": "Review regional routing evidence.",
        "definition_of_done": definition_of_done or [],
    }
