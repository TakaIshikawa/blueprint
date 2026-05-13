import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_vendor_risk_requirements import (
    SourceVendorRiskRequirement,
    SourceVendorRiskRequirementsReport,
    build_source_vendor_risk_requirements,
    derive_source_vendor_risk_requirements,
    extract_source_vendor_risk_requirements,
    generate_source_vendor_risk_requirements,
    source_vendor_risk_requirements_to_dict,
    source_vendor_risk_requirements_to_dicts,
    source_vendor_risk_requirements_to_markdown,
    summarize_source_vendor_risk_requirements,
)


def test_extracts_vendor_risk_categories_in_deterministic_order():
    result = build_source_vendor_risk_requirements(
        _source_brief(
            source_payload={
                "vendor_risk": [
                    "Vendor risk review requires security questionnaire approval before launch.",
                    "Data processing agreement must include data transfer terms.",
                    "SOC 2 compliance attestation and ISO 27001 certificate are required.",
                    "SLA dependency requires 99.9 uptime and support response commitments.",
                    "Exit plan must cover fallback provider, data return, and termination plan.",
                    "Subprocessor approval is required before the processor receives customer data.",
                    "Procurement approval and vendor onboarding must pass the approval gate.",
                    "Incident notification terms must notify security within 24 hours of a vendor incident.",
                ]
            }
        )
    )

    assert isinstance(result, SourceVendorRiskRequirementsReport)
    assert all(isinstance(record, SourceVendorRiskRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "security_review",
        "data_processing_terms",
        "compliance_attestation",
        "uptime_sla_dependency",
        "exit_plan",
        "subprocessors",
        "procurement_approval",
        "incident_notification",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["security_review"].suggested_owner == "security"
    assert by_category["exit_plan"].suggested_planning_note.startswith("Define contingency")
    assert result.gap_messages == ()
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"]["incident_notification"] == 1


def test_gap_messages_flag_missing_review_details_and_exit_strategy():
    result = build_source_vendor_risk_requirements(
        _source_brief(
            summary="Vendor risk requirements for a new third-party processor.",
            requirements=[
                "DPA must be signed before launch.",
                "Subprocessors require approval and incident notification terms.",
                "Procurement approval is required before go-live.",
            ],
        )
    )

    assert [record.category for record in result.records] == [
        "data_processing_terms",
        "subprocessors",
        "procurement_approval",
        "incident_notification",
    ]
    assert "Missing security or compliance review details" in result.gap_messages[0]
    assert "Missing exit or contingency strategy" in result.gap_messages[1]
    assert result.summary["gap_count"] == 2


def test_sourcebrief_implementationbrief_object_and_plain_text_inputs_are_supported():
    source_payload = _source_brief(
        source_id="vendor-model",
        summary="Security review must approve the vendor risk questionnaire before launch.",
    )
    source_model = SourceBrief.model_validate({key: value for key, value in source_payload.items() if key != "requirements"})
    implementation_model = ImplementationBrief.model_validate(_implementation_brief())
    object_result = build_source_vendor_risk_requirements(
        SimpleNamespace(
            id="object-vendor-risk",
            requirements=["Vendor risk requires SOC 2 attestation and an exit strategy."],
        )
    )
    text_result = build_source_vendor_risk_requirements("Vendor risk review requires incident notification and an exit plan.")

    assert generate_source_vendor_risk_requirements(source_model).source_id == "vendor-model"
    assert extract_source_vendor_risk_requirements(implementation_model)[0].category == "uptime_sla_dependency"
    assert object_result.records[0].category == "compliance_attestation"
    assert object_result.records[1].category == "exit_plan"
    assert text_result.records[0].category == "security_review"
    assert text_result.records[1].category == "exit_plan"
    assert text_result.records[2].category == "incident_notification"


def test_generic_vendor_mentions_negated_scope_and_serialization_are_stable():
    generic = build_source_vendor_risk_requirements(_source_brief(summary="The vendor list includes Stripe and SendGrid."))
    negated = build_source_vendor_risk_requirements(_source_brief(summary="No vendor risk review or DPA work is required."))
    result = derive_source_vendor_risk_requirements(
        _source_brief(
            source_id="vendor-risk-serialize",
            summary="Vendor risk review requires security assessment approval and exit plan evidence.",
        )
    )
    payload = source_vendor_risk_requirements_to_dict(result)

    assert generic.records == ()
    assert negated.records == ()
    assert source_vendor_risk_requirements_to_dicts(result) == payload["requirements"]
    assert source_vendor_risk_requirements_to_dicts(result.records) == payload["records"]
    assert summarize_source_vendor_risk_requirements(result) == result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "gap_messages", "summary", "records", "gaps"]
    assert "# Source Vendor Risk Requirements Report: vendor-risk-serialize" in source_vendor_risk_requirements_to_markdown(result)
    assert "No source vendor risk requirements were inferred" in generic.to_markdown()


def _source_brief(
    *,
    source_id="source-vendor-risk",
    title="Vendor risk requirements",
    domain="platform",
    summary="General vendor risk requirements.",
    requirements=None,
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
        "requirements": [] if requirements is None else requirements,
    }


def _implementation_brief():
    return {
        "id": "implementation-vendor-risk",
        "source_brief_id": "source-vendor-risk",
        "title": "Vendor risk implementation",
        "domain": "platform",
        "problem_statement": "Implement vendor risk requirements.",
        "mvp_goal": "Track third-party risk gates.",
        "scope": [
            "SLA dependency requires vendor uptime evidence and support response commitments.",
            "Exit strategy must document fallback provider and data return.",
        ],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Review requirements.",
        "definition_of_done": ["Vendor risk safeguards are planned."],
    }
