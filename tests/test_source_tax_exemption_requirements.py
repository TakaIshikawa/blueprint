import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_tax_exemption_requirements import (
    SourceTaxExemptionRequirement,
    SourceTaxExemptionRequirementsReport,
    build_source_tax_exemption_requirements,
    derive_source_tax_exemption_requirements,
    extract_source_tax_exemption_requirements,
    generate_source_tax_exemption_requirements,
    source_tax_exemption_requirements_to_dict,
    source_tax_exemption_requirements_to_dicts,
    source_tax_exemption_requirements_to_markdown,
    summarize_source_tax_exemption_requirements,
)


def test_nested_source_payload_extracts_all_tax_exemption_categories():
    result = build_source_tax_exemption_requirements(
        _source_brief(
            source_payload={
                "tax_exemption_rules": {
                    "collection": "Customers must upload an exemption certificate or resale certificate before tax is removed.",
                    "validation": "Validation status must show pending review, approved exemption, or rejected exemption with reason.",
                    "renewal": "Certificates should track expiration dates and renewal reminders before expired exemptions lapse.",
                    "jurisdiction": "Jurisdiction scope must cover country, state, province, VAT, GST, and sales tax applicability.",
                    "invoice": "Invoice tax suppression must zero-rate tax calculation for validated tax-exempt customers.",
                    "audit": "Audit evidence must retain certificate records, approval history, and exportable evidence.",
                    "support": "Support review queue must allow customer support escalation and support overrides.",
                }
            }
        )
    )

    assert isinstance(result, SourceTaxExemptionRequirementsReport)
    assert result.source_id == "sb-tax"
    assert all(isinstance(record, SourceTaxExemptionRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "exemption_certificate_collection",
        "validation_status",
        "renewal_expiration",
        "jurisdiction_scope",
        "invoice_tax_suppression",
        "audit_evidence",
        "support_review",
    ]
    assert all(record.confidence == "high" for record in result.records)
    assert result.summary["requirement_count"] == 7
    assert result.summary["category_counts"] == {
        "exemption_certificate_collection": 1,
        "validation_status": 1,
        "renewal_expiration": 1,
        "jurisdiction_scope": 1,
        "invoice_tax_suppression": 1,
        "audit_evidence": 1,
        "support_review": 1,
    }
    assert result.summary["confidence_counts"] == {"high": 7, "medium": 0, "low": 0}
    assert any(
        "source_payload.tax_exemption_rules.collection" in evidence
        for record in result.records
        for evidence in record.evidence
    )


def test_validation_and_audit_evidence_are_kept_as_separate_categories():
    result = build_source_tax_exemption_requirements(
        _source_brief(
            source_payload={
                "compliance": [
                    "Validation status must reject invalid certificate documents with a reason.",
                    "Audit evidence must retain immutable review history without treating it as validation status.",
                ]
            }
        )
    )

    by_category = {record.category: record for record in result.records}

    assert set(by_category) == {"validation_status", "audit_evidence"}
    assert by_category["validation_status"].owner_suggestion == "tax_compliance"
    assert by_category["audit_evidence"].owner_suggestion == "finance"
    assert any("Validation status" in item for item in by_category["validation_status"].evidence)
    assert any("Audit evidence" in item for item in by_category["audit_evidence"].evidence)
    assert by_category["validation_status"].evidence != by_category["audit_evidence"].evidence
    assert "validation" in by_category["validation_status"].planning_notes[0].casefold()
    assert "evidence" in by_category["audit_evidence"].planning_notes[0].casefold()


def test_implementation_brief_plain_text_and_object_inputs_are_supported():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Tax exemption certificate collection must support resale certificate upload.",
                "Invoice tax suppression must remove sales tax for validated tax-exempt customers.",
            ],
            definition_of_done=[
                "Jurisdiction scope covers VAT countries and US state sales tax applicability.",
                "Audit evidence must export retained certificate approval history.",
            ],
        )
    )
    text = build_source_tax_exemption_requirements(
        "Support review should route tax exemption cases to customer support."
    )
    object_result = build_source_tax_exemption_requirements(
        SimpleNamespace(
            id="object-tax",
            summary="Validation status must show approved exemption and rejected exemption states.",
            metadata={"renewal_expiration": "Expired certificates require renewal reminders and grace periods."},
        )
    )

    model_result = extract_source_tax_exemption_requirements(brief)

    assert model_result.source_id == "impl-tax"
    assert {
        "exemption_certificate_collection",
        "jurisdiction_scope",
        "invoice_tax_suppression",
        "audit_evidence",
    } <= {record.category for record in model_result.records}
    assert text.records[0].category == "support_review"
    assert [record.category for record in object_result.records] == [
        "validation_status",
        "renewal_expiration",
    ]


def test_empty_no_signal_negated_and_malformed_inputs_return_stable_empty_reports():
    class BriefLike:
        id = "object-empty"
        summary = "No tax exemption or certificate changes are in scope."

    empty = build_source_tax_exemption_requirements(
        _source_brief(summary="This billing copy update has no tax exemption requirements.")
    )
    negated = build_source_tax_exemption_requirements(BriefLike())
    malformed = build_source_tax_exemption_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_tax_exemption_requirements(42)

    assert empty.source_id == "sb-tax"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "exemption_certificate_collection": 0,
            "validation_status": 0,
            "renewal_expiration": 0,
            "jurisdiction_scope": 0,
            "invoice_tax_suppression": 0,
            "audit_evidence": 0,
            "support_review": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
    }
    assert "No tax exemption requirements were found" in empty.to_markdown()
    assert negated.records == ()
    assert malformed.records == ()
    assert invalid.records == ()


def test_sourcebrief_aliases_json_serialization_ordering_markdown_and_no_mutation():
    source = _source_brief(
        source_id="tax-model",
        summary="Tax exemption handling must collect certificates and validate exemption status.",
        source_payload={
            "acceptance_criteria": [
                "Exemption certificate collection must collect account | certificate notes.",
                "Validation status must approve or reject invalid tax exemption certificates.",
                "Invoice tax suppression must suppress tax once the exemption is validated.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_tax_exemption_requirements(source)
    model_result = generate_source_tax_exemption_requirements(model)
    derived = derive_source_tax_exemption_requirements(model)
    payload = source_tax_exemption_requirements_to_dict(model_result)
    markdown = source_tax_exemption_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_tax_exemption_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert source_tax_exemption_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_tax_exemption_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_tax_exemption_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "evidence",
        "confidence",
        "owner_suggestion",
        "planning_notes",
    ]
    assert [(record.source_brief_id, record.category) for record in model_result.records] == [
        ("tax-model", "exemption_certificate_collection"),
        ("tax-model", "validation_status"),
        ("tax-model", "invoice_tax_suppression"),
    ]
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Category | Confidence | Owner | Planning Notes | Evidence |" in markdown
    assert "account \\| certificate notes" in markdown


def _source_brief(
    *,
    source_id="sb-tax",
    title="Tax exemption requirements",
    domain="billing",
    summary="General tax exemption requirements.",
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


def _implementation_brief(
    *,
    scope=None,
    definition_of_done=None,
):
    return {
        "id": "impl-tax",
        "source_brief_id": "source-tax",
        "title": "Tax exemption",
        "domain": "billing",
        "target_user": "customers",
        "buyer": None,
        "workflow_context": "Tax exemption handling.",
        "problem_statement": "Customers need source-backed tax exemption handling.",
        "mvp_goal": "Ship tax exemption requirements.",
        "product_surface": "billing settings",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run tax exemption validation.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
