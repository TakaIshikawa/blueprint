import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_partner_onboarding_requirements import (
    SourcePartnerOnboardingRequirement,
    SourcePartnerOnboardingRequirementsReport,
    build_source_partner_onboarding_requirements,
    derive_source_partner_onboarding_requirements,
    extract_source_partner_onboarding_requirements,
    generate_source_partner_onboarding_requirements,
    source_partner_onboarding_requirements_to_dict,
    source_partner_onboarding_requirements_to_dicts,
    source_partner_onboarding_requirements_to_markdown,
)


def test_classifies_partner_onboarding_categories_from_sourcebrief_and_nested_metadata():
    source = SourceBrief.model_validate(
        _source_brief(
            summary=(
                "Partner approval from Acme Marketplace is required before launch. "
                "Owner: Integrations PM. Partner contact: partner-success@acme.test. "
                "Production environment is the launch target."
            ),
            source_payload={
                "partner_onboarding": {
                    "sandbox": "Sandbox credentials and developer account are needed for staging environment.",
                    "certification": "Marketplace certification must pass before launch.",
                    "launch": "Go-live checklist must include launch criteria and approval gate.",
                    "support": "Joint support model needs escalation contact and SLA terms.",
                    "contract": "Legal must approve marketplace terms and DPA.",
                    "runbook": "Operational runbook covers cutover, rollback runbook, and external handoff procedure.",
                }
            },
        )
    )

    result = build_source_partner_onboarding_requirements(source)
    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourcePartnerOnboardingRequirementsReport)
    assert all(isinstance(record, SourcePartnerOnboardingRequirement) for record in result.records)
    assert set(by_category) == {
        "partner_approval",
        "sandbox_access",
        "certification",
        "launch_checklist",
        "support_model",
        "contract_terms",
        "operational_runbook",
        "contact_handoff",
    }
    assert by_category["partner_approval"].confidence == "high"
    assert by_category["partner_approval"].missing_details == (
        "owner",
        "partner_contact",
        "environment",
    )
    assert by_category["sandbox_access"].source_field_paths == (
        "source_payload.partner_onboarding.sandbox",
    )
    assert "source_payload.partner_onboarding.launch" in by_category["launch_checklist"].source_field_paths
    assert result.summary["category_counts"]["certification"] == 1
    assert result.summary["confidence_counts"]["high"] >= 1


def test_plain_mapping_filters_negated_scope_and_keeps_generic_vendor_low_confidence():
    result = build_source_partner_onboarding_requirements(
        {
            "id": "partner-scope",
            "summary": "No partner onboarding, external handoff, or marketplace approval is required for this release.",
            "metadata": {
                "vendors": [
                    "Vendor dependency exists for reporting.",
                    "Vendor setup should be tracked by integrations.",
                ],
                "launch": "Partner approval gate is required before launch.",
            },
        }
    )

    by_category = {record.category: record for record in result.records}

    assert set(by_category) == {"partner_approval"}
    assert by_category["partner_approval"].confidence == "high"
    assert all("No partner onboarding" not in item for item in by_category["partner_approval"].evidence)

    generic = build_source_partner_onboarding_requirements(
        {"id": "generic-vendor", "metadata": {"vendor_setup": "Vendor setup should be tracked."}}
    )

    assert generic.records[0].category == "partner_approval"
    assert generic.records[0].confidence == "low"

    unrelated = build_source_partner_onboarding_requirements(
        {"id": "vendor-only", "summary": "We use a vendor dependency for analytics reporting."}
    )
    assert unrelated.records == ()


def test_missing_detail_flags_evidence_deduplication_and_field_paths_are_stable():
    result = build_source_partner_onboarding_requirements(
        _source_brief(
            source_id="dedupe-partner",
            source_payload={
                "requirements": [
                    "Sandbox credentials are required from the partner before launch.",
                    "sandbox credentials are required from the partner before launch.",
                ],
                "partner": {
                    "approval": "Partner approval is required before launch.",
                    "approval_duplicate": "Partner approval is required before launch.",
                },
            },
        )
    )

    sandbox = next(record for record in result.records if record.category == "sandbox_access")
    approval = next(record for record in result.records if record.category == "partner_approval")

    assert sandbox.evidence == ("source_payload.requirements[0]: Sandbox credentials are required from the partner before launch.",)
    assert sandbox.source_field_paths == (
        "source_payload.requirements[0]",
        "source_payload.requirements[1]",
    )
    assert sandbox.missing_details == ("owner", "partner_contact", "approval_gate", "launch_criteria")
    assert approval.evidence == ("source_payload.partner.approval: Partner approval is required before launch.",)
    assert approval.source_field_paths == (
        "source_payload.partner.approval",
        "source_payload.partner.approval_duplicate",
    )


def test_confidence_ordering_places_actionable_requirements_before_low_generic_vendor_setup():
    result = build_source_partner_onboarding_requirements(
        [
            {
                "id": "confidence-order",
                "metadata": {
                    "partner": (
                        "Partner approval gate is required before launch. "
                        "Owner: Alliances. Partner contact: Pat. Production environment launch criteria are documented."
                    ),
                    "sandbox": "Sandbox credentials are needed for the partner test environment.",
                },
            },
            {"id": "generic-order", "metadata": {"vendor_setup": "Vendor setup should be tracked."}},
        ]
    )

    confidences = [record.confidence for record in result.records]

    assert confidences == sorted(confidences, key={"high": 0, "medium": 1, "low": 2}.get)
    assert result.records[0].category == "partner_approval"
    assert result.records[0].confidence == "high"
    assert result.records[-1].confidence == "low"


def test_serialization_helpers_markdown_aliases_sourcebrief_object_and_no_mutation():
    source = _source_brief(
        source_id="partner-serialize",
        summary="Partner approval is required before launch.",
        source_payload={"partner": {"contact": "Partner contact must be named for handoff."}},
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    brief_like = BriefLike(
        id="brief-like",
        summary="Sandbox access and marketplace certification are required before go-live.",
        metadata={"partner": {"runbook": "Operational runbook should define external handoff."}},
    )

    mapping_result = build_source_partner_onboarding_requirements(source)
    model_result = generate_source_partner_onboarding_requirements(model)
    derived = derive_source_partner_onboarding_requirements(model)
    extracted = extract_source_partner_onboarding_requirements(model)
    object_result = build_source_partner_onboarding_requirements(brief_like)
    payload = source_partner_onboarding_requirements_to_dict(model_result)
    markdown = source_partner_onboarding_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_partner_onboarding_requirements_to_dict(mapping_result)
    assert derived == model_result
    assert extracted == model_result.requirements
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_brief_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "confidence",
        "evidence",
        "source_field_paths",
        "matched_terms",
        "missing_details",
        "planning_note",
    ]
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_partner_onboarding_requirements_to_dicts(model_result) == payload["records"]
    assert source_partner_onboarding_requirements_to_dicts(model_result.records) == payload["records"]
    assert markdown.startswith("# Source Partner Onboarding Requirements Report: partner-serialize")
    assert "| Category | Confidence | Source Field Paths | Missing Details | Evidence | Planning Note |" in markdown
    assert {record.category for record in object_result.records} >= {
        "sandbox_access",
        "certification",
        "operational_runbook",
    }


class BriefLike:
    def __init__(self, *, id, summary, metadata):
        self.id = id
        self.summary = summary
        self.metadata = metadata


def _source_brief(
    *,
    source_id="source-partner",
    title="Partner onboarding requirements",
    domain="integrations",
    summary="General partner onboarding requirements.",
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
