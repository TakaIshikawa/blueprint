import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_regional_availability_requirements import (
    SourceRegionalAvailabilityRequirement,
    SourceRegionalAvailabilityRequirementsReport,
    build_source_regional_availability_requirements,
    extract_source_regional_availability_requirements,
    generate_source_regional_availability_requirements,
    source_regional_availability_requirements_to_dict,
    source_regional_availability_requirements_to_dicts,
    source_regional_availability_requirements_to_markdown,
)


def test_extracts_included_excluded_regions_phase_and_fallback_from_narrative_text():
    result = build_source_regional_availability_requirements(
        _source_brief(
            summary=(
                "Checkout must launch in the United States and Canada during phase 1. "
                "Exclude Quebec until Legal approves tax handling and show an unavailable message."
            ),
            source_payload={
                "acceptance_criteria": [
                    "Feature flag checkout for US and Canada users first.",
                    "Compliance owner: Legal validates Quebec fallback before launch.",
                ]
            },
        )
    )

    checkout = _surface(result, "checkout")

    assert isinstance(result, SourceRegionalAvailabilityRequirementsReport)
    assert all(isinstance(record, SourceRegionalAvailabilityRequirement) for record in result.records)
    assert checkout.included_regions == ("Canada", "United States")
    assert checkout.excluded_regions == ("Quebec",)
    assert "phase 1" in checkout.rollout_phase_hints
    assert any("unavailable message" in hint for hint in checkout.fallback_compliance_hints)
    assert any("Legal" in evidence for evidence in checkout.evidence)
    assert "missing_fallback_behavior" not in checkout.missing_detail_flags
    assert result.summary["included_region_count"] == 2
    assert result.summary["excluded_region_count"] == 1


def test_structured_payloads_metadata_and_source_payloads_extract_availability_requirements():
    result = build_source_regional_availability_requirements(
        _source_brief(
            source_payload={
                "regional_availability": {
                    "payments": {
                        "included_regions": ["EU", "UK"],
                        "excluded_regions": ["China"],
                        "rollout": "Wave 2 after compliance review.",
                        "fallback": "Disable payments and contact support in blocked markets.",
                    }
                },
                "metadata": {
                    "currency_dependencies": "Billing available in Japan after JPY currency support.",
                    "localization": "Mobile app beta launches in APAC when translations are ready.",
                },
            },
        )
    )

    payments = _surface(result, "payments")
    billing = _surface(result, "billing")
    mobile = _surface(result, "mobile app")

    assert payments.included_regions == ("European Union", "United Kingdom")
    assert payments.excluded_regions == ("China",)
    assert "Wave 2" in payments.rollout_phase_hints
    assert any("Disable" in evidence or "fallback" in evidence for evidence in payments.evidence)
    assert billing.included_regions == ("Japan",)
    assert any("currency" in hint.casefold() for hint in billing.fallback_compliance_hints)
    assert mobile.included_regions == ("APAC",)
    assert "beta" in mobile.rollout_phase_hints


def test_sourcebrief_model_aliases_serialization_markdown_and_no_source_mutation():
    source = _source_brief(
        source_id="regional-model",
        title="Regional availability | signup",
        summary="Signup should be available in Australia first, excluding China until licensing review.",
        source_payload={"requirements": ["Owner: Product defines waitlist fallback for China."]},
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_regional_availability_requirements(source)
    model_result = generate_source_regional_availability_requirements(model)
    extracted = extract_source_regional_availability_requirements(model)
    payload = source_regional_availability_requirements_to_dict(model_result)
    markdown = source_regional_availability_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_regional_availability_requirements_to_dict(mapping_result)
    assert extracted == model_result.requirements
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_regional_availability_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_regional_availability_requirements_to_dicts(model_result.records) == payload["records"]
    assert list(payload) == ["source_brief_id", "title", "summary", "requirements", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "availability_surface",
        "included_regions",
        "excluded_regions",
        "rollout_phase_hints",
        "fallback_compliance_hints",
        "missing_detail_flags",
        "confidence",
        "evidence",
    ]
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source Regional Availability Requirements Report: regional-model")
    assert (
        "| Surface | Included Regions | Excluded Regions | Phase Hints | Fallback/Compliance | Missing Details | Confidence | Evidence |"
        in markdown
    )
    assert "Regional availability \\| signup" not in markdown
    assert "Australia" in markdown


def test_implementation_brief_and_missing_detail_flags_are_supported():
    result = build_source_regional_availability_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=["Rollout dashboard to EMEA customers."],
                assumptions=["Regional launch owner is TBD."],
                validation_plan="Validate blocked-market behavior after phase is confirmed.",
            )
        )
    )

    dashboard = _surface(result, "dashboard")

    assert result.source_brief_id == "impl-brief"
    assert dashboard.included_regions == ("EMEA",)
    assert "missing_launch_phase" in dashboard.missing_detail_flags
    assert "missing_fallback_behavior" in dashboard.missing_detail_flags
    assert "missing_owner" not in dashboard.missing_detail_flags


def test_data_residency_only_invalid_and_plain_text_inputs_are_deterministic():
    empty = build_source_regional_availability_requirements(
        _source_brief(
            title="Data residency",
            summary="EU data must stay in EU storage and cross-border processing is restricted.",
            source_payload={"body": "No user-facing launch or market availability changes."},
        )
    )
    repeat = build_source_regional_availability_requirements(
        _source_brief(
            title="Data residency",
            summary="EU data must stay in EU storage and cross-border processing is restricted.",
            source_payload={"body": "No user-facing launch or market availability changes."},
        )
    )
    invalid = build_source_regional_availability_requirements(object())
    text = build_source_regional_availability_requirements("Mobile app available in India during pilot.")

    assert empty.to_dict() == repeat.to_dict()
    assert empty.records == ()
    assert empty.summary == {
        "requirement_count": 0,
        "included_region_count": 0,
        "excluded_region_count": 0,
        "included_regions": [],
        "excluded_regions": [],
        "availability_surfaces": [],
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_counts": {
            "missing_region_list": 0,
            "missing_launch_phase": 0,
            "missing_fallback_behavior": 0,
            "missing_owner": 0,
            "missing_legal_compliance_basis": 0,
        },
    }
    assert empty.to_markdown() == repeat.to_markdown()
    assert "No source regional availability requirements were found" in empty.to_markdown()
    assert invalid.source_brief_id is None
    assert invalid.records == ()
    assert text.source_brief_id is None
    assert text.records[0].availability_surface == "mobile app"
    assert text.records[0].included_regions == ("India",)


def _surface(result, name):
    return next(requirement for requirement in result.requirements if requirement.availability_surface == name)


def _source_brief(
    *,
    source_id="source-regional",
    title="Regional availability requirements",
    domain="growth",
    summary="General regional availability requirements.",
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


def _implementation_brief(*, scope=None, assumptions=None, validation_plan="Run rollout validation."):
    return {
        "id": "impl-brief",
        "source_brief_id": "source-brief",
        "title": "Regional dashboard",
        "domain": "growth",
        "target_user": "Ops",
        "buyer": "Growth",
        "workflow_context": "Regional rollout",
        "problem_statement": "Teams need market-specific dashboard availability.",
        "mvp_goal": "Ship dashboard gates.",
        "product_surface": "Dashboard",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [] if assumptions is None else assumptions,
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": validation_plan,
        "definition_of_done": [],
        "status": "draft",
    }
