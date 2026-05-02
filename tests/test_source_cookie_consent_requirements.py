import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_cookie_consent_requirements import (
    SourceCookieConsentRequirement,
    SourceCookieConsentRequirementsReport,
    build_source_cookie_consent_requirements,
    derive_source_cookie_consent_requirements,
    extract_source_cookie_consent_requirements,
    generate_source_cookie_consent_requirements,
    source_cookie_consent_requirements_to_dict,
    source_cookie_consent_requirements_to_dicts,
    source_cookie_consent_requirements_to_markdown,
    summarize_source_cookie_consent_requirements,
)


def test_extracts_cookie_consent_requirements_from_markdown_in_stable_order():
    result = build_source_cookie_consent_requirements(
        _source_brief(
            source_payload={
                "body": """
# Cookie consent requirements

- Cookie banner must show Accept all, Reject all, and Manage choices before tracking.
- Cookie categories must include necessary, functional, analytics, and marketing cookies.
- Meta Pixel and Google Tag Manager must be blocked until consent.
- Analytics opt-in should use consent mode for GA4 before product analytics fires.
- GDPR, ePrivacy, CPRA, and California visitors require regional consent rules.
- Preference center must let users manage cookie settings by category.
- Users must withdraw consent, opt out, and delete cookies from settings.
- Consent events must be recorded in an audit log and exported as proof of consent.
- Cookie expiration and consent record retention must be 13 months.
"""
            }
        )
    )

    assert isinstance(result, SourceCookieConsentRequirementsReport)
    assert all(isinstance(record, SourceCookieConsentRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "consent_banner",
        "cookie_category",
        "tracking_pixel",
        "analytics_opt_in",
        "regional_consent",
        "preference_center",
        "withdrawal",
        "audit_evidence",
        "retention_expiration",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["consent_banner"].value == "accept all, manage choices"
    assert by_type["cookie_category"].categories == (
        "necessary",
        "functional",
        "analytics",
        "marketing",
    )
    assert by_type["tracking_pixel"].value == "meta pixel"
    assert by_type["analytics_opt_in"].value == "analytics opt-in"
    assert by_type["regional_consent"].regions == ("gdpr", "eprivacy", "cpra", "california")
    assert by_type["preference_center"].value == "preference center"
    assert by_type["withdrawal"].value == "withdraw consent, delete cookies"
    assert by_type["audit_evidence"].confidence == "high"
    assert by_type["retention_expiration"].value == "13 months"
    assert result.summary["requirement_count"] == 9
    assert result.summary["requirement_type_counts"]["tracking_pixel"] == 1
    assert result.summary["category_counts"]["analytics"] == 2
    assert result.summary["region_counts"]["gdpr"] == 1
    assert result.summary["status"] == "ready_for_cookie_consent_planning"


def test_structured_payload_and_implementation_brief_are_supported():
    structured = build_source_cookie_consent_requirements(
        {
            "id": "structured-cookie",
            "title": "Cookie consent rollout",
            "metadata": {
                "cookie_consent": {
                    "banner": "Consent banner must show Accept all and Reject all.",
                    "categories": "Necessary, analytics, and advertising cookies use category toggles.",
                    "pixels": "Tracking pixels and Google Tag Manager are blocked until opt-in.",
                    "regions": "GDPR and UK users require regional consent.",
                    "preference_center": "Cookie settings must expose manage preferences.",
                    "withdrawal": "Users can revoke consent and clear cookies.",
                    "evidence": "Consent receipt and consent log export are required.",
                    "expiration": "Consent duration expires after 12 months.",
                }
            },
        }
    )
    implementation = generate_source_cookie_consent_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=[
                    "Cookie banner must block session replay and marketing pixels until consent.",
                    "Analytics opt-in requires GA4 consent mode before product analytics events.",
                ],
                definition_of_done=[
                    "Preference center supports withdrawal and audit evidence export for consent events.",
                    "Cookie expiration is 180 days for marketing cookies.",
                ],
            )
        )
    )
    object_result = build_source_cookie_consent_requirements(
        SimpleNamespace(
            id="object-cookie",
            metadata={"tracking": "Analytics opt-in must keep tracking default-off until consent."},
        )
    )

    assert [record.requirement_type for record in structured.records] == [
        "consent_banner",
        "cookie_category",
        "tracking_pixel",
        "regional_consent",
        "preference_center",
        "withdrawal",
        "audit_evidence",
        "retention_expiration",
    ]
    by_type = {record.requirement_type: record for record in structured.records}
    assert by_type["consent_banner"].source_field == "metadata.cookie_consent"
    assert by_type["cookie_category"].categories == ("necessary", "analytics", "advertising")
    assert by_type["regional_consent"].regions == ("gdpr", "uk")
    assert by_type["audit_evidence"].value == "consent receipt"
    assert by_type["retention_expiration"].value == "12 months"

    assert implementation.source_id == "implementation-cookie"
    assert [record.requirement_type for record in implementation.records] == [
        "consent_banner",
        "cookie_category",
        "tracking_pixel",
        "analytics_opt_in",
        "preference_center",
        "withdrawal",
        "audit_evidence",
        "retention_expiration",
    ]
    assert implementation.records[0].source_field == "scope[0]"
    assert object_result.records[0].requirement_type == "analytics_opt_in"


def test_negated_no_impact_malformed_and_blank_inputs_return_stable_empty_report():
    empty = build_source_cookie_consent_requirements(
        _source_brief(
            title="Web copy update",
            summary="No cookie consent, cookie banner, tracking, analytics, pixels, or CMP changes are required for this release.",
            source_payload={"requirements": ["Update help center copy."]},
        )
    )
    repeat = build_source_cookie_consent_requirements(
        _source_brief(
            title="Web copy update",
            summary="No cookie consent, cookie banner, tracking, analytics, pixels, or CMP changes are required for this release.",
            source_payload={"requirements": ["Update help center copy."]},
        )
    )
    malformed = build_source_cookie_consent_requirements({"source_payload": {"notes": object()}})
    blank = build_source_cookie_consent_requirements("")
    object_empty = build_source_cookie_consent_requirements(
        SimpleNamespace(id="object-empty", summary="Cookie consent has no impact and no tracking work is needed.")
    )

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "requirement_types": [],
        "requirement_type_counts": {
            "consent_banner": 0,
            "cookie_category": 0,
            "tracking_pixel": 0,
            "analytics_opt_in": 0,
            "regional_consent": 0,
            "preference_center": 0,
            "withdrawal": 0,
            "audit_evidence": 0,
            "retention_expiration": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "category_counts": {
            "necessary": 0,
            "functional": 0,
            "performance": 0,
            "analytics": 0,
            "marketing": 0,
            "advertising": 0,
        },
        "region_counts": {
            "gdpr": 0,
            "eprivacy": 0,
            "eu": 0,
            "eea": 0,
            "uk": 0,
            "ccpa": 0,
            "cpra": 0,
            "california": 0,
        },
        "status": "no_cookie_consent_language",
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert malformed.records == ()
    assert blank.records == ()
    assert object_empty.records == ()
    assert "No source cookie consent requirements were inferred." in empty.to_markdown()


def test_duplicate_merging_preserves_evidence_source_fields_and_markdown_escaping():
    result = build_source_cookie_consent_requirements(
        _source_brief(
            source_id="cookie-dedupe",
            source_payload={
                "requirements": [
                    "Cookie banner must show Accept all for customer | partner markets.",
                    "Cookie banner must show Accept all for customer | partner markets.",
                    "Consent banner must show Reject all and Manage choices.",
                ],
                "acceptance_criteria": [
                    "Cookie banner must show Accept all for customer | partner markets.",
                ],
            },
        )
    )

    assert [record.requirement_type for record in result.records] == ["consent_banner"]
    banner = result.records[0]
    assert banner.evidence == (
        "source_payload.acceptance_criteria[0]: Cookie banner must show Accept all for customer | partner markets.",
        "source_payload.requirements[2]: Consent banner must show Reject all and Manage choices.",
    )
    assert banner.source_fields == (
        "source_payload.acceptance_criteria[0]",
        "source_payload.requirements[0]",
        "source_payload.requirements[1]",
        "source_payload.requirements[2]",
    )
    markdown = result.to_markdown()
    assert "| Source Brief | Requirement Type | Requirement | Value | Categories | Regions | Source Field | Source Fields | Confidence | Missing Detail Guidance | Evidence |" in markdown
    assert "customer \\| partner markets" in markdown


def test_aliases_serialization_json_ordering_and_no_input_mutation_are_stable():
    source = _source_brief(
        source_id="cookie-model",
        source_payload={
            "requirements": [
                "Cookie categories must include necessary and analytics cookies.",
                "Marketing pixels must be blocked until consent.",
                "GDPR regional consent requires consent log audit evidence.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_cookie_consent_requirements(source)
    model_result = extract_source_cookie_consent_requirements(model)
    derived = derive_source_cookie_consent_requirements(model)
    generated = generate_source_cookie_consent_requirements(model)
    text_result = build_source_cookie_consent_requirements("Cookie banner must show Reject all and Manage choices.")
    payload = source_cookie_consent_requirements_to_dict(model_result)
    markdown = source_cookie_consent_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_cookie_consent_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_cookie_consent_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_cookie_consent_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_cookie_consent_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_type",
        "requirement_text",
        "value",
        "categories",
        "regions",
        "source_field",
        "source_fields",
        "evidence",
        "matched_terms",
        "confidence",
        "missing_detail_guidance",
    ]
    assert [record.requirement_type for record in model_result.records] == [
        "cookie_category",
        "tracking_pixel",
        "regional_consent",
        "audit_evidence",
    ]
    assert model_result.records[0].category == "cookie_category"
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source Cookie Consent Requirements Report: cookie-model")
    assert text_result.records[0].requirement_type == "consent_banner"


def _source_brief(
    *,
    source_id="source-cookie",
    title="Cookie consent requirements",
    domain="privacy",
    summary="General cookie consent requirements.",
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


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "implementation-cookie",
        "source_brief_id": "source-cookie",
        "title": "Cookie consent rollout",
        "domain": "privacy",
        "target_user": "privacy ops",
        "buyer": "legal",
        "workflow_context": "Teams need cookie consent requirements before frontend and analytics planning.",
        "problem_statement": "Cookie consent requirements need to be extracted early.",
        "mvp_goal": "Plan cookie consent work from source briefs.",
        "product_surface": "web app",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run cookie consent extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
