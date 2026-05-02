import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_vendor_dependencies import (
    SourceVendorDependenciesReport,
    SourceVendorDependency,
    build_source_vendor_dependencies,
    generate_source_vendor_dependencies,
    source_vendor_dependencies_to_dict,
    source_vendor_dependencies_to_markdown,
)


def test_detects_vendor_dependencies_across_brief_fields_with_checks_and_questions():
    result = build_source_vendor_dependencies(
        _source_brief(
            summary="Checkout must use Stripe payments with PCI cardholder data and webhook uptime requirements.",
            source_payload={
                "constraints": [
                    "Auth0 OAuth credentials and SSO claims are required before launch.",
                    "Segment analytics cannot receive PII until consent rules are confirmed.",
                    "SendGrid email provider has rate limit and sender domain constraints.",
                ],
                "risks": ["AWS SQS queue quota could throttle order events during outages."],
                "metadata": {"marketplace_app": "Shopify app contract terms need app review."},
            },
        )
    )

    assert isinstance(result, SourceVendorDependenciesReport)
    assert all(isinstance(dependency, SourceVendorDependency) for dependency in result.dependencies)
    by_vendor_type = {(dependency.vendor_name, dependency.dependency_type): dependency for dependency in result.dependencies}

    assert by_vendor_type[("Stripe", "payment_provider")].confidence == "high"
    assert any("PCI" in item for item in by_vendor_type[("Stripe", "payment_provider")].evidence)
    assert any("credential" in item.lower() for item in by_vendor_type[("Auth0", "identity_provider")].recommended_checks)
    assert any("legal" in item.lower() for item in by_vendor_type[("Shopify", "marketplace_app")].planning_questions)
    assert ("Segment", "analytics_tool") in by_vendor_type
    assert ("SendGrid", "email_sms_provider") in by_vendor_type
    assert ("AWS", "cloud_service") in by_vendor_type
    assert ("Shopify", "marketplace_app") in by_vendor_type
    assert result.summary["dependency_type_counts"]["payment_provider"] == 1
    assert result.summary["dependency_type_counts"]["identity_provider"] == 1
    assert result.summary["high_attention_count"] >= 5


def test_duplicate_vendor_evidence_is_normalized_with_stable_ordering():
    result = build_source_vendor_dependencies(
        {
            "id": "dupes",
            "summary": "Use Stripe API for checkout. Use Stripe API for checkout.",
            "constraints": [
                "Stripe API has rate limits.",
                "Provider: Stripe API has rate limits.",
            ],
            "risks": ["stripe outage affects checkout uptime."],
            "metadata": {"processor": "Stripe"},
        }
    )

    stripe_api = next(
        dependency
        for dependency in result.dependencies
        if dependency.vendor_name == "Stripe" and dependency.dependency_type == "api"
    )

    assert stripe_api.evidence == tuple(sorted(set(stripe_api.evidence), key=lambda item: item.casefold()))
    assert len(stripe_api.evidence) == len(set(stripe_api.evidence))
    assert result.summary["vendors"].count("Stripe") == sum(
        1 for dependency in result.dependencies if dependency.vendor_name == "Stripe"
    )


def test_mapping_and_sourcebrief_inputs_match_and_serialize_to_json_compatible_payload():
    source = _source_brief(
        source_id="vendor-model",
        summary="Slack marketplace app needs OAuth scopes and contract approval.",
        source_payload={
            "requirements": ["Twilio SMS API sends OTP codes with rate limit handling."],
            "metadata": {"identity_provider": "Okta SSO requires uptime SLA."},
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_vendor_dependencies(source)
    model_result = generate_source_vendor_dependencies(model)
    payload = source_vendor_dependencies_to_dict(model_result)
    markdown = source_vendor_dependencies_to_markdown(model_result)

    assert source == original
    assert payload == source_vendor_dependencies_to_dict(mapping_result)
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.to_dicts() == payload["dependencies"]
    assert list(payload) == ["brief_id", "dependencies", "summary"]
    assert list(payload["dependencies"][0]) == [
        "vendor_name",
        "dependency_type",
        "confidence",
        "evidence",
        "recommended_checks",
        "planning_questions",
    ]
    assert markdown.startswith("# Source Vendor Dependencies Report: vendor-model")
    assert "| Vendor | Type | Confidence | Evidence | Recommended Checks | Planning Questions |" in markdown


def test_generic_api_sdk_and_empty_inputs_are_handled():
    result = build_source_vendor_dependencies(
        {
            "id": "generic",
            "body": "A third-party SDK and external REST API are required, but the vendor is TBD.",
        }
    )
    empty = build_source_vendor_dependencies({"id": "empty", "summary": "Update internal copy only."})
    invalid = build_source_vendor_dependencies("not a source brief")

    assert any(dependency.dependency_type == "sdk" and dependency.vendor_name == "" for dependency in result.dependencies)
    assert any("Which third-party vendor" in question for dependency in result.dependencies for question in dependency.planning_questions)
    assert empty.brief_id == "empty"
    assert empty.dependencies == ()
    assert empty.summary["dependency_count"] == 0
    assert "No vendor dependencies were found" in empty.to_markdown()
    assert invalid.brief_id is None
    assert invalid.dependencies == ()


def _source_brief(
    *,
    source_id="sb-vendors",
    title="Vendor dependencies",
    domain="platform",
    summary="General vendor requirements.",
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
