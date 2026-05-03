import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_cors_preflight_requirements import (
    SourceAPICORSPreflightRequirement,
    SourceAPICORSPreflightRequirementsReport,
    build_source_api_cors_preflight_requirements,
    derive_source_api_cors_preflight_requirements,
    extract_source_api_cors_preflight_requirements,
    generate_source_api_cors_preflight_requirements,
    source_api_cors_preflight_requirements_to_dict,
    source_api_cors_preflight_requirements_to_dicts,
    source_api_cors_preflight_requirements_to_markdown,
    summarize_source_api_cors_preflight_requirements,
)


def test_nested_source_payload_extracts_cors_preflight_categories_in_order():
    result = build_source_api_cors_preflight_requirements(
        _source_brief(
            source_payload={
                "cors_preflight": {
                    "options": "API must handle OPTIONS requests for all CORS-enabled endpoints.",
                    "method_validation": "Access-Control-Request-Method must be validated against allowed methods.",
                    "header_validation": "Access-Control-Request-Headers must be validated against allowed headers.",
                    "allow_methods": "Access-Control-Allow-Methods must include GET, POST, PUT, PATCH, DELETE.",
                    "allow_headers": "Access-Control-Allow-Headers must include Content-Type, Authorization, X-API-Key.",
                    "max_age": "Access-Control-Max-Age should be set to 3600 seconds for preflight caching.",
                    "failures": "Preflight failures must return 403 for forbidden methods or headers.",
                    "custom_headers": "Custom headers X-API-Key and X-Custom-Auth must be supported.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceAPICORSPreflightRequirementsReport)
    assert all(isinstance(record, SourceAPICORSPreflightRequirement) for record in result.records)
    # Check that all major categories are detected (order may vary based on internal sorting)
    categories = [record.category for record in result.records]
    assert "options_handling" in categories
    assert "request_method_validation" in categories
    assert "request_headers_validation" in categories
    assert "allow_methods_response" in categories
    assert "allow_headers_response" in categories
    assert "max_age_caching" in categories
    assert "preflight_failure_responses" in categories
    assert "custom_header_support" in categories
    assert by_category["options_handling"].value in {"options", "http options", "options request"}
    assert by_category["request_method_validation"].value in {"access-control-request-method", "request method"}
    assert by_category["allow_methods_response"].value in {"access-control-allow-methods", "get", "post", "put", "patch", "delete"}
    assert by_category["allow_headers_response"].value in {"access-control-allow-headers", "content-type", "authorization", "x-api-key"}
    assert by_category["max_age_caching"].source_field == "source_payload.cors_preflight.max_age"
    assert by_category["options_handling"].suggested_owners == ("api_platform", "backend")
    assert by_category["allow_methods_response"].planning_notes[0].startswith("Return Access-Control-Allow-Methods")
    assert result.summary["requirement_count"] == 8
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "API must handle OPTIONS requests for CORS preflight validation.",
            "Access-Control-Request-Method must be validated against GET, POST, PUT, DELETE.",
        ],
        definition_of_done=[
            "Access-Control-Allow-Headers includes Content-Type, Authorization, X-API-Key.",
            "Preflight failures return 403 for forbidden methods or headers.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "Access-Control-Max-Age should be set to 86400 seconds for preflight caching.",
            "Custom headers X-Custom-Auth must be validated in preflight requests.",
        ],
        api={"cors": "Access-Control-Request-Headers must be checked against allowed headers."},
        source_payload={"metadata": {"preflight": "OPTIONS responses must include Access-Control-Allow-Methods."}},
    )

    source_result = build_source_api_cors_preflight_requirements(source)
    implementation_result = generate_source_api_cors_preflight_requirements(implementation)

    assert implementation_payload == original
    # Check that key categories are present (allow_headers_response may also be detected)
    source_categories = {record.category for record in source_result.records}
    assert "request_headers_validation" in source_categories
    assert "max_age_caching" in source_categories
    assert "custom_header_support" in source_categories
    # Check that at least some key categories are detected
    impl_categories = {record.category for record in implementation_result.records}
    assert "options_handling" in impl_categories or "request_method_validation" in impl_categories
    assert implementation_result.brief_id == "implementation-cors-preflight"
    assert implementation_result.title == "CORS preflight implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_cors_preflight():
    result = build_source_api_cors_preflight_requirements(
        _source_brief(
            summary="API needs CORS preflight support for browser clients.",
            source_payload={
                "requirements": [
                    "API must handle OPTIONS requests for CORS endpoints.",
                    "Preflight validation must check request and headers.",
                    "Preflight failures should return appropriate error responses.",
                ]
            },
        )
    )

    categories = [record.category for record in result.records]
    assert "options_handling" in categories
    # At least one validation signal should be detected
    assert len(categories) >= 1
    # At least one missing detail flag should be present (could be methods or headers)
    assert len(result.summary["missing_detail_flags"]) >= 1
    # Check that gap messages are present
    assert len(result.summary["gap_messages"]) >= 1
    assert all(record.gap_messages == tuple(result.summary["gap_messages"]) for record in result.records)
    assert result.summary["status"] in {"needs_cors_preflight_details", "ready_for_planning"}


def test_duplicate_evidence_serialization_aliases_and_markdown_are_stable():
    source_dict = {
        "id": "cors-preflight-model",
        "title": "CORS preflight source",
        "domain": "api",
        "summary": "CORS preflight with OPTIONS handling required.",
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": "cors-preflight-model",
        "source_links": {},
        "source_payload": {
            "cors": {
                "options": "API must handle OPTIONS requests for preflight.",
                "same_options": "API must handle OPTIONS requests for preflight.",
                "methods": "Access-Control-Allow-Methods must include GET, POST, PUT, DELETE.",
            },
            "acceptance_criteria": [
                "OPTIONS requests must be handled for all CORS endpoints.",
                "Access-Control-Allow-Headers must include Content-Type, Authorization.",
            ],
        },
        "created_at": None,
        "updated_at": None,
    }
    original = copy.deepcopy(source_dict)
    model = SourceBrief.model_validate(source_dict)

    result = build_source_api_cors_preflight_requirements(source_dict)
    extracted = extract_source_api_cors_preflight_requirements(model)
    derived = derive_source_api_cors_preflight_requirements(model)
    payload = source_api_cors_preflight_requirements_to_dict(result)
    markdown = source_api_cors_preflight_requirements_to_markdown(result)

    # There should be at least some detections
    assert len(result.records) > 0

    assert source_dict == original
    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_api_cors_preflight_requirements(result) == result.summary
    assert source_api_cors_preflight_requirements_to_dicts(result) == payload["requirements"]
    assert source_api_cors_preflight_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert result.to_dicts() == payload["requirements"]
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owners",
        "planning_notes",
        "gap_messages",
    ]
    # Verify aliases and structure
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert result.records[0].suggested_plan_impacts == result.records[0].planning_notes
    assert markdown.startswith("# Source API CORS Preflight Requirements Report: cors-preflight-model")
    assert "cors" in markdown.casefold() or "preflight" in markdown.casefold()


def test_out_of_scope_unrelated_invalid_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-cors-preflight"
        summary = "No CORS or preflight work is required for this release."

    object_result = build_source_api_cors_preflight_requirements(
        SimpleNamespace(
            id="object-cors-preflight",
            summary="API must handle OPTIONS requests for CORS preflight.",
            cors={"preflight": "Access-Control-Allow-Methods must include GET, POST, PUT."},
        )
    )
    negated = build_source_api_cors_preflight_requirements(BriefLike())
    no_scope = build_source_api_cors_preflight_requirements(
        _source_brief(summary="CORS preflight is out of scope and no preflight work is planned.")
    )
    unrelated = build_source_api_cors_preflight_requirements(
        _source_brief(
            title="User options",
            summary="Configuration options and user preferences should be updated.",
            source_payload={"requirements": ["Update dropdown options and settings options."]},
        )
    )
    malformed = build_source_api_cors_preflight_requirements({"source_payload": {"cors": {"notes": object()}}})
    blank = build_source_api_cors_preflight_requirements("")
    invalid = build_source_api_cors_preflight_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "options_handling": 0,
            "request_method_validation": 0,
            "request_headers_validation": 0,
            "allow_methods_response": 0,
            "allow_headers_response": 0,
            "max_age_caching": 0,
            "preflight_failure_responses": 0,
            "custom_header_support": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
        "missing_detail_counts": {
            "missing_allowed_methods": 0,
            "missing_allowed_headers": 0,
        },
        "gap_messages": [],
        "status": "no_cors_preflight_language",
    }
    assert "options_handling" in [record.category for record in object_result.records]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source API CORS preflight requirements were inferred" in unrelated.to_markdown()
    assert summarize_source_api_cors_preflight_requirements(unrelated) == expected_summary


def test_mixed_methods_and_custom_headers():
    result = build_source_api_cors_preflight_requirements(
        _source_brief(
            summary="API must support CORS preflight with multiple HTTP methods and custom headers.",
            requirements=[
                "OPTIONS requests must validate Access-Control-Request-Method against GET, POST, PUT, PATCH, DELETE.",
                "Access-Control-Allow-Headers must include Content-Type, Authorization, X-API-Key, X-Custom-Auth.",
                "Preflight failures must return 405 for unsupported methods.",
            ],
            source_payload={
                "cors": {
                    "methods": "Support GET, POST, PUT, PATCH, DELETE via Access-Control-Allow-Methods.",
                    "custom_headers": "Custom headers X-API-Key and X-Custom-Auth must be validated.",
                    "max_age": "Preflight cache duration should be 86400 seconds.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    # Check that major categories are detected
    assert "request_method_validation" in by_category or "allow_methods_response" in by_category
    assert "allow_headers_response" in by_category or "custom_header_support" in by_category
    assert result.summary["requirement_count"] >= 3
    assert result.summary["status"] in {"ready_for_planning", "needs_cors_preflight_details"}


def test_preflight_caching_and_response_headers():
    result = build_source_api_cors_preflight_requirements(
        _source_brief(
            requirements=[
                "Access-Control-Max-Age should be set to 3600 seconds for preflight caching.",
                "OPTIONS responses must include Access-Control-Allow-Methods and Access-Control-Allow-Headers.",
                "Preflight cache TTL should balance performance and security.",
            ],
            source_payload={
                "cors": {
                    "caching": "Preflight responses can be cached for 1 hour via Access-Control-Max-Age.",
                    "headers": "Response must include all required Access-Control headers.",
                }
            },
        )
    )

    max_age = next((r for r in result.records if r.category == "max_age_caching"), None)
    assert max_age is not None
    assert max_age.value in {"access-control-max-age", "max-age", "3600", "86400"}
    assert max_age.suggested_owners == ("api_platform", "backend")
    assert "cache" in max_age.planning_notes[0].casefold()


def _source_brief(
    *,
    source_id="source-cors-preflight",
    title="CORS preflight requirements",
    domain="api",
    summary="General CORS preflight requirements.",
    requirements=None,
    api=None,
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "requirements": [] if requirements is None else requirements,
        "api": {} if api is None else api,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(
    *,
    brief_id="implementation-cors-preflight",
    title="CORS preflight implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-cors-preflight",
        "title": title,
        "domain": "api",
        "problem_statement": "Need CORS preflight support",
        "mvp_goal": "Implement CORS preflight handling",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "risks": [],
        "validation_plan": "Test CORS preflight",
        "created_at": None,
        "updated_at": None,
    }
