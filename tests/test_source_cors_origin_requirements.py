import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_cors_origin_requirements import (
    SourceCORSOriginRequirement,
    SourceCORSOriginRequirementsReport,
    build_source_cors_origin_requirements,
    build_source_cors_origin_requirements_report,
    derive_source_cors_origin_requirements,
    extract_source_cors_origin_requirements,
    generate_source_cors_origin_requirements,
    source_cors_origin_requirements_to_dict,
    source_cors_origin_requirements_to_dicts,
    source_cors_origin_requirements_to_markdown,
    summarize_source_cors_origin_requirements,
)


def test_structured_payload_extracts_all_cors_origin_categories_in_order():
    result = build_source_cors_origin_requirements(
        _source_brief(
            source_payload={
                "cors": {
                    "trusted_origins": "Allowed origins must include https://app.example.com and https://admin.example.com.",
                    "credentials": "Credentialed requests must use Access-Control-Allow-Credentials for session cookies.",
                    "preflight": "OPTIONS preflight must allow Authorization and X-Tenant-ID custom headers.",
                    "wildcard": "Wildcard origins must be blocked; do not use Access-Control-Allow-Origin: *.",
                    "environments": "Dev origins use http://localhost:3000, staging origins use https://staging.example.com, and production origins use https://app.example.com.",
                    "browser": "Browser clients and the SPA frontend must call the API cross-origin.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceCORSOriginRequirementsReport)
    assert all(isinstance(record, SourceCORSOriginRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "trusted_origin",
        "credentialed_request",
        "preflight",
        "wildcard_block",
        "environment_origin",
        "browser_client",
    ]
    assert by_category["trusted_origin"].confidence == "high"
    assert by_category["trusted_origin"].origins == ("https://admin.example.com", "https://app.example.com")
    assert by_category["credentialed_request"].planning_note.startswith("Plan credentials policy")
    assert by_category["preflight"].source_field == "source_payload.cors.preflight"
    assert by_category["wildcard_block"].origins == ("*",)
    assert by_category["environment_origin"].environment == "dev, local, staging, production"
    assert "http://localhost:3000" in by_category["environment_origin"].origins
    assert by_category["browser_client"].requirement_category == "browser_client"
    assert result.summary["requirement_count"] == 6
    assert result.summary["category_counts"]["preflight"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_natural_language_extraction_duplicates_collapse_and_ordering_is_stable():
    result = build_source_cors_origin_requirements(
        """
# CORS rollout

- Browser clients require CORS for the SPA frontend.
- Trusted origins must include https://app.example.com and https://admin.example.com.
- trusted origins must include https://app.example.com and https://admin.example.com.
- Credentialed requests should include cookies with Access-Control-Allow-Credentials.
- Preflight OPTIONS should allow Authorization and X-Requested-With headers.
- Do not use wildcard origins for credentialed browser requests.
"""
    )

    by_category = {record.category: record for record in result.records}

    assert [record.category for record in result.records] == [
        "trusted_origin",
        "credentialed_request",
        "preflight",
        "wildcard_block",
        "browser_client",
    ]
    assert len(by_category["trusted_origin"].evidence) == 1
    assert by_category["trusted_origin"].origins == ("https://admin.example.com", "https://app.example.com")
    assert by_category["credentialed_request"].confidence == "high"
    assert any("Access-Control-Allow-Credentials" in item for item in by_category["credentialed_request"].evidence)
    assert by_category["preflight"].unresolved_questions == ()
    assert by_category["wildcard_block"].category == by_category["wildcard_block"].requirement_category


def test_model_object_serialization_markdown_aliases_and_no_mutation_are_stable():
    source = _source_brief(
        source_id="cors-model",
        summary="CORS trusted origins must include https://app.example.com for browser clients.",
        source_payload={
            "cors": {
                "credentials": "Credentialed requests must support session cookies.",
                "environment_origins": "Preview origins must include https://preview.example.com.",
            }
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            integration_points=[
                "Preflight OPTIONS must allow Authorization and X-Client-Version headers.",
                "Wildcard origins must be blocked for browser clients.",
            ]
        )
    )
    obj = SimpleNamespace(
        id="object-cors",
        cors="Allowed origins must include http://localhost:5173 for dev browser clients.",
    )

    result = build_source_cors_origin_requirements(source)
    report_result = build_source_cors_origin_requirements_report(model)
    generated = generate_source_cors_origin_requirements(model)
    derived = derive_source_cors_origin_requirements(model)
    extracted = extract_source_cors_origin_requirements(model)
    implementation_result = build_source_cors_origin_requirements(implementation)
    object_result = build_source_cors_origin_requirements(obj)
    payload = source_cors_origin_requirements_to_dict(generated)
    markdown = source_cors_origin_requirements_to_markdown(generated)

    assert source == original
    assert report_result.to_dict() == result.to_dict()
    assert generated.to_dict() == result.to_dict()
    assert derived.to_dict() == generated.to_dict()
    assert extracted == generated.requirements
    assert summarize_source_cors_origin_requirements(generated) == generated.summary
    assert source_cors_origin_requirements_to_dicts(generated) == payload["requirements"]
    assert source_cors_origin_requirements_to_dicts(generated.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert generated.records == generated.requirements
    assert generated.findings == generated.requirements
    assert list(payload) == ["source_id", "title", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "origins",
        "environment",
        "source_field",
        "evidence",
        "confidence",
        "planning_note",
        "unresolved_questions",
    ]
    assert markdown == generated.to_markdown()
    assert markdown.startswith("# Source CORS Origin Requirements Report: cors-model")
    assert "| Category | Origins | Environment | Confidence | Source | Evidence | Planning Note | Unresolved Questions |" in markdown
    assert "https://preview.example.com" in markdown
    assert [record.category for record in implementation_result.records] == [
        "preflight",
        "wildcard_block",
        "browser_client",
    ]
    assert [record.category for record in object_result.records] == [
        "trusted_origin",
        "environment_origin",
        "browser_client",
    ]
    assert object_result.records[0].origin_requirements == ("http://localhost:5173",)


def test_out_of_scope_invalid_and_repeated_inputs_are_stable_empty_reports():
    class BriefLike:
        id = "object-no-cors"
        summary = "CORS, trusted origins, browser clients, and preflight work are out of scope for this release."

    empty = build_source_cors_origin_requirements(
        _source_brief(
            source_id="empty-cors",
            title="API help copy",
            summary="Keep API help copy unchanged. No CORS, preflight, or browser origin changes are required.",
        )
    )
    repeat = build_source_cors_origin_requirements(
        _source_brief(
            source_id="empty-cors",
            title="API help copy",
            summary="Keep API help copy unchanged. No CORS, preflight, or browser origin changes are required.",
        )
    )
    negated = build_source_cors_origin_requirements(BriefLike())
    invalid = build_source_cors_origin_requirements(b"not text")

    expected_summary = {
        "requirement_count": 0,
        "category_counts": {
            "trusted_origin": 0,
            "credentialed_request": 0,
            "preflight": 0,
            "wildcard_block": 0,
            "environment_origin": 0,
            "browser_client": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
        "origin_signals": [],
        "status": "no_cors_origin_requirements_found",
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-cors"
    assert empty.requirements == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No source CORS origin requirements were inferred." in empty.to_markdown()
    assert negated.requirements == ()
    assert invalid.source_id is None
    assert invalid.requirements == ()
    assert invalid.summary == expected_summary


def test_ambiguous_origin_words_without_cors_context_do_not_create_requirements():
    result = build_source_cors_origin_requirements(
        _source_brief(
            title="Origin story content",
            summary="Update the product origin story and browser screenshots for the marketing site.",
            source_payload={
                "notes": [
                    "No CORS, cross-origin, trusted origin, or preflight changes are required.",
                    "The frontend copy mentions where the company originated.",
                ],
            },
        )
    )

    assert result.records == ()


def _source_brief(
    *,
    source_id="cors-source",
    title="CORS origin requirements",
    domain="api",
    summary="General API origin requirements.",
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


def _implementation_brief(*, integration_points=None):
    return {
        "id": "impl-cors",
        "source_brief_id": "source-cors",
        "title": "Browser API rollout",
        "domain": "api",
        "target_user": "developers",
        "buyer": None,
        "workflow_context": "Browser API clients require CORS planning before launch.",
        "problem_statement": "CORS origin requirements need planning coverage.",
        "mvp_goal": "Plan browser endpoint access.",
        "product_surface": "api",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [] if integration_points is None else integration_points,
        "risks": [],
        "validation_plan": "Review generated plan for CORS coverage.",
        "definition_of_done": ["CORS origin requirements are represented."],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
