import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_etag_requirements import (
    SourceAPIETagRequirement,
    SourceAPIETagRequirementsReport,
    build_source_api_etag_requirements,
    derive_source_api_etag_requirements,
    extract_source_api_etag_requirements,
    generate_source_api_etag_requirements,
    source_api_etag_requirements_to_dict,
    source_api_etag_requirements_to_dicts,
    source_api_etag_requirements_to_markdown,
    summarize_source_api_etag_requirements,
)


def test_nested_source_payload_extracts_etag_categories_in_order():
    result = build_source_api_etag_requirements(
        _source_brief(
            source_payload={
                "etag": {
                    "generation": "API responses must generate strong ETags using SHA-256 hash of entity content.",
                    "headers": "ETag header must be included in all GET and HEAD responses.",
                    "validation": "API must validate If-None-Match header and compare with current ETag.",
                    "not_modified": "API must return 304 Not Modified when ETag matches If-None-Match.",
                    "last_modified": "Last-Modified header should be included for cache validation.",
                    "cache_key": "Cache key computation must use MD5 hash of entity attributes.",
                    "versioning": "Entity version field must track resource revisions for optimistic locking.",
                    "concurrent": "Concurrent update detection must prevent lost updates via ETag comparison.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceAPIETagRequirementsReport)
    assert all(isinstance(record, SourceAPIETagRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "etag_generation",
        "etag_header_inclusion",
        "if_none_match_validation",
        "not_modified_responses",
        "last_modified_headers",
        "cache_key_computation",
        "entity_versioning",
        "concurrent_update_detection",
    ]
    assert by_category["etag_generation"].value in {"strong", "hash", "sha"}
    assert by_category["if_none_match_validation"].value in {"if-none-match", "validate", "validation"}
    assert by_category["not_modified_responses"].value in {"304", "not modified"}
    assert by_category["cache_key_computation"].value in {"cache key", "md5", "hash"}
    assert by_category["etag_generation"].source_field == "source_payload.etag.generation"
    assert by_category["etag_generation"].suggested_owners == ("api_platform", "backend")
    assert by_category["etag_generation"].planning_notes[0].startswith("Define ETag generation strategy")
    assert result.summary["requirement_count"] == 8
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "API responses must include ETag header with strong entity tags.",
            "If-None-Match validation should return 304 Not Modified for cache hits.",
        ],
        definition_of_done=[
            "ETag generation uses SHA-256 hash of entity content.",
            "Concurrent update detection prevents lost updates via ETag comparison.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "Last-Modified headers must be included for cache validation.",
            "Entity versioning must track resource revisions for optimistic locking.",
        ],
        api={"cache": "Cache key computation must use content hash for cache invalidation."},
        source_payload={"metadata": {"etag": "ETag header must be returned in all API responses."}},
    )

    source_result = build_source_api_etag_requirements(source)
    implementation_result = generate_source_api_etag_requirements(implementation)

    assert implementation_payload == original
    # The extractor finds additional signals based on context
    source_categories = [record.category for record in source_result.records]
    assert "etag_header_inclusion" in source_categories
    assert "last_modified_headers" in source_categories
    assert "cache_key_computation" in source_categories
    assert "entity_versioning" in source_categories
    etag_header_record = next(r for r in source_result.records if r.category == "etag_header_inclusion")
    assert etag_header_record.source_field == "source_payload.metadata.etag"
    assert {
        "etag_generation",
        "if_none_match_validation",
        "not_modified_responses",
        "concurrent_update_detection",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-etag"
    assert implementation_result.title == "ETag implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_etag():
    result = build_source_api_etag_requirements(
        _source_brief(
            summary="API needs ETag support for cache validation.",
            source_payload={
                "requirements": [
                    "API responses must include ETag header for caching.",
                    "Cache validation should reduce bandwidth usage.",
                    "Entity versioning may be used for conflict detection.",
                ]
            },
        )
    )

    categories = [record.category for record in result.records]
    assert "etag_header_inclusion" in categories
    assert "entity_versioning" in categories
    assert result.summary["missing_detail_flags"] == [
        "missing_etag_strategy",
        "missing_validation_logic",
    ]
    assert "Specify ETag generation strategy (strong vs weak) and hash algorithm to use." in result.summary["gap_messages"]
    assert "Define If-None-Match validation logic and ETag comparison semantics." in result.summary["gap_messages"]
    assert all(record.gap_messages == tuple(result.summary["gap_messages"]) for record in result.records)
    assert result.summary["missing_detail_counts"]["missing_etag_strategy"] >= 1
    assert result.summary["status"] == "needs_etag_details"


def test_duplicate_evidence_serialization_aliases_and_markdown_are_stable():
    source = _source_brief(
        source_id="etag-model",
        title="ETag source",
        summary="ETag source.",
        source_payload={
            "etag": {
                "generation": "Strong ETags must be generated using SHA-256 hash.",
                "same_generation": "Strong ETags must be generated using SHA-256 hash.",
                "validation": "If-None-Match validation must compare request ETag with current ETag.",
            },
            "acceptance_criteria": [
                "Strong ETags must be generated using SHA-256 hash.",
                "304 Not Modified response must be returned when ETag matches.",
            ],
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(
        {
            key: value
            for key, value in source.items()
            if key not in {"requirements", "api"}
        }
    )

    result = build_source_api_etag_requirements(source)
    extracted = extract_source_api_etag_requirements(model)
    derived = derive_source_api_etag_requirements(model)
    payload = source_api_etag_requirements_to_dict(result)
    markdown = source_api_etag_requirements_to_markdown(result)
    generation = next(record for record in result.records if record.category == "etag_generation")

    assert source == original
    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_api_etag_requirements(result) == result.summary
    assert source_api_etag_requirements_to_dicts(result) == payload["requirements"]
    assert source_api_etag_requirements_to_dicts(result.records) == payload["records"]
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
    # Evidence should be deduplicated and sorted
    assert len(generation.evidence) == 1
    assert "Strong ETags must be generated using SHA-256 hash" in generation.evidence[0]
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert result.records[0].suggested_plan_impacts == result.records[0].planning_notes
    assert markdown.startswith("# Source API ETag Requirements Report: etag-model")
    assert "etag" in markdown.casefold()


def test_out_of_scope_unrelated_invalid_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-etag"
        summary = "No ETag or cache validation work is required for this release."

    object_result = build_source_api_etag_requirements(
        SimpleNamespace(
            id="object-etag",
            summary="API responses must include ETag header with strong entity tags.",
            etag={"generation": "Strong ETags require SHA-256 hash of entity content."},
        )
    )
    negated = build_source_api_etag_requirements(BriefLike())
    no_scope = build_source_api_etag_requirements(
        _source_brief(summary="ETag support is out of scope and no cache validation work is planned.")
    )
    unrelated = build_source_api_etag_requirements(
        _source_brief(
            title="Tag copy",
            summary="Price tag labels and gift tag wording should be updated.",
            source_payload={"requirements": ["Update meta tag descriptions and HTML tag attributes."]},
        )
    )
    malformed = build_source_api_etag_requirements({"source_payload": {"etag": {"notes": object()}}})
    blank = build_source_api_etag_requirements("")
    invalid = build_source_api_etag_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "etag_generation": 0,
            "etag_header_inclusion": 0,
            "if_none_match_validation": 0,
            "not_modified_responses": 0,
            "last_modified_headers": 0,
            "cache_key_computation": 0,
            "entity_versioning": 0,
            "concurrent_update_detection": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
        "missing_detail_counts": {
            "missing_etag_strategy": 0,
            "missing_validation_logic": 0,
        },
        "gap_messages": [],
        "status": "no_etag_language",
    }
    assert "etag_generation" in [record.category for record in object_result.records]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source API ETag requirements were inferred" in unrelated.to_markdown()
    assert summarize_source_api_etag_requirements(unrelated) == expected_summary


def test_weak_vs_strong_etags_and_cache_key_algorithms():
    result = build_source_api_etag_requirements(
        _source_brief(
            summary="API must support both weak and strong ETags for cache validation.",
            requirements=[
                "Strong ETags must use SHA-256 hash for exact content matching.",
                "Weak ETags may use entity version number for semantic equivalence.",
                "Cache key computation should combine entity ID and version.",
            ],
            source_payload={
                "etag": {
                    "strategy": "Support both W/ weak ETags and strong ETags based on endpoint.",
                    "validation": "If-None-Match should handle both weak and strong ETag comparison.",
                    "last_modified": "Last-Modified header with timestamp precision to seconds.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert "etag_generation" in by_category
    assert "if_none_match_validation" in by_category
    assert "cache_key_computation" in by_category
    assert "last_modified_headers" in by_category
    assert result.summary["requirement_count"] >= 4
    assert result.summary["status"] in {"ready_for_planning", "needs_etag_details"}


def test_concurrent_update_detection_and_optimistic_locking():
    result = build_source_api_etag_requirements(
        _source_brief(
            requirements=[
                "API must detect concurrent updates via ETag comparison on PUT/PATCH.",
                "Optimistic locking should prevent lost updates using entity version.",
                "412 Precondition Failed must be returned on ETag mismatch for updates.",
            ],
            source_payload={
                "etag": {
                    "concurrent": "Concurrent update detection prevents race conditions via ETag validation.",
                    "versioning": "Entity version increments on each update for optimistic concurrency control.",
                }
            },
        )
    )

    concurrent = next((r for r in result.records if r.category == "concurrent_update_detection"), None)
    versioning = next((r for r in result.records if r.category == "entity_versioning"), None)
    assert concurrent is not None
    assert versioning is not None
    assert concurrent.value in {"concurrent", "race condition", "optimistic lock", "conflict"}
    assert concurrent.suggested_owners == ("api_platform", "backend")
    assert "concurrent" in concurrent.planning_notes[0].casefold()


def _source_brief(
    *,
    source_id="source-etag",
    title="ETag requirements",
    domain="api",
    summary="General ETag requirements.",
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
    brief_id="implementation-etag",
    title="ETag implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-etag",
        "title": title,
        "domain": "api",
        "target_user": "developer",
        "buyer": "platform",
        "workflow_context": "API developers need ETag planning.",
        "problem_statement": "ETag requirements need to be extracted early.",
        "mvp_goal": "Plan ETag generation, validation, cache keys, and concurrent update detection.",
        "product_surface": "api",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run ETag extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
