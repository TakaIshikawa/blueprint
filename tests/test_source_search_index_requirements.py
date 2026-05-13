import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_search_index_requirements import (
    SourceSearchIndexRequirement,
    SourceSearchIndexRequirementsReport,
    build_source_search_index_requirements,
    derive_source_search_index_requirements,
    extract_source_search_index_requirements,
    generate_source_search_index_requirements,
    source_search_index_requirements_to_dict,
    source_search_index_requirements_to_dicts,
    source_search_index_requirements_to_markdown,
    summarize_source_search_index_requirements,
)


def test_extracts_search_index_categories_in_stable_order():
    result = build_source_search_index_requirements(
        _source_brief(
            source_payload={
                "indexing": {
                    "entity": "Product search must index products and catalog records.",
                    "mapping": "Field mapping must map title, description, SKU, status, and tags.",
                    "analysis": "Analyzer and tokenizer requirements include stemming and synonyms.",
                    "backfill": "Reindex backfill must rebuild index data for historical records.",
                    "freshness": "Index freshness must refresh within 5 minutes of product updates.",
                    "ranking": "Ranking and sorting must boost exact matches and support recency sort.",
                    "access": "Access filtering must enforce tenant filter and permission filter rules.",
                    "monitoring": "Search observability must monitor index lag, metrics, and indexing failures.",
                }
            }
        )
    )

    assert isinstance(result, SourceSearchIndexRequirementsReport)
    assert all(isinstance(record, SourceSearchIndexRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "indexed_entity",
        "field_mapping",
        "analyzer_tokenizer",
        "reindex_backfill",
        "freshness_lag",
        "ranking_sorting",
        "access_filtering",
        "observability",
    ]
    assert result.summary["requirement_count"] == 8
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"
    assert result.records[0].suggested_owners == ("search_platform", "backend")


def test_models_objects_strings_and_missing_summary_are_supported():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Document search must index documents with permission filter enforcement.",
                "Search index freshness must refresh within 1 minute and emit metrics.",
            ]
        )
    )
    source = SourceBrief.model_validate(_source_brief(summary="Product search indexing must index products for catalog lookup."))
    object_result = build_source_search_index_requirements(
        SimpleNamespace(id="object-search", summary="Search index analyzer must use tokenizer synonyms for product names.")
    )
    text_result = build_source_search_index_requirements("Search index reindex backfill must rebuild index data.")

    implementation_result = generate_source_search_index_requirements(implementation)
    source_result = derive_source_search_index_requirements(source)

    assert {"indexed_entity", "freshness_lag", "access_filtering", "observability"} <= {record.category for record in implementation_result.records}
    assert source_result.summary["missing_detail_flags"] == ["missing_mapping", "missing_freshness", "missing_access_filtering"]
    assert [record.category for record in object_result.records] == ["analyzer_tokenizer"]
    assert [record.category for record in text_result.records] == ["reindex_backfill"]


def test_serialization_aliases_and_markdown_are_deterministic():
    source = _source_brief(summary="Search index field mapping must map title and monitor index health metrics.")
    model = SourceBrief.model_validate(source)

    result = build_source_search_index_requirements(source)
    extracted = extract_source_search_index_requirements(model)
    payload = source_search_index_requirements_to_dict(result)
    markdown = source_search_index_requirements_to_markdown(result)

    assert extracted == result.records
    assert summarize_source_search_index_requirements(result) == result.summary
    assert source_search_index_requirements_to_dicts(result) == payload["requirements"]
    assert source_search_index_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert markdown.startswith("# Source Search Index Requirements Report: source-search-index")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |" in markdown


def test_negated_and_unrelated_search_mentions_return_empty_reports():
    negated = build_source_search_index_requirements(
        _source_brief(summary="Search indexing is out of scope and no field mapping changes are required.")
    )
    unrelated = build_source_search_index_requirements(
        _source_brief(summary="Browser search and replace copy must be updated.")
    )
    blank = build_source_search_index_requirements("")
    invalid = build_source_search_index_requirements(42)

    assert negated.records == ()
    assert unrelated.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated.summary["requirement_count"] == 0
    assert unrelated.summary["status"] == "no_search_index_language"
    assert "No source search index requirements were inferred" in unrelated.to_markdown()


def _source_brief(*, source_id="source-search-index", summary="General search requirements.", source_payload=None):
    return {
        "id": source_id,
        "title": "Search index requirements",
        "domain": "search",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None):
    return {
        "id": "implementation-search-index",
        "source_brief_id": "source-search-index",
        "title": "Search index implementation",
        "domain": "search",
        "target_user": "operator",
        "buyer": "platform",
        "workflow_context": "Operators need search indexing readiness.",
        "problem_statement": "Search results need deterministic indexing behavior.",
        "mvp_goal": "Plan search index handling.",
        "product_surface": "search",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run search index extractor tests.",
        "definition_of_done": [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
