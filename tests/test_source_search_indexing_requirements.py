import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_search_indexing_requirements import (
    SourceSearchIndexingRequirement,
    SourceSearchIndexingRequirementsReport,
    build_source_search_indexing_requirements,
    derive_source_search_indexing_requirements,
    extract_source_search_indexing_requirements,
    generate_source_search_indexing_requirements,
    source_search_indexing_requirements_to_dict,
    source_search_indexing_requirements_to_dicts,
    source_search_indexing_requirements_to_markdown,
    summarize_source_search_indexing_requirements,
)


def test_extracts_search_indexing_and_ranking_requirements_with_evidence():
    result = build_source_search_indexing_requirements(
        _source_brief(
            summary=(
                "Product search must index title, description, SKU, and tags. "
                "Ranking should boost exact title matches and recent inventory updates. "
                "Index freshness must be within 5 minutes with retry alerts on failures."
            ),
            source_payload={
                "requirements": [
                    "Product filters must support status and price filtering.",
                    "Product facets need facet counts for brand and category.",
                ]
            },
        )
    )

    assert isinstance(result, SourceSearchIndexingRequirementsReport)
    assert all(isinstance(record, SourceSearchIndexingRequirement) for record in result.records)
    assert [record.requirement_mode for record in result.records] == [
        "search",
        "indexing",
        "ranking",
        "filters",
        "facets",
        "index_freshness",
    ]
    by_mode = {record.requirement_mode: record for record in result.records}
    assert by_mode["search"].search_surface == "product search"
    assert by_mode["indexing"].confidence == "high"
    assert "indexed fields" not in " ".join(by_mode["indexing"].missing_detail_flags)
    assert "missing_failure_behavior" not in by_mode["index_freshness"].missing_detail_flags
    assert "ranking" in by_mode["ranking"].matched_terms
    assert any("summary: Product search must index" in item for item in by_mode["search"].evidence)
    assert any("source_payload.requirements[0]" in item for item in by_mode["filters"].evidence)
    assert result.summary["requirement_count"] == 6
    assert result.summary["requirement_mode_counts"]["index_freshness"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_extracts_metadata_modes_multiple_surfaces_and_missing_details():
    result = build_source_search_indexing_requirements(
        _source_brief(
            source_payload={
                "search_requirements": {
                    "admin_search": [
                        "Admin search requires autocomplete suggestions and typo tolerance.",
                        "Synonyms should map cancelled to canceled for support tickets.",
                    ],
                    "catalog_indexing": {
                        "freshness": "Catalog indexing is eventually consistent after writes.",
                        "reindex": "Catalog reindex must backfill existing records and retry failures.",
                    },
                }
            },
        )
    )

    by_mode = {record.requirement_mode: record for record in result.records}

    assert [record.requirement_mode for record in result.records] == [
        "search",
        "indexing",
        "reindex",
        "autocomplete",
        "synonyms",
        "typo_tolerance",
        "eventual_consistency",
    ]
    assert result.summary["search_surfaces"] == ["admin search", "catalog search"]
    assert by_mode["autocomplete"].search_surface == "admin search"
    assert by_mode["reindex"].search_surface == "catalog search"
    assert "missing_indexed_fields" in by_mode["autocomplete"].missing_detail_flags
    assert "missing_backfill_strategy" not in by_mode["reindex"].missing_detail_flags
    assert "missing_failure_behavior" not in by_mode["reindex"].missing_detail_flags
    assert "missing_freshness_target" in by_mode["eventual_consistency"].missing_detail_flags
    assert any(
        "source_payload.search_requirements.catalog_indexing.reindex" in item
        for item in by_mode["reindex"].evidence
    )


def test_extracts_from_implementation_brief_and_plain_text():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Knowledge base search must support filters by locale and permission group.",
                "Definition of done: index freshness within 1 minute after article publish.",
            ],
        )
    )
    model_result = extract_source_search_indexing_requirements(brief)
    text_result = build_source_search_indexing_requirements(
        "Marketplace search should support synonyms and fuzzy match for buyer queries."
    )

    assert model_result.source_id == "impl-search"
    assert [record.requirement_mode for record in model_result.records] == [
        "search",
        "filters",
        "index_freshness",
    ]
    assert model_result.records[0].search_surface == "knowledge base search"
    assert text_result.source_id is None
    assert [record.requirement_mode for record in text_result.records] == [
        "search",
        "synonyms",
        "typo_tolerance",
    ]
    assert text_result.records[0].search_surface == "marketplace search"


def test_aliases_serialization_markdown_and_no_source_mutation_are_stable():
    source = _source_brief(
        source_id="search-model",
        summary="Global search must index name and email with ranking by exact matches.",
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_search_indexing_requirements(source)
    generated = generate_source_search_indexing_requirements(model)
    derived = derive_source_search_indexing_requirements(model)
    extracted = extract_source_search_indexing_requirements(model)
    payload = source_search_indexing_requirements_to_dict(generated)
    markdown = source_search_indexing_requirements_to_markdown(generated)

    assert source == original
    assert mapping_result.to_dict() == generated.to_dict()
    assert derived.to_dict() == generated.to_dict()
    assert extracted.to_dict() == generated.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert generated.records == generated.requirements
    assert generated.findings == generated.requirements
    assert source_search_indexing_requirements_to_dicts(generated) == payload["requirements"]
    assert source_search_indexing_requirements_to_dicts(generated.records) == payload["records"]
    assert summarize_source_search_indexing_requirements(generated) == generated.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "search_surface",
        "requirement_mode",
        "missing_detail_flags",
        "evidence",
        "source_field_paths",
        "matched_terms",
        "confidence",
        "planning_note",
    ]
    assert markdown == generated.to_markdown()
    assert markdown.startswith("# Source Search Indexing Requirements Report: search-model")
    assert "| Source Brief | Surface | Mode | Confidence | Missing Details |" in markdown


def test_empty_invalid_and_negated_inputs_return_stable_empty_reports():
    result = build_source_search_indexing_requirements(
        _source_brief(
            title="Profile settings",
            summary="No search indexing changes are required for this copy update.",
            source_payload={"requirements": ["Keep form submission behavior unchanged."]},
        )
    )
    malformed = build_source_search_indexing_requirements({"source_payload": {"notes": object()}})
    blank_text = build_source_search_indexing_requirements("")
    unrelated = build_source_search_indexing_requirements(
        _source_brief(
            title="Release notes",
            summary="Publish support documentation after deploy.",
        )
    )

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "requirement_modes": [],
        "requirement_mode_counts": {
            "search": 0,
            "indexing": 0,
            "reindex": 0,
            "ranking": 0,
            "filters": 0,
            "facets": 0,
            "autocomplete": 0,
            "synonyms": 0,
            "typo_tolerance": 0,
            "eventual_consistency": 0,
            "index_freshness": 0,
        },
        "missing_detail_counts": {
            "missing_indexed_fields": 0,
            "missing_freshness_target": 0,
            "missing_ranking_filter_behavior": 0,
            "missing_backfill_strategy": 0,
            "missing_failure_behavior": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "search_surfaces": [],
        "status": "no_search_indexing_language",
    }
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == expected_summary
    assert malformed.summary == expected_summary
    assert blank_text.summary == expected_summary
    assert unrelated.summary == expected_summary
    assert "No source search indexing requirements were inferred" in result.to_markdown()


def _source_brief(
    *,
    source_id="search-source",
    title="Search indexing requirements",
    domain="discovery",
    summary="General search requirements.",
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
    source_id="impl-search",
    title="Knowledge base discovery",
    summary="Search implementation requirements.",
    scope=None,
):
    return {
        "id": source_id,
        "source_brief_id": source_id,
        "title": title,
        "domain": "support",
        "target_user": None,
        "buyer": None,
        "workflow_context": None,
        "problem_statement": summary,
        "mvp_goal": "Improve support article discovery.",
        "product_surface": None,
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Validate search behavior.",
        "definition_of_done": [],
    }
