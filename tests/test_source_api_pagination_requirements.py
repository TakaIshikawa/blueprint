import json

from blueprint.domain.models import SourceBrief
from blueprint.source_api_pagination_requirements import (
    SourceApiPaginationRequirement,
    SourceApiPaginationRequirementsReport,
    build_source_api_pagination_requirements,
    extract_source_api_pagination_requirements,
    source_api_pagination_requirements_to_dict,
    source_api_pagination_requirements_to_dicts,
    source_api_pagination_requirements_to_markdown,
    summarize_source_api_pagination_requirements,
)


def test_extracts_multi_signal_api_pagination_requirements_with_evidence():
    result = build_source_api_pagination_requirements(
        _source_brief(
            summary=(
                "Implement cursor-based pagination for the users API endpoint. "
                "Support offset pagination for backwards compatibility."
            ),
            source_payload={
                "requirements": [
                    "Default page size of 50 items with a maximum of 200 results per page.",
                    "Ensure stable sort order for pagination consistency.",
                    "Support filtering with pagination to allow paginated filtered results.",
                ],
                "acceptance_criteria": [
                    "Return next and previous tokens in API responses.",
                    "Optimize performance for large result sets exceeding 10k records.",
                ],
            },
        )
    )

    assert isinstance(result, SourceApiPaginationRequirementsReport)
    assert all(isinstance(record, SourceApiPaginationRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "cursor_pagination",
        "offset_pagination",
        "page_size_limits",
        "sort_order_stability",
        "filtering_compatibility",
        "next_previous_tokens",
        "backwards_compatibility",
        "large_result_performance",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert any("cursor" in item.lower() for item in by_type["cursor_pagination"].evidence)
    assert any("offset" in item.lower() for item in by_type["offset_pagination"].evidence)
    assert any("50 items" in item or "200 results" in item for item in by_type["page_size_limits"].evidence)
    assert "source_payload.requirements[1]" in by_type["sort_order_stability"].source_field_paths
    assert any("next" in term.lower() or "previous" in term.lower() for term in by_type["next_previous_tokens"].matched_terms)
    assert result.summary["requirement_count"] == 8
    assert result.summary["type_counts"]["cursor_pagination"] == 1
    assert result.summary["pagination_strategy_coverage"] > 0
    assert result.summary["performance_coverage"] > 0
    assert result.summary["compatibility_coverage"] > 0


def test_brief_without_pagination_language_returns_stable_empty_report():
    result = build_source_api_pagination_requirements(
        _source_brief(
            title="Database migration",
            summary="Update user table schema to add new columns.",
            source_payload={
                "requirements": [
                    "Add email_verified column with default false.",
                    "Backfill existing user records.",
                ],
            },
        )
    )
    repeat = build_source_api_pagination_requirements(
        _source_brief(
            title="Database migration",
            summary="Update user table schema to add new columns.",
            source_payload={
                "requirements": [
                    "Add email_verified column with default false.",
                    "Backfill existing user records.",
                ],
            },
        )
    )

    expected_summary = {
        "requirement_count": 0,
        "source_count": 1,
        "type_counts": {
            "cursor_pagination": 0,
            "offset_pagination": 0,
            "page_size_limits": 0,
            "sort_order_stability": 0,
            "filtering_compatibility": 0,
            "next_previous_tokens": 0,
            "backwards_compatibility": 0,
            "large_result_performance": 0,
        },
        "requirement_types": [],
        "follow_up_question_count": 0,
        "pagination_strategy_coverage": 0,
        "performance_coverage": 0,
        "compatibility_coverage": 0,
    }
    assert result.summary == expected_summary
    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.to_dict() == repeat.to_dict()


def test_performance_only_listing_does_not_imply_pagination_requirement():
    result = build_source_api_pagination_requirements(
        _source_brief(
            summary="Improve query performance when listing all users.",
            source_payload={
                "requirements": [
                    "Add database index on created_at for faster queries.",
                    "Cache frequently accessed user lists.",
                ],
            },
        )
    )

    # Should not detect pagination requirements from general "list" mention
    assert result.summary["requirement_count"] == 0
    assert result.requirements == ()


def test_ambiguous_list_wording_does_not_trigger_false_positives():
    result = build_source_api_pagination_requirements(
        _source_brief(
            summary="Create a todo list feature for users.",
            source_payload={
                "requirements": [
                    "Users can create multiple task lists.",
                    "Support list reordering and deletion.",
                ],
            },
        )
    )

    # Should not detect pagination from "list" in different context
    assert result.summary["requirement_count"] == 0
    assert result.requirements == ()


def test_requirement_deduplication_merges_evidence_without_losing_source_fields():
    result = build_source_api_pagination_requirements(
        _source_brief(
            summary="Use cursor pagination for the products endpoint.",
            source_payload={
                "requirements": [
                    "Implement cursor-based pagination for scalability.",
                    "Cursor tokens should be opaque and base64 encoded.",
                ],
                "acceptance": "Pagination must use cursors for all list operations.",
            },
        )
    )

    by_type = {record.requirement_type: record for record in result.records}
    cursor_req = by_type["cursor_pagination"]
    # Multiple source fields should be captured
    assert len(cursor_req.source_field_paths) >= 2
    assert "summary" in cursor_req.source_field_paths
    assert any("requirements" in field for field in cursor_req.source_field_paths)
    assert len(cursor_req.evidence) >= 2
    assert any("cursor" in evidence.lower() for evidence in cursor_req.evidence)


def test_dict_serialization_round_trips_without_mutation():
    original = build_source_api_pagination_requirements(
        _source_brief(
            summary="Cursor pagination with next tokens and page size limits.",
            source_payload={
                "requirements": [
                    "Maximum page size of 100 items.",
                ],
            },
        )
    )

    serialized = original.to_dict()
    assert isinstance(serialized, dict)
    assert serialized["source_brief_id"] == "pagination-source"
    assert len(serialized["requirements"]) == len(original.requirements)
    assert serialized["summary"]["requirement_count"] == len(original.requirements)

    # Repeat to verify no mutation
    repeat = original.to_dict()
    assert repeat == serialized


def test_to_dicts_helper_serializes_requirements_list():
    report = build_source_api_pagination_requirements(
        _source_brief(
            summary="Cursor pagination with filtering compatibility.",
            source_payload={
                "requirements": [
                    "Support pagination with filters for search results.",
                ],
            },
        )
    )

    dicts = source_api_pagination_requirements_to_dicts(report)
    assert isinstance(dicts, list)
    assert all(isinstance(item, dict) for item in dicts)
    assert len(dicts) == report.summary["requirement_count"]

    # Also test tuple input
    tuple_dicts = source_api_pagination_requirements_to_dicts(report.requirements)
    assert tuple_dicts == dicts


def test_markdown_output_renders_deterministic_table():
    report = build_source_api_pagination_requirements(
        _source_brief(
            source_id="pagination-markdown-test",
            summary="Offset pagination with page size limits.",
            source_payload={
                "requirements": [
                    "Support offset and limit parameters for paging.",
                ],
            },
        )
    )

    markdown = source_api_pagination_requirements_to_markdown(report)
    assert isinstance(markdown, str)
    assert "# Source API Pagination Requirements Report: pagination-markdown-test" in markdown
    assert "## Summary" in markdown
    assert "## Requirements" in markdown
    assert "| Type | Source Field Paths | Evidence | Follow-up Questions |" in markdown
    assert "offset_pagination" in markdown

    # Repeat to verify deterministic output
    repeat_markdown = report.to_markdown()
    assert repeat_markdown == markdown


def test_empty_report_markdown_includes_no_requirements_message():
    report = build_source_api_pagination_requirements(
        _source_brief(
            summary="Internal refactoring with no API changes.",
        )
    )

    markdown = report.to_markdown()
    assert "No source API pagination requirements were inferred." in markdown
    assert "## Requirements" not in markdown


def test_extracts_from_raw_text_input():
    result = build_source_api_pagination_requirements(
        "Implement cursor-based pagination with next page tokens and a maximum of 100 results per page."
    )

    assert len(result.requirements) >= 2
    types = {req.requirement_type for req in result.requirements}
    assert "cursor_pagination" in types
    assert "page_size_limits" in types or "next_previous_tokens" in types


def test_extracts_from_mapping_input():
    result = build_source_api_pagination_requirements(
        {
            "id": "mapping-source",
            "title": "API pagination requirements",
            "summary": "Offset pagination with stable sort order.",
            "source_payload": {
                "requirements": "Support filtering with pagination.",
            },
        }
    )

    assert result.source_brief_id == "mapping-source"
    types = {req.requirement_type for req in result.requirements}
    assert "offset_pagination" in types
    assert "sort_order_stability" in types or "filtering_compatibility" in types


def test_extracts_from_pydantic_model():
    model = SourceBrief(
        id="pydantic-source",
        title="Pagination requirements",
        domain="api",
        summary="Cursor pagination with large result performance optimization.",
        source_project="test",
        source_entity_type="issue",
        source_id="pydantic-source",
        source_payload={
            "requirements": "Handle large datasets efficiently with pagination.",
        },
        source_links={},
    )

    result = build_source_api_pagination_requirements(model)
    assert result.source_brief_id == "pydantic-source"
    types = {req.requirement_type for req in result.requirements}
    assert "cursor_pagination" in types
    assert "large_result_performance" in types


def test_extract_helper_returns_tuple_of_requirements():
    requirements = extract_source_api_pagination_requirements(
        _source_brief(
            summary="Cursor pagination with next tokens.",
        )
    )

    assert isinstance(requirements, tuple)
    assert all(isinstance(req, SourceApiPaginationRequirement) for req in requirements)
    assert len(requirements) >= 1


def test_summarize_helper_returns_summary_dict():
    summary = summarize_source_api_pagination_requirements(
        _source_brief(
            summary="Offset pagination with filtering compatibility.",
        )
    )

    assert isinstance(summary, dict)
    assert "requirement_count" in summary
    assert "type_counts" in summary
    assert summary["requirement_count"] >= 1


def test_summarize_accepts_report_object():
    report = build_source_api_pagination_requirements(
        _source_brief(summary="Cursor pagination.")
    )
    summary = summarize_source_api_pagination_requirements(report)

    assert summary == report.summary


def test_coverage_metrics_calculated_correctly():
    result = build_source_api_pagination_requirements(
        _source_brief(
            summary="Cursor and offset pagination with backwards compatibility.",
            source_payload={
                "requirements": [
                    "Support filtering with pagination.",
                    "Optimize for large result sets.",
                    "Maximum page size of 200 items.",
                ],
            },
        )
    )

    summary = result.summary
    # Both pagination strategies present (cursor, offset)
    assert summary["pagination_strategy_coverage"] == 100
    # Performance requirements present (large results, page size)
    assert summary["performance_coverage"] == 100
    # Compatibility present (backwards compat, filtering compat)
    assert summary["compatibility_coverage"] == 100


def test_follow_up_questions_reduced_when_evidence_is_specific():
    result = build_source_api_pagination_requirements(
        _source_brief(
            summary="Use base64 encoded cursor tokens for pagination.",
            source_payload={
                "requirements": [
                    "Default page size of 50, maximum of 100 items per page.",
                    "Backwards compatibility period of 90 days for offset pagination.",
                ],
            },
        )
    )

    by_type = {req.requirement_type: req for req in result.requirements}
    # Cursor mentions encoding format, so should have fewer questions
    cursor = by_type.get("cursor_pagination")
    if cursor:
        assert len(cursor.follow_up_questions) < 2
    # Page size mentions specific limits, so should have no questions
    page_size = by_type.get("page_size_limits")
    if page_size:
        assert len(page_size.follow_up_questions) == 0
    # Backwards compatibility mentions timeline, so should have fewer questions
    compat = by_type.get("backwards_compatibility")
    if compat:
        assert len(compat.follow_up_questions) <= 1


def test_matched_terms_captured_for_each_requirement():
    result = build_source_api_pagination_requirements(
        _source_brief(
            summary="Cursor pagination with next page tokens and sort order stability.",
        )
    )

    for req in result.requirements:
        assert len(req.matched_terms) > 0
        # Matched terms should be deduplicated
        assert len(req.matched_terms) == len(set(term.casefold() for term in req.matched_terms))


def test_explicit_pagination_specs_detected():
    result = build_source_api_pagination_requirements(
        _source_brief(
            summary="Implement cursor-based pagination for users endpoint.",
            source_payload={
                "requirements": [
                    "Use cursor pagination with opaque tokens.",
                    "Support offset pagination as fallback.",
                    "Page size limited to 100 items maximum.",
                    "Ensure deterministic sort order for stable pagination.",
                    "Next and previous page tokens in response.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "cursor_pagination" in types
    assert "offset_pagination" in types
    assert "page_size_limits" in types
    assert "sort_order_stability" in types
    assert "next_previous_tokens" in types


def test_partial_pagination_concerns_identified():
    result = build_source_api_pagination_requirements(
        _source_brief(
            summary="Add pagination to search endpoint.",
            source_payload={
                "requirements": [
                    "Return maximum 50 results per page.",
                ],
            },
        )
    )

    # Should detect page size limits even without full pagination spec
    types = {req.requirement_type for req in result.requirements}
    assert "page_size_limits" in types
    assert result.summary["requirement_count"] >= 1


def _source_brief(
    *,
    source_id="pagination-source",
    title="API pagination requirements",
    domain="platform",
    summary="General API pagination requirements.",
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
