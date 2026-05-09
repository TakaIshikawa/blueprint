"""Tests for search requirements extractor."""

import pytest

from blueprint.source_search_requirements import (
    SearchRequirement,
    SearchRequirementsReport,
    SearchRequirementType,
    extract_search_requirements,
    build_search_requirements_report,
)


def test_empty_source_returns_no_requirements():
    """Empty source should return no requirements."""
    result = extract_search_requirements({})

    assert isinstance(result, tuple)
    assert len(result) == 0


def test_search_scope_detected():
    """Detect search scope requirements."""
    source = {
        "description": "Implement full-text search across all user documents",
        "requirements": ["Global search capability", "Multi-field search support"],
    }

    result = extract_search_requirements(source)

    assert len(result) > 0
    assert any(req.requirement_type == "search_scope" for req in result)


def test_query_syntax_detected():
    """Detect query syntax requirements."""
    source = {
        "description": "Support boolean search with AND, OR, NOT operators",
        "requirements": ["Advanced query syntax", "Wildcard search capability"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "query_syntax" for req in result)


def test_ranking_algorithm_detected():
    """Detect ranking algorithm requirements."""
    source = {
        "description": "Use BM25 ranking algorithm for relevance scoring",
        "requirements": ["TF-IDF based search ranking"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "ranking_algorithm" for req in result)


def test_filtering_options_detected():
    """Detect filtering options requirements."""
    source = {
        "description": "Add search filters by category, date, and author",
        "requirements": ["Post-filter results", "Refinement options"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "filtering_options" for req in result)


def test_faceted_search_detected():
    """Detect faceted search requirements."""
    source = {
        "description": "Implement faceted navigation with drill-down capability",
        "requirements": ["Show facet counts", "Guided search experience"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "faceted_search" for req in result)


def test_relevance_tuning_detected():
    """Detect relevance tuning requirements."""
    source = {
        "description": "Boost title field with 2x weight for relevance tuning",
        "requirements": ["Custom scoring factors", "Field boosting"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "relevance_tuning" for req in result)


def test_performance_optimization_detected():
    """Detect performance optimization requirements."""
    source = {
        "description": "Optimize search performance with result caching",
        "requirements": ["Fast search with sub-100ms latency", "Search pagination"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "performance_optimization" for req in result)


def test_index_freshness_detected():
    """Detect index freshness requirements."""
    source = {
        "description": "Real-time search indexing for immediate searchability",
        "requirements": ["Incremental indexing", "Near-real-time search updates"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "index_freshness" for req in result)


def test_typo_tolerance_detected():
    """Detect typo tolerance requirements."""
    source = {
        "description": "Add spell-check with did you mean suggestions",
        "requirements": ["Fuzzy matching for typos", "Spell correction"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "typo_tolerance" for req in result)


def test_multi_language_support_detected():
    """Detect multi-language support requirements."""
    source = {
        "description": "Multi-language search with language detection",
        "requirements": ["Stemming for English and Spanish", "Internationalization support"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "multi_language_support" for req in result)


def test_fuzzy_search_detected():
    """Detect fuzzy search requirements."""
    source = {
        "description": "Implement fuzzy matching for approximate search",
        "requirements": ["Similarity search", "Phonetic matching with Soundex"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "fuzzy_search" for req in result)


def test_autocomplete_detected():
    """Detect autocomplete requirements."""
    source = {
        "description": "Add autocomplete with type-ahead suggestions",
        "requirements": ["Search as you type", "Instant search results"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "autocomplete" for req in result)


def test_search_suggestions_detected():
    """Detect search suggestions requirements."""
    source = {
        "description": "Show popular searches and query suggestions",
        "requirements": ["Related search terms", "Trending searches display"],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "search_suggestions" for req in result)


def test_comprehensive_search_all_types():
    """Test comprehensive search specification with all requirement types."""
    source = {
        "title": "Advanced Search Implementation",
        "description": (
            "Build full-text search across documents with boolean query syntax. "
            "Use BM25 ranking algorithm with relevance tuning and field boosting. "
            "Add filtering options and faceted navigation for drill-down. "
            "Optimize search performance with caching and real-time indexing. "
            "Support typo tolerance with spell-check and fuzzy search. "
            "Enable multi-language search with autocomplete and search suggestions."
        ),
        "requirements": [
            "Global search scope",
            "Advanced query parser",
            "TF-IDF scoring",
            "Facet counts",
            "Sub-100ms latency",
            "Near-real-time updates",
            "Did you mean suggestions",
            "Stemming support",
            "Approximate matching",
            "Type-ahead search",
            "Popular searches",
        ],
    }

    result = extract_search_requirements(source)

    assert len(result) == 13  # All requirement types
    types_found = {req.requirement_type for req in result}
    assert "search_scope" in types_found
    assert "query_syntax" in types_found
    assert "ranking_algorithm" in types_found
    assert "filtering_options" in types_found
    assert "faceted_search" in types_found
    assert "relevance_tuning" in types_found
    assert "performance_optimization" in types_found
    assert "index_freshness" in types_found
    assert "typo_tolerance" in types_found
    assert "multi_language_support" in types_found
    assert "fuzzy_search" in types_found
    assert "autocomplete" in types_found
    assert "search_suggestions" in types_found


def test_requirement_has_evidence():
    """Requirements should include evidence snippets."""
    source = {
        "description": "Implement fuzzy search for approximate matching with edit distance",
    }

    result = extract_search_requirements(source)

    req = next((r for r in result if r.requirement_type == "fuzzy_search"), None)
    assert req is not None
    assert len(req.evidence) > 0
    assert any("fuzzy" in ev.lower() for ev in req.evidence)


def test_requirement_has_source_field_paths():
    """Requirements should track source field paths."""
    source = {
        "description": "Add autocomplete feature",
        "requirements": ["Type-ahead search"],
    }

    result = extract_search_requirements(source)

    req = next((r for r in result if r.requirement_type == "autocomplete"), None)
    assert req is not None
    assert len(req.source_field_paths) > 0
    assert any("description" in path or "requirements" in path for path in req.source_field_paths)


def test_requirement_has_matched_terms():
    """Requirements should include matched search terms."""
    source = {
        "description": "Support Boolean search with AND operator and wildcard queries",
    }

    result = extract_search_requirements(source)

    req = next((r for r in result if r.requirement_type == "query_syntax"), None)
    assert req is not None
    assert len(req.matched_terms) > 0


def test_requirement_has_follow_up_questions():
    """Requirements should include follow-up questions."""
    source = {
        "description": "Implement search scope across multiple entities",
    }

    result = extract_search_requirements(source)

    req = next((r for r in result if r.requirement_type == "search_scope"), None)
    assert req is not None
    assert len(req.follow_up_questions) > 0


def test_build_report_includes_summary():
    """Report should include summary statistics."""
    source = {
        "description": "Build search with faceted navigation and BM25 ranking",
    }

    report = build_search_requirements_report(source)

    assert isinstance(report, SearchRequirementsReport)
    assert "requirement_count" in report.summary
    assert "feature_coverage" in report.summary
    assert "ux_coverage" in report.summary
    assert "performance_coverage" in report.summary


def test_report_to_dict_serialization():
    """Report should serialize to dict."""
    source = {
        "description": "Add autocomplete with fuzzy matching",
    }

    report = build_search_requirements_report(source)
    result = report.to_dict()

    assert isinstance(result, dict)
    assert "requirements" in result
    assert "summary" in result
    assert "records" in result


def test_report_to_markdown():
    """Report should render as markdown."""
    source = {
        "id": "TEST-001",
        "description": "Implement full-text search with BM25 ranking algorithm and filtering options",
    }

    report = build_search_requirements_report(source)
    markdown = report.to_markdown()

    assert isinstance(markdown, str)
    assert "# Search Requirements Report: TEST-001" in markdown
    assert "## Summary" in markdown
    assert "## Requirements" in markdown


def test_string_input_creates_body_field():
    """String input should be treated as body field."""
    result = extract_search_requirements("Implement full-text search with BM25 ranking")

    assert len(result) > 0
    assert any(req.requirement_type == "search_scope" for req in result)
    assert any(req.requirement_type == "ranking_algorithm" for req in result)


def test_case_insensitive_matching():
    """Pattern matching should be case-insensitive."""
    source = {
        "description": "FUZZY SEARCH with AUTOCOMPLETE and FACETED NAVIGATION",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "fuzzy_search" for req in result)
    assert any(req.requirement_type == "autocomplete" for req in result)
    assert any(req.requirement_type == "faceted_search" for req in result)


def test_scoped_search():
    """Detect scoped search requirements."""
    source = {
        "description": "Implement local search within user's documents",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "search_scope" for req in result)


def test_phrase_search():
    """Detect phrase search in query syntax."""
    source = {
        "description": "Support phrase search with quoted queries",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "query_syntax" for req in result)


def test_proximity_search():
    """Detect proximity search in query syntax."""
    source = {
        "description": "Implement proximity search for nearby terms",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "query_syntax" for req in result)


def test_tf_idf_ranking():
    """Detect TF-IDF ranking algorithm."""
    source = {
        "description": "Use TF-IDF for search scoring",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "ranking_algorithm" for req in result)


def test_relevance_score():
    """Detect relevance score in ranking."""
    source = {
        "description": "Display relevance score for each result",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "ranking_algorithm" for req in result)


def test_pre_filtering():
    """Detect pre-filtering options."""
    source = {
        "description": "Apply pre-filter before search execution",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "filtering_options" for req in result)


def test_filter_refinement():
    """Detect filter refinement."""
    source = {
        "description": "Refine search results with additional filters",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "filtering_options" for req in result)


def test_aggregations():
    """Detect aggregations in faceted search."""
    source = {
        "description": "Show aggregations for faceted navigation",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "faceted_search" for req in result)


def test_field_weights():
    """Detect field weights in relevance tuning."""
    source = {
        "description": "Configure field weights for custom scoring",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "relevance_tuning" for req in result)


def test_boost_fields():
    """Detect field boosting."""
    source = {
        "description": "Boost title and description fields",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "relevance_tuning" for req in result)


def test_search_caching():
    """Detect search result caching."""
    source = {
        "description": "Cache search results for performance",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "performance_optimization" for req in result)


def test_search_latency():
    """Detect search latency requirements."""
    source = {
        "description": "Achieve search latency under 50ms",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "performance_optimization" for req in result)


def test_delta_indexing():
    """Detect delta/incremental indexing."""
    source = {
        "description": "Use delta indexing for efficiency",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "index_freshness" for req in result)


def test_index_rebuild():
    """Detect index rebuild requirements."""
    source = {
        "description": "Schedule periodic index rebuild",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "index_freshness" for req in result)


def test_spell_correction():
    """Detect spell correction."""
    source = {
        "description": "Provide spell correction for misspelled queries",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "typo_tolerance" for req in result)


def test_edit_distance():
    """Detect edit distance for typo tolerance."""
    source = {
        "description": "Use edit distance for fuzzy matching",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "typo_tolerance" for req in result)


def test_language_detection():
    """Detect language detection."""
    source = {
        "description": "Auto-detect query language for proper analysis",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "multi_language_support" for req in result)


def test_lemmatization():
    """Detect lemmatization for multi-language."""
    source = {
        "description": "Apply lemmatization for better search results",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "multi_language_support" for req in result)


def test_approximate_search():
    """Detect approximate search."""
    source = {
        "description": "Enable approximate search for similar matches",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "fuzzy_search" for req in result)


def test_metaphone():
    """Detect Metaphone phonetic algorithm."""
    source = {
        "description": "Use Metaphone for phonetic matching",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "fuzzy_search" for req in result)


def test_query_completion():
    """Detect query completion."""
    source = {
        "description": "Provide query completion suggestions",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "autocomplete" for req in result)


def test_prefix_matching():
    """Detect prefix matching for autocomplete."""
    source = {
        "description": "Use prefix matching for instant suggestions",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "autocomplete" for req in result)


def test_related_queries():
    """Detect related query suggestions."""
    source = {
        "description": "Show related queries to help users",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "search_suggestions" for req in result)


def test_suggested_terms():
    """Detect suggested search terms."""
    source = {
        "description": "Display suggested terms based on popular searches",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "search_suggestions" for req in result)


def test_elasticsearch_patterns():
    """Detect Elasticsearch-specific patterns."""
    source = {
        "description": "Use Elasticsearch with BM25 and aggregations for faceted search",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "ranking_algorithm" for req in result)
    assert any(req.requirement_type == "faceted_search" for req in result)


def test_solr_patterns():
    """Detect Solr-specific patterns."""
    source = {
        "description": "Implement Solr search with facets and spell-check",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "faceted_search" for req in result)
    assert any(req.requirement_type == "typo_tolerance" for req in result)


def test_algolia_patterns():
    """Detect Algolia-specific patterns."""
    source = {
        "description": "Use Algolia for type-ahead search with typo tolerance",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "autocomplete" for req in result)
    assert any(req.requirement_type == "typo_tolerance" for req in result)


def test_multiple_evidence_snippets():
    """Requirements should collect multiple evidence snippets."""
    source = {
        "description": "Implement fuzzy search for typos",
        "requirements": ["Approximate matching needed", "Similarity search capability"],
        "acceptance_criteria": ["Fuzzy matching with configurable threshold"],
    }

    result = extract_search_requirements(source)

    req = next((r for r in result if r.requirement_type == "fuzzy_search"), None)
    assert req is not None
    assert len(req.evidence) >= 1


def test_feature_coverage_calculation():
    """Summary should calculate feature coverage percentage."""
    source = {
        "description": "Full-text search with boolean queries and BM25 ranking",
    }

    report = build_search_requirements_report(source)

    assert "feature_coverage" in report.summary
    coverage = report.summary["feature_coverage"]
    assert 0 <= coverage <= 100


def test_ux_coverage_calculation():
    """Summary should calculate UX coverage percentage."""
    source = {
        "description": "Add autocomplete with spell-check suggestions",
    }

    report = build_search_requirements_report(source)

    assert "ux_coverage" in report.summary
    coverage = report.summary["ux_coverage"]
    assert 0 <= coverage <= 100


def test_performance_coverage_calculation():
    """Summary should calculate performance coverage percentage."""
    source = {
        "description": "Optimize search with caching and real-time indexing",
    }

    report = build_search_requirements_report(source)

    assert "performance_coverage" in report.summary
    coverage = report.summary["performance_coverage"]
    assert 0 <= coverage <= 100


def test_type_counts_in_summary():
    """Summary should include type counts."""
    source = {
        "description": "Fuzzy search with autocomplete and facets",
    }

    report = build_search_requirements_report(source)

    assert "type_counts" in report.summary
    type_counts = report.summary["type_counts"]
    assert isinstance(type_counts, dict)
    assert "fuzzy_search" in type_counts
    assert "autocomplete" in type_counts
    assert "faceted_search" in type_counts


def test_records_property():
    """Report should expose requirements via records property."""
    source = {
        "description": "Search with ranking and filtering",
    }

    report = build_search_requirements_report(source)

    assert hasattr(report, "records")
    assert report.records == report.requirements


def test_to_dicts_method():
    """Report should convert requirements to list of dicts."""
    source = {
        "description": "Autocomplete with fuzzy matching",
    }

    report = build_search_requirements_report(source)
    dicts = report.to_dicts()

    assert isinstance(dicts, list)
    assert all(isinstance(d, dict) for d in dicts)


def test_requirement_to_dict():
    """Requirement should serialize to dict."""
    source = {
        "description": "Faceted search implementation",
    }

    result = extract_search_requirements(source)
    req = result[0]
    req_dict = req.to_dict()

    assert isinstance(req_dict, dict)
    assert "requirement_type" in req_dict
    assert "evidence" in req_dict
    assert "source_field_paths" in req_dict
    assert "matched_terms" in req_dict
    assert "follow_up_questions" in req_dict


def test_compatibility_aliases():
    """Module should provide compatibility function aliases."""
    from blueprint.source_search_requirements import (
        generate_search_requirements,
        analyze_search_requirements,
        derive_search_requirements,
    )

    source = {"description": "Search functionality"}

    result1 = generate_search_requirements(source)
    result2 = analyze_search_requirements(source)
    result3 = derive_search_requirements(source)

    assert result1 == result2 == result3


def test_summarize_alias():
    """Module should provide summarize alias."""
    from blueprint.source_search_requirements import summarize_search_requirements

    source = {"description": "Full-text search"}

    summary = summarize_search_requirements(source)

    assert isinstance(summary, dict)
    assert "requirement_count" in summary


def test_edge_case_autocomplete_variations():
    """Test autocomplete edge case variations."""
    source = {
        "description": "Search-as-you-type with instant search results",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "autocomplete" for req in result)


def test_edge_case_fuzzy_search_phonetic():
    """Test fuzzy search phonetic matching edge case."""
    source = {
        "description": "Soundex phonetic search for name matching",
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "fuzzy_search" for req in result)


def test_no_requirements_markdown():
    """Markdown should handle no requirements gracefully."""
    source = {"description": "Some unrelated content"}

    report = build_search_requirements_report(source)
    markdown = report.to_markdown()

    assert "No search requirements were inferred" in markdown


def test_source_brief_id_extraction():
    """Report should extract source brief ID."""
    source = {
        "id": "SEARCH-123",
        "description": "Implement search",
    }

    report = build_search_requirements_report(source)

    assert report.source_brief_id == "SEARCH-123"


def test_nested_metadata_scanning():
    """Extractor should scan nested metadata fields."""
    source = {
        "metadata": {
            "search_features": "Autocomplete and fuzzy matching",
            "performance": "Real-time indexing required",
        },
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "autocomplete" for req in result)
    assert any(req.requirement_type == "index_freshness" for req in result)


def test_empty_fields_ignored():
    """Empty fields should be ignored."""
    source = {
        "description": "Implement full-text search with BM25 ranking algorithm",
        "requirements": [],
        "acceptance_criteria": [""],
    }

    result = extract_search_requirements(source)

    assert any(req.requirement_type == "search_scope" for req in result)
    assert any(req.requirement_type == "ranking_algorithm" for req in result)
