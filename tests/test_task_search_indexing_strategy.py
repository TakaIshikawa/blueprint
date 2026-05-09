from blueprint.task_search_indexing_strategy import (
    SearchIndexingStrategyAnalysis,
    analyze_search_indexing_strategy,
)


def test_detects_index_schema_definition():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Define index schema with text and keyword fields",
            acceptance_criteria=["Create Elasticsearch mapping for document structure"],
        )
    )

    assert analysis.index_schema_defined is True


def test_detects_field_type_mapping():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Configure field mappings with numeric field for price and date field for timestamp",
        )
    )

    assert analysis.index_schema_defined is True


def test_detects_tokenization_strategy():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Use standard analyzer with stemming and stop words",
            requirements=["Configure tokenization for full-text search"],
        )
    )

    assert analysis.tokenization_strategy_defined is True


def test_detects_custom_analyzer():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Create custom analyzer with edge ngram tokenizer",
            approach="Define language-specific analyzer for improved search",
        )
    )

    assert analysis.tokenization_strategy_defined is True


def test_detects_relevance_tuning():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Tune relevance scoring with field boosting",
            acceptance_criteria=["Boost title field by factor of 2.0"],
        )
    )

    assert analysis.relevance_tuning_planned is True


def test_detects_fuzzy_matching():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Enable fuzzy search with edit distance 2",
            approach="Use fuzzy matching for typo tolerance",
        )
    )

    assert analysis.relevance_tuning_planned is True


def test_detects_bm25_scoring():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Configure BM25 ranking algorithm for search",
        )
    )

    assert analysis.relevance_tuning_planned is True


def test_detects_real_time_indexing():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Implement real-time indexing for instant search updates",
            requirements=["Index documents immediately upon creation"],
        )
    )

    assert analysis.update_frequency_specified is True


def test_detects_batch_indexing():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Schedule batch indexing every hour",
            approach="Use periodic reindex for cost optimization",
        )
    )

    assert analysis.update_frequency_specified is True


def test_detects_near_real_time_indexing():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Near real-time updates with 1-second refresh interval",
        )
    )

    assert analysis.update_frequency_specified is True


def test_detects_index_rebuild_strategy():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Plan zero-downtime reindex for schema changes",
            approach="Use online reindex with alias switching",
        )
    )

    assert analysis.index_rebuild_considered is True


def test_detects_parallel_reindex():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Rebuild index using parallel reindexing",
        )
    )

    assert analysis.index_rebuild_considered is True


def test_detects_consistency_guarantees():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Provide eventual consistency for search results",
            requirements=["Document acceptable search staleness of 5 seconds"],
        )
    )

    assert analysis.consistency_guarantees_defined is True


def test_detects_strong_consistency():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Ensure strong consistency with read-after-write guarantee",
        )
    )

    assert analysis.consistency_guarantees_defined is True


def test_detects_query_performance_optimization():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Optimize query performance with result caching",
            approach="Use filter context for non-scoring queries",
        )
    )

    assert analysis.query_performance_optimized is True


def test_detects_search_caching():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Cache search results for improved latency",
        )
    )

    assert analysis.query_performance_optimized is True


def test_detects_storage_cost_considerations():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Configure sharding strategy with 3 primary shards",
            requirements=["Set replication factor to 2 for high availability"],
        )
    )

    assert analysis.storage_costs_considered is True


def test_detects_compression():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Enable compression to reduce index size",
        )
    )

    assert analysis.storage_costs_considered is True


def test_detects_multi_language_support():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Support multi-language search with English and Spanish analyzers",
            approach="Use language-specific stemming and stop words",
        )
    )

    assert analysis.multi_language_support is True


def test_detects_internationalization():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Implement i18n for search with locale-specific analysis",
        )
    )

    assert analysis.multi_language_support is True


def test_detects_faceted_search():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Implement faceted search with category filters",
            acceptance_criteria=["Support filter aggregations for refinement"],
        )
    )

    assert analysis.faceted_search_planned is True


def test_detects_drill_down_search():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Enable drill-down with attribute filtering",
        )
    )

    assert analysis.faceted_search_planned is True


def test_detects_monitoring_and_observability():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Add monitoring for search latency and throughput",
            requirements=["Set up alerts for query performance degradation"],
        )
    )

    assert analysis.monitoring_observability_planned is True


def test_detects_search_analytics():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Track search analytics and query statistics",
        )
    )

    assert analysis.monitoring_observability_planned is True


def test_comprehensive_search_indexing_plan():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            title="Implement production-ready search with Elasticsearch",
            description=(
                "Define index schema with text fields using standard analyzer and keyword fields for filtering. "
                "Configure edge ngram tokenizer for autocomplete. Set up real-time indexing with 1-second refresh. "
                "Implement faceted search with category aggregations."
            ),
            approach=(
                "Use BM25 for relevance scoring with boosted title field. "
                "Configure 5 primary shards with replication factor 2. "
                "Enable query caching for filter context. "
                "Plan zero-downtime reindex strategy for schema changes."
            ),
            acceptance_criteria=[
                "Index schema defined with proper field mappings",
                "Search latency under 100ms for 95th percentile",
                "Support multi-language search with English and Spanish",
                "Strong consistency for critical data with eventual consistency for general search",
            ],
            requirements=[
                "Monitor query performance and index health",
                "Document storage costs and sharding strategy",
            ],
        )
    )

    assert analysis.index_schema_defined is True
    assert analysis.tokenization_strategy_defined is True
    assert analysis.relevance_tuning_planned is True
    assert analysis.update_frequency_specified is True
    assert analysis.index_rebuild_considered is True
    assert analysis.consistency_guarantees_defined is True
    assert analysis.query_performance_optimized is True
    assert analysis.storage_costs_considered is True
    assert analysis.multi_language_support is True
    assert analysis.faceted_search_planned is True
    assert analysis.monitoring_observability_planned is True
    assert analysis.readiness_score > 0.9


def test_readiness_score_calculation():
    # High readiness: all aspects covered
    high_readiness = analyze_search_indexing_strategy(
        _change_brief(
            description="Define index schema with text and keyword fields using standard analyzer",
            approach="Real-time indexing with query caching, BM25 scoring, and field boosting",
            requirements=["Monitor search metrics", "Configure sharding with 3 shards and 2 replicas"],
            acceptance_criteria=["Eventual consistency with 1-second staleness", "Zero-downtime reindex"],
        )
    )
    assert high_readiness.readiness_score > 0.7

    # Low readiness: minimal planning
    low_readiness = analyze_search_indexing_strategy(
        _change_brief(description="Add search functionality")
    )
    assert low_readiness.readiness_score < 0.3


def test_recommendations_for_incomplete_plan():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Add search to application",
        )
    )

    recs = analysis.recommendations
    assert len(recs) > 0
    assert any("index schema" in rec.lower() for rec in recs)
    assert any("tokenization" in rec.lower() for rec in recs)
    assert any("relevance" in rec.lower() for rec in recs)
    assert any("update frequency" in rec.lower() for rec in recs)


def test_recommendations_for_schema_without_rebuild():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Define index schema with text fields",
        )
    )

    recs = analysis.recommendations
    assert any("rebuild" in rec.lower() for rec in recs)


def test_recommendations_for_storage_costs():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Configure index with text fields and tokenizer",
        )
    )

    recs = analysis.recommendations
    assert any("storage" in rec.lower() for rec in recs)


def test_recommendations_for_multi_language():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Support multi-language search",
        )
    )

    recs = analysis.recommendations
    assert any("analyzer" in rec.lower() for rec in recs)


def test_recommendations_for_faceted_search():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Implement faceted search with filters",
        )
    )

    recs = analysis.recommendations
    assert any("aggregation" in rec.lower() or "performance" in rec.lower() for rec in recs)


def test_elasticsearch_mapping():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Create Elasticsearch mapping for products index",
        )
    )

    assert analysis.index_schema_defined is True


def test_opensearch_index():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Define OpenSearch index schema",
        )
    )

    assert analysis.index_schema_defined is True


def test_algolia_configuration():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Configure Algolia index with custom ranking",
        )
    )

    assert analysis.index_schema_defined is True


def test_solr_schema():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Set up Solr schema for document search",
        )
    )

    assert analysis.index_schema_defined is True


def test_stemming_configuration():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Configure stemming for English language search",
        )
    )

    assert analysis.tokenization_strategy_defined is True


def test_synonym_support():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Add synonym filter for improved search recall",
        )
    )

    assert analysis.tokenization_strategy_defined is True


def test_vector_search():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Implement vector search for semantic similarity",
        )
    )

    assert analysis.relevance_tuning_planned is True


def test_tf_idf_scoring():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Use TF-IDF for document ranking",
        )
    )

    assert analysis.relevance_tuning_planned is True


def test_incremental_indexing():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Implement incremental indexing for changed documents",
        )
    )

    assert analysis.update_frequency_specified is True


def test_continuous_indexing():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Set up continuous indexing pipeline",
        )
    )

    assert analysis.update_frequency_specified is True


def test_full_reindex():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Schedule full reindex weekly",
        )
    )

    assert analysis.index_rebuild_considered is True


def test_offline_reindex():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Perform offline reindex during maintenance window",
        )
    )

    assert analysis.index_rebuild_considered is True


def test_search_freshness():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Ensure search results are fresh within 2 seconds",
        )
    )

    assert analysis.consistency_guarantees_defined is True


def test_index_latency():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Monitor index latency and document lag",
        )
    )

    assert analysis.consistency_guarantees_defined is True


def test_filter_context():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Use filter context for category and status queries",
        )
    )

    assert analysis.query_performance_optimized is True


def test_cached_queries():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Enable cached search for frequently used queries",
        )
    )

    assert analysis.query_performance_optimized is True


def test_data_retention():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Configure data retention policy for old documents",
        )
    )

    assert analysis.storage_costs_considered is True


def test_index_lifecycle():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Set up index lifecycle management for cost optimization",
        )
    )

    assert analysis.storage_costs_considered is True


def test_language_detection():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Implement language detection for automatic analyzer selection",
        )
    )

    assert analysis.multi_language_support is True


def test_cross_language_search():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Enable cross-language search capabilities",
        )
    )

    assert analysis.multi_language_support is True


def test_aggregation_queries():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Build aggregation queries for search facets",
        )
    )

    assert analysis.faceted_search_planned is True


def test_bucketing():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Configure bucketing for category aggregations",
        )
    )

    assert analysis.faceted_search_planned is True


def test_search_metrics():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Track search query metrics and performance",
        )
    )

    assert analysis.monitoring_observability_planned is True


def test_index_health_monitoring():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Monitor index health and shard status",
        )
    )

    assert analysis.monitoring_observability_planned is True


def test_empty_brief_returns_defaults():
    analysis = analyze_search_indexing_strategy({})

    assert analysis.index_schema_defined is False
    assert analysis.tokenization_strategy_defined is False
    assert analysis.relevance_tuning_planned is False
    assert analysis.readiness_score == 0.0


def test_non_dict_input_returns_defaults():
    analysis = analyze_search_indexing_strategy("not a dict")

    assert analysis.index_schema_defined is False
    assert analysis.readiness_score == 0.0


def test_to_dict_serialization():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Define index schema with standard analyzer",
            approach="Use real-time indexing with query caching",
        )
    )

    result = analysis.to_dict()
    assert isinstance(result, dict)
    assert "index_schema_defined" in result
    assert "tokenization_strategy_defined" in result
    assert "query_performance_optimized" in result
    assert isinstance(result["index_schema_defined"], bool)


def test_edge_case_real_time_indexing():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Implement live indexing for instant search updates",
            requirements=["Real-time consistency for critical documents"],
        )
    )

    assert analysis.update_frequency_specified is True
    # Should recommend consistency guarantees if not defined
    recs = analysis.recommendations
    if not analysis.consistency_guarantees_defined:
        assert any("consistency" in rec.lower() for rec in recs)


def test_edge_case_multi_language_without_analyzers():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Support search in English, Spanish, and French",
        )
    )

    assert analysis.multi_language_support is True
    # Should recommend language-specific analyzers
    recs = analysis.recommendations
    assert any("analyzer" in rec.lower() for rec in recs)


def test_edge_case_faceted_search_without_performance():
    analysis = analyze_search_indexing_strategy(
        _change_brief(
            description="Add facets for product categories and price ranges",
        )
    )

    assert analysis.faceted_search_planned is True
    # Should recommend aggregation optimization
    recs = analysis.recommendations
    assert any("aggregation" in rec.lower() or "performance" in rec.lower() for rec in recs)


def _change_brief(
    *,
    title="Search indexing task",
    description="",
    summary="",
    body="",
    requirements=None,
    acceptance_criteria=None,
    approach="",
    implementation="",
    rollback_plan="",
    testing_strategy="",
    risks=None,
    constraints=None,
):
    brief = {
        "title": title,
        "description": description,
        "summary": summary,
        "body": body,
        "approach": approach,
        "implementation": implementation,
        "rollback_plan": rollback_plan,
        "testing_strategy": testing_strategy,
    }
    if requirements:
        brief["requirements"] = requirements
    if acceptance_criteria:
        brief["acceptance_criteria"] = acceptance_criteria
    if risks:
        brief["risks"] = risks
    if constraints:
        brief["constraints"] = constraints
    return brief
