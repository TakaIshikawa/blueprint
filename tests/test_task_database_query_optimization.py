"""Tests for database query performance optimization readiness analyzer."""

import pytest

from blueprint.task_database_query_optimization import (
    DatabaseQueryOptimizationReadiness,
    analyze_database_query_optimization,
)


def test_empty_change_brief_returns_all_false():
    """Empty change brief should return all fields as False."""
    result = analyze_database_query_optimization({})

    assert isinstance(result, DatabaseQueryOptimizationReadiness)
    assert result.n_plus_one_addressed is False
    assert result.index_strategy_defined is False
    assert result.inefficient_joins_optimized is False
    assert result.full_table_scans_avoided is False
    assert result.pagination_optimized is False
    assert result.query_plan_analysis_included is False
    assert result.query_caching_configured is False
    assert result.connection_pooling_implemented is False
    assert result.read_replicas_considered is False
    assert result.query_complexity_managed is False
    assert result.monitoring_metrics_planned is False


def test_n_plus_one_query_detected():
    """Detect N+1 query patterns in change brief."""
    brief = {
        "title": "Fix N+1 query problem",
        "description": "Prevent N+1 queries by adding eager loading",
    }

    result = analyze_database_query_optimization(brief)

    assert result.n_plus_one_addressed is True
    assert result.index_strategy_defined is False


def test_eager_loading_detected():
    """Detect eager loading as N+1 prevention."""
    brief = {
        "description": "Implement eager loading to avoid N+1 queries",
        "acceptance_criteria": ["Use prefetching for related objects"],
    }

    result = analyze_database_query_optimization(brief)

    assert result.n_plus_one_addressed is True


def test_batch_loading_detected():
    """Detect batch loading as N+1 prevention."""
    brief = {
        "description": "Add batch loading to prevent query in loop",
    }

    result = analyze_database_query_optimization(brief)

    assert result.n_plus_one_addressed is True


def test_index_strategy_detected():
    """Detect index strategy in change brief."""
    brief = {
        "title": "Add database indexes",
        "description": "Create indexes for performance optimization",
        "acceptance_criteria": ["Add composite index on user_id and created_at"],
    }

    result = analyze_database_query_optimization(brief)

    assert result.index_strategy_defined is True
    assert result.n_plus_one_addressed is False


def test_missing_indexes_detected():
    """Detect missing indexes concern."""
    brief = {
        "description": "Identify and add missing indexes for query optimization",
    }

    result = analyze_database_query_optimization(brief)

    assert result.index_strategy_defined is True


def test_composite_index_detected():
    """Detect composite index strategy."""
    brief = {
        "description": "Add composite index on frequently queried columns",
    }

    result = analyze_database_query_optimization(brief)

    assert result.index_strategy_defined is True


def test_covering_index_detected():
    """Detect covering index strategy."""
    brief = {
        "description": "Create covering index to avoid table lookups",
    }

    result = analyze_database_query_optimization(brief)

    assert result.index_strategy_defined is True


def test_inefficient_joins_detected():
    """Detect inefficient joins optimization."""
    brief = {
        "title": "Optimize inefficient joins",
        "description": "Improve join performance by optimizing join order",
    }

    result = analyze_database_query_optimization(brief)

    assert result.inefficient_joins_optimized is True


def test_join_optimization_detected():
    """Detect join optimization strategy."""
    brief = {
        "description": "Implement join optimization to improve query performance",
        "acceptance_criteria": ["Optimize left join queries"],
    }

    result = analyze_database_query_optimization(brief)

    assert result.inefficient_joins_optimized is True


def test_avoid_cross_join_detected():
    """Detect cross join avoidance."""
    brief = {
        "description": "Avoid cross join to prevent cartesian product",
    }

    result = analyze_database_query_optimization(brief)

    assert result.inefficient_joins_optimized is True


def test_full_table_scan_detected():
    """Detect full table scan concern."""
    brief = {
        "title": "Avoid full table scans",
        "description": "Prevent table scans by adding appropriate indexes",
    }

    result = analyze_database_query_optimization(brief)

    assert result.full_table_scans_avoided is True


def test_sequential_scan_detected():
    """Detect sequential scan as full table scan."""
    brief = {
        "description": "Replace sequential scan with index scan",
    }

    result = analyze_database_query_optimization(brief)

    assert result.full_table_scans_avoided is True


def test_index_scan_vs_table_scan_detected():
    """Detect index scan vs table scan optimization."""
    brief = {
        "description": "Ensure queries use index scan instead of table scan",
    }

    result = analyze_database_query_optimization(brief)

    assert result.full_table_scans_avoided is True


def test_pagination_optimization_detected():
    """Detect pagination optimization."""
    brief = {
        "title": "Optimize pagination",
        "description": "Implement cursor-based pagination for better performance",
    }

    result = analyze_database_query_optimization(brief)

    assert result.pagination_optimized is True


def test_keyset_pagination_detected():
    """Detect keyset pagination strategy."""
    brief = {
        "description": "Use keyset pagination instead of offset pagination",
    }

    result = analyze_database_query_optimization(brief)

    assert result.pagination_optimized is True


def test_limit_offset_optimization_detected():
    """Detect limit offset optimization."""
    brief = {
        "description": "Address limit offset performance problem",
    }

    result = analyze_database_query_optimization(brief)

    assert result.pagination_optimized is True


def test_query_plan_analysis_detected():
    """Detect query plan analysis."""
    brief = {
        "title": "Analyze query execution plan",
        "description": "Use EXPLAIN ANALYZE to optimize query performance",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_plan_analysis_included is True


def test_explain_plan_detected():
    """Detect EXPLAIN plan usage."""
    brief = {
        "description": "Review execution plan for slow queries",
        "acceptance_criteria": ["Analyze query plan for optimization"],
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_plan_analysis_included is True


def test_query_optimizer_detected():
    """Detect query optimizer consideration."""
    brief = {
        "description": "Leverage query optimizer hints for better performance",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_plan_analysis_included is True


def test_query_caching_detected():
    """Detect query caching strategy."""
    brief = {
        "title": "Add query caching",
        "description": "Cache query results using Redis",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_caching_configured is True


def test_result_caching_detected():
    """Detect result caching strategy."""
    brief = {
        "description": "Implement result caching for expensive queries",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_caching_configured is True


def test_materialized_view_detected():
    """Detect materialized view as caching strategy."""
    brief = {
        "description": "Create materialized view for aggregated data",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_caching_configured is True


def test_redis_caching_detected():
    """Detect Redis caching for queries."""
    brief = {
        "description": "Use Redis caching to reduce database load",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_caching_configured is True


def test_memcached_detected():
    """Detect Memcached as caching solution."""
    brief = {
        "description": "Implement memcached for query result caching",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_caching_configured is True


def test_connection_pooling_detected():
    """Detect connection pooling configuration."""
    brief = {
        "title": "Configure connection pooling",
        "description": "Set up database connection pool with appropriate size",
    }

    result = analyze_database_query_optimization(brief)

    assert result.connection_pooling_implemented is True


def test_pgbouncer_detected():
    """Detect PgBouncer as connection pooling."""
    brief = {
        "description": "Use PgBouncer for connection pooling",
    }

    result = analyze_database_query_optimization(brief)

    assert result.connection_pooling_implemented is True


def test_pool_configuration_detected():
    """Detect pool configuration and management."""
    brief = {
        "description": "Optimize pool size and connection timeout",
        "acceptance_criteria": ["Configure max connections"],
    }

    result = analyze_database_query_optimization(brief)

    assert result.connection_pooling_implemented is True


def test_read_replicas_detected():
    """Detect read replica usage."""
    brief = {
        "title": "Add read replicas",
        "description": "Route read queries to replica database servers",
    }

    result = analyze_database_query_optimization(brief)

    assert result.read_replicas_considered is True


def test_read_write_splitting_detected():
    """Detect read-write splitting strategy."""
    brief = {
        "description": "Implement read-write splitting for read scaling",
    }

    result = analyze_database_query_optimization(brief)

    assert result.read_replicas_considered is True


def test_primary_replica_architecture_detected():
    """Detect primary-replica architecture."""
    brief = {
        "description": "Set up primary-replica configuration for high availability",
    }

    result = analyze_database_query_optimization(brief)

    assert result.read_replicas_considered is True


def test_query_complexity_detected():
    """Detect query complexity management."""
    brief = {
        "title": "Simplify complex queries",
        "description": "Optimize complex query for better performance",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_complexity_managed is True


def test_subquery_optimization_detected():
    """Detect subquery optimization."""
    brief = {
        "description": "Optimize subquery performance using CTE",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_complexity_managed is True


def test_cte_detected():
    """Detect common table expression usage."""
    brief = {
        "description": "Refactor query using common table expressions",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_complexity_managed is True


def test_recursive_query_detected():
    """Detect recursive query optimization (edge case)."""
    brief = {
        "description": "Optimize recursive query for hierarchical data",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_complexity_managed is True


def test_window_function_detected():
    """Detect window function as query complexity."""
    brief = {
        "description": "Use window function for efficient ranking",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_complexity_managed is True


def test_aggregate_optimization_detected():
    """Detect aggregate optimization (edge case)."""
    brief = {
        "description": "Optimize aggregate queries for large datasets",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_complexity_managed is True


def test_monitoring_metrics_detected():
    """Detect query monitoring and metrics."""
    brief = {
        "title": "Add query performance monitoring",
        "description": "Track query performance metrics and latency",
    }

    result = analyze_database_query_optimization(brief)

    assert result.monitoring_metrics_planned is True


def test_slow_query_log_detected():
    """Detect slow query log configuration."""
    brief = {
        "description": "Enable slow query log for performance analysis",
    }

    result = analyze_database_query_optimization(brief)

    assert result.monitoring_metrics_planned is True


def test_apm_detected():
    """Detect APM for database monitoring."""
    brief = {
        "description": "Use APM to monitor database query performance",
    }

    result = analyze_database_query_optimization(brief)

    assert result.monitoring_metrics_planned is True


def test_database_observability_detected():
    """Detect database observability strategy."""
    brief = {
        "description": "Implement database observability for query telemetry",
    }

    result = analyze_database_query_optimization(brief)

    assert result.monitoring_metrics_planned is True


def test_comprehensive_optimization_all_aspects_detected():
    """Test comprehensive query optimization with all aspects present."""
    brief = {
        "title": "Complete database query optimization implementation",
        "description": (
            "Optimize database queries by preventing N+1 queries with eager loading. "
            "Add composite indexes and covering indexes to avoid full table scans. "
            "Optimize inefficient joins and implement cursor-based pagination. "
            "Analyze query execution plans using EXPLAIN ANALYZE. "
            "Configure Redis caching for query results and materialized views. "
            "Set up connection pooling with PgBouncer and read replicas for scaling. "
            "Simplify complex queries using CTEs and window functions. "
            "Enable slow query log and APM for query performance monitoring."
        ),
        "acceptance_criteria": [
            "N+1 queries prevented",
            "Database indexes created",
            "Join optimization completed",
            "Table scans avoided",
            "Pagination optimized",
            "Query plans analyzed",
            "Query caching configured",
            "Connection pooling implemented",
            "Read replicas configured",
            "Query complexity managed",
            "Monitoring metrics enabled",
        ],
    }

    result = analyze_database_query_optimization(brief)

    assert result.n_plus_one_addressed is True
    assert result.index_strategy_defined is True
    assert result.inefficient_joins_optimized is True
    assert result.full_table_scans_avoided is True
    assert result.pagination_optimized is True
    assert result.query_plan_analysis_included is True
    assert result.query_caching_configured is True
    assert result.connection_pooling_implemented is True
    assert result.read_replicas_considered is True
    assert result.query_complexity_managed is True
    assert result.monitoring_metrics_planned is True


def test_invalid_change_brief_non_mapping():
    """Test with invalid input (non-mapping type)."""
    result = analyze_database_query_optimization("not a mapping")

    assert isinstance(result, DatabaseQueryOptimizationReadiness)
    assert result.n_plus_one_addressed is False


def test_invalid_change_brief_none():
    """Test with None input."""
    result = analyze_database_query_optimization(None)

    assert isinstance(result, DatabaseQueryOptimizationReadiness)
    assert result.n_plus_one_addressed is False


def test_invalid_change_brief_list():
    """Test with list input instead of mapping."""
    result = analyze_database_query_optimization([{"key": "value"}])

    assert isinstance(result, DatabaseQueryOptimizationReadiness)
    assert result.n_plus_one_addressed is False


def test_change_brief_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    brief = {
        "title": "Database query improvements",
        "acceptance_criteria": [
            "Prevent N+1 queries",
            "Add database indexes",
            "Optimize joins",
            "Avoid full table scans",
            "Optimize pagination",
        ],
    }

    result = analyze_database_query_optimization(brief)

    assert result.n_plus_one_addressed is True
    assert result.index_strategy_defined is True
    assert result.inefficient_joins_optimized is True
    assert result.full_table_scans_avoided is True
    assert result.pagination_optimized is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    brief = {
        "description": "PREVENT N+1 QUERIES and ADD DATABASE INDEXES",
        "acceptance_criteria": ["OPTIMIZE JOINS", "AVOID TABLE SCANS"],
    }

    result = analyze_database_query_optimization(brief)

    assert result.n_plus_one_addressed is True
    assert result.index_strategy_defined is True
    assert result.inefficient_joins_optimized is True
    assert result.full_table_scans_avoided is True


def test_to_dict_method():
    """Test DatabaseQueryOptimizationReadiness.to_dict() serialization."""
    readiness = DatabaseQueryOptimizationReadiness(
        n_plus_one_addressed=True,
        index_strategy_defined=True,
        inefficient_joins_optimized=False,
        full_table_scans_avoided=True,
        pagination_optimized=False,
        query_plan_analysis_included=True,
        query_caching_configured=False,
        connection_pooling_implemented=True,
        read_replicas_considered=False,
        query_complexity_managed=True,
        monitoring_metrics_planned=False,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["n_plus_one_addressed"] is True
    assert result["index_strategy_defined"] is True
    assert result["inefficient_joins_optimized"] is False
    assert result["full_table_scans_avoided"] is True
    assert result["pagination_optimized"] is False
    assert result["query_plan_analysis_included"] is True
    assert result["query_caching_configured"] is False
    assert result["connection_pooling_implemented"] is True
    assert result["read_replicas_considered"] is False
    assert result["query_complexity_managed"] is True
    assert result["monitoring_metrics_planned"] is False


def test_dataclass_immutability():
    """Test that DatabaseQueryOptimizationReadiness is frozen/immutable."""
    readiness = DatabaseQueryOptimizationReadiness(n_plus_one_addressed=True)

    with pytest.raises(AttributeError):
        readiness.n_plus_one_addressed = False


def test_multiple_fields_in_different_sections():
    """Test detection across multiple brief sections."""
    brief = {
        "title": "Query optimization",
        "description": "Prevent N+1 queries",
        "acceptance_criteria": ["Add database indexes"],
        "requirements": ["Optimize joins"],
        "notes": ["Configure query caching"],
        "risks": ["Query complexity not managed"],
    }

    result = analyze_database_query_optimization(brief)

    assert result.n_plus_one_addressed is True
    assert result.index_strategy_defined is True
    assert result.inefficient_joins_optimized is True
    assert result.query_caching_configured is True
    assert result.query_complexity_managed is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    brief = {
        "acceptance_criteria": "Prevent N+1 queries and add database indexes for optimization",
    }

    result = analyze_database_query_optimization(brief)

    assert result.n_plus_one_addressed is True
    assert result.index_strategy_defined is True


def test_partial_index_detected():
    """Test partial index as index strategy (edge case)."""
    brief = {
        "description": "Create partial index for filtered queries",
    }

    result = analyze_database_query_optimization(brief)

    assert result.index_strategy_defined is True


def test_in_memory_caching_detected():
    """Test in-memory caching detection."""
    brief = {
        "description": "Use in-memory caching for frequently accessed queries",
    }

    result = analyze_database_query_optimization(brief)

    assert result.query_caching_configured is True


def test_query_in_loop_as_n_plus_one():
    """Test query in loop detected as N+1 pattern."""
    brief = {
        "description": "Fix query in loop performance issue",
    }

    result = analyze_database_query_optimization(brief)

    assert result.n_plus_one_addressed is True


def test_follower_read_as_read_replica():
    """Test follower read detected as read replica strategy."""
    brief = {
        "description": "Route reads to follower instances",
    }

    result = analyze_database_query_optimization(brief)

    assert result.read_replicas_considered is True


def test_index_seek_vs_scan():
    """Test index seek as optimization indicator."""
    brief = {
        "description": "Ensure queries use index seek for performance",
    }

    result = analyze_database_query_optimization(brief)

    assert result.full_table_scans_avoided is True
