"""Tests for caching strategy analyzer."""

import pytest

from blueprint.task_caching_strategy import (
    CachingStrategy,
    analyze_caching_strategy,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_caching_strategy({})

    assert isinstance(result, CachingStrategy)
    assert result.cache_layers_defined is False
    assert result.cache_keys_designed is False
    assert result.ttl_policy_configured is False
    assert result.invalidation_strategy_planned is False
    assert result.cache_stampede_prevented is False
    assert result.stale_data_handled is False
    assert result.cache_coherence_maintained is False
    assert result.cold_start_optimized is False
    assert result.memory_limits_managed is False
    assert result.cache_monitoring_enabled is False
    assert result.readiness_score == 0.0


def test_cache_layers_detected():
    """Detect cache layers configuration in task data."""
    task = {
        "title": "Configure caching layers",
        "description": "Set up multi-tier caching with Redis cache and CDN cache layer",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True
    assert result.cache_keys_designed is False
    assert result.readiness_score == 0.1


def test_cache_keys_detected():
    """Detect cache key design in task data."""
    task = {
        "description": "Design cache key strategy with proper key generation pattern",
        "acceptance_criteria": ["Define cache key format", "Implement cache key naming"],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_keys_designed is True
    assert result.cache_layers_defined is False


def test_ttl_policy_detected():
    """Detect TTL policy configuration in task data."""
    task = {
        "description": "Configure TTL for cache entries with expiration policy of 5 minutes",
        "acceptance_criteria": ["Set cache expiration", "Define time to live"],
    }

    result = analyze_caching_strategy(task)

    assert result.ttl_policy_configured is True
    assert result.cache_layers_defined is False


def test_invalidation_strategy_detected():
    """Detect cache invalidation strategy in task data."""
    task = {
        "description": "Implement cache invalidation strategy with purge cache and evict logic",
        "acceptance_criteria": ["Cache busting implemented", "Flush cache on updates"],
    }

    result = analyze_caching_strategy(task)

    assert result.invalidation_strategy_planned is True


def test_cache_stampede_detected():
    """Detect cache stampede prevention in task data."""
    task = {
        "description": "Prevent cache stampede with cache locking and single flight pattern",
        "acceptance_criteria": ["Thundering herd protection", "Cache deduplication"],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_stampede_prevented is True


def test_stale_data_detected():
    """Detect stale data handling in task data."""
    task = {
        "description": "Handle stale cache with stale-while-revalidate and cache freshness checks",
        "acceptance_criteria": ["Manage cache staleness", "Ensure cache consistency"],
    }

    result = analyze_caching_strategy(task)

    assert result.stale_data_handled is True


def test_cache_coherence_detected():
    """Detect cache coherence in task data."""
    task = {
        "description": "Maintain cache coherence across distributed cache nodes with cache synchronization",
        "acceptance_criteria": ["Multi-region cache setup", "Cache propagation enabled"],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_coherence_maintained is True


def test_cold_start_detected():
    """Detect cold start optimization in task data."""
    task = {
        "description": "Optimize cold start performance with cache warming and cache preloading",
        "acceptance_criteria": ["Prime cache on startup", "Prevent cold cache"],
    }

    result = analyze_caching_strategy(task)

    assert result.cold_start_optimized is True


def test_memory_limits_detected():
    """Detect memory limits management in task data."""
    task = {
        "description": "Manage cache memory limits with LRU eviction policy and cache size constraints",
        "acceptance_criteria": ["Set max cache size", "Configure cache capacity"],
    }

    result = analyze_caching_strategy(task)

    assert result.memory_limits_managed is True


def test_cache_monitoring_detected():
    """Detect cache monitoring in task data."""
    task = {
        "description": "Enable cache monitoring with cache hit rate tracking and cache metrics",
        "acceptance_criteria": ["Monitor cache performance", "Track cache misses"],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_monitoring_enabled is True


def test_comprehensive_caching_all_detected():
    """Test comprehensive caching strategy with all aspects present."""
    task = {
        "title": "Complete caching implementation",
        "description": (
            "Implement multi-tier caching with Redis cache layer and CDN cache. "
            "Design cache keys with proper key generation strategy. "
            "Configure TTL policy with 10-minute expiration. "
            "Implement cache invalidation with purge cache mechanism. "
            "Prevent cache stampede with cache locking. "
            "Handle stale data with stale-while-revalidate pattern. "
            "Maintain cache coherence across distributed cache nodes. "
            "Optimize cold start with cache warming. "
            "Manage cache memory limits with LRU eviction policy. "
            "Enable cache monitoring with hit rate tracking."
        ),
        "acceptance_criteria": [
            "Cache layers configured",
            "Cache key design complete",
            "TTL policy set",
            "Invalidation strategy implemented",
            "Stampede prevention active",
            "Stale data handling verified",
            "Cache coherence maintained",
            "Cold start optimized",
            "Memory limits managed",
            "Monitoring enabled",
        ],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True
    assert result.cache_keys_designed is True
    assert result.ttl_policy_configured is True
    assert result.invalidation_strategy_planned is True
    assert result.cache_stampede_prevented is True
    assert result.stale_data_handled is True
    assert result.cache_coherence_maintained is True
    assert result.cold_start_optimized is True
    assert result.memory_limits_managed is True
    assert result.cache_monitoring_enabled is True
    assert result.readiness_score == 1.0


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_caching_strategy(None)  # type: ignore

    assert isinstance(result, CachingStrategy)
    assert result.cache_layers_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_caching_strategy([{"key": "value"}])  # type: ignore

    assert isinstance(result, CachingStrategy)
    assert result.cache_layers_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_string():
    """Test with string input instead of mapping."""
    result = analyze_caching_strategy("not a mapping")  # type: ignore

    assert isinstance(result, CachingStrategy)
    assert result.cache_layers_defined is False


def test_invalid_task_data_tuple():
    """Test with tuple input instead of mapping."""
    result = analyze_caching_strategy(("tuple", "data"))  # type: ignore

    assert isinstance(result, CachingStrategy)
    assert result.cache_layers_defined is False


def test_partial_data_missing_fields():
    """Test with partial task data missing some fields."""
    task = {
        "title": "Caching implementation",
        # Missing description, acceptance_criteria, etc.
    }

    result = analyze_caching_strategy(task)

    assert isinstance(result, CachingStrategy)
    assert result.readiness_score == 0.0


def test_partial_caching_readiness():
    """Test partial caching readiness with some aspects covered."""
    task = {
        "title": "Basic caching setup",
        "description": "Implement Redis cache layer",
        "acceptance_criteria": [
            "Configure cache TTL",
            "Design cache keys",
        ],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True
    assert result.ttl_policy_configured is True
    assert result.cache_keys_designed is True
    assert result.invalidation_strategy_planned is False
    assert result.cache_stampede_prevented is False
    assert result.stale_data_handled is False
    assert result.cache_coherence_maintained is False
    assert result.cold_start_optimized is False
    assert result.memory_limits_managed is False
    assert result.cache_monitoring_enabled is False
    assert result.readiness_score == 0.3


def test_task_data_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    task = {
        "title": "Caching improvements",
        "acceptance_criteria": [
            "Implement cache invalidation",
            "Add cache monitoring",
            "Configure cache memory limits",
        ],
    }

    result = analyze_caching_strategy(task)

    assert result.invalidation_strategy_planned is True
    assert result.cache_monitoring_enabled is True
    assert result.memory_limits_managed is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    task = {
        "title": "Cache setup",
        "validation_command": "pytest tests/test_cache_keys.py tests/test_ttl.py",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_keys_designed is True
    assert result.ttl_policy_configured is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "REDIS CACHE with TTL POLICY and CACHE INVALIDATION",
        "acceptance_criteria": ["CACHE STAMPEDE prevention", "MONITOR cache hits"],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True
    assert result.ttl_policy_configured is True
    assert result.invalidation_strategy_planned is True
    assert result.cache_stampede_prevented is True
    assert result.cache_monitoring_enabled is True


def test_alternative_terminology_cache_layer_cdn():
    """Test CDN cache terminology is recognized."""
    task = {
        "description": "Configure CDN cache for static assets",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True


def test_alternative_terminology_cache_layer_application():
    """Test application cache terminology is recognized."""
    task = {
        "description": "Set up application cache with in-memory cache",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True


def test_alternative_terminology_cache_layer_database():
    """Test database cache terminology is recognized."""
    task = {
        "description": "Enable database cache for query results",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True


def test_alternative_terminology_cache_layer_memcached():
    """Test memcached cache terminology is recognized."""
    task = {
        "description": "Configure memcached cache cluster",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True


def test_alternative_terminology_cache_layer_varnish():
    """Test Varnish cache terminology is recognized."""
    task = {
        "description": "Set up Varnish cache for HTTP acceleration",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True


def test_alternative_terminology_ttl_expiration():
    """Test expiration terminology is recognized as TTL."""
    task = {
        "description": "Set cache expiration to 30 seconds",
    }

    result = analyze_caching_strategy(task)

    assert result.ttl_policy_configured is True


def test_alternative_terminology_ttl_max_age():
    """Test max-age terminology is recognized as TTL."""
    task = {
        "description": "Configure max-age for cache headers",
    }

    result = analyze_caching_strategy(task)

    assert result.ttl_policy_configured is True


def test_alternative_terminology_invalidation_purge():
    """Test purge cache terminology is recognized."""
    task = {
        "description": "Purge cache after database updates",
    }

    result = analyze_caching_strategy(task)

    assert result.invalidation_strategy_planned is True


def test_alternative_terminology_invalidation_evict():
    """Test evict cache terminology is recognized."""
    task = {
        "description": "Evict cache entries on demand",
    }

    result = analyze_caching_strategy(task)

    assert result.invalidation_strategy_planned is True


def test_alternative_terminology_invalidation_flush():
    """Test flush cache terminology is recognized."""
    task = {
        "description": "Flush cache on configuration changes",
    }

    result = analyze_caching_strategy(task)

    assert result.invalidation_strategy_planned is True


def test_alternative_terminology_invalidation_cache_busting():
    """Test cache busting terminology is recognized."""
    task = {
        "description": "Implement cache busting for static assets",
    }

    result = analyze_caching_strategy(task)

    assert result.invalidation_strategy_planned is True


def test_alternative_terminology_stampede_thundering_herd():
    """Test thundering herd terminology is recognized."""
    task = {
        "description": "Prevent thundering herd problem",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_stampede_prevented is True


def test_alternative_terminology_stampede_dogpile():
    """Test dogpile effect terminology is recognized."""
    task = {
        "description": "Implement dogpile prevention mechanism",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_stampede_prevented is True


def test_alternative_terminology_stampede_cache_lock():
    """Test cache locking terminology is recognized."""
    task = {
        "description": "Use cache locking to prevent concurrent regeneration",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_stampede_prevented is True


def test_alternative_terminology_stale_swr():
    """Test stale-while-revalidate terminology is recognized."""
    task = {
        "description": "Implement stale-while-revalidate pattern",
    }

    result = analyze_caching_strategy(task)

    assert result.stale_data_handled is True


def test_alternative_terminology_stale_consistency():
    """Test cache consistency terminology is recognized."""
    task = {
        "description": "Ensure cache consistency with strong consistency model",
    }

    result = analyze_caching_strategy(task)

    assert result.stale_data_handled is True


def test_alternative_terminology_coherence_distributed():
    """Test distributed cache coherence terminology is recognized."""
    task = {
        "description": "Maintain distributed cache consistency",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_coherence_maintained is True


def test_alternative_terminology_coherence_sync():
    """Test cache synchronization terminology is recognized."""
    task = {
        "description": "Implement cache synchronization across nodes",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_coherence_maintained is True


def test_alternative_terminology_cold_start_warming():
    """Test cache warming terminology is recognized."""
    task = {
        "description": "Implement cache warming on application startup",
    }

    result = analyze_caching_strategy(task)

    assert result.cold_start_optimized is True


def test_alternative_terminology_cold_start_priming():
    """Test cache priming terminology is recognized."""
    task = {
        "description": "Prime cache with frequently accessed data",
    }

    result = analyze_caching_strategy(task)

    assert result.cold_start_optimized is True


def test_alternative_terminology_cold_start_preload():
    """Test cache preloading terminology is recognized."""
    task = {
        "description": "Preload cache with essential data",
    }

    result = analyze_caching_strategy(task)

    assert result.cold_start_optimized is True


def test_alternative_terminology_memory_eviction():
    """Test cache eviction terminology is recognized."""
    task = {
        "description": "Configure cache eviction policy",
    }

    result = analyze_caching_strategy(task)

    assert result.memory_limits_managed is True


def test_alternative_terminology_memory_lru():
    """Test LRU eviction terminology is recognized."""
    task = {
        "description": "Use LRU cache eviction strategy",
    }

    result = analyze_caching_strategy(task)

    assert result.memory_limits_managed is True


def test_alternative_terminology_memory_lfu():
    """Test LFU eviction terminology is recognized."""
    task = {
        "description": "Implement LFU cache policy",
    }

    result = analyze_caching_strategy(task)

    assert result.memory_limits_managed is True


def test_alternative_terminology_monitoring_hit_rate():
    """Test cache hit rate terminology is recognized."""
    task = {
        "description": "Track cache hit rate and miss ratio",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_monitoring_enabled is True


def test_alternative_terminology_monitoring_metrics():
    """Test cache metrics terminology is recognized."""
    task = {
        "description": "Collect cache metrics for analysis",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_monitoring_enabled is True


def test_to_dict_method():
    """Test CachingStrategy.to_dict() serialization."""
    strategy = CachingStrategy(
        cache_layers_defined=True,
        cache_keys_designed=True,
        ttl_policy_configured=False,
        invalidation_strategy_planned=True,
        cache_stampede_prevented=False,
        stale_data_handled=True,
        cache_coherence_maintained=False,
        cold_start_optimized=True,
        memory_limits_managed=False,
        cache_monitoring_enabled=True,
    )

    result = strategy.to_dict()

    assert isinstance(result, dict)
    assert result["cache_layers_defined"] is True
    assert result["cache_keys_designed"] is True
    assert result["ttl_policy_configured"] is False
    assert result["invalidation_strategy_planned"] is True
    assert result["cache_stampede_prevented"] is False
    assert result["stale_data_handled"] is True
    assert result["cache_coherence_maintained"] is False
    assert result["cold_start_optimized"] is True
    assert result["memory_limits_managed"] is False
    assert result["cache_monitoring_enabled"] is True
    assert result["readiness_score"] == 0.6


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "Caching implementation",
        "description": "Configure Redis cache",
        "acceptance_criteria": ["Design cache keys"],
        "requirements": ["Set TTL policy"],
        "notes": ["Implement cache invalidation"],
        "risks": ["No cache stampede prevention"],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True
    assert result.cache_keys_designed is True
    assert result.ttl_policy_configured is True
    assert result.invalidation_strategy_planned is True
    assert result.cache_stampede_prevented is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    task = {
        "validation_commands": [
            "test_cache_layers.py",
            "test_ttl_policy.py",
        ],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True
    assert result.ttl_policy_configured is True


def test_dataclass_immutability():
    """Test that CachingStrategy is frozen/immutable."""
    strategy = CachingStrategy(cache_layers_defined=True)

    with pytest.raises(AttributeError):
        strategy.cache_layers_defined = False  # type: ignore


def test_write_through_pattern():
    """Test write-through caching pattern detection."""
    task = {
        "description": "Implement write-through cache for consistency",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True


def test_write_behind_pattern():
    """Test write-behind caching pattern detection."""
    task = {
        "description": "Use write-behind cache for performance",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True


def test_cache_aside_pattern():
    """Test cache-aside pattern detection."""
    task = {
        "description": "Implement cache-aside pattern for lazy loading",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True


def test_negative_caching_edge_case():
    """Test negative caching detection."""
    task = {
        "description": "Implement negative caching for 404 responses",
        "acceptance_criteria": [
            "Cache null results",
            "Cache empty responses",
        ],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True


def test_distributed_caching_edge_case():
    """Test distributed caching detection."""
    task = {
        "description": "Set up distributed cache across multiple regions with cache replication",
        "acceptance_criteria": [
            "Configure multi-region cache",
            "Implement cache coherence",
            "Enable cache synchronization",
        ],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True
    assert result.cache_coherence_maintained is True


def test_cache_warming_edge_case():
    """Test cache warming on deployment detection."""
    task = {
        "description": "Prime cache on deployment to avoid cold start performance issues",
        "acceptance_criteria": [
            "Preload frequently accessed data",
            "Initialize cache on startup",
        ],
    }

    result = analyze_caching_strategy(task)

    assert result.cold_start_optimized is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Configure Redis cache with TTL and cache invalidation",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True
    assert result.ttl_policy_configured is True
    assert result.invalidation_strategy_planned is True


def test_readiness_score_calculation():
    """Test readiness score calculation with different combinations."""
    # 0/10 = 0.0
    task1 = {"description": "Generic task"}
    result1 = analyze_caching_strategy(task1)
    assert result1.readiness_score == 0.0

    # 1/10 = 0.1
    task2 = {"description": "Set up Redis cache"}
    result2 = analyze_caching_strategy(task2)
    assert result2.readiness_score == 0.1

    # 5/10 = 0.5
    task3 = {
        "description": "Redis cache, cache keys, TTL, invalidation, and cache monitoring"
    }
    result3 = analyze_caching_strategy(task3)
    assert result3.readiness_score == 0.5

    # 10/10 = 1.0
    task4 = {
        "description": (
            "Redis cache, cache keys, TTL, invalidation, cache stampede prevention, "
            "stale data handling, cache coherence, cache warming, memory limits, "
            "and cache monitoring"
        )
    }
    result4 = analyze_caching_strategy(task4)
    assert result4.readiness_score == 1.0


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is False
    assert result.readiness_score == 0.0


def test_edge_cache_pattern():
    """Test edge cache pattern detection."""
    task = {
        "description": "Configure edge cache for global content delivery",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True


def test_multi_tier_cache_pattern():
    """Test multi-tier cache pattern detection."""
    task = {
        "description": "Implement multi-tier caching with L1 and L2 cache hierarchy",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_layers_defined is True


def test_cache_key_generation_pattern():
    """Test cache key generation pattern detection."""
    task = {
        "description": "Generate cache keys based on request parameters",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_keys_designed is True


def test_cache_key_structure_pattern():
    """Test cache key structure pattern detection."""
    task = {
        "description": "Define cache key structure for efficient lookups",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_keys_designed is True


def test_cache_refresh_pattern():
    """Test cache refresh pattern detection."""
    task = {
        "description": "Refresh cache periodically to maintain freshness",
    }

    result = analyze_caching_strategy(task)

    assert result.invalidation_strategy_planned is True


def test_single_flight_pattern():
    """Test single flight pattern detection."""
    task = {
        "description": "Use single flight pattern to prevent duplicate cache loads",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_stampede_prevented is True


def test_cache_coalescing_pattern():
    """Test cache coalescing pattern detection."""
    task = {
        "description": "Implement cache coalescing for concurrent requests",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_stampede_prevented is True


def test_eventual_consistency_pattern():
    """Test eventual consistency pattern detection."""
    task = {
        "description": "Use eventual consistency model for distributed cache",
    }

    result = analyze_caching_strategy(task)

    assert result.stale_data_handled is True


def test_cache_capacity_pattern():
    """Test cache capacity pattern detection."""
    task = {
        "description": "Set cache capacity to 1GB with overflow protection",
    }

    result = analyze_caching_strategy(task)

    assert result.memory_limits_managed is True


def test_cache_quota_pattern():
    """Test cache quota pattern detection."""
    task = {
        "description": "Configure cache quota per tenant",
    }

    result = analyze_caching_strategy(task)

    assert result.memory_limits_managed is True


def test_cache_instrumentation_pattern():
    """Test cache instrumentation pattern detection."""
    task = {
        "description": "Add cache instrumentation for observability",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_monitoring_enabled is True


def test_cache_telemetry_pattern():
    """Test cache telemetry pattern detection."""
    task = {
        "description": "Enable cache telemetry for performance tracking",
    }

    result = analyze_caching_strategy(task)

    assert result.cache_monitoring_enabled is True
