import json
from types import SimpleNamespace

from blueprint.task_api_caching_readiness import (
    TaskApiCachingReadiness,
    TaskApiCachingReadinessFinding,
    TaskApiCachingReadinessPlan,
    analyze_task_api_caching_readiness,
    build_task_api_caching_readiness_plan,
    derive_task_api_caching_readiness_plan,
    extract_task_api_caching_readiness_findings,
    generate_task_api_caching_readiness_plan,
    summarize_task_api_caching_readiness,
)


def test_cache_implementation_detected_with_partial_readiness():
    """Test basic cache implementation detection with partial safeguards."""
    plan = {
        "id": "plan-cache-impl",
        "tasks": [
            {
                "id": "task-cache-impl",
                "title": "Implement API caching layer",
                "description": "Add caching to API responses with cache key design and TTL configuration.",
                "acceptance_criteria": [
                    "Cache implementation uses distributed cache (Redis)",
                    "Cache keys are properly namespaced",
                ],
            },
        ],
    }

    result = build_task_api_caching_readiness_plan(plan)
    finding = result.findings[0]

    assert isinstance(result, TaskApiCachingReadinessPlan)
    assert isinstance(finding, TaskApiCachingReadinessFinding)
    assert finding.task_id == "task-cache-impl"
    assert "cache_implementation" in finding.detected_signals
    assert "cache_key_design" in finding.detected_signals
    assert "distributed_cache" in finding.detected_signals
    assert finding.readiness in {"weak", "partial"}
    assert len(finding.actionable_remediations) > 0


def test_cache_invalidation_with_weak_readiness():
    """Test cache invalidation logic without proper tests results in weak readiness."""
    plan = {
        "id": "plan-cache-invalidation",
        "tasks": [
            {
                "id": "task-invalidation",
                "title": "Add cache invalidation logic for user updates",
                "description": "Implement cache purge hooks when user data changes. Clear cache on profile updates.",
                "acceptance_criteria": ["Cache is cleared when user profile is updated"],
            },
        ],
    }

    result = build_task_api_caching_readiness_plan(plan)
    finding = result.findings[0]

    assert "cache_invalidation_logic" in finding.detected_signals
    assert "cache_invalidation_tests" not in finding.present_safeguards
    assert "cache_invalidation_tests" in finding.missing_safeguards
    assert finding.readiness == "weak"
    assert any("invalidation" in r.lower() for r in finding.actionable_remediations)


def test_strong_readiness_with_comprehensive_safeguards():
    """Test comprehensive caching implementation with all safeguards results in strong readiness."""
    plan = {
        "id": "plan-complete-caching",
        "tasks": [
            {
                "id": "task-complete",
                "title": "Implement complete API caching strategy",
                "description": (
                    "Add caching layer with Redis distributed cache. "
                    "Implement cache key design with proper namespacing. "
                    "Configure cache TTL and cache headers (Cache-Control, max-age). "
                    "Add cache invalidation logic with purge hooks. "
                    "Set up cache monitoring metrics with Prometheus dashboards. "
                    "Establish performance baselines and benchmarks for cache hit ratios."
                ),
                "acceptance_criteria": [
                    "Cache invalidation tests verify purge mechanisms and prevent stale data",
                    "Cache key collision tests ensure uniqueness and proper namespace isolation",
                    "Cache consistency tests validate data freshness and race condition handling",
                    "Performance benchmarks measure cache hit ratios and response time improvements",
                    "Monitoring dashboards track cache metrics, eviction rates, and alerts",
                    "Cache documentation covers strategy, TTL policies, and invalidation hooks",
                ],
            },
        ],
    }

    result = build_task_api_caching_readiness_plan(plan)
    finding = result.findings[0]

    # All signals should be detected
    assert "cache_implementation" in finding.detected_signals
    assert "cache_invalidation_logic" in finding.detected_signals
    assert "cache_key_design" in finding.detected_signals
    assert "cache_monitoring_metrics" in finding.detected_signals
    assert "performance_baseline" in finding.detected_signals

    # All safeguards should be present
    assert "cache_invalidation_tests" in finding.present_safeguards
    assert "cache_key_collision_tests" in finding.present_safeguards
    assert "cache_consistency_tests" in finding.present_safeguards
    assert "performance_benchmarks" in finding.present_safeguards
    assert "monitoring_dashboards" in finding.present_safeguards
    assert "cache_documentation" in finding.present_safeguards

    assert len(finding.missing_safeguards) == 0
    assert finding.readiness == "strong"
    assert len(finding.actionable_remediations) == 0


def test_performance_baseline_and_monitoring_detection():
    """Test detection of performance baseline and monitoring requirements."""
    plan = {
        "id": "plan-monitoring",
        "tasks": [
            {
                "id": "task-monitoring",
                "title": "Add cache monitoring and performance tracking",
                "description": (
                    "Set up cache monitoring metrics and performance baseline measurements. "
                    "Track cache hit ratio and response time improvements. "
                    "Configure Grafana dashboards for cache observability."
                ),
                "acceptance_criteria": [
                    "Cache metrics are tracked in Prometheus",
                    "Performance benchmarks establish baseline measurements",
                ],
            },
        ],
    }

    result = build_task_api_caching_readiness_plan(plan)
    finding = result.findings[0]

    assert "cache_monitoring_metrics" in finding.detected_signals
    assert "cache_hit_ratio_tracking" in finding.detected_signals
    assert "performance_baseline" in finding.detected_signals
    assert "monitoring_dashboards" in finding.present_safeguards
    assert "performance_benchmarks" in finding.present_safeguards


def test_analyze_task_api_caching_readiness_function():
    """Test the analyze_task_api_caching_readiness function."""
    task = {
        "id": "task-analyze",
        "title": "Add caching with invalidation strategy",
        "description": (
            "Implement cache layer with cache invalidation logic. "
            "Add cache monitoring metrics and performance baseline. "
            "Include cache invalidation tests and monitoring dashboards."
        ),
    }

    readiness = analyze_task_api_caching_readiness(task)

    assert isinstance(readiness, TaskApiCachingReadiness)
    assert readiness.task_id == "task-analyze"
    assert readiness.cache_implementation_status in {"not_started", "planned", "in_progress"}
    assert readiness.invalidation_strategy in {"not_defined", "defined", "tested"}
    assert readiness.monitoring_plan in {"not_defined", "planned", "implemented"}
    assert readiness.performance_baseline in {"not_established", "planned", "established"}


def test_no_caching_scope_is_filtered_out():
    """Test that tasks explicitly stating no caching impact are filtered out."""
    plan = {
        "id": "plan-no-cache",
        "tasks": [
            {
                "id": "task-no-cache",
                "title": "Update API endpoint",
                "description": "This change has no caching impact. No cache changes required.",
                "acceptance_criteria": ["Endpoint returns updated data"],
            },
        ],
    }

    result = build_task_api_caching_readiness_plan(plan)

    assert len(result.findings) == 0
    assert "task-no-cache" in result.not_applicable_task_ids


def test_compatibility_functions():
    """Test that derive and generate functions work as expected."""
    plan = {
        "id": "plan-compat",
        "tasks": [
            {
                "id": "task-compat",
                "title": "Add cache implementation",
                "description": "Implement caching with proper cache key design",
            },
        ],
    }

    result1 = build_task_api_caching_readiness_plan(plan)
    result2 = derive_task_api_caching_readiness_plan(plan)
    result3 = generate_task_api_caching_readiness_plan(plan)

    assert len(result1.findings) == len(result2.findings) == len(result3.findings)
    assert result1.findings[0].task_id == result2.findings[0].task_id == result3.findings[0].task_id


def test_extract_findings():
    """Test extracting findings directly."""
    plan = {
        "id": "plan-extract",
        "tasks": [
            {
                "id": "task-extract",
                "title": "Implement cache invalidation",
                "description": "Add cache purge mechanism",
            },
        ],
    }

    findings = extract_task_api_caching_readiness_findings(plan)

    assert len(findings) == 1
    assert findings[0].task_id == "task-extract"


def test_summarize_readiness():
    """Test summarizing caching readiness."""
    plan = {
        "id": "plan-summary",
        "tasks": [
            {
                "id": "task-summary-1",
                "title": "Add caching",
                "description": "Implement cache with tests",
                "acceptance_criteria": ["Cache invalidation tests verify correctness"],
            },
            {
                "id": "task-summary-2",
                "title": "No caching needed",
                "description": "This has no cache impact",
            },
        ],
    }

    summary = summarize_task_api_caching_readiness(plan)

    assert "caching_task_count" in summary
    assert "not_applicable_task_count" in summary
    assert "readiness_counts" in summary
    assert "overall_readiness" in summary
    assert summary["caching_task_count"] >= 1


def test_to_dict_serialization():
    """Test that findings can be serialized to dictionaries."""
    plan = {
        "id": "plan-serialize",
        "tasks": [
            {
                "id": "task-serialize",
                "title": "Add caching",
                "description": "Implement cache layer",
            },
        ],
    }

    result = build_task_api_caching_readiness_plan(plan)
    result_dict = result.to_dict()
    finding_dicts = result.to_dicts()

    assert isinstance(result_dict, dict)
    assert "plan_id" in result_dict
    assert "findings" in result_dict
    assert isinstance(finding_dicts, list)
    # Can be JSON serialized
    json.dumps(result_dict)
    json.dumps(finding_dicts)


def test_to_markdown_rendering():
    """Test that results can be rendered as Markdown."""
    plan = {
        "id": "plan-markdown",
        "tasks": [
            {
                "id": "task-md",
                "title": "Add caching",
                "description": "Implement cache",
            },
        ],
    }

    result = build_task_api_caching_readiness_plan(plan)
    markdown = result.to_markdown()

    assert isinstance(markdown, str)
    assert "# Task API Caching Readiness" in markdown
    assert "task-md" in markdown


def test_simple_namespace_plan():
    """Test that plans represented as SimpleNamespace objects work."""
    task = SimpleNamespace(
        id="task-ns",
        title="Add caching",
        description="Implement cache implementation",
    )
    plan = SimpleNamespace(
        id="plan-ns",
        tasks=[task],
    )

    result = build_task_api_caching_readiness_plan(plan)

    assert len(result.findings) == 1
    assert result.findings[0].task_id == "task-ns"
