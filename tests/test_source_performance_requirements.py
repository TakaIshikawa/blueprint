"""Tests for performance requirements extractor."""

import pytest

from blueprint.source_performance_requirements import (
    PerformanceRequirements,
    extract_performance_requirements,
)


def test_empty_source_data_returns_all_false():
    """Empty source data should return all fields as False."""
    result = extract_performance_requirements({})

    assert isinstance(result, PerformanceRequirements)
    assert result.response_time_targets_specified is False
    assert result.throughput_requirements_defined is False
    assert result.concurrency_levels_identified is False
    assert result.resource_utilization_limits_set is False
    assert result.load_testing_strategy_planned is False
    assert result.performance_budgets_established is False
    assert result.optimization_priorities_defined is False
    assert result.scalability_targets_specified is False
    assert result.metric_clarity_ensured is False
    assert result.slo_alignment_addressed is False
    assert result.completeness_score == 0.0


def test_response_time_detected():
    """Detect response time targets in source data."""
    source = {
        "title": "Performance requirements",
        "description": "API response time target of 200ms with p95 latency under 500ms",
    }

    result = extract_performance_requirements(source)

    assert result.response_time_targets_specified is True
    assert result.throughput_requirements_defined is False


def test_throughput_detected():
    """Detect throughput requirements in source data."""
    source = {
        "description": "System must handle 10000 requests per second with peak throughput of 50000 RPS",
    }

    result = extract_performance_requirements(source)

    assert result.throughput_requirements_defined is True


def test_concurrency_detected():
    """Detect concurrency levels in source data."""
    source = {
        "description": "Support 5000 concurrent users with maximum concurrent connections of 10000",
    }

    result = extract_performance_requirements(source)

    assert result.concurrency_levels_identified is True


def test_load_testing_detected():
    """Detect load testing strategy in source data."""
    source = {
        "requirements": ["Load testing strategy required", "Performance testing plan"],
    }

    result = extract_performance_requirements(source)

    assert result.load_testing_strategy_planned is True


def test_scalability_targets_detected():
    """Detect scalability targets in source data."""
    source = {
        "description": "System must scale to millions of users with horizontal scaling and auto-scale capability",
    }

    result = extract_performance_requirements(source)

    assert result.scalability_targets_specified is True


def test_slo_alignment_detected():
    """Detect SLO alignment in source data."""
    source = {
        "requirements": ["99.9% availability SLO", "Performance SLO aligned with SLA"],
    }

    result = extract_performance_requirements(source)

    assert result.slo_alignment_addressed is True


def test_comprehensive_performance_all_detected():
    """Test comprehensive performance requirements with all aspects present."""
    source = {
        "title": "Complete performance specification",
        "description": (
            "API response time target of 200ms with p99 latency under 1s. "
            "Handle 50000 requests per second throughput with 10000 concurrent users. "
            "CPU usage limit of 80% and memory utilization under 4GB. "
            "Load testing strategy includes stress testing and capacity testing. "
            "Performance budget established for all endpoints. "
            "Optimize for latency with priority on critical path. "
            "Scale to millions of users with auto-scaling. "
            "Track performance metrics and KPIs. "
            "99.95% availability SLO with performance SLA."
        ),
    }

    result = extract_performance_requirements(source)

    assert result.response_time_targets_specified is True
    assert result.throughput_requirements_defined is True
    assert result.concurrency_levels_identified is True
    assert result.resource_utilization_limits_set is True
    assert result.load_testing_strategy_planned is True
    assert result.performance_budgets_established is True
    assert result.optimization_priorities_defined is True
    assert result.scalability_targets_specified is True
    assert result.metric_clarity_ensured is True
    assert result.slo_alignment_addressed is True
    assert result.completeness_score == 1.0


def test_invalid_source_data_none():
    """Test with None input."""
    result = extract_performance_requirements(None)  # type: ignore

    assert isinstance(result, PerformanceRequirements)
    assert result.response_time_targets_specified is False
    assert result.completeness_score == 0.0


def test_to_dict_method():
    """Test PerformanceRequirements.to_dict() serialization."""
    reqs = PerformanceRequirements(
        response_time_targets_specified=True,
        throughput_requirements_defined=True,
        concurrency_levels_identified=False,
        resource_utilization_limits_set=True,
        load_testing_strategy_planned=False,
        performance_budgets_established=True,
        optimization_priorities_defined=False,
        scalability_targets_specified=True,
        metric_clarity_ensured=False,
        slo_alignment_addressed=True,
    )

    result = reqs.to_dict()

    assert isinstance(result, dict)
    assert result["response_time_targets_specified"] is True
    assert result["throughput_requirements_defined"] is True
    assert result["concurrency_levels_identified"] is False
    assert result["completeness_score"] == 0.6


def test_dataclass_immutability():
    """Test that PerformanceRequirements is frozen/immutable."""
    reqs = PerformanceRequirements(response_time_targets_specified=True)

    with pytest.raises(AttributeError):
        reqs.response_time_targets_specified = False  # type: ignore


def test_percentile_targets_edge_case():
    """Test percentile targets detection."""
    source = {
        "description": "p95 response time under 300ms and p99 latency target of 800ms",
    }

    result = extract_performance_requirements(source)

    assert result.response_time_targets_specified is True


def test_peak_load_scenarios_edge_case():
    """Test peak load scenario detection."""
    source = {
        "requirements": ["Handle peak load of 100K QPS", "Stress testing for peak traffic"],
    }

    result = extract_performance_requirements(source)

    assert result.throughput_requirements_defined is True
    assert result.load_testing_strategy_planned is True


def test_cold_start_performance_edge_case():
    """Test cold start performance (no specific pattern, should not match)."""
    source = {
        "description": "Cold start performance optimization",
    }

    result = extract_performance_requirements(source)

    assert result.optimization_priorities_defined is True
