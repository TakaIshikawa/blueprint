"""Tests for observability instrumentation readiness analyzer."""

import pytest

from blueprint.task_observability_instrumentation_readiness import (
    ObservabilityInstrumentationReadiness,
    analyze_observability_instrumentation_readiness,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_observability_instrumentation_readiness({})

    assert isinstance(result, ObservabilityInstrumentationReadiness)
    assert result.metrics_collection_defined is False
    assert result.distributed_tracing_enabled is False
    assert result.structured_logging_configured is False
    assert result.alerting_rules_specified is False
    assert result.sli_slo_defined is False
    assert result.trace_coverage_complete is False
    assert result.log_aggregation_configured is False
    assert result.dashboard_requirements_specified is False
    assert result.readiness_score == 0.0


def test_metrics_collection_detected():
    """Detect metrics collection in task data."""
    task = {
        "title": "Add metrics collection",
        "description": "Implement Prometheus metrics tracking for API endpoints",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.metrics_collection_defined is True
    assert result.distributed_tracing_enabled is False
    assert result.readiness_score == 0.125


def test_distributed_tracing_detected():
    """Detect distributed tracing in task data."""
    task = {
        "description": "Enable OpenTelemetry distributed tracing with span propagation",
        "acceptance_criteria": ["Trace context propagated", "Jaeger integration configured"],
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.distributed_tracing_enabled is True
    assert result.metrics_collection_defined is False


def test_structured_logging_detected():
    """Detect structured logging in task data."""
    task = {
        "description": "Implement structured logging with JSON format and correlation IDs",
        "acceptance_criteria": ["Log context added", "Correlation ID tracking enabled"],
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.structured_logging_configured is True
    assert result.metrics_collection_defined is False


def test_alerting_rules_detected():
    """Detect alerting rules in task data."""
    task = {
        "title": "Configure alerting rules",
        "description": "Set up PagerDuty alerting with threshold-based alert conditions",
        "acceptance_criteria": ["Alert rules defined", "On-call integration configured"],
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.alerting_rules_specified is True
    assert result.metrics_collection_defined is False


def test_sli_slo_detected():
    """Detect SLI/SLO definitions in task data."""
    task = {
        "description": "Define SLIs and SLOs for service availability and latency targets",
        "acceptance_criteria": [
            "Service level indicators documented",
            "Error budget established",
        ],
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.sli_slo_defined is True
    assert result.metrics_collection_defined is False


def test_trace_coverage_detected():
    """Detect trace coverage requirements in task data."""
    task = {
        "description": "Instrument all critical paths with full trace coverage",
        "acceptance_criteria": ["All endpoints instrumented", "Span creation complete"],
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.trace_coverage_complete is True
    assert result.metrics_collection_defined is False


def test_log_aggregation_detected():
    """Detect log aggregation configuration in task data."""
    task = {
        "description": "Configure Elasticsearch for centralized log aggregation with Logstash",
        "acceptance_criteria": ["ELK stack deployed", "Log forwarding enabled"],
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.log_aggregation_configured is True
    assert result.metrics_collection_defined is False


def test_dashboard_detected():
    """Detect dashboard requirements in task data."""
    task = {
        "description": "Create Grafana dashboard for metrics visualization",
        "acceptance_criteria": [
            "Monitoring dashboard created",
            "Key metrics visualized",
        ],
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.dashboard_requirements_specified is True
    assert result.metrics_collection_defined is False


def test_comprehensive_observability_all_detected():
    """Test comprehensive observability with all aspects present."""
    task = {
        "title": "Complete observability instrumentation",
        "description": (
            "Implement comprehensive observability with Prometheus metrics collection "
            "and OpenTelemetry distributed tracing. Configure structured logging with "
            "correlation IDs and ELK stack log aggregation. Define SLIs and SLOs "
            "with PagerDuty alerting rules. Ensure complete trace coverage and "
            "create Grafana dashboards for visualization."
        ),
        "acceptance_criteria": [
            "Metrics collection implemented",
            "Distributed tracing enabled",
            "Structured logging configured",
            "Alerting rules defined",
            "SLIs and SLOs documented",
            "Full trace coverage achieved",
            "Log aggregation configured",
            "Dashboards created",
        ],
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.metrics_collection_defined is True
    assert result.distributed_tracing_enabled is True
    assert result.structured_logging_configured is True
    assert result.alerting_rules_specified is True
    assert result.sli_slo_defined is True
    assert result.trace_coverage_complete is True
    assert result.log_aggregation_configured is True
    assert result.dashboard_requirements_specified is True
    assert result.readiness_score == 1.0


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_observability_instrumentation_readiness(None)  # type: ignore

    assert isinstance(result, ObservabilityInstrumentationReadiness)
    assert result.metrics_collection_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_observability_instrumentation_readiness([{"key": "value"}])  # type: ignore

    assert isinstance(result, ObservabilityInstrumentationReadiness)
    assert result.metrics_collection_defined is False
    assert result.readiness_score == 0.0


def test_partial_observability_readiness():
    """Test partial observability readiness with some aspects covered."""
    task = {
        "title": "Basic observability setup",
        "description": "Add metrics collection and structured logging",
        "acceptance_criteria": [
            "Prometheus metrics enabled",
            "JSON logging configured",
        ],
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.metrics_collection_defined is True
    assert result.structured_logging_configured is True
    assert result.distributed_tracing_enabled is False
    assert result.alerting_rules_specified is False
    assert result.sli_slo_defined is False
    assert result.trace_coverage_complete is False
    assert result.log_aggregation_configured is False
    assert result.dashboard_requirements_specified is False
    assert result.readiness_score == 0.25


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "METRICS COLLECTION with DISTRIBUTED TRACING and STRUCTURED LOGGING",
        "acceptance_criteria": ["ALERTING RULES configured", "SLO DEFINED"],
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.metrics_collection_defined is True
    assert result.distributed_tracing_enabled is True
    assert result.structured_logging_configured is True
    assert result.alerting_rules_specified is True
    assert result.sli_slo_defined is True


def test_alternative_terminology_metrics():
    """Test alternative metrics terminology is recognized."""
    task = {
        "description": "Track performance with custom counter metrics and histogram metrics",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.metrics_collection_defined is True


def test_alternative_terminology_tracing():
    """Test alternative tracing terminology is recognized."""
    task = {
        "description": "Implement Zipkin for end-to-end tracing with trace ID propagation",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.distributed_tracing_enabled is True


def test_alternative_terminology_logging():
    """Test alternative logging terminology is recognized."""
    task = {
        "description": "Configure contextual logging with log metadata and log levels",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.structured_logging_configured is True


def test_alternative_terminology_alerting():
    """Test alternative alerting terminology is recognized."""
    task = {
        "description": "Define alert thresholds with incident alert notifications",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.alerting_rules_specified is True


def test_alternative_terminology_slo():
    """Test alternative SLO terminology is recognized."""
    task = {
        "description": "Set availability target and error budget for reliability",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.sli_slo_defined is True


def test_alternative_terminology_trace_coverage():
    """Test alternative trace coverage terminology is recognized."""
    task = {
        "description": "Instrument all critical operations with span instrumentation",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.trace_coverage_complete is True


def test_alternative_terminology_log_aggregation():
    """Test alternative log aggregation terminology is recognized."""
    task = {
        "description": "Set up Fluentd log collector with log forwarding pipeline",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.log_aggregation_configured is True


def test_alternative_terminology_dashboard():
    """Test alternative dashboard terminology is recognized."""
    task = {
        "description": "Create Kibana visualization for observability metrics",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.dashboard_requirements_specified is True


def test_to_dict_method():
    """Test ObservabilityInstrumentationReadiness.to_dict() serialization."""
    readiness = ObservabilityInstrumentationReadiness(
        metrics_collection_defined=True,
        distributed_tracing_enabled=True,
        structured_logging_configured=False,
        alerting_rules_specified=True,
        sli_slo_defined=False,
        trace_coverage_complete=True,
        log_aggregation_configured=False,
        dashboard_requirements_specified=True,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["metrics_collection_defined"] is True
    assert result["distributed_tracing_enabled"] is True
    assert result["structured_logging_configured"] is False
    assert result["alerting_rules_specified"] is True
    assert result["sli_slo_defined"] is False
    assert result["trace_coverage_complete"] is True
    assert result["log_aggregation_configured"] is False
    assert result["dashboard_requirements_specified"] is True
    assert result["readiness_score"] == 0.625


def test_dataclass_immutability():
    """Test that ObservabilityInstrumentationReadiness is frozen/immutable."""
    readiness = ObservabilityInstrumentationReadiness(metrics_collection_defined=True)

    with pytest.raises(AttributeError):
        readiness.metrics_collection_defined = False  # type: ignore


def test_high_cardinality_metrics():
    """Test high-cardinality metrics concern detection."""
    task = {
        "description": "Emit custom metrics while avoiding high-cardinality labels",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.metrics_collection_defined is True


def test_sampling_strategies():
    """Test sampling strategy detection in tracing."""
    task = {
        "description": "Configure trace sampling with OpenTelemetry for high-volume services",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.distributed_tracing_enabled is True


def test_log_volume_management():
    """Test log volume management concern detection."""
    task = {
        "description": "Implement structured logging with appropriate log levels to manage volume",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.structured_logging_configured is True


def test_readiness_score_calculation():
    """Test readiness score calculation with different combinations."""
    # 0/8 = 0.0
    task1 = {"description": "Generic task"}
    result1 = analyze_observability_instrumentation_readiness(task1)
    assert result1.readiness_score == 0.0

    # 1/8 = 0.125
    task2 = {"description": "Add metrics collection"}
    result2 = analyze_observability_instrumentation_readiness(task2)
    assert result2.readiness_score == 0.125

    # 4/8 = 0.5
    task3 = {
        "description": "Metrics collection with distributed tracing, structured logging, and alerting"
    }
    result3 = analyze_observability_instrumentation_readiness(task3)
    assert result3.readiness_score == 0.5


def test_prometheus_metrics():
    """Test Prometheus-specific metrics detection."""
    task = {
        "description": "Implement Prometheus gauge and counter metrics",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.metrics_collection_defined is True


def test_statsd_metrics():
    """Test StatsD metrics detection."""
    task = {
        "description": "Configure StatsD for metrics emission",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.metrics_collection_defined is True


def test_otel_tracing():
    """Test OpenTelemetry (OTEL) abbreviation detection."""
    task = {
        "description": "Enable OTEL instrumentation for request tracing",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.distributed_tracing_enabled is True


def test_correlation_id_logging():
    """Test correlation ID in structured logging."""
    task = {
        "description": "Add correlation ID to all log entries for request tracking",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.structured_logging_configured is True


def test_error_budget():
    """Test error budget as SLO indicator."""
    task = {
        "description": "Define error budget based on uptime SLO",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.sli_slo_defined is True


def test_anomaly_detection():
    """Test anomaly detection in alerting."""
    task = {
        "description": "Configure anomaly detection for automated alerting",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.alerting_rules_specified is True


def test_cloudwatch_logs():
    """Test AWS CloudWatch Logs detection."""
    task = {
        "description": "Forward logs to CloudWatch Logs for aggregation",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.log_aggregation_configured is True


def test_datadog_integration():
    """Test Datadog integration detection."""
    task = {
        "description": "Configure Datadog logs and dashboard for observability",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.log_aggregation_configured is True
    assert result.dashboard_requirements_specified is True


def test_splunk_logs():
    """Test Splunk log aggregation detection."""
    task = {
        "description": "Send logs to Splunk for centralized logging",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.log_aggregation_configured is True


def test_span_propagation():
    """Test span propagation detection."""
    task = {
        "description": "Ensure span context propagation across service boundaries",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.distributed_tracing_enabled is True


def test_service_level_indicator():
    """Test full spelling of service level indicator."""
    task = {
        "description": "Define service level indicators for API performance",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.sli_slo_defined is True


def test_latency_slo():
    """Test latency SLO detection."""
    task = {
        "description": "Set latency SLO at p99 < 500ms",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.sli_slo_defined is True


def test_instrument_endpoints():
    """Test endpoint instrumentation for trace coverage."""
    task = {
        "description": "Instrument all API endpoints with tracing spans",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.trace_coverage_complete is True


def test_business_metrics():
    """Test business metrics detection."""
    task = {
        "description": "Track business metrics like conversion rate and revenue",
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.metrics_collection_defined is True


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "Observability setup",
        "description": "Add metrics collection",
        "acceptance_criteria": ["Distributed tracing enabled"],
        "requirements": ["Structured logging configured"],
        "notes": ["Alerting rules needed"],
        "risks": ["No SLO defined yet"],
    }

    result = analyze_observability_instrumentation_readiness(task)

    assert result.metrics_collection_defined is True
    assert result.distributed_tracing_enabled is True
    assert result.structured_logging_configured is True
    assert result.alerting_rules_specified is True
    assert result.sli_slo_defined is True
