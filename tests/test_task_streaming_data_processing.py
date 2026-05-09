"""Tests for streaming data processing readiness analyzer."""

import pytest

from blueprint.task_streaming_data_processing import (
    StreamingDataProcessingReadiness,
    analyze_streaming_data_processing_readiness,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_streaming_data_processing_readiness({})

    assert isinstance(result, StreamingDataProcessingReadiness)
    assert result.event_schema_defined is False
    assert result.processing_guarantees_specified is False
    assert result.windowing_strategy_defined is False
    assert result.state_management_addressed is False
    assert result.backpressure_handling_implemented is False
    assert result.out_of_order_events_handled is False
    assert result.watermarks_configured is False
    assert result.checkpoint_recovery_enabled is False
    assert result.event_time_semantics_used is False
    assert result.monitoring_configured is False
    assert result.readiness_score == 0.0


def test_event_schema_detected():
    """Detect event schema in task data."""
    task = {
        "title": "Define event schema",
        "description": "Create Avro schema for stream events",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True


def test_processing_guarantees_detected():
    """Detect processing guarantees in task data."""
    task = {
        "description": "Implement exactly-once processing semantics",
        "acceptance_criteria": ["Processing guarantees verified", "Idempotent processing ensured"],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.processing_guarantees_specified is True


def test_windowing_strategy_detected():
    """Detect windowing strategy in task data."""
    task = {
        "title": "Configure stream windowing",
        "description": "Set up tumbling window with 5-minute intervals",
        "acceptance_criteria": ["Sliding window implemented"],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.windowing_strategy_defined is True


def test_state_management_detected():
    """Detect state management in task data."""
    task = {
        "description": "Configure RocksDB state backend for stateful processing",
        "acceptance_criteria": ["State store configured", "State checkpoint enabled"],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.state_management_addressed is True


def test_backpressure_handling_detected():
    """Detect backpressure handling in task data."""
    task = {
        "title": "Implement flow control",
        "description": "Add backpressure handling with rate limiting",
        "acceptance_criteria": ["Throttling configured", "Buffer overflow prevented"],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.backpressure_handling_implemented is True


def test_out_of_order_events_detected():
    """Detect out-of-order event handling in task data."""
    task = {
        "description": "Handle out-of-order events with 10 minute allowed lateness",
        "acceptance_criteria": ["Late arrival handling implemented"],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.out_of_order_events_handled is True


def test_watermarks_detected():
    """Detect watermark configuration in task data."""
    task = {
        "title": "Configure watermarks",
        "description": "Set up event-time watermarks for windowing",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.watermarks_configured is True


def test_checkpoint_recovery_detected():
    """Detect checkpoint recovery in task data."""
    task = {
        "description": "Enable checkpointing for fault tolerance",
        "acceptance_criteria": ["State snapshots configured", "Crash recovery tested"],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.checkpoint_recovery_enabled is True


def test_event_time_semantics_detected():
    """Detect event time semantics in task data."""
    task = {
        "description": "Use event-time processing instead of processing-time",
        "acceptance_criteria": ["Event timestamp handling implemented"],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_time_semantics_used is True


def test_monitoring_detected():
    """Detect monitoring configuration in task data."""
    task = {
        "title": "Set up stream monitoring",
        "description": "Monitor consumer lag and processing latency",
        "acceptance_criteria": ["Stream metrics collected", "Throughput monitoring enabled"],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.monitoring_configured is True


def test_comprehensive_streaming_all_detected():
    """Test comprehensive streaming setup with all aspects present."""
    task = {
        "title": "Complete streaming data processing setup",
        "description": (
            "Define event schema with Avro. "
            "Implement exactly-once processing guarantees. "
            "Configure tumbling window strategy. "
            "Set up RocksDB state management. "
            "Add backpressure handling with flow control. "
            "Handle out-of-order events with allowed lateness. "
            "Configure event-time watermarks. "
            "Enable checkpointing for fault tolerance. "
            "Use event-time semantics for processing. "
            "Monitor stream lag and throughput."
        ),
        "acceptance_criteria": [
            "Event schema defined",
            "Processing guarantees verified",
            "Windowing strategy implemented",
            "State management configured",
            "Backpressure handling added",
            "Late arrivals handled",
            "Watermarks configured",
            "Checkpoint recovery tested",
            "Event time processing enabled",
            "Stream monitoring active",
        ],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True
    assert result.processing_guarantees_specified is True
    assert result.windowing_strategy_defined is True
    assert result.state_management_addressed is True
    assert result.backpressure_handling_implemented is True
    assert result.out_of_order_events_handled is True
    assert result.watermarks_configured is True
    assert result.checkpoint_recovery_enabled is True
    assert result.event_time_semantics_used is True
    assert result.monitoring_configured is True
    assert abs(result.readiness_score - 1.0) < 0.01


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_streaming_data_processing_readiness(None)  # type: ignore

    assert isinstance(result, StreamingDataProcessingReadiness)
    assert result.event_schema_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_streaming_data_processing_readiness([{"key": "value"}])  # type: ignore

    assert isinstance(result, StreamingDataProcessingReadiness)
    assert result.readiness_score == 0.0


def test_invalid_task_data_string():
    """Test with string input instead of mapping."""
    result = analyze_streaming_data_processing_readiness("not a mapping")  # type: ignore

    assert isinstance(result, StreamingDataProcessingReadiness)
    assert result.event_schema_defined is False


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "EVENT SCHEMA with EXACTLY-ONCE and TUMBLING WINDOW",
        "acceptance_criteria": ["STATE MANAGEMENT configured", "WATERMARKS defined"],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True
    assert result.processing_guarantees_specified is True
    assert result.windowing_strategy_defined is True
    assert result.state_management_addressed is True
    assert result.watermarks_configured is True


def test_kafka_streams_patterns():
    """Test detection of Kafka Streams specific patterns."""
    task = {
        "title": "Implement Kafka Streams topology",
        "description": (
            "Build stream processing with Kafka Streams. "
            "Use RocksDB state-store for stateful operations. "
            "Configure exactly-once semantics. "
            "Set up session windows for user activity."
        ),
        "acceptance_criteria": [
            "Event schema registered",
            "State management with RocksDB",
            "Session windows configured",
        ],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True
    assert result.state_management_addressed is True
    assert result.processing_guarantees_specified is True
    assert result.windowing_strategy_defined is True


def test_flink_patterns():
    """Test detection of Apache Flink specific patterns."""
    task = {
        "title": "Apache Flink streaming job",
        "description": (
            "Implement Flink job with checkpointing. "
            "Handle watermarks for event-time processing. "
            "Configure state backend with savepoints. "
            "Add sliding window aggregations."
        ),
        "acceptance_criteria": [
            "Checkpoint recovery enabled",
            "Watermark generation configured",
            "Sliding windows implemented",
        ],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.checkpoint_recovery_enabled is True
    assert result.watermarks_configured is True
    assert result.state_management_addressed is True
    assert result.windowing_strategy_defined is True


def test_spark_streaming_patterns():
    """Test detection of Spark Streaming specific patterns."""
    task = {
        "title": "Spark Structured Streaming pipeline",
        "description": (
            "Build structured streaming with Spark. "
            "Define Avro event schema. "
            "Handle late data with watermarks. "
            "Configure stateful streaming with state store."
        ),
        "acceptance_criteria": [
            "Event schema with Avro",
            "Watermarks for late data",
            "Stateful processing enabled",
        ],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True
    assert result.watermarks_configured is True
    assert result.out_of_order_events_handled is True
    assert result.state_management_addressed is True


def test_session_windows():
    """Test session window detection."""
    task = {
        "description": "Implement session windows for user activity tracking",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.windowing_strategy_defined is True


def test_tumbling_windows():
    """Test tumbling window detection."""
    task = {
        "description": "Configure tumbling window with 5-minute fixed intervals",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.windowing_strategy_defined is True


def test_sliding_windows():
    """Test sliding window detection."""
    task = {
        "description": "Set up sliding window aggregations",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.windowing_strategy_defined is True


def test_event_time_vs_processing_time():
    """Test event time vs processing time distinction."""
    task = {
        "description": "Use event-time vs processing-time for windowing",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_time_semantics_used is True


def test_at_least_once_semantics():
    """Test at-least-once processing guarantee detection."""
    task = {
        "description": "Configure at-least-once delivery guarantee",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.processing_guarantees_specified is True


def test_at_most_once_semantics():
    """Test at-most-once processing guarantee detection."""
    task = {
        "description": "Implement at-most-once processing with fire-and-forget",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.processing_guarantees_specified is True


def test_deduplication():
    """Test deduplication as processing guarantee."""
    task = {
        "description": "Add deduplication logic to prevent duplicate processing",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.processing_guarantees_specified is True


def test_protobuf_schema():
    """Test Protobuf schema detection."""
    task = {
        "description": "Define Protobuf schema for event serialization",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True


def test_json_schema():
    """Test JSON schema detection."""
    task = {
        "description": "Validate events against JSON schema",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True


def test_message_schema():
    """Test message schema detection."""
    task = {
        "description": "Define message schema for stream messages",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True


def test_reactive_streams():
    """Test reactive streams backpressure detection."""
    task = {
        "description": "Implement reactive streams for backpressure support",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.backpressure_handling_implemented is True


def test_rate_limiter():
    """Test rate limiting detection."""
    task = {
        "description": "Add rate limiter to prevent overwhelming downstream",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.backpressure_handling_implemented is True


def test_consumer_lag_monitoring():
    """Test consumer lag monitoring detection."""
    task = {
        "description": "Monitor consumer lag to detect processing delays",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.monitoring_configured is True


def test_processing_latency_monitoring():
    """Test processing latency monitoring detection."""
    task = {
        "description": "Track processing latency metrics for streams",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.monitoring_configured is True


def test_readiness_score_core_only():
    """Test score with only core requirements (40%)."""
    task = {
        "description": (
            "Define event schema. "
            "Ensure exactly-once processing. "
            "Enable checkpointing."
        ),
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True
    assert result.processing_guarantees_specified is True
    assert result.checkpoint_recovery_enabled is True
    assert result.readiness_score == 0.4


def test_readiness_score_consistency_only():
    """Test score with only consistency checks (40%)."""
    task = {
        "description": (
            "Configure tumbling windows. "
            "Set up state management. "
            "Handle out-of-order events. "
            "Configure watermarks. "
            "Use event-time semantics."
        ),
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.windowing_strategy_defined is True
    assert result.state_management_addressed is True
    assert result.out_of_order_events_handled is True
    assert result.watermarks_configured is True
    assert result.event_time_semantics_used is True
    assert result.readiness_score == 0.4


def test_readiness_score_operations_only():
    """Test score with only operations checks (20%)."""
    task = {
        "description": (
            "Implement backpressure handling. "
            "Configure stream monitoring."
        ),
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.backpressure_handling_implemented is True
    assert result.monitoring_configured is True
    assert result.readiness_score == 0.2


def test_readiness_score_combined():
    """Test score with combined aspects."""
    task = {
        "description": (
            "Define event-schema with exactly-once processing. "
            "Configure tumbling-window with state-management. "
            "Add backpressure-handling and stream-monitoring."
        ),
    }

    result = analyze_streaming_data_processing_readiness(task)

    # Core: 2/3*40%=26.67%, Consistency: 2/5*40%=16%, Operations: 2/2*20%=20%
    expected_score = (2 / 3) * 0.4 + (2 / 5) * 0.4 + (2 / 2) * 0.2
    assert abs(result.readiness_score - expected_score) < 0.01


def test_to_dict_method():
    """Test StreamingDataProcessingReadiness.to_dict() serialization."""
    readiness = StreamingDataProcessingReadiness(
        event_schema_defined=True,
        processing_guarantees_specified=False,
        windowing_strategy_defined=True,
        state_management_addressed=True,
        backpressure_handling_implemented=False,
        out_of_order_events_handled=True,
        watermarks_configured=True,
        checkpoint_recovery_enabled=False,
        event_time_semantics_used=True,
        monitoring_configured=True,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["event_schema_defined"] is True
    assert result["processing_guarantees_specified"] is False
    assert result["windowing_strategy_defined"] is True
    assert result["state_management_addressed"] is True
    assert result["backpressure_handling_implemented"] is False
    assert result["out_of_order_events_handled"] is True
    assert result["watermarks_configured"] is True
    assert result["checkpoint_recovery_enabled"] is False
    assert result["event_time_semantics_used"] is True
    assert result["monitoring_configured"] is True
    # Core: 1/3*40%=13.33%, Consistency: 5/5*40%=40%, Operations: 1/2*20%=10%
    expected_score = (1 / 3) * 0.4 + (5 / 5) * 0.4 + (1 / 2) * 0.2
    assert abs(result["readiness_score"] - expected_score) < 0.01


def test_dataclass_immutability():
    """Test that StreamingDataProcessingReadiness is frozen/immutable."""
    readiness = StreamingDataProcessingReadiness(event_schema_defined=True)

    with pytest.raises(AttributeError):
        readiness.event_schema_defined = False  # type: ignore


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "Streaming improvements",
        "description": "Implement event-schema",
        "acceptance_criteria": ["Exactly-once processing"],
        "requirements": ["Tumbling-windows configured"],
        "notes": ["State-management needed"],
        "risks": ["No watermarks defined"],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True
    assert result.processing_guarantees_specified is True
    assert result.windowing_strategy_defined is True
    assert result.state_management_addressed is True
    assert result.watermarks_configured is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    task = {
        "validation_command": "pytest test-event-schema.py test-windowing.py",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True
    assert result.windowing_strategy_defined is True


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is False
    assert result.readiness_score == 0.0


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Implement event-schema with windowing-strategy",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True
    assert result.windowing_strategy_defined is True


def test_partial_streaming_setup():
    """Test partial streaming setup with some aspects covered."""
    task = {
        "title": "Initial streaming setup",
        "description": "Define event-schema and configure exactly-once semantics",
        "acceptance_criteria": [
            "Schema registered",
            "Processing guarantees configured",
        ],
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.event_schema_defined is True
    assert result.processing_guarantees_specified is True
    assert result.windowing_strategy_defined is False
    assert result.checkpoint_recovery_enabled is False
    # Core: 2/3*40%=26.67%, Consistency: 0/5*40%=0%, Operations: 0/2*20%=0%
    expected_score = (2 / 3) * 0.4
    assert abs(result.readiness_score - expected_score) < 0.01


def test_late_data_handling():
    """Test late data handling detection."""
    task = {
        "description": "Handle late data with 10 minute grace period",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.out_of_order_events_handled is True


def test_allowed_lateness():
    """Test allowed lateness detection."""
    task = {
        "description": "Configure allowed lateness for windowed aggregations",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.out_of_order_events_handled is True


def test_savepoints():
    """Test savepoint detection as checkpoint recovery."""
    task = {
        "description": "Configure savepoints for stateful recovery",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.checkpoint_recovery_enabled is True


def test_fault_tolerance():
    """Test fault tolerance detection."""
    task = {
        "description": "Ensure fault tolerance with state snapshots",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.checkpoint_recovery_enabled is True


def test_low_watermark():
    """Test low watermark detection."""
    task = {
        "description": "Track low watermark for progress tracking",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.watermarks_configured is True


def test_high_watermark():
    """Test high watermark detection."""
    task = {
        "description": "Monitor high watermark for consumer progress",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.watermarks_configured is True


def test_buffer_overflow_prevention():
    """Test buffer overflow as backpressure concern."""
    task = {
        "description": "Prevent buffer overflow with flow control",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.backpressure_handling_implemented is True


def test_congestion_control():
    """Test congestion control detection."""
    task = {
        "description": "Implement congestion control for stream processing",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.backpressure_handling_implemented is True


def test_throughput_monitoring():
    """Test throughput monitoring detection."""
    task = {
        "description": "Monitor stream throughput and processing rates",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.monitoring_configured is True


def test_backlog_monitoring():
    """Test backlog monitoring detection."""
    task = {
        "description": "Track backlog monitoring for queue depths",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.monitoring_configured is True


def test_stream_health_checks():
    """Test stream health monitoring detection."""
    task = {
        "description": "Monitor stream health and processing status",
    }

    result = analyze_streaming_data_processing_readiness(task)

    assert result.monitoring_configured is True
