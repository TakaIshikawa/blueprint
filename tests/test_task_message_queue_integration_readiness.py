"""Tests for message queue integration readiness analyzer."""

import pytest

from blueprint.task_message_queue_integration_readiness import (
    MessageQueueIntegrationReadiness,
    analyze_message_queue_integration_readiness,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_message_queue_integration_readiness({})

    assert isinstance(result, MessageQueueIntegrationReadiness)
    assert result.message_schema_defined is False
    assert result.delivery_guarantees_specified is False
    assert result.retry_policy_defined is False
    assert result.dead_letter_queue_configured is False
    assert result.idempotency_handled is False
    assert result.message_ordering_addressed is False
    assert result.backpressure_managed is False
    assert result.monitoring_configured is False
    assert result.readiness_score == 0.0


def test_message_schema_detected():
    """Detect message schema definition in task data."""
    task = {
        "title": "Define message schema",
        "description": "Create message schema validation for user events",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True
    assert result.delivery_guarantees_specified is False
    assert result.readiness_score == 0.125


def test_delivery_guarantees_detected():
    """Detect delivery guarantees in task data."""
    task = {
        "description": "Implement at-least-once delivery guarantee for messages",
        "acceptance_criteria": ["Delivery semantic documented", "Message acknowledgment configured"],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.delivery_guarantees_specified is True
    assert result.message_schema_defined is False


def test_retry_policy_detected():
    """Detect retry policy in task data."""
    task = {
        "description": "Implement exponential backoff retry strategy for failed messages",
        "acceptance_criteria": ["Retry policy configured", "Max retry attempts defined"],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.retry_policy_defined is True
    assert result.message_schema_defined is False


def test_dead_letter_queue_detected():
    """Detect dead letter queue configuration in task data."""
    task = {
        "title": "Configure DLQ",
        "description": "Set up dead-letter queue for poison message handling",
        "acceptance_criteria": ["DLQ configured", "Failed message routing enabled"],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.dead_letter_queue_configured is True
    assert result.message_schema_defined is False


def test_idempotency_detected():
    """Detect idempotency handling in task data."""
    task = {
        "description": "Ensure idempotent consumer with duplicate detection",
        "acceptance_criteria": [
            "Idempotency keys implemented",
            "Duplicate message prevention configured",
        ],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.idempotency_handled is True
    assert result.message_schema_defined is False


def test_message_ordering_detected():
    """Detect message ordering requirements in task data."""
    task = {
        "description": "Maintain message ordering with FIFO queue and partition keys",
        "acceptance_criteria": ["Message order guaranteed", "Sequential processing enabled"],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_ordering_addressed is True
    assert result.message_schema_defined is False


def test_backpressure_detected():
    """Detect backpressure management in task data."""
    task = {
        "description": "Implement backpressure handling with rate limiting and consumer scaling",
        "acceptance_criteria": ["Flow control configured", "Throttling enabled"],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.backpressure_managed is True
    assert result.message_schema_defined is False


def test_monitoring_detected():
    """Detect monitoring configuration in task data."""
    task = {
        "description": "Set up queue monitoring with consumer lag metrics and alerting",
        "acceptance_criteria": [
            "Queue depth monitoring enabled",
            "Consumer lag alerts configured",
        ],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.monitoring_configured is True
    assert result.message_schema_defined is False


def test_comprehensive_queue_integration_all_detected():
    """Test comprehensive message queue integration with all aspects present."""
    task = {
        "title": "Complete message queue integration",
        "description": (
            "Implement message queue with schema validation and at-least-once delivery. "
            "Configure retry policy with exponential backoff and dead-letter queue. "
            "Ensure idempotent processing with duplicate detection. "
            "Maintain message ordering with FIFO queue. "
            "Implement backpressure handling and consumer lag monitoring."
        ),
        "acceptance_criteria": [
            "Message schema defined and validated",
            "Delivery guarantees specified",
            "Retry policy with exponential backoff",
            "DLQ configured for poison messages",
            "Idempotency keys implemented",
            "Message ordering maintained",
            "Backpressure managed with rate limiting",
            "Queue monitoring and alerting enabled",
        ],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True
    assert result.delivery_guarantees_specified is True
    assert result.retry_policy_defined is True
    assert result.dead_letter_queue_configured is True
    assert result.idempotency_handled is True
    assert result.message_ordering_addressed is True
    assert result.backpressure_managed is True
    assert result.monitoring_configured is True
    assert result.readiness_score == 1.0


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_message_queue_integration_readiness(None)  # type: ignore

    assert isinstance(result, MessageQueueIntegrationReadiness)
    assert result.message_schema_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_message_queue_integration_readiness([{"key": "value"}])  # type: ignore

    assert isinstance(result, MessageQueueIntegrationReadiness)
    assert result.message_schema_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_string():
    """Test with string input instead of mapping."""
    result = analyze_message_queue_integration_readiness("not a mapping")  # type: ignore

    assert isinstance(result, MessageQueueIntegrationReadiness)
    assert result.message_schema_defined is False


def test_invalid_task_data_tuple():
    """Test with tuple input instead of mapping."""
    result = analyze_message_queue_integration_readiness(("tuple", "data"))  # type: ignore

    assert isinstance(result, MessageQueueIntegrationReadiness)
    assert result.message_schema_defined is False


def test_partial_data_missing_fields():
    """Test with partial task data missing some fields."""
    task = {
        "title": "Queue setup",
        # Missing description, acceptance_criteria, etc.
    }

    result = analyze_message_queue_integration_readiness(task)

    assert isinstance(result, MessageQueueIntegrationReadiness)
    assert result.readiness_score == 0.0


def test_partial_queue_integration_readiness():
    """Test partial queue integration readiness with some aspects covered."""
    task = {
        "title": "Partial queue integration",
        "description": "Define message schema and configure retry policy",
        "acceptance_criteria": [
            "Schema validation enabled",
            "Retry strategy implemented",
        ],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True
    assert result.retry_policy_defined is True
    assert result.delivery_guarantees_specified is False
    assert result.dead_letter_queue_configured is False
    assert result.idempotency_handled is False
    assert result.message_ordering_addressed is False
    assert result.backpressure_managed is False
    assert result.monitoring_configured is False
    assert result.readiness_score == 0.25


def test_task_data_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    task = {
        "title": "Queue improvements",
        "acceptance_criteria": [
            "Define payload schema validation",
            "Configure exactly-once delivery guarantee",
            "Implement DLQ for failed messages",
        ],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True
    assert result.delivery_guarantees_specified is True
    assert result.dead_letter_queue_configured is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    task = {
        "title": "Setup queue",
        "validation_command": "pytest tests/test_message_schema.py tests/test_idempotency.py",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True
    assert result.idempotency_handled is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "MESSAGE SCHEMA with RETRY POLICY and IDEMPOTENCY",
        "acceptance_criteria": ["DLQ configured", "BACKPRESSURE managed"],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True
    assert result.retry_policy_defined is True
    assert result.idempotency_handled is True
    assert result.dead_letter_queue_configured is True
    assert result.backpressure_managed is True


def test_alternative_terminology_schema():
    """Test alternative schema terminology is recognized."""
    task = {
        "description": "Define payload format with Avro schema and schema registry",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True


def test_alternative_terminology_delivery():
    """Test alternative delivery guarantee terminology is recognized."""
    task = {
        "description": "Implement exactly-once delivery with message acknowledgment",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.delivery_guarantees_specified is True


def test_alternative_terminology_retry():
    """Test alternative retry terminology is recognized."""
    task = {
        "description": "Configure backoff strategy with max retries for failure retry",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.retry_policy_defined is True


def test_alternative_terminology_dlq():
    """Test alternative DLQ terminology is recognized."""
    task = {
        "description": "Set up poison message queue for undeliverable messages",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.dead_letter_queue_configured is True


def test_alternative_terminology_idempotency():
    """Test alternative idempotency terminology is recognized."""
    task = {
        "description": "Implement deduplication with unique message ID to prevent duplicate processing",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.idempotency_handled is True


def test_alternative_terminology_ordering():
    """Test alternative ordering terminology is recognized."""
    task = {
        "description": "Ensure FIFO ordering with partition key for sequential processing",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_ordering_addressed is True


def test_alternative_terminology_backpressure():
    """Test alternative backpressure terminology is recognized."""
    task = {
        "description": "Implement flow control with throttling and prefetch limit",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.backpressure_managed is True


def test_alternative_terminology_monitoring():
    """Test alternative monitoring terminology is recognized."""
    task = {
        "description": "Set up queue metrics with processing time tracking and lag alerting",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.monitoring_configured is True


def test_to_dict_method():
    """Test MessageQueueIntegrationReadiness.to_dict() serialization."""
    readiness = MessageQueueIntegrationReadiness(
        message_schema_defined=True,
        delivery_guarantees_specified=True,
        retry_policy_defined=False,
        dead_letter_queue_configured=True,
        idempotency_handled=False,
        message_ordering_addressed=True,
        backpressure_managed=False,
        monitoring_configured=True,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["message_schema_defined"] is True
    assert result["delivery_guarantees_specified"] is True
    assert result["retry_policy_defined"] is False
    assert result["dead_letter_queue_configured"] is True
    assert result["idempotency_handled"] is False
    assert result["message_ordering_addressed"] is True
    assert result["backpressure_managed"] is False
    assert result["monitoring_configured"] is True
    assert result["readiness_score"] == 0.625


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "Queue setup",
        "description": "Define message schema",
        "acceptance_criteria": ["Delivery guarantees specified"],
        "requirements": ["Retry policy configured"],
        "notes": ["DLQ needed"],
        "risks": ["No idempotency handling plan"],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True
    assert result.delivery_guarantees_specified is True
    assert result.retry_policy_defined is True
    assert result.dead_letter_queue_configured is True
    assert result.idempotency_handled is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    task = {
        "validation_commands": [
            "test_schema_validation.py",
            "test_retry_mechanism.py",
        ],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True
    assert result.retry_policy_defined is True


def test_dataclass_immutability():
    """Test that MessageQueueIntegrationReadiness is frozen/immutable."""
    readiness = MessageQueueIntegrationReadiness(message_schema_defined=True)

    with pytest.raises(AttributeError):
        readiness.message_schema_defined = False  # type: ignore


def test_kafka_specific_terminology():
    """Test Kafka-specific terminology is recognized."""
    task = {
        "description": "Configure Kafka with partition key for message ordering and consumer lag monitoring",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_ordering_addressed is True
    assert result.monitoring_configured is True


def test_rabbitmq_specific_terminology():
    """Test RabbitMQ-specific terminology is recognized."""
    task = {
        "description": "Set up RabbitMQ with message acknowledgment and prefetch count",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.delivery_guarantees_specified is True
    assert result.backpressure_managed is True


def test_sqs_specific_terminology():
    """Test SQS-specific terminology is recognized."""
    task = {
        "description": "Configure SQS FIFO queue with message deduplication and DLQ",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_ordering_addressed is True
    assert result.idempotency_handled is True
    assert result.dead_letter_queue_configured is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Define message schema and configure retry policy",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True
    assert result.retry_policy_defined is True


def test_readiness_score_calculation():
    """Test readiness score calculation with different combinations."""
    # 0/8 = 0.0
    task1 = {"description": "Generic task"}
    result1 = analyze_message_queue_integration_readiness(task1)
    assert result1.readiness_score == 0.0

    # 1/8 = 0.125
    task2 = {"description": "Define message schema"}
    result2 = analyze_message_queue_integration_readiness(task2)
    assert result2.readiness_score == 0.125

    # 4/8 = 0.5
    task3 = {
        "description": "Message schema with retry policy, DLQ, and idempotency"
    }
    result3 = analyze_message_queue_integration_readiness(task3)
    assert result3.readiness_score == 0.5

    # 8/8 = 1.0
    task4 = {
        "description": (
            "Message schema with delivery guarantees, retry policy, DLQ, "
            "idempotency, message ordering, backpressure, and monitoring"
        )
    }
    result4 = analyze_message_queue_integration_readiness(task4)
    assert result4.readiness_score == 1.0


def test_exactly_once_delivery():
    """Test exactly-once delivery pattern detection."""
    task = {
        "description": "Implement exactly-once delivery with idempotent processing",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.delivery_guarantees_specified is True
    assert result.idempotency_handled is True


def test_consumer_scaling_pattern():
    """Test consumer scaling as part of backpressure management."""
    task = {
        "description": "Implement consumer scaling based on queue depth",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.backpressure_managed is True
    assert result.monitoring_configured is True


def test_poison_message_handling():
    """Test poison message handling patterns."""
    task = {
        "description": "Handle poison messages with DLQ and retry limit",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.dead_letter_queue_configured is True
    assert result.retry_policy_defined is True


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is False
    assert result.readiness_score == 0.0


def test_protobuf_schema():
    """Test Protobuf schema detection."""
    task = {
        "description": "Define message contract with Protobuf schema",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True


def test_json_schema():
    """Test JSON schema detection."""
    task = {
        "description": "Validate messages with JSON schema",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True


def test_at_most_once_delivery():
    """Test at-most-once delivery pattern."""
    task = {
        "description": "Configure at-most-once delivery semantic",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.delivery_guarantees_specified is True


def test_load_shedding_pattern():
    """Test load shedding as backpressure management."""
    task = {
        "description": "Implement load shedding to handle high message throughput",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.backpressure_managed is True


def test_message_latency_monitoring():
    """Test message latency monitoring detection."""
    task = {
        "description": "Track message latency metrics and processing time",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.monitoring_configured is True


def test_sequence_number_ordering():
    """Test sequence number for message ordering."""
    task = {
        "description": "Use sequence number to maintain message order",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_ordering_addressed is True


def test_schema_evolution():
    """Test schema evolution handling."""
    task = {
        "description": "Support schema evolution for backward compatibility",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.message_schema_defined is True


def test_dlq_monitoring():
    """Test DLQ monitoring detection."""
    task = {
        "description": "Monitor DLQ for failed messages with alerting",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.dead_letter_queue_configured is True
    assert result.monitoring_configured is True


def test_reprocessing_strategy():
    """Test reprocessing strategy as retry policy."""
    task = {
        "description": "Define reprocessing logic for failed messages",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.retry_policy_defined is True


def test_message_durability():
    """Test message durability as delivery guarantee."""
    task = {
        "description": "Ensure message durability with persistent storage",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.delivery_guarantees_specified is True


def test_ack_mode_configuration():
    """Test acknowledgment mode configuration."""
    task = {
        "description": "Configure ack mode for message processing confirmation",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.delivery_guarantees_specified is True


def test_queue_depth_monitoring():
    """Test queue depth monitoring."""
    task = {
        "description": "Monitor queue depth to prevent overflow",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.monitoring_configured is True


def test_message_throughput_monitoring():
    """Test message throughput monitoring."""
    task = {
        "description": "Track message throughput metrics for capacity planning",
    }

    result = analyze_message_queue_integration_readiness(task)

    assert result.monitoring_configured is True
