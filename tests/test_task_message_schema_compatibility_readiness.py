import json

from blueprint.task_message_schema_compatibility_readiness import (
    MessageSchemaCompatibilityReadinessPlan,
    analyze_task_message_schema_compatibility_readiness,
    build_task_message_schema_compatibility_readiness_plan,
    extract_task_message_schema_compatibility_readiness,
    generate_task_message_schema_compatibility_readiness,
    recommend_task_message_schema_compatibility_readiness,
    summarize_task_message_schema_compatibility_readiness,
    task_message_schema_compatibility_readiness_plan_to_dict,
    task_message_schema_compatibility_readiness_plan_to_dicts,
    task_message_schema_compatibility_readiness_plan_to_markdown,
)


def test_complete_message_schema_task_is_ready():
    result = build_task_message_schema_compatibility_readiness_plan(
        _plan(
            [
                _task(
                    "task-complete",
                    "Update Kafka message schema",
                    (
                        "Change Kafka topic message schema and Avro schema registry contract. "
                        "Inventory producers and affected consumers. Use backward compatible schema evolution. "
                        "Add schema versioning, producer fixtures, consumer fixture tests, replay and backfill plan, "
                        "dead-letter handling, and monitoring alerts for deserialization failures."
                    ),
                    ["src/kafka/topics/invoices.avsc"],
                )
            ]
        )
    )

    assert isinstance(result, MessageSchemaCompatibilityReadinessPlan)
    record = result.records[0]
    assert record.readiness == "ready"
    assert record.detected_signals == (
        "pubsub_topic",
        "kafka_message",
        "json_schema",
        "avro",
        "producer_consumer",
    )
    assert record.missing_criteria == ()
    assert result.summary["impacted_task_count"] == 1
    assert result.summary["missing_criterion_count"] == 0


def test_detects_schema_tasks_from_text_metadata_and_paths_with_gaps():
    result = analyze_task_message_schema_compatibility_readiness(
        _plan(
            [
                _task(
                    "task-json",
                    "Revise queue event JSON schema",
                    "Update queued event JSON schema for billing messages.",
                    ["schemas/billing_event.schema.json"],
                ),
                _task(
                    "task-proto",
                    "Change Pub/Sub protobuf",
                    "Pub/Sub topic protobuf message with compatibility mode and schema version.",
                    ["proto/account_event.proto"],
                ),
                _task("task-copy", "Docs copy", "No message schema changes are in scope.", []),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert result.impacted_task_ids == ("task-json", "task-proto")
    assert result.ignored_task_ids == ("task-copy",)
    assert by_id["task-json"].detected_signals == ("queue_event", "json_schema")
    assert "producer_consumer_inventory" in by_id["task-json"].missing_criteria
    assert by_id["task-proto"].present_criteria == ("compatibility_mode", "versioning")
    assert result.summary["task_count"] == 3
    assert result.summary["impacted_task_count"] == 2
    assert result.summary["missing_criterion_count"] > 0
    assert result.summary["readiness_counts"]["needs_planning"] == 1
    assert result.summary["readiness_counts"]["partial"] == 1


def test_aliases_serialization_and_markdown_are_stable():
    plan = _plan([_task("task-alias", "Avro schema", "Avro schema with compatibility mode.", ["schemas/a.avsc"])])
    results = [
        build_task_message_schema_compatibility_readiness_plan(plan),
        analyze_task_message_schema_compatibility_readiness(plan),
        extract_task_message_schema_compatibility_readiness(plan),
        generate_task_message_schema_compatibility_readiness(plan),
        recommend_task_message_schema_compatibility_readiness(plan),
        summarize_task_message_schema_compatibility_readiness(plan),
    ]
    payload = task_message_schema_compatibility_readiness_plan_to_dict(results[0])

    assert all(result.to_dict() == results[0].to_dict() for result in results)
    assert task_message_schema_compatibility_readiness_plan_to_dicts(results[0]) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Task Message Schema Compatibility Readiness: plan-message-schema" in task_message_schema_compatibility_readiness_plan_to_markdown(results[0])


def _plan(tasks):
    return {"id": "plan-message-schema", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
