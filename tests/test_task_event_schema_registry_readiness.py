import json

from blueprint.task_event_schema_registry_readiness import (
    TaskEventSchemaRegistryReadinessPlan,
    analyze_task_event_schema_registry_readiness,
    build_task_event_schema_registry_readiness_plan,
    derive_task_event_schema_registry_readiness,
    extract_task_event_schema_registry_readiness,
    generate_task_event_schema_registry_readiness,
    recommend_task_event_schema_registry_readiness,
    summarize_task_event_schema_registry_readiness,
    task_event_schema_registry_readiness_plan_to_dict,
    task_event_schema_registry_readiness_plan_to_dicts,
    task_event_schema_registry_readiness_plan_to_markdown,
)


def test_complete_event_schema_registry_task_is_ready():
    result = build_task_event_schema_registry_readiness_plan(
        _plan(
            [
                _task(
                    "schema-ready",
                    "Register invoice event schema",
                    (
                        "Update event schema in the schema registry for producers and downstream consumers. "
                        "Schema owner is billing platform. Compatibility policy is backward compatible. "
                        "Use schema version v2 with deprecation rules, validation tests, contract tests, "
                        "and consumer communication release notes."
                    ),
                    ["src/events/schema_registry/invoice_event.avsc"],
                )
            ]
        )
    )

    assert isinstance(result, TaskEventSchemaRegistryReadinessPlan)
    record = result.records[0]
    assert record.readiness == "ready"
    assert record.present_criteria == (
        "schema_ownership",
        "compatibility_policy",
        "versioning",
        "validation_tests",
        "consumer_communication",
    )
    assert record.missing_criteria == ()
    assert result.summary["missing_criterion_count"] == 0


def test_detects_event_contract_tasks_and_ignores_no_impact():
    result = analyze_task_event_schema_registry_readiness(
        _plan(
            [
                _task("producer", "Producer event contract", "Publisher changes event contract for downstream services.", ["src/producers/orders.py"]),
                _task("consumer", "Consumer schema docs", "Consumer group reads schema registry topic contract.", ["src/consumers/orders.py"]),
                _task("docs", "Docs only", "No event schema or schema registry changes are required.", []),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert result.impacted_task_ids == ("consumer", "producer")
    assert result.ignored_task_ids == ("docs",)
    assert "producer" in by_id["producer"].detected_signals
    assert "consumer" in by_id["consumer"].detected_signals
    assert by_id["producer"].readiness == "needs_planning"
    assert result.summary["readiness_counts"]["needs_planning"] == 2


def test_aliases_serialization_and_markdown_are_stable():
    plan = _plan([_task("alias", "Schema registry", "Schema registry with compatibility policy.", ["schemas/event.avsc"])])
    results = [
        build_task_event_schema_registry_readiness_plan(plan),
        analyze_task_event_schema_registry_readiness(plan),
        extract_task_event_schema_registry_readiness(plan),
        generate_task_event_schema_registry_readiness(plan),
        derive_task_event_schema_registry_readiness(plan),
        summarize_task_event_schema_registry_readiness(plan),
        recommend_task_event_schema_registry_readiness(plan),
    ]
    payload = task_event_schema_registry_readiness_plan_to_dict(results[0])

    assert all(result.to_dict() == results[0].to_dict() for result in results)
    assert task_event_schema_registry_readiness_plan_to_dicts(results[0]) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Task Event Schema Registry Readiness: plan-event-schema-registry" in task_event_schema_registry_readiness_plan_to_markdown(results[0])


def _plan(tasks):
    return {"id": "plan-event-schema-registry", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
