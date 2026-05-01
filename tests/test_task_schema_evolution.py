import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_schema_evolution import (
    TaskSchemaEvolutionPlan,
    TaskSchemaEvolutionRecord,
    analyze_task_schema_evolution,
    build_task_schema_evolution_plan,
    summarize_task_schema_evolution,
    task_schema_evolution_plan_to_dict,
    task_schema_evolution_plan_to_markdown,
)


def test_additive_database_column_is_additive_safe_with_database_guardrails():
    result = build_task_schema_evolution_plan(
        _plan(
            [
                _task(
                    "task-add-column",
                    title="Add nullable account status column",
                    description="Add column account_status with a defaulted nullable value.",
                    files_or_modules=["migrations/versions/20260501_add_account_status.sql"],
                )
            ]
        )
    )

    assert isinstance(result, TaskSchemaEvolutionPlan)
    assert result.plan_id == "plan-schema-evolution"
    assert result.schema_task_ids == ("task-add-column",)
    assert result.summary["risk_counts"]["additive_safe"] == 1
    record = result.records[0]
    assert isinstance(record, TaskSchemaEvolutionRecord)
    assert record.evolution_risk == "additive_safe"
    assert record.schema_surfaces == ("database",)
    assert record.change_indicators == ("additive schema or contract extension",)
    assert (
        "Confirm additive fields are optional, nullable, or defaulted for existing producers and consumers."
        in record.guardrails
    )
    assert any("expand, backfill, verify, contract" in value for value in record.guardrails)
    assert record.evidence[:2] == (
        "files_or_modules: migrations/versions/20260501_add_account_status.sql",
        "title: Add nullable account status column",
    )


def test_renamed_serialized_field_is_breaking_change_risk():
    result = build_task_schema_evolution_plan(
        _plan(
            [
                _task(
                    "task-rename-field",
                    title="Rename serialized customer field",
                    description="Rename customer_name to display_name in the Pydantic model.",
                    files_or_modules=["src/blueprint/models/customer.py"],
                    acceptance_criteria=[
                        "Publish deprecation notice and keep backward compatibility aliases."
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.evolution_risk == "breaking_change_risk"
    assert record.schema_surfaces == ("serialization_model",)
    assert record.change_indicators == (
        "breaking schema or serialized contract change",
        "compatibility, versioning, migration, or deprecation signal",
    )
    assert any("dual-read/write" in value for value in record.guardrails)
    assert any("deprecation notice" in value for value in record.guardrails)
    assert any("round-trip fixtures" in value for value in record.guardrails)


def test_enum_change_is_compatibility_guarded_with_fixture_and_contract_tests():
    result = build_task_schema_evolution_plan(
        [
            _task(
                "task-enum",
                title="Extend invoice status enum",
                description="Add enum value disputed to the domain model and keep compatibility tests.",
                files_or_modules=["src/blueprint/domain/models.py"],
                metadata={"compatibility_plan": "Run versioned contract tests with old fixtures."},
            )
        ]
    )

    record = result.records[0]

    assert record.evolution_risk == "compatibility_guarded"
    assert record.schema_surfaces == ("serialization_model",)
    assert record.change_indicators == (
        "additive schema or contract extension",
        "compatibility, versioning, migration, or deprecation signal",
    )
    assert any("versioned contract tests" in value for value in record.guardrails)
    assert any("serialized fixtures" in value for value in record.guardrails)


def test_event_payload_and_api_spec_changes_receive_surface_specific_guardrails():
    result = analyze_task_schema_evolution(
        _plan(
            [
                _task(
                    "task-event",
                    title="Add event payload field",
                    description="Add optional trace_id to the webhook event payload.",
                    files_or_modules=["schemas/events/payment_event.schema.json"],
                ),
                _task(
                    "task-openapi",
                    title="Change checkout OpenAPI response schema",
                    description="Change type of checkout total in the OpenAPI contract.",
                    files_or_modules=["api/openapi.yaml"],
                ),
                _task(
                    "task-proto",
                    title="Add protobuf message field",
                    description="Add optional field to the invoice grpc proto.",
                    files_or_modules=["proto/invoice.proto"],
                ),
                _task(
                    "task-config",
                    title="Add config schema setting",
                    description="Add optional timeout to the configuration schema.",
                    files_or_modules=["config/service.schema.yaml"],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert by_id["task-event"].schema_surfaces == ("api_schema", "event_payload")
    assert any("dual-read/write event payload" in value for value in by_id["task-event"].guardrails)
    assert any("event fixtures" in value for value in by_id["task-event"].guardrails)
    assert by_id["task-openapi"].evolution_risk == "breaking_change_risk"
    assert by_id["task-openapi"].schema_surfaces == ("api_schema", "openapi")
    assert any(
        "API requests, responses, generated clients" in value
        for value in by_id["task-openapi"].guardrails
    )
    assert by_id["task-proto"].schema_surfaces == ("protobuf",)
    assert any("field numbers" in value for value in by_id["task-proto"].guardrails)
    assert by_id["task-config"].schema_surfaces == ("config_schema",)
    assert any("config defaults" in value for value in by_id["task-config"].guardrails)


def test_non_schema_task_is_not_flagged():
    result = build_task_schema_evolution_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings page copy",
                    description="Adjust labels and helper text.",
                    files_or_modules=["src/blueprint/ui/settings_copy.py"],
                )
            ]
        )
    )

    assert result.schema_task_ids == ()
    assert result.records[0].evolution_risk == "not_schema_related"
    assert result.records[0].schema_surfaces == ()
    assert result.records[0].guardrails == ()


def test_model_inputs_malformed_fields_and_serialization_are_stable():
    task_dict = _task(
        "task-malformed",
        title="Add API schema field | invoices",
        description="Add optional invoice_note to the response schema.",
        files_or_modules={"main": "api/schemas/invoice_response.schema.json", "none": None},
        acceptance_criteria={"contract": "Run versioned contract tests."},
        metadata={"fixtures": [{"path": "tests/fixtures/invoice_response.json"}, None, 7]},
    )
    original = copy.deepcopy(task_dict)
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Remove event payload field",
            description="Remove legacy_id from the billing event payload.",
            files_or_modules=["schemas/events/billing_event.schema.json"],
        )
    )
    raw_result = build_task_schema_evolution_plan(_plan([task_dict]))
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")]))

    first = summarize_task_schema_evolution(
        _plan([task_dict, task_model.model_dump(mode="python")])
    )
    second = build_task_schema_evolution_plan(plan_model)
    payload = task_schema_evolution_plan_to_dict(first)
    markdown = task_schema_evolution_plan_to_markdown(first)

    assert task_dict == original
    assert raw_result.records[0].task_id == "task-malformed"
    assert task_schema_evolution_plan_to_dict(second)["records"][0]["task_id"] == "task-model"
    assert first.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "schema_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "evolution_risk",
        "schema_surfaces",
        "change_indicators",
        "guardrails",
        "evidence",
    ]
    assert markdown.startswith("# Task Schema Evolution Plan: plan-schema-evolution")
    assert payload["records"][1]["title"] == "Add API schema field | invoices"


def _plan(tasks):
    return {
        "id": "plan-schema-evolution",
        "implementation_brief_id": "brief-schema-evolution",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
