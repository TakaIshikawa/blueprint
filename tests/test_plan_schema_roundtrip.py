import json
from typing import Any

import pytest

from blueprint.domain import ExecutionPlan, get_model_json_schema


def test_minimal_plan_passes_schema_validation_and_round_trips_with_defaults():
    payload = {
        "id": "plan-minimal",
        "implementation_brief_id": "brief-minimal",
        "milestones": [],
    }

    schema = get_model_json_schema("execution-plan")
    _assert_matches_schema(payload, schema)

    plan = ExecutionPlan.model_validate(payload)
    assert plan.tasks == []
    assert plan.metadata == {}
    assert plan.status == "draft"
    assert plan.target_engine is None
    assert plan.generation_tokens is None

    _assert_round_trip(plan, schema)


@pytest.mark.parametrize(
    "payload",
    [
        {
            "id": "plan-dependencies",
            "implementation_brief_id": "brief-dependencies",
            "target_engine": "codex",
            "target_repo": "example/repo",
            "project_type": "cli",
            "milestones": [
                {"name": "Schema coverage", "description": "Protect plan shape"},
                {"name": "Exporter coverage", "description": "Cover downstream consumers"},
            ],
            "test_strategy": "Run focused schema round-trip tests",
            "handoff_prompt": "Add coverage without changing model behavior",
            "status": "ready",
            "metadata": {"priority": "medium", "source": "manual"},
            "tasks": [
                {
                    "id": "task-schema",
                    "execution_plan_id": "plan-dependencies",
                    "title": "Validate exported schema",
                    "description": "Check representative plans against the public JSON schema",
                    "milestone": "Schema coverage",
                    "owner_type": "agent",
                    "suggested_engine": "codex",
                    "depends_on": [],
                    "files_or_modules": ["src/blueprint/domain/schema.py"],
                    "acceptance_criteria": [
                        "Valid execution plans pass exported schema validation",
                        "Round-trip serialization preserves task defaults",
                    ],
                    "estimated_complexity": "low",
                    "estimated_hours": 1.5,
                    "risk_level": "low",
                    "test_command": "poetry run pytest tests/test_plan_schema_roundtrip.py -o addopts=''",
                    "status": "pending",
                    "metadata": {"area": "schema"},
                },
                {
                    "id": "task-roundtrip",
                    "execution_plan_id": "plan-dependencies",
                    "title": "Round-trip plan payload",
                    "description": "Serialize and reload the domain model",
                    "milestone": "Schema coverage",
                    "owner_type": "developer",
                    "suggested_engine": "codex",
                    "depends_on": ["task-schema"],
                    "files_or_modules": [
                        "src/blueprint/domain/models.py",
                        "tests/test_plan_schema_roundtrip.py",
                    ],
                    "acceptance_criteria": [
                        "Dependencies survive JSON serialization",
                        "File impact hints survive JSON serialization",
                    ],
                    "estimated_complexity": "medium",
                    "risk_level": "medium",
                    "status": "in_progress",
                },
            ],
        },
        {
            "id": "plan-omitted-optionals",
            "implementation_brief_id": "brief-omitted-optionals",
            "milestones": [{"name": "Defaults"}],
            "tasks": [
                {
                    "id": "task-omitted-optionals",
                    "title": "Use task defaults",
                    "description": "Omit optional task fields and rely on model defaults",
                    "acceptance_criteria": ["The payload validates without optional fields"],
                }
            ],
        },
    ],
)
def test_representative_plans_pass_schema_validation_and_domain_round_trip(payload):
    schema = get_model_json_schema("execution-plan")
    _assert_matches_schema(payload, schema)

    plan = ExecutionPlan.model_validate(payload)

    _assert_round_trip(plan, schema)


def test_omitted_optional_task_fields_use_documented_defaults():
    payload = {
        "id": "plan-task-defaults",
        "implementation_brief_id": "brief-task-defaults",
        "milestones": [{"name": "Defaults"}],
        "tasks": [
            {
                "id": "task-defaults",
                "title": "Check defaults",
                "description": "Document optional field behavior",
                "acceptance_criteria": ["Defaults are populated by the domain model"],
            }
        ],
    }

    schema = get_model_json_schema("execution-plan")
    _assert_matches_schema(payload, schema)
    plan = ExecutionPlan.model_validate(payload)
    task = plan.tasks[0]

    assert task.execution_plan_id is None
    assert task.depends_on == []
    assert task.files_or_modules is None
    assert task.metadata == {}
    assert task.owner_type is None
    assert task.risk_level is None
    assert task.status == "pending"

    _assert_round_trip(plan, schema)


def _assert_round_trip(plan: ExecutionPlan, schema: dict[str, Any]) -> None:
    dumped = json.loads(plan.model_dump_json())
    _assert_matches_schema(dumped, schema)

    round_tripped = ExecutionPlan.model_validate_json(plan.model_dump_json())
    assert round_tripped.model_dump(mode="json") == plan.model_dump(mode="json")


def _assert_matches_schema(instance: Any, schema: dict[str, Any]) -> None:
    _validate_schema_node(instance, schema, schema, "$")


def _validate_schema_node(
    instance: Any,
    schema_node: dict[str, Any],
    root_schema: dict[str, Any],
    path: str,
) -> None:
    if "$ref" in schema_node:
        schema_node = _resolve_ref(schema_node["$ref"], root_schema)

    if "anyOf" in schema_node:
        errors = []
        for candidate in schema_node["anyOf"]:
            try:
                _validate_schema_node(instance, candidate, root_schema, path)
                return
            except AssertionError as exc:
                errors.append(str(exc))
        raise AssertionError(f"{path} did not match any schema option: {errors}")

    expected_type = schema_node.get("type")
    if expected_type is not None:
        _assert_json_type(instance, expected_type, path)

    if "enum" in schema_node:
        assert instance in schema_node["enum"], f"{path} expected one of {schema_node['enum']!r}"

    if "minLength" in schema_node:
        assert len(instance) >= schema_node["minLength"], f"{path} is shorter than minLength"

    if "minimum" in schema_node:
        assert instance >= schema_node["minimum"], f"{path} is less than minimum"

    if expected_type == "object":
        required = set(schema_node.get("required", []))
        missing = required.difference(instance)
        assert not missing, f"{path} missing required fields: {sorted(missing)!r}"

        properties = schema_node.get("properties", {})
        if schema_node.get("additionalProperties") is False:
            extra = set(instance).difference(properties)
            assert not extra, f"{path} contains unexpected fields: {sorted(extra)!r}"

        for key, value in instance.items():
            if key in properties:
                _validate_schema_node(value, properties[key], root_schema, f"{path}.{key}")

    if expected_type == "array":
        item_schema = schema_node.get("items")
        if item_schema is not None:
            for index, item in enumerate(instance):
                _validate_schema_node(item, item_schema, root_schema, f"{path}[{index}]")


def _resolve_ref(ref: str, root_schema: dict[str, Any]) -> dict[str, Any]:
    assert ref.startswith("#/$defs/"), f"Unsupported schema ref: {ref}"
    return root_schema["$defs"][ref.removeprefix("#/$defs/")]


def _assert_json_type(instance: Any, expected_type: str, path: str) -> None:
    type_map = {
        "array": list,
        "integer": int,
        "null": type(None),
        "number": (int, float),
        "object": dict,
        "string": str,
    }
    assert expected_type in type_map, f"{path} has unsupported schema type: {expected_type}"
    assert isinstance(instance, type_map[expected_type]), f"{path} expected {expected_type}"
    if expected_type in {"integer", "number"}:
        assert not isinstance(instance, bool), f"{path} expected {expected_type}, not boolean"
