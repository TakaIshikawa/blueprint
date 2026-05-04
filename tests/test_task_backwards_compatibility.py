import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_backwards_compatibility import (
    TaskBackwardsCompatibilityPlan,
    build_task_backwards_compatibility_plan,
    recommend_task_backwards_compatibility,
    task_backwards_compatibility_plan_to_dict,
    task_backwards_compatibility_plan_to_markdown,
)


def test_mapping_input_detects_api_cli_config_and_dedupes_checks():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-public",
                    title="Change public API and CLI config defaults",
                    description=(
                        "Rename GET /api/orders response field and update CLI flag "
                        "defaults for existing consumers."
                    ),
                    files_or_modules=[
                        "src/api/orders.py",
                        "src/api/orders.py",
                        "src/cli.py",
                        "config/settings.yaml",
                    ],
                    acceptance_criteria=[
                        "Contract fixture covers legacy API response fields.",
                    ],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.task_id == "task-public"
    assert recommendation.trigger_categories == ("api", "cli", "schema", "config")
    assert recommendation.severity == "high"
    assert recommendation.suggested_checks.count(
        "Contract fixture proves existing request and response consumers still work."
    ) == 1
    assert any("Deprecation note" in check for check in recommendation.suggested_checks)
    assert len(recommendation.evidence) == len(set(recommendation.evidence))
    assert result.summary == {
        "task_count": 1,
        "recommendation_count": 1,
        "high_severity_count": 1,
        "medium_severity_count": 0,
        "low_severity_count": 0,
        "trigger_category_count": 4,
    }


def test_execution_plan_input_sorts_by_severity_then_task_id_and_serializes():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Add config setting",
                description="Add an environment variable with the existing default preserved.",
                files_or_modules=["src/settings.py"],
                acceptance_criteria=["Existing env var behavior remains backwards compatible."],
            ),
            _task(
                "task-a",
                title="Add user profile migration",
                description="Database migration adds nullable profile columns.",
                files_or_modules=["migrations/20260501_profiles.sql"],
                acceptance_criteria=["Migration path and rollback condition are documented."],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_backwards_compatibility_plan(ExecutionPlan.model_validate(plan))
    payload = task_backwards_compatibility_plan_to_dict(result)

    assert plan == original
    assert result.compatible_task_ids == ("task-a", "task-z")
    assert [item.severity for item in result.recommendations] == ["high", "medium"]
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["recommendations"]
    assert list(payload) == ["plan_id", "recommendations", "compatible_task_ids", "summary"]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "trigger_categories",
        "severity",
        "rationale",
        "suggested_checks",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_detects_database_importer_exporter_schema_and_persisted_metadata_categories():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-data",
                    title="Update importer exporter schema for persisted metadata",
                    description=(
                        "Change CSV import and report export formats, alter database model, "
                        "and migrate stored metadata fields."
                    ),
                    files_or_modules=[
                        "src/importers/customer_importer.py",
                        "src/exporters/customer_exporter.py",
                        "src/schemas/customer.avsc",
                        "src/db/models/customer.py",
                        "src/metadata/customer_metadata.py",
                    ],
                    acceptance_criteria=["Legacy import and export fixtures still pass."],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.trigger_categories == (
        "schema",
        "database",
        "importer",
        "exporter",
        "persisted_metadata",
    )
    assert recommendation.severity == "high"
    assert "Importer fixture covers legacy input files or payloads." in (
        recommendation.suggested_checks
    )
    assert "Exporter contract fixture covers legacy output columns, fields, and ordering." in (
        recommendation.suggested_checks
    )
    assert any("persisted metadata" in check for check in recommendation.suggested_checks)


def test_execution_task_input_and_alias_work_for_single_task():
    task = ExecutionTask.model_validate(
        _task(
            "task-cli",
            title="Rename CLI subcommand",
            description="Rename the command line subcommand and keep old flags working.",
            files_or_modules=["src/blueprint/commands/cleanup.py"],
            acceptance_criteria=["CLI compatibility fixture covers old command aliases."],
        )
    )

    result = recommend_task_backwards_compatibility(task)

    assert isinstance(result, TaskBackwardsCompatibilityPlan)
    assert result.plan_id is None
    assert result.compatible_task_ids == ("task-cli",)
    assert result.recommendations[0].trigger_categories == ("cli",)
    assert result.recommendations[0].severity == "high"


def test_low_risk_tasks_are_ignored_without_noisy_recommendations():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update onboarding copy",
                    description="Clarify empty state text in the admin dashboard.",
                    files_or_modules=["src/ui/onboarding.py"],
                    acceptance_criteria=["Copy appears in the empty state."],
                )
            ]
        )
    )

    assert result.recommendations == ()
    assert result.compatible_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "recommendation_count": 0,
        "high_severity_count": 0,
        "medium_severity_count": 0,
        "low_severity_count": 0,
        "trigger_category_count": 0,
    }


def test_markdown_output_summarizes_task_severity_rationale_and_checks():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-api",
                    title="Add REST endpoint",
                    description="Add API response fields for mobile clients.",
                    files_or_modules=["src/api/mobile.py"],
                    acceptance_criteria=["Response shape includes the new field."],
                )
            ]
        )
    )

    markdown = task_backwards_compatibility_plan_to_markdown(result)

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Backwards Compatibility Plan: plan-compat")
    assert "| `task-api` | medium | api, schema |" in markdown
    assert "existing consumers" in markdown
    assert "Contract fixture proves existing request and response consumers still work." in markdown


def test_invalid_and_empty_plan_handling():
    empty = build_task_backwards_compatibility_plan({"id": "plan-empty", "tasks": []})
    invalid = build_task_backwards_compatibility_plan({"id": "plan-invalid", "tasks": "nope"})
    none_source = build_task_backwards_compatibility_plan(None)

    assert empty.to_markdown() == "\n".join(
        [
            "# Task Backwards Compatibility Plan: plan-empty",
            "",
            "No backwards compatibility recommendations were derived.",
        ]
    )
    assert invalid.summary["task_count"] == 0
    assert invalid.recommendations == ()
    assert none_source.summary["task_count"] == 0
    assert none_source.recommendations == ()


def _plan(tasks):
    return {
        "id": "plan-compat",
        "implementation_brief_id": "brief-compat",
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
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
