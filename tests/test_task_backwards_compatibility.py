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


def test_malformed_task_missing_required_fields():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                {},
                {"title": "Task without ID"},
                {"id": "task-no-files"},
            ]
        )
    )

    assert len(result.recommendations) == 0
    assert result.summary["task_count"] == 3
    assert result.compatible_task_ids == ()


def test_boundary_conditions_empty_and_whitespace_strings():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-empty",
                    title="",
                    description="",
                    files_or_modules=[""],
                    acceptance_criteria=[""],
                ),
                _task(
                    "task-whitespace",
                    title="   ",
                    description="   \n\t  ",
                    files_or_modules=["  ", "\t"],
                    acceptance_criteria=["  \n  "],
                ),
            ]
        )
    )

    assert result.summary["task_count"] == 2
    assert result.recommendations == ()


def test_complex_multi_category_triggers_with_duplicate_evidence():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-complex",
                    title="Comprehensive system migration",
                    description=(
                        "Update API endpoint schemas, CLI command flags, database migrations, "
                        "config settings, import/export formats, and persisted metadata fields. "
                        "Ensure backwards compatible rollback path for existing consumers."
                    ),
                    files_or_modules=[
                        "src/api/endpoints/users.py",
                        "src/api/endpoints/users.py",
                        "src/cli/commands.py",
                        "db/migrations/001_users.sql",
                        "config/defaults.yaml",
                        "src/importers/user_importer.py",
                        "src/exporters/user_exporter.py",
                        "src/schemas/user.avsc",
                        "src/metadata/user_metadata.py",
                    ],
                    acceptance_criteria=[
                        "API contract fixtures validate legacy response formats.",
                        "CLI backwards compatibility tested with deprecated flags.",
                        "Migration rollback procedure documented and tested.",
                        "Legacy import formats still accepted.",
                    ],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.severity == "high"
    assert len(recommendation.trigger_categories) >= 5
    assert "api" in recommendation.trigger_categories
    assert "cli" in recommendation.trigger_categories
    assert "schema" in recommendation.trigger_categories
    assert "database" in recommendation.trigger_categories
    assert "importer" in recommendation.trigger_categories
    assert "exporter" in recommendation.trigger_categories
    assert "config" in recommendation.trigger_categories
    assert len(recommendation.evidence) == len(set(recommendation.evidence))


def test_migration_with_rollback_and_deprecation_warnings():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-migration",
                    title="Database schema migration with deprecation",
                    description=(
                        "Add new columns to user table, deprecate old format fields, "
                        "preserve migration path and rollback steps for existing data."
                    ),
                    files_or_modules=["db/migrations/202605_add_user_columns.sql"],
                    acceptance_criteria=[
                        "Migration path tested with production dataset snapshot.",
                        "Rollback script verified in staging environment.",
                        "Deprecation warnings emitted for legacy field access.",
                    ],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.severity == "high"
    assert "database" in recommendation.trigger_categories
    assert any("migration" in check.lower() for check in recommendation.suggested_checks)
    assert any("deprecation" in evidence.lower() for evidence in recommendation.evidence)


def test_versioned_api_endpoint_changes():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-api-v2",
                    title="Add v2 API endpoint with breaking changes",
                    description=(
                        "Introduce API v2 with renamed response fields while maintaining "
                        "v1 compatibility. Existing consumers should continue using v1."
                    ),
                    files_or_modules=["src/api/v2/orders.py"],
                    acceptance_criteria=[
                        "v1 API contract fixtures still pass.",
                        "v2 breaking changes documented in API docs.",
                    ],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.severity in ("high", "medium")
    assert "api" in recommendation.trigger_categories
    assert any("contract" in check.lower() for check in recommendation.suggested_checks)


def test_config_with_new_defaults_and_env_vars():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-config",
                    title="Update configuration defaults and environment variables",
                    description=(
                        "Change default timeout settings and add new environment variable "
                        "while preserving existing configuration behavior."
                    ),
                    files_or_modules=["config/settings.py", "config/defaults.yaml"],
                    acceptance_criteria=["Existing env vars remain backwards compatible."],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert "config" in recommendation.trigger_categories
    assert recommendation.severity in ("high", "medium")


def test_import_export_format_changes_with_legacy_support():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-formats",
                    title="Update CSV import/export column ordering",
                    description=(
                        "Change CSV export column order and add new import fields "
                        "while supporting legacy file formats."
                    ),
                    files_or_modules=[
                        "src/importers/csv_importer.py",
                        "src/exporters/csv_exporter.py",
                    ],
                    acceptance_criteria=[
                        "Legacy CSV fixtures still import successfully.",
                        "Export format change documented for consumers.",
                    ],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert "importer" in recommendation.trigger_categories
    assert "exporter" in recommendation.trigger_categories
    assert recommendation.severity == "high"


def test_persisted_metadata_field_changes():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Rename persisted metadata fields",
                    description=(
                        "Update stored metadata field names and serialization format "
                        "for user preferences and session data."
                    ),
                    files_or_modules=["src/metadata/preferences.py"],
                    acceptance_criteria=[
                        "Migration handles old metadata field names.",
                        "Session data deserialization backwards compatible.",
                    ],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert "persisted_metadata" in recommendation.trigger_categories
    assert recommendation.severity == "high"


def test_mixed_task_types_dict_and_model():
    dict_task = _task(
        "task-dict",
        title="API change via dict",
        files_or_modules=["src/api/users.py"],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="CLI change via model",
            files_or_modules=["src/cli/commands.py"],
        )
    )

    result = recommend_task_backwards_compatibility([dict_task, model_task])

    assert isinstance(result, TaskBackwardsCompatibilityPlan)
    assert result.plan_id is None
    assert len(result.recommendations) == 2
    task_ids = [rec.task_id for rec in result.recommendations]
    assert "task-dict" in task_ids
    assert "task-model" in task_ids


def test_special_characters_in_task_fields():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-special",
                    title="Update API with <special> & \"quoted\" characters",
                    description=(
                        "Change API endpoint with unicode: \u00e9\u00f1\u00fc and symbols: #$%^&*"
                    ),
                    files_or_modules=["src/api/\u00e9ndpoint.py"],
                    acceptance_criteria=["API contract | fixture | passes"],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.task_id == "task-special"
    markdown = task_backwards_compatibility_plan_to_markdown(result)
    assert "task-special" in markdown


def test_very_long_task_descriptions_and_file_lists():
    long_description = " ".join(
        [
            "Update API endpoint schema database migration CLI flags config settings",
            "importer exporter persisted metadata backwards compatible rollback",
        ]
        * 20
    )
    many_files = [f"src/module_{i}.py" for i in range(100)]

    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-long",
                    title="Complex task with extensive details",
                    description=long_description,
                    files_or_modules=many_files,
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert len(recommendation.evidence) > 0
    assert len(recommendation.suggested_checks) > 0


def test_task_with_metadata_compatibility_signals():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-meta",
                    title="Task with metadata signals",
                    description="Update feature with metadata hints.",
                    files_or_modules=["src/feature.py"],
                    metadata={
                        "compatibility": {
                            "breaking_change": True,
                            "migration_required": True,
                        },
                        "notes": "API endpoint renamed, CLI flags deprecated.",
                    },
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.task_id == "task-meta"
    assert len(recommendation.evidence) > 0


def test_sorting_by_severity_then_task_id():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-z-high",
                    title="High severity last ID",
                    description="Breaking API change for existing consumers.",
                    files_or_modules=["src/api/critical.py"],
                ),
                _task(
                    "task-a-high",
                    title="High severity first ID",
                    description="Database migration with schema change.",
                    files_or_modules=["db/migrations/001.sql"],
                ),
                _task(
                    "task-m-medium",
                    title="Medium severity middle ID",
                    description="Add config setting with default.",
                    files_or_modules=["config/settings.yaml"],
                ),
            ]
        )
    )

    task_ids = [rec.task_id for rec in result.recommendations]
    severities = [rec.severity for rec in result.recommendations]

    high_tasks = [task_id for task_id, sev in zip(task_ids, severities) if sev == "high"]
    assert high_tasks == sorted(high_tasks)

    for i in range(len(severities) - 1):
        if severities[i] == "high" and severities[i + 1] == "medium":
            assert True
        elif severities[i] == severities[i + 1]:
            assert task_ids[i] <= task_ids[i + 1]


def test_serialization_round_trip_preserves_structure():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-serialize",
                    title="Serialization test",
                    description="API endpoint change for backwards compatibility.",
                    files_or_modules=["src/api/test.py"],
                    acceptance_criteria=["Contract fixtures pass."],
                )
            ]
        )
    )

    payload = task_backwards_compatibility_plan_to_dict(result)
    json_payload = json.loads(json.dumps(payload))

    assert payload == json_payload
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["recommendations"]


def test_edge_case_file_path_patterns():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="File path edge cases",
                    description="Test various path formats.",
                    files_or_modules=[
                        "/absolute/path/to/api/endpoint.py",
                        "./relative/path/cli.py",
                        "../parent/dir/schema.avsc",
                        "no/extension/file",
                        "multiple.dots.in.name.py",
                        "path\\with\\backslashes.py",
                    ],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert len(recommendation.trigger_categories) > 0


def test_acceptance_criteria_with_compatibility_keywords():
    result = build_task_backwards_compatibility_plan(
        _plan(
            [
                _task(
                    "task-ac",
                    title="Task with compatibility in AC",
                    description="Simple feature update.",
                    files_or_modules=["src/feature.py"],
                    acceptance_criteria=[
                        "Backwards compatible with existing clients.",
                        "Migration path validated.",
                        "Deprecation warnings added for old format.",
                        "Contract fixture proves non-breaking change.",
                    ],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert len(recommendation.evidence) > 0
    compat_evidence = [e for e in recommendation.evidence if "acceptance_criteria" in e]
    assert len(compat_evidence) > 0


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
