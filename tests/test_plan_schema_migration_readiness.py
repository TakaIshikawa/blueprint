import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_schema_migration_readiness import (
    PlanSchemaMigrationReadinessMatrix,
    PlanSchemaMigrationReadinessRecord,
    build_plan_schema_migration_readiness_matrix,
    plan_schema_migration_readiness_matrix_to_dict,
    plan_schema_migration_readiness_matrix_to_markdown,
    summarize_plan_schema_migration_readiness,
)


def test_detects_ddl_index_and_backfill_surfaces_with_missing_safeguards():
    result = build_plan_schema_migration_readiness_matrix(
        _plan(
            [
                _task(
                    "task-orders-ddl",
                    title="Add orders table index and status backfill",
                    description=(
                        "Run an Alembic schema migration to add a column, create index, "
                        "and backfill existing production table rows."
                    ),
                    files_or_modules=[
                        "migrations/versions/20260502_add_orders_status_index.sql",
                        "src/jobs/backfills/order_status_backfill.py",
                    ],
                    acceptance_criteria=[
                        "Rollback path documents the down migration.",
                        "Backfill validation reconciles row counts after chunked batches.",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, PlanSchemaMigrationReadinessMatrix)
    assert result.plan_id == "plan-schema-migration"
    assert result.schema_migration_task_ids == ("task-orders-ddl",)
    record = result.records[0]
    assert isinstance(record, PlanSchemaMigrationReadinessRecord)
    assert record.migration_surfaces == ("migration", "ddl", "index", "backfill")
    assert record.required_safeguards == (
        "reversible_migration_or_rollback_path",
        "expand_contract_compatibility",
        "backfill_validation",
        "lock_downtime_assessment",
        "deployment_ordering",
        "data_verification",
    )
    assert record.missing_acceptance_criteria == (
        "expand_contract_compatibility",
        "lock_downtime_assessment",
        "deployment_ordering",
        "data_verification",
    )
    assert record.risk_level == "high"
    assert "files_or_modules: migrations/versions/20260502_add_orders_status_index.sql" in record.evidence
    assert "files_or_modules: src/jobs/backfills/order_status_backfill.py" in record.evidence
    assert "title: Add orders table index and status backfill" in record.evidence


def test_tags_metadata_generated_schema_and_orm_detection_with_criteria_coverage():
    result = summarize_plan_schema_migration_readiness(
        _plan(
            [
                _task(
                    "task-model",
                    title="Regenerate customer schema artifacts",
                    description="Update ORM model change after migration.",
                    files_or_modules=["src/generated/customer_schema.generated.ts", "src/models/customer.py"],
                    tags=["schema migration"],
                    metadata={
                        "schema_surface": "generated schema",
                        "safeguards": [
                            "Rollback uses reversible migration steps.",
                            "Expand/contract compatibility keeps old and new readers working.",
                            "Backfill validation uses reconciliation checks.",
                            "Lock downtime assessment confirms an online migration.",
                            "Deployment ordering runs expand before app deploy and contract after verification.",
                            "Data verification compares checksums after rollout.",
                        ],
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.migration_surfaces == ("migration", "generated_schema", "orm_model")
    assert record.missing_acceptance_criteria == ()
    assert record.risk_level == "low"
    assert any(item.startswith("metadata.schema_surface:") for item in record.evidence)
    assert "tags[0]: schema migration" in record.evidence
    assert any("Regenerate clients or ORM artifacts" in note for note in record.rollout_notes)


def test_deterministic_ordering_dict_serialization_and_non_mutating_behavior():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Add nullable account column",
                description="Add column with a defaulted additive schema migration.",
                files_or_modules=["db/migrations/20260502_add_account_column.sql"],
                acceptance_criteria=[
                    "Rollback path uses a down migration.",
                    "Expand/contract compatibility keeps both versions working.",
                    "Backfill validation confirms no rows need update.",
                    "Lock downtime assessment confirms no blocking table rewrite.",
                    "Deployment ordering runs migration before application deploy.",
                    "Data verification checks row count smoke tests.",
                ],
            ),
            _task(
                "task-a",
                title="Drop legacy column",
                description="Drop column from a large production table.",
                files_or_modules=["migrations/versions/20260502_drop_legacy_column.sql"],
                acceptance_criteria=["Rollback path exists."],
            ),
            _task(
                "task-m",
                title="Create audit table",
                description="Create table through DDL migration.",
                files_or_modules=["schema/audit_tables.sql"],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_schema_migration_readiness_matrix(plan)
    payload = plan_schema_migration_readiness_matrix_to_dict(result)

    assert plan == original
    assert result.schema_migration_task_ids == ("task-a", "task-m", "task-z")
    assert [record.risk_level for record in result.records] == ["high", "high", "low"]
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "schema_migration_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "migration_surfaces",
        "required_safeguards",
        "missing_acceptance_criteria",
        "rollout_notes",
        "risk_level",
        "evidence",
    ]
    assert payload["summary"]["task_count"] == 3
    assert payload["summary"]["schema_migration_task_count"] == 3
    assert payload["summary"]["risk_counts"] == {"high": 2, "medium": 0, "low": 1}
    assert payload["summary"]["surface_counts"]["ddl"] == 3


def test_execution_plan_model_input_and_markdown_rendering_are_supported():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-pipe",
                    title="Customer | account migration",
                    description="Create table for account history.",
                    files_or_modules=["db/migrations/20260502_create_account_history.sql"],
                    acceptance_criteria=[
                        "Rollback path is reversible.",
                        "Expand contract compatibility keeps reads safe.",
                        "Backfill validation is not required because the table starts empty.",
                        "Lock downtime assessment uses online DDL.",
                        "Deployment ordering runs migration before app writes.",
                        "Data verification checks row counts.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )

    result = build_plan_schema_migration_readiness_matrix(plan)
    markdown = plan_schema_migration_readiness_matrix_to_markdown(result)

    assert result.plan_id == "plan-model"
    assert result.records[0].risk_level == "low"
    assert result.records[0].missing_acceptance_criteria == ()
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Plan Schema Migration Readiness Matrix: plan-model")
    assert "Customer \\| account migration" in markdown
    assert "| `task-pipe` |" in markdown
    assert "No schema migration tasks were detected." not in markdown


def test_non_schema_tasks_are_suppressed_and_empty_markdown_is_stable():
    result = build_plan_schema_migration_readiness_matrix(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Clarify onboarding labels.",
                    files_or_modules=["src/ui/settings_copy.py"],
                    tags=["frontend"],
                    metadata={"surface": "settings page"},
                )
            ]
        )
    )

    assert result.records == ()
    assert result.schema_migration_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "schema_migration_task_count": 0,
        "missing_acceptance_criteria_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "surface_counts": {
            "migration": 0,
            "ddl": 0,
            "index": 0,
            "backfill": 0,
            "generated_schema": 0,
            "orm_model": 0,
        },
    }
    assert result.to_markdown() == (
        "# Plan Schema Migration Readiness Matrix: plan-schema-migration\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Schema migration task count: 0\n"
        "- Missing acceptance criteria count: 0\n"
        "- Risk counts: high 0, medium 0, low 0\n"
        "- Surface counts: migration 0, ddl 0, index 0, backfill 0, generated_schema 0, orm_model 0\n"
        "\n"
        "No schema migration tasks were detected."
    )


def _plan(tasks, plan_id="plan-schema-migration"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-schema-migration",
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
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "depends_on": [],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
