import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_schema_migration_readiness import (
    TaskSchemaMigrationReadinessPlan,
    TaskSchemaMigrationReadinessRecord,
    analyze_task_schema_migration_readiness,
    build_task_schema_migration_readiness_plan,
    extract_task_schema_migration_readiness,
    generate_task_schema_migration_readiness,
    summarize_task_schema_migration_readiness,
    task_schema_migration_readiness_plan_to_dict,
    task_schema_migration_readiness_plan_to_dicts,
    task_schema_migration_readiness_plan_to_markdown,
)


def test_migration_paths_and_task_text_detect_schema_signals_and_missing_safeguards():
    result = build_task_schema_migration_readiness_plan(
        _plan(
            [
                _task(
                    "task-customers",
                    title="Rename customer status column",
                    description=(
                        "Run a schema migration to rename column status to lifecycle_status, "
                        "backfill existing records, add a foreign key, and create index."
                    ),
                    files_or_modules=["db/migrations/20260503_rename_customer_status.sql"],
                    acceptance_criteria=["Migration applies in staging."],
                    risks=["Large production table can lock writes during the migration."],
                ),
                _task(
                    "task-ui",
                    title="Update settings copy",
                    description="Adjust UI labels only.",
                    files_or_modules=["src/ui/settings.tsx"],
                ),
            ]
        )
    )

    assert isinstance(result, TaskSchemaMigrationReadinessPlan)
    assert result.migration_task_ids == ("task-customers",)
    assert result.no_signal_task_ids == ("task-ui",)
    record = result.records[0]
    assert isinstance(record, TaskSchemaMigrationReadinessRecord)
    assert record.detected_signals == (
        "migration",
        "schema_change",
        "column_rename",
        "backfill",
        "index_creation",
        "foreign_key",
    )
    assert record.present_safeguards == ("migration_test", "production_volume_check")
    assert record.missing_safeguards == (
        "backwards_compatible_rollout",
        "expand_contract_steps",
        "backfill_plan",
        "lock_timeout",
        "rollback_strategy",
        "monitoring",
    )
    assert record.readiness_level == "partial"
    assert "files_or_modules: db/migrations/20260503_rename_customer_status.sql" in record.evidence
    assert result.summary["task_count"] == 2
    assert result.summary["migration_task_count"] == 1
    assert result.summary["migration_task_ids"] == ["task-customers"]
    assert result.summary["no_signal_task_ids"] == ["task-ui"]
    assert result.summary["signal_counts"]["column_rename"] == 1
    assert result.summary["missing_safeguard_counts"]["rollback_strategy"] == 1
    assert result.summary["status"] == "missing_schema_migration_safeguards"


def test_ready_plan_requires_rollout_backfill_rollback_validation_and_monitoring():
    result = analyze_task_schema_migration_readiness(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Create tenant audit table | ready",
                    description=(
                        "Create table tenant_audit and add index using an additive, "
                        "backward-compatible rollout."
                    ),
                    files_or_modules=["alembic/versions/20260503_create_tenant_audit.py"],
                    acceptance_criteria=[
                        "Expand-contract steps are documented before the contract phase.",
                        "Backfill plan uses chunked backfill with throttling and resumability.",
                        "Lock timeout and statement timeout are configured; index creation is concurrent.",
                        "Rollback strategy includes a down migration and revert plan.",
                        "Migration test dry-run applies in staging and validates rollback.",
                        "Production volume check records table size and row count.",
                        "Monitoring dashboard and alerts watch slow query and migration errors.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.present_safeguards == (
        "backwards_compatible_rollout",
        "expand_contract_steps",
        "backfill_plan",
        "lock_timeout",
        "rollback_strategy",
        "migration_test",
        "production_volume_check",
        "monitoring",
    )
    assert record.missing_safeguards == ()
    assert record.readiness_level == "ready"
    assert result.summary["readiness_counts"] == {"missing_safeguards": 0, "partial": 0, "ready": 1}
    assert result.summary["missing_safeguard_count"] == 0
    assert result.summary["status"] == "ready"


def test_descriptions_acceptance_criteria_risks_metadata_and_file_paths_are_scanned():
    result = build_task_schema_migration_readiness_plan(
        _plan(
            [
                _task(
                    "task-risk",
                    title="Tighten account constraints",
                    description="Alter table account and add not null constraint.",
                    files_or_modules=["prisma/migrations/20260503_account_constraints/migration.sql"],
                    risks=[
                        "Risk: production volume is millions of rows and needs a lock timeout.",
                        "Rollback plan must preserve existing writes.",
                    ],
                    acceptance_criteria=[
                        "Migration test covers migrate up and down.",
                        "Monitoring watches deadlock and replication lag metrics.",
                    ],
                    metadata={
                        "rollout": "Use a backwards compatible nullable first rollout.",
                        "expand_contract_steps": "expand, validate, then contract",
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == ("migration", "schema_change", "constraint")
    assert record.present_safeguards == (
        "backwards_compatible_rollout",
        "expand_contract_steps",
        "lock_timeout",
        "rollback_strategy",
        "migration_test",
        "production_volume_check",
        "monitoring",
    )
    assert record.missing_safeguards == ()
    assert record.readiness_level == "ready"
    assert any("risks[0]" in item for item in record.evidence)
    assert any("metadata.expand_contract_steps" in item for item in record.evidence)


def test_no_signal_and_malformed_inputs_are_stable_and_json_compatible():
    result = build_task_schema_migration_readiness_plan(
        _plan(
            [
                _task(
                    "task-api",
                    title="Tune cache TTL",
                    description="Adjust API cache behavior with unit tests.",
                    files_or_modules=["src/services/cache.py"],
                )
            ],
            plan_id="plan-no-schema",
        )
    )
    malformed = build_task_schema_migration_readiness_plan(42)

    assert result.records == ()
    assert result.migration_task_ids == ()
    assert result.no_signal_task_ids == ("task-api",)
    assert result.to_dicts() == []
    assert json.loads(json.dumps(result.to_dict())) == result.to_dict()
    assert result.summary == {
        "task_count": 1,
        "migration_task_count": 0,
        "migration_task_ids": [],
        "no_signal_task_count": 1,
        "no_signal_task_ids": ["task-api"],
        "missing_safeguard_count": 0,
        "readiness_counts": {"missing_safeguards": 0, "partial": 0, "ready": 0},
        "signal_counts": {
            "migration": 0,
            "schema_change": 0,
            "table_creation": 0,
            "column_rename": 0,
            "backfill": 0,
            "index_creation": 0,
            "foreign_key": 0,
            "constraint": 0,
            "data_migration": 0,
        },
        "safeguard_counts": {
            "backwards_compatible_rollout": 0,
            "expand_contract_steps": 0,
            "backfill_plan": 0,
            "lock_timeout": 0,
            "rollback_strategy": 0,
            "migration_test": 0,
            "production_volume_check": 0,
            "monitoring": 0,
        },
        "missing_safeguard_counts": {
            "backwards_compatible_rollout": 0,
            "expand_contract_steps": 0,
            "backfill_plan": 0,
            "lock_timeout": 0,
            "rollback_strategy": 0,
            "migration_test": 0,
            "production_volume_check": 0,
            "monitoring": 0,
        },
        "status": "no_schema_migration_signals",
    }
    assert "No schema migration readiness records were inferred." in result.to_markdown()
    assert "No-signal tasks: task-api" in result.to_markdown()
    assert malformed.records == ()
    assert malformed.summary["task_count"] == 0


def test_deterministic_serialization_markdown_aliases_models_objects_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Data migration for billing tiers",
                description="Data migration transforms existing records.",
                files_or_modules=["db/data_migrations/20260503_billing_tiers.sql"],
                acceptance_criteria=["Backfill plan is idempotent and migration test passes."],
            ),
            _task(
                "task-a",
                title="Create product table",
                description="Create table product and add index.",
                acceptance_criteria=["Rollback strategy is documented."],
            ),
            _task("task-copy", title="Update copy", description="Change UI copy."),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)
    object_task = SimpleNamespace(
        id="task-object",
        title="Add foreign key",
        description="Add foreign key constraint with lock timeout.",
        acceptance_criteria=["Migration test runs."],
        files_or_modules=["db/migrations/20260503_fk.sql"],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Backfill account status",
            description="Backfill existing records after schema migration.",
            acceptance_criteria=["Rollback plan and monitoring are ready."],
        )
    )

    result = summarize_task_schema_migration_readiness(plan)
    generated = generate_task_schema_migration_readiness(model)
    extracted = extract_task_schema_migration_readiness([object_task, task_model])
    payload = task_schema_migration_readiness_plan_to_dict(result)
    markdown = task_schema_migration_readiness_plan_to_markdown(result)

    assert plan == original
    assert generated.to_dict() == result.to_dict()
    assert summarize_task_schema_migration_readiness(result) is result
    assert json.loads(json.dumps(payload)) == payload
    assert task_schema_migration_readiness_plan_to_dicts(result) == payload["records"]
    assert task_schema_migration_readiness_plan_to_dicts(result.records) == payload["records"]
    assert list(payload) == ["plan_id", "records", "migration_task_ids", "no_signal_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness_level",
        "evidence",
        "recommended_readiness_steps",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Schema Migration Readiness: plan-schema-migration")
    assert "| Task | Title | Readiness | Signals | Present Safeguards | Missing Safeguards | Evidence |" in markdown
    assert "Create product table" in markdown
    assert "No-signal tasks: task-copy" in markdown
    assert extracted.migration_task_ids == ("task-model", "task-object")
    assert extracted.records[0].task_id == "task-model"
    assert extracted.records[1].task_id == "task-object"


def _plan(tasks, *, plan_id="plan-schema-migration"):
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
    risks=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if risks is not None:
        task["risks"] = risks
    if metadata is not None:
        task["metadata"] = metadata
    return task
