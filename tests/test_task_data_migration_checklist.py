import json

from blueprint.domain.models import ExecutionTask
from blueprint.task_data_migration_checklist import (
    DataMigrationRiskNote,
    TaskDataMigrationChecklist,
    build_task_data_migration_checklist,
    task_data_migration_checklist_to_dict,
)


def test_sqlalchemy_model_changes_get_migration_checklist():
    result = build_task_data_migration_checklist(
        _plan(
            [
                _task(
                    "task-model",
                    title="Add SQLAlchemy UserPreference model",
                    description="Add a persisted ORM model with a nullable account_id column.",
                    files_or_modules=["src/blueprint/store/models.py"],
                    acceptance_criteria=[
                        "Model tests verify the relationship and persisted field defaults."
                    ],
                    test_command="poetry run pytest tests/test_store_models.py",
                )
            ]
        )
    )

    checklist = result.checklists[0]

    assert isinstance(checklist, TaskDataMigrationChecklist)
    assert checklist.task_id == "task-model"
    assert checklist.risk_level == "medium"
    assert checklist.suggested_validation_commands == (
        "poetry run pytest tests/test_store_models.py",
    )
    assert _risk_note(checklist, "model") == DataMigrationRiskNote(
        category="model",
        message="Task appears to change persistence models or ORM mappings.",
        evidence=(
            "files_or_modules: src/blueprint/store/models.py",
            "title: Add SQLAlchemy UserPreference model",
            "description: Add a persisted ORM model with a nullable account_id column.",
            "acceptance_criteria[0]: Model tests verify the relationship and persisted field defaults.",
        ),
    )
    assert "Update model-level tests for the persisted field or relationship changes." in (
        checklist.required_steps
    )


def test_alembic_migration_files_include_schema_and_migration_risks():
    result = build_task_data_migration_checklist(
        _plan(
            [
                _task(
                    "task-alembic",
                    title="Create Alembic revision for audit events",
                    description="Use op.create_table and op.add_column in the migration.",
                    files_or_modules=["migrations/versions/20260501_add_audit_events.py"],
                    acceptance_criteria=[
                        "Run alembic upgrade head before the integration tests.",
                        "Run alembic downgrade -1 to validate rollback.",
                    ],
                )
            ]
        )
    )

    checklist = result.checklists[0]

    assert [note.category for note in checklist.risk_notes] == [
        "migration",
        "schema",
    ]
    assert _risk_note(checklist, "migration").evidence == (
        "files_or_modules: migrations/versions/20260501_add_audit_events.py",
        "title: Create Alembic revision for audit events",
        "description: Use op.create_table and op.add_column in the migration.",
        "acceptance_criteria[0]: Run alembic upgrade head before the integration tests.",
        "acceptance_criteria[1]: Run alembic downgrade -1 to validate rollback.",
    )
    assert checklist.suggested_validation_commands == (
        "Run alembic upgrade head before the integration tests.",
        "Run alembic downgrade -1 to validate rollback.",
    )


def test_backfill_wording_is_high_risk_and_retains_evidence():
    result = build_task_data_migration_checklist(
        _plan(
            [
                _task(
                    "task-backfill",
                    title="Backfill account slugs",
                    description="Populate existing rows in batches and recompute search indexes.",
                    acceptance_criteria=["Existing rows have stable slugs after the data migration."],
                )
            ]
        )
    )

    checklist = result.checklists[0]

    assert checklist.risk_level == "high"
    assert _risk_note(checklist, "backfill").evidence == (
        "title: Backfill account slugs",
        "description: Populate existing rows in batches and recompute search indexes.",
        "acceptance_criteria[0]: Existing rows have stable slugs after the data migration.",
    )
    assert "Make the backfill idempotent and record expected row counts." in (
        checklist.required_steps
    )


def test_non_data_tasks_return_empty_result():
    result = build_task_data_migration_checklist(
        _plan(
            [
                _task(
                    "task-ui",
                    title="Tighten button spacing",
                    files_or_modules=["src/components/Button.tsx"],
                    acceptance_criteria=["Button spacing matches the design."],
                )
            ]
        )
    )

    assert result.plan_id == "plan-data-migration"
    assert result.checklists == ()
    assert result.to_dict() == {"plan_id": "plan-data-migration", "checklists": []}


def test_custom_metadata_hints_and_model_inputs_serialize_stably():
    task_model = ExecutionTask.model_validate(
        _task(
            "task-metadata",
            title="Load reference data",
            description="Import the new region catalog.",
            acceptance_criteria=["Region rows are seeded once."],
            metadata={
                "migration_required": True,
                "validation_command": "poetry run pytest tests/test_regions.py",
                "data_notes": ["Seed reference data for regions."],
            },
        )
    )

    result = build_task_data_migration_checklist(task_model)
    payload = task_data_migration_checklist_to_dict(result)

    assert payload == result.to_dict()
    assert list(payload) == ["plan_id", "checklists"]
    assert list(payload["checklists"][0]) == [
        "task_id",
        "title",
        "risk_level",
        "required_steps",
        "risk_notes",
        "suggested_validation_commands",
    ]
    assert result.checklists[0].suggested_validation_commands == (
        "poetry run pytest tests/test_regions.py",
    )
    assert _risk_note(result.checklists[0], "metadata").evidence == (
        "metadata.data_notes[0]: Seed reference data for regions.",
        "metadata.migration_required: True",
        "metadata.validation_command: poetry run pytest tests/test_regions.py",
    )
    assert json.loads(json.dumps(payload)) == payload


def _risk_note(checklist, category):
    return next(note for note in checklist.risk_notes if note.category == category)


def _plan(tasks):
    return {
        "id": "plan-data-migration",
        "implementation_brief_id": "brief-data-migration",
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
    test_command=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if test_command is not None:
        task["test_command"] = test_command
    if metadata is not None:
        task["metadata"] = metadata
    return task
