import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_database_index_impact import (
    TaskDatabaseIndexImpactPlan,
    TaskDatabaseIndexImpactRecord,
    analyze_task_database_index_impact,
    build_task_database_index_impact_plan,
    summarize_task_database_index_impact,
    summarize_task_database_index_impacts,
    task_database_index_impact_plan_to_dict,
    task_database_index_impact_plan_to_markdown,
)


def test_high_impact_migration_receives_explain_backfill_rollout_and_rollback_safeguards():
    result = build_task_database_index_impact_plan(
        _plan(
            [
                _task(
                    "task-index",
                    title="Add concurrent index for order search backfill",
                    description=(
                        "Create index on the production orders table, backfill indexed state, "
                        "and support rollback if query latency increases."
                    ),
                    files_or_modules=["migrations/versions/20260501_add_orders_search_index.sql"],
                    acceptance_criteria=[
                        "Capture EXPLAIN plans for search filters before rollout.",
                        "Backfill runs in chunks without table locks.",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskDatabaseIndexImpactPlan)
    assert result.plan_id == "plan-db-index"
    assert result.impacted_task_ids == ("task-index",)
    assert result.summary["impact_counts"]["high"] == 1
    record = result.records[0]
    assert isinstance(record, TaskDatabaseIndexImpactRecord)
    assert record.impact_level == "high"
    assert record.data_access_patterns == (
        "migration",
        "schema_change",
        "index_change",
        "sql_query",
        "filtering",
        "search",
        "backfill",
        "high_volume_table",
    )
    assert any("EXPLAIN plans" in value for value in record.safeguards)
    assert any("backfill safety controls" in value for value in record.safeguards)
    assert any("Roll out behind a feature flag" in value for value in record.safeguards)
    assert any("Prepare rollback steps" in value for value in record.safeguards)
    assert any("EXPLAIN plan capture" in value for value in record.validation_commands_to_add)
    assert record.evidence[:2] == (
        "files_or_modules: migrations/versions/20260501_add_orders_search_index.sql",
        "title: Add concurrent index for order search backfill",
    )


def test_query_features_are_classified_medium_or_high_by_access_pattern_mix():
    result = analyze_task_database_index_impact(
        _plan(
            [
                _task(
                    "task-orm",
                    title="Add account ORM model scopes",
                    description="Add SQLAlchemy repository filters for account status.",
                    files_or_modules=["src/blueprint/models/account.py"],
                ),
                _task(
                    "task-dashboard",
                    title="Build audit dashboard pagination",
                    description=(
                        "Dashboard queries search, filter, sort, and paginate audit log rows "
                        "with order by created_at."
                    ),
                    files_or_modules=["src/blueprint/reports/audit_dashboard.py"],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert by_id["task-orm"].impact_level == "medium"
    assert by_id["task-orm"].data_access_patterns == ("sql_query", "orm_model", "filtering")
    assert by_id["task-dashboard"].impact_level == "high"
    assert by_id["task-dashboard"].data_access_patterns == (
        "sql_query",
        "filtering",
        "sorting",
        "pagination",
        "search",
        "dashboard",
        "high_volume_table",
    )
    assert result.summary["pattern_counts"]["pagination"] == 1
    assert result.summary["pattern_counts"]["orm_model"] == 1


def test_low_impact_and_empty_or_invalid_inputs_are_stable():
    result = build_task_database_index_impact_plan(
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
    empty = build_task_database_index_impact_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_database_index_impact_plan(37)

    assert result.impacted_task_ids == ()
    assert result.records[0].impact_level == "low"
    assert result.records[0].data_access_patterns == ()
    assert result.records[0].safeguards == ()
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert empty.summary["task_count"] == 0
    assert "No execution tasks" in empty.to_markdown()
    assert invalid.plan_id is None
    assert invalid.records == ()


def test_execution_plan_models_object_like_tasks_and_validation_command_evidence():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add filtered search endpoint",
        description="Search accounts with filters and cursor pagination.",
        files_or_modules=["src/blueprint/repositories/account_search.py"],
        acceptance_criteria=["Sort by newest account and page by cursor."],
        metadata={"validation_commands": {"test": ["poetry run pytest tests/db/test_account_search.py"]}},
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Add customer table migration",
            description="Create table for customer notes.",
            files_or_modules=["db/migrations/20260502_create_customer_notes.sql"],
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan([task_model.model_dump(mode="python")], plan_id="plan-model")
    )

    first = summarize_task_database_index_impacts([object_task])
    second = build_task_database_index_impact_plan(plan_model)

    object_record = first.records[0]
    assert object_record.task_id == "task-object"
    assert object_record.impact_level == "high"
    assert (
        "validation_commands: poetry run pytest tests/db/test_account_search.py"
        in object_record.evidence
    )
    assert any("database performance assertions" in value for value in object_record.validation_commands_to_add)
    assert second.plan_id == "plan-model"
    assert second.records[0].task_id == "task-model"
    assert second.records[0].impact_level == "medium"


def test_malformed_fields_serialization_markdown_summary_counts_and_deduplication_are_stable():
    task_dict = _task(
        "task-malformed",
        title="Add index | invoice search",
        description="Add index for invoice search and search filters.",
        files_or_modules={
            "main": "db/migrations/add_invoice_search_index.sql",
            "duplicate": "db/migrations/add_invoice_search_index.sql",
            "none": None,
        },
        acceptance_criteria={"plan": "Run EXPLAIN plan and rollout checks."},
        metadata={
            "index_notes": [{"detail": "Composite index for invoice filters"}, None, 7],
            "validation_commands": {"test": ["poetry run pytest tests/db/test_invoice_search.py"]},
        },
        test_command="poetry run pytest tests/db/test_invoice_search.py",
    )
    original = copy.deepcopy(task_dict)

    result = summarize_task_database_index_impact(_plan([task_dict]))
    payload = task_database_index_impact_plan_to_dict(result)
    markdown = task_database_index_impact_plan_to_markdown(result)
    record = result.records[0]

    assert task_dict == original
    assert result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "impacted_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "impact_level",
        "data_access_patterns",
        "safeguards",
        "validation_commands_to_add",
        "evidence",
        "follow_up_questions",
    ]
    assert payload["summary"]["task_count"] == 1
    assert payload["summary"]["impacted_task_count"] == 1
    assert payload["summary"]["impact_counts"]["high"] == 1
    assert payload["summary"]["pattern_counts"]["index_change"] == 1
    assert len(record.evidence) == len(set(record.evidence))
    assert record.evidence.count("files_or_modules: db/migrations/add_invoice_search_index.sql") == 1
    assert markdown.startswith("# Task Database Index Impact Plan: plan-db-index")
    assert "Add index \\| invoice search" not in markdown
    assert "EXPLAIN plan capture" in markdown


def _plan(tasks, plan_id="plan-db-index"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-db-index",
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
    test_command=None,
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
    if test_command is not None:
        task["test_command"] = test_command
    return task
