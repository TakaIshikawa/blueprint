import copy
import json

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord
from blueprint.domain.models import ExecutionPlan
from blueprint.task_data_seeding_readiness import (
    TaskDataSeedingReadinessPlan,
    analyze_task_data_seeding_readiness,
    build_task_data_seeding_readiness_plan,
    recommend_task_data_seeding_readiness,
    summarize_task_data_seeding_readiness,
    summarize_task_data_seeding_readiness_plan,
    task_data_seeding_readiness_plan_to_dict,
    task_data_seeding_readiness_plan_to_dicts,
    task_data_seeding_readiness_plan_to_markdown,
)


def test_complete_data_seeding_task_is_ready_with_no_missing_requirements():
    result = build_task_data_seeding_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Seed reference data for sandbox tenants",
                    description="Run data seeding from a canonical JSON source file into staging.",
                    acceptance_criteria=[
                        "Seed source is the checked-in reference dataset.",
                        "Idempotent upserts with natural keys make the seed safe to rerun.",
                        "Environment targeting limits execution to sandbox and staging with a production guard.",
                        "Cleanup removes seeded records and rollback restores the pre-run snapshot.",
                        "Data platform owner is the DRI and approver.",
                        "Validation checks verify row count, checksums, and referential integrity.",
                        "Sensitive data masking uses synthetic records with no personal data.",
                    ],
                    files_or_modules=["db/seeds/reference_roles.json"],
                ),
                _task("task-docs", title="Update docs", description="Clarify onboarding copy."),
            ]
        )
    )

    assert isinstance(result, TaskDataSeedingReadinessPlan)
    assert isinstance(result, SimpleReadinessPlan)
    assert result.impacted_task_ids == ("task-ready",)
    assert result.ignored_task_ids == ("task-docs",)
    record = result.records[0]
    assert isinstance(record, SimpleReadinessRecord)
    assert record.detected_signals == ("data_seeding",)
    assert record.present_criteria == (
        "seed_source",
        "idempotency",
        "environment_targeting",
        "cleanup_or_rollback",
        "owner",
        "validation_checks",
        "sensitive_data_masking",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_partial_data_seeding_task_reports_deterministic_actionable_gaps():
    result = analyze_task_data_seeding_readiness(
        [
            _task(
                "task-partial",
                title="Load sample data",
                description="Load sample data for onboarding demos.",
            )
        ]
    )

    record = result.records[0]
    assert record.readiness == "needs_planning"
    assert record.present_criteria == ()
    assert record.missing_criteria == (
        "seed_source",
        "idempotency",
        "environment_targeting",
        "cleanup_or_rollback",
        "owner",
        "validation_checks",
        "sensitive_data_masking",
    )
    assert record.recommended_follow_up_actions == (
        "Identify the canonical seed source such as a fixture, dump, CSV, JSON, or reference dataset.",
        "Make seed execution safe to rerun using upserts, unique keys, dedupe guards, or equivalent controls.",
        "Specify target environments and production guards for the seeding run.",
        "Document cleanup, rollback, restore, truncate, or teardown steps for seeded records.",
        "Name the owner, DRI, responsible team, approver, or data steward for the seed data.",
        "Add validation checks such as counts, smoke tests, checksums, or integrity assertions.",
        "Confirm sensitive data is masked, synthetic, anonymized, scrubbed, or explicitly absent.",
    )


def test_path_hints_and_nested_metadata_contribute_evidence_without_mutation():
    source = _plan(
        [
            _task(
                "task-paths",
                title="Bootstrap tenant roles",
                description="Bootstrap data for new tenants.",
                files_or_modules=[
                    "fixtures/tenant_roles.yml",
                    "migrations/20260514_seed_roles.sql",
                    "sample-data/demo-users.json",
                ],
                metadata={
                    "runbook": {
                        "owner": "Admin data owner approves the seed.",
                        "safety": "Upsert with unique constraint and post-run validation.",
                    }
                },
            )
        ]
    )
    original = copy.deepcopy(source)

    result = build_task_data_seeding_readiness_plan(ExecutionPlan.model_validate(source))

    assert source == original
    record = result.records[0]
    assert record.detected_signals == ("data_seeding", "fixture_load", "migration_seed")
    assert record.present_criteria == ("seed_source", "idempotency", "owner", "validation_checks")
    assert record.missing_criteria == (
        "environment_targeting",
        "cleanup_or_rollback",
        "sensitive_data_masking",
    )
    assert any("metadata.runbook.owner" in item for item in record.evidence)
    assert any("files_or_modules: fixtures/tenant_roles.yml" in item for item in record.evidence)
    assert any("files_or_modules: migrations/20260514_seed_roles.sql" in item for item in record.evidence)
    assert any("files_or_modules: sample-data/demo-users.json" in item for item in record.evidence)


def test_no_impact_tasks_are_not_findings_and_are_not_applicable_ids():
    result = summarize_task_data_seeding_readiness(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Refresh empty state copy",
                    description="No seed data or fixtures are required for this text-only update.",
                ),
                _task(
                    "task-partial",
                    title="Seed demo accounts",
                    description="Seed demo accounts with idempotent upserts.",
                ),
            ],
            plan_id="plan-data-seeding-sort",
        )
    )

    payload = task_data_seeding_readiness_plan_to_dict(result)
    markdown = task_data_seeding_readiness_plan_to_markdown(result)

    assert [record.task_id for record in result.records] == ["task-partial"]
    assert result.ignored_task_ids == ("task-copy",)
    assert analyze_task_data_seeding_readiness(result) is result
    assert summarize_task_data_seeding_readiness_plan(result) is result
    assert recommend_task_data_seeding_readiness(result) == result.records
    assert result.to_dicts() == payload["records"]
    assert task_data_seeding_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-data-seeding-sort"
    assert markdown.startswith("# Task Data Seeding Readiness: plan-data-seeding-sort")
    assert "| Task | Title | Readiness |" in markdown


def test_invalid_inputs_return_empty_simple_plan():
    assert build_task_data_seeding_readiness_plan(42).records == ()
    assert build_task_data_seeding_readiness_plan({"id": "bad", "tasks": "not a list"}).records == ()
    assert build_task_data_seeding_readiness_plan({"tasks": []}).summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-data-seeding"):
    return {"id": plan_id, "implementation_brief_id": "brief-data-seeding", "milestones": [], "tasks": tasks}


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
