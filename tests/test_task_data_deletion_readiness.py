import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_data_deletion_readiness import (
    TaskDataDeletionReadinessPlan,
    TaskDataDeletionReadinessRecord,
    analyze_task_data_deletion_readiness,
    build_task_data_deletion_readiness_plan,
    extract_task_data_deletion_readiness,
    generate_task_data_deletion_readiness,
    recommend_task_data_deletion_readiness,
    summarize_task_data_deletion_readiness,
    task_data_deletion_readiness_plan_to_dict,
    task_data_deletion_readiness_plan_to_dicts,
    task_data_deletion_readiness_plan_to_markdown,
)


def test_detects_destructive_deletion_signals_and_missing_safeguards():
    result = build_task_data_deletion_readiness_plan(
        _plan(
            [
                _task(
                    "task-purge",
                    title="Purge account data for erasure requests",
                    description=(
                        "Hard delete customer account data for GDPR deletion and right to erasure requests. "
                        "Cascade delete child records, write tombstones, remove from backups, search index, "
                        "and analytics warehouse, and keep audit evidence."
                    ),
                    files_or_modules=[
                        "src/privacy/account_deletion.py",
                        "src/jobs/search_index_removal.py",
                        "src/analytics/delete_from_warehouse.py",
                    ],
                    acceptance_criteria=[
                        "Deletion receipt is visible in audit log.",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskDataDeletionReadinessPlan)
    assert result.deletion_task_ids == ("task-purge",)
    record = result.records[0]
    assert isinstance(record, TaskDataDeletionReadinessRecord)
    assert {
        "hard_delete",
        "purge",
        "erasure",
        "account_deletion",
        "gdpr_deletion",
        "tombstone",
        "cascading_delete",
        "backup_deletion",
        "search_index_removal",
        "analytics_removal",
        "audit_evidence",
    } <= set(record.matched_deletion_signals)
    assert record.present_safeguards == ("audit_trail",)
    assert {
        "dry_run_counts",
        "cascade_inventory",
        "backup_restore_implications",
        "legal_hold_check",
        "idempotency",
        "downstream_deletion_propagation",
        "customer_confirmation",
    } <= set(record.missing_safeguards)
    assert record.risk_level == "high"
    assert any("Propagate deletions to downstream stores" in item for item in record.recommended_checks)
    assert any("description:" in item and "right to erasure" in item for item in record.evidence)
    assert "files_or_modules: src/privacy/account_deletion.py" in record.evidence
    assert result.summary["signal_counts"]["account_deletion"] == 1
    assert result.summary["missing_safeguard_counts"]["dry_run_counts"] == 1


def test_metadata_acceptance_criteria_and_validation_plan_detect_safeguards():
    result = analyze_task_data_deletion_readiness(
        _plan(
            [
                _task(
                    "task-ready",
                    title="GDPR erasure execution",
                    description="Erase user profile data for data subject deletion requests.",
                    metadata={
                        "deletion_readiness": {
                            "dry_run": "Dry-run counts list affected row counts before delete.",
                            "holds": "Legal hold check blocks deletion for retention hold records.",
                            "backup": "Backup/restore implications are documented for snapshots.",
                            "cascade": "Cascade inventory covers child records and dependent records.",
                            "idempotency": "Deletion worker is idempotent and retry safe when already deleted.",
                        }
                    },
                    acceptance_criteria=[
                        "Audit trail records actor, erasure event, target scope, and outcome.",
                        "Downstream deletion propagation removes data from search index and analytics.",
                        "Customer confirmation is required before account deletion starts.",
                    ],
                    validation_plan="Run dry-run preview mode before the purge job.",
                )
            ]
        )
    )

    record = result.records[0]
    assert {"erasure", "gdpr_deletion", "purge", "search_index_removal", "analytics_removal"} <= set(
        record.matched_deletion_signals
    )
    assert record.present_safeguards == (
        "dry_run_counts",
        "cascade_inventory",
        "backup_restore_implications",
        "legal_hold_check",
        "audit_trail",
        "idempotency",
        "downstream_deletion_propagation",
        "customer_confirmation",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "low"
    assert any("metadata.deletion_readiness.dry_run" in item for item in record.evidence)
    assert any("validation_plan:" in item and "preview mode" in item for item in record.evidence)
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_risk_escalates_when_irreversible_deletion_lacks_dry_run_or_legal_hold():
    result = build_task_data_deletion_readiness_plan(
        _plan(
            [
                _task(
                    "task-hard-delete",
                    title="Hard delete expired profiles",
                    description="Permanently delete expired profiles and make the operation idempotent.",
                    acceptance_criteria=["Audit trail records deletion event."],
                ),
                _task(
                    "task-controlled",
                    title="Purge expired tombstones with controls",
                    description="Purge expired tombstone records after retention.",
                    acceptance_criteria=[
                        "Dry-run counts show affected row counts.",
                        "Legal hold check blocks compliance hold records.",
                        "Audit trail records the purge event.",
                        "Deletion job is idempotent and retry safe.",
                        "Backup/restore implications are documented.",
                        "Cascade inventory confirms no dependent records.",
                    ],
                ),
            ]
        )
    )

    assert result.deletion_task_ids == ("task-hard-delete", "task-controlled")
    by_id = {record.task_id: record for record in result.records}
    assert by_id["task-hard-delete"].risk_level == "high"
    assert "dry_run_counts" in by_id["task-hard-delete"].missing_safeguards
    assert "legal_hold_check" in by_id["task-hard-delete"].missing_safeguards
    assert by_id["task-controlled"].risk_level == "low"


def test_execution_plan_execution_task_single_task_and_no_impact_handling():
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Delete account workflow",
            description="Account deletion closes the account and propagates erasure downstream.",
            acceptance_criteria=[
                "Customer confirmation is required.",
                "Downstream deletion propagation removes data from processors.",
            ],
        )
    )
    single_task = build_task_data_deletion_readiness_plan(model_task)
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-erase",
                    title="Right to erasure",
                    description="Implement right to erasure request handling with dry-run counts.",
                ),
                _task("task-copy", title="Update copy", description="Adjust empty state wording."),
            ]
        )
    )
    plan_result = generate_task_data_deletion_readiness(plan)
    empty = build_task_data_deletion_readiness_plan([])
    noop = build_task_data_deletion_readiness_plan(
        _plan([_task("task-copy", title="Update copy", description="Static text.")])
    )

    assert single_task.plan_id is None
    assert single_task.deletion_task_ids == ("task-model",)
    assert plan_result.plan_id == "plan-data-deletion"
    assert plan_result.deletion_task_ids == ("task-erase",)
    assert plan_result.no_impact_task_ids == ("task-copy",)
    assert empty.records == ()
    assert empty.no_impact_task_ids == ()
    assert noop.records == ()
    assert noop.deletion_task_ids == ()
    assert noop.no_impact_task_ids == ("task-copy",)
    assert noop.summary == {
        "task_count": 1,
        "deletion_task_count": 0,
        "no_impact_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "hard_delete": 0,
            "purge": 0,
            "erasure": 0,
            "account_deletion": 0,
            "gdpr_deletion": 0,
            "tombstone": 0,
            "cascading_delete": 0,
            "retention_exception": 0,
            "backup_deletion": 0,
            "search_index_removal": 0,
            "analytics_removal": 0,
            "audit_evidence": 0,
        },
        "missing_safeguard_counts": {
            "dry_run_counts": 0,
            "cascade_inventory": 0,
            "backup_restore_implications": 0,
            "legal_hold_check": 0,
            "audit_trail": 0,
            "idempotency": 0,
            "downstream_deletion_propagation": 0,
            "customer_confirmation": 0,
        },
        "present_safeguard_counts": {
            "dry_run_counts": 0,
            "cascade_inventory": 0,
            "backup_restore_implications": 0,
            "legal_hold_check": 0,
            "audit_trail": 0,
            "idempotency": 0,
            "downstream_deletion_propagation": 0,
            "customer_confirmation": 0,
        },
        "deletion_task_ids": [],
    }
    assert "No task data deletion readiness records" in noop.to_markdown()
    assert "No-impact tasks: task-copy" in noop.to_markdown()


def test_deterministic_serialization_markdown_aliases_sorting_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Account deletion | ready",
                description="Account deletion for right to erasure requests.",
                acceptance_criteria=[
                    "Dry-run counts show affected row counts.",
                    "Cascade inventory covers child records.",
                    "Backup/restore implications are documented.",
                    "Legal hold check blocks records.",
                    "Audit trail records erasure event.",
                    "Idempotency handles already deleted records.",
                    "Downstream deletion propagation removes search index and analytics data.",
                    "Customer confirmation is required.",
                ],
            ),
            _task(
                "task-a",
                title="Purge analytics events",
                description="Purge analytics events and remove from warehouse without dry-run controls.",
            ),
            _task("task-copy", title="Copy update", description="Update helper text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_data_deletion_readiness(plan)
    payload = task_data_deletion_readiness_plan_to_dict(result)
    markdown = task_data_deletion_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_data_deletion_readiness_plan_to_dicts(result) == payload["records"]
    assert task_data_deletion_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_data_deletion_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_data_deletion_readiness(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "recommendations",
        "deletion_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "matched_deletion_signals",
        "required_safeguards",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "recommended_checks",
        "evidence",
    ]
    assert result.deletion_task_ids == ("task-a", "task-z")
    assert [record.risk_level for record in result.records] == ["high", "low"]
    assert result.no_impact_task_ids == ("task-copy",)
    assert markdown.startswith("# Task Data Deletion Readiness: plan-data-deletion")
    assert "Account deletion \\| ready" in markdown
    assert (
        "| Task | Title | Risk | Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |"
        in markdown
    )


def _plan(tasks, plan_id="plan-data-deletion"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-data-deletion",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    validation_plan=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-data-deletion",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "medium",
        "estimated_hours": 2.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if validation_plan is not None:
        payload["validation_plan"] = validation_plan
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
