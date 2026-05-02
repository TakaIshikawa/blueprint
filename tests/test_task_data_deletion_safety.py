import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_data_deletion_safety import (
    TaskDataDeletionSafetyPlan,
    TaskDataDeletionSafetyRecord,
    build_task_data_deletion_safety,
    derive_task_data_deletion_safety,
    generate_task_data_deletion_safety,
    summarize_task_data_deletion_safety,
    task_data_deletion_safety_to_dict,
    task_data_deletion_safety_to_dicts,
    task_data_deletion_safety_to_markdown,
)


def test_risky_deletion_with_missing_safeguards_is_blocked():
    result = build_task_data_deletion_safety(
        _plan(
            [
                _task(
                    "task-hard-delete",
                    title="Hard delete inactive workspaces",
                    description="Permanently delete workspace records and remove dependent files.",
                    acceptance_criteria=[
                        "Deletion job removes database rows for inactive workspaces.",
                    ],
                    risks=[
                        "Irreversible data loss if dependent records are removed incorrectly.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert isinstance(record, TaskDataDeletionSafetyRecord)
    assert record.deletion_vectors == ("hard_delete",)
    assert record.readiness_level == "blocked"
    assert record.required_safeguards == (
        "authorization_check",
        "dry_run_or_preview",
        "backup_or_restore_path",
        "audit_event",
        "dependency_cascade_review",
    )
    assert record.present_safeguards == ()
    assert record.missing_safeguards == record.required_safeguards
    assert any("Block implementation until" in action for action in record.recommended_actions)
    assert result.summary["blocked_task_count"] == 1
    assert result.summary["missing_safeguard_count"] == 5


def test_strong_deletion_readiness_detects_all_required_safeguards():
    result = build_task_data_deletion_safety(
        _plan(
            [
                _task(
                    "task-retention-purge",
                    title="Retention purge for expired exports",
                    description=(
                        "Scheduled retention purge deletes expired export records. "
                        "Run a dry-run preview with affected-record counts. "
                        "Check retention policy and legal hold rules before purge. "
                        "Emit an audit event for actor, scope, target, and outcome. "
                        "Backups and point-in-time restore cover the purge window."
                    ),
                )
            ]
        )
    )

    record = result.records[0]
    assert record.deletion_vectors == ("retention_purge",)
    assert record.present_safeguards == (
        "dry_run_or_preview",
        "backup_or_restore_path",
        "audit_event",
        "retention_policy_check",
    )
    assert record.missing_safeguards == ()
    assert record.readiness_level == "ready"
    assert record.recommended_actions == (
        "Ready to implement after preserving the documented deletion safeguards.",
    )
    assert result.summary["ready_task_count"] == 1


def test_metadata_evidence_detects_erasure_and_customer_confirmation():
    result = build_task_data_deletion_safety(
        _plan(
            [
                _task(
                    "task-erasure",
                    title="Account privacy workflow",
                    metadata={
                        "deletion_safety": {
                            "vector": "GDPR erasure for customer delete my data requests",
                            "authorization": "RBAC admin policy check is required",
                            "customer_confirmation": "typed confirmation from the customer",
                            "audit_event": "security audit event records request id and outcome",
                            "retention_policy": "legal hold and retention policy check before erasure",
                        }
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert record.deletion_vectors == ("user_request_erasure",)
    assert record.present_safeguards == (
        "authorization_check",
        "audit_event",
        "retention_policy_check",
        "customer_confirmation",
    )
    assert record.missing_safeguards == ()
    assert record.readiness_level == "ready"
    assert (
        "metadata.deletion_safety.customer_confirmation: customer confirmation" in record.evidence
    )
    assert "metadata.deletion_safety.vector: GDPR erasure" in " ".join(record.evidence)


def test_path_detection_finds_anonymization_and_tombstone_cleanup():
    result = build_task_data_deletion_safety(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Background maintenance paths",
                    files_or_modules=[
                        "src/jobs/tombstone_cleanup_worker.py",
                        "src/privacy/anonymize_user_events.py",
                    ],
                    acceptance_criteria=[
                        "Preview affected records before running.",
                        "Audit log records anonymization and cleanup results.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.deletion_vectors == ("anonymization", "tombstone_cleanup")
    assert "dry_run_or_preview" in record.present_safeguards
    assert "audit_event" in record.present_safeguards
    assert "dependency_cascade_review" in record.missing_safeguards
    assert "retention_policy_check" in record.missing_safeguards
    assert any("files_or_modules[0]" in item for item in record.evidence)
    assert any("files_or_modules[1]" in item for item in record.evidence)


def test_ignored_tasks_return_stable_summary_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-copy",
                title="Update onboarding copy",
                description="Refresh help text and labels.",
            ),
            _task(
                "task-negated",
                title="Profile read API",
                description="No deletion, purge, erasure, anonymization, or tombstone changes are in scope.",
            ),
        ],
        plan_id="plan-ignore",
    )
    original = copy.deepcopy(plan)

    result = build_task_data_deletion_safety(plan)

    assert plan == original
    assert isinstance(result, TaskDataDeletionSafetyPlan)
    assert result.records == ()
    assert result.ignored_task_ids == ("task-copy", "task-negated")
    assert result.to_dict() == {
        "plan_id": "plan-ignore",
        "summary": {
            "total_task_count": 2,
            "deletion_task_count": 0,
            "ignored_task_count": 2,
            "ready_task_count": 0,
            "needs_safeguards_task_count": 0,
            "blocked_task_count": 0,
            "missing_safeguard_count": 0,
            "vector_counts": {
                "hard_delete": 0,
                "soft_delete": 0,
                "cascade_delete": 0,
                "retention_purge": 0,
                "user_request_erasure": 0,
                "anonymization": 0,
                "tombstone_cleanup": 0,
            },
            "missing_safeguard_counts": {
                "authorization_check": 0,
                "dry_run_or_preview": 0,
                "backup_or_restore_path": 0,
                "audit_event": 0,
                "dependency_cascade_review": 0,
                "retention_policy_check": 0,
                "customer_confirmation": 0,
            },
        },
        "records": [],
        "ignored_task_ids": ["task-copy", "task-negated"],
    }
    assert result.to_markdown() == (
        "# Task Data Deletion Safety Plan: plan-ignore\n\n"
        "## Summary\n\n"
        "- Total tasks: 2\n"
        "- Deletion-related tasks: 0\n"
        "- Ignored tasks: 2\n"
        "- Ready tasks: 0\n"
        "- Tasks needing safeguards: 0\n"
        "- Blocked tasks: 0\n"
        "- Missing safeguards: 0\n\n"
        "No data deletion safety records were inferred."
    )
    assert build_task_data_deletion_safety({"tasks": "not a list"}).ignored_task_ids == ()
    assert build_task_data_deletion_safety(None).summary["total_task_count"] == 0


def test_deterministic_serialization_aliases_and_model_input():
    plan = _plan(
        [
            _task(
                "task-md",
                title="Soft delete | archive account",
                description=(
                    "Soft delete uses deleted_at with RBAC authorization. "
                    "Audit trail records actor | target. Snapshot backup supports restore."
                ),
            )
        ],
        plan_id="plan-model",
    )
    model = ExecutionPlan.model_validate(plan)

    result = build_task_data_deletion_safety(model)
    derived = derive_task_data_deletion_safety(result)
    summarized = summarize_task_data_deletion_safety(plan)
    records = generate_task_data_deletion_safety(model)
    payload = task_data_deletion_safety_to_dict(result)
    markdown = task_data_deletion_safety_to_markdown(result)

    assert derived is result
    assert summarized.to_dict() == result.to_dict()
    assert records == result.records
    assert result.to_dicts() == payload["records"]
    assert task_data_deletion_safety_to_dicts(result) == payload["records"]
    assert task_data_deletion_safety_to_dicts(records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "records", "ignored_task_ids"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "deletion_vectors",
        "required_safeguards",
        "present_safeguards",
        "missing_safeguards",
        "readiness_level",
        "evidence",
        "recommended_actions",
    ]
    assert payload["records"][0]["readiness_level"] == "ready"
    assert "Soft delete \\| archive account" in markdown
    assert "actor \\| target" in markdown
    assert markdown == result.to_markdown()


def _plan(tasks, *, plan_id="plan-deletion"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-deletion",
        "milestones": [{"name": "Launch"}],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
    risks=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if risks is not None:
        task["risks"] = risks
    return task
