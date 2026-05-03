import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_data_retention_purge_readiness import (
    TaskDataRetentionPurgeReadinessPlan,
    TaskDataRetentionPurgeReadinessRecord,
    analyze_task_data_retention_purge_readiness,
    build_task_data_retention_purge_readiness_plan,
    extract_task_data_retention_purge_readiness,
    generate_task_data_retention_purge_readiness,
    recommend_task_data_retention_purge_readiness,
    summarize_task_data_retention_purge_readiness,
    task_data_retention_purge_readiness_plan_to_dict,
    task_data_retention_purge_readiness_plan_to_dicts,
    task_data_retention_purge_readiness_plan_to_markdown,
)


def test_retention_sensitive_cleanup_requires_separate_purge_safeguards():
    result = build_task_data_retention_purge_readiness_plan(
        _plan(
            [
                _task(
                    "task-purge",
                    title="Purge expired lifecycle data",
                    description=(
                        "Purge expired records for archived entities, temporary files, sessions, tokens, "
                        "exports, and event logs using a scheduled deletion job."
                    ),
                    files_or_modules=[
                        "src/jobs/retention_purge_worker.py",
                        "src/exports/temporary_files_cleanup.py",
                        "src/sessions/expired_tokens.py",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskDataRetentionPurgeReadinessPlan)
    assert result.retention_purge_task_ids == ("task-purge",)
    record = result.records[0]
    assert isinstance(record, TaskDataRetentionPurgeReadinessRecord)
    assert record.detected_signals == (
        "retention_policy",
        "purge_cadence",
        "expired_records",
        "archived_entities",
        "temporary_files",
        "sessions_tokens",
        "exports",
        "event_logs",
        "scheduled_deletion_job",
    )
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "retention_policy",
        "purge_cadence",
        "legal_hold",
        "dry_run_backfill",
        "idempotency",
        "restore_window",
        "audit_evidence",
        "monitoring",
    )
    assert record.risk_level == "high"
    assert any("Check legal holds" in item for item in record.recommended_readiness_steps)
    assert any("description:" in item and "scheduled deletion job" in item for item in record.evidence)
    assert "files_or_modules: src/jobs/retention_purge_worker.py" in record.evidence
    assert result.summary["signal_counts"]["sessions_tokens"] == 1
    assert result.summary["missing_safeguard_counts"]["monitoring"] == 1


def test_existing_safeguard_mentions_reduce_missing_safeguard_list():
    result = analyze_task_data_retention_purge_readiness(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Cleanup expired exports",
                    description="Scheduled purge removes export files older than the retention period.",
                    acceptance_criteria=[
                        "Retention policy keeps exports for 30 days and documents owner-approved exceptions.",
                        "Purge cadence runs nightly with bounded batches.",
                        "Legal hold check skips compliance hold records.",
                        "Dry-run counts and backfill plan preview affected row counts before purge.",
                        "Cleanup job is idempotent and retry safe for already purged batches.",
                        "Restore window uses soft delete for seven days.",
                        "Audit evidence records purge event scope, timing, counts, and outcome.",
                        "Monitoring dashboard and failure alerts track purge lag and job health.",
                    ],
                    metadata={"retention": {"policy": "Data lifecycle policy covers export files."}},
                )
            ]
        )
    )

    record = result.records[0]
    assert record.present_safeguards == (
        "retention_policy",
        "purge_cadence",
        "legal_hold",
        "dry_run_backfill",
        "idempotency",
        "restore_window",
        "audit_evidence",
        "monitoring",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "low"
    assert any("metadata.retention.policy" in item for item in record.evidence)
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_plan_mapping_model_single_task_object_empty_and_invalid_inputs():
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Expire stale sessions",
            description="Cleanup expired sessions and refresh tokens after TTL.",
            acceptance_criteria=["Restore window and audit evidence are documented."],
        )
    )
    object_task = SimpleNamespace(
        id="task-object",
        title="Prune event logs",
        description="Event retention purge job removes event logs after retention policy.",
        acceptance_criteria=["Monitoring alerts track cleanup lag."],
        metadata={},
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-plan",
                    title="Archive cleanup",
                    description="Scheduled cleanup removes archived records after the retention period.",
                ),
                _task("task-copy", title="Copy update", description="Update dashboard wording."),
            ],
            plan_id="plan-model",
        )
    )

    single = build_task_data_retention_purge_readiness_plan(model_task)
    object_result = build_task_data_retention_purge_readiness_plan(object_task)
    plan_result = generate_task_data_retention_purge_readiness(plan_model)
    empty = build_task_data_retention_purge_readiness_plan([])
    invalid = build_task_data_retention_purge_readiness_plan(42)
    mapping_noop = build_task_data_retention_purge_readiness_plan(
        _plan([_task("task-copy", title="Copy update", description="Static text.")])
    )

    assert single.plan_id is None
    assert single.retention_purge_task_ids == ("task-model",)
    assert object_result.retention_purge_task_ids == ("task-object",)
    assert plan_result.plan_id == "plan-model"
    assert plan_result.retention_purge_task_ids == ("task-plan",)
    assert plan_result.no_impact_task_ids == ("task-copy",)
    assert empty.records == ()
    assert invalid.records == ()
    assert mapping_noop.records == ()
    assert mapping_noop.no_impact_task_ids == ("task-copy",)
    assert mapping_noop.summary == {
        "task_count": 1,
        "retention_purge_task_count": 0,
        "no_impact_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "retention_policy": 0,
            "purge_cadence": 0,
            "expired_records": 0,
            "archived_entities": 0,
            "temporary_files": 0,
            "sessions_tokens": 0,
            "exports": 0,
            "event_logs": 0,
            "scheduled_deletion_job": 0,
        },
        "missing_safeguard_counts": {
            "retention_policy": 0,
            "purge_cadence": 0,
            "legal_hold": 0,
            "dry_run_backfill": 0,
            "idempotency": 0,
            "restore_window": 0,
            "audit_evidence": 0,
            "monitoring": 0,
        },
        "present_safeguard_counts": {
            "retention_policy": 0,
            "purge_cadence": 0,
            "legal_hold": 0,
            "dry_run_backfill": 0,
            "idempotency": 0,
            "restore_window": 0,
            "audit_evidence": 0,
            "monitoring": 0,
        },
        "retention_purge_task_ids": [],
    }
    assert "No task data retention purge readiness records" in mapping_noop.to_markdown()
    assert "No-impact tasks: task-copy" in mapping_noop.to_markdown()


def test_audit_log_retention_and_account_deletion_do_not_match_general_purge():
    result = build_task_data_retention_purge_readiness_plan(
        _plan(
            [
                _task(
                    "task-audit",
                    title="Audit log retention settings",
                    description="Configure immutable audit log retention for security investigations.",
                ),
                _task(
                    "task-account",
                    title="Account deletion workflow",
                    description="Implement account deletion and right-to-erasure request handling.",
                ),
            ]
        )
    )

    assert result.records == ()
    assert result.no_impact_task_ids == ("task-audit", "task-account")


def test_deterministic_serialization_markdown_aliases_sorting_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Retention purge | ready",
                description="Scheduled purge removes temporary exports after the retention period.",
                acceptance_criteria=[
                    "Retention policy keeps exports for 30 days.",
                    "Purge cadence runs nightly.",
                    "Legal hold check skips held records.",
                    "Dry-run counts and backfill plan are documented.",
                    "Idempotency handles already purged batches.",
                    "Restore window uses soft delete.",
                    "Audit evidence records purge event counts.",
                    "Monitoring alerts track failure and purge lag.",
                ],
            ),
            _task(
                "task-a",
                title="Purge old event logs",
                description="Scheduled deletion job purges event logs older than the retention period.",
            ),
            _task("task-copy", title="Copy update", description="Update empty state text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_data_retention_purge_readiness(plan)
    payload = task_data_retention_purge_readiness_plan_to_dict(result)
    markdown = task_data_retention_purge_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_data_retention_purge_readiness_plan_to_dicts(result) == payload["records"]
    assert task_data_retention_purge_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_data_retention_purge_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_data_retention_purge_readiness(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "recommendations",
        "retention_purge_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "required_safeguards",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "recommended_readiness_steps",
        "evidence",
    ]
    assert result.retention_purge_task_ids == ("task-a", "task-z")
    assert [record.risk_level for record in result.records] == ["high", "low"]
    assert result.no_impact_task_ids == ("task-copy",)
    assert markdown.startswith("# Task Data Retention Purge Readiness: plan-retention-purge")
    assert "Retention purge \\| ready" in markdown
    assert (
        "| Task | Title | Risk | Signals | Present Safeguards | Missing Safeguards | Recommended Steps | Evidence |"
        in markdown
    )


def _plan(tasks, plan_id="plan-retention-purge"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-retention-purge",
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
        "execution_plan_id": "plan-retention-purge",
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
