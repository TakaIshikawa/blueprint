import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_disaster_recovery_readiness import (
    PlanDisasterRecoveryReadinessMatrix,
    PlanDisasterRecoveryReadinessRow,
    build_plan_disaster_recovery_readiness_matrix,
    plan_disaster_recovery_readiness_matrix_to_dict,
    plan_disaster_recovery_readiness_matrix_to_markdown,
    summarize_plan_disaster_recovery_readiness,
)


def test_recovery_signals_are_grouped_by_capability_with_sorted_task_ids():
    result = build_plan_disaster_recovery_readiness_matrix(
        _plan(
            [
                _task(
                    "task-z-restore",
                    title="Restore database backup",
                    description="Run a restore validation from the nightly snapshot.",
                    metadata={"owner": "database DRI"},
                ),
                _task(
                    "task-a-backup",
                    title="Create customer backup snapshot",
                    description="Capture database backup before the persistence migration.",
                    files_or_modules=["infra/backups/customer-db.tf"],
                    metadata={"owner": "SRE"},
                ),
                _task(
                    "task-b-backup",
                    title="Verify backup retention",
                    description="Confirm backup retention for account data.",
                    files_or_modules=["docs/backup-retention.md"],
                    metadata={"owner": "SRE"},
                ),
            ]
        )
    )

    assert isinstance(result, PlanDisasterRecoveryReadinessMatrix)
    by_capability = {row.recovery_capability: row for row in result.rows}

    assert by_capability["backup"].covered_task_ids == (
        "task-a-backup",
        "task-b-backup",
        "task-z-restore",
    )
    assert by_capability["restore"].covered_task_ids == ("task-z-restore",)
    assert any("files_or_modules:" in item for item in by_capability["backup"].evidence)
    assert result.summary["covered_task_count"] == 3
    assert result.summary["capability_counts"]["backup"] == 1


def test_detects_failover_replica_region_incident_data_and_manual_recovery_signals():
    result = build_plan_disaster_recovery_readiness_matrix(
        _plan(
            [
                _task(
                    "task-failover",
                    title="Failover primary database",
                    description="Promote standby replica and test failback during a regional outage.",
                    files_or_modules=["infra/failover/postgres.yaml"],
                    metadata={"owner": "SRE"},
                ),
                _task(
                    "task-incident",
                    title="Incident response for data recovery",
                    description="Open war room, assign incident commander, and replay events for corrupt records.",
                    files_or_modules=["runbooks/incidents/data_recovery.md"],
                    metadata={"owner": "support DRI"},
                ),
                _task(
                    "task-manual",
                    title="Manual recovery runbook",
                    description="Document break glass operator action for manual restore.",
                    files_or_modules=["runbooks/manual/recovery.md"],
                    metadata={"owner": "operations"},
                ),
            ],
            metadata={
                "dr_note": "RPO 15 minutes and RTO 1 hour are approved for multi-region DR.",
                "latest_drill": "Restore validation drill completed with checksum evidence.",
            },
        )
    )

    capabilities = {row.recovery_capability for row in result.rows}

    assert {
        "failover",
        "replica",
        "regional_outage",
        "incident_response",
        "data_recovery",
        "manual_recovery",
        "restore",
        "rpo_rto",
    } <= capabilities
    assert result.summary["severity_counts"]["high"] >= 4


def test_missing_recovery_readiness_information_creates_follow_up_questions():
    result = build_plan_disaster_recovery_readiness_matrix(
        _plan(
            [
                _task(
                    "task-restore",
                    title="Restore customer database",
                    description="Restore customer database after data loss.",
                    metadata={},
                )
            ]
        )
    )

    row = result.rows[0]

    assert row.recovery_capability == "restore"
    assert row.readiness_gaps == (
        "Missing RPO target",
        "Missing RTO target",
        "Missing restore validation evidence",
        "Missing recovery owner",
    )
    assert row.follow_up_questions == (
        "What RPO target applies to restore?",
        "What RTO target applies to restore?",
        "Which restore validation or rehearsal artifact proves restore works?",
        "Who owns recovery decisions for restore?",
    )
    assert row.owner_hints == ("database owner", "service owner")
    assert result.summary["follow_up_question_count"] >= 4
    assert result.summary["gap_count"] >= 4


def test_metadata_overrides_are_extracted_and_serialized_without_mutation():
    plan = _plan(
        [
            _task(
                "task-rpo",
                title="Define RPO | RTO",
                description="Set recovery objectives for the ledger service.",
                metadata={
                    "recovery_capabilities": ["rpo_rto"],
                    "owner_hints": ["ledger owner", "SRE | platform"],
                    "validation": "Restore validation rehearsal completed.",
                    "objectives": "RPO 5 minutes and RTO 30 minutes.",
                },
            )
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_plan_disaster_recovery_readiness(model)
    payload = plan_disaster_recovery_readiness_matrix_to_dict(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "summary"]
    assert list(payload["rows"][0]) == [
        "recovery_capability",
        "covered_task_ids",
        "readiness_gaps",
        "required_rehearsal_artifacts",
        "owner_hints",
        "severity",
        "evidence",
        "follow_up_questions",
    ]
    row = {row.recovery_capability: row for row in result.rows}["rpo_rto"]
    assert isinstance(row, PlanDisasterRecoveryReadinessRow)
    assert row.recovery_capability == "rpo_rto"
    assert row.readiness_gaps == ()
    assert row.owner_hints[:2] == ("ledger owner", "SRE | platform")


def test_no_signal_plan_returns_empty_matrix_with_deterministic_summary():
    result = build_plan_disaster_recovery_readiness_matrix(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Adjust labels and helper text.",
                    files_or_modules=["docs/settings-copy.md"],
                )
            ]
        )
    )

    assert result.plan_id == "plan-dr"
    assert result.rows == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "recovery_capability_count": 0,
        "covered_task_count": 0,
        "follow_up_question_count": 0,
        "gap_count": 0,
        "severity_counts": {"high": 0, "medium": 0, "low": 0},
        "capability_counts": {
            "backup": 0,
            "restore": 0,
            "failover": 0,
            "replica": 0,
            "rpo_rto": 0,
            "regional_outage": 0,
            "incident_response": 0,
            "data_recovery": 0,
            "manual_recovery": 0,
        },
    }
    assert result.to_markdown() == (
        "# Plan Disaster Recovery Readiness Matrix: plan-dr\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Recovery capability count: 0\n"
        "- Covered task count: 0\n"
        "- Follow-up question count: 0\n"
        "- Severity counts: high 0, medium 0, low 0\n"
        "\n"
        "No disaster recovery readiness signals were detected."
    )


def test_markdown_renderer_escapes_pipes_stably():
    result = build_plan_disaster_recovery_readiness_matrix(
        _plan(
            [
                _task(
                    "task-pipe",
                    title="Manual recovery | ledger",
                    description="Manual recovery runbook includes RPO, RTO, and restore validation.",
                    metadata={"owner": "ops | ledger"},
                )
            ]
        )
    )

    markdown = plan_disaster_recovery_readiness_matrix_to_markdown(result)

    assert markdown == result.to_markdown()
    assert "ops \\| ledger" in markdown
    assert "Manual recovery \\| ledger" in markdown


def _plan(tasks, *, metadata=None):
    return {
        "id": "plan-dr",
        "implementation_brief_id": "brief-dr",
        "milestones": [],
        "tasks": tasks,
        "metadata": metadata or {},
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    risk_level=None,
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
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
