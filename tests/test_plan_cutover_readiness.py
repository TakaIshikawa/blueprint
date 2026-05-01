import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_cutover_readiness import (
    PlanCutoverReadinessMatrix,
    PlanCutoverReadinessRow,
    build_plan_cutover_readiness_matrix,
    plan_cutover_readiness_matrix_to_dict,
    plan_cutover_readiness_matrix_to_markdown,
    summarize_plan_cutover_readiness,
)


def test_cutover_sensitive_tasks_are_ordered_deterministically():
    result = build_plan_cutover_readiness_matrix(
        _plan(
            [
                _task(
                    "task-backfill",
                    title="Backfill historical invoices",
                    description="Run a backfill for missing invoice rows.",
                    files_or_modules=["src/blueprint/backfills/invoices.py"],
                ),
                _task(
                    "task-dns",
                    title="DNS cutover for public API",
                    description="Switch Cloudflare CNAME records during the approved window.",
                    files_or_modules=["infra/dns/public-api.tf"],
                    acceptance_criteria=[
                        "Runbook, owner, validation health check, and rollback records are ready."
                    ],
                ),
                _task(
                    "task-traffic",
                    title="Shift traffic to new checkout service",
                    description="Canary the service and ramp up weighted routing.",
                    files_or_modules=["deploy/traffic/checkout.yaml"],
                    acceptance_criteria=["Rollback to zero percent traffic if errors increase."],
                ),
            ]
        )
    )

    assert isinstance(result, PlanCutoverReadinessMatrix)
    assert [row.task_id for row in result.rows] == [
        "task-backfill",
        "task-traffic",
        "task-dns",
    ]
    assert [row.cutover_type for row in result.rows] == [
        "backfill",
        "traffic_shift",
        "dns",
    ]
    assert result.rows[0].readiness_status == "needs_rollback"
    assert result.rows[1].readiness_status == "needs_validation"
    assert result.rows[2].readiness_status == "ready"
    assert result.summary["cutover_task_count"] == 3
    assert result.summary["status_counts"] == {
        "needs_rollback": 1,
        "needs_validation": 1,
        "needs_prerequisites": 0,
        "ready": 1,
    }


def test_multiple_cutover_signal_types_and_file_paths_are_detected():
    result = build_plan_cutover_readiness_matrix(
        _plan(
            [
                _task(
                    "task-migration",
                    title="Apply account schema migration",
                    description="Migrate account status values.",
                    files_or_modules=["migrations/versions/20260501_account_status.sql"],
                ),
                _task(
                    "task-dual-write",
                    title="Enable dual-write for profile store",
                    description="Write-both old and new profile records behind a feature flag.",
                    files_or_modules=["src/blueprint/services/profile_dual_write.py"],
                ),
                _task(
                    "task-queue",
                    title="Drain queue before worker switchover",
                    description="Drain backlog from the import queue before switching consumers.",
                    files_or_modules=["src/blueprint/queues/import_drain.py"],
                ),
                _task(
                    "task-rollout",
                    title="Production rollout of billing API",
                    description="Prod deploy activates the new endpoint.",
                    files_or_modules=["deployments/billing.yaml"],
                ),
            ]
        )
    )

    by_id = {row.task_id: row for row in result.rows}

    assert by_id["task-migration"].cutover_type == "migration"
    assert by_id["task-dual-write"].cutover_type == "dual_write"
    assert by_id["task-queue"].cutover_type == "queue_drain"
    assert by_id["task-rollout"].cutover_type == "production_rollout"
    assert "database owner" in by_id["task-migration"].owner_hints
    assert "release owner" in by_id["task-rollout"].owner_hints
    assert any("files_or_modules:" in item for item in by_id["task-queue"].evidence)


def test_metadata_overrides_are_extracted_and_serialized_without_mutation():
    plan = _plan(
        [
            _task(
                "task-meta",
                title="Traffic shift for search | API",
                description="Shift traffic to search v2.",
                metadata={
                    "prerequisites": ["Approval from search lead", "Capacity check complete"],
                    "validation_checkpoints": ["p95 latency below 200ms"],
                    "rollback_checkpoint": "Return traffic to v1 at 25 percent error budget burn.",
                    "owner_hints": ["search lead", "SRE"],
                    "readiness_status": "ready",
                    "cutover_note": "traffic shift evidence",
                },
            )
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_plan_cutover_readiness(model)
    payload = plan_cutover_readiness_matrix_to_dict(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "summary"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "cutover_type",
        "prerequisites",
        "validation_checkpoints",
        "rollback_checkpoint",
        "owner_hints",
        "readiness_status",
        "evidence",
    ]
    row = result.rows[0]
    assert isinstance(row, PlanCutoverReadinessRow)
    assert row.cutover_type == "traffic_shift"
    assert row.prerequisites[:2] == ("Approval from search lead", "Capacity check complete")
    assert row.validation_checkpoints[0] == "p95 latency below 200ms"
    assert row.rollback_checkpoint == "Return traffic to v1 at 25 percent error budget burn."
    assert row.owner_hints[:2] == ("search lead", "SRE")
    assert row.readiness_status == "ready"


def test_non_cutover_plan_returns_empty_valid_matrix_with_counts():
    result = build_plan_cutover_readiness_matrix(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings page copy",
                    description="Adjust labels and helper text.",
                    files_or_modules=["docs/settings-copy.md"],
                )
            ]
        )
    )

    assert result.plan_id == "plan-cutover"
    assert result.rows == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "cutover_task_count": 0,
        "ready_count": 0,
        "not_ready_count": 0,
        "status_counts": {
            "needs_rollback": 0,
            "needs_validation": 0,
            "needs_prerequisites": 0,
            "ready": 0,
        },
        "type_counts": {
            "traffic_shift": 0,
            "dns": 0,
            "dual_write": 0,
            "dual_read": 0,
            "feature_flag_activation": 0,
            "queue_drain": 0,
            "migration": 0,
            "backfill": 0,
            "production_rollout": 0,
            "switchover": 0,
            "cutover": 0,
        },
    }
    assert result.to_markdown() == (
        "# Plan Cutover Readiness Matrix: plan-cutover\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Cutover task count: 0\n"
        "- Ready count: 0\n"
        "- Not ready count: 0\n"
        "\n"
        "No cutover-sensitive tasks were detected."
    )


def test_markdown_renderer_escapes_pipes_stably():
    result = build_plan_cutover_readiness_matrix(
        _plan(
            [
                _task(
                    "task-pipe",
                    title="Feature flag activation for search | recommendations",
                    description="Enable flag for search users.",
                    metadata={
                        "validation_checkpoints": ["metric a | metric b is healthy"],
                        "rollback_checkpoint": "Disable flag | restore default cohort.",
                        "owner": "growth | search",
                    },
                )
            ]
        )
    )

    markdown = plan_cutover_readiness_matrix_to_markdown(result)

    assert markdown == result.to_markdown()
    assert "search \\| recommendations" in markdown
    assert "metric a \\| metric b is healthy" in markdown
    assert "Disable flag \\| restore default cohort." in markdown
    assert "growth \\| search" in markdown


def _plan(tasks):
    return {
        "id": "plan-cutover",
        "implementation_brief_id": "brief-cutover",
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
