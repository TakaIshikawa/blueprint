import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_destructive_action_safeguards import (
    TaskDestructiveActionSafeguardPlan,
    TaskDestructiveActionSafeguardRecord,
    build_task_destructive_action_safeguard_plan,
    summarize_task_destructive_action_safeguards,
    task_destructive_action_safeguard_plan_to_dict,
    task_destructive_action_safeguard_plan_to_markdown,
)


def test_detects_destructive_keywords_across_task_fields_and_reports_missing_criteria():
    result = build_task_destructive_action_safeguard_plan(
        _task(
            "task-destructive",
            title="Delete dormant user accounts",
            description="Purge customer records and revoke access tokens for inactive production users.",
            files_or_modules=["src/jobs/purge_user_data.py"],
            acceptance_criteria=[
                "Dry-run previews affected users before mutation.",
                "Audit log records operator, scope, timestamp, and outcome.",
            ],
            metadata={"operation": "destructive purge", "scope": "production user data"},
            tags=["destructive", "user-data"],
        )
    )

    assert isinstance(result, TaskDestructiveActionSafeguardPlan)
    assert result.destructive_action_task_ids == ("task-destructive",)
    record = result.records[0]
    assert isinstance(record, TaskDestructiveActionSafeguardRecord)
    assert record.risk_level == "high"
    assert record.destructive_actions == ("delete", "purge", "revoke")
    assert record.safeguards == (
        "confirmation_gate",
        "dry_run",
        "audit_log",
        "backup_restore",
        "scoped_rollout",
        "operator_permissions",
    )
    assert record.missing_acceptance_criteria == (
        "Acceptance criteria require an explicit confirmation gate before destructive execution.",
        "Acceptance criteria require verified backup, restore, rollback, or recovery coverage.",
        "Acceptance criteria require scoped rollout controls such as canary, batch, or tenant limits.",
        "Acceptance criteria require privileged operator permissions or approval checks.",
    )
    assert "title: Delete dormant user accounts" in record.evidence
    assert "files_or_modules: src/jobs/purge_user_data.py" in record.evidence
    assert any(item.startswith("metadata.operation:") for item in record.evidence)
    assert "tags[0]: destructive" in record.evidence


def test_bulk_production_updates_are_classified_as_high_risk_even_with_controls():
    result = build_task_destructive_action_safeguard_plan(
        _plan(
            [
                _task(
                    "task-bulk-prod",
                    title="Bulk update production customer data",
                    description="Batch update all accounts and overwrite existing billing records in production.",
                    acceptance_criteria=[
                        "Manual approval and confirmation gate are required.",
                        "Dry run previews affected rows.",
                        "Audit log captures every change.",
                        "Backup snapshot and restore path are verified.",
                        "Canary batches limit rollout scope.",
                        "Admin-only operator permissions are enforced.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]

    assert record.risk_level == "high"
    assert record.destructive_actions == ("overwrite", "bulk_update")
    assert record.missing_acceptance_criteria == ()
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}
    assert result.summary["action_counts"]["bulk_update"] == 1


def test_structured_metadata_keys_and_tags_detect_truncate_archive_disable_actions():
    result = summarize_task_destructive_action_safeguards(
        [
            _task(
                "task-truncate",
                title="Clean expired sessions",
                description="Maintenance job for session table.",
                metadata={"truncate": "session records", "environment": "production"},
                tags=["database"],
            ),
            _task(
                "task-archive",
                title="Archive old account records",
                description="Archive customer records after retention window.",
                files_or_modules=["src/workers/archive_account_data.py"],
            ),
            _task(
                "task-disable",
                title="Disable compromised user permissions",
                description="Disable access roles for affected production users.",
                metadata={"safeguards": {"approval": "Operator approval is required."}},
            ),
        ]
    )

    by_id = {record.task_id: record for record in result.records}

    assert result.destructive_action_task_ids == ("task-archive", "task-disable", "task-truncate")
    assert by_id["task-truncate"].risk_level == "high"
    assert by_id["task-truncate"].destructive_actions == ("truncate",)
    assert by_id["task-archive"].destructive_actions == ("archive",)
    assert by_id["task-disable"].destructive_actions == ("disable",)
    assert any(item.startswith("metadata.truncate:") for item in by_id["task-truncate"].evidence)
    assert "files_or_modules: src/workers/archive_account_data.py" in by_id["task-archive"].evidence


def test_model_inputs_match_mapping_inputs_without_mutation_and_serialize():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Revoke production account access",
                description="Remove access permissions for user accounts flagged by security.",
                acceptance_criteria=["Audit trail is written before and after revoke."],
            )
        ],
        plan_id="plan-destructive-model",
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)
    task_model = ExecutionTask.model_validate(plan["tasks"][0])

    mapping_result = build_task_destructive_action_safeguard_plan(plan)
    model_result = build_task_destructive_action_safeguard_plan(model)
    task_result = build_task_destructive_action_safeguard_plan(task_model)
    payload = task_destructive_action_safeguard_plan_to_dict(model_result)
    markdown = task_destructive_action_safeguard_plan_to_markdown(model_result)

    assert plan == original
    assert payload == task_destructive_action_safeguard_plan_to_dict(mapping_result)
    assert task_result.records[0].task_id == "task-model"
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "destructive_action_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "risk_level",
        "destructive_actions",
        "safeguards",
        "missing_acceptance_criteria",
        "evidence",
    ]
    assert markdown.startswith("# Task Destructive Action Safeguard Plan: plan-destructive-model")


def test_read_only_cosmetic_and_malformed_inputs_are_ignored_with_complete_summary():
    result = build_task_destructive_action_safeguard_plan(
        _plan(
            [
                _task(
                    "task-read",
                    title="Read-only report for deleted users",
                    description="SELECT query lists archived accounts in an analytics view.",
                    files_or_modules=["src/ui/deleted_users_report.py"],
                ),
                _task(
                    "task-ui",
                    title="Update disabled button copy",
                    description="Cosmetic CSS and copy change for disabled controls.",
                    tags=["copy"],
                ),
            ]
        )
    )

    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 2,
        "destructive_action_task_count": 0,
        "missing_acceptance_criteria_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "action_counts": {
            "delete": 0,
            "purge": 0,
            "overwrite": 0,
            "revoke": 0,
            "truncate": 0,
            "archive": 0,
            "disable": 0,
            "bulk_update": 0,
        },
    }
    assert build_task_destructive_action_safeguard_plan({"tasks": "not a list"}).records == ()
    assert build_task_destructive_action_safeguard_plan("not a plan").records == ()
    assert build_task_destructive_action_safeguard_plan(None).records == ()


def _plan(tasks, *, plan_id="plan-destructive-actions"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-destructive-actions",
        "milestones": [{"name": "Destructive Actions"}],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": description,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": (
            ["Behavior is validated."]
            if acceptance_criteria is None
            else acceptance_criteria
        ),
        "metadata": {} if metadata is None else metadata,
    }
    if tags is not None:
        task["tags"] = tags
    return task
