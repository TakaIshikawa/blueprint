import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_rollback_rehearsal import (
    RollbackRehearsalChecklist,
    RollbackRehearsalChecklistItem,
    build_plan_rollback_rehearsal_checklist,
    plan_rollback_rehearsal_checklist_to_dict,
    plan_rollback_rehearsal_checklist_to_markdown,
    summarize_plan_rollback_rehearsal,
)


def test_high_risk_migrations_are_grouped_into_rehearsal_items():
    result = build_plan_rollback_rehearsal_checklist(
        _plan(
            [
                _task(
                    "task-schema",
                    title="Apply production account schema migration",
                    description="Migrate account status values in production.",
                    files_or_modules=["migrations/versions/20260501_account_status.sql"],
                    risk_level="high",
                    acceptance_criteria=[
                        "Dry-run command: poetry run alembic downgrade -1 --sql",
                        "Rollback validation proves application smoke checks pass.",
                    ],
                ),
                _task(
                    "task-index",
                    title="Add production invoice index migration",
                    description="Run a DDL migration for invoice lookup in prod.",
                    files_or_modules=["schema/invoices/add_lookup_index.sql"],
                    risk_level="critical",
                ),
            ]
        )
    )

    assert isinstance(result, RollbackRehearsalChecklist)
    assert len(result.items) == 1
    item = result.items[0]
    assert isinstance(item, RollbackRehearsalChecklistItem)
    assert item.rehearsal_type == "migration"
    assert item.rehearsal_scope == "Rehearse rollback for 2 migration tasks"
    assert item.linked_task_ids == ("task-index", "task-schema")
    assert any("Database backup" in value for value in item.preconditions)
    assert item.dry_run_command_hints == (
        "acceptance_criteria: Dry-run command: poetry run alembic downgrade -1 --sql",
    )
    assert (
        "acceptance_criteria: Rollback validation proves application smoke checks pass."
        in item.validation_evidence
    )
    assert any("Abort production execution" in value for value in item.abort_criteria)
    assert result.rehearsal_task_ids == ("task-index", "task-schema")
    assert result.summary["type_counts"]["migration"] == 2


def test_feature_flag_and_infra_rollbacks_have_distinct_rehearsal_items():
    result = build_plan_rollback_rehearsal_checklist(
        _plan(
            [
                _task(
                    "task-flag",
                    title="Enable checkout feature flag in production",
                    description="Activate the LaunchDarkly flag for checkout users.",
                    files_or_modules=["src/blueprint/feature_flags/checkout.py"],
                    acceptance_criteria=["Validate rollback by disabling the flag for the cohort."],
                ),
                _task(
                    "task-infra",
                    title="Apply production Terraform for queue workers",
                    description="Update infrastructure capacity for live workers.",
                    files_or_modules=["infra/terraform/workers.tf"],
                    metadata={
                        "dry_run_commands": ["terraform plan -out rollback-rehearsal.tfplan"],
                        "abort_criteria": [
                            "Abort if terraform plan cannot restore prior worker count."
                        ],
                    },
                ),
            ]
        )
    )

    by_type = {item.rehearsal_type: item for item in result.items}

    assert [item.rehearsal_type for item in result.items] == ["feature_flag", "infrastructure"]
    assert by_type["feature_flag"].linked_task_ids == ("task-flag",)
    assert any(
        "disabled-flag behavior" in value for value in by_type["feature_flag"].validation_evidence
    )
    assert (
        "acceptance_criteria: Validate rollback by disabling the flag for the cohort."
        in by_type["feature_flag"].validation_evidence
    )
    assert by_type["infrastructure"].dry_run_command_hints == (
        "terraform plan -out rollback-rehearsal.tfplan",
        "metadata.dry_run_commands: terraform plan -out rollback-rehearsal.tfplan",
    )
    assert by_type["infrastructure"].abort_criteria[0] == (
        "Abort if terraform plan cannot restore prior worker count."
    )


def test_queue_external_service_and_existing_rehearsal_metadata_are_used_as_evidence():
    plan = _plan(
        [
            _task(
                "task-queue",
                title="Drain production import queue",
                description="Drain backlog before worker rollback.",
                files_or_modules=["src/blueprint/queues/import_drain.py"],
                metadata={
                    "preconditions": ["Import producers can be paused in staging."],
                    "rehearsal_command": "poetry run worker-drain --dry-run",
                    "rollback_validation_evidence": [
                        "Staging rehearsal verifies rollback restores queue consumers."
                    ],
                },
            ),
            _task(
                "task-vendor",
                title="Switch production billing webhook vendor",
                description="Update third-party integration and keep fallback route available.",
                files_or_modules=["src/blueprint/integrations/billing_webhook.py"],
                acceptance_criteria=[
                    "Run rehearsal in vendor sandbox before production.",
                    "Rollback validation confirms old webhook endpoint receives events.",
                ],
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_plan_rollback_rehearsal(model)
    payload = plan_rollback_rehearsal_checklist_to_dict(result)
    by_type = {item.rehearsal_type: item for item in result.items}

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["items"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "items", "rehearsal_task_ids", "summary"]
    assert list(payload["items"][0]) == [
        "rehearsal_type",
        "rehearsal_scope",
        "linked_task_ids",
        "preconditions",
        "dry_run_command_hints",
        "validation_evidence",
        "abort_criteria",
        "evidence",
    ]
    assert by_type["queue"].preconditions[0] == "Import producers can be paused in staging."
    assert by_type["queue"].dry_run_command_hints == (
        "poetry run worker-drain --dry-run",
        "metadata.rehearsal_command: poetry run worker-drain --dry-run",
    )
    assert (
        "Staging rehearsal verifies rollback restores queue consumers."
        in by_type["queue"].validation_evidence
    )
    assert by_type["external_service"].linked_task_ids == ("task-vendor",)
    assert (
        "acceptance_criteria: Run rehearsal in vendor sandbox before production."
        in by_type["external_service"].validation_evidence
    )


def test_no_candidate_plan_returns_stable_empty_checklist():
    result = build_plan_rollback_rehearsal_checklist(
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

    assert result.plan_id == "plan-rollback-rehearsal"
    assert result.items == ()
    assert result.rehearsal_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "rehearsal_task_count": 0,
        "checklist_item_count": 0,
        "type_counts": {
            "deployment": 0,
            "migration": 0,
            "data_movement": 0,
            "feature_flag": 0,
            "queue": 0,
            "infrastructure": 0,
            "external_service": 0,
        },
    }
    assert result.to_markdown() == (
        "# Rollback Rehearsal Checklist: plan-rollback-rehearsal\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Rehearsal task count: 0\n"
        "- Checklist item count: 0\n"
        "\n"
        "No rollback rehearsal candidates were detected."
    )


def test_markdown_renderer_escapes_pipes_stably():
    result = build_plan_rollback_rehearsal_checklist(
        _plan(
            [
                _task(
                    "task-pipe",
                    title="Enable production feature flag for search | recommendations",
                    description="Rollback by disabling the flag.",
                    metadata={
                        "dry_run_command": "flagctl disable search | cat",
                        "validation_evidence": "metric a | metric b rollback validation passes",
                        "abort_criteria": "Abort on error | latency regression.",
                    },
                )
            ]
        )
    )

    markdown = plan_rollback_rehearsal_checklist_to_markdown(result)

    assert markdown == result.to_markdown()
    assert "search \\| recommendations" in markdown
    assert "flagctl disable search \\| cat" in markdown
    assert "metric a \\| metric b rollback validation passes" in markdown
    assert "Abort on error \\| latency regression." in markdown


def _plan(tasks):
    return {
        "id": "plan-rollback-rehearsal",
        "implementation_brief_id": "brief-rollback-rehearsal",
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
