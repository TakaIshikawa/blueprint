from blueprint.task_tenant_migration_readiness import (
    analyze_task_tenant_migration_readiness,
    task_tenant_migration_readiness_plan_to_dict,
    task_tenant_migration_readiness_plan_to_markdown,
)


def test_ready_tenant_migration_detects_text_metadata_notes_paths_and_commands():
    plan = analyze_task_tenant_migration_readiness(
        {
            "id": "plan-tenant-move",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Tenant migration workspace move and account transfer",
                    "description": (
                        "Migrate tenant accounts between plans and systems with workspace move, "
                        "shard relocation, region move, cutover cohort, and tenant rollback."
                    ),
                    "notes": ["No-downtime plan uses dual write and a read-only freeze window."],
                    "metadata": {
                        "owner": "Migration owner is platform on-call.",
                        "selection": "Tenant selection uses affected tenants, exclusions, and pilot cohort.",
                        "communication": "Customer notice, workspace admin notification, support runbook, and status page are prepared.",
                        "verification": "Post-migration verification includes success criteria and a migration report.",
                    },
                    "files_or_modules": ["src/migrations/tenant_shard_relocation/cutover_cohort.py"],
                    "validation_commands": ["poetry run tenant-move validate --checksums --smoke-test"],
                }
            ],
        }
    )

    record = plan.records[0]
    assert plan.impacted_task_ids == ("task-ready",)
    assert record.detected_signals == (
        "tenant_migration",
        "workspace_move",
        "account_transfer",
        "shard_relocation",
        "region_move",
        "cutover_cohort",
        "tenant_rollback",
    )
    assert record.present_criteria == (
        "owner",
        "tenant_selection",
        "validation",
        "downtime_plan",
        "rollback_path",
        "communication",
        "post_migration_verification",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"
    assert any("metadata.owner:" in item for item in record.evidence)
    assert any("notes[0]:" in item for item in record.evidence)
    assert any("files_or_modules: src/migrations/tenant_shard_relocation/cutover_cohort.py" in item for item in record.evidence)
    assert any("validation_commands[0]:" in item for item in record.evidence)


def test_partial_tenant_migration_returns_ordered_gaps():
    plan = analyze_task_tenant_migration_readiness(
        [
            {
                "id": "task-partial",
                "title": "Move workspace to EU region",
                "description": "Workspace move for affected tenants has tenant scope and validation by reconciliation.",
            }
        ]
    )

    record = plan.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("tenant_selection", "validation")
    assert record.missing_criteria == (
        "owner",
        "downtime_plan",
        "rollback_path",
        "communication",
        "post_migration_verification",
    )
    assert record.recommended_follow_up_actions[0].startswith("Name the owner")


def test_sparse_tenant_migration_needs_planning():
    plan = analyze_task_tenant_migration_readiness(
        [{"id": "task-sparse", "title": "Account transfer", "description": "Transfer accounts to a new shard."}]
    )

    assert plan.records[0].readiness == "needs_planning"
    assert len(plan.records[0].missing_criteria) == 7


def test_unrelated_task_is_ignored():
    plan = analyze_task_tenant_migration_readiness(
        [{"id": "task-copy", "title": "Update settings copy", "description": "Polish labels."}]
    )

    assert plan.records == ()
    assert plan.ignored_task_ids == ("task-copy",)


def test_serialization_and_markdown_are_deterministic():
    plan = analyze_task_tenant_migration_readiness(
        [{"id": "task-move", "title": "Tenant migration", "description": "Tenant migration has owner and rollback."}]
    )

    payload = task_tenant_migration_readiness_plan_to_dict(plan)
    markdown = task_tenant_migration_readiness_plan_to_markdown(plan)

    assert list(payload) == ["plan_id", "records", "findings", "recommendations", "impacted_task_ids", "ignored_task_ids", "summary"]
    assert payload["summary"]["missing_criterion_count"] == 5
    assert markdown == plan.to_markdown()
    assert "# Task Tenant Migration Readiness" in markdown
