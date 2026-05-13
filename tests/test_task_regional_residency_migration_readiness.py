from blueprint.task_regional_residency_migration_readiness import (
    analyze_task_regional_residency_migration_readiness,
    task_regional_residency_migration_readiness_plan_to_dict,
    task_regional_residency_migration_readiness_plan_to_markdown,
)


def test_ready_regional_residency_migration_detects_text_and_path_signals():
    plan = analyze_task_regional_residency_migration_readiness(
        {
            "id": "plan-region-move",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Move tenant customer data for regional residency migration",
                    "description": (
                        "Migrate tenant data from source region US to target region EU to enforce data "
                        "residency. Tenant selection uses eligible tenant cohorts. Data classes include PII, "
                        "attachments, backups, and audit logs. Migration sequence runs in waves with a "
                        "pre-cutover freeze window. Validation uses row counts, checksums, reconciliation, "
                        "and residency tests. Rollback restores the source region. Customer communication "
                        "includes admin notice, support runbook, and release notes. Compliance evidence "
                        "produces an audit trail and migration report."
                    ),
                    "files_or_modules": ["src/migrations/regional_residency/us_to_eu_tenant_relocation.py"],
                }
            ],
        }
    )

    record = plan.records[0]
    assert plan.impacted_task_ids == ("task-ready",)
    assert record.detected_signals == (
        "regional_residency_migration",
        "tenant_data_relocation",
        "source_target_region",
        "residency_enforcement",
    )
    assert record.present_criteria == (
        "region_mapping",
        "tenant_scope",
        "data_classes",
        "migration_sequence",
        "validation",
        "rollback",
        "customer_communication",
        "audit_evidence",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"
    assert any("files_or_modules: src/migrations/regional_residency/us_to_eu_tenant_relocation.py" in item for item in record.evidence)


def test_partial_regional_residency_migration_returns_actionable_followups():
    plan = analyze_task_regional_residency_migration_readiness(
        [
            {
                "id": "task-partial",
                "title": "Relocate customer data from EU to Canada",
                "description": (
                    "Move customer data from source region EU to target region Canada. "
                    "Affected tenants are selected by customer scope and validation uses reconciliation."
                ),
            }
        ]
    )

    record = plan.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("region_mapping", "tenant_scope", "data_classes", "validation")
    assert record.missing_criteria == (
        "migration_sequence",
        "rollback",
        "customer_communication",
        "audit_evidence",
    )
    assert "Specify migration sequencing" in record.recommended_follow_up_actions[0]


def test_residency_relocation_signal_without_criteria_needs_planning():
    plan = analyze_task_regional_residency_migration_readiness(
        [
            {
                "id": "task-sparse",
                "title": "Enforce tenant region lock",
                "description": "Prepare regional residency relocation for selected customers.",
            }
        ]
    )

    assert plan.records[0].readiness == "needs_planning"
    assert len(plan.records[0].missing_criteria) == 8


def test_generic_multi_region_failover_is_ignored_without_residency_or_relocation():
    plan = analyze_task_regional_residency_migration_readiness(
        {
            "tasks": [
                {
                    "id": "task-failover",
                    "title": "Add multi-region failover",
                    "description": "Route traffic to a passive region during outages and validate health checks.",
                    "files_or_modules": ["infra/multi_region/failover.tf"],
                },
                {
                    "id": "task-dr",
                    "title": "Replicate service metrics across regions",
                    "description": "Improve regional failover dashboards without moving customer data.",
                },
            ]
        }
    )

    assert plan.records == ()
    assert plan.ignored_task_ids == ("task-failover", "task-dr")


def test_multi_region_task_is_detected_when_it_includes_residency_relocation():
    plan = analyze_task_regional_residency_migration_readiness(
        [
            {
                "id": "task-residency-failover",
                "title": "Multi-region residency relocation",
                "description": (
                    "During the multi-region program, relocate tenant data into the EU residency region. "
                    "Region mapping lists source region US and target region EU with rollback and audit evidence."
                ),
            }
        ]
    )

    record = plan.records[0]
    assert "tenant_data_relocation" in record.detected_signals
    assert record.present_criteria == ("region_mapping", "data_classes", "rollback", "audit_evidence")


def test_serialization_and_markdown_are_deterministic():
    plan = analyze_task_regional_residency_migration_readiness(
        [{"id": "task-move", "title": "Move tenant data to EU", "description": "Regional residency migration needs a source region and target region."}]
    )

    payload = task_regional_residency_migration_readiness_plan_to_dict(plan)
    markdown = task_regional_residency_migration_readiness_plan_to_markdown(plan)

    assert list(payload) == ["plan_id", "records", "findings", "recommendations", "impacted_task_ids", "ignored_task_ids", "summary"]
    assert payload["summary"]["missing_criterion_count"] == 6
    assert markdown == plan.to_markdown()
    assert "# Task Regional Residency Migration Readiness" in markdown
