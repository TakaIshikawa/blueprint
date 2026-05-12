from blueprint.task_audit_log_retention_readiness import (
    analyze_task_audit_log_retention_readiness,
    task_audit_log_retention_readiness_plan_to_dict,
    task_audit_log_retention_readiness_plan_to_markdown,
)


def test_ready_audit_log_retention_task_detects_text_and_path_signals():
    plan = analyze_task_audit_log_retention_readiness(
        {
            "id": "plan-audit",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Implement audit log retention archive and purge",
                    "description": (
                        "Audit logs retain for seven years, archive target is an immutable S3 bucket, "
                        "and a retention worker performs batched purge mechanics. Legal hold exceptions "
                        "skip compliance hold records. RBAC access controls gate purge and export. "
                        "Evidence export produces a compliance CSV with actor details. Monitoring dashboard "
                        "and alerts cover archive failures and purge lag. Integration tests cover the flow."
                    ),
                    "files_or_modules": ["src/audit_log_retention/archive_purge_worker.py"],
                }
            ],
        }
    )

    record = plan.records[0]
    assert plan.impacted_task_ids == ("task-ready",)
    assert "audit_log_retention" in record.detected_signals
    assert "audit_retention_path" in record.detected_signals
    assert record.present_criteria == (
        "retention_period",
        "storage_archive_target",
        "purge_mechanics",
        "legal_hold_exceptions",
        "access_controls",
        "evidence_export",
        "monitoring",
        "tests",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_partial_audit_log_retention_task_returns_actionable_followups():
    plan = analyze_task_audit_log_retention_readiness(
        [
            {
                "id": "task-partial",
                "title": "Archive audit logs after retention period",
                "description": "Audit log retention period is 365 days and archive target is object storage.",
            }
        ]
    )

    record = plan.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("retention_period", "storage_archive_target")
    assert record.missing_criteria == (
        "purge_mechanics",
        "legal_hold_exceptions",
        "access_controls",
        "evidence_export",
        "monitoring",
        "tests",
    )
    assert "Describe purge mechanics" in record.recommended_follow_up_actions[0]


def test_audit_retention_signal_without_criteria_needs_planning():
    plan = analyze_task_audit_log_retention_readiness(
        [{"id": "task-sparse", "title": "Archive audit logs", "description": "Prepare compliance archive work."}]
    )

    assert plan.records[0].readiness == "needs_planning"
    assert len(plan.records[0].missing_criteria) == 8


def test_generic_audit_logging_and_observability_without_retention_are_ignored():
    plan = analyze_task_audit_log_retention_readiness(
        {
            "tasks": [
                {"id": "task-audit", "title": "Add audit logging", "description": "Emit audit logs for account updates."},
                {"id": "task-obs", "title": "Add observability", "description": "Create metrics and alerts for request logs."},
            ]
        }
    )

    assert plan.records == ()
    assert plan.ignored_task_ids == ("task-audit", "task-obs")


def test_serialization_and_markdown_are_deterministic():
    plan = analyze_task_audit_log_retention_readiness(
        [{"id": "task-audit", "title": "Purge audit logs", "description": "Audit log retention period exists."}]
    )

    payload = task_audit_log_retention_readiness_plan_to_dict(plan)
    markdown = task_audit_log_retention_readiness_plan_to_markdown(plan)

    assert list(payload) == ["plan_id", "records", "findings", "recommendations", "impacted_task_ids", "ignored_task_ids", "summary"]
    assert payload["summary"]["missing_criterion_count"] == 7
    assert markdown == plan.to_markdown()
    assert "# Task Audit Log Retention Readiness" in markdown
