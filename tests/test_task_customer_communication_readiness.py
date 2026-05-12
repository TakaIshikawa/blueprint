import json

from blueprint.task_customer_communication_readiness import (
    TaskCustomerCommunicationReadinessPlan,
    analyze_task_customer_communication_readiness,
    build_task_customer_communication_readiness_plan,
    derive_task_customer_communication_readiness,
    extract_task_customer_communication_readiness,
    generate_task_customer_communication_readiness,
    recommend_task_customer_communication_readiness,
    summarize_task_customer_communication_readiness,
    task_customer_communication_readiness_plan_to_dict,
    task_customer_communication_readiness_plan_to_dicts,
    task_customer_communication_readiness_plan_to_markdown,
)


def test_complete_customer_communication_task_is_ready():
    result = build_task_customer_communication_readiness_plan(
        _plan(
            [
                _task(
                    "customer-comms-ready",
                    "Plan migration customer notice",
                    (
                        "Prepare customer communication and migration notice for affected customers. "
                        "Audience segmentation covers enterprise and self-serve customer cohorts. "
                        "Message timing includes a 14-day advance notice and scheduled send time. "
                        "Comms owner approval and legal sign-off are required. Channel coverage includes "
                        "email, in-app announcement, status page, and support bulletin. Rollback messaging "
                        "and follow-up update messaging are drafted if delayed."
                    ),
                    ["comms/customer_notice/migration_email.md"],
                )
            ]
        )
    )

    assert isinstance(result, TaskCustomerCommunicationReadinessPlan)
    record = result.records[0]
    assert record.readiness == "ready"
    assert record.detected_signals == (
        "customer_notice",
        "migration_notice",
        "status_page_update",
        "support_bulletin",
        "in_app_announcement",
    )
    assert record.present_criteria == (
        "audience_segmentation",
        "message_timing",
        "owner_approval",
        "channel_coverage",
        "rollback_update_messaging",
    )
    assert record.missing_criteria == ()
    assert result.summary["missing_criterion_count"] == 0


def test_detects_customer_facing_notice_status_release_and_support_bulletin_tasks():
    result = analyze_task_customer_communication_readiness(
        _plan(
            [
                _task(
                    "notice",
                    "Customer-facing notice",
                    "Notify customers about account changes with affected customers listed.",
                    ["comms/customer-notification/account_change.md"],
                ),
                _task(
                    "status",
                    "Status page maintenance update",
                    "Post status page maintenance status update with send time and support channel coverage.",
                    ["status_page/maintenance.md"],
                ),
                _task(
                    "release",
                    "Release communication",
                    "Prepare release communication for launch announcement and approval owner.",
                    ["release_communication/launch.md"],
                ),
                _task(
                    "support",
                    "Support bulletin",
                    "Publish support bulletin with rollback communication for support teams.",
                    ["support/support_bulletin.md"],
                ),
                _task(
                    "docs",
                    "Product documentation",
                    "Update product documentation for the settings page.",
                    ["docs/product/settings.md"],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert set(result.impacted_task_ids) == {"notice", "status", "release", "support"}
    assert result.ignored_task_ids == ("docs",)
    assert "customer_notice" in by_id["notice"].detected_signals
    assert "status_page_update" in by_id["status"].detected_signals
    assert "release_communication" in by_id["release"].detected_signals
    assert "support_bulletin" in by_id["support"].detected_signals
    assert by_id["notice"].readiness == "partial"
    assert by_id["support"].readiness == "partial"


def test_product_documentation_only_and_no_impact_tasks_are_ignored():
    result = build_task_customer_communication_readiness_plan(
        _plan(
            [
                _task(
                    "docs-only",
                    "Update product docs",
                    "Refresh product documentation and README screenshots only.",
                    ["docs/product/dashboard.md"],
                ),
                _task(
                    "no-comms",
                    "Internal migration notes",
                    "No customer communication or status page changes are required.",
                    ["docs/internal/migration.md"],
                ),
                _task(
                    "in-app",
                    "Add in-app announcement",
                    "Add in-app announcement banner with message timing.",
                    ["src/announcements/in_app_message.ts"],
                ),
            ]
        )
    )

    assert result.impacted_task_ids == ("in-app",)
    assert result.ignored_task_ids == ("docs-only", "no-comms")
    assert result.records[0].detected_signals == ("in_app_announcement",)
    assert result.records[0].readiness == "partial"


def test_aliases_serialization_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "alias",
                "Customer notice",
                "Customer notice with audience segmentation and owner approval.",
                ["customer_comms/notice.md"],
            )
        ]
    )
    results = [
        build_task_customer_communication_readiness_plan(plan),
        analyze_task_customer_communication_readiness(plan),
        extract_task_customer_communication_readiness(plan),
        generate_task_customer_communication_readiness(plan),
        derive_task_customer_communication_readiness(plan),
        summarize_task_customer_communication_readiness(plan),
        recommend_task_customer_communication_readiness(plan),
    ]
    payload = task_customer_communication_readiness_plan_to_dict(results[0])

    assert all(result.to_dict() == results[0].to_dict() for result in results)
    assert task_customer_communication_readiness_plan_to_dicts(results[0]) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Task Customer Communication Readiness: plan-customer-communication" in (
        task_customer_communication_readiness_plan_to_markdown(results[0])
    )


def _plan(tasks):
    return {"id": "plan-customer-communication", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
