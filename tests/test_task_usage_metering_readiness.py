import json

from blueprint.task_usage_metering_readiness import (
    analyze_task_usage_metering_readiness,
    build_task_usage_metering_readiness_plan,
    recommend_task_usage_metering_readiness,
    task_usage_metering_readiness_plan_to_dict,
    task_usage_metering_readiness_plan_to_dicts,
    task_usage_metering_readiness_plan_to_markdown,
)


def test_complete_usage_metering_task_is_ready():
    result = build_task_usage_metering_readiness_plan(
        {
            "id": "plan-metering",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Usage metering for billable API counters",
                    "description": "Metered usage captures usage events for consumption billing.",
                    "acceptance_criteria": [
                        "Event source identifies the authoritative producer and capture point.",
                        "Idempotency key and event ID support deduplication of duplicate events.",
                        "Aggregation window defines daily rollup and billing window boundaries.",
                        "Billing reconciliation matches usage ledger records to invoice totals.",
                        "Quota enforcement checks hard limit, soft limit, and entitlement usage.",
                        "Backfill and replay behavior covers late events and historical usage.",
                        "Observability includes metrics dashboard, alerts, logs, and anomaly monitoring.",
                        "Tests cover metering tests, reconciliation tests, quota tests, and replay tests.",
                    ],
                    "files_or_modules": ["src/billing/usage_metering/usage_events.py"],
                }
            ],
        }
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "event_source",
        "idempotency_deduplication",
        "aggregation_window",
        "billing_reconciliation",
        "quota_enforcement",
        "backfill_replay_behavior",
        "observability",
        "tests",
    )
    assert any("src/billing/usage_metering/usage_events.py" in item for item in record.evidence)


def test_partial_usage_metering_reports_actionable_gaps_and_ignores_no_impact():
    result = analyze_task_usage_metering_readiness(
        [
            {
                "id": "task-partial",
                "title": "Add consumption tracking quotas",
                "description": "Usage metering includes event source and metrics dashboard.",
                "metadata": {"billing": {"counter": "Billable counter tracks API call counters."}},
                "validation_commands": ["python -m pytest tests/billing/test_usage_metering.py"],
            },
            {
                "id": "task-copy",
                "title": "Billing copy cleanup",
                "description": "No usage metering, usage events, quotas, or consumption tracking changes are planned.",
            },
        ]
    )

    record = result.records[0]
    assert result.ignored_task_ids == ("task-copy",)
    assert record.readiness == "partial"
    assert record.present_criteria == ("event_source", "observability", "tests")
    assert record.missing_criteria == (
        "idempotency_deduplication",
        "aggregation_window",
        "billing_reconciliation",
        "quota_enforcement",
        "backfill_replay_behavior",
    )
    assert record.recommended_follow_up_actions[0].startswith("Define idempotency")
    assert any("metadata.billing.counter" in item for item in record.evidence)
    assert any("validation_commands[0]" in item for item in record.evidence)


def test_usage_metering_path_hints_serialization_and_markdown_are_stable():
    result = build_task_usage_metering_readiness_plan(
        {
            "id": "plan-path",
            "tasks": [
                {
                    "id": "task-path",
                    "title": "Refactor billing worker",
                    "files_or_modules": ["src/billing/quotas/consumption_tracking.py"],
                }
            ],
        }
    )
    payload = task_usage_metering_readiness_plan_to_dict(result)

    assert result.records[0].detected_signals == ("quota_consumption_tracking",)
    assert recommend_task_usage_metering_readiness(result) == result.records
    assert task_usage_metering_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-path"
    assert task_usage_metering_readiness_plan_to_markdown(result).startswith("# Task Usage Metering Readiness: plan-path")
