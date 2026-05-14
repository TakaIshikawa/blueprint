import json

from blueprint.task_data_quality_monitoring_readiness import (
    analyze_task_data_quality_monitoring_readiness,
    build_task_data_quality_monitoring_readiness_plan,
    recommend_task_data_quality_monitoring_readiness,
    task_data_quality_monitoring_readiness_plan_to_dict,
    task_data_quality_monitoring_readiness_plan_to_dicts,
    task_data_quality_monitoring_readiness_plan_to_markdown,
)


def test_complete_data_quality_monitoring_task_is_ready():
    result = build_task_data_quality_monitoring_readiness_plan(
        {
            "id": "plan-dq",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Add data quality monitoring for warehouse tables",
                    "description": "Data quality monitoring covers freshness, completeness, duplicate detection, reconciliation, anomaly alerts, schema drift, and validation monitors.",
                    "acceptance_criteria": [
                        "Metric ownership names the data steward DRI and on-call owner.",
                        "Thresholds include freshness SLA, 99 percent completeness, null rate, and alert threshold.",
                        "Scan scope defines full scan for daily partitions and sampled checks for history.",
                        "Alert routing sends PagerDuty, Slack, email alert, and ticket routing.",
                        "Remediation runbook includes triage steps and repair workflow.",
                        "Backfill strategy reprocesses historical partitions with a correction job.",
                    ],
                    "files_or_modules": ["src/dq/freshness_monitor.py"],
                }
            ],
        }
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert result.summary["readiness_counts"]["ready"] == 1


def test_partial_data_quality_monitoring_reports_missing_requirements():
    result = analyze_task_data_quality_monitoring_readiness(
        [
            {
                "id": "task-partial",
                "title": "Schema drift and freshness monitor",
                "description": "Data quality checks have owner and thresholds.",
                "files_or_modules": ["pipelines/data_quality/schema_drift.yml"],
            },
            {"id": "task-docs", "title": "Docs", "description": "No data quality monitoring changes are required."},
        ]
    )

    record = result.records[0]
    assert result.impacted_task_ids == ("task-partial",)
    assert result.ignored_task_ids == ("task-docs",)
    assert record.readiness == "partial"
    assert record.present_criteria == ("metric_ownership", "thresholds")
    assert record.missing_criteria == ("scan_scope", "alert_routing", "remediation_runbook", "backfill_strategy")
    assert result.summary["missing_criterion_counts"]["alert_routing"] == 1


def test_serialization_and_invalid_inputs_are_stable():
    result = build_task_data_quality_monitoring_readiness_plan("Freshness data quality monitor")
    payload = task_data_quality_monitoring_readiness_plan_to_dict(result)

    assert result.summary["impacted_task_count"] == 1
    assert recommend_task_data_quality_monitoring_readiness(result) == result.records
    assert task_data_quality_monitoring_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["summary"]["missing_criterion_count"] == 6
    assert task_data_quality_monitoring_readiness_plan_to_markdown(result).startswith("# Task Data Quality Monitoring Readiness")
    assert build_task_data_quality_monitoring_readiness_plan({"tasks": []}).records == ()

