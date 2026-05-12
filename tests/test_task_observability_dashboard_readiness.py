from blueprint.task_observability_dashboard_readiness import (
    evaluate_task_observability_dashboard_readiness,
    summarize_task_observability_dashboard_readiness,
    task_observability_dashboard_readiness_to_dict,
)


def test_ready_dashboard_plan_covers_all_readiness_criteria():
    result = evaluate_task_observability_dashboard_readiness(
        {
            "title": "Build checkout observability dashboard",
            "description": (
                "Audience is SRE and payments on-call. Metrics include p95 latency, traffic, "
                "error rate, and availability SLO. Filters are service, region, tenant, and environment. "
                "Data source is Prometheus in Grafana. Freshness refreshes within 1 min. Alert linkage "
                "connects panels to PagerDuty thresholds. Owner is payments reliability."
            ),
            "validation_plan": "Validate queries and attach screenshot evidence after deploy.",
        }
    )

    assert result.readiness_level == "ready"
    assert result.missing_criteria == ()
    assert result.satisfied_criteria == (
        "audience",
        "metrics",
        "filters_dimensions",
        "data_source",
        "freshness_slo",
        "alert_linkage",
        "ownership",
        "validation_evidence",
    )


def test_partial_dashboard_plan_returns_actionable_gaps():
    result = summarize_task_observability_dashboard_readiness(
        "Create a Grafana dashboard for on-call with latency and error rate by service."
    )

    assert result.readiness_level == "partial"
    assert result.satisfied_criteria == (
        "audience",
        "metrics",
        "filters_dimensions",
        "data_source",
    )
    assert result.missing_criteria == (
        "freshness_slo",
        "alert_linkage",
        "ownership",
        "validation_evidence",
    )
    assert "Set data freshness" in result.gaps[0]


def test_absent_dashboard_plan_marks_all_criteria_missing():
    result = evaluate_task_observability_dashboard_readiness(
        {"description": "Refactor account settings form labels."}
    )

    assert result.readiness_level == "missing"
    assert result.satisfied_criteria == ()
    assert result.missing_criteria == (
        "audience",
        "metrics",
        "filters_dimensions",
        "data_source",
        "freshness_slo",
        "alert_linkage",
        "ownership",
        "validation_evidence",
    )
    assert task_observability_dashboard_readiness_to_dict(result)["evidence"] == {}
