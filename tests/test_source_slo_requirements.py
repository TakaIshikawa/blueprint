from blueprint.source_slo_requirements import (
    SourceSloRequirementsReport,
    build_source_slo_requirements,
    extract_source_slo_requirements,
    source_slo_requirements_to_dict,
)


def test_complete_slo_text_extracts_all_requirement_signals():
    report = build_source_slo_requirements(
        {
            "id": "source-slo",
            "title": "Checkout SLO",
            "summary": (
                "Service level objective: checkout availability must be 99.95% over a rolling 30 day "
                "measurement window. Latency p95 must be under 250ms. Error budget burn rate alerts "
                "page PagerDuty. Owner team is payments reliability. Exclusions include scheduled "
                "maintenance and third-party payment provider outages."
            ),
        }
    )

    by_signal = {requirement.signal: requirement for requirement in report.requirements}

    assert isinstance(report, SourceSloRequirementsReport)
    assert set(by_signal) == {
        "availability",
        "latency",
        "error_budget",
        "measurement_window",
        "alerting",
        "owner",
        "exclusions",
    }
    assert by_signal["availability"].value == "99.95%"
    assert by_signal["latency"].value == "p95 must be under 250ms"
    assert by_signal["measurement_window"].value == "rolling 30 day"
    assert report.missing_signals == ()
    assert report.weak_signals == ()


def test_partial_slo_text_reports_actionable_missing_and_weak_signals():
    report = build_source_slo_requirements(
        "The API should be reliable and fast. Alert the on-call team when latency p99 exceeds 1s."
    )

    signals = [requirement.signal for requirement in report.requirements]

    assert signals == ["latency", "alerting"]
    assert "availability" in report.missing_signals
    assert "error_budget" in report.missing_signals
    assert "measurement_window" in report.missing_signals
    assert "owner" in report.missing_signals
    assert "exclusions" in report.missing_signals
    assert report.weak_signals
    assert "clarify measurable SLO target" in report.weak_signals[0]


def test_absent_slo_language_returns_empty_findings_with_all_gaps():
    report = build_source_slo_requirements(
        {"summary": "Add saved filters to the reporting table and update the empty state copy."}
    )

    assert extract_source_slo_requirements({"summary": "No SLOs here"}) == ()
    assert report.requirements == ()
    assert report.records == ()
    assert report.missing_signals == (
        "availability",
        "latency",
        "error_budget",
        "measurement_window",
        "alerting",
        "owner",
        "exclusions",
    )
    assert report.summary["requirement_count"] == 0
    assert source_slo_requirements_to_dict(report)["requirements"] == []
