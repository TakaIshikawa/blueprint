from blueprint.source_api_quota_management_requirements import (
    SourceApiQuotaManagementRequirement,
    SourceApiQuotaManagementRequirementsReport,
    build_source_api_quota_management_requirements,
    extract_source_api_quota_management_requirements,
    source_api_quota_management_requirements_to_markdown,
    summarize_source_api_quota_management_requirements,
)


def test_extracts_multi_signal_quota_requirements_with_evidence():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary=(
                "Implement API quota management with per-user and per-organization limits. "
                "Track usage per-minute and per-day with hard enforcement and throttling."
            ),
            source_payload={
                "requirements": [
                    "Monitor quota usage with alerts at 80% and 90% thresholds.",
                    "Support quota increase requests with approval workflow.",
                    "Handle overage with temporary burst and billing integration.",
                    "Reset quotas monthly at the start of each billing period.",
                ],
                "acceptance_criteria": [
                    "Track and report usage metrics for each user and organization.",
                ],
            },
        )
    )

    assert isinstance(result, SourceApiQuotaManagementRequirementsReport)
    assert all(isinstance(record, SourceApiQuotaManagementRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "quota_types",
        "time_windows",
        "enforcement_strategy",
        "usage_tracking",
        "quota_increase",
        "overage_handling",
        "quota_reset",
        "quota_monitoring",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert any("per-user" in item.lower() or "per-organization" in item.lower() for item in by_type["quota_types"].evidence)
    assert any("per-minute" in item.lower() or "per-day" in item.lower() for item in by_type["time_windows"].evidence)
    assert result.summary["requirement_count"] == 8
    assert result.summary["policy_coverage"] > 0
    assert result.summary["observability_coverage"] > 0
    assert result.summary["flexibility_coverage"] > 0


def test_brief_without_quota_language_returns_empty_report():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            title="User authentication",
            summary="Add user login with JWT tokens.",
            source_payload={
                "requirements": ["Validate credentials.", "Return access token."],
            },
        )
    )

    assert result.summary["requirement_count"] == 0
    assert result.requirements == ()


def test_aws_style_quota_patterns_detected():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Implement AWS-style quota management.",
            source_payload={
                "requirements": [
                    "Per-account quotas for API calls.",
                    "Per-second and per-day rate limits.",
                    "Hard limits with request rejection on quota exceeded.",
                    "Quota monitoring dashboard with usage visibility.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "quota_types" in types
    assert "time_windows" in types
    assert "enforcement_strategy" in types
    assert "quota_monitoring" in types


def test_google_cloud_quota_patterns_detected():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Google Cloud-style quota management.",
            source_payload={
                "requirements": [
                    "Per-project quotas with shared limits.",
                    "Rolling window quota tracking per-minute.",
                    "Soft limits with throttling instead of blocking.",
                    "Request quota increase through support tickets.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "quota_types" in types
    assert "time_windows" in types
    assert "enforcement_strategy" in types
    assert "quota_increase" in types


def test_stripe_api_quota_patterns_detected():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Stripe-style API quota with overage billing.",
            source_payload={
                "requirements": [
                    "Per-API key rate limits.",
                    "Charge overage fees for usage beyond quota.",
                    "Track metered usage for billing.",
                    "Monthly quota reset aligned with billing cycle.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "quota_types" in types
    assert "overage_handling" in types
    assert "usage_tracking" in types
    assert "quota_reset" in types


def test_quota_monitoring_and_alerts_detected():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Quota monitoring and alerting system.",
            source_payload={
                "requirements": [
                    "Monitor quota consumption in real-time.",
                    "Send alerts when approaching quota limits.",
                    "Provide quota status dashboard.",
                    "Notify at 75%, 90%, and 100% thresholds.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "quota_monitoring" in types


def test_usage_tracking_mechanisms_detected():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Track API usage for quota management.",
            source_payload={
                "requirements": [
                    "Meter usage for each API call.",
                    "Track consumption metrics by user and endpoint.",
                    "Report usage data for billing and monitoring.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "usage_tracking" in types


def test_enforcement_strategies_detected():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Quota enforcement with hard and soft limits.",
            source_payload={
                "requirements": [
                    "Hard limits that block requests when quota exceeded.",
                    "Soft limits with throttling and warnings.",
                    "Reject requests exceeding quota with 429 status code.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "enforcement_strategy" in types


def test_quota_increase_workflows_detected():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Support quota increase requests.",
            source_payload={
                "requirements": [
                    "Allow users to request higher quotas.",
                    "Approval workflow for quota increases.",
                    "Raise quota limits for premium customers.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "quota_increase" in types


def test_overage_and_burst_handling_detected():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Handle quota overage with burst and billing.",
            source_payload={
                "requirements": [
                    "Allow temporary burst beyond quota.",
                    "Charge overage fees for pay-as-you-go billing.",
                    "Handle exceeded quota with billing integration.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "overage_handling" in types


def test_quota_reset_cycles_detected():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Quota reset and renewal cycles.",
            source_payload={
                "requirements": [
                    "Reset quotas monthly at billing period start.",
                    "Renew quota limits at the end of each cycle.",
                    "Refresh usage counters on quota rollover.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "quota_reset" in types


def test_dict_serialization_round_trips():
    original = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Per-user quotas with monthly limits.",
            source_payload={
                "requirements": ["Track usage per-user.", "Reset quotas monthly."],
            },
        )
    )

    serialized = original.to_dict()
    assert isinstance(serialized, dict)
    assert serialized["source_brief_id"] == "quota-source"
    assert len(serialized["requirements"]) == len(original.requirements)

    repeat = original.to_dict()
    assert repeat == serialized


def test_markdown_output_renders_table():
    report = build_source_api_quota_management_requirements(
        _source_brief(
            source_id="quota-markdown-test",
            summary="Per-user quota limits.",
            source_payload={"requirements": ["Per-user rate limits."]},
        )
    )

    markdown = source_api_quota_management_requirements_to_markdown(report)
    assert isinstance(markdown, str)
    assert "# Source API Quota Management Requirements Report: quota-markdown-test" in markdown
    assert "## Summary" in markdown
    assert "quota_types" in markdown


def test_extracts_from_raw_text():
    result = build_source_api_quota_management_requirements(
        "Implement per-user quotas with per-minute rate limits, "
        "hard enforcement blocking, and usage tracking with monitoring alerts."
    )

    assert len(result.requirements) >= 4
    types = {req.requirement_type for req in result.requirements}
    assert "quota_types" in types
    assert "time_windows" in types
    assert "enforcement_strategy" in types


def test_extract_helper_returns_tuple():
    requirements = extract_source_api_quota_management_requirements(
        _source_brief(summary="Per-user quotas.")
    )

    assert isinstance(requirements, tuple)
    assert all(isinstance(req, SourceApiQuotaManagementRequirement) for req in requirements)


def test_summarize_helper_returns_dict():
    summary = summarize_source_api_quota_management_requirements(
        _source_brief(summary="Per-user quotas with usage tracking.")
    )

    assert isinstance(summary, dict)
    assert "requirement_count" in summary


def test_coverage_metrics_calculated():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Per-user quotas, per-minute limits, hard enforcement, usage tracking, monitoring, quota increases, overage billing, monthly resets.",
            source_payload={
                "requirements": [
                    "Per-user and per-org quota types.",
                    "Per-minute and per-day time windows.",
                    "Hard limit enforcement.",
                    "Track usage metrics.",
                    "Monitor quota status.",
                    "Support quota increase requests.",
                    "Handle overage with billing.",
                    "Reset quotas monthly.",
                ],
            },
        )
    )

    summary = result.summary
    assert summary["policy_coverage"] == 100
    assert summary["observability_coverage"] == 100
    assert summary["flexibility_coverage"] == 100


def test_follow_up_questions_reduced_with_specifics():
    result = build_source_api_quota_management_requirements(
        _source_brief(
            summary="Per-user quotas with per-minute rate limits.",
            source_payload={
                "requirements": [
                    "Hard limits that block requests.",
                    "Fixed time windows for quota tracking.",
                ],
            },
        )
    )

    by_type = {req.requirement_type: req for req in result.requirements}
    quota_types = by_type.get("quota_types")
    if quota_types:
        assert len(quota_types.follow_up_questions) < 2
    time_windows = by_type.get("time_windows")
    if time_windows:
        assert len(time_windows.follow_up_questions) < 2
    enforcement = by_type.get("enforcement_strategy")
    if enforcement:
        assert len(enforcement.follow_up_questions) < 2


def _source_brief(
    *,
    source_id="quota-source",
    title="Quota management requirements",
    domain="platform",
    summary="General quota management requirements.",
    source_payload=None,
    source_links=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }
