from blueprint.source_rate_limiting_requirements import (
    SourceRateLimitingRequirement,
    SourceRateLimitingRequirementsReport,
    build_source_rate_limiting_requirements,
    extract_source_rate_limiting_requirements,
    source_rate_limiting_requirements_to_markdown,
    summarize_source_rate_limiting_requirements,
)


def test_extracts_multi_signal_rate_limiting_requirements_with_evidence():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary=(
                "Implement API rate limiting with per-user and per-IP limits. "
                "Enforce 100 requests per minute with sliding windows and burst allowances."
            ),
            source_payload={
                "requirements": [
                    "Return 429 status code with Retry-After header when limits exceeded.",
                    "Communicate rate limit status via X-RateLimit-* headers.",
                    "Support exponential backoff for retry strategies.",
                    "Use distributed rate limiting with Redis for multi-node coordination.",
                ],
                "acceptance_criteria": [
                    "Implement token bucket algorithm with burst capacity.",
                ],
            },
        )
    )

    assert isinstance(result, SourceRateLimitingRequirementsReport)
    assert all(isinstance(record, SourceRateLimitingRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "rate_limits",
        "quota_types",
        "time_windows",
        "burst_allowances",
        "enforcement_mechanisms",
        "backoff_strategies",
        "client_communication",
        "distributed_limiting",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert any("per-user" in item.lower() or "per-ip" in item.lower() for item in by_type["quota_types"].evidence)
    assert any("sliding window" in item.lower() for item in by_type["time_windows"].evidence)
    assert result.summary["requirement_count"] == 8
    assert result.summary["abuse_prevention_coverage"] > 0
    assert result.summary["client_experience_coverage"] > 0
    assert result.summary["scalability_coverage"] > 0


def test_brief_without_rate_limiting_language_returns_empty_report():
    result = build_source_rate_limiting_requirements(
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


def test_rate_limits_detected():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="API rate limiting.",
            source_payload={
                "requirements": [
                    "Limit to 100 requests per minute per user.",
                    "Enforce 1000 requests per hour per API key.",
                    "Set global rate limit at 10000 rpm.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "rate_limits" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    rate_limits = by_type["rate_limits"]
    assert any("100" in item and "per minute" in item.lower() for item in rate_limits.evidence)


def test_quota_types_detected():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Multi-level quota types.",
            source_payload={
                "requirements": [
                    "Per-user rate limits for individual users.",
                    "Per-IP address limits to prevent abuse.",
                    "Per-API-key limits for application-level throttling.",
                    "Global limits for the entire service.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "quota_types" in types


def test_time_windows_detected():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Rate limiting with time windows.",
            source_payload={
                "requirements": [
                    "Use sliding windows for rate limit calculation.",
                    "Support rolling window counters.",
                    "Implement token bucket algorithm for smooth rate limiting.",
                    "Use fixed windows aligned to UTC hour boundaries.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "time_windows" in types


def test_burst_allowances_detected():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Handle traffic bursts.",
            source_payload={
                "requirements": [
                    "Allow burst capacity of 150% above steady-state rate.",
                    "Handle temporary traffic spikes gracefully.",
                    "Support burst mode during peak usage.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "burst_allowances" in types


def test_enforcement_mechanisms_detected():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Rate limit enforcement.",
            source_payload={
                "requirements": [
                    "Return HTTP 429 Too Many Requests when limit exceeded.",
                    "Block requests that exceed the rate limit.",
                    "Queue requests during rate limit violations.",
                    "Implement rate limiting enforcement at the API gateway.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "enforcement_mechanisms" in types


def test_backoff_strategies_detected():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Client retry strategies.",
            source_payload={
                "requirements": [
                    "Clients should implement exponential backoff.",
                    "Include Retry-After header in 429 responses.",
                    "Add jitter to prevent thundering herd.",
                    "Document recommended retry policy.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "backoff_strategies" in types


def test_client_communication_detected():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Communicate rate limit status.",
            source_payload={
                "requirements": [
                    "Return X-RateLimit-Limit header with max requests allowed.",
                    "Include X-RateLimit-Remaining with remaining quota.",
                    "Add X-RateLimit-Reset with reset timestamp.",
                    "Provide rate limit visibility to clients.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "client_communication" in types


def test_distributed_limiting_detected():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Multi-node rate limiting.",
            source_payload={
                "requirements": [
                    "Implement distributed rate limiting across cluster.",
                    "Use Redis for centralized rate limit counters.",
                    "Coordinate rate limiting across multi-instance deployment.",
                    "Support cluster-wide rate limit enforcement.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "distributed_limiting" in types


def test_github_style_rate_limiting_patterns():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="GitHub-style rate limiting.",
            source_payload={
                "requirements": [
                    "5000 requests per hour for authenticated users.",
                    "60 requests per hour for unauthenticated requests.",
                    "Return X-RateLimit headers with limit, remaining, reset.",
                    "Use sliding window for rate limit calculation.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "rate_limits" in types
    assert "client_communication" in types
    assert "time_windows" in types


def test_stripe_style_rate_limiting_patterns():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Stripe-style rate limiting.",
            source_payload={
                "requirements": [
                    "Per-API-key rate limits of 100 requests per second.",
                    "Return 429 with Retry-After header.",
                    "Implement exponential backoff for retries.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "rate_limits" in types
    assert "quota_types" in types
    assert "enforcement_mechanisms" in types
    assert "backoff_strategies" in types


def test_cloudflare_style_rate_limiting_patterns():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Cloudflare-style rate limiting.",
            source_payload={
                "requirements": [
                    "Per-IP rate limits with 10-second window.",
                    "Distributed rate limiting across edge nodes.",
                    "Burst allowances for temporary spikes.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "quota_types" in types
    assert "time_windows" in types
    assert "distributed_limiting" in types
    assert "burst_allowances" in types


def test_sliding_window_rate_limiting():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Sliding window rate limiting implementation.",
            source_payload={
                "requirements": [
                    "Use sliding window counters for accurate rate limiting.",
                    "Prevent window boundary gaming attacks.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "time_windows" in types


def test_tiered_quotas():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Tiered rate limits.",
            source_payload={
                "requirements": [
                    "Free tier: 100 requests per hour per user.",
                    "Pro tier: 1000 requests per hour per user.",
                    "Enterprise tier: 10000 requests per hour per organization.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "rate_limits" in types
    assert "quota_types" in types


def test_dict_serialization_round_trips():
    original = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Per-user rate limits with 100 requests per minute.",
            source_payload={
                "requirements": ["Enforce sliding windows.", "Return 429 on violations."],
            },
        )
    )

    serialized = original.to_dict()
    assert isinstance(serialized, dict)
    assert serialized["source_brief_id"] == "rate-limit-source"
    assert len(serialized["requirements"]) == len(original.requirements)

    repeat = original.to_dict()
    assert repeat == serialized


def test_markdown_output_renders_table():
    report = build_source_rate_limiting_requirements(
        _source_brief(
            source_id="rate-limit-markdown-test",
            summary="Per-user rate limits.",
            source_payload={"requirements": ["100 requests per minute per user."]},
        )
    )

    markdown = source_rate_limiting_requirements_to_markdown(report)
    assert isinstance(markdown, str)
    assert "# Source Rate Limiting Requirements Report: rate-limit-markdown-test" in markdown
    assert "## Summary" in markdown
    assert "rate_limits" in markdown


def test_extracts_from_raw_text():
    result = build_source_rate_limiting_requirements(
        "Implement per-user rate limiting with 100 requests per minute, "
        "sliding windows, burst capacity, and 429 enforcement with X-RateLimit headers."
    )

    assert len(result.requirements) >= 5
    types = {req.requirement_type for req in result.requirements}
    assert "rate_limits" in types
    assert "quota_types" in types
    assert "time_windows" in types


def test_extract_helper_returns_tuple():
    requirements = extract_source_rate_limiting_requirements(
        _source_brief(summary="Per-user rate limits.")
    )

    assert isinstance(requirements, tuple)
    assert all(isinstance(req, SourceRateLimitingRequirement) for req in requirements)


def test_summarize_helper_returns_dict():
    summary = summarize_source_rate_limiting_requirements(
        _source_brief(summary="Per-user rate limits with enforcement.")
    )

    assert isinstance(summary, dict)
    assert "requirement_count" in summary


def test_coverage_metrics_calculated():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Comprehensive rate limiting with all features.",
            source_payload={
                "requirements": [
                    "100 requests per minute per user.",
                    "Per-IP and per-API-key quotas.",
                    "Sliding window calculation.",
                    "Burst capacity of 150%.",
                    "Return 429 with enforcement.",
                    "Exponential backoff with Retry-After header.",
                    "X-RateLimit headers for client communication.",
                    "Distributed rate limiting with Redis.",
                ],
            },
        )
    )

    summary = result.summary
    assert summary["abuse_prevention_coverage"] == 100
    assert summary["client_experience_coverage"] == 100
    assert summary["scalability_coverage"] == 100


def test_follow_up_questions_reduced_with_specifics():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="100 requests per minute per user with sliding windows.",
            source_payload={
                "requirements": [
                    "Enforce with 429 status code and blocking.",
                ],
            },
        )
    )

    by_type = {req.requirement_type: req for req in result.requirements}
    rate_limits = by_type.get("rate_limits")
    if rate_limits:
        assert len(rate_limits.follow_up_questions) < 2
    time_windows = by_type.get("time_windows")
    if time_windows:
        assert len(time_windows.follow_up_questions) < 2
    enforcement = by_type.get("enforcement_mechanisms")
    if enforcement:
        assert len(enforcement.follow_up_questions) < 2


def test_redis_distributed_rate_limiting():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Redis-based distributed rate limiting.",
            source_payload={
                "requirements": [
                    "Use Redis for cluster-wide rate limit tracking.",
                    "Implement distributed counters with atomic increments.",
                    "Coordinate rate limiting across multi-node deployment.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "distributed_limiting" in types


def test_token_bucket_algorithm():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Token bucket rate limiting.",
            source_payload={
                "requirements": [
                    "Implement token bucket algorithm.",
                    "Refill tokens at constant rate.",
                    "Allow burst up to bucket capacity.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "time_windows" in types
    assert "burst_allowances" in types


def test_leaky_bucket_algorithm():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Leaky bucket rate limiting.",
            source_payload={
                "requirements": [
                    "Implement leaky bucket for smooth rate limiting.",
                    "Process requests at constant rate.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "time_windows" in types


def test_per_endpoint_rate_limits():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Per-endpoint rate limiting.",
            source_payload={
                "requirements": [
                    "Different rate limits per endpoint.",
                    "POST endpoints limited to 10 requests per minute.",
                    "GET endpoints limited to 100 requests per minute.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "rate_limits" in types
    assert "quota_types" in types


def test_retry_after_header_communication():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Retry-After header for rate limiting.",
            source_payload={
                "requirements": [
                    "Include Retry-After header in 429 responses.",
                    "Indicate seconds until rate limit resets.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "backoff_strategies" in types
    assert "client_communication" in types


def test_abuse_prevention_focus():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Prevent API abuse.",
            source_payload={
                "requirements": [
                    "Rate limit to prevent denial of service attacks.",
                    "Per-IP limits to block malicious actors.",
                    "Enforce hard limits with request blocking.",
                ],
            },
        )
    )

    summary = result.summary
    assert summary["abuse_prevention_coverage"] > 0


def test_fair_usage_policy():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Fair usage policy.",
            source_payload={
                "requirements": [
                    "Ensure fair usage across all users.",
                    "Per-user quotas to prevent monopolization.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "quota_types" in types


def test_graceful_degradation():
    result = build_source_rate_limiting_requirements(
        _source_brief(
            summary="Graceful degradation under load.",
            source_payload={
                "requirements": [
                    "Queue requests during rate limit violations.",
                    "Throttle instead of hard rejection.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "enforcement_mechanisms" in types


def _source_brief(
    *,
    source_id="rate-limit-source",
    title="Rate limiting requirements",
    domain="api",
    summary="General rate limiting requirements.",
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
