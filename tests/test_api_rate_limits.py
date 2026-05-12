from datetime import datetime, timezone

from blueprint.api.rate_limits import (
    InMemorySlidingWindowRateLimiter,
    RateLimitEvaluation,
    RateLimitPolicy,
    RequestIdentity,
    build_rate_limit_headers,
)


def test_policy_supports_per_minute_and_per_hour_quotas():
    policy = RateLimitPolicy(policy_id="default", requests_per_minute=1, requests_per_hour=10)
    limiter = InMemorySlidingWindowRateLimiter()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    first = limiter.evaluate(policy, RequestIdentity(api_key="key-a"), now=now)
    second = limiter.evaluate(policy, RequestIdentity(api_key="key-a"), now=now)

    assert first.allowed is True
    assert first.remaining == 0
    assert second.allowed is False
    assert second.retry_after_seconds == 60


def test_separate_identities_consume_quota_independently():
    policy = RateLimitPolicy(policy_id="default", requests_per_minute=1)
    limiter = InMemorySlidingWindowRateLimiter()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    assert limiter.evaluate(policy, RequestIdentity(api_key="a"), now=now).allowed is True
    assert limiter.evaluate(policy, RequestIdentity(api_key="b"), now=now).allowed is True


def test_limiter_can_be_reset():
    policy = RateLimitPolicy(policy_id="default", requests_per_minute=1)
    limiter = InMemorySlidingWindowRateLimiter()
    identity = RequestIdentity(user_id="u1")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    limiter.evaluate(policy, identity, now=now)
    limiter.reset()

    assert limiter.evaluate(policy, identity, now=now).allowed is True


def test_build_rate_limit_headers_for_allowed_evaluation_omits_retry_after():
    evaluation = RateLimitEvaluation(
        allowed=True,
        remaining=4,
        reset_at=datetime(2026, 1, 1, 12, 30, tzinfo=timezone.utc),
        policy_id="default",
    )

    headers = build_rate_limit_headers(evaluation)

    assert headers == {
        "X-RateLimit-Policy": "default",
        "X-RateLimit-Remaining": "4",
        "X-RateLimit-Reset": "2026-01-01T12:30:00Z",
    }
    assert evaluation.retry_after_seconds is None


def test_build_rate_limit_headers_for_blocked_evaluation_includes_retry_after():
    evaluation = RateLimitEvaluation(
        allowed=False,
        remaining=0,
        reset_at=datetime(2026, 1, 1, 21, 30, tzinfo=timezone.utc),
        retry_after_seconds=45,
        policy_id="strict",
    )

    headers = build_rate_limit_headers(evaluation)

    assert headers["X-RateLimit-Policy"] == "strict"
    assert headers["X-RateLimit-Remaining"] == "0"
    assert headers["X-RateLimit-Reset"] == "2026-01-01T21:30:00Z"
    assert headers["Retry-After"] == "45"


def test_build_rate_limit_headers_normalizes_reset_to_utc():
    evaluation = RateLimitEvaluation(
        allowed=True,
        remaining=2,
        reset_at=datetime.fromisoformat("2026-01-01T21:30:00+09:00"),
        policy_id="tokyo",
    )

    headers = build_rate_limit_headers(evaluation)

    assert headers["X-RateLimit-Reset"] == "2026-01-01T12:30:00Z"
