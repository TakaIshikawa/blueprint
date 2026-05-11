from datetime import datetime, timezone

from blueprint.api.rate_limits import (
    InMemorySlidingWindowRateLimiter,
    RateLimitPolicy,
    RequestIdentity,
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

