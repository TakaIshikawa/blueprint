"""Framework-neutral rate limit policy evaluation."""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel, ConfigDict, Field


class RateLimitPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policy_id: str = Field(min_length=1)
    requests_per_minute: int | None = Field(default=None, ge=1)
    requests_per_hour: int | None = Field(default=None, ge=1)


class RequestIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_key: str | None = None
    user_id: str | None = None
    ip_address: str | None = None

    @property
    def key(self) -> str:
        return self.api_key or self.user_id or self.ip_address or "anonymous"


class RateLimitEvaluation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    allowed: bool
    remaining: int
    reset_at: datetime
    retry_after_seconds: int | None = None
    policy_id: str


class InMemorySlidingWindowRateLimiter:
    """In-memory sliding-window limiter suitable for deterministic tests."""

    def __init__(self) -> None:
        self._hits: dict[tuple[str, str], deque[datetime]] = defaultdict(deque)

    def reset(self) -> None:
        self._hits.clear()

    def evaluate(
        self,
        policy: RateLimitPolicy,
        identity: RequestIdentity,
        *,
        now: datetime | None = None,
    ) -> RateLimitEvaluation:
        current = now or datetime.now(timezone.utc)
        key = (policy.policy_id, identity.key)
        hits = self._hits[key]
        hour_window = current - timedelta(hours=1)
        while hits and hits[0] <= hour_window:
            hits.popleft()

        windows: list[tuple[int, timedelta, int]] = []
        if policy.requests_per_minute is not None:
            minute_count = sum(1 for hit in hits if hit > current - timedelta(minutes=1))
            windows.append((policy.requests_per_minute, timedelta(minutes=1), minute_count))
        if policy.requests_per_hour is not None:
            windows.append((policy.requests_per_hour, timedelta(hours=1), len(hits)))
        if not windows:
            raise ValueError("At least one policy quota must be configured")

        blocked = [window for window in windows if window[2] >= window[0]]
        if blocked:
            limit, span, _count = min(blocked, key=lambda item: item[1])
            window_hits = [hit for hit in hits if hit > current - span]
            reset_at = (window_hits[0] + span) if window_hits else current
            retry = max(0, int((reset_at - current).total_seconds()))
            return RateLimitEvaluation(
                allowed=False,
                remaining=0,
                reset_at=reset_at,
                retry_after_seconds=retry,
                policy_id=policy.policy_id,
            )

        hits.append(current)
        remaining = min(limit - count - 1 for limit, _span, count in windows)
        shortest = min(windows, key=lambda item: item[1])[1]
        return RateLimitEvaluation(
            allowed=True,
            remaining=max(0, remaining),
            reset_at=current + shortest,
            policy_id=policy.policy_id,
        )

