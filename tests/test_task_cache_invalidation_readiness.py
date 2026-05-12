from blueprint.task_cache_invalidation_readiness import (
    TaskCacheInvalidationReadiness,
    analyze_task_cache_invalidation_readiness,
    summarize_task_cache_invalidation_readiness,
    task_cache_invalidation_readiness_to_dict,
)


def test_ready_cache_invalidation_plan_scores_full_readiness():
    result = analyze_task_cache_invalidation_readiness(
        {
            "title": "Invalidate account summary caches after profile writes",
            "description": (
                "Cache keys use tenant scoped keys in the account-summary namespace. "
                "Invalidation trigger runs after update and after delete domain events. "
                "TTL is 15 minutes with max-age aligned to the API response cache. "
                "Stale read tolerance is under 30 seconds with read-after-write checks. "
                "Warmup backfill rehydrates and primes cache entries for active accounts. "
                "Rollback uses a kill switch to disable invalidation and bypass cache. "
                "Observability includes metrics, dashboard, stale rate, and alerts. "
                "Owner is the accounts platform on-call team."
            ),
            "acceptance_criteria": [
                "Purge latency and invalidation failures are logged.",
            ],
        }
    )

    assert isinstance(result, TaskCacheInvalidationReadiness)
    assert result.cache_keys_defined is True
    assert result.invalidation_triggers_defined is True
    assert result.ttl_behavior_defined is True
    assert result.stale_read_tolerance_defined is True
    assert result.warmup_backfill_defined is True
    assert result.rollback_path_defined is True
    assert result.observability_defined is True
    assert result.ownership_defined is True
    assert result.missing_requirements == ()
    assert result.actionable_gaps == ()
    assert result.readiness_score == 1.0
    assert result.is_ready is True


def test_partial_cache_invalidation_plan_returns_actionable_gaps():
    result = summarize_task_cache_invalidation_readiness(
        {
            "title": "Purge catalog Redis cache",
            "description": (
                "Cache key pattern is catalog:{tenant}:{sku}. "
                "Invalidate on publish event. "
                "TTL expires after 10 minutes."
            ),
            "metadata": {"owner": "catalog team"},
        }
    )

    assert result.cache_keys_defined is True
    assert result.invalidation_triggers_defined is True
    assert result.ttl_behavior_defined is True
    assert result.ownership_defined is True
    assert result.stale_read_tolerance_defined is False
    assert result.warmup_backfill_defined is False
    assert result.rollback_path_defined is False
    assert result.observability_defined is False
    assert result.missing_requirements == (
        "stale_read_tolerance",
        "warmup_backfill",
        "rollback",
        "observability",
    )
    assert result.actionable_gaps == (
        "State tolerated stale-read windows and freshness guarantees.",
        "Add a cache warmup, rehydration, priming, or backfill plan.",
        "Provide rollback, bypass, kill-switch, or manual purge fallback steps.",
        "Add metrics, alerts, dashboards, or logs for invalidation health.",
    )
    assert result.readiness_score == 0.5
    assert result.is_ready is False


def test_absent_cache_invalidation_plan_reports_every_gap():
    result = analyze_task_cache_invalidation_readiness(
        {
            "title": "Update onboarding copy",
            "description": "Adjust heading and button labels.",
        }
    )

    assert result.to_dict() == {
        "cache_keys_defined": False,
        "invalidation_triggers_defined": False,
        "ttl_behavior_defined": False,
        "stale_read_tolerance_defined": False,
        "warmup_backfill_defined": False,
        "rollback_path_defined": False,
        "observability_defined": False,
        "ownership_defined": False,
        "missing_requirements": [
            "cache_keys",
            "invalidation_triggers",
            "ttl_behavior",
            "stale_read_tolerance",
            "warmup_backfill",
            "rollback",
            "observability",
            "ownership",
        ],
        "actionable_gaps": [
            "Document affected cache keys, namespaces, and tenant or user scoping.",
            "Specify the events, writes, deletes, or deploys that trigger invalidation.",
            "Define TTL, max-age, expiry, or cache lifetime behavior.",
            "State tolerated stale-read windows and freshness guarantees.",
            "Add a cache warmup, rehydration, priming, or backfill plan.",
            "Provide rollback, bypass, kill-switch, or manual purge fallback steps.",
            "Add metrics, alerts, dashboards, or logs for invalidation health.",
            "Name the responsible owner, team, DRI, or on-call group.",
        ],
        "readiness_score": 0.0,
        "is_ready": False,
    }


def test_serialization_helper_preserves_stable_shape():
    result = analyze_task_cache_invalidation_readiness(
        {"description": "Cache namespace and owner team are documented."}
    )

    payload = task_cache_invalidation_readiness_to_dict(result)

    assert list(payload) == [
        "cache_keys_defined",
        "invalidation_triggers_defined",
        "ttl_behavior_defined",
        "stale_read_tolerance_defined",
        "warmup_backfill_defined",
        "rollback_path_defined",
        "observability_defined",
        "ownership_defined",
        "missing_requirements",
        "actionable_gaps",
        "readiness_score",
        "is_ready",
    ]
    assert payload["cache_keys_defined"] is True
    assert payload["ownership_defined"] is True
