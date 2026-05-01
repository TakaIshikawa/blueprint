import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_cache_warming_readiness import (
    TaskCacheWarmingReadinessPlan,
    TaskCacheWarmingReadinessRecommendation,
    build_task_cache_warming_readiness_plan,
    generate_task_cache_warming_readiness,
    summarize_task_cache_warming_readiness,
    task_cache_warming_readiness_plan_to_dict,
    task_cache_warming_readiness_plan_to_markdown,
    task_cache_warming_readiness_to_dicts,
)


def test_cache_surface_detection_from_paths_and_text():
    result = build_task_cache_warming_readiness_plan(
        _plan(
            [
                _task(
                    "task-path",
                    title="Add cache warmer files",
                    description="Move implementation only.",
                    files_or_modules=[
                        "src/cache/product_cdn_warmup.py",
                        "src/search/index_warmer.py",
                        "db/materialized_views/account_rollups.sql",
                    ],
                ),
                _task(
                    "task-text",
                    title="Refresh aggregates",
                    description=(
                        "Precomputed aggregates seed Redis cache and refresh a materialized view "
                        "for the search warmer."
                    ),
                ),
            ]
        )
    )

    assert isinstance(result, TaskCacheWarmingReadinessPlan)
    assert result.plan_id == "plan-cache-warming-readiness"
    by_id = {record.task_id: record for record in result.recommendations}
    assert set(by_id) == {"task-path", "task-text"}
    assert {"cache", "cdn", "materialized_view", "precomputed_aggregate", "search_warmer"} <= set(
        by_id["task-path"].cache_surfaces
    )
    assert {"cache", "materialized_view", "precomputed_aggregate", "search_warmer"} <= set(
        by_id["task-text"].cache_surfaces
    )
    assert "warmup_trigger" in by_id["task-path"].missing_controls
    assert any("files_or_modules" in item for item in by_id["task-path"].evidence)


def test_user_facing_launch_critical_cold_cache_escalates_to_high_risk_and_sorts_first():
    result = build_task_cache_warming_readiness_plan(
        _plan(
            [
                _task(
                    "task-medium",
                    title="Add admin cache",
                    description="Cache internal report filters in Redis.",
                ),
                _task(
                    "task-high",
                    title="Prewarm launch cache",
                    description=(
                        "Launch-critical user-facing cold cache path for homepage traffic "
                        "needs cold-start mitigation."
                    ),
                ),
            ]
        )
    )

    assert result.cache_task_ids == ("task-high", "task-medium")
    by_id = {record.task_id: record for record in result.recommendations}
    assert by_id["task-high"].risk_level == "high"
    assert "cold_start_mitigation" in by_id["task-high"].cache_surfaces
    assert {"launch", "user_facing", "cold_start"} <= set(by_id["task-high"].warming_triggers)
    assert by_id["task-medium"].risk_level == "medium"


def test_complete_controls_reduce_missing_control_counts_and_risk():
    result = build_task_cache_warming_readiness_plan(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Warm high traffic CDN cache",
                    description="User-facing high-traffic CDN cache warmup for release day.",
                    acceptance_criteria=[
                        "Warmup trigger runs after deploy.",
                        "Cold-start fallback uses origin fallback on cache misses.",
                        "Invalidation coordination covers cache invalidation and TTL coordination.",
                        "Capacity limit uses throttling and a warmup budget.",
                        "Stale data guard enforces freshness check and max age.",
                        "Monitoring metric tracks cache hit rate, miss rate, latency, and alerts.",
                        "Rollback or disable path uses a feature flag kill switch.",
                    ],
                ),
                _task(
                    "task-missing",
                    title="Warm high traffic CDN cache without controls",
                    description="User-facing high-traffic CDN cache warmup for release day.",
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.recommendations}
    assert by_id["task-complete"].missing_controls == ()
    assert by_id["task-complete"].risk_level == "low"
    assert by_id["task-missing"].risk_level == "high"
    assert result.summary["missing_control_counts"]["warmup_trigger"] == 1
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 1}


def test_unrelated_tasks_are_suppressed_and_no_op_behavior_is_stable():
    result = build_task_cache_warming_readiness_plan(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update settings docs",
                    description="Document profile settings only.",
                    files_or_modules=["docs/settings.md"],
                ),
                _task(
                    "task-cache",
                    title="Add cache warmup",
                    description="Prime Redis cache after deploy.",
                ),
            ]
        )
    )
    empty = build_task_cache_warming_readiness_plan(
        _plan([_task("task-ui", title="Polish dashboard", description="Adjust spacing.")])
    )

    assert result.cache_task_ids == ("task-cache",)
    assert result.suppressed_task_ids == ("task-docs",)
    assert result.summary["cache_task_count"] == 1
    assert result.summary["suppressed_task_count"] == 1
    assert empty.recommendations == ()
    assert empty.cache_task_ids == ()
    assert empty.summary["cache_task_count"] == 0
    assert empty.to_markdown().endswith("No cache warming readiness recommendations were inferred.")
    assert generate_task_cache_warming_readiness({"tasks": "not a list"}) == ()
    assert generate_task_cache_warming_readiness(None) == ()


def test_model_input_no_mutation_aliases_and_json_serialization_are_stable():
    source = _plan(
        [
            _task(
                "task-z",
                title="Add materialized view warmer",
                description="Scheduled warmup refreshes materialized view rollups nightly.",
                metadata={"traffic": "internal"},
            ),
            _task(
                "task-a",
                title="Add CDN cold-start warmer",
                description="User-facing launch warmup for CDN cold cache on release day.",
                metadata={
                    "controls": {
                        "warmup_trigger": "post-deploy warmup trigger",
                        "cold_start_fallback": "origin fallback handles cache miss fallback",
                        "invalidation_coordination": "cache invalidation coordination is documented",
                        "capacity_limit": "throttle with capacity limit",
                        "stale_data_guard": "freshness check prevents stale data",
                        "monitoring_metric": "cache hit rate metric and alerts",
                        "rollback_or_disable_path": "kill switch can disable warmer",
                    }
                },
            ),
        ]
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    result = build_task_cache_warming_readiness_plan(model)
    alias_result = summarize_task_cache_warming_readiness(source)
    records = generate_task_cache_warming_readiness(model)
    payload = task_cache_warming_readiness_plan_to_dict(result)
    markdown = task_cache_warming_readiness_plan_to_markdown(result)

    assert source == original
    assert isinstance(result.recommendations[0], TaskCacheWarmingReadinessRecommendation)
    assert result.cache_task_ids == ("task-z", "task-a")
    assert alias_result.to_dict() == result.to_dict()
    assert records == result.recommendations
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["recommendations"]
    assert task_cache_warming_readiness_to_dicts(records) == payload["recommendations"]
    assert task_cache_warming_readiness_to_dicts(result) == payload["recommendations"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "recommendations",
        "cache_task_ids",
        "suppressed_task_ids",
        "summary",
    ]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "cache_surfaces",
        "warming_triggers",
        "missing_controls",
        "risk_level",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Cache Warming Readiness: plan-cache-warming-readiness")


def _plan(tasks):
    return {
        "id": "plan-cache-warming-readiness",
        "implementation_brief_id": "brief-cache-warming-readiness",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    tags=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if tags is not None:
        task["tags"] = tags
    if metadata is not None:
        task["metadata"] = metadata
    return task
