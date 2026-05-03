import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_cache_warmup_readiness import (
    TaskCacheWarmupReadinessPlan,
    TaskCacheWarmupReadinessRecord,
    analyze_task_cache_warmup_readiness,
    build_task_cache_warmup_readiness_plan,
    derive_task_cache_warmup_readiness,
    generate_task_cache_warmup_readiness,
    recommend_task_cache_warmup_readiness,
    summarize_task_cache_warmup_readiness,
    task_cache_warmup_readiness_plan_to_dict,
    task_cache_warmup_readiness_plan_to_dicts,
    task_cache_warmup_readiness_plan_to_markdown,
    task_cache_warmup_readiness_to_dicts,
)


def test_high_risk_warmup_without_load_controls_sorts_first():
    result = build_task_cache_warmup_readiness_plan(
        _plan(
            [
                _task(
                    "task-medium",
                    title="Precompute account projection",
                    description="Precomputed projection refreshes account read model.",
                    acceptance_criteria=[
                        "Load shedding uses throttling and a concurrency limit.",
                        "Cache miss fallback uses on-demand compute.",
                    ],
                ),
                _task(
                    "task-high",
                    title="Prime launch cache",
                    description="Cache warmup primes Redis hot keys before launch.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskCacheWarmupReadinessPlan)
    assert result.cache_task_ids == ("task-high", "task-medium")
    by_id = {record.task_id: record for record in result.records}
    assert isinstance(by_id["task-high"], TaskCacheWarmupReadinessRecord)
    assert by_id["task-high"].risk_level == "high"
    assert {"warming_job", "primed_keys"} <= set(by_id["task-high"].signals)
    assert "load_shedding" in by_id["task-high"].missing_safeguards
    assert "cache_miss_fallback" in by_id["task-high"].missing_safeguards
    assert result.summary["risk_counts"] == {"high": 1, "medium": 1, "low": 0}


def test_medium_risk_partial_safeguards_and_summary_counts():
    result = analyze_task_cache_warmup_readiness(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Refresh materialized views",
                    description="Materialized view refresh precomputes analytics projections.",
                    acceptance_criteria=[
                        "Warmup backfill plan chunks the rebuild by account.",
                        "Stale data guard uses a source version watermark.",
                        "Load shedding rate limits refresh batches.",
                        "Cache miss fallback uses the uncached path.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.risk_level == "medium"
    assert set(record.signals) == {"precomputed_projection", "materialized_view"}
    assert record.safeguards == (
        "warmup_backfill_plan",
        "stale_data_guard",
        "load_shedding",
        "cache_miss_fallback",
    )
    assert record.present_safeguards == record.safeguards
    assert record.missing_safeguards == (
        "observability",
        "rollback_or_disable_switch",
        "owner_evidence",
    )
    assert result.summary["missing_safeguard_counts"]["observability"] == 1
    assert result.summary["signal_counts"]["materialized_view"] == 1


def test_low_risk_fully_covered_warmup():
    result = build_task_cache_warmup_readiness_plan(
        _plan(
            [
                _task(
                    "task-low",
                    title="Hydrate startup cache",
                    description="Startup cache hydration prewarms product availability cache.",
                    acceptance_criteria=[
                        "Warmup backfill plan supports checkpointed backfill and resume.",
                        "Stale data guard checks source version and max age.",
                        "Load shedding uses batch size, queue limit, and circuit breaker.",
                        "Cache miss fallback uses database fallback and lazy recompute.",
                        "Observability emits warmup success metrics, miss rate, stale rate, and alerts.",
                        "Rollback or disable switch uses a feature flag kill switch.",
                        "Owner evidence names the platform on-call service owner.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.risk_level == "low"
    assert "startup_cache_hydration" in record.signals
    assert record.missing_safeguards == ()
    assert record.recommended_checks == ()
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_unrelated_task_empty_state_and_markdown_are_stable():
    result = build_task_cache_warmup_readiness_plan(
        _plan([_task("task-copy", title="Polish dashboard", description="Adjust table spacing.")])
    )

    assert result.records == ()
    assert result.cache_task_ids == ()
    assert result.suppressed_task_ids == ("task-copy",)
    assert result.summary["cache_task_count"] == 0
    assert result.to_markdown() == "\n".join(
        [
            "# Task Cache Warmup Readiness: plan-cache-warmup",
            "",
            "## Summary",
            "",
            "- Task count: 1",
            "- Cache warmup task count: 0",
            "- Missing safeguard count: 0",
            "- Risk counts: high 0, medium 0, low 0",
            (
                "- Signal counts: warming_job 0, precomputed_projection 0, materialized_view 0, "
                "primed_keys 0, startup_cache_hydration 0"
            ),
            "",
            "No task cache warmup readiness records were inferred.",
            "",
            "Suppressed tasks: task-copy",
        ]
    )


def test_metadata_evidence_paths_and_path_signals_are_preserved():
    result = build_task_cache_warmup_readiness_plan(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Add cache precompute worker",
                    description="Move implementation files.",
                    files_or_modules=[
                        "src/cache/warmup/product_key_priming.py",
                        "db/materialized_views/product_rollups.sql",
                    ],
                    metadata={
                        "warmup": {
                            "backfill_plan": "Backfill plan chunks product cache warmup by tenant.",
                            "stale_data_guard": "Freshness check validates source version watermark.",
                            "load_shedding": "Warmup budget and throttle protect Redis.",
                            "fallback": "Cache miss fallback uses on-demand compute.",
                            "observability": "Dashboard tracks warmup success and queue depth.",
                            "disable": "Feature flag can disable warmup.",
                            "owner": "Search platform on-call owns the warmer.",
                        }
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert {"warming_job", "materialized_view", "primed_keys"} <= set(record.signals)
    assert record.risk_level == "low"
    assert any(item == "files_or_modules: src/cache/warmup/product_key_priming.py" for item in record.evidence)
    assert any("metadata.warmup.backfill_plan:" in item for item in record.evidence)
    assert any("metadata.warmup.stale_data_guard:" in item for item in record.evidence)


def test_serialization_aliases_model_inputs_and_no_mutation_are_stable():
    object_task = SimpleNamespace(
        id="task-object",
        title="Prime startup keys",
        description="Startup cache hydration primes keys during boot.",
        acceptance_criteria=[
            "Load shedding uses throttling.",
            "Cache miss fallback uses origin fallback.",
        ],
        metadata={"owner": "Core services owns the warmup."},
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Precompute materialized projection",
            description="Precomputed projection refreshes materialized view rollups.",
        )
    )
    source = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-covered",
                title="Prime cache keys",
                description="Cache priming seeds cache hot keys.",
                acceptance_criteria=[
                    "Warmup backfill plan covers chunks.",
                    "Stale data guard checks max age.",
                    "Load shedding uses batch size.",
                    "Cache miss fallback uses uncached path.",
                    "Monitoring metrics and alerts cover warmup failure.",
                    "Kill switch can disable warmup.",
                    "Service owner is data platform.",
                ],
            ),
        ],
        plan_id="plan-serialization",
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    result = summarize_task_cache_warmup_readiness(source)
    payload = task_cache_warmup_readiness_plan_to_dict(result)
    markdown = task_cache_warmup_readiness_plan_to_markdown(result)

    assert source == original
    assert build_task_cache_warmup_readiness_plan(object_task).records[0].task_id == "task-object"
    assert generate_task_cache_warmup_readiness(model).plan_id == "plan-serialization"
    assert derive_task_cache_warmup_readiness(source).to_dict() == result.to_dict()
    assert recommend_task_cache_warmup_readiness(source).to_dict() == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_cache_warmup_readiness_plan_to_dicts(result) == payload["records"]
    assert task_cache_warmup_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_cache_warmup_readiness_to_dicts(result) == payload["records"]
    assert task_cache_warmup_readiness_to_dicts(result.records) == payload["records"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Cache Warmup Readiness: plan-serialization")
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "cache_task_ids",
        "suppressed_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "signals",
        "safeguards",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "recommended_checks",
    ]


def _plan(tasks, *, plan_id="plan-cache-warmup"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-cache-warmup",
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
