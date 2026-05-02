import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_cache_stampede_readiness import (
    TaskCacheStampedeReadinessPlan,
    TaskCacheStampedeReadinessRecord,
    analyze_task_cache_stampede_readiness,
    build_task_cache_stampede_readiness_plan,
    derive_task_cache_stampede_readiness,
    extract_task_cache_stampede_readiness,
    generate_task_cache_stampede_readiness,
    recommend_task_cache_stampede_readiness,
    summarize_task_cache_stampede_readiness,
    task_cache_stampede_readiness_plan_to_dict,
    task_cache_stampede_readiness_plan_to_dicts,
    task_cache_stampede_readiness_plan_to_markdown,
)


def test_detects_stampede_signals_from_text_paths_tags_metadata_and_validation_commands():
    result = build_task_cache_stampede_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Harden cache stampede paths",
                    description="Move implementation files.",
                    files_or_modules=[
                        "src/cache/dogpile/hot_key_refresh.py",
                        "src/cache/query_cache_expensive_recompute.py",
                        "src/db/database_fanout_cache.py",
                        "src/traffic/qps_spike_cache.py",
                    ],
                    tags=["cache-stampede"],
                    validation_commands={"test": ["pytest tests/cache/test_singleflight_ttl_jitter.py"]},
                ),
                _task(
                    "task-metadata",
                    title="Protect launch cache",
                    description="Memoized result cache refresh can trigger a cache miss storm during launch traffic.",
                    metadata={
                        "hot_key": "Popular key is shared by all users.",
                        "ttl_refresh": "Refresh window expires hourly.",
                        "request_coalescing": "Single-flight request coalescing is implemented.",
                    },
                ),
            ]
        )
    )

    assert isinstance(result, TaskCacheStampedeReadinessPlan)
    assert result.plan_id == "plan-cache-stampede"
    by_id = {record.task_id: record for record in result.records}
    assert set(by_id) == {"task-paths", "task-metadata"}
    assert set(by_id["task-paths"].detected_signals) == {
        "cache_miss_storm",
        "hot_key",
        "ttl_refresh",
        "memoization",
        "expensive_recomputation",
        "database_fanout",
        "traffic_spike",
    }
    assert {"cache_miss_storm", "hot_key", "ttl_refresh", "memoization", "traffic_spike"} <= set(
        by_id["task-metadata"].detected_signals
    )
    assert "request_coalescing" in by_id["task-metadata"].present_safeguards
    assert "jittered_ttls" in by_id["task-paths"].present_safeguards
    assert any("files_or_modules:" in item for item in by_id["task-paths"].evidence)
    assert any("tags[0]:" in item for item in by_id["task-paths"].evidence)
    assert any("metadata.hot_key" in item for item in by_id["task-metadata"].evidence)
    assert any("validation_commands:" in item for item in by_id["task-paths"].evidence)


def test_high_medium_low_risk_and_recommended_actions_are_inferred():
    result = analyze_task_cache_stampede_readiness(
        _plan(
            [
                _task(
                    "task-low",
                    title="Ready hot key cache",
                    description="Hot key query cache handles traffic spikes and database fanout.",
                    acceptance_criteria=[
                        "Request coalescing uses a per-key single-flight lock.",
                        "Jittered TTLs spread expiration.",
                        "Stale-while-revalidate serves bounded stale data during background refresh.",
                        "Prewarming primes the hot cache before launch.",
                        "Rate limiting and backpressure protect origin reads.",
                        "Metrics, dashboard, and alerts track miss rate, hot keys, and origin QPS.",
                    ],
                ),
                _task(
                    "task-medium",
                    title="Memoized report cache refresh",
                    description="Memoization avoids expensive recomputation after TTL refresh.",
                    acceptance_criteria=[
                        "Request coalescing uses singleflight.",
                        "Stale cache is served while background refresh repopulates.",
                        "Metrics alert on cache miss rate.",
                    ],
                ),
                _task(
                    "task-high",
                    title="Cache miss storm for launch traffic",
                    description="Cold cache may cause a cache miss storm, database fanout, and traffic spike.",
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert by_id["task-low"].risk_level == "low"
    assert by_id["task-low"].missing_safeguards == ()
    assert by_id["task-low"].recommended_actions == ()
    assert by_id["task-medium"].risk_level == "medium"
    assert by_id["task-medium"].missing_safeguards == ("jittered_ttls", "prewarming", "rate_limiting")
    assert by_id["task-high"].risk_level == "high"
    assert by_id["task-high"].missing_safeguards == (
        "request_coalescing",
        "jittered_ttls",
        "stale_while_revalidate",
        "prewarming",
        "rate_limiting",
        "observability",
    )
    assert by_id["task-high"].recommended_actions[0].startswith("Add request coalescing")
    assert by_id["task-high"].recommendations == by_id["task-high"].recommended_actions
    assert by_id["task-high"].recommended_checks == by_id["task-high"].recommended_actions
    assert result.impacted_task_ids == ("task-high", "task-medium", "task-low")
    assert result.summary["risk_counts"] == {"high": 1, "medium": 1, "low": 1}
    assert result.summary["missing_safeguard_counts"]["request_coalescing"] == 1


def test_no_impact_invalid_and_empty_inputs_have_stable_markdown():
    result = build_task_cache_stampede_readiness_plan(
        _plan(
            [
                _task("task-copy", title="Update dashboard copy", description="Text only."),
                _task(
                    "task-cache",
                    title="Add cache stampede guard",
                    description="Cache stampede protection uses singleflight request coalescing.",
                ),
            ]
        )
    )
    empty = build_task_cache_stampede_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_cache_stampede_readiness_plan(13)
    no_signal = build_task_cache_stampede_readiness_plan(
        _plan([_task("task-copy", title="Update helper copy", description="Static text only.")])
    )

    assert result.impacted_task_ids == ("task-cache",)
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary["no_impact_task_ids"] == ["task-copy"]
    assert empty.records == ()
    assert invalid.records == ()
    assert no_signal.records == ()
    assert no_signal.no_impact_task_ids == ("task-copy",)
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Cache Stampede Readiness: empty-plan",
            "",
            "## Summary",
            "",
            "- Task count: 0",
            "- Impacted task count: 0",
            "- Missing safeguard count: 0",
            "- Risk counts: high 0, medium 0, low 0",
            (
                "- Signal counts: cache_miss_storm 0, hot_key 0, ttl_refresh 0, memoization 0, "
                "expensive_recomputation 0, database_fanout 0, traffic_spike 0"
            ),
            "",
            "No task cache stampede-readiness records were inferred.",
        ]
    )
    assert "No-impact tasks: task-copy" in no_signal.to_markdown()


def test_model_objects_serialization_markdown_aliases_and_no_mutation_are_stable():
    object_task = SimpleNamespace(
        id="task-object",
        title="Prewarm hot key cache",
        description="Hot key cache is prewarmed before launch traffic.",
        files_or_modules=["src/cache/hotkey_prewarm.py"],
        acceptance_criteria=["Observability tracks cache hit rate and miss rate."],
        metadata={"rate_limiting": "Concurrency limit protects origin."},
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Set TTL refresh guard | launch",
            description="TTL refresh for query cache can trigger expensive recomputation.",
            acceptance_criteria=["Jittered TTL and stale-while-revalidate are configured."],
        )
    )
    plan = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-a",
                title="Cache miss storm guard",
                description="Cache miss storm protection for database fanout uses request coalescing.",
            ),
            _task("task-copy", title="Copy update", description="Update helper text."),
        ],
        plan_id="plan-serialization",
    )
    original = copy.deepcopy(plan)

    result = summarize_task_cache_stampede_readiness(plan)
    object_result = build_task_cache_stampede_readiness_plan([object_task])
    model_result = generate_task_cache_stampede_readiness(ExecutionPlan.model_validate(plan))
    payload = task_cache_stampede_readiness_plan_to_dict(result)
    markdown = task_cache_stampede_readiness_plan_to_markdown(result)

    assert plan == original
    assert isinstance(result.records[0], TaskCacheStampedeReadinessRecord)
    assert object_result.records[0].task_id == "task-object"
    assert model_result.plan_id == "plan-serialization"
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_cache_stampede_readiness_plan_to_dicts(result) == payload["records"]
    assert task_cache_stampede_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_cache_stampede_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_cache_stampede_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_cache_stampede_readiness(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "recommended_actions",
        "evidence",
    ]
    assert result.impacted_task_ids == ("task-a", "task-model")
    assert result.no_impact_task_ids == ("task-copy",)
    assert analyze_task_cache_stampede_readiness(plan).to_dict() == result.to_dict()
    assert markdown.startswith("# Task Cache Stampede Readiness: plan-serialization")
    assert "Set TTL refresh guard \\| launch" in markdown
    assert (
        "| Task | Title | Risk | Detected Signals | Present Safeguards | Missing Safeguards | Recommended Actions | Evidence |"
        in markdown
    )


def _plan(tasks, plan_id="plan-cache-stampede"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-cache-stampede",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    tags=None,
    metadata=None,
    validation_commands=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-cache-stampede",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        payload["tags"] = tags
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    return payload
