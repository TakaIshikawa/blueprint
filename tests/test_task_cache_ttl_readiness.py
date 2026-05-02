import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_cache_ttl_readiness import (
    TaskCacheTTLReadinessPlan,
    TaskCacheTTLReadinessRecord,
    analyze_task_cache_ttl_readiness,
    build_task_cache_ttl_readiness_plan,
    derive_task_cache_ttl_readiness,
    generate_task_cache_ttl_readiness,
    recommend_task_cache_ttl_readiness,
    summarize_task_cache_ttl_readiness,
    task_cache_ttl_readiness_plan_to_dict,
    task_cache_ttl_readiness_plan_to_dicts,
    task_cache_ttl_readiness_plan_to_markdown,
)


def test_detects_cache_ttl_surfaces_from_text_paths_tags_and_metadata():
    result = build_task_cache_ttl_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Review cache expiry paths",
                    description="Move implementation files.",
                    files_or_modules=[
                        "src/redis/session_ttl.py",
                        "src/memcached/catalog_expiration.py",
                        "infra/cdn/cloudfront_cache_headers.tf",
                        "public/service-worker/browser-cache.js",
                        "src/object_cache/profile_cache.py",
                        "src/results/computed_result_cache.py",
                    ],
                    tags=["cache-ttl"],
                ),
                _task(
                    "task-text",
                    title="Add HTTP cache headers",
                    description=(
                        "Cache-Control and ETag behavior covers browser cache, CDN cache, "
                        "object cache, and computed result cache expiration."
                    ),
                    metadata={"redis": {"ttl": "30 minutes"}, "cache_surface": "memcached"},
                ),
            ]
        )
    )

    assert isinstance(result, TaskCacheTTLReadinessPlan)
    assert result.plan_id == "plan-cache-ttl"
    by_id = {record.task_id: record for record in result.records}
    assert set(by_id) == {"task-paths", "task-text"}
    assert set(by_id["task-paths"].cache_surfaces) == {
        "redis",
        "memcached",
        "cdn_cache",
        "browser_cache",
        "http_cache_headers",
        "object_cache",
        "computed_result_cache",
    }
    assert set(by_id["task-text"].cache_surfaces) == {
        "redis",
        "memcached",
        "cdn_cache",
        "browser_cache",
        "http_cache_headers",
        "object_cache",
        "computed_result_cache",
    }
    assert "explicit_ttl" in by_id["task-text"].present_safeguards
    assert any("files_or_modules:" in item for item in by_id["task-paths"].evidence)
    assert any("tags[0]:" in item for item in by_id["task-paths"].evidence)
    assert any("metadata.redis" in item for item in by_id["task-text"].evidence)


def test_strong_readiness_requires_all_safeguards_and_weak_when_ttl_absent():
    result = analyze_task_cache_ttl_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Harden Redis TTL cache",
                    description="Redis cache for tenant-scoped account data.",
                    acceptance_criteria=[
                        "Explicit TTL is 15 minutes with Cache-Control max-age.",
                        "Invalidation trigger expires keys after profile changes.",
                        "Stale-while-revalidate serves stale data only during background refresh.",
                        "Tenant scoped cache key includes user and workspace ids.",
                        "Emergency purge command is documented in a runbook.",
                        "Metrics, dashboard, and alerts track cache hit rate and stale age.",
                        "Origin fallback and uncached path handle cache miss behavior.",
                        "Integration tests cover TTL expiry and stale data behavior.",
                    ],
                ),
                _task(
                    "task-weak",
                    title="Add CDN cache",
                    description="CDN cache and browser cache for public catalog responses.",
                    acceptance_criteria=[
                        "Invalidation trigger runs on publish.",
                        "Cache hit rate metrics and alerts are visible.",
                    ],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    strong = by_id["task-strong"]
    weak = by_id["task-weak"]

    assert strong.present_safeguards == (
        "explicit_ttl",
        "invalidation_trigger",
        "stale_while_revalidate",
        "tenant_user_key_scope",
        "purge_tooling",
        "observability",
        "fallback_behavior",
        "test_coverage",
    )
    assert strong.missing_safeguards == ()
    assert strong.readiness == "strong"
    assert strong.recommended_checks == ()
    assert weak.readiness == "weak"
    assert "explicit_ttl" in weak.missing_safeguards
    assert "fallback_behavior" in weak.missing_safeguards
    assert weak.recommended_checks[0] == (
        "Define the TTL or cache-header lifetime for every affected cache surface."
    )
    assert result.cache_task_ids == ("task-weak", "task-strong")
    assert result.summary["readiness_counts"] == {"weak": 1, "moderate": 0, "strong": 1}
    assert result.summary["missing_safeguard_counts"]["explicit_ttl"] == 1


def test_moderate_readiness_when_ttl_present_but_other_safeguards_missing():
    result = build_task_cache_ttl_readiness_plan(
        _plan(
            [
                _task(
                    "task-moderate",
                    title="Set memcached TTL",
                    description="Memcached result cache uses TTL of 10 minutes.",
                    acceptance_criteria=["Unit tests verify TTL expiry."],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "moderate"
    assert "explicit_ttl" in record.present_safeguards
    assert "test_coverage" in record.present_safeguards
    assert "invalidation_trigger" in record.missing_safeguards
    assert record.risk_level == record.readiness


def test_unrelated_tasks_are_suppressed_and_empty_invalid_inputs_are_stable():
    result = build_task_cache_ttl_readiness_plan(
        _plan(
            [
                _task("task-copy", title="Update dashboard copy", description="Text only."),
                _task("task-cache", title="Add object cache TTL", description="Object cache expires after 1 hour."),
            ]
        )
    )
    empty = build_task_cache_ttl_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_cache_ttl_readiness_plan(13)

    assert result.cache_task_ids == ("task-cache",)
    assert result.suppressed_task_ids == ("task-copy",)
    assert result.summary["suppressed_task_count"] == 1
    assert empty.records == ()
    assert invalid.records == ()
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Cache TTL Readiness: empty-plan",
            "",
            "## Summary",
            "",
            "- Task count: 0",
            "- Cache task count: 0",
            "- Missing safeguard count: 0",
            "- Readiness counts: weak 0, moderate 0, strong 0",
            (
                "- Surface counts: redis 0, memcached 0, cdn_cache 0, browser_cache 0, "
                "http_cache_headers 0, object_cache 0, computed_result_cache 0, generic_cache 0"
            ),
            "",
            "No task cache TTL-readiness records were inferred.",
        ]
    )


def test_model_object_serialization_markdown_aliases_and_no_mutation_are_stable():
    object_task = SimpleNamespace(
        id="task-object",
        title="Set browser cache TTL",
        description="Browser cache uses Cache-Control max-age and stale-while-revalidate.",
        files_or_modules=["public/service-worker/cache_control.js"],
        acceptance_criteria=["Smoke tests verify cache header behavior."],
        metadata={"observability": "Cache status metric is emitted."},
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Set Redis TTL | scoped",
            description="Redis cache uses explicit TTL for tenant scoped keys.",
            acceptance_criteria=["Purge command runbook is documented."],
        )
    )
    plan = _plan(
        [
            model_task.model_dump(mode="python"),
                _task(
                    "task-a",
                    title="Add query cache",
                    description="Query cache memoized result cache has no lifetime details.",
                ),
        ],
        plan_id="plan-serialization",
    )
    original = copy.deepcopy(plan)

    result = summarize_task_cache_ttl_readiness(plan)
    object_result = build_task_cache_ttl_readiness_plan([object_task])
    model_result = generate_task_cache_ttl_readiness(ExecutionPlan.model_validate(plan))
    payload = task_cache_ttl_readiness_plan_to_dict(result)
    markdown = task_cache_ttl_readiness_plan_to_markdown(result)

    assert plan == original
    assert isinstance(result.records[0], TaskCacheTTLReadinessRecord)
    assert object_result.records[0].task_id == "task-object"
    assert model_result.plan_id == "plan-serialization"
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_cache_ttl_readiness_plan_to_dicts(result) == payload["records"]
    assert task_cache_ttl_readiness_plan_to_dicts(result.records) == payload["records"]
    assert derive_task_cache_ttl_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_cache_ttl_readiness(plan).to_dict() == result.to_dict()
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
        "cache_surfaces",
        "present_safeguards",
        "missing_safeguards",
        "readiness",
        "evidence",
        "recommended_checks",
    ]
    assert result.cache_task_ids == ("task-a", "task-model")
    assert markdown.startswith("# Task Cache TTL Readiness: plan-serialization")
    assert "Set Redis TTL \\| scoped" in markdown
    assert (
        "| Task | Title | Readiness | Cache Surfaces | Present Safeguards | Missing Safeguards | Evidence | Recommended Checks |"
        in markdown
    )


def _plan(tasks, plan_id="plan-cache-ttl"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-cache-ttl",
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
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-cache-ttl",
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
    return payload
