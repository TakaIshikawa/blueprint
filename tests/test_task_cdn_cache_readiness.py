import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_cdn_cache_readiness import (
    TaskCdnCacheReadinessPlan,
    TaskCdnCacheReadinessRecord,
    analyze_task_cdn_cache_readiness,
    build_task_cdn_cache_readiness_plan,
    derive_task_cdn_cache_readiness,
    extract_task_cdn_cache_readiness,
    generate_task_cdn_cache_readiness,
    recommend_task_cdn_cache_readiness,
    summarize_task_cdn_cache_readiness,
    task_cdn_cache_readiness_plan_to_dict,
    task_cdn_cache_readiness_plan_to_dicts,
    task_cdn_cache_readiness_plan_to_markdown,
)


def test_detects_cdn_edge_behaviors_from_text_paths_tags_and_metadata():
    result = build_task_cdn_cache_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Wire CloudFront edge cache configuration",
                    description="Move implementation files.",
                    files_or_modules=[
                        "infra/cdn/cloudfront_cache_control.tf",
                        "src/edge/surrogate_keys.py",
                        "src/cache/stale_while_revalidate.py",
                        "scripts/purge_invalidation.py",
                        "public/assets/versioned_manifest.json",
                        "infra/regional_edge_rollout.yaml",
                    ],
                    tags=["cdn-cache"],
                ),
                _task(
                    "task-text",
                    title="Add CDN cache headers",
                    description=(
                        "Fastly CDN and edge cache use Cache-Control, Surrogate-Control, "
                        "surrogate keys, stale-while-revalidate, signed URLs, asset versioning, "
                        "and regional edge rollout language."
                    ),
                    metadata={"cache_key_design": "Vary by locale and query string."},
                ),
            ]
        )
    )

    assert isinstance(result, TaskCdnCacheReadinessPlan)
    assert result.plan_id == "plan-cdn-cache"
    by_id = {record.task_id: record for record in result.records}
    assert set(by_id) == {"task-paths", "task-text"}
    assert set(by_id["task-paths"].cache_behaviors) == {
        "cdn_cache",
        "edge_cache",
        "http_cache_headers",
        "surrogate_keys",
        "stale_while_revalidate",
        "purge_invalidation",
        "asset_versioning",
        "regional_edge_rollout",
    }
    assert set(by_id["task-text"].cache_behaviors) == {
        "cdn_cache",
        "edge_cache",
        "http_cache_headers",
        "surrogate_keys",
        "stale_while_revalidate",
        "signed_urls",
        "asset_versioning",
        "regional_edge_rollout",
    }
    assert "cache_key_design" in by_id["task-text"].present_safeguards
    assert any("files_or_modules:" in item for item in by_id["task-paths"].evidence)
    assert any("tags[0]:" in item for item in by_id["task-paths"].evidence)
    assert any("metadata.cache_key_design" in item for item in by_id["task-text"].evidence)


def test_recommends_missing_safeguards_and_marks_strong_when_all_present():
    result = analyze_task_cdn_cache_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Harden CDN cache rollout",
                    description="CloudFront CDN cache for tenant scoped catalog responses.",
                    acceptance_criteria=[
                        "TTL policy sets Cache-Control max-age and s-maxage per response class.",
                        "Invalidation paths purge surrogate keys after publish and deploy events.",
                        "Private data protection uses no-store for authenticated user-specific responses.",
                        "Signed URL policy defines signature expiry and key rotation.",
                        "Rollout monitoring tracks cache hit rate, cache status, origin errors, and regional canaries.",
                        "Cache key design varies by locale and normalizes query string inputs.",
                        "Origin fallback, bypass cache, and stale-if-error behavior are exercised.",
                    ],
                ),
                _task(
                    "task-weak",
                    title="Enable edge cache",
                    description="Edge cache and CDN cache for public catalog responses.",
                    acceptance_criteria=["Cache hit rate dashboard is visible."],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    strong = by_id["task-strong"]
    weak = by_id["task-weak"]

    assert strong.present_safeguards == (
        "ttl_policy",
        "invalidation_paths",
        "private_data_protection",
        "signed_url_policy",
        "rollout_monitoring",
        "cache_key_design",
        "fallback_behavior",
    )
    assert strong.missing_safeguards == ()
    assert strong.readiness == "strong"
    assert strong.recommendations == ()
    assert weak.readiness == "weak"
    assert weak.missing_safeguards == (
        "ttl_policy",
        "invalidation_paths",
        "private_data_protection",
        "signed_url_policy",
        "cache_key_design",
        "fallback_behavior",
    )
    assert weak.recommendations[0] == (
        "Define explicit browser, CDN, and surrogate TTLs for each cached response class."
    )
    assert result.cache_task_ids == ("task-weak", "task-strong")
    assert result.summary["readiness_counts"] == {"weak": 1, "moderate": 0, "strong": 1}
    assert result.summary["missing_safeguard_counts"]["ttl_policy"] == 1


def test_moderate_readiness_when_core_safeguards_exist_but_optional_safeguards_are_missing():
    result = build_task_cdn_cache_readiness_plan(
        _plan(
            [
                _task(
                    "task-moderate",
                    title="Set CDN TTL and cache key",
                    description=(
                        "CDN cache uses explicit TTL policy, purge invalidation paths, "
                        "private data protection with no-store, and cache key design by Vary header."
                    ),
                    acceptance_criteria=["Integration tests verify cache header behavior."],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "moderate"
    assert "ttl_policy" in record.present_safeguards
    assert "invalidation_paths" in record.present_safeguards
    assert "private_data_protection" in record.present_safeguards
    assert "cache_key_design" in record.present_safeguards
    assert "signed_url_policy" in record.missing_safeguards
    assert "rollout_monitoring" in record.missing_safeguards
    assert record.risk_level == record.readiness


def test_unrelated_inputs_return_empty_deterministic_result():
    result = build_task_cdn_cache_readiness_plan(
        _plan(
            [
                _task("task-copy", title="Update dashboard copy", description="Text only."),
                _task("task-db", title="Add database migration", description="Backfill a reporting table."),
            ]
        )
    )
    empty = build_task_cdn_cache_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_cdn_cache_readiness_plan(13)

    assert result.records == ()
    assert result.cache_task_ids == ()
    assert result.suppressed_task_ids == ("task-copy", "task-db")
    assert result.summary["cache_task_count"] == 0
    assert empty.records == ()
    assert invalid.records == ()
    assert empty.to_markdown() == "\n".join(
        [
            "# Task CDN Cache Readiness: empty-plan",
            "",
            "## Summary",
            "",
            "- Task count: 0",
            "- CDN cache task count: 0",
            "- Missing safeguard count: 0",
            "- Readiness counts: weak 0, moderate 0, strong 0",
            (
                "- Behavior counts: cdn_cache 0, edge_cache 0, http_cache_headers 0, "
                "surrogate_keys 0, stale_while_revalidate 0, purge_invalidation 0, "
                "signed_urls 0, asset_versioning 0, regional_edge_rollout 0"
            ),
            "",
            "No CDN or edge-cache readiness records were inferred.",
        ]
    )


def test_model_object_serialization_markdown_aliases_and_no_mutation_are_stable():
    object_task = SimpleNamespace(
        id="task-object",
        title="Set edge cache TTL",
        description="Edge cache uses Cache-Control max-age and stale-while-revalidate.",
        files_or_modules=["infra/cdn/cache_control.tf"],
        acceptance_criteria=["Smoke tests verify cache header behavior."],
        metadata={"rollout_monitoring": "Cache status metric is emitted."},
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Configure CDN cache | scoped",
            description="CDN cache uses explicit TTL for tenant scoped cache key inputs.",
            acceptance_criteria=["Purge command runbook is documented."],
        )
    )
    plan = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-a",
                title="Add signed URL asset cache",
                description="Signed URLs protect private CDN assets with tokenized URL expiry.",
            ),
        ],
        plan_id="plan-serialization",
    )
    original = copy.deepcopy(plan)

    result = summarize_task_cdn_cache_readiness(plan)
    object_result = build_task_cdn_cache_readiness_plan([object_task])
    model_result = generate_task_cdn_cache_readiness(ExecutionPlan.model_validate(plan))
    payload = task_cdn_cache_readiness_plan_to_dict(result)
    markdown = task_cdn_cache_readiness_plan_to_markdown(result)

    assert plan == original
    assert isinstance(result.records[0], TaskCdnCacheReadinessRecord)
    assert object_result.records[0].task_id == "task-object"
    assert model_result.plan_id == "plan-serialization"
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_cdn_cache_readiness_plan_to_dicts(result) == payload["records"]
    assert task_cdn_cache_readiness_plan_to_dicts(result.records) == payload["records"]
    assert analyze_task_cdn_cache_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_cdn_cache_readiness(plan).to_dict() == result.to_dict()
    assert extract_task_cdn_cache_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_cdn_cache_readiness(plan).to_dict() == result.to_dict()
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
        "cache_behaviors",
        "present_safeguards",
        "missing_safeguards",
        "readiness",
        "evidence",
        "recommendations",
    ]
    assert result.cache_task_ids == ("task-a", "task-model")
    assert markdown.startswith("# Task CDN Cache Readiness: plan-serialization")
    assert "Configure CDN cache \\| scoped" in markdown
    assert (
        "| Task | Title | Readiness | Cache Behaviors | Present Safeguards | Missing Safeguards | Recommendations |"
        in markdown
    )


def _plan(tasks, plan_id="plan-cdn-cache"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-cdn-cache",
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
        "execution_plan_id": "plan-cdn-cache",
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
