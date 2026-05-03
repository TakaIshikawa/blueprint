import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_cache_invalidation_readiness_matrix import (
    PlanCacheInvalidationReadinessMatrix,
    PlanCacheInvalidationReadinessRow,
    analyze_plan_cache_invalidation_readiness_matrix,
    build_plan_cache_invalidation_readiness_matrix,
    derive_plan_cache_invalidation_readiness_matrix,
    extract_plan_cache_invalidation_readiness_matrix,
    generate_plan_cache_invalidation_readiness_matrix,
    plan_cache_invalidation_readiness_matrix_to_dict,
    plan_cache_invalidation_readiness_matrix_to_dicts,
    plan_cache_invalidation_readiness_matrix_to_markdown,
    summarize_plan_cache_invalidation_readiness_matrix,
)


def test_cache_invalidation_tasks_group_by_surface_with_evidence_and_stable_ordering():
    result = build_plan_cache_invalidation_readiness_matrix(
        _plan(
            [
                _task(
                    "task-product-purge",
                    title="Invalidate product catalog cache",
                    description="Invalidate the product_catalog Redis cache when product write events publish.",
                    acceptance_criteria=[
                        "Owner: Catalog platform team.",
                        "TTL staleness policy keeps stale data under five minutes.",
                        "Dependency ordering runs after the database commit and before downstream search indexing.",
                        "Backfill warming preloads hot product pages after purge.",
                        "Observability dashboard tracks purge outcomes, hit rate, stale rate, and alerts.",
                        "Rollback fallback bypasses cache and restores the prior namespace.",
                        "Customer-visible impact covers stale product data for tenants.",
                    ],
                    metadata={"cache_keyspace": "product_catalog"},
                ),
                _task(
                    "task-product-warm",
                    title="Warm product_catalog cache after invalidation",
                    description="Prewarm product_catalog cache keys after the purge event.",
                    acceptance_criteria=["Owner: Catalog SRE and fallback runbook are documented."],
                ),
                _task(
                    "task-cdn-purge",
                    title="Purge CDN cache for marketing pages",
                    description="Purge the marketing_pages CDN cache on deploy trigger.",
                    acceptance_criteria=[
                        "Owner: Web platform.",
                        "TTL and stale-while-revalidate policy are documented.",
                        "Dependency ordering waits for asset publish.",
                        "Backfill warming primes landing pages.",
                        "Monitoring alerts cover purge outcome and miss rate.",
                        "Rollback fallback uses bypass cache.",
                        "Customer impact notes visible stale content.",
                    ],
                    metadata={"cache_surface": "marketing_pages"},
                ),
                _task("task-copy", title="Update help copy", description="Refresh settings labels."),
            ]
        )
    )

    assert isinstance(result, PlanCacheInvalidationReadinessMatrix)
    assert all(isinstance(row, PlanCacheInvalidationReadinessRow) for row in result.rows)
    assert result.plan_id == "plan-cache-invalidation"
    assert result.cache_task_ids == ("task-cdn-purge", "task-product-purge", "task-product-warm")
    assert result.no_cache_task_ids == ("task-copy",)
    assert [row.cache_surface for row in result.rows] == ["marketing_pages", "product_catalog"]

    product = _row(result, "product_catalog")
    assert product.task_ids == ("task-product-purge", "task-product-warm")
    assert product.cache_keyspace == "product_catalog"
    assert product.readiness == "ready"
    assert product.severity == "low"
    assert product.gaps == ()
    assert any("product_catalog" in item for item in product.evidence)

    cdn = _row(result, "marketing_pages")
    assert cdn.readiness == "ready"
    assert cdn.observability == "present"


def test_partial_and_blocked_rows_report_actionable_gaps_and_summary_counts():
    result = build_plan_cache_invalidation_readiness_matrix(
        _plan(
            [
                _task(
                    "task-missing-trigger",
                    title="Invalidate account_summary cache",
                    description="Invalidate account_summary cache with TTL and cache warming.",
                    acceptance_criteria=[
                        "Owner: Accounts team.",
                        "Observability metrics and alerts watch stale rate.",
                        "Rollback fallback bypasses cache.",
                        "Customer impact covers stale account totals.",
                    ],
                ),
                _task(
                    "task-partial",
                    title="Evict profile_card cache",
                    description="Owner profile team evicts profile_card cache on user update trigger.",
                    acceptance_criteria=[
                        "TTL staleness policy is five minutes.",
                        "Dependency ordering waits for profile writes.",
                        "Rollback fallback uses uncached path.",
                        "Customer-visible impact is stale profile details.",
                    ],
                ),
            ]
        )
    )

    blocked = _row(result, "account_summary")
    assert blocked.readiness == "blocked"
    assert blocked.severity == "high"
    assert "Missing invalidation trigger." in blocked.gaps
    assert "Missing dependency ordering." in blocked.gaps

    partial = _row(result, "profile_card")
    assert partial.readiness == "partial"
    assert partial.severity == "medium"
    assert partial.gaps == ("Missing backfill or warming plan.", "Missing observability or alerting.")
    assert result.summary["readiness_counts"] == {"blocked": 1, "partial": 1, "ready": 0}
    assert result.summary["severity_counts"] == {"high": 1, "medium": 1, "low": 0}
    assert result.summary["surface_counts"] == {"account_summary": 1, "profile_card": 1}


def test_no_cache_invalidation_signals_return_empty_rows_and_stable_summary_counts():
    result = build_plan_cache_invalidation_readiness_matrix(
        _plan(
            [
                _task("task-api", title="Build API endpoint", description="Implement normal CRUD behavior."),
                _task("task-docs", title="Document endpoint", description="Update docs."),
            ]
        )
    )

    assert result.rows == ()
    assert result.cache_task_ids == ()
    assert result.no_cache_task_ids == ("task-api", "task-docs")
    assert result.summary == {
        "task_count": 2,
        "row_count": 0,
        "cache_task_count": 0,
        "no_cache_task_count": 2,
        "readiness_counts": {"blocked": 0, "partial": 0, "ready": 0},
        "severity_counts": {"high": 0, "medium": 0, "low": 0},
        "gap_counts": {},
        "surface_counts": {},
    }
    assert "No cache invalidation readiness rows were inferred." in result.to_markdown()
    assert "No cache invalidation signals: task-api, task-docs" in result.to_markdown()


def test_serialization_aliases_markdown_model_object_input_invalid_input_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-cache | purge",
                title="Invalidate billing | portal cache",
                description="Invalidate the billing_portal cache when invoice update events trigger.",
                acceptance_criteria=[
                    "Owner: Billing platform.",
                    "TTL staleness policy, dependency ordering, backfill warming, observability alerts, rollback fallback, and customer impact are documented.",
                ],
            )
        ]
    )
    original = copy.deepcopy(plan)
    model_plan = ExecutionPlan.model_validate(plan)

    result = build_plan_cache_invalidation_readiness_matrix(model_plan)
    payload = plan_cache_invalidation_readiness_matrix_to_dict(result)
    markdown = plan_cache_invalidation_readiness_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_cache_invalidation_readiness_matrix(plan).to_dict() == result.to_dict()
    assert analyze_plan_cache_invalidation_readiness_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_cache_invalidation_readiness_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_cache_invalidation_readiness_matrix(plan).to_dict() == result.to_dict()
    assert summarize_plan_cache_invalidation_readiness_matrix(result) == result.summary
    assert plan_cache_invalidation_readiness_matrix_to_dicts(result) == payload["rows"]
    assert plan_cache_invalidation_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "cache_task_ids",
        "no_cache_task_ids",
        "summary",
    ]
    assert "billing \\| portal" in markdown
    assert "task-cache \\| purge" in markdown
    assert payload["rows"][0]["cache_keyspace"] == payload["rows"][0]["cache_surface"]

    object_result = build_plan_cache_invalidation_readiness_matrix(
        SimpleNamespace(
            id="object-cache",
            title="Invalidate object_cache keyspace",
            description="Owner invalidates object_cache when update events trigger.",
            acceptance_criteria=[
                "TTL staleness policy, dependency ordering, backfill warming, observability alerts, rollback fallback, and customer impact are ready."
            ],
        )
    )
    invalid = build_plan_cache_invalidation_readiness_matrix(23)

    assert object_result.rows[0].task_ids == ("object-cache",)
    assert object_result.rows[0].readiness == "ready"
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0


def _row(result, surface):
    return next(row for row in result.rows if row.cache_surface == surface)


def _plan(tasks, *, plan_id="plan-cache-invalidation"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-cache-invalidation",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    depends_on=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "depends_on": [] if depends_on is None else depends_on,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
