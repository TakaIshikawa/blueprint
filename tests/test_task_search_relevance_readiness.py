import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_search_relevance_readiness import (
    TaskSearchRelevanceReadinessPlan,
    TaskSearchRelevanceReadinessRecord,
    build_task_search_relevance_readiness_plan,
    derive_task_search_relevance_readiness_plan,
    generate_task_search_relevance_readiness,
    summarize_task_search_relevance_readiness,
    task_search_relevance_readiness_plan_to_dict,
    task_search_relevance_readiness_plan_to_dicts,
    task_search_relevance_readiness_plan_to_markdown,
)


def test_relevance_tasks_missing_readiness_safeguards_are_flagged():
    result = build_task_search_relevance_readiness_plan(
        _plan(
            [
                _task(
                    "task-ranking",
                    title="Tune product search ranking",
                    description=(
                        "Change relevance scoring, boost weights, synonyms, typo tolerance, "
                        "facets, filters, stemming, and query analytics for product search."
                    ),
                    files_or_modules=["src/search/relevance/product_ranking.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert isinstance(result, TaskSearchRelevanceReadinessPlan)
    assert isinstance(record, TaskSearchRelevanceReadinessRecord)
    assert record.relevance_signals == (
        "ranking",
        "relevance",
        "stemming",
        "synonyms",
        "typo_tolerance",
        "facets",
        "filters",
        "indexing_weights",
        "query_analytics",
    )
    assert record.review_level == "elevated"
    assert record.required_safeguards == (
        "golden_queries",
        "offline_evaluation",
        "relevance_metrics",
        "rollback_plan",
        "index_rebuild_validation",
        "analytics_instrumentation",
        "manual_review",
    )
    assert record.present_safeguards == ("analytics_instrumentation",)
    assert record.missing_safeguards == (
        "golden_queries",
        "offline_evaluation",
        "relevance_metrics",
        "rollback_plan",
        "index_rebuild_validation",
        "manual_review",
    )
    assert record.readiness_level == "needs_safeguards"
    assert any("golden query set" in action for action in record.recommended_actions)
    assert result.summary["relevance_task_count"] == 1
    assert result.summary["missing_safeguard_count"] == 6


def test_embedding_and_personalization_signals_need_sensitive_review():
    result = build_task_search_relevance_readiness_plan(
        _plan(
            [
                _task(
                    "task-embedding",
                    title="Add semantic search embeddings",
                    description=(
                        "Use embeddings and hybrid vector search for support docs. "
                        "Run offline evaluation with NDCG and a golden query set. "
                        "Rollback uses the previous query path."
                    ),
                ),
                _task(
                    "task-personalized",
                    title="Personalized search ranking",
                    description=(
                        "Personalized user-specific ranking changes product order. "
                        "Search analytics and click-through events are instrumented."
                    ),
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert by_id["task-embedding"].review_level == "sensitive"
    assert by_id["task-embedding"].readiness_level == "needs_sensitive_review"
    assert "embeddings" in by_id["task-embedding"].relevance_signals
    assert "manual_review" in by_id["task-embedding"].missing_safeguards
    assert "index_rebuild_validation" in by_id["task-embedding"].missing_safeguards
    assert by_id["task-personalized"].review_level == "sensitive"
    assert by_id["task-personalized"].readiness_level == "needs_sensitive_review"
    assert "personalization" in by_id["task-personalized"].relevance_signals
    assert any("Require sensitive review" in action for action in by_id["task-personalized"].recommended_actions)
    assert result.summary["needs_sensitive_review_task_count"] == 2


def test_complete_safeguards_make_relevance_task_ready():
    record = generate_task_search_relevance_readiness(
        _task(
            "task-ready",
            title="Tune search synonyms",
            description=(
                "Synonym and ranking updates use a golden query set and offline evaluation. "
                "NDCG, MRR, precision, recall, and zero-result rate are quality gates. "
                "Rollback is handled by feature flag. Reindex validation checks document counts, "
                "mapping validation, and alias cutover. Search analytics instrumentation records "
                "query logs and click-through. Manual review approves the relevance change."
            ),
        )
    )[0]

    assert record.relevance_signals == ("ranking", "relevance", "synonyms", "query_analytics")
    assert record.present_safeguards == record.required_safeguards
    assert record.missing_safeguards == ()
    assert record.readiness_level == "ready"
    assert record.recommended_actions == (
        "Ready to implement after preserving the documented relevance safeguards.",
    )


def test_unrelated_indexing_only_maintenance_is_ignored_unless_relevance_signals_are_present():
    result = build_task_search_relevance_readiness_plan(
        _plan(
            [
                _task(
                    "task-index-only",
                    title="Refresh account indexing pipeline",
                    description="Incremental document upsert and index freshness maintenance for search index.",
                    files_or_modules=["src/search/indexing/account_indexer.py"],
                ),
                _task(
                    "task-index-weight",
                    title="Adjust indexing weights",
                    description="Change indexed field weights for title and description scoring.",
                    files_or_modules=["src/search/indexing/field_weights.py"],
                ),
            ],
            plan_id="plan-index-ignore",
        )
    )

    assert result.ignored_task_ids == ("task-index-only",)
    assert tuple(record.task_id for record in result.records) == ("task-index-weight",)
    assert result.records[0].relevance_signals == ("ranking", "indexing_weights")


def test_model_object_inputs_no_mutation_aliases_serialization_and_markdown_are_stable():
    task = _task(
        "task-model",
        title="Improve support search relevance | docs",
        description=(
            "Ranking updates include representative queries, offline eval, NDCG, rollback, "
            "search analytics instrumentation, and manual review."
        ),
    )
    original = copy.deepcopy(task)
    plan_model = ExecutionPlan.model_validate(_plan([task], plan_id="plan-model"))
    task_model = ExecutionTask.model_validate(task)
    object_task = SimpleNamespace(
        id="task-object",
        title="Add synonym readiness",
        description="Synonym changes need a golden query set and relevance metrics.",
        files_or_modules=["src/search/synonyms.py"],
        acceptance_criteria=["Rollback plan exists."],
        status="pending",
    )

    result = summarize_task_search_relevance_readiness(plan_model)
    direct = build_task_search_relevance_readiness_plan(task_model)
    object_records = generate_task_search_relevance_readiness([object_task])
    derived = derive_task_search_relevance_readiness_plan(result)
    payload = task_search_relevance_readiness_plan_to_dict(result)
    markdown = task_search_relevance_readiness_plan_to_markdown(result)

    assert task == original
    assert derived is result
    assert direct.records[0].task_id == "task-model"
    assert object_records[0].task_id == "task-object"
    assert result.to_dicts() == payload["records"]
    assert task_search_relevance_readiness_plan_to_dicts(result) == payload["records"]
    assert task_search_relevance_readiness_plan_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "records", "ignored_task_ids"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "relevance_signals",
        "review_level",
        "required_safeguards",
        "present_safeguards",
        "missing_safeguards",
        "readiness_level",
        "recommended_actions",
        "evidence",
    ]
    assert markdown.startswith("# Task Search Relevance Readiness Plan: plan-model")
    assert "Improve support search relevance \\| docs" in markdown


def test_invalid_input_empty_state_and_markdown_summary_are_stable():
    result = build_task_search_relevance_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Adjust labels and helper text.",
                ),
                _task(
                    "task-negated",
                    title="Search indexing maintenance",
                    description="No ranking, relevance, synonym, typo, facet, filter, embedding, personalization, or query analytics changes are in scope.",
                ),
            ],
            plan_id="plan-empty",
        )
    )
    empty = build_task_search_relevance_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_search_relevance_readiness_plan(37)

    assert result.records == ()
    assert result.ignored_task_ids == ("task-copy", "task-negated")
    assert result.summary == {
        "total_task_count": 2,
        "relevance_task_count": 0,
        "ignored_task_count": 2,
        "ready_task_count": 0,
        "needs_safeguards_task_count": 0,
        "needs_sensitive_review_task_count": 0,
        "missing_safeguard_count": 0,
        "signal_counts": {
            "ranking": 0,
            "relevance": 0,
            "stemming": 0,
            "synonyms": 0,
            "typo_tolerance": 0,
            "facets": 0,
            "filters": 0,
            "embeddings": 0,
            "indexing_weights": 0,
            "personalization": 0,
            "query_analytics": 0,
        },
        "missing_safeguard_counts": {
            "golden_queries": 0,
            "offline_evaluation": 0,
            "relevance_metrics": 0,
            "rollback_plan": 0,
            "index_rebuild_validation": 0,
            "analytics_instrumentation": 0,
            "manual_review": 0,
        },
    }
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert invalid.plan_id is None
    assert invalid.records == ()
    assert result.to_markdown() == (
        "# Task Search Relevance Readiness Plan: plan-empty\n\n"
        "## Summary\n\n"
        "- Total tasks: 2\n"
        "- Relevance tasks: 0\n"
        "- Ignored tasks: 2\n"
        "- Ready tasks: 0\n"
        "- Tasks needing safeguards: 0\n"
        "- Sensitive review tasks: 0\n"
        "- Missing safeguards: 0\n\n"
        "No search relevance readiness records were inferred."
    )


def _plan(tasks, *, plan_id="plan-search-relevance"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-search-relevance",
        "milestones": [{"name": "Launch"}],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    test_command=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if test_command is not None:
        task["test_command"] = test_command
    return task
