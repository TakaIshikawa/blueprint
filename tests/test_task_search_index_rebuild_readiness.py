import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_search_index_rebuild_readiness import (
    build_task_search_index_rebuild_readiness_plan,
    recommend_task_search_index_rebuild_readiness,
    summarize_task_search_index_rebuild_readiness,
    task_search_index_rebuild_readiness_plan_to_dict,
    task_search_index_rebuild_readiness_plan_to_dicts,
    task_search_index_rebuild_readiness_plan_to_markdown,
)


def test_detects_search_index_rebuild_and_all_required_safeguards():
    result = build_task_search_index_rebuild_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Rebuild product search index",
                    description="Full reindex rebuilds the search index before cutover.",
                    acceptance_criteria=[
                        "Index versioning uses versioned index names and index aliases.",
                        "Dual write or backfill plan uses CDC watermark and batch plan.",
                        "Query parity checks compare sample queries, hit count, and facet parity.",
                        "Cutover rollback swaps back to the previous index alias behind a feature flag.",
                        "Capacity planning covers shards, replicas, bulk size, throughput, and throttling.",
                        "Stale index monitoring tracks freshness lag, indexing lag, dashboards, and alerts.",
                    ],
                    files_or_modules=["src/search/rebuild_index.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == ("search_index_rebuild", "reindex")
    assert record.present_criteria == (
        "index_versioning",
        "dual_write_or_backfill_plan",
        "query_parity_checks",
        "cutover_rollback",
        "capacity_planning",
        "stale_index_monitoring",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_metadata_dependencies_and_paths_report_missing_search_safeguards():
    source = _plan(
        [
            _task(
                "task-partial",
                title="Backfill search documents",
                description="Search backfill populates the new OpenSearch mapping.",
                depends_on=["query parity validation"],
                metadata={"capacity": {"plan": "cluster capacity and shard count reviewed"}},
                files_or_modules=["src/search/index_backfill_worker.py"],
            ),
            _task("task-copy", title="Search copy", description="No search index rebuild is required."),
        ]
    )

    result = build_task_search_index_rebuild_readiness_plan(ExecutionPlan.model_validate(source))

    assert result.impacted_task_ids == ("task-partial",)
    assert result.ignored_task_ids == ("task-copy",)
    record = result.records[0]
    assert record.detected_signals == ("search_index_rebuild", "search_backfill")
    assert record.present_criteria == (
        "dual_write_or_backfill_plan",
        "query_parity_checks",
        "capacity_planning",
    )
    assert record.missing_criteria == (
        "index_versioning",
        "cutover_rollback",
        "stale_index_monitoring",
    )
    assert any("depends_on" in item for item in record.evidence)
    assert any("metadata.capacity.plan" in item for item in record.evidence)


def test_aliases_serialization_sorting_and_invalid_inputs_are_stable():
    source = _plan(
        [
            _task("task-missing", title="Reindex all invoices", description="Reindex the invoice search index."),
            _task(
                "task-partial",
                title="Search index migration",
                description="Index migration has index versioning and alias rollback.",
            ),
        ],
        plan_id="plan-search-sort",
    )

    result = summarize_task_search_index_rebuild_readiness(source)
    payload = task_search_index_rebuild_readiness_plan_to_dict(result)
    markdown = task_search_index_rebuild_readiness_plan_to_markdown(result)

    assert [record.task_id for record in result.records] == ["task-missing", "task-partial"]
    assert recommend_task_search_index_rebuild_readiness(source).to_dict() == result.to_dict()
    assert task_search_index_rebuild_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert markdown.startswith("# Task Search Index Rebuild Readiness: plan-search-sort")
    assert build_task_search_index_rebuild_readiness_plan(42).records == ()
    assert build_task_search_index_rebuild_readiness_plan({"tasks": "bad"}).records == ()


def _plan(tasks, *, plan_id="plan-search-index-rebuild"):
    return {"id": plan_id, "implementation_brief_id": "brief-search-index-rebuild", "milestones": [], "tasks": tasks}


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    depends_on=None,
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
    if depends_on is not None:
        task["depends_on"] = depends_on
    if metadata is not None:
        task["metadata"] = metadata
    return task
