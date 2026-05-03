import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_search_reindex_backfill_readiness import (
    TaskSearchReindexBackfillReadinessPlan,
    TaskSearchReindexBackfillReadinessRecord,
    analyze_task_search_reindex_backfill_readiness,
    build_task_search_reindex_backfill_readiness,
    build_task_search_reindex_backfill_readiness_plan,
    derive_task_search_reindex_backfill_readiness_plan,
    generate_task_search_reindex_backfill_readiness_plan,
    summarize_task_search_reindex_backfill_readiness,
    task_search_reindex_backfill_readiness_to_dict,
    task_search_reindex_backfill_readiness_to_dicts,
    task_search_reindex_backfill_readiness_to_markdown,
)


def test_detects_search_reindex_work_and_high_risk_missing_core_safeguards():
    result = build_task_search_reindex_backfill_readiness_plan(
        _plan(
            [
                _task(
                    "task-risk",
                    title="Reindex Elasticsearch tasks",
                    description="Bulk reindex the task search index in Elasticsearch for the new index schema.",
                    acceptance_criteria=["Existing tasks are searchable after the migration."],
                ),
                _task("task-copy", title="Update onboarding copy", description="Polish dashboard labels."),
            ]
        )
    )

    assert isinstance(result, TaskSearchReindexBackfillReadinessPlan)
    assert result.impacted_task_ids == ("task-risk",)
    assert result.no_impact_task_ids == ("task-copy",)
    record = result.readiness_records[0]
    assert isinstance(record, TaskSearchReindexBackfillReadinessRecord)
    assert record.matched_signals == (
        "search_index",
        "elasticsearch",
        "reindex",
        "index_migration",
    )
    assert record.risk_level == "high"
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "resumable_jobs",
        "alias_cutover",
        "stale_read_tolerance",
        "throttling",
        "rollback",
        "validation_counts",
        "monitoring",
    )
    assert any("document counts" in check for check in record.recommended_checks)
    assert result.summary["high_risk_count"] == 1
    assert result.summary["signal_counts"]["reindex"] == 1


def test_present_and_missing_safeguards_are_categorized_deterministically():
    result = build_task_search_reindex_backfill_readiness_plan(
        [
            _task(
                "task-safe",
                title="Backfill Algolia index with alias cutover",
                description=(
                    "Backfill historical records into Algolia with a dual-write window, query compatibility checks, "
                    "and stale results tolerated during eventual consistency."
                ),
                acceptance_criteria=[
                    "Job resumes from a checkpoint cursor.",
                    "Read alias and write alias cutover is atomic.",
                    "Throttle with batch size limits and off-peak pacing.",
                    "Rollback swaps back to the old index snapshot.",
                    "Validation compares document counts and sampled query parity.",
                    "Monitoring dashboard alerts on progress, lag, and query errors.",
                ],
            )
        ]
    )

    record = result.records[0]
    assert record.risk_level == "low"
    assert record.matched_signals == (
        "algolia",
        "backfill",
        "dual_write",
        "alias_cutover",
        "query_compatibility",
    )
    assert record.required_safeguards == (
        "resumable_jobs",
        "alias_cutover",
        "stale_read_tolerance",
        "throttling",
        "rollback",
        "validation_counts",
        "monitoring",
    )
    assert record.present_safeguards == record.required_safeguards
    assert record.missing_safeguards == ()
    assert result.summary["present_safeguard_counts"]["alias_cutover"] == 1
    assert result.summary["missing_safeguard_counts"]["rollback"] == 0


def test_path_metadata_and_plan_context_detect_search_providers_and_safeguards_without_mutation():
    plan = _plan(
        [
            _task(
                "task-path",
                title="Build semantic catalog migration",
                description="Move catalog search to a vector index.",
                files_or_modules=["jobs/search/opensearch/reindex_vector_catalog.py"],
                metadata={
                    "index_migration": {
                        "provider": "OpenSearch",
                        "validation_counts": "Compare doc count parity before cutover.",
                    }
                },
            ),
            _task("task-ui", title="Render settings", description="Update preferences UI."),
        ],
        risks=[
            "Use stale-read tolerance during query compatibility checks.",
            "Monitoring alerts must cover indexing lag.",
        ],
    )
    original = copy.deepcopy(plan)

    result = build_task_search_reindex_backfill_readiness_plan(plan)

    assert plan == original
    record = result.readiness_records[0]
    assert record.task_id == "task-path"
    assert {"search_index", "opensearch", "vector_index", "reindex", "index_migration"} <= set(
        record.matched_signals
    )
    assert "stale_read_tolerance" in record.present_safeguards
    assert "validation_counts" in record.present_safeguards
    assert "monitoring" in record.present_safeguards
    assert "alias_cutover" in record.missing_safeguards
    assert "files_or_modules: jobs/search/opensearch/reindex_vector_catalog.py" in record.evidence
    assert result.no_impact_task_ids == ("task-ui",)


def test_model_inputs_aliases_markdown_and_serialization_are_stable():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Meilisearch reindex | compatibility",
                    description="Re-index Meilisearch documents and keep query compatibility.",
                    acceptance_criteria=[
                        "Checkpoint resume is supported.",
                        "Alias cutover swaps reads atomically.",
                        "Throttling uses chunk size limits.",
                        "Rollback returns to the previous index.",
                        "Validation checks document count parity.",
                        "Monitoring emits progress metrics.",
                    ],
                ),
                _task("task-none", title="Adjust help text", description="No search changes."),
            ],
            plan_id="plan-search-model",
        )
    )
    task = ExecutionTask.model_validate(
        _task(
            "task-single",
            title="Solr backfill",
            description="Backfill Solr index with validation counts and monitoring.",
        )
    )

    result = build_task_search_reindex_backfill_readiness(plan)
    generated = generate_task_search_reindex_backfill_readiness_plan(plan)
    derived = derive_task_search_reindex_backfill_readiness_plan(result)
    analyzed = analyze_task_search_reindex_backfill_readiness(plan)
    summarized = summarize_task_search_reindex_backfill_readiness(task)
    payload = task_search_reindex_backfill_readiness_to_dict(result)
    markdown = task_search_reindex_backfill_readiness_to_markdown(result)

    assert result.plan_id == "plan-search-model"
    assert generated.to_dict() == result.to_dict()
    assert derived is result
    assert analyzed == result.readiness_records
    assert summarized["impacted_task_count"] == 1
    assert result.records == result.readiness_records
    assert result.recommendations == result.readiness_records
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["readiness_records"]
    assert task_search_reindex_backfill_readiness_to_dicts(result) == payload["readiness_records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "readiness_records",
        "records",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["readiness_records"][0]) == [
        "task_id",
        "title",
        "matched_signals",
        "required_safeguards",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "recommended_checks",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Search Reindex Backfill Readiness: plan-search-model")
    assert "Meilisearch reindex \\| compatibility" in markdown
    assert "- Impacted task IDs: task-model" in markdown
    assert "- No-impact task IDs: task-none" in markdown


def test_no_match_behavior_invalid_input_and_sorting_are_stable():
    result = build_task_search_reindex_backfill_readiness_plan(
        _plan(
            [
                _task(
                    "task-low",
                    title="OpenSearch index migration with safeguards",
                    description="Migrate OpenSearch index with query compatibility and stale results tolerated.",
                    acceptance_criteria=[
                        "Checkpoint resume, alias cutover, throttle limits, rollback, validation counts, and monitoring."
                    ],
                ),
                _task(
                    "task-high",
                    title="Full reindex search index",
                    description="Full reindex of the search index.",
                ),
                _task("task-none", title="Style admin table", description="Update CSS."),
            ]
        )
    )
    empty = build_task_search_reindex_backfill_readiness_plan(
        [_task("task-copy", title="Copy update", description="Update labels.")]
    )
    invalid = build_task_search_reindex_backfill_readiness_plan({"id": "bad", "tasks": "not a list"})

    assert [record.task_id for record in result.readiness_records] == ["task-high", "task-low"]
    assert result.summary["no_impact_task_ids"] == ["task-none"]
    assert empty.readiness_records == ()
    assert empty.impacted_task_ids == ()
    assert empty.no_impact_task_ids == ("task-copy",)
    assert "No search reindex backfill readiness records were inferred." in empty.to_markdown()
    assert invalid.readiness_records == ()
    assert invalid.summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-search", risks=None, acceptance_criteria=None):
    plan = {
        "id": plan_id,
        "implementation_brief_id": "brief-search",
        "milestones": [],
        "tasks": tasks,
    }
    if risks is not None:
        plan["risks"] = risks
    if acceptance_criteria is not None:
        plan["acceptance_criteria"] = acceptance_criteria
    return plan


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
