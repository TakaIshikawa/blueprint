import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_search_index_rebuild_readiness import (
    TaskSearchIndexRebuildReadinessPlan,
    TaskSearchIndexRebuildReadinessRecord,
    analyze_task_search_index_rebuild_readiness,
    build_task_search_index_rebuild_readiness_plan,
    derive_task_search_index_rebuild_readiness,
    extract_task_search_index_rebuild_readiness,
    generate_task_search_index_rebuild_readiness,
    recommend_task_search_index_rebuild_readiness,
    summarize_task_search_index_rebuild_readiness,
    task_search_index_rebuild_readiness_plan_to_dict,
    task_search_index_rebuild_readiness_plan_to_dicts,
    task_search_index_rebuild_readiness_plan_to_markdown,
    task_search_index_rebuild_readiness_to_dicts,
)


def test_ready_search_rebuild_detects_all_required_evidence():
    result = build_task_search_index_rebuild_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Rebuild product search index with alias cutover",
                    description="Full reindex builds a shadow index before an index alias swap.",
                    acceptance_criteria=[
                        "Batching uses checkpoints, throttling, and incremental watermark resume.",
                        "Alias swap promotes the rebuilt read alias atomically.",
                        "Zero-downtime rollout keeps search available with dual writes during cutover.",
                        "Stale result handling checks source version, deleted documents, and index lag.",
                        "Validation checks compare document count, mapping validation, and sample query parity.",
                        "Monitoring dashboard tracks reindex progress, failed documents, throughput, and alerts.",
                        "Rollback restores the previous index alias and can swap back to the old index.",
                    ],
                    validation_commands={"test": ["poetry run pytest tests/search/test_reindex_validation.py"]},
                )
            ]
        )
    )

    assert isinstance(result, TaskSearchIndexRebuildReadinessPlan)
    assert result.search_task_ids == ("task-ready",)
    record = result.records[0]
    assert isinstance(record, TaskSearchIndexRebuildReadinessRecord)
    assert record.readiness_level == "ready"
    assert {"search_index_rebuild", "full_reindex", "index_alias_swap"} <= set(record.signals)
    assert record.safeguards == (
        "batching_or_incremental_rebuild",
        "alias_or_swap_strategy",
        "zero_downtime_rollout",
        "stale_result_handling",
        "validation_checks",
        "observability",
        "rollback_plan",
    )
    assert record.missing_safeguards == ()
    assert record.readiness_gaps == ()
    assert any("description:" in item and "Full reindex" in item for item in record.evidence)
    assert any("Zero-downtime rollout" in item for item in record.evidence)
    assert any("Rollback restores" in item for item in record.evidence)
    assert result.summary["readiness_counts"] == {"blocked": 0, "partial": 0, "ready": 1}


def test_partial_and_blocked_tasks_expose_deterministic_gaps_and_sorting():
    result = analyze_task_search_index_rebuild_readiness(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Backfill search index incrementally",
                    description="Search backfill uses incremental indexing from a change feed.",
                    acceptance_criteria=[
                        "Batch size and checkpoint resume control the backfill.",
                        "Read alias swap promotes the shadow index.",
                        "Zero downtime keeps search available while serving traffic.",
                        "Validation checks compare document count and sample queries.",
                        "Monitoring emits indexing lag and error rate alerts.",
                        "Rollback swaps back to the previous index.",
                    ],
                ),
                _task(
                    "task-blocked",
                    title="Reindex all invoices",
                    description="Reindex all invoice search documents from scratch.",
                    acceptance_criteria=[
                        "Batching uses cursors.",
                        "Monitoring tracks reindex progress.",
                    ],
                ),
            ]
        )
    )

    assert result.search_task_ids == ("task-blocked", "task-partial")
    by_id = {record.task_id: record for record in result.records}
    assert by_id["task-blocked"].readiness_level == "blocked"
    assert by_id["task-blocked"].missing_safeguards == (
        "alias_or_swap_strategy",
        "zero_downtime_rollout",
        "stale_result_handling",
        "validation_checks",
        "rollback_plan",
    )
    assert any("rollback" in gap.casefold() for gap in by_id["task-blocked"].readiness_gaps)
    assert any("validation checks" in gap.casefold() for gap in by_id["task-blocked"].readiness_gaps)
    assert any("search stays available" in gap.casefold() for gap in by_id["task-blocked"].readiness_gaps)
    assert by_id["task-partial"].readiness_level == "partial"
    assert by_id["task-partial"].missing_safeguards == ("stale_result_handling",)
    assert result.summary["missing_safeguard_counts"]["rollback_plan"] == 1
    assert result.summary["readiness_counts"] == {"blocked": 1, "partial": 1, "ready": 0}


def test_metadata_paths_and_validation_commands_detect_signals_and_controls():
    result = build_task_search_index_rebuild_readiness_plan(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Add OpenSearch index migration worker",
                    description="Move implementation files.",
                    files_or_modules=[
                        "src/search/rebuild_index_alias_swap.py",
                        "src/search/incremental_index_backfill.py",
                    ],
                    metadata={
                        "search_index_rebuild": {
                            "batching": "Backfill window uses cursor batches and checkpoints.",
                            "alias_strategy": "Blue-green index alias swap promotes the shadow index.",
                            "zero_downtime": "Online rebuild keeps search available.",
                            "stale_results": "Freshness check rejects stale results and index lag.",
                            "validation": "Query parity and document count verification are required.",
                            "observability": "Dashboard tracks queue depth, throughput, and failed documents.",
                            "rollback": "Feature flag restores the old index alias.",
                        }
                    },
                    validation_commands={"test": ["pytest tests/search/test_query_parity.py --validate"]},
                )
            ]
        )
    )

    record = result.records[0]
    assert {"search_index_rebuild", "incremental_indexing", "index_alias_swap", "search_backfill"} <= set(
        record.signals
    )
    assert record.readiness_level == "ready"
    assert "files_or_modules: src/search/rebuild_index_alias_swap.py" in record.evidence
    assert any("metadata.search_index_rebuild.alias_strategy" in item for item in record.evidence)
    assert any("validation_commands[0]:" in item for item in record.evidence)


def test_unrelated_negated_invalid_repeated_string_mapping_and_object_inputs_are_stable():
    object_task = SimpleNamespace(
        id="task-object",
        title="Rebuild search index alias",
        description="Search index rebuild uses alias swap and zero downtime.",
        acceptance_criteria=[
            "Validation checks compare hit count.",
            "Rollback swaps back to previous index.",
        ],
    )
    no_signal = build_task_search_index_rebuild_readiness_plan(
        _plan(
            [
                _task("task-copy", title="Polish dashboard", description="Adjust table spacing."),
                _task(
                    "task-negated",
                    title="Update search copy",
                    description="No search index rebuild is required or planned for this copy change.",
                ),
            ]
        )
    )
    repeated = build_task_search_index_rebuild_readiness_plan(
        [
            _task(
                "task-repeat",
                title="Reindex all search documents",
                description="Reindex all search documents. Reindex all search documents.",
            )
        ]
    )

    assert build_task_search_index_rebuild_readiness_plan("reindex search index").records == ()
    assert build_task_search_index_rebuild_readiness_plan({"tasks": "not a list"}).records == ()
    assert build_task_search_index_rebuild_readiness_plan(42).records == ()
    assert build_task_search_index_rebuild_readiness_plan(object_task).records[0].task_id == "task-object"
    assert no_signal.records == ()
    assert no_signal.no_impact_task_ids == ("task-copy", "task-negated")
    assert repeated.records[0].evidence == (
        "title: Reindex all search documents",
        "description: Reindex all search documents. Reindex all search documents.",
    )
    assert "No task search index rebuild readiness records were inferred." in no_signal.to_markdown()
    assert "No-impact tasks: task-copy, task-negated" in no_signal.to_markdown()


def test_serialization_markdown_aliases_models_and_no_mutation_are_stable():
    source = _plan(
        [
            _task(
                "task-model",
                title="Rebuild customer search index",
                description="Search index rebuild uses a full reindex.",
                acceptance_criteria=[
                    "Batching uses chunks.",
                    "Alias swap promotes a shadow index.",
                    "Zero-downtime rollout keeps search available.",
                    "Stale results use max age and source version checks.",
                    "Validation checks compare document count.",
                    "Metrics and alerts track reindex progress.",
                    "Rollback restores the previous index.",
                ],
            )
        ],
        plan_id="plan-serialization",
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)
    task_model = ExecutionTask.model_validate(source["tasks"][0])

    result = summarize_task_search_index_rebuild_readiness(source)
    payload = task_search_index_rebuild_readiness_plan_to_dict(result)
    markdown = task_search_index_rebuild_readiness_plan_to_markdown(result)

    assert source == original
    assert build_task_search_index_rebuild_readiness_plan(task_model).records[0].task_id == "task-model"
    assert generate_task_search_index_rebuild_readiness(model).plan_id == "plan-serialization"
    assert derive_task_search_index_rebuild_readiness(source).to_dict() == result.to_dict()
    assert extract_task_search_index_rebuild_readiness(source).to_dict() == result.to_dict()
    assert recommend_task_search_index_rebuild_readiness(source).to_dict() == result.to_dict()
    assert build_task_search_index_rebuild_readiness_plan(result) is result
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_search_index_rebuild_readiness_plan_to_dicts(result) == payload["records"]
    assert task_search_index_rebuild_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_search_index_rebuild_readiness_to_dicts(result) == payload["records"]
    assert task_search_index_rebuild_readiness_to_dicts(result.records) == payload["records"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Search Index Rebuild Readiness: plan-serialization")
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "search_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "signals",
        "safeguards",
        "missing_safeguards",
        "readiness_level",
        "evidence",
        "readiness_gaps",
    ]


def _plan(tasks, *, plan_id="plan-search-index-rebuild"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-search-index-rebuild",
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
    validation_commands=None,
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
    if validation_commands is not None:
        task["validation_commands"] = validation_commands
    return task
