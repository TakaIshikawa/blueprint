import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_search_indexing_impact import (
    TaskSearchIndexingImpactPlan,
    TaskSearchIndexingImpactRecord,
    build_task_search_indexing_impact_plan,
    derive_task_search_indexing_impact_plan,
    generate_task_search_indexing_impact,
    summarize_task_search_indexing_impact,
    task_search_indexing_impact_plan_to_dict,
    task_search_indexing_impact_plan_to_markdown,
)


def test_search_engines_indexes_reindexing_and_ranking_signals_are_flagged():
    result = build_task_search_indexing_impact_plan(
        _plan(
            [
                _task(
                    "task-elastic",
                    title="Rebuild Elasticsearch product search index",
                    description=(
                        "Change product mappings and analyzer settings, then run a full reindex "
                        "before alias cutover."
                    ),
                    files_or_modules=["src/search/elasticsearch/product_index.py"],
                ),
                _task(
                    "task-algolia",
                    title="Tune Algolia relevance synonyms",
                    description="Adjust search relevance ranking, boosts, and synonym coverage for product queries.",
                    files_or_modules=["src/search/algolia/relevance.py"],
                ),
                _task(
                    "task-vector",
                    title="Add vector search pagination",
                    description="Create an embedding index for semantic search and cursor pagination.",
                    files_or_modules=["src/search/vector_indexes/documents.py"],
                ),
            ]
        )
    )

    assert isinstance(result, TaskSearchIndexingImpactPlan)
    assert result.plan_id == "plan-search-index"
    assert result.impacted_task_ids == ("task-elastic", "task-vector", "task-algolia")
    by_id = {record.task_id: record for record in result.records}

    assert isinstance(by_id["task-elastic"], TaskSearchIndexingImpactRecord)
    assert by_id["task-elastic"].impacted_index_surfaces == (
        "elasticsearch",
        "search_index",
        "analyzer",
    )
    assert by_id["task-elastic"].reindex_requirement == "full_reindex"
    assert by_id["task-algolia"].impacted_index_surfaces == (
        "algolia",
        "search_index",
        "synonym",
        "relevance_ranking",
    )
    assert by_id["task-algolia"].reindex_requirement == "relevance_only"
    assert by_id["task-vector"].impacted_index_surfaces == (
        "vector_index",
        "search_index",
        "pagination",
    )
    assert by_id["task-vector"].reindex_requirement == "full_reindex"


def test_incremental_updates_and_validation_guidance_cover_stale_missing_and_ranked_results():
    record = generate_task_search_indexing_impact(
        _task(
            "task-incremental",
            title="Update OpenSearch account indexing pipeline",
            description=(
                "Incremental index update for account document upserts and deletes with "
                "search result pagination."
            ),
            acceptance_criteria=[
                "Validate stale results, missing results, and incorrectly ranked results.",
                "Existing validation command checks search freshness.",
            ],
            metadata={"validation_commands": {"test": ["poetry run pytest tests/search/test_accounts.py"]}},
        )
    )[0]

    assert record.impacted_index_surfaces == (
        "opensearch",
        "search_index",
        "relevance_ranking",
        "pagination",
    )
    assert record.reindex_requirement == "incremental_index_update"
    assert any("stale" in value for value in record.validation_checks)
    assert any("missing-result" in value for value in record.validation_checks)
    assert any("incorrectly ranked" in value for value in record.validation_checks)
    assert any("stale search results" in value for value in record.customer_visible_risk_notes)
    assert any("eligible records" in value for value in record.customer_visible_risk_notes)
    assert any("incorrectly ranked results" in value for value in record.customer_visible_risk_notes)
    assert any("index refresh lag" in value for value in record.rollout_safeguards)
    assert "validation_commands: poetry run pytest tests/search/test_accounts.py" in record.evidence


def test_solr_full_reindex_and_pagination_validation_are_detected_from_metadata_and_paths():
    record = generate_task_search_indexing_impact(
        _task(
            "task-solr",
            title="Update search results",
            description="Result pagination must remain stable.",
            files_or_modules={"main": "services/solr/schema/product_search.xml"},
            metadata={
                "search_notes": {
                    "change": "Solr field mapping change requires complete reindex of products."
                }
            },
        )
    )[0]

    assert record.impacted_index_surfaces == ("solr", "search_index", "pagination")
    assert record.reindex_requirement == "full_reindex"
    assert any("duplicates, skipped results" in value for value in record.validation_checks)
    assert any("duplicate, skipped" in value for value in record.customer_visible_risk_notes)
    assert record.evidence[0] == "files_or_modules: services/solr/schema/product_search.xml"


def test_model_object_inputs_no_mutation_serialization_markdown_and_derive_are_stable():
    task = _task(
        "task-model",
        title="Tune search ranking | support docs",
        description="Tune relevance scoring for support search without document reindexing.",
        acceptance_criteria=["Run search quality judgment-list comparison."],
    )
    original = copy.deepcopy(task)
    plan_model = ExecutionPlan.model_validate(_plan([task], plan_id="plan-model"))
    task_model = ExecutionTask.model_validate(task)
    object_task = SimpleNamespace(
        id="task-object",
        title="Add OpenSearch synonym support",
        description="Synonym set changes improve search recall.",
        files_or_modules=["src/search/opensearch/synonyms.py"],
        acceptance_criteria=["Ranking comparison passes."],
        status="pending",
    )

    result = summarize_task_search_indexing_impact(plan_model)
    direct = build_task_search_indexing_impact_plan(task_model)
    object_records = generate_task_search_indexing_impact([object_task])
    derived = derive_task_search_indexing_impact_plan(result)
    payload = task_search_indexing_impact_plan_to_dict(result)
    markdown = task_search_indexing_impact_plan_to_markdown(result)

    assert task == original
    assert derived is result
    assert direct.records[0].task_id == "task-model"
    assert object_records[0].task_id == "task-object"
    assert result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "impacted_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "impacted_index_surfaces",
        "reindex_requirement",
        "rollout_safeguards",
        "validation_checks",
        "customer_visible_risk_notes",
        "evidence",
    ]
    assert markdown.startswith("# Task Search Indexing Impact Plan: plan-model")
    assert "Tune search ranking \\| support docs" not in markdown


def test_non_search_tasks_empty_and_invalid_inputs_produce_no_records():
    result = build_task_search_indexing_impact_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings page copy",
                    description="Adjust labels and helper text.",
                    files_or_modules=["src/blueprint/ui/settings.py"],
                )
            ],
            plan_id="plan-empty",
        )
    )
    empty = build_task_search_indexing_impact_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_search_indexing_impact_plan(37)

    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "impacted_task_count": 0,
        "reindex_requirement_counts": {
            "full_reindex": 0,
            "incremental_index_update": 0,
            "relevance_only": 0,
        },
        "surface_counts": {
            "elasticsearch": 0,
            "opensearch": 0,
            "solr": 0,
            "algolia": 0,
            "vector_index": 0,
            "search_index": 0,
            "analyzer": 0,
            "synonym": 0,
            "relevance_ranking": 0,
            "pagination": 0,
        },
    }
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert invalid.plan_id is None
    assert invalid.records == ()
    assert result.to_markdown() == (
        "# Task Search Indexing Impact Plan: plan-empty\n\nNo search-index impacts were detected."
    )


def _plan(tasks, plan_id="plan-search-index"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-search-index",
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
