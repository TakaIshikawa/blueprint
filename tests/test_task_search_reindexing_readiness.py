import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_search_reindexing_readiness import (
    TaskSearchReindexingReadinessFinding,
    TaskSearchReindexingReadinessPlan,
    analyze_task_search_reindexing_readiness,
    build_task_search_reindexing_readiness_plan,
    summarize_task_search_reindexing_readiness,
    summarize_task_search_reindexing_readiness_plan,
    task_search_reindexing_readiness_plan_to_dict,
)


def test_high_risk_production_reindex_recommends_all_readiness_checks():
    result = build_task_search_reindexing_readiness_plan(
        _plan(
            [
                _task(
                    "task-prod-reindex",
                    title="Reindex all customer search data",
                    description=(
                        "Production reindex of all customer documents after index mapping change. "
                        "Use schema compatibility checks, backfill plan, freshness validation, "
                        "dual-write strategy, ranking regression tests, and failure retry handling."
                    ),
                    files_or_modules=["scripts/reindex_customer_search.py"],
                    acceptance_criteria=[
                        "Index schema changes are backward compatible.",
                        "Reindex plan with zero-downtime strategy is documented.",
                        "Freshness and lag monitoring is in place.",
                        "Dual-write and rollback strategy is tested.",
                        "Ranking and filter regression tests pass.",
                        "Failure retry logic handles partial errors.",
                    ],
                ),
                _task("task-ui", title="Polish dashboard copy", description="Update settings labels."),
            ]
        )
    )

    assert isinstance(result, TaskSearchReindexingReadinessPlan)
    assert result.impacted_task_ids == ("task-prod-reindex",)
    assert result.ignored_task_ids == ("task-ui",)
    finding = result.findings[0]
    assert finding.risk_level == "high"
    assert "reindex_job" in finding.work_types
    assert "index_mapping_change" in finding.work_types
    assert finding.readiness_checks == (
        "index_schema_compatibility",
        "backfill_reindex_plan",
        "freshness_lag_validation",
        "rollback_dual_write_strategy",
        "ranking_filter_regression_checks",
        "failure_retry_handling",
    )
    assert finding.missing_acceptance_criteria == ()
    assert any("production" in item.lower() for item in finding.evidence)
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}


def test_low_risk_local_search_with_acceptance_criteria_is_low_risk():
    result = build_task_search_reindexing_readiness_plan(
        [
            _task(
                "task-local",
                title="Update local search index for testing",
                description="Reindex local test data for development search feature testing.",
                files_or_modules=["tools/reindex_local_test_data.py"],
                acceptance_criteria=[
                    "Index schema is backward compatible.",
                    "Reindex plan is documented.",
                    "Freshness checks validate indexing lag.",
                    "Ranking regression tests cover key queries.",
                ],
            )
        ]
    )

    finding = result.records[0]
    assert finding.risk_level == "low"
    assert "reindex_job" in finding.work_types
    assert finding.missing_acceptance_criteria == ("rollback_dual_write_strategy", "failure_retry_handling")


def test_missing_acceptance_criteria_are_reported_even_when_description_mentions_checks():
    result = build_task_search_reindexing_readiness_plan(
        _plan(
            [
                _task(
                    "task-reindex",
                    title="Rebuild search index after schema change",
                    description=(
                        "Reindex all documents with schema compatibility, backfill plan, "
                        "freshness validation, dual-write, ranking checks, and retry logic."
                    ),
                    acceptance_criteria=["All documents are reindexed successfully."],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.risk_level == "high"
    assert {"reindex_job", "index_mapping_change"} <= set(finding.work_types)
    assert finding.missing_acceptance_criteria == (
        "index_schema_compatibility",
        "backfill_reindex_plan",
        "freshness_lag_validation",
        "rollback_dual_write_strategy",
        "ranking_filter_regression_checks",
        "failure_retry_handling",
    )


def test_metadata_tags_and_execution_plan_inputs_detect_search_work_without_mutation():
    tag_result = build_task_search_reindexing_readiness_plan(
        [_task("task-tag", title="Search maintenance", description="Update search config.", tags=["search-reindex"])]
    )
    assert len(tag_result.findings) == 1
    assert "reindex_job" in tag_result.findings[0].work_types

    plan = ExecutionPlan.model_validate(
        _plan([_task("task-plan", title="Elasticsearch upgrade", description="Reindex after Elasticsearch upgrade.")])
    )
    plan_result = build_task_search_reindexing_readiness_plan(plan)
    assert len(plan_result.findings) == 1

    source_result = build_task_search_reindexing_readiness_plan(
        _plan([_task("task-source", title="Index backfill", description="Backfill search index for new field.")])
    )
    assert source_result.plan_id == "plan-search"
    assert source_result.findings[0].work_types == ("backfill",)

    # Source should not be mutated
    source_plan = _plan([_task("task-immutable", description="Search reindex.")])
    original = copy.deepcopy(source_plan)
    build_task_search_reindexing_readiness_plan(source_plan)
    assert source_plan == original


def test_detection_from_text_files_acceptance_criteria_and_metadata():
    # Detection from title and description
    text_result = build_task_search_reindexing_readiness_plan(
        [_task("task-text", title="Add autocomplete to search", description="Implement search autocomplete feature.")]
    )
    assert "autocomplete" in text_result.findings[0].work_types

    # Detection from files_or_modules
    file_result = build_task_search_reindexing_readiness_plan(
        [
            _task(
                "task-file",
                title="Search optimization",
                description="Optimize search performance.",
                files_or_modules=["src/search/ranking_algorithm.py"],
            )
        ]
    )
    assert "ranking_change" in file_result.findings[0].work_types

    # Detection from acceptance criteria
    ac_result = build_task_search_reindexing_readiness_plan(
        [
            _task(
                "task-ac",
                title="Search filters",
                description="Add filters to search.",
                acceptance_criteria=["Faceted search is implemented.", "Filter options are tested."],
            )
        ]
    )
    assert "facet_filter" in ac_result.findings[0].work_types

    # Detection from metadata
    metadata_result = build_task_search_reindexing_readiness_plan(
        [
            _task(
                "task-metadata",
                title="Search work",
                description="Search task.",
                metadata={"search_type": "eventual consistency monitoring"},
            )
        ]
    )
    assert "eventual_consistency" in metadata_result.findings[0].work_types


def test_high_risk_for_reindex_backfill_or_schema_changes_without_safeguards():
    # Reindex without safeguards
    reindex_result = build_task_search_reindexing_readiness_plan(
        [_task("task-reindex", title="Production reindex", description="Reindex all production customer data.")]
    )
    assert reindex_result.findings[0].risk_level == "high"
    assert len(reindex_result.findings[0].missing_acceptance_criteria) == 6

    # Backfill without safeguards
    backfill_result = build_task_search_reindexing_readiness_plan(
        [
            _task(
                "task-backfill",
                title="Backfill search index",
                description="Backfill search index for all production tenants.",
            )
        ]
    )
    assert backfill_result.findings[0].risk_level == "high"

    # Index mapping change without safeguards
    mapping_result = build_task_search_reindexing_readiness_plan(
        [
            _task(
                "task-mapping",
                title="Add field to index mapping",
                description="Add new field to production Elasticsearch mapping.",
            )
        ]
    )
    assert mapping_result.findings[0].risk_level == "high"

    # Ranking change without safeguards
    ranking_result = build_task_search_reindexing_readiness_plan(
        [
            _task(
                "task-ranking",
                title="Update search ranking",
                description="Update production search ranking algorithm for all customers.",
            )
        ]
    )
    assert ranking_result.findings[0].risk_level == "high"


def test_low_risk_with_all_safeguards_present():
    result = build_task_search_reindexing_readiness_plan(
        [
            _task(
                "task-safe",
                title="Reindex with safeguards",
                description="Safe reindex with all safeguards in place.",
                acceptance_criteria=[
                    "Schema compatibility is verified.",
                    "Backfill plan with zero downtime is ready.",
                    "Freshness lag is monitored.",
                    "Rollback via dual-write strategy is tested.",
                    "Ranking regression checks pass.",
                    "Failure retry logic handles errors.",
                ],
            )
        ]
    )

    assert result.findings[0].risk_level == "low"
    assert result.findings[0].missing_acceptance_criteria == ()


def test_ignored_tasks_without_search_context():
    result = build_task_search_reindexing_readiness_plan(
        _plan(
            [
                _task("task-frontend", title="Add button", description="Add button to UI."),
                _task("task-api", title="Create endpoint", description="Create REST API endpoint."),
                _task("task-db", title="Add column", description="Add database column."),
            ]
        )
    )

    assert result.findings == ()
    assert len(result.ignored_task_ids) == 3
    assert result.summary["impacted_task_count"] == 0


def test_serialization_to_dict_and_dicts():
    result = build_task_search_reindexing_readiness_plan(
        [
            _task(
                "task-serialize",
                title="Search reindex",
                description="Reindex search with missing safeguards.",
                acceptance_criteria=["Schema compatibility checked."],
            )
        ]
    )

    # Test to_dict
    result_dict = task_search_reindexing_readiness_plan_to_dict(result)
    assert result_dict == result.to_dict()
    assert list(result_dict.keys()) == [
        "plan_id",
        "findings",
        "records",
        "impacted_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert json.loads(json.dumps(result_dict)) == result_dict

    # Test to_dicts
    dicts = result.to_dicts()
    assert dicts == result_dict["findings"]
    assert len(dicts) == 1
    assert list(dicts[0].keys()) == [
        "task_id",
        "title",
        "work_types",
        "readiness_checks",
        "missing_acceptance_criteria",
        "risk_level",
        "evidence",
    ]


def test_markdown_output_is_deterministic():
    result = build_task_search_reindexing_readiness_plan(
        _plan(
            [
                _task(
                    "task-md1",
                    title="Reindex alpha",
                    description="Production reindex.",
                    acceptance_criteria=["Schema compatibility verified."],
                ),
                _task(
                    "task-md2",
                    title="Reindex beta",
                    description="Local reindex for dev.",
                    acceptance_criteria=[
                        "Schema compatibility verified.",
                        "Backfill plan ready.",
                        "Freshness monitored.",
                        "Rollback tested.",
                    ],
                ),
            ]
        )
    )

    markdown1 = result.to_markdown()
    markdown2 = result.to_markdown()
    assert markdown1 == markdown2
    assert "# Task Search Reindexing Readiness Plan: plan-search" in markdown1
    assert "## Summary" in markdown1
    assert "## Findings" in markdown1
    assert "task-md1" in markdown1
    assert "task-md2" in markdown1
    assert "**Missing Acceptance Criteria:**" in markdown1


def test_aliases_work():
    source = [_task("task-alias", title="Search work", description="Reindex search index.")]

    result1 = build_task_search_reindexing_readiness_plan(source)
    result2 = analyze_task_search_reindexing_readiness(source)
    result3 = summarize_task_search_reindexing_readiness(source)
    result4 = summarize_task_search_reindexing_readiness_plan(source)

    assert result2 == result1.findings
    assert result3 == result1
    assert result4 == result1


def test_list_and_single_task_inputs():
    single_task = _task("task-single", title="Single task", description="Reindex search.")
    list_tasks = [
        _task("task-list1", title="List task 1", description="Reindex search."),
        _task("task-list2", title="List task 2", description="Backfill index."),
    ]

    single_result = build_task_search_reindexing_readiness_plan(single_task)
    assert len(single_result.findings) == 1
    assert single_result.findings[0].task_id == "task-single"

    list_result = build_task_search_reindexing_readiness_plan(list_tasks)
    assert len(list_result.findings) == 2
    assert list_result.findings[0].task_id in ("task-list1", "task-list2")


def test_dict_and_model_inputs():
    dict_plan = _plan([_task("task-dict", title="Dict task", description="Reindex search.")])
    model_plan = ExecutionPlan.model_validate(dict_plan)

    dict_result = build_task_search_reindexing_readiness_plan(dict_plan)
    model_result = build_task_search_reindexing_readiness_plan(model_plan)

    assert len(dict_result.findings) == 1
    assert len(model_result.findings) == 1
    assert dict_result.findings[0].task_id == model_result.findings[0].task_id


def test_summary_includes_all_expected_fields():
    result = build_task_search_reindexing_readiness_plan(
        [
            _task("task-summary1", title="High risk reindex", description="Production reindex for all customers."),
            _task(
                "task-summary2",
                title="Low risk reindex",
                description="Local dev reindex.",
                acceptance_criteria=[
                    "Schema compatible.",
                    "Plan ready.",
                    "Freshness monitored.",
                    "Rollback tested.",
                    "Ranking validated.",
                ],
            ),
            _task("task-summary3", title="Non-search task", description="Add button to UI."),
        ]
    )

    summary = result.summary
    assert summary["task_count"] == 3
    assert summary["impacted_task_count"] == 2
    assert summary["ignored_task_count"] == 1
    assert summary["risk_counts"]["high"] == 1
    assert summary["risk_counts"]["medium"] == 0
    assert summary["risk_counts"]["low"] == 1
    assert "work_type_counts" in summary
    assert "missing_acceptance_criteria_counts" in summary


def test_records_property_alias():
    result = build_task_search_reindexing_readiness_plan(
        [_task("task-records", title="Search work", description="Reindex.")]
    )

    assert result.records == result.findings
    assert len(result.records) == 1


def _plan(tasks):
    return {
        "id": "plan-search",
        "implementation_brief_id": "brief-search",
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
    task_dict = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags:
        task_dict["tags"] = tags
    return task_dict
