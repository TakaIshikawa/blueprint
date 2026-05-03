import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_graphql_query_readiness import (
    TaskGraphQLQueryReadinessPlan,
    TaskGraphQLQueryReadinessRecord,
    analyze_task_graphql_query_readiness,
    build_task_graphql_query_readiness_plan,
    derive_task_graphql_query_readiness,
    extract_task_graphql_query_readiness,
    generate_task_graphql_query_readiness,
    recommend_task_graphql_query_readiness,
    summarize_task_graphql_query_readiness,
    task_graphql_query_readiness_plan_to_dict,
    task_graphql_query_readiness_plan_to_dicts,
    task_graphql_query_readiness_plan_to_markdown,
    task_graphql_query_readiness_to_dicts,
)


def test_weak_graphql_query_task_sorts_first_and_separates_no_impact_ids():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Add GraphQL query complexity limits",
                    description="Configure complexity scoring and depth limits for GraphQL queries.",
                    acceptance_criteria=[
                        "Complexity scoring algorithm implemented.",
                        "Depth limit enforcement configured.",
                        "Query performance tests cover complexity scenarios.",
                    ],
                ),
                _task(
                    "task-copy",
                    title="Polish settings copy",
                    description="Update labels in the admin settings page.",
                ),
                _task(
                    "task-weak",
                    title="Enable GraphQL schema",
                    description="Add GraphQL schema definition with query complexity limits and N+1 risk.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskGraphQLQueryReadinessPlan)
    assert result.graphql_task_ids == ("task-weak", "task-partial")
    assert result.impacted_task_ids == result.graphql_task_ids
    assert result.no_impact_task_ids == ("task-copy",)
    assert [record.task_id for record in result.records] == ["task-weak", "task-partial"]
    weak = result.records[0]
    assert isinstance(weak, TaskGraphQLQueryReadinessRecord)
    assert weak.readiness == "weak"
    assert weak.impact == "high"
    assert {"schema_definition", "query_complexity_limit", "n_plus_1_risk"} <= set(weak.detected_signals)
    assert "complexity_scoring" in weak.missing_safeguards
    assert "dataloader_implementation" in weak.missing_safeguards


def test_strong_readiness_reflects_all_safeguards_and_summary_counts():
    result = analyze_task_graphql_query_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Add GraphQL query protection",
                    description=(
                        "GraphQL schema with query complexity limits and N+1 risk mitigation. "
                        "Implement complexity scoring, depth limit enforcement, and DataLoader pattern. "
                        "Configure query timeouts and resolver error handling."
                    ),
                    acceptance_criteria=[
                        "Complexity scoring algorithm calculates field costs.",
                        "Depth limit enforcement prevents deeply nested queries.",
                        "Query timeout config limits execution time.",
                        "DataLoader implementation eliminates N+1 queries.",
                        "Resolver error handling formats errors properly.",
                        "Schema validation tests verify type safety.",
                        "Query performance tests cover complexity and load scenarios benchmark.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "strong"
    assert record.impact == "medium"
    assert record.present_safeguards == (
        "complexity_scoring",
        "depth_limit_enforcement",
        "query_timeout_config",
        "dataloader_implementation",
        "resolver_error_handling",
        "schema_validation",
        "query_performance_tests",
    )
    assert record.missing_safeguards == ()
    assert record.recommended_checks == ()
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "strong": 1}
    assert result.summary["impact_counts"] == {"high": 0, "medium": 1, "low": 0}
    assert result.summary["signal_counts"]["schema_definition"] == 1
    assert result.summary["present_safeguard_counts"]["query_performance_tests"] == 1


def test_mapping_input_collects_title_description_paths_and_validation_command_evidence():
    result = build_task_graphql_query_readiness_plan(
        {
            "id": "task-mapping",
            "title": "Add GraphQL complexity scoring",
            "description": "Configure complexity scoring for GraphQL queries with depth limits and DataLoader implementation.",
            "files_or_modules": [
                "src/graphql/schema.py",
                "src/graphql/complexity_scoring.py",
                "tests/test_query_performance.py",
            ],
            "validation_commands": {
                "tests": [
                    "poetry run pytest tests/test_graphql_complexity.py",
                    "poetry run pytest tests/test_query_performance_benchmark.py",
                ]
            },
        }
    )

    record = result.records[0]
    assert {"schema_definition", "query_complexity_limit", "dataloader_pattern"} <= set(
        record.detected_signals
    )
    assert {"complexity_scoring", "dataloader_implementation"} <= set(record.present_safeguards)
    assert any(item == "title: Add GraphQL complexity scoring" for item in record.evidence)
    assert any("description: Configure complexity scoring" in item for item in record.evidence)
    assert any(item == "files_or_modules: src/graphql/complexity_scoring.py" for item in record.evidence)
    assert any("validation_commands: poetry run pytest tests/test_query_performance_benchmark.py" in item for item in record.evidence)


def test_execution_plan_execution_task_and_object_inputs_are_supported_without_mutation():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add GraphQL schema",
        description="Enable GraphQL schema definition with query complexity limits.",
        acceptance_criteria=["Query performance tests cover complexity scenarios."],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Configure depth limits",
            description="Implement depth limit enforcement for nested queries.",
            acceptance_criteria=["Depth limit enforcement prevents deep nesting."],
        )
    )
    source = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-covered",
                title="Add GraphQL middleware",
                description="GraphQL schema with query complexity limits and N+1 risk.",
                acceptance_criteria=[
                    "Complexity scoring implemented.",
                    "Depth limit enforcement configured.",
                    "Query timeout config set.",
                    "DataLoader implementation complete.",
                    "Resolver error handling added.",
                    "Schema validation tests passing.",
                    "Query performance tests passing.",
                ],
            ),
        ],
        plan_id="plan-graphql-objects",
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    result = summarize_task_graphql_query_readiness(source)

    assert source == original
    assert build_task_graphql_query_readiness_plan(object_task).records[0].task_id == "task-object"
    assert generate_task_graphql_query_readiness(model).plan_id == "plan-graphql-objects"
    assert derive_task_graphql_query_readiness(source).to_dict() == result.to_dict()
    assert recommend_task_graphql_query_readiness(source).to_dict() == result.to_dict()
    assert extract_task_graphql_query_readiness(source).to_dict() == result.to_dict()
    assert result.records == result.findings
    assert result.records == result.recommendations


def test_empty_state_markdown_and_summary_are_stable():
    result = build_task_graphql_query_readiness_plan(
        _plan([_task("task-ui", title="Polish dashboard", description="Adjust table spacing.")])
    )

    assert result.records == ()
    assert result.graphql_task_ids == ()
    assert result.no_impact_task_ids == ("task-ui",)
    assert result.summary["graphql_task_count"] == 0
    assert result.to_markdown() == "\n".join(
        [
            "# Task GraphQL Query Readiness: plan-graphql-query",
            "",
            "## Summary",
            "",
            "- Task count: 1",
            "- GraphQL task count: 0",
            "- Missing safeguard count: 0",
            "- Readiness counts: weak 0, partial 0, strong 0",
            "- Impact counts: high 0, medium 0, low 0",
            "",
            "No task GraphQL query readiness records were inferred.",
            "",
            "No-impact tasks: task-ui",
        ]
    )


def test_serialization_aliases_to_dict_output_and_markdown_are_json_safe():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-graphql",
                    title="Add GraphQL complexity scoring",
                    description="GraphQL schema with query complexity limits and depth limit enforcement.",
                    acceptance_criteria=[
                        "Complexity scoring implemented.",
                        "Depth limit enforcement configured.",
                        "Query performance tests passing.",
                    ],
                )
            ],
            plan_id="plan-serialization",
        )
    )
    payload = task_graphql_query_readiness_plan_to_dict(result)
    markdown = task_graphql_query_readiness_plan_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_graphql_query_readiness_plan_to_dicts(result) == payload["records"]
    assert task_graphql_query_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_graphql_query_readiness_to_dicts(result) == payload["records"]
    assert task_graphql_query_readiness_to_dicts(result.records) == payload["records"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task GraphQL Query Readiness: plan-serialization")
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "graphql_task_ids",
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
        "readiness",
        "impact",
        "recommended_checks",
        "evidence",
    ]


def test_schema_definition_signal_detection_and_impact_assessment():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-schema",
                    title="Add GraphQL schema definition",
                    description="Define GraphQL types and schema description language (SDL).",
                    acceptance_criteria=[
                        "Schema validation tests verify type safety.",
                        "GraphQL API endpoint configured.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "schema_definition" in record.detected_signals
    assert "schema_validation" in record.present_safeguards
    # Only 1 safeguard present, needs 3+ for partial
    assert record.readiness == "weak"


def test_query_complexity_limit_signal_and_complexity_scoring_safeguard():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-complexity",
                    title="Add query complexity limits",
                    description="Implement complexity scoring algorithm with field costs and max complexity threshold.",
                    acceptance_criteria=[
                        "Complexity scoring calculates query cost.",
                        "Complexity threshold prevents expensive queries.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "query_complexity_limit" in record.detected_signals
    assert "complexity_scoring" in record.present_safeguards


def test_mutation_operation_signal_detection():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-mutation",
                    title="Add GraphQL mutations",
                    description="Implement mutation operations for create, update, and delete actions with resolver error handling.",
                    acceptance_criteria=[
                        "Mutation resolvers handle create, update, delete.",
                        "Resolver error handling formats mutation errors.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "mutation_operation" in record.detected_signals
    assert "resolver_error_handling" in record.present_safeguards


def test_subscription_support_signal_and_impact():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-subscription",
                    title="Add GraphQL subscriptions",
                    description="Implement real-time subscriptions with WebSocket and pub-sub pattern.",
                    acceptance_criteria=[
                        "Subscription type configured for real-time updates.",
                        "WebSocket connection handles event streams.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "subscription_support" in record.detected_signals
    # subscription_support is a high-impact signal
    assert record.impact in ("high", "medium")


def test_n_plus_1_risk_signal_and_dataloader_safeguard():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-n-plus-1",
                    title="Fix N+1 query problem",
                    description="Eliminate N+1 queries in nested resolvers using DataLoader implementation with batch loading.",
                    acceptance_criteria=[
                        "DataLoader implementation batches resolver queries.",
                        "Batched loading eliminates N+1 query patterns.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "n_plus_1_risk" in record.detected_signals
    assert "dataloader_implementation" in record.present_safeguards


def test_dataloader_pattern_signal_and_safeguard_detection():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-dataloader",
                    title="Implement DataLoader pattern",
                    description="Add DataLoader with batching layer and caching loader for request batching.",
                    acceptance_criteria=[
                        "DataLoader implementation batches and caches requests.",
                        "Batch resolver reduces database queries.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "dataloader_pattern" in record.detected_signals
    assert "dataloader_implementation" in record.present_safeguards


def test_field_resolver_signal_detection():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-resolver",
                    title="Add custom field resolvers",
                    description="Implement field resolver logic with resolver error handling and resolver timeout.",
                    acceptance_criteria=[
                        "Custom resolver function implemented.",
                        "Resolver error handling catches exceptions.",
                        "Query timeout config prevents long-running resolvers.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "field_resolver" in record.detected_signals
    assert "resolver_error_handling" in record.present_safeguards
    assert "query_timeout_config" in record.present_safeguards


def test_depth_limit_enforcement_safeguard():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-depth",
                    title="Add query depth limits",
                    description="Enforce depth limit enforcement to prevent deeply nested queries with max depth threshold.",
                    acceptance_criteria=[
                        "Depth limit enforcement configured.",
                        "Maximum depth restriction blocks deep queries.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "depth_limit_enforcement" in record.present_safeguards


def test_query_timeout_config_safeguard():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-timeout",
                    title="Configure query timeouts for GraphQL",
                    description="Set query timeout configuration with execution timeout limits and timeout policy for GraphQL schema.",
                    acceptance_criteria=[
                        "Query timeout config limits execution time.",
                        "Resolver timeout prevents long-running queries.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "query_timeout_config" in record.present_safeguards


def test_out_of_scope_tasks_produce_no_records():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task("task-db", title="Optimize database queries", description="Add indexes to user table."),
                _task("task-cache", title="Add Redis cache", description="Cache frequently accessed data."),
                _task("task-logging", title="Improve logging", description="Add structured logging."),
            ]
        )
    )

    assert result.records == ()
    assert result.graphql_task_ids == ()
    assert result.no_impact_task_ids == ("task-db", "task-cache", "task-logging")
    assert result.summary["graphql_task_count"] == 0


def test_invalid_inputs_produce_empty_results():
    for invalid in [None, "", [], {}, 42, b"bytes"]:
        result = build_task_graphql_query_readiness_plan(invalid)
        assert result.records == ()
        assert result.graphql_task_ids == ()
        assert result.summary["graphql_task_count"] == 0


def test_path_signal_detection_from_files_or_modules():
    result = build_task_graphql_query_readiness_plan(
        _task(
            "task-files",
            title="Update GraphQL schema",
            files_or_modules=[
                "src/graphql/schema.py",
                "src/resolvers/user_resolver.py",
                "src/dataloader/batch_loader.py",
                "tests/test_query_performance.py",
            ],
        )
    )

    record = result.records[0]
    assert {"schema_definition", "field_resolver", "dataloader_pattern"} <= set(record.detected_signals)
    assert any("files_or_modules: src/graphql/schema.py" in item for item in record.evidence)


def test_evidence_extraction_from_task_fields_and_validation_commands():
    result = build_task_graphql_query_readiness_plan(
        _task(
            "task-evidence",
            title="Add GraphQL complexity scoring",
            description="Configure query complexity limits with depth enforcement for GraphQL schema.",
            acceptance_criteria=[
                "Complexity scoring calculates field costs.",
                "Depth limit enforcement prevents deep nesting.",
                "Query performance benchmark tests verify behavior.",
            ],
            validation_commands=["npm run test:graphql", "npm run graphql-performance-test"],
        )
    )

    record = result.records[0]
    assert any("title: Add GraphQL complexity scoring" in item for item in record.evidence)
    assert any("description: Configure query complexity limits" in item for item in record.evidence)
    assert any("acceptance_criteria[0]: Complexity scoring calculates field costs" in item for item in record.evidence)
    assert any("validation_commands: npm run graphql-performance-test" in item for item in record.evidence)


def test_metadata_field_signal_detection():
    result = build_task_graphql_query_readiness_plan(
        _task(
            "task-metadata",
            title="Update GraphQL API",
            metadata={
                "graphql_config": {
                    "complexity_scoring": True,
                    "depth_limit": 10,
                    "dataloader": "enabled",
                },
                "schema_definition": "SDL",
            },
        )
    )

    record = result.records[0]
    # Metadata keys trigger signals for schema_definition, complexity_scoring, depth_limit, dataloader
    assert {"schema_definition", "dataloader_pattern"} <= set(record.detected_signals)
    assert {"complexity_scoring", "depth_limit_enforcement"} <= set(record.present_safeguards)


def test_recommended_checks_match_missing_safeguards():
    result = build_task_graphql_query_readiness_plan(
        _task(
            "task-recommendations",
            title="Add GraphQL schema",
            description="Define GraphQL types with query complexity limits.",
        )
    )

    record = result.records[0]
    assert len(record.recommended_checks) == len(record.missing_safeguards)
    assert all(check for check in record.recommended_checks)
    assert record.recommended_checks == record.recommendations
    assert record.recommended_checks == record.recommended_actions


def test_signal_and_safeguard_ordering_is_deterministic():
    result = build_task_graphql_query_readiness_plan(
        _task(
            "task-order",
            title="GraphQL configuration",
            description=(
                "DataLoader pattern, N+1 risk, field resolver, subscription support, "
                "mutation operation, query complexity limit, schema definition."
            ),
            acceptance_criteria=[
                "Query performance benchmark tests, schema validation, resolver error handling, "
                "DataLoader implementation, query timeout config, depth limit enforcement, complexity scoring."
            ],
        )
    )

    record = result.records[0]
    # Signals should be in _SIGNAL_ORDER
    expected_signals = (
        "schema_definition",
        "query_complexity_limit",
        "mutation_operation",
        "subscription_support",
        "field_resolver",
        "n_plus_1_risk",
        "dataloader_pattern",
    )
    assert record.detected_signals == expected_signals

    # Safeguards should be in _SAFEGUARD_ORDER
    expected_safeguards = (
        "complexity_scoring",
        "depth_limit_enforcement",
        "query_timeout_config",
        "dataloader_implementation",
        "resolver_error_handling",
        "schema_validation",
        "query_performance_tests",
    )
    assert record.present_safeguards == expected_safeguards


def test_summary_counts_match_records():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task("weak-1", title="Add GraphQL", description="GraphQL schema with query complexity."),
                _task(
                    "partial-1",
                    title="GraphQL middleware",
                    description="GraphQL schema with complexity scoring.",
                    acceptance_criteria=["Complexity scoring.", "Depth limit enforcement.", "Query performance benchmark tests."],
                ),
                _task(
                    "strong-1",
                    title="Full GraphQL",
                    description="GraphQL schema with query complexity limits.",
                    acceptance_criteria=[
                        "Complexity scoring.",
                        "Depth limit enforcement.",
                        "Query timeout config.",
                        "DataLoader implementation.",
                        "Resolver error handling.",
                        "Schema validation.",
                        "Query performance benchmark tests.",
                    ],
                ),
                _task("no-impact", title="Database migration", description="Add column."),
            ]
        )
    )

    assert result.summary["task_count"] == 4
    assert result.summary["graphql_task_count"] == 3
    assert result.summary["no_impact_task_count"] == 1
    assert result.summary["readiness_counts"]["weak"] == 1
    assert result.summary["readiness_counts"]["partial"] == 1
    assert result.summary["readiness_counts"]["strong"] == 1
    assert result.summary["signal_counts"]["schema_definition"] == 3
    # Both partial and strong tasks have query performance tests
    assert result.summary["present_safeguard_counts"]["query_performance_tests"] == 2


def test_high_impact_with_missing_complexity_scoring():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-high-impact",
                    title="Add GraphQL schema",
                    description="GraphQL schema definition with query complexity limits.",
                    # No complexity scoring safeguard
                )
            ]
        )
    )

    record = result.records[0]
    assert "schema_definition" in record.detected_signals
    assert "query_complexity_limit" in record.detected_signals
    assert "complexity_scoring" in record.missing_safeguards
    # High impact due to high-impact signal + missing complexity_scoring
    assert record.impact == "high"


def test_high_impact_with_n_plus_1_risk_and_missing_dataloader():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-n-plus-1-impact",
                    title="Add nested resolvers",
                    description="GraphQL schema with N+1 risk in nested queries.",
                    # No DataLoader implementation
                )
            ]
        )
    )

    record = result.records[0]
    assert "schema_definition" in record.detected_signals
    assert "n_plus_1_risk" in record.detected_signals
    assert "dataloader_implementation" in record.missing_safeguards
    # High impact due to high-impact signal + n_plus_1_risk + missing dataloader
    assert record.impact == "high"


def test_partial_readiness_with_three_safeguards():
    result = build_task_graphql_query_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial-readiness",
                    title="Add GraphQL protection",
                    description="GraphQL schema with query complexity limits.",
                    acceptance_criteria=[
                        "Complexity scoring implemented.",
                        "Depth limit enforcement configured.",
                        "Query timeout config set.",
                        # Missing 4 other safeguards
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert len(record.present_safeguards) == 3
    assert len(record.missing_safeguards) == 4
    assert record.readiness == "partial"


def test_property_aliases_work_correctly():
    result = build_task_graphql_query_readiness_plan(
        _task(
            "task-aliases",
            title="Add GraphQL schema",
            description="GraphQL schema definition with complexity scoring.",
            acceptance_criteria=["Complexity scoring implemented."],
        )
    )

    record = result.records[0]
    assert record.signals == record.detected_signals
    assert record.safeguards == record.present_safeguards
    assert result.findings == result.records
    assert result.recommendations == result.records
    assert result.impacted_task_ids == result.graphql_task_ids


def _plan(tasks, *, plan_id="plan-graphql-query"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-graphql-query",
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
    if metadata is not None:
        task["metadata"] = metadata
    if validation_commands is not None:
        task["validation_commands"] = validation_commands
    return task
