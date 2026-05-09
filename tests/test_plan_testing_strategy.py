import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_testing_strategy import (
    TaskTestingRecord,
    TestingRequirement,
    build_testing_strategy_matrix,
    summarize_testing_strategy,
    testing_strategy_matrix_to_dict,
    testing_strategy_matrix_to_markdown,
)


def test_comprehensive_testing_strategy_extracts_multiple_test_types():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-unit",
                    title="Add user validation logic",
                    description="Implement user input validation with 90% unit test coverage using pytest.",
                    acceptance_criteria=["Unit tests pass", "Coverage >= 90%"],
                    test_command="pytest tests/unit/",
                ),
                _task(
                    "task-integration",
                    title="Add payment API integration",
                    description="Integrate with payment service API and add integration tests with mocks.",
                    files_or_modules=["api/payment.py"],
                ),
                _task(
                    "task-e2e",
                    title="Implement checkout flow",
                    description="End-to-end testing with Cypress for user checkout workflow.",
                    files_or_modules=["web/pages/checkout.tsx"],
                ),
            ],
            test_strategy="All features require unit and integration tests with 80% coverage minimum.",
        )
    )

    by_id = {record.task_id: record for record in matrix.records}

    # Unit test task
    assert "unit" in by_id["task-unit"].test_types
    assert by_id["task-unit"].completeness_score >= 0.5
    assert any("90%" in str(req.coverage_target) for req in by_id["task-unit"].requirements)

    # Integration test task
    assert "integration" in by_id["task-integration"].test_types
    assert any(req.data_strategy == "mocks" for req in by_id["task-integration"].requirements)

    # E2E test task
    assert "e2e" in by_id["task-e2e"].test_types
    assert any("cypress" in req.tools_mentioned for req in by_id["task-e2e"].requirements)

    assert matrix.summary["task_count"] == 3
    assert matrix.summary["average_completeness_score"] > 0


def test_property_based_testing_signals_are_detected():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-parser",
                    title="Implement JSON parser validation",
                    description=(
                        "Build JSON parser with property-based testing using Hypothesis. "
                        "Test all edge cases with generated inputs."
                    ),
                    files_or_modules=["parsers/json_parser.py"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "property_based" in record.test_types
    assert any("hypothesis" in req.tools_mentioned for req in record.requirements)
    assert "no_property_testing" not in record.gaps


def test_chaos_engineering_requirements_are_extracted():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-resilience",
                    title="Add fault injection testing",
                    description=(
                        "Implement chaos engineering tests with fault injection for critical "
                        "payment service. Test resilience under network failures."
                    ),
                    metadata={"test_types": "chaos,integration"},
                    risk_level="high",
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "chaos" in record.test_types
    assert "no_chaos_testing" not in record.gaps
    assert record.completeness_score > 0.14  # Chaos tests without unit tests get lower score


def test_canary_testing_strategy_is_identified():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-deploy",
                    title="Add canary deployment validation",
                    description=(
                        "Implement canary testing for gradual rollout with feature toggles. "
                        "Monitor metrics during progressive deployment to production."
                    ),
                    files_or_modules=["deploy/canary.py"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "canary" in record.test_types
    assert "missing_canary_validation" not in record.gaps


def test_visual_regression_and_accessibility_testing_signals():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-ui",
                    title="Add visual regression tests",
                    description=(
                        "Implement visual regression testing with Percy. "
                        "Add accessibility tests for WCAG compliance with axe-core."
                    ),
                    files_or_modules=["tests/visual/", "tests/a11y/"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "visual_regression" in record.test_types
    assert "accessibility" in record.test_types
    assert any("percy" in req.tools_mentioned for req in record.requirements)


def test_performance_and_security_testing_gaps_identified():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    title="Build authentication API",
                    description=(
                        "Create auth API with token encryption and validation. "
                        "Needs to handle 1000 concurrent requests with low latency."
                    ),
                    files_or_modules=["api/auth.py"],
                )
            ]
        )
    )

    record = matrix.records[0]
    # Should detect need for security and performance tests
    if "security" not in record.test_types:
        assert "missing_security_tests" in record.gaps
    if "performance" not in record.test_types:
        assert "missing_performance_tests" in record.gaps


def test_testing_gaps_for_missing_coverage():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-no-tests",
                    title="Add feature flag service",
                    description="Implement feature flag service for gradual rollouts.",
                    files_or_modules=["services/feature_flags.py"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "missing_unit_tests" in record.gaps
    assert "inadequate_coverage" in record.gaps
    assert record.completeness_score < 0.5


def test_flaky_test_signals_are_detected():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-fix-flaky",
                    title="Fix flaky integration tests",
                    description=(
                        "Resolve intermittent failures in payment integration tests. "
                        "Tests are non-deterministic and fail randomly in CI."
                    ),
                    acceptance_criteria=["No flaky tests", "Stable CI pipeline"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "flaky_tests" in record.gaps
    assert "integration" in record.test_types


def test_environment_parity_requirements():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-env",
                    title="Set up test environment",
                    description=(
                        "Create Docker-based test environment with environment parity. "
                        "Tests run in CI with containerized dependencies."
                    ),
                    files_or_modules=[".github/workflows/test.yml"],
                )
            ]
        )
    )

    record = matrix.records[0]
    # Should detect at least one of the mentioned environments
    environments = {req.environment for req in record.requirements if req.environment}
    assert "ci" in environments or "containerized" in environments


def test_test_data_strategy_extraction():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-fixtures",
                    title="Add test fixtures",
                    description=(
                        "Create test fixtures with FactoryBot for user models. "
                        "Use Faker for generating test data."
                    ),
                    files_or_modules=["tests/fixtures/users.py"],
                ),
                _task(
                    "task-mocks",
                    title="Add API mocks",
                    description="Mock external API calls with jest.mock for unit tests.",
                    files_or_modules=["tests/mocks/api.ts"],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in matrix.records}

    # Fixtures task
    assert any(req.data_strategy == "fixtures" for req in by_id["task-fixtures"].requirements)

    # Mocks task
    assert any(req.data_strategy == "mocks" for req in by_id["task-mocks"].requirements)


def test_automation_level_detection():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-auto",
                    title="Automated CI tests",
                    description="Fully automated tests in CI pipeline with pytest.",
                    test_command="pytest --cov",
                ),
                _task(
                    "task-manual",
                    title="Manual QA testing",
                    description="Manual testing required for UI validation.",
                    metadata={"automation": "manual"},
                ),
                _task(
                    "task-partial",
                    title="Partially automated tests",
                    description="Semi-automated tests with some manual verification steps.",
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in matrix.records}

    assert by_id["task-auto"].automation_level == "fully_automated"
    assert by_id["task-manual"].automation_level == "manual"
    assert "manual_only_tests" in by_id["task-manual"].gaps


def test_completeness_scoring_with_comprehensive_strategy():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-comprehensive",
                    title="Comprehensive testing suite",
                    description=(
                        "Implement comprehensive testing: unit tests with 95% coverage, "
                        "integration tests with test fixtures, e2e tests with Playwright, "
                        "property-based tests with Hypothesis, performance tests with k6, "
                        "and security tests. All automated in CI."
                    ),
                    test_command="pytest --cov && playwright test && k6 run load.js",
                    files_or_modules=["tests/"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert record.completeness_score > 0.7
    assert "unit" in record.test_types
    assert "integration" in record.test_types
    assert "e2e" in record.test_types
    assert "property_based" in record.test_types
    assert len(record.gaps) < 3


def test_smoke_and_contract_testing_detection():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-smoke",
                    title="Add smoke tests",
                    description="Implement smoke tests for health checks and basic validation.",
                ),
                _task(
                    "task-contract",
                    title="Add contract tests",
                    description="Add Pact consumer-driven contract tests for API compatibility.",
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in matrix.records}

    assert "smoke" in by_id["task-smoke"].test_types
    assert "contract" in by_id["task-contract"].test_types


def test_stable_ordering_by_completeness_and_automation():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-low",
                    title="Low coverage task",
                    description="Basic implementation, no tests.",
                ),
                _task(
                    "task-high",
                    title="High coverage task",
                    description="Comprehensive unit, integration, and e2e tests with 95% coverage.",
                ),
                _task(
                    "task-medium",
                    title="Medium coverage task",
                    description="Unit tests with 80% coverage using pytest.",
                ),
            ]
        )
    )

    # Should be ordered by completeness score (descending)
    scores = [record.completeness_score for record in matrix.records]
    assert scores == sorted(scores, reverse=True)


def test_summary_metrics_and_recommendations():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-1",
                    title="Feature A",
                    description="Add feature with basic tests.",
                ),
                _task(
                    "task-2",
                    title="Feature B",
                    description="Add feature with unit and integration tests.",
                ),
                _task(
                    "task-3",
                    title="Algorithm implementation",
                    description="Implement complex algorithm, needs validation testing.",
                ),
            ]
        )
    )

    summary = matrix.summary

    assert summary["task_count"] == 3
    assert 0 <= summary["average_completeness_score"] <= 1
    assert isinstance(summary["test_types_coverage"], dict)
    assert summary["total_gaps_count"] >= 0
    assert isinstance(summary["recommendations"], list)


def test_dictionary_serialization_and_markdown_are_json_compatible():
    plan = _plan(
        [
            _task(
                "task-test",
                title="Add comprehensive tests",
                description="Unit tests with 90% coverage using pytest and mocks.",
                metadata={"test_types": "unit,integration"},
            )
        ]
    )
    model = ExecutionPlan.model_validate(plan)

    matrix = build_testing_strategy_matrix(model)
    payload = testing_strategy_matrix_to_dict(matrix)
    markdown = testing_strategy_matrix_to_markdown(matrix)

    assert payload == matrix.to_dict()
    assert matrix.to_dicts() == payload["records"]
    assert summarize_testing_strategy(matrix) == matrix.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "records"]
    assert markdown.startswith("# Testing Strategy Matrix: plan-testing")
    assert "## Summary" in markdown
    assert "## Testing Matrix" in markdown


def test_empty_plan_returns_empty_matrix():
    matrix = build_testing_strategy_matrix(_plan([]))

    assert len(matrix.records) == 0
    assert matrix.summary["task_count"] == 0
    assert matrix.summary["average_completeness_score"] == 0.0


def test_explicit_metadata_test_types_are_used():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-explicit",
                    title="Explicit test types",
                    description="Task with explicit test type configuration.",
                    metadata={"test_types": "unit,integration,e2e,performance"},
                )
            ]
        )
    )

    record = matrix.records[0]
    assert "unit" in record.test_types
    assert "integration" in record.test_types
    assert "e2e" in record.test_types
    assert "performance" in record.test_types


def test_no_testing_required_tasks():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-no-test",
                    title="Documentation update",
                    description="Update README, no test required.",
                    files_or_modules=["README.md"],
                )
            ]
        )
    )

    record = matrix.records[0]
    # Should not add missing_unit_tests gap if explicitly no test
    assert "missing_unit_tests" not in record.gaps or len(record.test_types) > 0


def test_integration_required_detection():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-db",
                    title="Database migration",
                    description="Add database schema changes with migration scripts.",
                    files_or_modules=["migrations/001_add_users.sql"],
                )
            ]
        )
    )

    record = matrix.records[0]
    # Should identify need for integration tests
    if "integration" not in record.test_types:
        assert "missing_integration_tests" in record.gaps


def test_e2e_required_detection():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-flow",
                    title="User onboarding workflow",
                    description="Implement complete user onboarding journey from signup to first login.",
                    files_or_modules=["frontend/pages/onboarding.tsx"],
                )
            ]
        )
    )

    record = matrix.records[0]
    # Should identify need for e2e tests
    if "e2e" not in record.test_types:
        assert "missing_e2e_tests" in record.gaps


def test_coverage_target_extraction():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-coverage",
                    title="High coverage implementation",
                    description="Implement with 95% code coverage threshold.",
                    acceptance_criteria=["95% coverage achieved"],
                )
            ]
        )
    )

    record = matrix.records[0]
    assert any("95%" in str(req.coverage_target) for req in record.requirements)


def test_multiple_tools_mentioned():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(
                    "task-tools",
                    title="Multi-tool testing setup",
                    description=(
                        "Set up testing with Jest for unit tests, Playwright for e2e, "
                        "and K6 for performance testing."
                    ),
                )
            ]
        )
    )

    record = matrix.records[0]
    all_tools = set()
    for req in record.requirements:
        all_tools.update(req.tools_mentioned)

    assert "jest" in all_tools or "playwright" in all_tools or "k6" in all_tools


def test_recommendations_for_low_coverage():
    matrix = build_testing_strategy_matrix(
        _plan(
            [
                _task(f"task-{i}", title=f"Feature {i}", description="Basic feature.")
                for i in range(5)
            ]
        )
    )

    summary = matrix.summary

    # Should recommend adding tests if most tasks have low coverage
    recommendations_text = " ".join(summary["recommendations"])
    assert len(summary["recommendations"]) > 0


def _plan(tasks, test_strategy=None):
    plan = {
        "id": "plan-testing",
        "implementation_brief_id": "brief-testing",
        "milestones": [],
        "tasks": tasks,
    }
    if test_strategy:
        plan["test_strategy"] = test_strategy
    return plan


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    test_command=None,
    metadata=None,
    risk_level=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "depends_on": [],
        "status": "pending",
    }
    if test_command is not None:
        task["test_command"] = test_command
    if metadata is not None:
        task["metadata"] = metadata
    if risk_level is not None:
        task["risk_level"] = risk_level
    return task
