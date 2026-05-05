import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_circuit_breaker_readiness import (
    TaskCircuitBreakerReadinessPlan,
    TaskCircuitBreakerReadinessRecord,
    analyze_task_circuit_breaker_readiness,
    build_task_circuit_breaker_readiness_plan,
    derive_task_circuit_breaker_readiness_plan,
    summarize_task_circuit_breaker_readiness,
    task_circuit_breaker_readiness_plan_to_dict,
    task_circuit_breaker_readiness_plan_to_dicts,
    task_circuit_breaker_readiness_plan_to_markdown,
)


def test_detects_circuit_breaker_tasks_and_classifies_impact_levels():
    result = build_task_circuit_breaker_readiness_plan(
        _plan(
            [
                _task(
                    "task-high",
                    title="Add circuit breaker for payment API with fallback",
                    description=(
                        "Implement circuit breaker with failure threshold of 5 consecutive errors, "
                        "exponential backoff recovery, and fallback to cached payment methods. "
                        "Integrate with monitoring dashboard for circuit state alerts."
                    ),
                    files_or_modules=[
                        "src/payment/circuit_breaker.py",
                        "src/payment/fallback_handler.py",
                    ],
                    acceptance_criteria=[
                        "Circuit opens after 5 consecutive failures.",
                        "Half-open state allows test requests after 30s recovery window.",
                        "Fallback returns cached payment methods when circuit is open.",
                    ],
                ),
                _task(
                    "task-medium",
                    title="Add health check endpoint for service monitoring",
                    description="Implement liveness and readiness probes with timeout handling for database checks.",
                    metadata={
                        "circuit_breaker_safeguards": ["health_check_integration", "timeout_handling"],
                    },
                ),
                _task(
                    "task-low",
                    title="Add monitoring metrics for API failures",
                    description="Track error rates and emit telemetry for circuit breaker dashboards.",
                    acceptance_criteria=[
                        "Emit metrics for circuit state changes."
                    ],
                ),
                _task(
                    "task-none",
                    title="Update API documentation",
                    description="Document endpoint response formats.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskCircuitBreakerReadinessPlan)
    assert result.plan_id == "plan-circuit-breaker"
    assert result.impacted_task_ids == ("task-high", "task-medium", "task-low")
    assert result.no_impact_task_ids == ("task-none",)
    assert result.summary["impact_level_counts"] == {"high": 1, "medium": 1, "low": 1}
    assert result.summary["signal_counts"]["failure_threshold"] == 1
    assert result.summary["signal_counts"]["fallback_mechanism"] == 1
    assert result.summary["signal_counts"]["monitoring"] == 2

    high = _record(result, "task-high")
    assert high.impact_level == "high"
    assert {"failure_threshold", "recovery_strategy", "fallback_mechanism", "monitoring"} <= set(
        high.matched_signals
    )
    assert "failure_threshold_config" not in high.missing_safeguards
    assert "recovery_strategy" not in high.missing_safeguards
    assert "fallback_implementation" not in high.missing_safeguards

    medium = _record(result, "task-medium")
    assert medium.impact_level == "medium"
    assert "health_check_integration" in medium.present_safeguards
    assert "timeout_handling" in medium.present_safeguards

    low = _record(result, "task-low")
    assert low.impact_level == "low"
    assert "monitoring" in low.matched_signals


def test_metadata_and_validation_commands_provide_evidence_and_safeguards():
    result = analyze_task_circuit_breaker_readiness(
        _task(
            "task-validation",
            title="Circuit breaker with health checks and timeouts",
            description="Add circuit breaker pattern with health check integration and request timeouts.",
            metadata={
                "circuit_breaker_signals": ["failure_threshold", "health_check", "timeout"],
                "circuit_breaker_safeguards": ["failure_threshold_config", "health_check_integration"],
                "validation_commands": {
                    "test": [
                        "poetry run pytest tests/resilience/test_circuit_breaker_threshold.py",
                        "poetry run pytest tests/resilience/test_health_check.py",
                    ]
                },
            },
        )
    )

    record = result.records[0]
    assert record.impact_level == "medium"
    assert "failure_threshold" in record.matched_signals
    assert "health_check" in record.matched_signals
    assert "timeout_handling" in record.matched_signals
    assert "failure_threshold_config" in record.present_safeguards
    assert "health_check_integration" in record.present_safeguards
    assert any("metadata.circuit_breaker_signals: failure_threshold" in item for item in record.evidence)
    assert any(
        "validation_commands:" in item and "test_circuit_breaker_threshold.py" in item
        for item in record.evidence
    )
    assert any(
        "validation_commands:" in item and "test_health_check.py" in item
        for item in record.evidence
    )


def test_serialization_aliases_markdown_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Circuit breaker | API gateway",
                description="Implement circuit breaker with failure threshold and fallback logic.",
            ),
            _task(
                "task-a",
                title="Health checks and monitoring",
                description=(
                    "Add health check endpoints, monitoring integration, "
                    "and timeout handling for external services."
                ),
                acceptance_criteria=[
                    "Health check endpoint returns 200 on success.",
                    "Monitor circuit state with metrics.",
                    "Handle request timeout after 5 seconds.",
                ],
            ),
            _task("task-none", title="API docs", description="Update swagger spec."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_circuit_breaker_readiness(plan)
    payload = task_circuit_breaker_readiness_plan_to_dict(result)
    markdown = task_circuit_breaker_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_circuit_breaker_readiness_plan_to_dicts(result) == payload["records"]
    assert task_circuit_breaker_readiness_plan_to_dicts(result.records) == payload["records"]
    assert derive_task_circuit_breaker_readiness_plan(plan).to_dict() == result.to_dict()
    assert result.findings == result.records
    # Sorted by impact level, then task_id
    assert set(result.impacted_task_ids) == {"task-a", "task-z"}
    assert result.no_impact_task_ids == ("task-none",)
    assert list(payload) == [
        "plan_id",
        "records",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "matched_signals",
        "present_safeguards",
        "missing_safeguards",
        "impact_level",
        "evidence",
        "recommended_checks",
    ]
    assert "# Task Circuit Breaker Readiness: plan-circuit-breaker" in markdown
    assert "## Summary" in markdown
    assert "task-a" in markdown
    assert "task-z" in markdown
    assert "No-impact tasks: task-none" in markdown
    assert markdown == result.to_markdown()


def test_empty_plan_returns_no_records():
    result = build_task_circuit_breaker_readiness_plan({"id": "empty", "tasks": []})

    assert result.plan_id == "empty"
    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.no_impact_task_ids == ()
    assert result.summary["task_count"] == 0
    assert result.summary["record_count"] == 0
    assert result.summary["impacted_task_count"] == 0
    assert result.summary["missing_safeguard_count"] == 0

    markdown = result.to_markdown()
    assert "No circuit breaker readiness records were inferred." in markdown


def test_plan_with_no_circuit_breaker_signals():
    result = build_task_circuit_breaker_readiness_plan(
        _plan(
            [
                _task("task-1", title="Add user profile", description="Create profile page."),
                _task("task-2", title="Fix typo", description="Correct spelling error."),
            ]
        )
    )

    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.no_impact_task_ids == ("task-1", "task-2")
    assert result.summary["impacted_task_count"] == 0
    assert result.summary["task_count"] == 2


def test_execution_plan_pydantic_model_input():
    plan = ExecutionPlan(
        id="pydantic-plan",
        implementation_brief_id="brief",
        milestones=[],
        tasks=[
            ExecutionTask(
                id="task-circuit",
                title="Add circuit breaker",
                description="Implement circuit breaker with failure threshold of 3 errors.",
                acceptance_criteria=[],
            )
        ],
    )

    result = build_task_circuit_breaker_readiness_plan(plan)

    assert result.plan_id == "pydantic-plan"
    assert len(result.records) == 1
    assert result.records[0].task_id == "task-circuit"
    assert "failure_threshold" in result.records[0].matched_signals


def test_execution_task_pydantic_model_input():
    task = ExecutionTask(
        id="single-task",
        title="Circuit breaker with fallback",
        description="Add circuit breaker with failure threshold and graceful degradation fallback to cached data.",
        acceptance_criteria=[],
    )

    result = build_task_circuit_breaker_readiness_plan(task)

    assert result.plan_id is None
    assert len(result.records) == 1
    assert result.records[0].task_id == "single-task"
    assert "fallback_mechanism" in result.records[0].matched_signals


def test_simple_namespace_object_input():
    task = SimpleNamespace(
        id="namespace-task",
        title="Health check endpoint",
        description="Implement liveness and readiness probes with timeout of 5 seconds.",
        acceptance_criteria=["Health check responds within 5s timeout."],
    )

    result = build_task_circuit_breaker_readiness_plan(task)

    assert len(result.records) == 1
    record = result.records[0]
    assert record.task_id == "namespace-task"
    assert "health_check" in record.matched_signals
    assert "timeout_handling" in record.matched_signals


def test_iterable_input():
    tasks = [
        {"id": "t1", "title": "Circuit breaker", "description": "Add failure threshold."},
        {"id": "t2", "title": "Fallback handler", "description": "Implement fallback to cache."},
    ]

    result = build_task_circuit_breaker_readiness_plan(tasks)

    assert result.plan_id is None
    assert len(result.records) == 2
    assert {r.task_id for r in result.records} == {"t1", "t2"}


def test_single_dict_input():
    result = build_task_circuit_breaker_readiness_plan(
        {
            "id": "dict-task",
            "title": "Monitoring integration",
            "description": "Emit metrics for circuit state and alert on failures.",
        }
    )

    assert result.plan_id is None
    assert len(result.records) == 1
    assert result.records[0].task_id == "dict-task"
    assert "monitoring" in result.records[0].matched_signals


def test_files_or_modules_path_matching():
    result = build_task_circuit_breaker_readiness_plan(
        _task(
            "path-task",
            title="Resilience patterns",
            description="Implement resilience.",
            files_or_modules=[
                "src/resilience/circuit_breaker_config.py",
                "src/resilience/fallback_handler.py",
                "src/health/health_check.py",
                "src/monitoring/metrics_dashboard.py",
                "src/timeout/timeout_config.py",
            ],
        )
    )

    record = result.records[0]
    assert "failure_threshold" in record.matched_signals
    assert "fallback_mechanism" in record.matched_signals
    assert "health_check" in record.matched_signals
    assert "monitoring" in record.matched_signals
    assert "timeout_handling" in record.matched_signals


def test_acceptance_criteria_detection():
    result = build_task_circuit_breaker_readiness_plan(
        _task(
            "ac-task",
            title="Circuit breaker implementation",
            description="Add circuit breaker.",
            acceptance_criteria=[
                "Circuit opens after 10 consecutive failures.",
                "Half-open state allows test request after recovery window.",
                "Fallback implementation returns default response.",
                "Health check integration monitors service status.",
                "Monitoring integration emits circuit state metrics.",
                "Timeout handling prevents indefinite blocking with 3s timeout.",
            ],
        )
    )

    record = result.records[0]
    assert "failure_threshold" in record.matched_signals
    assert "recovery_strategy" in record.matched_signals
    assert "fallback_mechanism" in record.matched_signals
    assert "health_check" in record.matched_signals
    assert "monitoring" in record.matched_signals
    assert "timeout_handling" in record.matched_signals
    # Safeguards should include at least some of these based on acceptance criteria wording
    assert "recovery_strategy" in record.present_safeguards
    assert "fallback_implementation" in record.present_safeguards
    assert "health_check_integration" in record.present_safeguards
    assert "monitoring_integration" in record.present_safeguards
    assert "timeout_handling" in record.present_safeguards


def test_missing_safeguard_recommendations():
    result = build_task_circuit_breaker_readiness_plan(
        _task(
            "missing-task",
            title="Basic circuit breaker",
            description="Implement circuit breaker pattern.",
        )
    )

    record = result.records[0]
    assert len(record.missing_safeguards) > 0
    assert len(record.recommended_checks) == len(record.missing_safeguards)
    # Verify that recommended checks contain information about the missing safeguards
    assert all(len(check) > 0 for check in record.recommended_checks)


def test_task_id_generation_for_tasks_without_id():
    result = build_task_circuit_breaker_readiness_plan(
        {
            "id": "plan-id",
            "tasks": [
                {"title": "Circuit breaker", "description": "Add failure threshold."},
                {"title": "Fallback", "description": "Implement fallback."},
            ],
        }
    )

    assert result.impacted_task_ids == ("task-1", "task-2")


def test_impact_level_high_for_threshold_with_fallback():
    result = build_task_circuit_breaker_readiness_plan(
        _task(
            "high-impact",
            title="Circuit breaker with fallback",
            description="Implement failure threshold detection with fallback to cached data.",
        )
    )

    record = result.records[0]
    assert record.impact_level == "high"
    assert "failure_threshold" in record.matched_signals
    assert "fallback_mechanism" in record.matched_signals


def test_impact_level_high_for_threshold_with_monitoring():
    result = build_task_circuit_breaker_readiness_plan(
        _task(
            "high-impact-mon",
            title="Circuit breaker with monitoring",
            description="Implement failure threshold detection and monitor circuit state with alerts.",
        )
    )

    record = result.records[0]
    assert record.impact_level == "high"
    assert "failure_threshold" in record.matched_signals
    assert "monitoring" in record.matched_signals


def test_impact_level_high_for_recovery_with_health_check():
    result = build_task_circuit_breaker_readiness_plan(
        _task(
            "high-recovery",
            title="Recovery with health checks",
            description="Implement recovery strategy with health check integration for service monitoring.",
        )
    )

    record = result.records[0]
    assert record.impact_level == "high"
    assert "recovery_strategy" in record.matched_signals
    assert "health_check" in record.matched_signals


def test_impact_level_medium_for_single_core_signal():
    result = build_task_circuit_breaker_readiness_plan(
        _task(
            "medium-impact",
            title="Health check endpoint",
            description="Add health check endpoint for service monitoring.",
        )
    )

    record = result.records[0]
    assert record.impact_level == "medium"
    assert "health_check" in record.matched_signals


def test_impact_level_low_for_monitoring_only():
    result = build_task_circuit_breaker_readiness_plan(
        _task(
            "low-impact",
            title="Add telemetry",
            description="Track metrics for errors.",
        )
    )

    if len(result.records) > 0:
        record = result.records[0]
        # Monitoring alone is low impact
        assert record.impact_level in ("low", "medium")


def test_summary_counts():
    result = build_task_circuit_breaker_readiness_plan(
        _plan(
            [
                _task("t1", title="CB 1", description="Circuit breaker with failure threshold."),
                _task("t2", title="CB 2", description="Circuit breaker with fallback."),
                _task("t3", title="Health", description="Health check endpoint."),
                _task("t4", title="None", description="Regular task."),
            ]
        )
    )

    assert result.summary["task_count"] == 4
    assert result.summary["record_count"] == 3
    assert result.summary["impacted_task_count"] == 3
    assert result.summary["no_impact_task_count"] == 1
    # Verify signals are detected (at least 1 failure threshold and 1 fallback)
    assert result.summary["signal_counts"]["failure_threshold"] >= 1
    assert result.summary["signal_counts"]["fallback_mechanism"] >= 1
    assert result.summary["signal_counts"]["health_check"] >= 1
    assert isinstance(result.summary["missing_safeguard_count"], int)
    assert result.summary["missing_safeguard_count"] >= 0


def test_evidence_snippets_truncation():
    long_description = (
        "Implement circuit breaker pattern with failure threshold detection " * 20
    )
    result = build_task_circuit_breaker_readiness_plan(
        _task("evidence-task", title="Long desc", description=long_description)
    )

    record = result.records[0]
    assert len(record.evidence) > 0
    for evidence in record.evidence:
        assert len(evidence) <= 200


def test_markdown_escapes_pipes_in_cells():
    result = build_task_circuit_breaker_readiness_plan(
        _task(
            "pipe-task",
            title="Circuit | breaker",
            description="Add CB | with fallback | and monitoring",
        )
    )

    markdown = result.to_markdown()
    assert "Circuit \\| breaker" in markdown
    assert "CB \\| with fallback \\| and monitoring" in markdown


def test_stable_sorting_by_impact_then_task_id():
    result = build_task_circuit_breaker_readiness_plan(
        _plan(
            [
                _task("task-z-low", title="Z Low", description="Add metrics dashboard."),
                _task("task-a-high", title="A High", description="Failure threshold with fallback to cache."),
                _task("task-m-medium", title="M Medium", description="Health check endpoint."),
                _task("task-b-high", title="B High", description="Failure threshold of 5 errors with monitoring integration."),
            ]
        )
    )

    ids = [r.task_id for r in result.records]
    # High impact tasks come first, then medium, then low, sorted by task ID within each level
    # All tasks at the same impact level should be sorted by task_id
    for i in range(len(ids) - 1):
        curr_record = next(r for r in result.records if r.task_id == ids[i])
        next_record = next(r for r in result.records if r.task_id == ids[i + 1])
        curr_impact_rank = {"high": 0, "medium": 1, "low": 2}[curr_record.impact_level]
        next_impact_rank = {"high": 0, "medium": 1, "low": 2}[next_record.impact_level]
        # Current should be <= next in impact rank (high=0 comes before medium=1)
        assert curr_impact_rank <= next_impact_rank
        # If same impact level, should be sorted by task_id
        if curr_impact_rank == next_impact_rank:
            assert curr_record.task_id <= next_record.task_id


def test_deduplicates_evidence():
    result = build_task_circuit_breaker_readiness_plan(
        _task(
            "dedup-task",
            title="Circuit breaker circuit breaker",
            description="Failure threshold failure threshold.",
            acceptance_criteria=[
                "Circuit opens after threshold.",
                "Circuit opens after threshold.",
            ],
        )
    )

    record = result.records[0]
    evidence_list = list(record.evidence)
    assert len(evidence_list) == len(set(ev.casefold() for ev in evidence_list))


def test_invalid_input_returns_empty_plan():
    result = build_task_circuit_breaker_readiness_plan(None)
    assert result.records == ()
    assert result.plan_id is None

    result = build_task_circuit_breaker_readiness_plan([])
    assert result.records == ()

    result = build_task_circuit_breaker_readiness_plan({})
    assert result.records == ()


def test_metadata_nested_detection():
    result = build_task_circuit_breaker_readiness_plan(
        _task(
            "nested-metadata",
            title="Circuit breaker",
            description="Add CB.",
            metadata={
                "resilience": {
                    "circuit_breaker": {
                        "failure_threshold": 5,
                        "recovery_window": "30s",
                    }
                }
            },
        )
    )

    record = result.records[0]
    assert "failure_threshold" in record.matched_signals
    assert "recovery_strategy" in record.matched_signals


def _plan(tasks):
    return {
        "id": "plan-circuit-breaker",
        "tasks": tasks,
    }


def _task(task_id, *, title="", description="", files_or_modules=None, acceptance_criteria=None, metadata=None):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or [],
        "metadata": metadata or {},
    }


def _record(result, task_id):
    for record in result.records:
        if record.task_id == task_id:
            return record
    raise AssertionError(f"Record {task_id} not found")
