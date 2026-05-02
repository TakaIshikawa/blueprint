import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_graceful_degradation_readiness import (
    TaskGracefulDegradationReadinessPlan,
    TaskGracefulDegradationReadinessRecord,
    analyze_task_graceful_degradation_readiness,
    build_task_graceful_degradation_readiness_plan,
    extract_task_graceful_degradation_readiness,
    generate_task_graceful_degradation_readiness,
    recommend_task_graceful_degradation_readiness,
    summarize_task_graceful_degradation_readiness,
    task_graceful_degradation_readiness_plan_to_dict,
    task_graceful_degradation_readiness_plan_to_markdown,
)


def test_strong_graceful_degradation_readiness_covers_all_safeguards():
    result = build_task_graceful_degradation_readiness_plan(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Add recommendations fallback mode",
                    description=(
                        "When the expensive recommendations service fails, fall back to last-known-good "
                        "cached results in degraded mode."
                    ),
                    files_or_modules=["src/recommendations/stale_cache_fallback.py"],
                    acceptance_criteria=[
                        "Users see a fallback state banner explaining recommendations may be stale.",
                        "Dependency timeout is 750 ms with cancellation.",
                        "Telemetry logs fallback events and degraded mode metrics.",
                        "Health checks detect recovery and return to normal fresh results.",
                    ],
                    validation_plan=[
                        "Tests simulate dependency failure, service unavailable, timeout, and recovery."
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskGracefulDegradationReadinessPlan)
    assert result.degradation_task_ids == ("task-strong",)
    assert result.not_applicable_task_ids == ()
    record = result.records[0]
    assert isinstance(record, TaskGracefulDegradationReadinessRecord)
    assert record.degradation_signals == (
        "fallback",
        "degraded_mode",
        "optional_dependency_failure",
        "stale_cache_fallback",
    )
    assert record.present_safeguards == (
        "fallback_ux_state",
        "dependency_timeout",
        "degraded_telemetry",
        "recovery_behavior",
        "dependency_failure_tests",
    )
    assert record.missing_safeguards == ()
    assert record.readiness_level == "strong"
    assert record.recommended_checks == ()
    assert "files_or_modules: src/recommendations/stale_cache_fallback.py" in record.evidence
    assert any("validation_plan[0]" in item for item in record.evidence)
    assert result.summary["readiness_counts"] == {"missing": 0, "partial": 0, "strong": 1}
    assert result.summary["signal_counts"]["stale_cache_fallback"] == 1


def test_partial_readiness_detects_metadata_validation_and_expected_paths():
    plan = _plan(
        [
            _task(
                "task-partial",
                title="Degrade checkout promotions",
                description="Use partial availability when the optional promotion integration is unavailable.",
                expected_files=["src/checkout/circuit_breaker.ts"],
                acceptance_criteria=[
                    "Temporarily unavailable message appears beside the promotion panel.",
                    "Timeout is bounded before checkout continues.",
                ],
                metadata={
                    "fallback_behavior": "Circuit breaker opens for provider failure.",
                    "validation": {
                        "dependency_failure": "Manual validation covers provider outage.",
                    },
                },
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = analyze_task_graceful_degradation_readiness(plan)

    assert plan == original
    record = result.records[0]
    assert record.degradation_signals == (
        "fallback",
        "partial_availability",
        "circuit_breaker",
        "optional_dependency_failure",
        "feature_unavailable_message",
    )
    assert record.present_safeguards == (
        "fallback_ux_state",
        "dependency_timeout",
        "dependency_failure_tests",
    )
    assert record.missing_safeguards == ("degraded_telemetry", "recovery_behavior")
    assert record.readiness_level == "partial"
    assert record.recommended_checks == (
        "Emit logs, metrics, or alerts when fallback or degraded mode starts and ends.",
        "Specify how the task detects recovery and returns to the normal path.",
    )
    assert "expected_files: src/checkout/circuit_breaker.ts" in record.evidence
    assert any("metadata.fallback_behavior" in item for item in record.evidence)
    assert any("metadata.validation.dependency_failure" in item for item in record.evidence)


def test_missing_readiness_has_all_required_safeguards_recommended():
    result = build_task_graceful_degradation_readiness_plan(
        _plan(
            [
                _task(
                    "task-missing",
                    title="Add read-only mode for billing",
                    description="If the downstream billing service has a partial outage, show read-only mode.",
                    files_or_modules=["src/billing/read_only_mode.py"],
                    acceptance_criteria=["Billing pages render during an outage."],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.degradation_signals == (
        "partial_availability",
        "optional_dependency_failure",
        "read_only_mode",
    )
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "fallback_ux_state",
        "dependency_timeout",
        "degraded_telemetry",
        "recovery_behavior",
        "dependency_failure_tests",
    )
    assert record.readiness_level == "missing"
    assert len(record.recommended_checks) == 5
    assert result.summary["missing_safeguard_counts"] == {
        "fallback_ux_state": 1,
        "dependency_timeout": 1,
        "degraded_telemetry": 1,
        "recovery_behavior": 1,
        "dependency_failure_tests": 1,
    }


def test_non_applicable_tasks_are_reported_in_dict_and_markdown():
    result = build_task_graceful_degradation_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="Adjust local settings labels.",
                    files_or_modules=["src/ui/settings_panel.tsx"],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.degradation_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "degradation_task_count": 0,
        "degradation_task_ids": [],
        "not_applicable_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "readiness_counts": {"missing": 0, "partial": 0, "strong": 0},
        "missing_safeguard_counts": {
            "fallback_ux_state": 0,
            "dependency_timeout": 0,
            "degraded_telemetry": 0,
            "recovery_behavior": 0,
            "dependency_failure_tests": 0,
        },
        "signal_counts": {
            "fallback": 0,
            "degraded_mode": 0,
            "partial_availability": 0,
            "circuit_breaker": 0,
            "optional_dependency_failure": 0,
            "stale_cache_fallback": 0,
            "read_only_mode": 0,
            "feature_unavailable_message": 0,
        },
    }
    markdown = result.to_markdown()
    assert "No graceful degradation readiness records were inferred." in markdown
    assert "- Applicable task ids: none" in markdown
    assert "- Not-applicable task ids: task-copy" in markdown


def test_deterministic_serialization_markdown_aliases_and_execution_plan_input():
    source = _plan(
        [
            _task(
                "task-z",
                title="Fallback UX | unavailable messaging",
                description="Feature unavailable messaging appears when the provider failure triggers fallback.",
                acceptance_criteria=["A user-visible fallback state is shown."],
            ),
            _task(
                "task-a",
                title="Optional fraud service degraded mode",
                description="Degrade gracefully when the optional fraud service fails.",
                acceptance_criteria=[
                    "Fallback UX explains the degraded state.",
                    "Dependency timeout is configured.",
                    "Telemetry records degraded events.",
                    "Recovery behavior exits degraded mode after health checks.",
                    "Tests simulate dependency failure.",
                ],
            ),
            _task("task-copy", title="Adjust empty state", description="Change local UI copy."),
        ]
    )
    model = ExecutionPlan.model_validate(source)

    result = summarize_task_graceful_degradation_readiness(model)
    payload = task_graceful_degradation_readiness_plan_to_dict(result)
    markdown = task_graceful_degradation_readiness_plan_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert extract_task_graceful_degradation_readiness(source).to_dict() == summarize_task_graceful_degradation_readiness(source).to_dict()
    assert generate_task_graceful_degradation_readiness(source).to_dict() == summarize_task_graceful_degradation_readiness(source).to_dict()
    assert recommend_task_graceful_degradation_readiness(source).to_dict() == summarize_task_graceful_degradation_readiness(source).to_dict()
    assert result.plan_id == "plan-degradation"
    assert result.degradation_task_ids == ("task-z", "task-a")
    assert result.not_applicable_task_ids == ("task-copy",)
    assert [record.readiness_level for record in result.records] == ["partial", "strong"]
    assert list(payload) == [
        "plan_id",
        "records",
        "degradation_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "degradation_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness_level",
        "recommended_checks",
        "evidence",
    ]
    assert markdown.startswith("# Task Graceful Degradation Readiness: plan-degradation")
    assert "Fallback UX \\| unavailable messaging" in markdown
    assert "| Task | Title | Readiness | Signals | Missing Safeguards | Recommended Checks | Evidence |" in markdown
    assert "- Applicable task ids: task-z, task-a" in markdown
    assert "- Not-applicable task ids: task-copy" in markdown


def _plan(tasks, plan_id="plan-degradation"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-degradation",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    expected_files=None,
    acceptance_criteria=None,
    validation_plan=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-degradation",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if expected_files is not None:
        payload["expected_files"] = expected_files
    if validation_plan is not None:
        payload["validation_plan"] = validation_plan
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
