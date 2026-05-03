import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_timezone_boundary_readiness import (
    TaskTimezoneBoundaryReadinessPlan,
    TaskTimezoneBoundaryReadinessRecord,
    analyze_task_timezone_boundary_readiness,
    build_task_timezone_boundary_readiness_plan,
    derive_task_timezone_boundary_readiness,
    extract_task_timezone_boundary_readiness,
    generate_task_timezone_boundary_readiness,
    recommend_task_timezone_boundary_readiness,
    summarize_task_timezone_boundary_readiness,
    task_timezone_boundary_readiness_plan_to_dict,
    task_timezone_boundary_readiness_plan_to_dicts,
    task_timezone_boundary_readiness_plan_to_markdown,
)


def test_detects_timezone_boundary_signals_from_text_paths_metadata_and_validation_commands():
    result = build_task_timezone_boundary_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Implement localized scheduling and calendar events",
                    description="Move timezone-aware files into the scheduler.",
                    files_or_modules=[
                        "src/timezone/utc_timestamp_store.py",
                        "src/scheduling/localized_time_display.py",
                        "src/calendar/ics_event_rrule.py",
                        "src/billing/end_of_day_billing_period.py",
                        "tests/timezone/test_dst_transition_fixed_clock.py",
                    ],
                    tags=["date-range"],
                    validation_commands={
                        "dst": "pytest tests/timezone/test_dst_transition_fixed_clock.py",
                    },
                ),
                _task(
                    "task-text",
                    title="Handle timezone conversion for billing date ranges",
                    description=(
                        "Timezone conversion stores canonical timestamps in UTC, displays user local time, "
                        "handles daylight saving time, billing periods, date ranges, scheduling, "
                        "calendar events, end of day boundaries, recurring jobs, and locale date formatting."
                    ),
                    metadata={
                        "timezone_boundary": {
                            "server_client_timezone_mismatch": "Check browser timezone and server timezone assumptions.",
                        }
                    },
                ),
            ]
        )
    )

    assert isinstance(result, TaskTimezoneBoundaryReadinessPlan)
    assert result.plan_id == "plan-timezone"
    by_id = {record.task_id: record for record in result.records}
    assert set(by_id) == {"task-paths", "task-text"}
    assert set(by_id["task-paths"].timezone_signals) == {
        "timezone_conversion",
        "utc_storage",
        "local_display",
        "daylight_saving_time",
        "date_range",
        "billing_period",
        "scheduling",
        "calendar_event",
        "end_of_day_boundary",
        "recurring_job",
        "locale_date_format",
    }
    assert "dst_transition_tests" in by_id["task-paths"].present_safeguards
    assert "fixed_clock_tests" in by_id["task-paths"].present_safeguards
    assert "utc_persistence" in by_id["task-text"].present_safeguards
    assert "server_client_timezone_mismatch" in by_id["task-text"].present_safeguards
    assert any("files_or_modules:" in item for item in by_id["task-paths"].evidence)
    assert any("tags[0]:" in item for item in by_id["task-paths"].evidence)
    assert any("validation_commands" in item for item in by_id["task-paths"].evidence)
    assert any("metadata.timezone_boundary.server_client_timezone_mismatch" in item for item in by_id["task-text"].evidence)


def test_acceptance_criteria_safeguards_reduce_missing_and_strong_when_all_present():
    result = analyze_task_timezone_boundary_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Harden scheduled billing period timezone behavior",
                    description="Billing period date ranges support timezone conversion and recurring scheduling.",
                    acceptance_criteria=[
                        "DST transition tests cover spring forward and fall back ambiguous time cases.",
                        "Date ranges use inclusive start and exclusive end boundary semantics.",
                        "Canonical timestamps are persisted in UTC.",
                        "User timezone preference is stored on the account.",
                        "Server/client timezone mismatch is checked between API and browser.",
                        "Deterministic tests use a fixed clock fixture.",
                    ],
                ),
                _task(
                    "task-weak",
                    title="Add appointment scheduling",
                    description="Create appointments with a scheduled local date and time.",
                    acceptance_criteria=["User timezone preference is captured."],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    strong = by_id["task-strong"]
    weak = by_id["task-weak"]

    assert strong.missing_safeguards == ()
    assert strong.recommendations == ()
    assert strong.readiness == "strong"
    assert strong.impact_level == "high"
    assert weak.readiness == "weak"
    assert weak.missing_safeguards == (
        "dst_transition_tests",
        "inclusive_exclusive_boundaries",
        "utc_persistence",
        "server_client_timezone_mismatch",
        "fixed_clock_tests",
    )
    assert "user_timezone_preference" in weak.present_safeguards
    assert weak.recommendations[0].startswith("Add DST transition cases")
    assert result.timezone_task_ids == ("task-strong", "task-weak")
    assert result.summary["readiness_counts"] == {"weak": 1, "moderate": 0, "strong": 1}
    assert result.summary["missing_safeguard_counts"]["utc_persistence"] == 1


def test_vague_date_copy_is_suppressed_but_boundary_copy_is_not_high_impact_without_boundary_behavior():
    result = build_task_timezone_boundary_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update date copy",
                    description="Copy only: polish date labels and helper text.",
                    files_or_modules=["src/ui/settings_copy.py"],
                ),
                _task(
                    "task-format",
                    title="Update locale date format copy",
                    description="Copy change for locale-specific date formatting labels.",
                    acceptance_criteria=["Use MM/DD/YYYY and DD/MM/YYYY examples."],
                ),
                _task(
                    "task-boundary",
                    title="Clarify end of day date range copy",
                    description="Copy explains the end of day boundary for a date range filter.",
                ),
                _task(
                    "task-docs",
                    title="Document timezone conversion runbook",
                    description="Documentation for DST and timezone conversion behavior.",
                    files_or_modules=["docs/timezone-boundaries.md"],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert set(by_id) == {"task-boundary", "task-format"}
    assert by_id["task-format"].impact_level == "low"
    assert by_id["task-boundary"].impact_level == "high"
    assert result.suppressed_task_ids == ("task-copy", "task-docs")


def test_empty_invalid_summary_markdown_and_serialization_are_deterministic():
    empty = build_task_timezone_boundary_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_timezone_boundary_readiness_plan(13)

    assert empty.to_dict() == {
        "plan_id": "empty-plan",
        "records": [],
        "findings": [],
        "recommendations": [],
        "timezone_task_ids": [],
        "suppressed_task_ids": [],
        "summary": {
            "task_count": 0,
            "timezone_task_count": 0,
            "timezone_task_ids": [],
            "suppressed_task_count": 0,
            "suppressed_task_ids": [],
            "missing_safeguard_count": 0,
            "readiness_counts": {"weak": 0, "moderate": 0, "strong": 0},
            "impact_counts": {"high": 0, "medium": 0, "low": 0},
            "signal_counts": {
                "timezone_conversion": 0,
                "utc_storage": 0,
                "local_display": 0,
                "daylight_saving_time": 0,
                "date_range": 0,
                "billing_period": 0,
                "scheduling": 0,
                "calendar_event": 0,
                "end_of_day_boundary": 0,
                "recurring_job": 0,
                "locale_date_format": 0,
            },
            "present_safeguard_counts": {
                "dst_transition_tests": 0,
                "inclusive_exclusive_boundaries": 0,
                "utc_persistence": 0,
                "user_timezone_preference": 0,
                "server_client_timezone_mismatch": 0,
                "fixed_clock_tests": 0,
            },
            "missing_safeguard_counts": {
                "dst_transition_tests": 0,
                "inclusive_exclusive_boundaries": 0,
                "utc_persistence": 0,
                "user_timezone_preference": 0,
                "server_client_timezone_mismatch": 0,
                "fixed_clock_tests": 0,
            },
        },
    }
    assert empty.to_markdown() == "\n".join(
        [
            "# Task Timezone Boundary Readiness: empty-plan",
            "",
            "## Summary",
            "",
            "- Task count: 0",
            "- Timezone task count: 0",
            "- Suppressed task count: 0",
            "- Missing safeguard count: 0",
            "- Readiness counts: weak 0, moderate 0, strong 0",
            "- Impact counts: high 0, medium 0, low 0",
            (
                "- Signal counts: timezone_conversion 0, utc_storage 0, local_display 0, "
                "daylight_saving_time 0, date_range 0, billing_period 0, scheduling 0, "
                "calendar_event 0, end_of_day_boundary 0, recurring_job 0, locale_date_format 0"
            ),
            "",
            "No timezone boundary readiness records were inferred.",
        ]
    )
    assert invalid.records == ()
    assert invalid.summary["timezone_task_count"] == 0


def test_model_object_aliases_markdown_escaping_and_no_mutation_are_stable():
    object_task = SimpleNamespace(
        id="task-object",
        title="Object scheduling task",
        description="Scheduling displays user local time from UTC.",
        acceptance_criteria=["Canonical timestamps are persisted in UTC."],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Timezone conversion | scoped",
            description="Timezone conversion uses user timezone preference and fixed clock tests.",
        )
    )
    plan = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-a",
                title="Calendar event timezone",
                description="Calendar events handle DST and server/client timezone mismatch.",
            ),
        ],
        plan_id="plan-serialization",
    )
    original = copy.deepcopy(plan)

    result = summarize_task_timezone_boundary_readiness(plan)
    object_result = build_task_timezone_boundary_readiness_plan([object_task])
    model_result = generate_task_timezone_boundary_readiness(ExecutionPlan.model_validate(plan))
    payload = task_timezone_boundary_readiness_plan_to_dict(result)
    markdown = task_timezone_boundary_readiness_plan_to_markdown(result)

    assert plan == original
    assert isinstance(result.records[0], TaskTimezoneBoundaryReadinessRecord)
    assert object_result.records[0].task_id == "task-object"
    assert "utc_persistence" in object_result.records[0].present_safeguards
    assert model_result.plan_id == "plan-serialization"
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_timezone_boundary_readiness_plan_to_dicts(result) == payload["records"]
    assert task_timezone_boundary_readiness_plan_to_dicts(result.records) == payload["records"]
    assert analyze_task_timezone_boundary_readiness(plan).to_dict() == result.to_dict()
    assert derive_task_timezone_boundary_readiness(plan).to_dict() == result.to_dict()
    assert extract_task_timezone_boundary_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_timezone_boundary_readiness(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "timezone_task_ids",
        "suppressed_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "timezone_signals",
        "matched_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness",
        "impact_level",
        "risk_level",
        "evidence",
        "recommendations",
        "recommended_checks",
    ]
    assert markdown.startswith("# Task Timezone Boundary Readiness: plan-serialization")
    assert "Timezone conversion \\| scoped" in markdown


def _plan(tasks, plan_id="plan-timezone"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-timezone",
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
    acceptance_criteria=None,
    metadata=None,
    tags=None,
    validation_commands=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-timezone",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "medium",
        "estimated_hours": 2.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        payload["tags"] = tags
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    return payload
