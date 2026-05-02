import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_time_zone_impact import (
    TaskTimeZoneImpactPlan,
    TaskTimeZoneImpactRecord,
    analyze_task_time_zone_impact,
    build_task_time_zone_impact_plan,
    extract_task_time_zone_impact,
    generate_task_time_zone_impact,
    recommend_task_time_zone_impact,
    summarize_task_time_zone_impact,
    task_time_zone_impact_plan_to_dict,
    task_time_zone_impact_plan_to_dicts,
    task_time_zone_impact_plan_to_markdown,
)


def test_detects_time_zone_signals_categories_and_missing_safeguards_from_dict_input():
    result = build_task_time_zone_impact_plan(
        _plan(
            [
                _task(
                    "task-schedule",
                    title="Schedule timezone-aware reminders",
                    description=(
                        "Recurring schedules must run in each user's timezone, handle DST, "
                        "respect midnight date boundaries, and display user local time."
                    ),
                    files_or_modules=[
                        "src/scheduling/timezone_reminders.py",
                        "src/calendar/dst_boundaries.py",
                    ],
                    acceptance_criteria=["Launch window reminders do not shift across daylight-saving transitions."],
                )
            ]
        )
    )

    assert isinstance(result, TaskTimeZoneImpactPlan)
    assert result.time_zone_task_ids == ("task-schedule",)
    record = result.records[0]
    assert isinstance(record, TaskTimeZoneImpactRecord)
    assert {
        "time_zone_handling",
        "dst_boundary",
        "recurring_schedule",
        "date_boundary",
        "local_time_display",
        "date_semantics",
    } <= set(record.impact_categories)
    assert record.required_safeguards == (
        "utc_persistence",
        "user_timezone_conversion",
        "dst_transition_validation",
        "recurrence_boundary_validation",
        "date_only_timestamp_semantics",
    )
    assert "utc_persistence" in record.missing_safeguards
    assert "Verify timestamps are persisted in UTC" in record.recommended_validation_checks[0]
    assert any("DST transition" in check or "daylight-saving transition" in check for check in record.recommended_validation_checks)
    assert any("UTC" in check for check in record.recommended_validation_checks)
    assert any("user time zone conversion" in check for check in record.recommended_validation_checks)
    assert any("recurring schedules" in check for check in record.recommended_validation_checks)
    assert record.risk_level == "high"
    assert any("description:" in item and "Recurring schedules" in item for item in record.evidence)
    assert "files_or_modules: src/scheduling/timezone_reminders.py" in record.evidence
    assert result.summary["time_zone_task_count"] == 1
    assert result.summary["category_counts"]["dst_boundary"] == 1


def test_metadata_and_acceptance_criteria_detect_present_safeguards():
    result = analyze_task_time_zone_impact(
        _plan(
            [
                _task(
                    "task-clock",
                    title="Validate local time display",
                    description="Display time in the viewer local timezone and account for browser clock skew.",
                    metadata={
                        "time_zone": {
                            "storage": "Persist database timestamps in UTC.",
                            "conversion": "Convert API output to the user's timezone before display.",
                            "clock": "Document server clock and client clock assumptions with NTP drift.",
                        }
                    },
                    acceptance_criteria=[
                        "DST transition validation covers spring forward and fall back.",
                        "Date-only semantics are separate from timestamp semantics.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert {"time_zone_handling", "local_time_display", "clock_assumption", "date_semantics"} <= set(
        record.impact_categories
    )
    assert record.present_safeguards == (
        "utc_persistence",
        "user_timezone_conversion",
        "dst_transition_validation",
        "server_client_clock_assumption",
        "date_only_timestamp_semantics",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "low"
    assert any("metadata.time_zone.storage" in item for item in record.evidence)
    assert any("metadata.time_zone.clock" in item for item in record.evidence)


def test_execution_plan_execution_task_single_task_and_empty_input():
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Recurring billing calendar",
            description="Monthly recurring schedule must validate recurrence boundary behavior.",
        )
    )
    single_task = build_task_time_zone_impact_plan(model_task)
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-locale",
                    title="Locale calendar | holidays",
                    description="Use the account locale calendar, business days, and first day of week.",
                ),
                _task("task-copy", title="Update copy", description="Adjust empty state wording."),
            ]
        )
    )
    plan_result = generate_task_time_zone_impact(plan)
    empty = build_task_time_zone_impact_plan([])
    noop = build_task_time_zone_impact_plan(_plan([_task("task-copy", title="Update copy", description="Static text.")]))

    assert single_task.plan_id is None
    assert single_task.time_zone_task_ids == ("task-model",)
    assert plan_result.plan_id == "plan-timezone"
    assert plan_result.time_zone_task_ids == ("task-locale",)
    assert plan_result.ignored_task_ids == ("task-copy",)
    assert empty.records == ()
    assert empty.ignored_task_ids == ()
    assert noop.records == ()
    assert noop.time_zone_task_ids == ()
    assert noop.ignored_task_ids == ("task-copy",)
    assert noop.summary == {
        "task_count": 1,
        "time_zone_task_count": 0,
        "ignored_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "category_counts": {},
        "present_safeguard_counts": {
            "utc_persistence": 0,
            "user_timezone_conversion": 0,
            "dst_transition_validation": 0,
            "server_client_clock_assumption": 0,
            "recurrence_boundary_validation": 0,
            "date_only_timestamp_semantics": 0,
        },
        "time_zone_task_ids": [],
    }
    assert "No task time-zone impact recommendations" in noop.to_markdown()
    assert "Ignored tasks: task-copy" in noop.to_markdown()


def test_serialization_markdown_aliases_order_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Recurring launch window | timezone",
                description="Recurring launch window crosses month end and DST date boundaries.",
            ),
            _task(
                "task-a",
                title="Timezone display conversion",
                description="Display local time using user timezone conversion and UTC storage.",
            ),
            _task("task-copy", title="Copy update", description="Update helper text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_time_zone_impact(plan)
    payload = task_time_zone_impact_plan_to_dict(result)
    markdown = task_time_zone_impact_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_time_zone_impact_plan_to_dicts(result) == payload["records"]
    assert task_time_zone_impact_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_time_zone_impact(plan).to_dict() == result.to_dict()
    assert recommend_task_time_zone_impact(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "recommendations",
        "time_zone_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "matched_time_signals",
        "impact_categories",
        "required_safeguards",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "recommended_validation_checks",
        "evidence",
    ]
    assert result.time_zone_task_ids == ("task-z", "task-a")
    assert result.ignored_task_ids == ("task-copy",)
    assert markdown.startswith("# Task Time Zone Impact Plan: plan-timezone")
    assert "Recurring launch window \\| timezone" in markdown
    assert "| Task | Title | Risk | Signals | Impact Categories | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |" in markdown


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
    risks=None,
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
    if risks is not None:
        payload["risks"] = risks
    return payload
