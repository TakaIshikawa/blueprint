"""Tests for source scheduling requirements extractor."""

import pytest

from blueprint.source_scheduling_requirements import (
    SchedulingRequirements,
    extract_scheduling_requirements,
)


def test_empty_source_data_returns_all_false():
    """Empty source data should return all fields as False."""
    result = extract_scheduling_requirements({})

    assert isinstance(result, SchedulingRequirements)
    assert result.cron_expressions_defined is False
    assert result.time_windows_specified is False
    assert result.timezone_handling_planned is False
    assert result.recurrence_patterns_defined is False
    assert result.calendar_integration_planned is False
    assert result.dst_handling_addressed is False
    assert result.conflict_resolution_planned is False
    assert result.missed_execution_handled is False
    assert result.schedule_monitoring_enabled is False
    assert result.one_time_schedules_supported is False
    assert result.completeness_score == 0.0


def test_cron_expressions_detected():
    """Detect cron expressions in source data."""
    source = {
        "title": "Scheduled task system",
        "description": "Support cron expressions for scheduling jobs",
    }

    result = extract_scheduling_requirements(source)

    assert result.cron_expressions_defined is True
    assert result.completeness_score == 0.1


def test_time_windows_detected():
    """Detect time windows in source data."""
    source = {
        "description": "Execute jobs during maintenance windows between 2am and 6am",
        "requirements": ["Time windows configured", "Off-hours execution"],
    }

    result = extract_scheduling_requirements(source)

    assert result.time_windows_specified is True


def test_timezone_handling_detected():
    """Detect timezone handling in source data."""
    source = {
        "description": "Support multiple timezones with UTC time conversion",
        "requirements": ["Timezone awareness", "Handle timezone conversions"],
    }

    result = extract_scheduling_requirements(source)

    assert result.timezone_handling_planned is True


def test_recurrence_patterns_detected():
    """Detect recurrence patterns in source data."""
    source = {
        "description": "Support recurring schedules with daily and weekly execution",
        "requirements": ["Run every day", "Monthly schedule pattern"],
    }

    result = extract_scheduling_requirements(source)

    assert result.recurrence_patterns_defined is True


def test_calendar_integration_detected():
    """Detect calendar integration in source data."""
    source = {
        "description": "Integrate with business calendar to skip holidays and weekends",
        "requirements": ["Working days only", "Holiday calendar support"],
    }

    result = extract_scheduling_requirements(source)

    assert result.calendar_integration_planned is True


def test_dst_handling_detected():
    """Detect DST handling in source data."""
    source = {
        "description": "Handle daylight saving time transitions correctly",
        "requirements": ["DST awareness", "Account for time changes"],
    }

    result = extract_scheduling_requirements(source)

    assert result.dst_handling_addressed is True


def test_conflict_resolution_detected():
    """Detect conflict resolution in source data."""
    source = {
        "description": "Resolve scheduling conflicts and prevent overlapping executions",
        "requirements": ["Conflict detection", "Prevent concurrent runs"],
    }

    result = extract_scheduling_requirements(source)

    assert result.conflict_resolution_planned is True


def test_missed_execution_detected():
    """Detect missed execution handling in source data."""
    source = {
        "description": "Handle missed executions with catch-up logic and backfill",
        "requirements": ["Misfire handling", "Recover missed runs"],
    }

    result = extract_scheduling_requirements(source)

    assert result.missed_execution_handled is True


def test_schedule_monitoring_detected():
    """Detect schedule monitoring in source data."""
    source = {
        "description": "Monitor scheduled executions with execution history and metrics",
        "requirements": ["Track schedule adherence", "Alert on missed schedules"],
    }

    result = extract_scheduling_requirements(source)

    assert result.schedule_monitoring_enabled is True


def test_one_time_schedules_detected():
    """Detect one-time schedule support in source data."""
    source = {
        "description": "Support one-time execution and ad-hoc jobs",
        "requirements": ["Schedule once", "Non-recurring jobs"],
    }

    result = extract_scheduling_requirements(source)

    assert result.one_time_schedules_supported is True


def test_comprehensive_scheduling_all_detected():
    """Test comprehensive scheduling with all aspects present."""
    source = {
        "title": "Complete scheduling system",
        "description": (
            "Support cron expressions for flexible scheduling. "
            "Configure time windows for maintenance execution. "
            "Handle multiple timezones with UTC conversion. "
            "Define recurrence patterns for daily and weekly jobs. "
            "Integrate with business calendar to skip holidays. "
            "Account for daylight saving time transitions. "
            "Resolve scheduling conflicts and prevent overlaps. "
            "Handle missed executions with catch-up logic. "
            "Monitor schedule adherence with execution tracking. "
            "Support one-time ad-hoc job execution."
        ),
        "requirements": [
            "Cron expressions",
            "Time windows",
            "Timezone support",
            "Recurring schedules",
            "Calendar integration",
            "DST handling",
            "Conflict resolution",
            "Missed execution recovery",
            "Schedule monitoring",
            "One-time schedules",
        ],
    }

    result = extract_scheduling_requirements(source)

    assert result.cron_expressions_defined is True
    assert result.time_windows_specified is True
    assert result.timezone_handling_planned is True
    assert result.recurrence_patterns_defined is True
    assert result.calendar_integration_planned is True
    assert result.dst_handling_addressed is True
    assert result.conflict_resolution_planned is True
    assert result.missed_execution_handled is True
    assert result.schedule_monitoring_enabled is True
    assert result.one_time_schedules_supported is True
    assert result.completeness_score == 1.0


def test_invalid_source_data_none():
    """Test with None input."""
    result = extract_scheduling_requirements(None)  # type: ignore

    assert isinstance(result, SchedulingRequirements)
    assert result.completeness_score == 0.0


def test_invalid_source_data_list():
    """Test with list input instead of mapping."""
    result = extract_scheduling_requirements([{"key": "value"}])  # type: ignore

    assert isinstance(result, SchedulingRequirements)
    assert result.completeness_score == 0.0


def test_dataclass_immutability():
    """Test that SchedulingRequirements is frozen/immutable."""
    requirements = SchedulingRequirements(cron_expressions_defined=True)

    with pytest.raises(AttributeError):
        requirements.cron_expressions_defined = False  # type: ignore


def test_to_dict_method():
    """Test SchedulingRequirements.to_dict() serialization."""
    requirements = SchedulingRequirements(
        cron_expressions_defined=True,
        time_windows_specified=True,
        timezone_handling_planned=False,
        recurrence_patterns_defined=True,
        calendar_integration_planned=False,
        dst_handling_addressed=True,
        conflict_resolution_planned=False,
        missed_execution_handled=True,
        schedule_monitoring_enabled=False,
        one_time_schedules_supported=True,
    )

    result = requirements.to_dict()

    assert isinstance(result, dict)
    assert result["cron_expressions_defined"] is True
    assert result["time_windows_specified"] is True
    assert result["timezone_handling_planned"] is False
    assert result["recurrence_patterns_defined"] is True
    assert result["calendar_integration_planned"] is False
    assert result["dst_handling_addressed"] is True
    assert result["conflict_resolution_planned"] is False
    assert result["missed_execution_handled"] is True
    assert result["schedule_monitoring_enabled"] is False
    assert result["one_time_schedules_supported"] is True
    assert result["completeness_score"] == 0.6


def test_crontab_pattern():
    """Test crontab pattern detection."""
    source = {
        "description": "Use crontab for scheduling jobs",
    }

    result = extract_scheduling_requirements(source)

    assert result.cron_expressions_defined is True


def test_business_days_pattern():
    """Test business days pattern detection."""
    source = {
        "description": "Run only on business days",
    }

    result = extract_scheduling_requirements(source)

    assert result.calendar_integration_planned is True


def test_hourly_schedule_pattern():
    """Test hourly schedule pattern detection."""
    source = {
        "description": "Execute every hour",
    }

    result = extract_scheduling_requirements(source)

    assert result.recurrence_patterns_defined is True


def test_multi_timezone_edge_case():
    """Test multi-timezone scheduling detection."""
    source = {
        "description": "Support scheduling across multiple timezones with UTC handling",
        "requirements": [
            "Timezone conversion",
            "Handle EST and PST",
        ],
    }

    result = extract_scheduling_requirements(source)

    assert result.timezone_handling_planned is True


def test_business_calendars_edge_case():
    """Test business calendars detection."""
    source = {
        "description": "Skip non-working days using holiday calendar",
        "requirements": [
            "Exclude weekends",
            "Business hours only",
        ],
    }

    result = extract_scheduling_requirements(source)

    assert result.calendar_integration_planned is True


def test_one_time_schedules_edge_case():
    """Test one-time schedules detection."""
    source = {
        "description": "Allow scheduling jobs for single execution at specific time",
        "requirements": [
            "Ad-hoc jobs",
            "Run once at exact time",
        ],
    }

    result = extract_scheduling_requirements(source)

    assert result.one_time_schedules_supported is True


def test_partial_completeness():
    """Test partial completeness with some aspects covered."""
    source = {
        "title": "Basic scheduling",
        "description": "Support cron expressions",
        "requirements": [
            "Recurring schedules",
            "Monitor executions",
        ],
    }

    result = extract_scheduling_requirements(source)

    assert result.cron_expressions_defined is True
    assert result.recurrence_patterns_defined is True
    assert result.schedule_monitoring_enabled is True
    assert result.time_windows_specified is False
    assert result.timezone_handling_planned is False
    assert result.calendar_integration_planned is False
    assert result.dst_handling_addressed is False
    assert result.conflict_resolution_planned is False
    assert result.missed_execution_handled is False
    assert result.one_time_schedules_supported is False
    assert result.completeness_score == 0.3


def test_multiple_fields_in_different_sections():
    """Test detection across multiple source data sections."""
    source = {
        "title": "Scheduling system",
        "description": "Cron expressions for scheduling",
        "requirements": ["Timezone support"],
        "notes": ["Handle DST"],
        "features": ["Monitor schedule execution"],
    }

    result = extract_scheduling_requirements(source)

    assert result.cron_expressions_defined is True
    assert result.timezone_handling_planned is True
    assert result.dst_handling_addressed is True
    assert result.schedule_monitoring_enabled is True
