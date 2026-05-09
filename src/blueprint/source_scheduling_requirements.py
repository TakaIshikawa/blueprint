"""Extract scheduling requirements from source brief data."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for scheduling requirements concepts
_CRON_EXPRESSIONS_RE = re.compile(
    r"(?:cron[_\s]+(?:expression|pattern|syntax|schedule)|"
    r"(?:0|[\*])[_\s]+[\*0-9]+[_\s]+[\*0-9]+|"  # Simplified cron pattern detection
    r"crontab|cronspec|"
    r"schedule[_\s]+(?:using|with|via)[_\s]+cron)",
    re.I,
)
_TIME_WINDOWS_RE = re.compile(
    r"(?:time[_\s]+windows?|scheduling[_\s]+windows?|"
    r"(?:execution|run)[_\s]+windows?|"
    r"(?:between|from)[_\s]+\d+(?:am|pm|:)|"  # e.g., "between 2am and 6am"
    r"maintenance[_\s]+windows?|"
    r"allowed[_\s]+(?:time|execution)[_\s]+(?:window|period)|"
    r"off[_\s-]*hours?[_\s]+(?:execution|window))",
    re.I,
)
_TIMEZONE_HANDLING_RE = re.compile(
    r"(?:timezone[_\s]+(?:handling|support|awareness|configuration)|"
    r"time[_\s]*zone(?:s)?|tz[_\s]+(?:handling|support)|"
    r"(?:utc|gmt|est|pst|cet)[_\s]+time|"
    r"(?:handle|support|manage)[_\s]+(?:multiple[_\s]+)?timezones?|"
    r"timezone[_\s-]*aware|"
    r"convert[_\s]+(?:to|between)[_\s]+timezones?)",
    re.I,
)
_RECURRENCE_PATTERNS_RE = re.compile(
    r"(?:recurrence[_\s]+(?:pattern|rule)|recurring[_\s]+(?:schedule|jobs?|tasks?)|"
    r"(?:daily|weekly|monthly|yearly|hourly)[_\s]+(?:schedule|execution|run)|"
    r"(?:run|execute)[_\s]+(?:every|each)[_\s]+(?:day|week|month|hour)|"
    r"repeat(?:ing)?[_\s]+(?:schedule|pattern)|"
    r"periodic[_\s]+(?:execution|schedule)|"
    r"frequency|interval[_\s]+(?:schedule|pattern))",
    re.I,
)
_CALENDAR_INTEGRATION_RE = re.compile(
    r"(?:calendar[_\s]+(?:integration|support|sync)|"
    r"(?:business|working)[_\s]+(?:days?|calendar)|"
    r"(?:skip|exclude)[_\s]+(?:holidays?|weekends?)|"
    r"holiday[_\s]+(?:calendar|handling|awareness)|"
    r"(?:integrate|sync)[_\s]+with[_\s]+calendar|"
    r"non[_\s-]*working[_\s]+days?|"
    r"business[_\s]+hours?)",
    re.I,
)
_DST_HANDLING_RE = re.compile(
    r"(?:daylight[_\s]+saving(?:s)?[_\s]+time|dst[_\s]+(?:handling|transition|awareness)|"
    r"(?:handle|manage|account[_\s]+for)[_\s]+dst|"
    r"time[_\s]+change|clock[_\s]+(?:change|adjustment)|"
    r"dst[_\s-]*aware|"
    r"summer[_\s]+time|winter[_\s]+time)",
    re.I,
)
_CONFLICT_RESOLUTION_RE = re.compile(
    r"(?:conflict[_\s]+(?:resolution|handling|detection)|"
    r"(?:resolve|handle|prevent)[_\s]+(?:scheduling[_\s]+)?conflicts?|"
    r"overlapping[_\s]+(?:schedules?|executions?)|"
    r"schedule[_\s]+collision|"
    r"concurrent[_\s]+execution[_\s]+(?:prevention|handling)|"
    r"prevent[_\s]+(?:duplicate|concurrent)[_\s]+runs?)",
    re.I,
)
_MISSED_EXECUTION_RE = re.compile(
    r"(?:missed[_\s]+(?:execution|run|job|schedule)|"
    r"(?:handle|catch[_\s]+up|recover)[_\s]+missed[_\s]+(?:runs?|executions?|jobs?)|"
    r"skip(?:ped)?[_\s]+(?:execution|run|job)|"
    r"late[_\s]+execution|"
    r"catch[_\s-]*up[_\s]+(?:logic|handling)|"
    r"backfill[_\s]+(?:missed|skipped)[_\s]+(?:runs?|jobs?)|"
    r"misfire[_\s]+(?:handling|policy))",
    re.I,
)
_SCHEDULE_MONITORING_RE = re.compile(
    r"(?:schedule[_\s]+(?:monitoring|tracking|observability|execution)|"
    r"(?:monitor|track|log)[_\s]+(?:scheduled?[_\s]+)?(?:executions?|runs?|schedule)|"
    r"execution[_\s]+(?:history|log|tracking)|"
    r"schedule[_\s]+(?:metrics|analytics|reporting)|"
    r"(?:track|monitor)[_\s]+schedule[_\s]+(?:adherence|compliance|execution)|"
    r"alert[_\s]+on[_\s]+(?:missed|failed)[_\s]+(?:schedule|execution))",
    re.I,
)
_ONE_TIME_SCHEDULES_RE = re.compile(
    r"(?:one[_\s-]*time[_\s]+(?:schedule|execution|run|job)|"
    r"(?:single|once)[_\s]+(?:execution|run)|"
    r"(?:schedule|run)[_\s]+(?:once|at)[_\s]+(?:specific|exact)[_\s]+time|"
    r"ad[_\s-]*hoc[_\s]+(?:schedule|execution|job)|"
    r"non[_\s-]*recurring[_\s]+(?:schedule|job))",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SchedulingRequirements:
    """Scheduling requirements extracted from source brief."""

    cron_expressions_defined: bool = False
    time_windows_specified: bool = False
    timezone_handling_planned: bool = False
    recurrence_patterns_defined: bool = False
    calendar_integration_planned: bool = False
    dst_handling_addressed: bool = False
    conflict_resolution_planned: bool = False
    missed_execution_handled: bool = False
    schedule_monitoring_enabled: bool = False
    one_time_schedules_supported: bool = False

    @property
    def completeness_score(self) -> float:
        """Calculate completeness score (0.0 to 1.0)."""
        total_checks = 10
        passed_checks = sum([
            self.cron_expressions_defined,
            self.time_windows_specified,
            self.timezone_handling_planned,
            self.recurrence_patterns_defined,
            self.calendar_integration_planned,
            self.dst_handling_addressed,
            self.conflict_resolution_planned,
            self.missed_execution_handled,
            self.schedule_monitoring_enabled,
            self.one_time_schedules_supported,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "cron_expressions_defined": self.cron_expressions_defined,
            "time_windows_specified": self.time_windows_specified,
            "timezone_handling_planned": self.timezone_handling_planned,
            "recurrence_patterns_defined": self.recurrence_patterns_defined,
            "calendar_integration_planned": self.calendar_integration_planned,
            "dst_handling_addressed": self.dst_handling_addressed,
            "conflict_resolution_planned": self.conflict_resolution_planned,
            "missed_execution_handled": self.missed_execution_handled,
            "schedule_monitoring_enabled": self.schedule_monitoring_enabled,
            "one_time_schedules_supported": self.one_time_schedules_supported,
            "completeness_score": self.completeness_score,
        }


def extract_scheduling_requirements(source_data: Mapping[str, Any]) -> SchedulingRequirements:
    """
    Extract scheduling requirements from source brief data.

    Args:
        source_data: A mapping containing source brief information with fields like
                    'title', 'description', 'requirements', etc.

    Returns:
        SchedulingRequirements with boolean flags for each aspect and overall score.
    """
    if not isinstance(source_data, Mapping):
        return SchedulingRequirements()

    searchable_text = _extract_searchable_text(source_data)

    return SchedulingRequirements(
        cron_expressions_defined=bool(_CRON_EXPRESSIONS_RE.search(searchable_text)),
        time_windows_specified=bool(_TIME_WINDOWS_RE.search(searchable_text)),
        timezone_handling_planned=bool(_TIMEZONE_HANDLING_RE.search(searchable_text)),
        recurrence_patterns_defined=bool(_RECURRENCE_PATTERNS_RE.search(searchable_text)),
        calendar_integration_planned=bool(_CALENDAR_INTEGRATION_RE.search(searchable_text)),
        dst_handling_addressed=bool(_DST_HANDLING_RE.search(searchable_text)),
        conflict_resolution_planned=bool(_CONFLICT_RESOLUTION_RE.search(searchable_text)),
        missed_execution_handled=bool(_MISSED_EXECUTION_RE.search(searchable_text)),
        schedule_monitoring_enabled=bool(_SCHEDULE_MONITORING_RE.search(searchable_text)),
        one_time_schedules_supported=bool(_ONE_TIME_SCHEDULES_RE.search(searchable_text)),
    )


def _extract_searchable_text(source_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the source data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale", "context"):
        value = source_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("requirements", "acceptance_criteria", "notes", "features", "specifications"):
        value = source_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "SchedulingRequirements",
    "extract_scheduling_requirements",
]
