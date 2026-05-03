"""Assess task readiness for timezone and date boundary work."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


TimezoneBoundarySignal = Literal[
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
]
TimezoneBoundarySafeguard = Literal[
    "dst_transition_tests",
    "inclusive_exclusive_boundaries",
    "utc_persistence",
    "user_timezone_preference",
    "server_client_timezone_mismatch",
    "fixed_clock_tests",
]
TimezoneBoundaryReadiness = Literal["weak", "moderate", "strong"]
TimezoneBoundaryImpact = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[TimezoneBoundarySignal, ...] = (
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
)
_SAFEGUARD_ORDER: tuple[TimezoneBoundarySafeguard, ...] = (
    "dst_transition_tests",
    "inclusive_exclusive_boundaries",
    "utc_persistence",
    "user_timezone_preference",
    "server_client_timezone_mismatch",
    "fixed_clock_tests",
)
_READINESS_ORDER: dict[TimezoneBoundaryReadiness, int] = {"weak": 0, "moderate": 1, "strong": 2}
_IMPACT_ORDER: dict[TimezoneBoundaryImpact, int] = {"high": 0, "medium": 1, "low": 2}
_CORE_SAFEGUARDS = frozenset(
    {
        "inclusive_exclusive_boundaries",
        "utc_persistence",
        "user_timezone_preference",
        "fixed_clock_tests",
    }
)
_BOUNDARY_SIGNALS = frozenset(
    {
        "timezone_conversion",
        "daylight_saving_time",
        "date_range",
        "billing_period",
        "end_of_day_boundary",
        "recurring_job",
    }
)

_SIGNAL_PATTERNS: dict[TimezoneBoundarySignal, re.Pattern[str]] = {
    "timezone_conversion": re.compile(
        r"\b(?:time ?zone conversion|convert(?:ing)? time ?zones?|tz conversion|utc offset|offset conversion|"
        r"iana time ?zone|time ?zone aware|timezone aware)\b",
        re.I,
    ),
    "utc_storage": re.compile(
        r"\b(?:utc storage|store(?:d|s)? in utc|persist(?:ed|s)? in utc|utc timestamp|utc datetime|"
        r"canonical utc|server utc)\b",
        re.I,
    ),
    "local_display": re.compile(
        r"\b(?:local display|display(?:ed)? in (?:the )?user'?s time ?zone|local time|localized time|"
        r"user local time|render(?:ed)? in local)\b",
        re.I,
    ),
    "daylight_saving_time": re.compile(
        r"\b(?:daylight saving|daylight savings|dst|spring forward|fall back|ambiguous time|nonexistent time|"
        r"clock change)\b",
        re.I,
    ),
    "date_range": re.compile(
        r"\b(?:date ranges?|time ranges?|range filter|from/to dates?|start date|end date|between dates?|"
        r"reporting window|window boundaries?)\b",
        re.I,
    ),
    "billing_period": re.compile(
        r"\b(?:billing periods?|billing cycle|invoice period|subscription period|proration|monthly period|"
        r"renewal date|metering window|usage period)\b",
        re.I,
    ),
    "scheduling": re.compile(
        r"\b(?:scheduling|schedule(?:d)? (?:at|for)|appointment(?:s)?|booking(?:s)?|reservation(?:s)?|"
        r"reminder(?:s)?|send time|run time)\b",
        re.I,
    ),
    "calendar_event": re.compile(
        r"\b(?:calendar events?|calendar invite|ics|ical|icalendar|google calendar|outlook calendar|"
        r"event start|event end)\b",
        re.I,
    ),
    "end_of_day_boundary": re.compile(
        r"\b(?:end of day|eod|start of day|sod|midnight|23:59|00:00|day boundary|daily boundary|"
        r"close of business)\b",
        re.I,
    ),
    "recurring_job": re.compile(
        r"\b(?:recurring jobs?|recurring schedule|recurrence|rrule|cron|scheduled worker|nightly|daily job|"
        r"weekly job|monthly job|runs? every)\b",
        re.I,
    ),
    "locale_date_format": re.compile(
        r"\b(?:locale date|localized date|date formatting|date format|mm/dd/yyyy|dd/mm/yyyy|yyyy-mm-dd|"
        r"regional date|international date|locale-specific date)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[TimezoneBoundarySignal, re.Pattern[str]] = {
    "timezone_conversion": re.compile(r"timezone|time_zone|tz|utc_offset|iana", re.I),
    "utc_storage": re.compile(r"utc|timestamp|datetime", re.I),
    "local_display": re.compile(r"local(?:ized)?[_-]?(?:time|date)|user[_-]?timezone", re.I),
    "daylight_saving_time": re.compile(r"dst|daylight|clock[_-]?change", re.I),
    "date_range": re.compile(r"date[_-]?range|time[_-]?range|range[_-]?filter|reporting[_-]?window", re.I),
    "billing_period": re.compile(r"billing|invoice|subscription|proration|metering|usage[_-]?period", re.I),
    "scheduling": re.compile(r"schedul|appointment|booking|reservation|reminder", re.I),
    "calendar_event": re.compile(r"calendar|ical|ics|event", re.I),
    "end_of_day_boundary": re.compile(r"end[_-]?of[_-]?day|start[_-]?of[_-]?day|midnight|day[_-]?boundary", re.I),
    "recurring_job": re.compile(r"recurr|rrule|cron|nightly|daily|weekly|monthly", re.I),
    "locale_date_format": re.compile(r"locale|localized|date[_-]?format|i18n", re.I),
}
_SAFEGUARD_PATTERNS: dict[TimezoneBoundarySafeguard, re.Pattern[str]] = {
    "dst_transition_tests": re.compile(
        r"\b(?:dst|daylight saving|spring forward|fall back|ambiguous time|nonexistent time|clock change)"
        r".{0,100}\b(?:test|tests|tested|case|fixture|coverage)\b|"
        r"\b(?:test|tests|tested|case|fixture|coverage).{0,100}\b(?:dst|daylight saving|spring forward|fall back|"
        r"ambiguous time|nonexistent time|clock change)\b",
        re.I,
    ),
    "inclusive_exclusive_boundaries": re.compile(
        r"\b(?:inclusive/exclusive|inclusive and exclusive|exclusive end|inclusive start|half-open|closed-open|"
        r"start inclusive|end exclusive|boundary semantics|range boundaries|from inclusive|to exclusive)\b",
        re.I,
    ),
    "utc_persistence": re.compile(
        r"\b(?:persist(?:ed|s)? in utc|store(?:d|s)? in utc|utc persistence|canonical utc|utc in (?:the )?database|"
        r"database stores utc|normalize(?:d)? to utc|save(?:d|s)? as utc|"
        r"store(?:s|d)? canonical timestamps? in utc|canonical timestamps? (?:are )?(?:store(?:d|s)|persist(?:ed|s)) in utc)\b",
        re.I,
    ),
    "user_timezone_preference": re.compile(
        r"\b(?:user time ?zone preference|user'?s time ?zone|account time ?zone|profile time ?zone|tenant time ?zone|"
        r"time ?zone setting|preferred time ?zone)\b",
        re.I,
    ),
    "server_client_timezone_mismatch": re.compile(
        r"\b(?:server/client time ?zone|client/server time ?zone|browser time ?zone|server time ?zone|"
        r"server local time|client local time|timezone mismatch|time ?zone mismatch|api time ?zone contract)\b",
        re.I,
    ),
    "fixed_clock_tests": re.compile(
        r"\b(?:fixed clock|freeze(?:d)? time|time freezer|fake clock|deterministic clock|clock fixture|"
        r"mock(?:ed)? clock|fixed now|freezegun|timecop)\b",
        re.I,
    ),
}
_SAFEGUARD_RECOMMENDATIONS: dict[TimezoneBoundarySafeguard, str] = {
    "dst_transition_tests": "Add DST transition cases for spring-forward, fall-back, ambiguous, and nonexistent local times.",
    "inclusive_exclusive_boundaries": "Define inclusive/exclusive semantics for date ranges, end dates, and end-of-day cutoffs.",
    "utc_persistence": "Persist canonical timestamps in UTC and convert only at system boundaries.",
    "user_timezone_preference": "Specify how the user, account, or tenant timezone preference is selected and stored.",
    "server_client_timezone_mismatch": "Check server, API, browser, and client timezone assumptions for mismatches.",
    "fixed_clock_tests": "Use deterministic tests with fixed clocks instead of relying on wall-clock time.",
}
_COPY_ONLY_RE = re.compile(r"\b(?:copy|label|labels|helper text|microcopy|tooltip|wording|text only|translations?)\b", re.I)
_VAGUE_DATE_RE = re.compile(r"\b(?:date|dates|time|times|timestamp|timestamps)\b", re.I)
_DOC_PATH_RE = re.compile(r"(?:^|/)(?:docs?|adr|design|rfcs?)(?:/|$)|\.(?:md|mdx|rst|txt)$", re.I)
_DOC_TASK_RE = re.compile(r"\b(?:docs?|documentation|readme|adr|design doc|runbook copy|changelog)\b", re.I)


@dataclass(frozen=True, slots=True)
class TaskTimezoneBoundaryReadinessRecord:
    """Readiness guidance for one timezone or date-boundary task."""

    task_id: str
    title: str
    timezone_signals: tuple[TimezoneBoundarySignal, ...]
    present_safeguards: tuple[TimezoneBoundarySafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[TimezoneBoundarySafeguard, ...] = field(default_factory=tuple)
    readiness: TimezoneBoundaryReadiness = "moderate"
    impact_level: TimezoneBoundaryImpact = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommendations: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[TimezoneBoundarySignal, ...]:
        """Compatibility view for callers that expect matched signals."""
        return self.timezone_signals

    @property
    def risk_level(self) -> TimezoneBoundaryImpact:
        """Compatibility view for callers that group by risk."""
        return self.impact_level

    @property
    def recommended_checks(self) -> tuple[str, ...]:
        """Compatibility view matching check-oriented planners."""
        return self.recommendations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "timezone_signals": list(self.timezone_signals),
            "matched_signals": list(self.matched_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "impact_level": self.impact_level,
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommendations": list(self.recommendations),
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskTimezoneBoundaryReadinessPlan:
    """Plan-level timezone and date-boundary readiness review."""

    plan_id: str | None = None
    records: tuple[TaskTimezoneBoundaryReadinessRecord, ...] = field(default_factory=tuple)
    timezone_task_ids: tuple[str, ...] = field(default_factory=tuple)
    suppressed_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskTimezoneBoundaryReadinessRecord, ...]:
        """Compatibility view matching analyzers that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskTimezoneBoundaryReadinessRecord, ...]:
        """Compatibility view matching analyzers that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "timezone_task_ids": list(self.timezone_task_ids),
            "suppressed_task_ids": list(self.suppressed_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return timezone readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render timezone boundary readiness guidance as deterministic Markdown."""
        title = "# Task Timezone Boundary Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        impact_counts = self.summary.get("impact_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Timezone task count: {self.summary.get('timezone_task_count', 0)}",
            f"- Suppressed task count: {self.summary.get('suppressed_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{readiness} {readiness_counts.get(readiness, 0)}" for readiness in _READINESS_ORDER),
            "- Impact counts: "
            + ", ".join(f"{impact} {impact_counts.get(impact, 0)}" for impact in _IMPACT_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No timezone boundary readiness records were inferred."])
            if self.suppressed_task_ids:
                lines.extend(["", f"Suppressed tasks: {_markdown_cell(', '.join(self.suppressed_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Task | Title | Readiness | Impact | Signals | Present Safeguards | "
                    "Missing Safeguards | Recommendations |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{record.impact_level} | "
                f"{_markdown_cell(', '.join(record.timezone_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommendations) or 'none')} |"
            )
        if self.suppressed_task_ids:
            lines.extend(["", f"Suppressed tasks: {_markdown_cell(', '.join(self.suppressed_task_ids))}"])
        return "\n".join(lines)


def build_task_timezone_boundary_readiness_plan(source: Any) -> TaskTimezoneBoundaryReadinessPlan:
    """Build timezone and date-boundary readiness records for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _IMPACT_ORDER[record.impact_level],
                _READINESS_ORDER[record.readiness],
                len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    record_ids = {record.task_id for record in records}
    suppressed_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in record_ids
    )
    return TaskTimezoneBoundaryReadinessPlan(
        plan_id=plan_id,
        records=records,
        timezone_task_ids=tuple(record.task_id for record in records),
        suppressed_task_ids=suppressed_task_ids,
        summary=_summary(records, task_count=len(tasks), suppressed_task_ids=suppressed_task_ids),
    )


def analyze_task_timezone_boundary_readiness(source: Any) -> TaskTimezoneBoundaryReadinessPlan:
    """Compatibility alias for building timezone boundary readiness plans."""
    return build_task_timezone_boundary_readiness_plan(source)


def derive_task_timezone_boundary_readiness(source: Any) -> TaskTimezoneBoundaryReadinessPlan:
    """Compatibility alias for deriving timezone boundary readiness plans."""
    return build_task_timezone_boundary_readiness_plan(source)


def extract_task_timezone_boundary_readiness(source: Any) -> TaskTimezoneBoundaryReadinessPlan:
    """Compatibility alias for extracting timezone boundary readiness plans."""
    return derive_task_timezone_boundary_readiness(source)


def generate_task_timezone_boundary_readiness(source: Any) -> TaskTimezoneBoundaryReadinessPlan:
    """Compatibility alias for generating timezone boundary readiness plans."""
    return build_task_timezone_boundary_readiness_plan(source)


def recommend_task_timezone_boundary_readiness(source: Any) -> TaskTimezoneBoundaryReadinessPlan:
    """Compatibility alias for recommending timezone boundary safeguards."""
    return build_task_timezone_boundary_readiness_plan(source)


def summarize_task_timezone_boundary_readiness(source: Any) -> TaskTimezoneBoundaryReadinessPlan:
    """Compatibility alias for summarizing timezone boundary readiness plans."""
    return build_task_timezone_boundary_readiness_plan(source)


def task_timezone_boundary_readiness_plan_to_dict(
    result: TaskTimezoneBoundaryReadinessPlan,
) -> dict[str, Any]:
    """Serialize a timezone boundary readiness plan to a plain dictionary."""
    return result.to_dict()


task_timezone_boundary_readiness_plan_to_dict.__test__ = False


def task_timezone_boundary_readiness_plan_to_dicts(
    result: TaskTimezoneBoundaryReadinessPlan | Iterable[TaskTimezoneBoundaryReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize timezone boundary readiness records to plain dictionaries."""
    if isinstance(result, TaskTimezoneBoundaryReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_timezone_boundary_readiness_plan_to_dicts.__test__ = False


def task_timezone_boundary_readiness_plan_to_markdown(
    result: TaskTimezoneBoundaryReadinessPlan,
) -> str:
    """Render a timezone boundary readiness plan as Markdown."""
    return result.to_markdown()


task_timezone_boundary_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[TimezoneBoundarySignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    safeguards: tuple[TimezoneBoundarySafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskTimezoneBoundaryReadinessRecord | None:
    if _is_documentation_only(task):
        return None
    signals = _signals(task)
    if not signals.signals or _is_vague_copy_date_change(task, signals.signals):
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.safeguards)
    task_id = _task_id(task, index)
    return TaskTimezoneBoundaryReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        timezone_signals=signals.signals,
        present_safeguards=signals.safeguards,
        missing_safeguards=missing,
        readiness=_readiness(signals.safeguards, missing),
        impact_level=_impact(signals.signals, signals.safeguards),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
        recommendations=tuple(_SAFEGUARD_RECOMMENDATIONS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[TimezoneBoundarySignal] = set()
    safeguard_hits: set[TimezoneBoundarySafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_signal = False
        matched_safeguard = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or _SIGNAL_PATTERNS[signal].search(searchable):
                signal_hits.add(signal)
                matched_signal = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if matched_signal:
            signal_evidence.append(f"files_or_modules: {path}")
        if matched_safeguard:
            safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        snippet = _evidence_snippet(source_field, text)
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_signal = False
        matched_safeguard = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched_signal = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if matched_signal:
            signal_evidence.append(snippet)
        if matched_safeguard:
            safeguard_evidence.append(snippet)

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _readiness(
    present: tuple[TimezoneBoundarySafeguard, ...],
    missing: tuple[TimezoneBoundarySafeguard, ...],
) -> TimezoneBoundaryReadiness:
    if not missing:
        return "strong"
    present_set = set(present)
    if _CORE_SAFEGUARDS <= present_set or len(present_set) >= 4:
        return "moderate"
    return "weak"


def _impact(
    signals: tuple[TimezoneBoundarySignal, ...],
    present: tuple[TimezoneBoundarySafeguard, ...],
) -> TimezoneBoundaryImpact:
    signal_set = set(signals)
    if "billing_period" in signal_set or "daylight_saving_time" in signal_set:
        return "high"
    if "timezone_conversion" in signal_set and signal_set & {"scheduling", "calendar_event", "recurring_job", "date_range"}:
        return "high"
    if "end_of_day_boundary" in signal_set and signal_set & {"date_range", "billing_period", "recurring_job"}:
        return "high"
    if signal_set & _BOUNDARY_SIGNALS:
        return "medium"
    if present:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskTimezoneBoundaryReadinessRecord, ...],
    *,
    task_count: int,
    suppressed_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "timezone_task_count": len(records),
        "timezone_task_ids": [record.task_id for record in records],
        "suppressed_task_count": len(suppressed_task_ids),
        "suppressed_task_ids": list(suppressed_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "impact_counts": {
            impact: sum(1 for record in records if record.impact_level == impact)
            for impact in _IMPACT_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.timezone_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


def _is_documentation_only(task: Mapping[str, Any]) -> bool:
    paths = _strings(task.get("files_or_modules") or task.get("files"))
    has_doc_path = bool(paths) and all(_DOC_PATH_RE.search(_normalized_path(path)) for path in paths)
    task_text = " ".join(_strings([task.get("title"), task.get("description"), task.get("tags"), task.get("labels")]))
    return has_doc_path and _DOC_TASK_RE.search(task_text) is not None


def _is_vague_copy_date_change(
    task: Mapping[str, Any],
    signals: tuple[TimezoneBoundarySignal, ...],
) -> bool:
    context = " ".join(text for _, text in _candidate_texts(task))
    if not (_COPY_ONLY_RE.search(context) and _VAGUE_DATE_RE.search(context)):
        return False
    if "locale_date_format" in signals:
        return False
    return not bool(set(signals) & _BOUNDARY_SIGNALS)


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
        "definition_of_done",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "definition_of_done",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
        "test_commands",
        "validation_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _metadata_key_is_signal(value: str) -> bool:
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value))
            else:
                commands.extend(_strings(metadata_value))
    return tuple(("validation_commands", command) for command in _dedupe(commands))


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "TaskTimezoneBoundaryReadinessPlan",
    "TaskTimezoneBoundaryReadinessRecord",
    "TimezoneBoundaryImpact",
    "TimezoneBoundaryReadiness",
    "TimezoneBoundarySafeguard",
    "TimezoneBoundarySignal",
    "analyze_task_timezone_boundary_readiness",
    "build_task_timezone_boundary_readiness_plan",
    "derive_task_timezone_boundary_readiness",
    "extract_task_timezone_boundary_readiness",
    "generate_task_timezone_boundary_readiness",
    "recommend_task_timezone_boundary_readiness",
    "summarize_task_timezone_boundary_readiness",
    "task_timezone_boundary_readiness_plan_to_dict",
    "task_timezone_boundary_readiness_plan_to_dicts",
    "task_timezone_boundary_readiness_plan_to_markdown",
]
