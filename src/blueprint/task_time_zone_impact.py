"""Plan time-zone handling safeguards for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


TimeZoneImpactCategory = Literal[
    "time_zone_handling",
    "dst_boundary",
    "recurring_schedule",
    "date_boundary",
    "local_time_display",
    "locale_calendar",
    "clock_assumption",
    "date_semantics",
]
TimeZoneSafeguard = Literal[
    "utc_persistence",
    "user_timezone_conversion",
    "dst_transition_validation",
    "server_client_clock_assumption",
    "recurrence_boundary_validation",
    "date_only_timestamp_semantics",
]
TimeZoneImpactRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[TimeZoneImpactRisk, int] = {"high": 0, "medium": 1, "low": 2}
_CATEGORY_ORDER: tuple[TimeZoneImpactCategory, ...] = (
    "time_zone_handling",
    "dst_boundary",
    "recurring_schedule",
    "date_boundary",
    "local_time_display",
    "locale_calendar",
    "clock_assumption",
    "date_semantics",
)
_SAFEGUARD_ORDER: tuple[TimeZoneSafeguard, ...] = (
    "utc_persistence",
    "user_timezone_conversion",
    "dst_transition_validation",
    "server_client_clock_assumption",
    "recurrence_boundary_validation",
    "date_only_timestamp_semantics",
)
_HIGH_RISK_CATEGORIES = {"dst_boundary", "recurring_schedule", "date_boundary"}

_TEXT_CATEGORY_PATTERNS: dict[TimeZoneImpactCategory, re.Pattern[str]] = {
    "time_zone_handling": re.compile(
        r"\b(?:time\s*zones?|timezone|tz|iana zone|iana timezone|utc offset|offsets?|"
        r"zoned datetime|zoneinfo|pytz)\b",
        re.I,
    ),
    "dst_boundary": re.compile(
        r"\b(?:dst|daylight[- ]saving|daylight saving|spring forward|fall back|"
        r"ambiguous time|nonexistent time)\b",
        re.I,
    ),
    "recurring_schedule": re.compile(
        r"\b(?:recurr(?:ing|ence|ences?)|repeat(?:ing|ed)?|rrule|cron|scheduled? every|"
        r"daily|weekly|monthly|annually|calendar invite)\b",
        re.I,
    ),
    "date_boundary": re.compile(
        r"\b(?:date boundar(?:y|ies)|day boundar(?:y|ies)|midnight|end of day|start of day|"
        r"month end|quarter end|year end|cross(?:es|ing)? midnight|next day|previous day)\b",
        re.I,
    ),
    "local_time_display": re.compile(
        r"\b(?:local time|user local|user[- ]local|viewer local|display time|shown to users?|"
        r"render(?:ed)? time|localized time)\b",
        re.I,
    ),
    "locale_calendar": re.compile(
        r"\b(?:locale calendar|locale-specific calendar|regional calendar|business days?|"
        r"week starts? on|first day of week|holiday calendar|fiscal calendar)\b",
        re.I,
    ),
    "clock_assumption": re.compile(
        r"\b(?:server clock|client clock|browser clock|device clock|clock skew|system time|"
        r"ntp|time drift|now\(\)|current time)\b",
        re.I,
    ),
    "date_semantics": re.compile(
        r"\b(?:date[- ]only|timestamp|datetime|date time|instant|epoch|unix time|iso[- ]?8601|"
        r"expires? at|due date|launch window)\b",
        re.I,
    ),
}
_PATH_CATEGORY_PATTERNS: dict[TimeZoneImpactCategory, re.Pattern[str]] = {
    "time_zone_handling": re.compile(r"(?:time[-_]?zones?|timezone|tz|zoneinfo|pytz)", re.I),
    "dst_boundary": re.compile(r"(?:dst|daylight[-_]?saving|ambiguous[-_]?time)", re.I),
    "recurring_schedule": re.compile(r"(?:recurr|rrule|cron|schedules?|calendar)", re.I),
    "date_boundary": re.compile(r"(?:date[-_]?boundar|midnight|end[-_]?of[-_]?day)", re.I),
    "local_time_display": re.compile(r"(?:local[-_]?time|display[-_]?time|localized[-_]?time)", re.I),
    "locale_calendar": re.compile(r"(?:locale|holidays?|business[-_]?days?|fiscal[-_]?calendar)", re.I),
    "clock_assumption": re.compile(r"(?:clock|ntp|time[-_]?drift|current[-_]?time)", re.I),
    "date_semantics": re.compile(r"(?:timestamps?|datetimes?|date[-_]?only|iso[-_]?8601|epoch)", re.I),
}
_SAFEGUARD_PATTERNS: dict[TimeZoneSafeguard, re.Pattern[str]] = {
    "utc_persistence": re.compile(
        r"\b(?:store|persist|save|write|database|db).{0,40}\butc\b|\butc.{0,40}(?:storage|persistence|persisted|timestamp)\b",
        re.I,
    ),
    "user_timezone_conversion": re.compile(
        r"\b(?:convert|conversion|translate|render|display).{0,50}(?:user(?:'s)? timezone|user time zone|local time|viewer timezone)|"
        r"\b(?:user(?:'s)? timezone|user time zone|iana timezone)\b",
        re.I,
    ),
    "dst_transition_validation": re.compile(
        r"\b(?:dst transition|daylight[- ]saving transition|spring forward|fall back|ambiguous time|nonexistent time)\b",
        re.I,
    ),
    "server_client_clock_assumption": re.compile(
        r"\b(?:server/client clock|server clock|client clock|browser clock|device clock|clock skew|ntp|time drift)\b",
        re.I,
    ),
    "recurrence_boundary_validation": re.compile(
        r"\b(?:recurrence boundary|recurring schedule test|rrule test|cron test|repeat across|monthly boundary|weekly boundary)\b",
        re.I,
    ),
    "date_only_timestamp_semantics": re.compile(
        r"\b(?:date[- ]only versus timestamp|date[- ]only vs timestamp|date[- ]only semantics|timestamp semantics|"
        r"date semantics|instant semantics)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[TimeZoneSafeguard, str] = {
    "utc_persistence": "Verify timestamps are persisted in UTC with an explicit zone or instant representation.",
    "user_timezone_conversion": "Verify user time zone conversion for input, API output, and UI display.",
    "dst_transition_validation": "Test daylight-saving transition cases, including skipped and repeated local times.",
    "server_client_clock_assumption": "Validate server, client, and device clock assumptions including skew and drift.",
    "recurrence_boundary_validation": "Validate recurring schedules across day, week, month, and DST boundaries.",
    "date_only_timestamp_semantics": "Confirm date-only values and timestamp instants use distinct semantics and validation.",
}


@dataclass(frozen=True, slots=True)
class TaskTimeZoneImpactRecord:
    """Time-zone impact guidance for one execution task."""

    task_id: str
    title: str
    matched_time_signals: tuple[str, ...] = field(default_factory=tuple)
    impact_categories: tuple[TimeZoneImpactCategory, ...] = field(default_factory=tuple)
    required_safeguards: tuple[TimeZoneSafeguard, ...] = field(default_factory=tuple)
    present_safeguards: tuple[TimeZoneSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[TimeZoneSafeguard, ...] = field(default_factory=tuple)
    risk_level: TimeZoneImpactRisk = "medium"
    recommended_validation_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_time_signals": list(self.matched_time_signals),
            "impact_categories": list(self.impact_categories),
            "required_safeguards": list(self.required_safeguards),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "recommended_validation_checks": list(self.recommended_validation_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskTimeZoneImpactPlan:
    """Plan-level time-zone impact recommendations."""

    plan_id: str | None = None
    records: tuple[TaskTimeZoneImpactRecord, ...] = field(default_factory=tuple)
    time_zone_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskTimeZoneImpactRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "time_zone_task_ids": list(self.time_zone_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return time-zone impact records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render time-zone impact recommendations as deterministic Markdown."""
        title = "# Task Time Zone Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Time-zone task count: {self.summary.get('time_zone_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task time-zone impact recommendations were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Recommendations",
                "",
                "| Task | Title | Risk | Signals | Impact Categories | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.matched_time_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.impact_categories) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_validation_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_time_zone_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskTimeZoneImpactPlan:
    """Build time-zone impact recommendations for execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    ignored_task_ids = tuple(
        _task_id(task, index) for index, task in enumerate(tasks, start=1) if candidates[index - 1] is None
    )
    return TaskTimeZoneImpactPlan(
        plan_id=plan_id,
        records=records,
        time_zone_task_ids=tuple(record.task_id for record in records),
        ignored_task_ids=ignored_task_ids,
        summary=_summary(records, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def analyze_task_time_zone_impact(source: Any) -> TaskTimeZoneImpactPlan:
    """Compatibility alias for building time-zone impact recommendations."""
    return build_task_time_zone_impact_plan(source)


def summarize_task_time_zone_impact(source: Any) -> TaskTimeZoneImpactPlan:
    """Compatibility alias for building time-zone impact recommendations."""
    return build_task_time_zone_impact_plan(source)


def extract_task_time_zone_impact(source: Any) -> TaskTimeZoneImpactPlan:
    """Compatibility alias for building time-zone impact recommendations."""
    return build_task_time_zone_impact_plan(source)


def generate_task_time_zone_impact(source: Any) -> TaskTimeZoneImpactPlan:
    """Compatibility alias for generating time-zone impact recommendations."""
    return build_task_time_zone_impact_plan(source)


def recommend_task_time_zone_impact(source: Any) -> TaskTimeZoneImpactPlan:
    """Compatibility alias for recommending time-zone impact safeguards."""
    return build_task_time_zone_impact_plan(source)


def task_time_zone_impact_plan_to_dict(result: TaskTimeZoneImpactPlan) -> dict[str, Any]:
    """Serialize a time-zone impact plan to a plain dictionary."""
    return result.to_dict()


task_time_zone_impact_plan_to_dict.__test__ = False


def task_time_zone_impact_plan_to_dicts(
    result: TaskTimeZoneImpactPlan | Iterable[TaskTimeZoneImpactRecord],
) -> list[dict[str, Any]]:
    """Serialize time-zone impact records to plain dictionaries."""
    if isinstance(result, TaskTimeZoneImpactPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_time_zone_impact_plan_to_dicts.__test__ = False


def task_time_zone_impact_plan_to_markdown(result: TaskTimeZoneImpactPlan) -> str:
    """Render a time-zone impact plan as Markdown."""
    return result.to_markdown()


task_time_zone_impact_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    categories: tuple[TimeZoneImpactCategory, ...] = field(default_factory=tuple)
    category_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[TimeZoneSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _record(task: Mapping[str, Any], index: int) -> TaskTimeZoneImpactRecord | None:
    signals = _signals(task)
    if not signals.categories:
        return None

    required_safeguards = _required_safeguards(signals.categories)
    missing_safeguards = tuple(
        safeguard for safeguard in required_safeguards if safeguard not in signals.present_safeguards
    )
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskTimeZoneImpactRecord(
        task_id=task_id,
        title=title,
        matched_time_signals=signals.categories,
        impact_categories=signals.categories,
        required_safeguards=required_safeguards,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing_safeguards,
        risk_level=_risk_level(signals.categories, missing_safeguards),
        recommended_validation_checks=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in required_safeguards),
        evidence=tuple(_dedupe([*signals.category_evidence, *signals.safeguard_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    category_hits: set[TimeZoneImpactCategory] = set()
    safeguard_hits: set[TimeZoneSafeguard] = set()
    category_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_categories = _path_categories(normalized)
        if path_categories:
            category_hits.update(path_categories)
            category_evidence.append(f"files_or_modules: {path}")
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(searchable) or pattern.search(normalized):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_category = False
        for category, pattern in _TEXT_CATEGORY_PATTERNS.items():
            if pattern.search(text):
                category_hits.add(category)
                matched_category = True
        if matched_category:
            category_evidence.append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    return _Signals(
        categories=tuple(category for category in _CATEGORY_ORDER if category in category_hits),
        category_evidence=tuple(_dedupe(category_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_categories(path: str) -> set[TimeZoneImpactCategory]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    categories: set[TimeZoneImpactCategory] = set()
    for category, pattern in _PATH_CATEGORY_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(text):
            categories.add(category)
    name = PurePosixPath(normalized).name
    if name in {"schedule.py", "scheduler.py", "calendar.ts", "time.py"}:
        categories.add("recurring_schedule" if "schedule" in name else "time_zone_handling")
    return categories


def _required_safeguards(categories: tuple[TimeZoneImpactCategory, ...]) -> tuple[TimeZoneSafeguard, ...]:
    required: set[TimeZoneSafeguard] = {"utc_persistence", "user_timezone_conversion", "dst_transition_validation"}
    if "clock_assumption" in categories:
        required.add("server_client_clock_assumption")
    if "recurring_schedule" in categories:
        required.add("recurrence_boundary_validation")
    if "date_boundary" in categories or "date_semantics" in categories:
        required.add("date_only_timestamp_semantics")
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required)


def _risk_level(
    categories: tuple[TimeZoneImpactCategory, ...],
    missing_safeguards: tuple[TimeZoneSafeguard, ...],
) -> TimeZoneImpactRisk:
    if not missing_safeguards:
        return "low"
    if any(category in _HIGH_RISK_CATEGORIES for category in categories):
        return "high"
    if len(missing_safeguards) >= 3 or "clock_assumption" in categories:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskTimeZoneImpactRecord, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "time_zone_task_count": len(records),
        "ignored_task_ids": list(ignored_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {risk: sum(1 for record in records if record.risk_level == risk) for risk in _RISK_ORDER},
        "category_counts": {
            category: sum(1 for record in records if category in record.impact_categories)
            for category in sorted({category for record in records for category in record.impact_categories})
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "time_zone_task_ids": [record.task_id for record in records],
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
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
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
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
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
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
    return any(pattern.search(value) for pattern in [*_TEXT_CATEGORY_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


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
    "TaskTimeZoneImpactPlan",
    "TaskTimeZoneImpactRecord",
    "TimeZoneImpactCategory",
    "TimeZoneImpactRisk",
    "TimeZoneSafeguard",
    "analyze_task_time_zone_impact",
    "build_task_time_zone_impact_plan",
    "extract_task_time_zone_impact",
    "generate_task_time_zone_impact",
    "recommend_task_time_zone_impact",
    "summarize_task_time_zone_impact",
    "task_time_zone_impact_plan_to_dict",
    "task_time_zone_impact_plan_to_dicts",
    "task_time_zone_impact_plan_to_markdown",
]
