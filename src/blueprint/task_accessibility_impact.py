"""Assess execution-plan tasks for accessibility impact."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


AccessibilityReviewArea = Literal[
    "ui_semantics",
    "navigation",
    "keyboard",
    "focus_management",
    "color_contrast",
    "forms",
    "error_messages",
    "media",
    "documents",
    "screen_reader",
    "general_accessibility",
]
AccessibilityImpactSeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_ACCESSIBILITY_ACCEPTANCE_RE = re.compile(
    r"\b(?:accessibility|accessible|a11y|wcag|screen reader|keyboard|focus|"
    r"contrast|aria|alt text|caption|transcript|label|error message)\b",
    re.IGNORECASE,
)
_AREA_ACCEPTANCE_RE: dict[AccessibilityReviewArea, re.Pattern[str]] = {
    "ui_semantics": re.compile(
        r"\b(?:semantic|semantics|landmark|landmarks|heading|headings|aria|role|roles|accessible name)\b",
        re.IGNORECASE,
    ),
    "navigation": re.compile(
        r"\b(?:navigation|nav|route|skip link|breadcrumb|menu)\b",
        re.IGNORECASE,
    ),
    "keyboard": re.compile(
        r"\b(?:keyboard|tab order|tabindex|shortcut|hotkey|enter key|space key)\b",
        re.IGNORECASE,
    ),
    "focus_management": re.compile(
        r"\b(?:focus|focus trap|focus order|focus visible|focus ring)\b",
        re.IGNORECASE,
    ),
    "color_contrast": re.compile(
        r"\b(?:contrast|color|colour|dark mode|theme|palette)\b",
        re.IGNORECASE,
    ),
    "forms": re.compile(
        r"\b(?:form|forms|label|input|field|fieldset|required field)\b",
        re.IGNORECASE,
    ),
    "error_messages": re.compile(
        r"\b(?:error|errors|validation message|alert|inline message)\b",
        re.IGNORECASE,
    ),
    "media": re.compile(
        r"\b(?:caption|captions|transcript|alt text|audio description|video|media)\b",
        re.IGNORECASE,
    ),
    "documents": re.compile(
        r"\b(?:document|documents|pdf|markdown|heading|table of contents)\b",
        re.IGNORECASE,
    ),
    "screen_reader": re.compile(
        r"\b(?:screen reader|screenreader|sr-only|aria|live region|announcement)\b",
        re.IGNORECASE,
    ),
    "general_accessibility": _ACCESSIBILITY_ACCEPTANCE_RE,
}
_SIGNAL_PATTERNS: dict[AccessibilityReviewArea, re.Pattern[str]] = {
    "ui_semantics": re.compile(
        r"\b(?:ui|user interface|frontend|front end|component|button|modal|dialog|"
        r"tooltip|popover|menu|tab panel|accordion|semantic|semantic html|landmark|heading|"
        r"aria|role|accessible name)\b",
        re.IGNORECASE,
    ),
    "navigation": re.compile(
        r"\b(?:navigation|navigate|nav|navbar|route|routing|router|breadcrumb|"
        r"menu|skip link|sidebar|pagination|wizard|stepper)\b",
        re.IGNORECASE,
    ),
    "keyboard": re.compile(
        r"\b(?:keyboard|tab order|tabindex|shortcut|hotkey|enter key|space key|"
        r"escape key|arrow keys|roving tabindex)\b",
        re.IGNORECASE,
    ),
    "focus_management": re.compile(
        r"\b(?:focus|focus management|focus trap|focus order|focus visible|"
        r"focus ring|restore focus|auto[- ]?focus)\b",
        re.IGNORECASE,
    ),
    "color_contrast": re.compile(
        r"\b(?:contrast|color contrast|colour contrast|color palette|colour palette|"
        r"theme|dark mode|light mode|foreground|background color|status color)\b",
        re.IGNORECASE,
    ),
    "forms": re.compile(
        r"\b(?:form|forms|input|field|fieldset|label|placeholder|select|checkbox|"
        r"radio|textarea|required field|validation state)\b",
        re.IGNORECASE,
    ),
    "error_messages": re.compile(
        r"\b(?:error message|error messages|validation message|inline error|"
        r"form error|alert|toast|banner|empty state error|failure state)\b",
        re.IGNORECASE,
    ),
    "media": re.compile(
        r"\b(?:image|images|icon|icons|video|audio|media|caption|captions|"
        r"transcript|alt text|alternative text|audio description|animation)\b",
        re.IGNORECASE,
    ),
    "documents": re.compile(
        r"\b(?:document|documents|pdf|markdown|docx|spreadsheet|csv|exported report|"
        r"report export|table of contents|reading order)\b",
        re.IGNORECASE,
    ),
    "screen_reader": re.compile(
        r"\b(?:screen reader|screenreader|assistive technology|sr-only|aria-live|"
        r"live region|announcement|announced|accessible description)\b",
        re.IGNORECASE,
    ),
}
_AREA_ORDER: dict[AccessibilityReviewArea, int] = {
    "ui_semantics": 0,
    "navigation": 1,
    "keyboard": 2,
    "focus_management": 3,
    "color_contrast": 4,
    "forms": 5,
    "error_messages": 6,
    "media": 7,
    "documents": 8,
    "screen_reader": 9,
    "general_accessibility": 10,
}
_SEVERITY_ORDER: dict[AccessibilityImpactSeverity, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}


@dataclass(frozen=True, slots=True)
class TaskAccessibilityImpactRecord:
    """Accessibility impact guidance for one execution task."""

    task_id: str
    task_title: str
    severity: AccessibilityImpactSeverity
    review_areas: tuple[AccessibilityReviewArea, ...]
    required_checks: tuple[str, ...]
    missing_acceptance_criteria: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "task_title": self.task_title,
            "severity": self.severity,
            "review_areas": list(self.review_areas),
            "required_checks": list(self.required_checks),
            "missing_acceptance_criteria": list(self.missing_acceptance_criteria),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskAccessibilityImpactPlan:
    """Plan-level accessibility impact guidance and rollup counts."""

    plan_id: str | None = None
    records: tuple[TaskAccessibilityImpactRecord, ...] = field(default_factory=tuple)
    accessibility_task_ids: tuple[str, ...] = field(default_factory=tuple)
    low_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "accessibility_task_ids": list(self.accessibility_task_ids),
            "low_impact_task_ids": list(self.low_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return accessibility records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render accessibility impact guidance as deterministic Markdown."""
        title = "# Task Accessibility Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No execution tasks were found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Severity | Review Areas | Required Checks | Missing Acceptance Criteria |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{record.severity} | "
                f"{_markdown_cell(', '.join(record.review_areas))} | "
                f"{_markdown_cell('; '.join(record.required_checks))} | "
                f"{_markdown_cell('; '.join(record.missing_acceptance_criteria))} |"
            )
        return "\n".join(lines)


def build_task_accessibility_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskAccessibilityImpactPlan:
    """Build accessibility review guidance for execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (_task_record(task, index) for index, task in enumerate(tasks, start=1)),
            key=lambda record: (record.task_id, record.task_title),
        )
    )
    accessibility_task_ids = tuple(record.task_id for record in records if record.severity != "low")
    low_impact_task_ids = tuple(record.task_id for record in records if record.severity == "low")
    severity_counts = {
        severity: sum(1 for record in records if record.severity == severity)
        for severity in _SEVERITY_ORDER
    }
    area_counts = {
        area: sum(1 for record in records if area in record.review_areas) for area in _AREA_ORDER
    }

    return TaskAccessibilityImpactPlan(
        plan_id=plan_id,
        records=records,
        accessibility_task_ids=accessibility_task_ids,
        low_impact_task_ids=low_impact_task_ids,
        summary={
            "record_count": len(records),
            "accessibility_task_count": len(accessibility_task_ids),
            "low_impact_task_count": len(low_impact_task_ids),
            "missing_acceptance_criteria_count": sum(
                len(record.missing_acceptance_criteria) for record in records
            ),
            "severity_counts": severity_counts,
            "review_area_counts": area_counts,
        },
    )


def task_accessibility_impact_plan_to_dict(
    result: TaskAccessibilityImpactPlan,
) -> dict[str, Any]:
    """Serialize an accessibility impact plan to a plain dictionary."""
    return result.to_dict()


task_accessibility_impact_plan_to_dict.__test__ = False


def task_accessibility_impact_plan_to_markdown(
    result: TaskAccessibilityImpactPlan,
) -> str:
    """Render an accessibility impact plan as Markdown."""
    return result.to_markdown()


task_accessibility_impact_plan_to_markdown.__test__ = False


def summarize_task_accessibility_impacts(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskAccessibilityImpactPlan:
    """Compatibility alias for building task accessibility impact plans."""
    return build_task_accessibility_impact_plan(source)


def _task_record(task: Mapping[str, Any], index: int) -> TaskAccessibilityImpactRecord:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    has_accessibility_signal = bool(signals)
    areas = tuple(sorted(signals, key=lambda area: _AREA_ORDER[area]))
    if not areas:
        areas = ("general_accessibility",)

    return TaskAccessibilityImpactRecord(
        task_id=task_id,
        task_title=title,
        severity=_severity(areas, has_accessibility_signal),
        review_areas=areas,
        required_checks=tuple(_dedupe(_required_checks(areas, has_accessibility_signal))),
        missing_acceptance_criteria=tuple(
            _missing_acceptance_criteria(task, areas, has_accessibility_signal)
        ),
        evidence=tuple(_dedupe(item for area in areas for item in signals.get(area, ()))),
    )


def _signals(task: Mapping[str, Any]) -> dict[AccessibilityReviewArea, tuple[str, ...]]:
    signals: dict[AccessibilityReviewArea, list[str]] = {}

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _add_path_signals(signals, path)

    for source_field, text in _candidate_texts(task):
        for area, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text):
                _append(signals, area, f"{source_field}: {text}")

    return {area: tuple(_dedupe(evidence)) for area, evidence in signals.items() if evidence}


def _add_path_signals(
    signals: dict[AccessibilityReviewArea, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    suffix = path.suffix
    evidence = f"files_or_modules: {original}"

    if suffix in {".tsx", ".jsx", ".vue", ".svelte", ".html", ".css", ".scss"} or bool(
        {"ui", "frontend", "components", "component", "views", "pages", "templates"} & parts
    ):
        _append(signals, "ui_semantics", evidence)
    if bool({"routes", "router", "navigation", "nav", "menus", "sidebar"} & parts):
        _append(signals, "navigation", evidence)
    if "keyboard" in name or "shortcut" in name:
        _append(signals, "keyboard", evidence)
    if "focus" in name or "modal" in name or bool({"modals", "dialogs"} & parts):
        _append(signals, "focus_management", evidence)
    if bool({"theme", "themes", "colors", "styles", "css"} & parts) or suffix in {
        ".css",
        ".scss",
    }:
        _append(signals, "color_contrast", evidence)
    if bool({"forms", "form", "inputs", "fields"} & parts) or "form" in name:
        _append(signals, "forms", evidence)
    if bool({"errors", "validation", "alerts", "toasts"} & parts) or "error" in name:
        _append(signals, "error_messages", evidence)
    if bool({"media", "images", "videos", "audio", "icons", "assets"} & parts) or suffix in {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".svg",
        ".mp4",
        ".mov",
        ".mp3",
        ".wav",
    }:
        _append(signals, "media", evidence)
    if bool({"docs", "documents", "reports", "exports"} & parts) or suffix in {
        ".md",
        ".pdf",
        ".docx",
        ".csv",
    }:
        _append(signals, "documents", evidence)
    if "aria" in name or "screen_reader" in name or "screen-reader" in name:
        _append(signals, "screen_reader", evidence)


def _severity(
    areas: tuple[AccessibilityReviewArea, ...],
    has_accessibility_signal: bool,
) -> AccessibilityImpactSeverity:
    if not has_accessibility_signal:
        return "low"
    if any(
        area in areas
        for area in (
            "keyboard",
            "focus_management",
            "forms",
            "error_messages",
            "screen_reader",
        )
    ):
        return "high"
    return "medium"


def _required_checks(
    areas: tuple[AccessibilityReviewArea, ...],
    has_accessibility_signal: bool,
) -> list[str]:
    if not has_accessibility_signal:
        return [
            "Confirm the task does not change user-facing accessibility behavior.",
            "Keep the smallest relevant validation note with the task evidence.",
        ]

    checks: list[str] = []
    for area in areas:
        checks.extend(_AREA_CHECKS[area])
    checks.append("Record the accessibility evidence with the task id and changed files.")
    return checks


_AREA_CHECKS: dict[AccessibilityReviewArea, tuple[str, ...]] = {
    "ui_semantics": (
        "Verify semantic structure, heading order, landmarks, roles, and accessible names follow WCAG 2.2 expectations.",
        "Check interactive controls expose clear state, purpose, and name to assistive technology.",
    ),
    "navigation": (
        "Verify navigation order, current location, skip-link behavior, and route changes are perceivable.",
        "Check menus, breadcrumbs, pagination, and steppers can be understood without visual-only cues.",
    ),
    "keyboard": (
        "Verify every changed interactive path works with keyboard alone and has a logical tab order.",
        "Check custom shortcuts do not block browser or assistive technology keyboard commands.",
    ),
    "focus_management": (
        "Verify visible focus indication, focus trapping, focus restoration, and route or dialog focus placement.",
        "Check focus does not move unexpectedly during validation, loading, or dynamic updates.",
    ),
    "color_contrast": (
        "Verify text, icons, focus indicators, and state colors meet WCAG contrast thresholds.",
        "Check color is not the only cue for status, validation, selection, or required actions.",
    ),
    "forms": (
        "Verify labels, instructions, required state, grouping, autocomplete, and error association are programmatic.",
        "Check form completion and correction paths work with keyboard and assistive technology.",
    ),
    "error_messages": (
        "Verify validation errors are announced, associated with fields, and include actionable correction guidance.",
        "Check page-level alerts, inline errors, and async failures do not rely on color or timing alone.",
    ),
    "media": (
        "Verify meaningful images have alt text and decorative media is hidden from assistive technology.",
        "Check video, audio, animation, captions, transcripts, and motion controls where applicable.",
    ),
    "documents": (
        "Verify exported or authored documents have headings, reading order, table structure, and link text.",
        "Check generated reports, PDFs, Markdown, and CSVs remain usable with assistive technology.",
    ),
    "screen_reader": (
        "Verify changed content, dynamic updates, labels, descriptions, and live regions are announced correctly.",
        "Check ARIA usage follows native semantics first and does not create contradictory roles or states.",
    ),
    "general_accessibility": (
        "Confirm the task does not change user-facing accessibility behavior.",
        "Keep the smallest relevant validation note with the task evidence.",
    ),
}


def _missing_acceptance_criteria(
    task: Mapping[str, Any],
    areas: tuple[AccessibilityReviewArea, ...],
    has_accessibility_signal: bool,
) -> list[str]:
    if not has_accessibility_signal:
        return []

    criteria_text = " ".join(_strings(task.get("acceptance_criteria")))
    missing: list[str] = []
    if not _ACCESSIBILITY_ACCEPTANCE_RE.search(criteria_text):
        missing.append(
            "Add acceptance criteria requiring accessibility review against WCAG-oriented checks for the changed user-facing path."
        )
    for area in areas:
        if area == "general_accessibility":
            continue
        if not _AREA_ACCEPTANCE_RE[area].search(criteria_text):
            missing.append(_AREA_ACCEPTANCE_CRITERIA[area])
    return _dedupe(missing)


_AREA_ACCEPTANCE_CRITERIA: dict[AccessibilityReviewArea, str] = {
    "ui_semantics": "Add acceptance criteria covering semantic HTML, landmarks, headings, roles, and accessible names.",
    "navigation": "Add acceptance criteria covering accessible navigation order, current location, and route-change behavior.",
    "keyboard": "Add acceptance criteria proving the changed interaction works with keyboard alone.",
    "focus_management": "Add acceptance criteria covering visible focus, focus order, focus trapping, and focus restoration.",
    "color_contrast": "Add acceptance criteria requiring WCAG contrast checks and non-color status cues.",
    "forms": "Add acceptance criteria covering labels, required state, instructions, and accessible field grouping.",
    "error_messages": "Add acceptance criteria proving validation errors are announced, associated with fields, and actionable.",
    "media": "Add acceptance criteria covering alt text, captions, transcripts, decorative media, and motion controls.",
    "documents": "Add acceptance criteria covering document headings, reading order, table structure, and link text.",
    "screen_reader": "Add acceptance criteria requiring screen-reader verification for labels, descriptions, and dynamic announcements.",
    "general_accessibility": "Add acceptance criteria requiring accessibility review for the changed user-facing path.",
}


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
    for field_name in ("acceptance_criteria", "tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
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

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
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


def _append(
    signals: dict[AccessibilityReviewArea, list[str]],
    area: AccessibilityReviewArea,
    evidence: str,
) -> None:
    signals.setdefault(area, []).append(evidence)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "AccessibilityImpactSeverity",
    "AccessibilityReviewArea",
    "TaskAccessibilityImpactPlan",
    "TaskAccessibilityImpactRecord",
    "build_task_accessibility_impact_plan",
    "summarize_task_accessibility_impacts",
    "task_accessibility_impact_plan_to_dict",
    "task_accessibility_impact_plan_to_markdown",
]
