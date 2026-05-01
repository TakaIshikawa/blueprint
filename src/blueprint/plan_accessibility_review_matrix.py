"""Build accessibility review matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


AccessibilityReviewArea = Literal[
    "ui_semantics",
    "mobile_accessibility",
    "design_system",
    "forms",
    "keyboard_navigation",
    "focus_management",
    "screen_reader",
    "color_contrast",
    "media_alternatives",
    "internationalization",
]
AccessibilityReviewSeverity = Literal["critical", "high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_AREA_ORDER: dict[AccessibilityReviewArea, int] = {
    "ui_semantics": 0,
    "mobile_accessibility": 1,
    "design_system": 2,
    "forms": 3,
    "keyboard_navigation": 4,
    "focus_management": 5,
    "screen_reader": 6,
    "color_contrast": 7,
    "media_alternatives": 8,
    "internationalization": 9,
}
_SEVERITY_ORDER: dict[AccessibilityReviewSeverity, int] = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}
_TEXT_PATTERNS: dict[AccessibilityReviewArea, re.Pattern[str]] = {
    "ui_semantics": re.compile(
        r"\b(?:ui|user interface|frontend|front end|component|button|modal|dialog|"
        r"tooltip|popover|menu|accordion|semantic|landmark|heading|aria|role|"
        r"accessible name)\b",
        re.I,
    ),
    "mobile_accessibility": re.compile(
        r"\b(?:mobile|ios|android|react native|react-native|expo|"
        r"dynamic type|font scaling|touch target|gesture)\b",
        re.I,
    ),
    "design_system": re.compile(
        r"\b(?:design system|component library|storybook|tokens?|"
        r"shared component|ui kit)\b",
        re.I,
    ),
    "forms": re.compile(
        r"\b(?:form|forms|input|field|fieldset|label|placeholder|select|checkbox|"
        r"radio|textarea|required field|validation state)\b",
        re.I,
    ),
    "keyboard_navigation": re.compile(
        r"\b(?:keyboard|tab order|tabindex|shortcut|hotkey|enter key|space key|"
        r"escape key|arrow keys|roving tabindex)\b",
        re.I,
    ),
    "focus_management": re.compile(
        r"\b(?:focus|focus management|focus trap|focus order|focus visible|"
        r"focus ring|restore focus|auto[- ]?focus)\b",
        re.I,
    ),
    "screen_reader": re.compile(
        r"\b(?:screen reader|screenreader|assistive technology|sr-only|aria-live|"
        r"live region|announcement|announced|accessible description|voiceover|talkback)\b",
        re.I,
    ),
    "color_contrast": re.compile(
        r"\b(?:contrast|color contrast|colour contrast|color palette|colour palette|"
        r"theme|dark mode|light mode|foreground|background color|status color)\b",
        re.I,
    ),
    "media_alternatives": re.compile(
        r"\b(?:image|images|icon|icons|video|audio|media|caption|captions|"
        r"transcript|alt text|alternative text|audio description|animation)\b",
        re.I,
    ),
    "internationalization": re.compile(
        r"\b(?:internationalization|internationalisation|i18n|localization|localisation|"
        r"locale|translation|rtl|right-to-left|bidirectional|bidi|language switcher|"
        r"date format|number format)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class PlanAccessibilityReviewRecord:
    """One accessibility review row for a relevant execution task."""

    task_id: str
    title: str
    severity: AccessibilityReviewSeverity
    review_areas: tuple[AccessibilityReviewArea, ...]
    required_evidence: tuple[str, ...]
    suggested_validation: tuple[str, ...]
    text_evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "severity": self.severity,
            "review_areas": list(self.review_areas),
            "required_evidence": list(self.required_evidence),
            "suggested_validation": list(self.suggested_validation),
            "text_evidence": list(self.text_evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanAccessibilityReviewMatrix:
    """Plan-level accessibility review matrix and summary counts."""

    plan_id: str | None = None
    records: tuple[PlanAccessibilityReviewRecord, ...] = field(default_factory=tuple)
    accessibility_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_review_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "accessibility_task_ids": list(self.accessibility_task_ids),
            "no_review_task_ids": list(self.no_review_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return accessibility review records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown with a summary section."""
        title = "# Plan Accessibility Review Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title, "", "## Summary", ""]
        severity_counts = self.summary.get("severity_counts", {})
        lines.extend(
            [
                f"- Tasks analyzed: {self.summary.get('task_count', 0)}",
                f"- Accessibility review tasks: {self.summary.get('accessibility_task_count', 0)}",
                (
                    "- Severity counts: "
                    f"critical {severity_counts.get('critical', 0)}, "
                    f"high {severity_counts.get('high', 0)}, "
                    f"medium {severity_counts.get('medium', 0)}, "
                    f"low {severity_counts.get('low', 0)}"
                ),
            ]
        )
        if self.no_review_task_ids:
            lines.append(f"- No accessibility review needed: {_markdown_cell(', '.join(self.no_review_task_ids))}")
        if not self.records:
            lines.extend(["", "No accessibility review records were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                "| Task | Severity | Review Areas | Required Evidence | Suggested Validation | Text Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.severity} | "
                f"{_markdown_cell(', '.join(record.review_areas))} | "
                f"{_markdown_cell('; '.join(record.required_evidence))} | "
                f"{_markdown_cell('; '.join(record.suggested_validation))} | "
                f"{_markdown_cell('; '.join(record.text_evidence))} |"
            )
        return "\n".join(lines)


def build_plan_accessibility_review_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanAccessibilityReviewMatrix:
    """Build accessibility review records for user-facing execution tasks.

    Backend-only tasks with no UI, mobile, design-system, form, assistive-technology,
    media, or internationalization-adjacent signals are classified as not needing an
    accessibility review and are omitted from the matrix records.
    """
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _SEVERITY_ORDER[record.severity],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    no_review_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    severity_counts = {
        severity: sum(1 for record in records if record.severity == severity)
        for severity in _SEVERITY_ORDER
    }
    review_area_counts = {
        area: sum(1 for record in records if area in record.review_areas)
        for area in _AREA_ORDER
    }
    return PlanAccessibilityReviewMatrix(
        plan_id=plan_id,
        records=records,
        accessibility_task_ids=tuple(record.task_id for record in records),
        no_review_task_ids=no_review_task_ids,
        summary={
            "task_count": len(tasks),
            "accessibility_task_count": len(records),
            "no_review_task_count": len(no_review_task_ids),
            "severity_counts": severity_counts,
            "review_area_counts": review_area_counts,
        },
    )


def summarize_plan_accessibility_review_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanAccessibilityReviewMatrix:
    """Compatibility alias for building plan accessibility review matrices."""
    return build_plan_accessibility_review_matrix(source)


def plan_accessibility_review_matrix_to_dict(
    result: PlanAccessibilityReviewMatrix,
) -> dict[str, Any]:
    """Serialize an accessibility review matrix to a plain dictionary."""
    return result.to_dict()


plan_accessibility_review_matrix_to_dict.__test__ = False


def plan_accessibility_review_matrix_to_markdown(
    result: PlanAccessibilityReviewMatrix,
) -> str:
    """Render an accessibility review matrix as Markdown."""
    return result.to_markdown()


plan_accessibility_review_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    areas: tuple[AccessibilityReviewArea, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> PlanAccessibilityReviewRecord | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals.areas:
        return None
    return PlanAccessibilityReviewRecord(
        task_id=task_id,
        title=title,
        severity=_severity(signals.areas),
        review_areas=signals.areas,
        required_evidence=_required_evidence(signals.areas),
        suggested_validation=_suggested_validation(signals.areas),
        text_evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    areas: set[AccessibilityReviewArea] = set()
    evidence: list[str] = []
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        path_areas = _path_areas(path)
        if path_areas:
            areas.update(path_areas)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched = False
        for area, pattern in _TEXT_PATTERNS.items():
            if pattern.search(text):
                areas.add(area)
                matched = True
        if matched:
            evidence.append(snippet)

    return _Signals(
        areas=tuple(area for area in _AREA_ORDER if area in areas),
        evidence=tuple(_dedupe(evidence)),
    )


def _path_areas(original: str) -> set[AccessibilityReviewArea]:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return set()
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    suffix = path.suffix
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    areas: set[AccessibilityReviewArea] = set()
    if suffix in {".tsx", ".jsx", ".vue", ".svelte", ".html", ".css", ".scss"} or bool(
        {"ui", "frontend", "components", "component", "views", "pages", "templates"} & parts
    ):
        areas.add("ui_semantics")
    if {"ios", "android", "mobile", "react-native", "react_native", "expo"} & parts:
        areas.add("mobile_accessibility")
    if bool({"design-system", "design_system", "storybook", "tokens"} & parts):
        areas.add("design_system")
    if bool({"forms", "form", "inputs", "fields"} & parts) or "form" in name:
        areas.add("forms")
    if "keyboard" in name or "shortcut" in name:
        areas.add("keyboard_navigation")
    if "focus" in name or "modal" in name or bool({"modals", "dialogs"} & parts):
        areas.add("focus_management")
    if "aria" in name or "screen_reader" in name or "screen-reader" in name:
        areas.add("screen_reader")
    if bool({"theme", "themes", "colors", "styles", "css"} & parts) or suffix in {".css", ".scss"}:
        areas.add("color_contrast")
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
        areas.add("media_alternatives")
    if any(token in text for token in ("i18n", "locale", "localization", "localisation", "rtl", "translation")):
        areas.add("internationalization")
    return areas


def _severity(areas: tuple[AccessibilityReviewArea, ...]) -> AccessibilityReviewSeverity:
    area_set = set(areas)
    if {"forms", "keyboard_navigation", "focus_management", "screen_reader"} & area_set:
        return "critical"
    if "mobile_accessibility" in area_set or "design_system" in area_set:
        return "high"
    if {"color_contrast", "media_alternatives", "internationalization"} & area_set:
        return "medium"
    return "low"


def _required_evidence(areas: tuple[AccessibilityReviewArea, ...]) -> tuple[str, ...]:
    evidence: list[str] = []
    for area in areas:
        evidence.extend(_AREA_EVIDENCE[area])
    evidence.append("Attach the accessibility reviewer, task id, changed files, and final sign-off decision.")
    return tuple(_dedupe(evidence))


def _suggested_validation(areas: tuple[AccessibilityReviewArea, ...]) -> tuple[str, ...]:
    validation: list[str] = []
    for area in areas:
        validation.extend(_AREA_VALIDATION[area])
    validation.append("Run automated accessibility checks and pair them with targeted manual assistive-technology review.")
    return tuple(_dedupe(validation))


_AREA_EVIDENCE: dict[AccessibilityReviewArea, tuple[str, ...]] = {
    "ui_semantics": (
        "Document semantic HTML, landmark, heading, role, name, state, and description checks.",
    ),
    "mobile_accessibility": (
        "Document VoiceOver or TalkBack coverage, font scaling, orientation, gesture, and touch-target checks.",
    ),
    "design_system": (
        "Document component states, token contrast, disabled state, focus style, and Storybook accessibility evidence.",
    ),
    "forms": (
        "Document labels, instructions, required state, autocomplete, grouping, and validation association evidence.",
    ),
    "keyboard_navigation": (
        "Document keyboard-only completion, tab order, shortcuts, roving tabindex, and escape behavior evidence.",
    ),
    "focus_management": (
        "Document focus placement, visible focus, focus trap, focus restoration, and async update behavior.",
    ),
    "screen_reader": (
        "Document screen-reader output for labels, descriptions, live regions, announcements, and dynamic updates.",
    ),
    "color_contrast": (
        "Document WCAG contrast checks for text, icons, focus indicators, charts, and state colors.",
    ),
    "media_alternatives": (
        "Document alt text, decorative treatment, captions, transcripts, audio descriptions, and motion controls.",
    ),
    "internationalization": (
        "Document translated copy expansion, locale formatting, language attributes, RTL, and bidirectional layout checks.",
    ),
}
_AREA_VALIDATION: dict[AccessibilityReviewArea, tuple[str, ...]] = {
    "ui_semantics": (
        "Inspect changed UI with browser accessibility tree or equivalent semantic tooling.",
    ),
    "mobile_accessibility": (
        "Validate on representative iOS and Android devices with VoiceOver or TalkBack enabled.",
    ),
    "design_system": (
        "Run component-level accessibility stories for default, hover, focus, active, disabled, error, and loading states.",
    ),
    "forms": (
        "Complete form success and correction paths with keyboard and assistive technology.",
    ),
    "keyboard_navigation": (
        "Navigate every changed interaction with keyboard alone and verify no traps except intentional modal traps.",
    ),
    "focus_management": (
        "Open, close, route, validate, and refresh changed views while verifying focus order and restoration.",
    ),
    "screen_reader": (
        "Verify announcements with at least one target screen reader for the changed platform.",
    ),
    "color_contrast": (
        "Check contrast thresholds in default, hover, focus, selected, disabled, error, and dark or light themes.",
    ),
    "media_alternatives": (
        "Verify media alternatives are present, accurate, synchronized where relevant, and not duplicated for decorative assets.",
    ),
    "internationalization": (
        "Validate long translations, RTL or bidirectional content, locale formats, and language switching with assistive technology.",
    ),
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
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes"):
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in _TEXT_PATTERNS.values()):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in _TEXT_PATTERNS.values()):
                texts.append((field, str(key)))
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


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
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
        "tags",
        "labels",
        "notes",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _text(text).rstrip(".")
    return f"{source_field}: {cleaned[:240]}"


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


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
    "AccessibilityReviewArea",
    "AccessibilityReviewSeverity",
    "PlanAccessibilityReviewMatrix",
    "PlanAccessibilityReviewRecord",
    "build_plan_accessibility_review_matrix",
    "plan_accessibility_review_matrix_to_dict",
    "plan_accessibility_review_matrix_to_markdown",
    "summarize_plan_accessibility_review_matrix",
]
