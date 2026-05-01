"""Plan browser and device compatibility checks for frontend execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


BrowserCompatibilityCategory = Literal[
    "layout_css",
    "javascript_api",
    "form_behavior",
    "media_canvas",
    "mobile_responsive",
    "legacy_browser",
]
BrowserCompatibilitySeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[BrowserCompatibilityCategory, ...] = (
    "layout_css",
    "javascript_api",
    "form_behavior",
    "media_canvas",
    "mobile_responsive",
    "legacy_browser",
)
_CATEGORY_RANK = {category: index for index, category in enumerate(_CATEGORY_ORDER)}
_SEVERITY_RANK: dict[BrowserCompatibilitySeverity, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}

_TEXT_SIGNAL_PATTERNS: dict[BrowserCompatibilityCategory, tuple[re.Pattern[str], ...]] = {
    "layout_css": (
        re.compile(
            r"\b(?:css|layout|grid|flex|flexbox|position(?:ing)?|sticky|overflow|"
            r"z-index|container quer(?:y|ies)|media quer(?:y|ies)|theme|dark mode|"
            r"visual regression|viewport|breakpoint)\b",
            re.IGNORECASE,
        ),
    ),
    "javascript_api": (
        re.compile(
            r"\b(?:browser api|web api|clipboard|navigator|permissions api|"
            r"geolocation|camera|microphone|polyfill|service worker|web worker|worker|intersection observer|"
            r"resize observer|websocket|indexeddb|localstorage|sessionstorage|"
            r"broadcastchannel|crypto\.subtle|webgl|webgpu|wasm|webassembly|"
            r"fetch|abortcontroller)\b",
            re.IGNORECASE,
        ),
    ),
    "form_behavior": (
        re.compile(
            r"\b(?:form|forms|input|select|textarea|checkbox|radio|date picker|"
            r"file upload|autocomplete|autofill|validation message|constraint validation|"
            r"ime|composition|keyboard|focus|tab order)\b",
            re.IGNORECASE,
        ),
    ),
    "media_canvas": (
        re.compile(
            r"\b(?:canvas|video|audio|media|webcam|camera|microphone|capture|"
            r"picture-in-picture|hls|mse|media source|web audio|animation|"
            r"requestanimationframe|svg|image rendering)\b",
            re.IGNORECASE,
        ),
    ),
    "mobile_responsive": (
        re.compile(
            r"\b(?:mobile|responsive|tablet|phone|touch|gesture|safe area|"
            r"orientation|portrait|landscape|small screen|viewport|breakpoint|"
            r"device pixel ratio|retina)\b",
            re.IGNORECASE,
        ),
    ),
    "legacy_browser": (
        re.compile(
            r"\b(?:safari|firefox|chromium|chrome|edge|ios safari|android browser|"
            r"legacy browser|older browser|cross[- ]browser|browser compatibility|"
            r"polyfill|transpile|browserslist|es5|es2015|ie11)\b",
            re.IGNORECASE,
        ),
    ),
}
_FILE_SIGNAL_PATTERNS: tuple[tuple[BrowserCompatibilityCategory, re.Pattern[str]], ...] = (
    ("layout_css", re.compile(r"\.(?:css|scss|sass|less|pcss)$", re.I)),
    ("layout_css", re.compile(r"(^|/)(?:styles?|theme|layout)(/|$)", re.I)),
    ("form_behavior", re.compile(r"(^|/)(?:forms?|inputs?|fields?|validators?)(/|$)", re.I)),
    ("media_canvas", re.compile(r"(^|/)(?:canvas|media|video|audio|player|camera)(/|$)", re.I)),
    ("mobile_responsive", re.compile(r"(^|/)(?:mobile|responsive|breakpoints?)(/|$)", re.I)),
    ("legacy_browser", re.compile(r"(^|/)(?:browserslist|babel|polyfills?)(?:\..*)?$", re.I)),
)
_FRONTEND_FILE_RE = re.compile(
    r"\.(?:tsx|jsx|vue|svelte|html|css|scss|sass|less)$|"
    r"(^|/)(?:frontend|client|web|ui|components?|pages?|views?|styles?|assets?)(/|$)",
    re.IGNORECASE,
)
_ACCESSIBILITY_RE = re.compile(
    r"\b(?:accessibility|accessible|a11y|wcag|keyboard|focus|screen reader|aria|"
    r"contrast|label|tab order)\b",
    re.IGNORECASE,
)
_RISKY_API_RE = re.compile(
    r"\b(?:clipboard|permissions api|service worker|web worker|intersection observer|"
    r"resize observer|indexeddb|broadcastchannel|crypto\.subtle|webgl|webgpu|"
    r"wasm|webassembly|camera|microphone|geolocation|abortcontroller)\b",
    re.IGNORECASE,
)
_VALIDATION_RE = re.compile(
    r"\b(?:playwright|cypress|selenium|browserstack|saucelabs|safari|firefox|"
    r"responsive|viewport|mobile|device|cross[- ]browser)\b",
    re.IGNORECASE,
)

_RATIONALES: dict[BrowserCompatibilityCategory, str] = {
    "layout_css": "Layout or styling changes can render differently across engines and viewport sizes.",
    "javascript_api": "Browser API usage may have permission, support, or polyfill differences.",
    "form_behavior": "Form controls, focus, validation, and autofill vary across browsers and devices.",
    "media_canvas": "Canvas, media, and animation behavior is sensitive to browser and device support.",
    "mobile_responsive": "Responsive and touch behavior needs explicit device and viewport validation.",
    "legacy_browser": "Compatibility, transpilation, or named browser support needs targeted validation.",
}
_SUGGESTED_CHECKS: dict[BrowserCompatibilityCategory, tuple[str, ...]] = {
    "layout_css": (
        "Run responsive viewport smoke tests at mobile, tablet, and desktop widths.",
        "Validate layout in Safari and Firefox for the affected route or component.",
    ),
    "javascript_api": (
        "Review browser support and required polyfills for the web APIs used.",
        "Validate behavior in Chromium, Firefox, and Safari with unsupported-permission fallbacks.",
    ),
    "form_behavior": (
        "Verify keyboard navigation, focus order, autofill, and native validation behavior.",
        "Check form submission and validation in Safari and Firefox.",
    ),
    "media_canvas": (
        "Smoke test canvas or media playback on Safari, Firefox, and Chromium.",
        "Validate device permission, codec, sizing, and fallback behavior where applicable.",
    ),
    "mobile_responsive": (
        "Run responsive viewport smoke tests at mobile, tablet, and desktop widths.",
        "Validate touch targets, orientation changes, and iOS Safari behavior.",
    ),
    "legacy_browser": (
        "Run the affected flow in the declared supported browser matrix.",
        "Review transpilation, Browserslist, and polyfill coverage for the changed code.",
    ),
}


@dataclass(frozen=True, slots=True)
class TaskBrowserCompatibilityRecord:
    """Browser compatibility guidance for one execution task category."""

    task_id: str
    title: str
    category: BrowserCompatibilityCategory
    severity: BrowserCompatibilitySeverity
    rationale: str
    suggested_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "category": self.category,
            "severity": self.severity,
            "rationale": self.rationale,
            "suggested_checks": list(self.suggested_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskBrowserCompatibilityPlan:
    """Task-level browser compatibility plan and rollup counts."""

    plan_id: str | None = None
    records: tuple[TaskBrowserCompatibilityRecord, ...] = field(default_factory=tuple)
    browser_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "browser_task_ids": list(self.browser_task_ids),
            "summary": dict(self.summary),
        }

    def to_markdown(self) -> str:
        """Render browser compatibility guidance as deterministic Markdown."""
        title = "# Task Browser Compatibility Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No browser compatibility signals detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Severity | Category | Suggested Checks | Rationale | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{record.severity} | "
                f"{record.category} | "
                f"{_markdown_cell('; '.join(record.suggested_checks))} | "
                f"{_markdown_cell(record.rationale)} | "
                f"{_markdown_cell('; '.join(record.evidence))} |"
            )
        return "\n".join(lines)


def build_task_browser_compatibility_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskBrowserCompatibilityPlan:
    """Detect frontend execution tasks needing browser/device compatibility checks."""
    plan_id, plan_evidence, tasks = _source_payload(source)
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        for record in _task_records(task, index, plan_evidence)
    ]
    records.sort(
        key=lambda record: (
            _SEVERITY_RANK[record.severity],
            record.task_id,
            _CATEGORY_RANK[record.category],
        )
    )
    result = tuple(records)
    browser_task_ids = tuple(_dedupe(record.task_id for record in result))
    severity_counts = {
        severity: sum(1 for record in result if record.severity == severity)
        for severity in _SEVERITY_RANK
    }
    category_counts = {
        category: sum(1 for record in result if record.category == category)
        for category in _CATEGORY_ORDER
    }
    return TaskBrowserCompatibilityPlan(
        plan_id=plan_id,
        records=result,
        browser_task_ids=browser_task_ids,
        summary={
            "task_count": len(tasks),
            "record_count": len(result),
            "browser_task_count": len(browser_task_ids),
            "severity_counts": severity_counts,
            "category_counts": category_counts,
        },
    )


def derive_task_browser_compatibility_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskBrowserCompatibilityPlan:
    """Compatibility alias for building a browser compatibility plan."""
    return build_task_browser_compatibility_plan(source)


def build_task_browser_compatibility(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskBrowserCompatibilityPlan:
    """Compatibility alias for building a browser compatibility plan."""
    return build_task_browser_compatibility_plan(source)


def derive_task_browser_compatibility(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskBrowserCompatibilityPlan:
    """Compatibility alias for building a browser compatibility plan."""
    return build_task_browser_compatibility_plan(source)


def task_browser_compatibility_plan_to_dict(
    plan: TaskBrowserCompatibilityPlan,
) -> dict[str, Any]:
    """Serialize a browser compatibility plan to a plain dictionary."""
    return plan.to_dict()


task_browser_compatibility_plan_to_dict.__test__ = False


def task_browser_compatibility_plan_to_markdown(
    plan: TaskBrowserCompatibilityPlan,
) -> str:
    """Render a browser compatibility plan as Markdown."""
    return plan.to_markdown()


task_browser_compatibility_plan_to_markdown.__test__ = False
task_browser_compatibility_to_dict = task_browser_compatibility_plan_to_dict
task_browser_compatibility_to_dict.__test__ = False
task_browser_compatibility_to_markdown = task_browser_compatibility_plan_to_markdown
task_browser_compatibility_to_markdown.__test__ = False


def _task_records(
    task: Mapping[str, Any],
    index: int,
    plan_evidence: tuple[str, ...],
) -> list[TaskBrowserCompatibilityRecord]:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    evidence_by_category = _detected_evidence(task)
    accessibility_evidence = _accessibility_evidence(task)

    return [
        TaskBrowserCompatibilityRecord(
            task_id=task_id,
            title=title,
            category=category,
            severity=_severity(category, evidence, accessibility_evidence),
            rationale=_rationale(category, evidence, accessibility_evidence),
            suggested_checks=tuple(
                _dedupe(
                    [
                        *_SUGGESTED_CHECKS[category],
                        *_accessibility_checks(category, accessibility_evidence),
                    ]
                )
            ),
            evidence=tuple(
                _dedupe(
                    [
                        *evidence,
                        *accessibility_evidence,
                        *_validation_values(task),
                        *plan_evidence,
                    ]
                )
            ),
        )
        for category, evidence in evidence_by_category.items()
    ]


def _detected_evidence(
    task: Mapping[str, Any],
) -> dict[BrowserCompatibilityCategory, tuple[str, ...]]:
    evidence_by_category: dict[BrowserCompatibilityCategory, list[str]] = {}
    frontend_file_seen = False

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        frontend_file_seen = frontend_file_seen or bool(_FRONTEND_FILE_RE.search(normalized))
        for category, pattern in _FILE_SIGNAL_PATTERNS:
            if pattern.search(normalized):
                _append_evidence(evidence_by_category, category, f"files_or_modules: {path}")
        _add_path_shape_signals(evidence_by_category, path)

    for field_path, text in _task_texts(task):
        for category, patterns in _TEXT_SIGNAL_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                _append_evidence(evidence_by_category, category, f"{field_path}: {text}")

    for field_path, text in _metadata_texts(task.get("metadata")):
        hinted_category = _metadata_hint_category(field_path)
        if hinted_category:
            _append_evidence(evidence_by_category, hinted_category, f"{field_path}: {text}")
        for category, patterns in _TEXT_SIGNAL_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                _append_evidence(evidence_by_category, category, f"{field_path}: {text}")

    if frontend_file_seen and not evidence_by_category:
        _append_evidence(
            evidence_by_category,
            "layout_css",
            "files_or_modules: frontend file requires browser smoke coverage",
        )

    return {
        category: tuple(_dedupe(evidence_by_category.get(category, ())))
        for category in _CATEGORY_ORDER
        if evidence_by_category.get(category)
    }


def _add_path_shape_signals(
    evidence_by_category: dict[BrowserCompatibilityCategory, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    evidence = f"files_or_modules: {original}"

    if {"forms", "form", "inputs", "fields"} & parts or "form" in name:
        _append_evidence(evidence_by_category, "form_behavior", evidence)
    if {"canvas", "media", "video", "audio", "player", "camera"} & parts:
        _append_evidence(evidence_by_category, "media_canvas", evidence)
    if {"mobile", "responsive", "breakpoints"} & parts:
        _append_evidence(evidence_by_category, "mobile_responsive", evidence)


def _severity(
    category: BrowserCompatibilityCategory,
    evidence: tuple[str, ...],
    accessibility_evidence: tuple[str, ...],
) -> BrowserCompatibilitySeverity:
    text = " ".join(evidence)
    if category in {"media_canvas", "legacy_browser"}:
        return "high"
    if category == "javascript_api" and _RISKY_API_RE.search(text):
        return "high"
    if accessibility_evidence and category in {"form_behavior", "mobile_responsive"}:
        return "high"
    if category in {"layout_css", "javascript_api", "form_behavior", "mobile_responsive"}:
        return "medium"
    return "low"


def _rationale(
    category: BrowserCompatibilityCategory,
    evidence: tuple[str, ...],
    accessibility_evidence: tuple[str, ...],
) -> str:
    parts = [_RATIONALES[category]]
    text = " ".join(evidence)
    if _VALIDATION_RE.search(text):
        parts.append("Existing validation notes already mention browser or device coverage.")
    if accessibility_evidence:
        parts.append("Accessibility-related evidence raises the priority for interactive checks.")
    return " ".join(parts)


def _accessibility_checks(
    category: BrowserCompatibilityCategory,
    accessibility_evidence: tuple[str, ...],
) -> tuple[str, ...]:
    if not accessibility_evidence:
        return ()
    if category in {"form_behavior", "mobile_responsive", "layout_css"}:
        return ("Include keyboard and focus checks in the browser compatibility pass.",)
    return ()


def _accessibility_evidence(task: Mapping[str, Any]) -> tuple[str, ...]:
    evidence = [
        f"{field_path}: {text}"
        for field_path, text in [*_task_texts(task), *_metadata_texts(task.get("metadata"))]
        if _ACCESSIBILITY_RE.search(text)
    ]
    return tuple(_dedupe(evidence))


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, tuple[str, ...], list[dict[str, Any]]]:
    if source is None:
        return None, (), []
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        plan = source.model_dump(mode="python")
        return (
            _optional_text(plan.get("id")),
            tuple(_plan_evidence(plan)),
            [task.model_dump(mode="python") for task in source.tasks],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return (
                _optional_text(plan.get("id")),
                tuple(_plan_evidence(plan)),
                _task_payloads(plan.get("tasks")),
            )
        return None, (), [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return (
            _optional_text(plan.get("id")),
            tuple(_plan_evidence(plan)),
            _task_payloads(plan.get("tasks")),
        )

    tasks: list[dict[str, Any]] = []
    for item in source:
        if task := _task_payload(item):
            tasks.append(task)
    return None, (), tasks


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
    return _object_payload(value)


def _object_payload(value: Any) -> dict[str, Any]:
    fields = (
        "id",
        "tasks",
        "title",
        "description",
        "milestone",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "test_strategy",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "validation_plan",
        "metadata",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    return texts


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


def _plan_evidence(plan: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    if text := _optional_text(plan.get("test_strategy")):
        evidence.append(f"test_strategy: {text}")
    evidence.extend(_validation_values(plan))
    return _dedupe(evidence)


def _validation_values(item: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command", "validation_plan"):
        if text := _optional_text(item.get(key)):
            values.append(f"{key}: {text}")
    metadata = item.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "browser_compatibility",
            "browser_matrix",
            "validation_plan",
            "validation_plans",
            "validation_gates",
            "validation_gate",
            "validation_commands",
            "validation_command",
            "test_commands",
            "test_command",
        ):
            value = metadata.get(key)
            if isinstance(value, Mapping):
                values.extend(
                    f"metadata.{key}: {command}" for command in flatten_validation_commands(value)
                )
            else:
                values.extend(f"metadata.{key}: {text}" for text in _strings(value))
    return _dedupe(values)


def _metadata_hint_category(source_field: str) -> BrowserCompatibilityCategory | None:
    field = source_field.casefold()
    if any(token in field for token in ("css", "layout", "style", "theme")):
        return "layout_css"
    if any(token in field for token in ("browser_api", "web_api", "polyfill", "api_support")):
        return "javascript_api"
    if any(token in field for token in ("form", "input", "focus", "keyboard")):
        return "form_behavior"
    if any(token in field for token in ("canvas", "media", "video", "audio", "camera")):
        return "media_canvas"
    if any(token in field for token in ("mobile", "responsive", "viewport", "breakpoint")):
        return "mobile_responsive"
    if any(token in field for token in ("browser_matrix", "legacy", "safari", "firefox")):
        return "legacy_browser"
    return None


def _append_evidence(
    evidence_by_category: dict[BrowserCompatibilityCategory, list[str]],
    category: BrowserCompatibilityCategory,
    evidence: str,
) -> None:
    evidence_by_category.setdefault(category, []).append(evidence)


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
    normalized = value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.strip("/")


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _dedupe(values: Iterable[_T | None]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "BrowserCompatibilityCategory",
    "BrowserCompatibilitySeverity",
    "TaskBrowserCompatibilityPlan",
    "TaskBrowserCompatibilityRecord",
    "build_task_browser_compatibility",
    "build_task_browser_compatibility_plan",
    "derive_task_browser_compatibility",
    "derive_task_browser_compatibility_plan",
    "task_browser_compatibility_to_dict",
    "task_browser_compatibility_to_markdown",
    "task_browser_compatibility_plan_to_dict",
    "task_browser_compatibility_plan_to_markdown",
]
