"""Plan accessibility readiness work for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


AccessibilitySignal = Literal[
    "accessibility",
    "wcag_aa",
    "semantic_structure",
    "keyboard_access",
    "focus_states",
    "screen_reader_labels",
    "contrast",
    "motion_preferences",
    "media_alternatives",
    "automated_checks",
    "manual_assistive_qa",
]
AccessibilityReadinessCategory = Literal[
    "semantic_structure",
    "keyboard_access",
    "focus_states",
    "screen_reader_labels",
    "contrast",
    "motion_preferences",
    "media_alternatives",
    "automated_checks",
    "manual_assistive_qa",
]
AccessibilityReadinessLevel = Literal["needs_planning", "partial", "ready"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[AccessibilityReadinessLevel, int] = {
    "needs_planning": 0,
    "partial": 1,
    "ready": 2,
}
_SIGNAL_ORDER: tuple[AccessibilitySignal, ...] = (
    "accessibility",
    "wcag_aa",
    "semantic_structure",
    "keyboard_access",
    "focus_states",
    "screen_reader_labels",
    "contrast",
    "motion_preferences",
    "media_alternatives",
    "automated_checks",
    "manual_assistive_qa",
)
_CATEGORY_ORDER: tuple[AccessibilityReadinessCategory, ...] = (
    "semantic_structure",
    "keyboard_access",
    "focus_states",
    "screen_reader_labels",
    "contrast",
    "motion_preferences",
    "media_alternatives",
    "automated_checks",
    "manual_assistive_qa",
)
_SIGNAL_PATTERNS: dict[AccessibilitySignal, re.Pattern[str]] = {
    "accessibility": re.compile(
        r"\b(?:accessibility|accessible|a11y|inclusive design|assistive technolog(?:y|ies)|"
        r"screen reader|keyboard only|keyboard[- ]accessible|aria|wcag)\b",
        re.I,
    ),
    "wcag_aa": re.compile(r"\b(?:wcag(?:\s*2(?:\.[012])?)?\s*(?:aa|a)|level aa|aa compliance)\b", re.I),
    "semantic_structure": re.compile(
        r"\b(?:semantic html|semantic structure|landmarks?|heading hierarchy|headings?|"
        r"main landmark|nav landmark|form semantics|native (?:html )?(?:button|link|input)s?)\b",
        re.I,
    ),
    "keyboard_access": re.compile(
        r"\b(?:keyboard(?: only| access| navigation| operable| accessible)?|tab order|tabbing|"
        r"no mouse|without a mouse|enter and space|escape key|arrow key|roving tabindex)\b",
        re.I,
    ),
    "focus_states": re.compile(
        r"\b(?:focus state|focus states|focus indicator|visible focus|focus ring|focus trap|"
        r"focus management|restore focus|initial focus|skip link)\b",
        re.I,
    ),
    "screen_reader_labels": re.compile(
        r"\b(?:screen reader|aria[- ]label|aria labelledby|aria describedby|accessible name|"
        r"alt text|image alt|label text|form label|announce|live region)\b",
        re.I,
    ),
    "contrast": re.compile(
        r"\b(?:color contrast|contrast ratio|text contrast|non[- ]text contrast|4\.5:1|3:1|"
        r"contrast requirements?|high contrast)\b",
        re.I,
    ),
    "motion_preferences": re.compile(
        r"\b(?:prefers-reduced-motion|reduced motion|motion preference|motion safe|disable animation|"
        r"pause animation|auto[- ]play motion|vestibular)\b",
        re.I,
    ),
    "media_alternatives": re.compile(
        r"\b(?:captions?|closed captions?|subtitles?|transcripts?|audio description|media alternative|"
        r"video alternative|audio alternative|described video)\b",
        re.I,
    ),
    "automated_checks": re.compile(
        r"\b(?:axe|pa11y|lighthouse accessibility|accessibility audit|automated accessibility|"
        r"jest-axe|playwright accessibility|storybook a11y|eslint-plugin-jsx-a11y)\b",
        re.I,
    ),
    "manual_assistive_qa": re.compile(
        r"\b(?:manual accessibility|assistive technology qa|screen reader qa|nvda|jaws|voiceover|"
        r"talkback|keyboard qa|manual a11y)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[AccessibilitySignal, re.Pattern[str]] = {
    "accessibility": re.compile(r"(?:a11y|accessibility|accessible|aria|wcag)", re.I),
    "semantic_structure": re.compile(r"(?:semantic|landmark|heading|form[_-]?label)", re.I),
    "keyboard_access": re.compile(r"(?:keyboard|tab[_-]?order|roving[_-]?tabindex)", re.I),
    "focus_states": re.compile(r"(?:focus|skip[_-]?link)", re.I),
    "screen_reader_labels": re.compile(r"(?:screen[_-]?reader|aria|alt[_-]?text|accessible[_-]?name)", re.I),
    "contrast": re.compile(r"(?:contrast|color[_-]?tokens?)", re.I),
    "motion_preferences": re.compile(r"(?:reduced[_-]?motion|motion[_-]?preference)", re.I),
    "media_alternatives": re.compile(r"(?:caption|transcript|audio[_-]?description|media[_-]?alternative)", re.I),
    "automated_checks": re.compile(r"(?:axe|pa11y|lighthouse|jsx[_-]?a11y)", re.I),
    "manual_assistive_qa": re.compile(r"(?:nvda|jaws|voiceover|talkback|assistive[_-]?qa)", re.I),
}
_NO_ACCESSIBILITY_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:accessibility|a11y|wcag|screen reader|keyboard access|captions?)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|requirements?)\b",
    re.I,
)
_CATEGORY_GUIDANCE: dict[AccessibilityReadinessCategory, tuple[str, tuple[str, ...]]] = {
    "semantic_structure": (
        "Implement semantic HTML structure for the affected interface.",
        (
            "Interactive controls use native elements or equivalent roles with valid states.",
            "Page or component headings, landmarks, and form relationships are deterministic.",
            "Automated or unit coverage verifies required roles, labels, and heading structure.",
        ),
    ),
    "keyboard_access": (
        "Make every affected workflow operable with the keyboard alone.",
        (
            "All actionable controls are reachable in a logical tab order.",
            "Keyboard handlers support expected keys without requiring pointer input.",
            "Keyboard-only tests cover the primary success path and escape or dismissal behavior.",
        ),
    ),
    "focus_states": (
        "Provide visible focus indicators and predictable focus management.",
        (
            "Focus indicators meet WCAG AA contrast and visibility expectations.",
            "Dialogs, menus, route changes, and validation errors move or restore focus intentionally.",
            "Regression coverage verifies focus order, focus trap, or focus restoration where applicable.",
        ),
    ),
    "screen_reader_labels": (
        "Expose meaningful accessible names, descriptions, and status announcements.",
        (
            "Controls, icons, images, and form fields have accessible names or alternatives.",
            "Dynamic content and errors are announced through appropriate live regions or descriptions.",
            "Screen-reader-oriented assertions verify labels, descriptions, and announcements.",
        ),
    ),
    "contrast": (
        "Verify color contrast for text, controls, focus indicators, and state changes.",
        (
            "Text and essential UI indicators meet WCAG AA contrast ratios.",
            "Information is not conveyed by color alone.",
            "Design tokens or visual tests prevent regressions for affected states.",
        ),
    ),
    "motion_preferences": (
        "Respect reduced-motion preferences for animations and transitions.",
        (
            "Motion-heavy effects are disabled, reduced, or replaced when reduced motion is requested.",
            "Auto-playing or looping motion can be paused, stopped, or avoided.",
            "Tests or implementation notes cover the reduced-motion branch.",
        ),
    ),
    "media_alternatives": (
        "Provide accessible alternatives for required audio, video, and image media.",
        (
            "Video or audio content has synchronized captions or an equivalent transcript.",
            "Meaningful images and media controls have text alternatives and accessible labels.",
            "Media QA verifies captions, transcripts, or audio descriptions in the target experience.",
        ),
    ),
    "automated_checks": (
        "Add automated accessibility checks to the implementation validation path.",
        (
            "The task includes an axe, pa11y, Lighthouse, jest-axe, or equivalent automated check.",
            "The automated check runs in CI or the task validation command for affected screens.",
            "Known false positives or exclusions are documented with owner-approved rationale.",
        ),
    ),
    "manual_assistive_qa": (
        "Validate the affected workflow with manual keyboard and assistive technology QA.",
        (
            "Manual QA covers keyboard-only operation across the primary workflow.",
            "Manual QA covers at least one target screen reader or platform assistive technology.",
            "Findings are recorded with pass/fail evidence and follow-up defects for blockers.",
        ),
    ),
}


@dataclass(frozen=True, slots=True)
class AccessibilityReadinessTask:
    """One generated implementation task for accessibility readiness."""

    category: AccessibilityReadinessCategory
    title: str
    description: str
    acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "title": self.title,
            "description": self.description,
            "acceptance_criteria": list(self.acceptance_criteria),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskAccessibilityReadinessRecord:
    """Accessibility readiness guidance for one execution task or requirement text."""

    task_id: str
    title: str
    detected_signals: tuple[AccessibilitySignal, ...]
    generated_tasks: tuple[AccessibilityReadinessTask, ...] = field(default_factory=tuple)
    readiness: AccessibilityReadinessLevel = "needs_planning"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[AccessibilitySignal, ...]:
        """Compatibility view for planners that name detected signals matched signals."""
        return self.detected_signals

    @property
    def recommended_tasks(self) -> tuple[AccessibilityReadinessTask, ...]:
        """Compatibility view for generated readiness tasks."""
        return self.generated_tasks

    @property
    def acceptance_criteria(self) -> tuple[str, ...]:
        """Flatten generated task acceptance criteria for simple consumers."""
        return tuple(criteria for task in self.generated_tasks for criteria in task.acceptance_criteria)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "generated_tasks": [task.to_dict() for task in self.generated_tasks],
            "readiness": self.readiness,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskAccessibilityReadinessPlan:
    """Plan-level accessibility readiness tasks."""

    plan_id: str | None = None
    records: tuple[TaskAccessibilityReadinessRecord, ...] = field(default_factory=tuple)
    accessibility_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskAccessibilityReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskAccessibilityReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    @property
    def impacted_task_ids(self) -> tuple[str, ...]:
        """Compatibility view matching planners that expose impacted task ids."""
        return self.accessibility_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "accessibility_task_ids": list(self.accessibility_task_ids),
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return accessibility readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render accessibility readiness guidance as deterministic Markdown."""
        title = "# Task Accessibility Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        category_counts = self.summary.get("generated_task_category_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Accessibility task count: {self.summary.get('accessibility_task_count', 0)}",
            f"- Generated readiness task count: {self.summary.get('generated_task_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
            "- Generated task counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task accessibility readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Generated Tasks | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            generated = "; ".join(f"{task.category}: {task.title}" for task in record.generated_tasks)
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(generated or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_accessibility_readiness_plan(source: Any) -> TaskAccessibilityReadinessPlan:
    """Build accessibility readiness records for task-shaped or requirement-text input."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                -len(record.generated_tasks),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    accessibility_task_ids = tuple(record.task_id for record in records)
    impacted = set(accessibility_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted
    )
    return TaskAccessibilityReadinessPlan(
        plan_id=plan_id,
        records=records,
        accessibility_task_ids=accessibility_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_accessibility_readiness(source: Any) -> TaskAccessibilityReadinessPlan:
    """Compatibility alias for building accessibility readiness plans."""
    return build_task_accessibility_readiness_plan(source)


def recommend_task_accessibility_readiness(source: Any) -> TaskAccessibilityReadinessPlan:
    """Compatibility alias for recommending accessibility readiness tasks."""
    return build_task_accessibility_readiness_plan(source)


def summarize_task_accessibility_readiness(source: Any) -> TaskAccessibilityReadinessPlan:
    """Compatibility alias for summarizing accessibility readiness plans."""
    if isinstance(source, TaskAccessibilityReadinessPlan):
        return source
    return build_task_accessibility_readiness_plan(source)


def generate_task_accessibility_readiness(source: Any) -> TaskAccessibilityReadinessPlan:
    """Compatibility alias for generating accessibility readiness plans."""
    return build_task_accessibility_readiness_plan(source)


def extract_task_accessibility_readiness(source: Any) -> TaskAccessibilityReadinessPlan:
    """Compatibility alias for extracting accessibility readiness plans."""
    return build_task_accessibility_readiness_plan(source)


def derive_task_accessibility_readiness(source: Any) -> TaskAccessibilityReadinessPlan:
    """Compatibility alias for deriving accessibility readiness plans."""
    return build_task_accessibility_readiness_plan(source)


def task_accessibility_readiness_plan_to_dict(result: TaskAccessibilityReadinessPlan) -> dict[str, Any]:
    """Serialize an accessibility readiness plan to a plain dictionary."""
    return result.to_dict()


task_accessibility_readiness_plan_to_dict.__test__ = False


def task_accessibility_readiness_plan_to_dicts(
    result: TaskAccessibilityReadinessPlan | Iterable[TaskAccessibilityReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize accessibility readiness records to plain dictionaries."""
    if isinstance(result, TaskAccessibilityReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_accessibility_readiness_plan_to_dicts.__test__ = False
task_accessibility_readiness_to_dicts = task_accessibility_readiness_plan_to_dicts
task_accessibility_readiness_to_dicts.__test__ = False


def task_accessibility_readiness_plan_to_markdown(result: TaskAccessibilityReadinessPlan) -> str:
    """Render an accessibility readiness plan as Markdown."""
    return result.to_markdown()


task_accessibility_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[AccessibilitySignal, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _task_record(task: Mapping[str, Any], index: int) -> TaskAccessibilityReadinessRecord | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None

    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    generated_tasks = _generated_tasks(title, signals)
    return TaskAccessibilityReadinessRecord(
        task_id=task_id,
        title=title,
        detected_signals=signals.signals,
        generated_tasks=generated_tasks,
        readiness=_readiness(signals.signals, generated_tasks),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[AccessibilitySignal] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(
        task.get("files_or_modules")
        or task.get("files")
        or task.get("expected_file_paths")
        or task.get("expected_files")
        or task.get("paths")
    ):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or _SIGNAL_PATTERNS[signal].search(searchable):
                signal_hits.add(signal)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        if _NO_ACCESSIBILITY_RE.search(text):
            explicitly_no_impact = True
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    if signal_hits & set(_SIGNAL_ORDER[1:]):
        signal_hits.add("accessibility")
    if "wcag_aa" in signal_hits:
        signal_hits.update(
            {
                "semantic_structure",
                "keyboard_access",
                "focus_states",
                "screen_reader_labels",
                "contrast",
                "motion_preferences",
                "automated_checks",
                "manual_assistive_qa",
            }
        )

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _generated_tasks(
    source_title: str,
    signals: _Signals,
) -> tuple[AccessibilityReadinessTask, ...]:
    categories: set[AccessibilityReadinessCategory] = {
        "semantic_structure",
        "keyboard_access",
        "focus_states",
        "screen_reader_labels",
        "contrast",
        "motion_preferences",
        "automated_checks",
        "manual_assistive_qa",
    }
    if "media_alternatives" in signals.signals:
        categories.add("media_alternatives")

    evidence = tuple(sorted(signals.evidence, key=_evidence_priority))[:3]
    rationale = "; ".join(evidence) if evidence else "Accessibility-related task context was detected."
    tasks: list[AccessibilityReadinessTask] = []
    for category in _CATEGORY_ORDER:
        if category not in categories:
            continue
        guidance, acceptance = _CATEGORY_GUIDANCE[category]
        tasks.append(
            AccessibilityReadinessTask(
                category=category,
                title=f"{_category_title(category)} for {source_title}",
                description=f"{guidance} Rationale: {rationale}",
                acceptance_criteria=acceptance,
                evidence=evidence,
            )
        )
    return tuple(tasks)


def _readiness(
    signals: tuple[AccessibilitySignal, ...],
    generated_tasks: tuple[AccessibilityReadinessTask, ...],
) -> AccessibilityReadinessLevel:
    categories = {task.category for task in generated_tasks}
    required = set(_CATEGORY_ORDER) - {"media_alternatives"}
    if required <= categories and {"automated_checks", "manual_assistive_qa"} <= set(signals):
        return "ready"
    if {"wcag_aa", "automated_checks", "manual_assistive_qa", "media_alternatives"} & set(signals):
        return "partial"
    return "needs_planning"


def _summary(
    records: tuple[TaskAccessibilityReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    generated_tasks = [task for record in records for task in record.generated_tasks]
    return {
        "task_count": task_count,
        "accessibility_task_count": len(records),
        "accessibility_task_ids": [record.task_id for record in records],
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
        "generated_task_count": len(generated_tasks),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "generated_task_category_counts": {
            category: sum(1 for task in generated_tasks if task.category == category)
            for category in _CATEGORY_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, str):
        text = _optional_text(source)
        if not text:
            return None, []
        return None, [{"id": "requirement-text", "title": "Accessibility requirements", "description": text}]
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
    if isinstance(value, str):
        text = _optional_text(value)
        return {"id": "requirement-text", "title": "Accessibility requirements", "description": text} if text else {}
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
        "expected_file_paths",
        "expected_files",
        "paths",
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
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
        "criteria",
        "definition_of_done",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
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
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values()])


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value) or _strings(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value) or _strings(metadata_value))
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
    path = value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
    return str(PurePosixPath(path)) if path else ""


def _category_title(category: AccessibilityReadinessCategory) -> str:
    return category.replace("_", " ").title()


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _evidence_priority(value: str) -> tuple[int, str]:
    if value.startswith("description:"):
        return (0, value)
    if value.startswith("metadata."):
        return (1, value)
    if value.startswith("acceptance_criteria"):
        return (2, value)
    return (3, value)


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
    "AccessibilityReadinessCategory",
    "AccessibilityReadinessLevel",
    "AccessibilityReadinessTask",
    "AccessibilitySignal",
    "TaskAccessibilityReadinessPlan",
    "TaskAccessibilityReadinessRecord",
    "analyze_task_accessibility_readiness",
    "build_task_accessibility_readiness_plan",
    "derive_task_accessibility_readiness",
    "extract_task_accessibility_readiness",
    "generate_task_accessibility_readiness",
    "recommend_task_accessibility_readiness",
    "summarize_task_accessibility_readiness",
    "task_accessibility_readiness_plan_to_dict",
    "task_accessibility_readiness_plan_to_dicts",
    "task_accessibility_readiness_plan_to_markdown",
    "task_accessibility_readiness_to_dicts",
]
