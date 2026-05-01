"""Build localization and internationalization readiness matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


LocalizationReadinessCategory = Literal[
    "locale_copy",
    "translation",
    "date_time_currency_formatting",
    "rtl_layout",
    "pluralization",
    "regional_compliance_copy",
    "localized_qa",
]
LocalizationReadinessSeverity = Literal["critical", "high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_CATEGORY_ORDER: dict[LocalizationReadinessCategory, int] = {
    "locale_copy": 0,
    "translation": 1,
    "date_time_currency_formatting": 2,
    "rtl_layout": 3,
    "pluralization": 4,
    "regional_compliance_copy": 5,
    "localized_qa": 6,
}
_SEVERITY_ORDER: dict[LocalizationReadinessSeverity, int] = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}
_TEXT_PATTERNS: dict[LocalizationReadinessCategory, re.Pattern[str]] = {
    "locale_copy": re.compile(
        r"\b(?:localized? copy|localised? copy|locale copy|locale-specific copy|"
        r"market copy|language copy|copy deck|microcopy|user-facing copy|ui copy|"
        r"email copy|notification copy|content strings?)\b",
        re.I,
    ),
    "translation": re.compile(
        r"\b(?:translation|translations|translate|translated|translator|"
        r"localization|localisation|l10n|i18n|internationalization|internationalisation|"
        r"locale|locales|language switcher|resource bundle|message catalog)\b",
        re.I,
    ),
    "date_time_currency_formatting": re.compile(
        r"\b(?:date format|time format|datetime|time zone|timezone|tz|locale format|"
        r"number format|currency|money|price|amount|decimal separator|thousand separator|"
        r"intl\.|toLocaleString|strftime|relative time)\b",
        re.I,
    ),
    "rtl_layout": re.compile(
        r"\b(?:rtl|right-to-left|right to left|bidirectional|bidi|dir=|directionality|"
        r"arabic|hebrew|farsi|persian|urdu|mirrored layout)\b",
        re.I,
    ),
    "pluralization": re.compile(
        r"\b(?:plural|plurals|pluralization|pluralisation|singular|count-aware|"
        r"icu message|messageformat|zero state|one item|many items)\b",
        re.I,
    ),
    "regional_compliance_copy": re.compile(
        r"\b(?:regional compliance|market compliance|region-specific disclosure|"
        r"localized disclosure|localised disclosure|legal copy|terms copy|privacy notice|"
        r"cookie banner|consent copy|tax disclosure|vat|gdpr|ccpa|cpra|impressum)\b",
        re.I,
    ),
    "localized_qa": re.compile(
        r"\b(?:localized qa|localised qa|lqa|linguistic qa|locale qa|translation qa|"
        r"pseudo[- ]?localization|pseudo[- ]?localisation|screenshot review|"
        r"copy expansion|locale regression|in-country review)\b",
        re.I,
    ),
}
_SEVERITY_BY_CATEGORY: dict[LocalizationReadinessCategory, LocalizationReadinessSeverity] = {
    "regional_compliance_copy": "critical",
    "rtl_layout": "high",
    "pluralization": "high",
    "date_time_currency_formatting": "high",
    "translation": "medium",
    "locale_copy": "medium",
    "localized_qa": "low",
}


@dataclass(frozen=True, slots=True)
class PlanLocalizationReadinessRow:
    """One localization readiness row for a task and readiness category."""

    task_id: str
    category: LocalizationReadinessCategory
    severity: LocalizationReadinessSeverity
    required_artifacts: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "category": self.category,
            "severity": self.severity,
            "required_artifacts": list(self.required_artifacts),
            "evidence": list(self.evidence),
            "follow_up_questions": list(self.follow_up_questions),
        }


@dataclass(frozen=True, slots=True)
class PlanLocalizationReadinessMatrix:
    """Plan-level localization readiness matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanLocalizationReadinessRow, ...] = field(default_factory=tuple)
    localized_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "localized_task_ids": list(self.localized_task_ids),
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return localization readiness rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Localization Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('localized_task_count', 0)} localization tasks "
                f"(critical: {counts.get('critical', 0)}, high: {counts.get('high', 0)}, "
                f"medium: {counts.get('medium', 0)}, low: {counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No localization readiness rows were inferred."])
            if self.no_signal_task_ids:
                lines.extend(
                    [
                        "",
                        f"No localization signals: {_markdown_cell(', '.join(self.no_signal_task_ids))}",
                    ]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Category | Severity | Required Artifacts | Evidence | Follow-up Questions |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{row.category} | "
                f"{row.severity} | "
                f"{_markdown_cell('; '.join(row.required_artifacts))} | "
                f"{_markdown_cell('; '.join(row.evidence))} | "
                f"{_markdown_cell('; '.join(row.follow_up_questions))} |"
            )
        if self.no_signal_task_ids:
            lines.extend(
                [
                    "",
                    f"No localization signals: {_markdown_cell(', '.join(self.no_signal_task_ids))}",
                ]
            )
        return "\n".join(lines)


def build_plan_localization_readiness_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanLocalizationReadinessMatrix:
    """Build localization and internationalization readiness rows for an execution plan."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_rows(task, index) for index, task in enumerate(tasks, start=1)]
    rows = tuple(row for task_rows in candidates for row in task_rows)
    localized_task_ids = tuple(_dedupe(row.task_id for row in rows))
    no_signal_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if not candidates[index - 1]
    )
    severity_counts = {
        severity: sum(1 for row in rows if row.severity == severity) for severity in _SEVERITY_ORDER
    }
    category_counts = {
        category: sum(1 for row in rows if row.category == category) for category in _CATEGORY_ORDER
    }
    return PlanLocalizationReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        localized_task_ids=localized_task_ids,
        no_signal_task_ids=no_signal_task_ids,
        summary={
            "task_count": len(tasks),
            "localized_task_count": len(localized_task_ids),
            "no_signal_task_count": len(no_signal_task_ids),
            "severity_counts": severity_counts,
            "category_counts": category_counts,
        },
    )


def plan_localization_readiness_matrix_to_dict(
    result: PlanLocalizationReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a localization readiness matrix to a plain dictionary."""
    return result.to_dict()


plan_localization_readiness_matrix_to_dict.__test__ = False


def plan_localization_readiness_matrix_to_markdown(
    result: PlanLocalizationReadinessMatrix,
) -> str:
    """Render a localization readiness matrix as Markdown."""
    return result.to_markdown()


plan_localization_readiness_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    categories: tuple[LocalizationReadinessCategory, ...] = field(default_factory=tuple)
    evidence_by_category: Mapping[LocalizationReadinessCategory, tuple[str, ...]] = field(
        default_factory=dict
    )


def _task_rows(task: Mapping[str, Any], index: int) -> tuple[PlanLocalizationReadinessRow, ...]:
    task_id = _task_id(task, index)
    signals = _signals(task)
    return tuple(
        PlanLocalizationReadinessRow(
            task_id=task_id,
            category=category,
            severity=_SEVERITY_BY_CATEGORY[category],
            required_artifacts=_required_artifacts(category),
            evidence=signals.evidence_by_category.get(category, ()),
            follow_up_questions=_follow_up_questions(category),
        )
        for category in signals.categories
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    categories: set[LocalizationReadinessCategory] = set()
    evidence: dict[LocalizationReadinessCategory, list[str]] = {
        category: [] for category in _CATEGORY_ORDER
    }

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        for category in _path_categories(normalized):
            categories.add(category)
            evidence[category].append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        for category, pattern in _TEXT_PATTERNS.items():
            if pattern.search(text):
                categories.add(category)
                evidence[category].append(snippet)

    return _Signals(
        categories=tuple(category for category in _CATEGORY_ORDER if category in categories),
        evidence_by_category={
            category: tuple(_dedupe(evidence[category]))
            for category in _CATEGORY_ORDER
            if category in categories
        },
    )


def _path_categories(path: str) -> set[LocalizationReadinessCategory]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    suffix = posix.suffix
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    categories: set[LocalizationReadinessCategory] = set()
    if bool({"copy", "content", "emails", "notifications", "messages"} & parts) or "copy" in name:
        categories.add("locale_copy")
    if bool(
        {"i18n", "l10n", "locales", "locale", "translations", "translation", "lang"} & parts
    ) or suffix in {
        ".po",
        ".pot",
        ".mo",
        ".xliff",
        ".xlf",
    }:
        categories.add("translation")
    if any(
        token in text
        for token in (
            "currency",
            "money",
            "price",
            "date format",
            "datetime",
            "timezone",
            "time zone",
        )
    ):
        categories.add("date_time_currency_formatting")
    if any(
        token in text
        for token in ("rtl", "right to left", "right-to-left", "bidi", "arabic", "hebrew")
    ):
        categories.add("rtl_layout")
    if any(token in text for token in ("plural", "icu", "messageformat")):
        categories.add("pluralization")
    if any(
        token in text
        for token in (
            "gdpr",
            "ccpa",
            "consent",
            "privacy notice",
            "legal",
            "disclosure",
            "tax",
            "vat",
        )
    ):
        categories.add("regional_compliance_copy")
    if any(
        token in text
        for token in ("lqa", "localized qa", "pseudo", "screenshot review", "locale regression")
    ):
        categories.add("localized_qa")
    return categories


def _required_artifacts(category: LocalizationReadinessCategory) -> tuple[str, ...]:
    return _REQUIRED_ARTIFACTS[category]


def _follow_up_questions(category: LocalizationReadinessCategory) -> tuple[str, ...]:
    return _FOLLOW_UP_QUESTIONS[category]


_REQUIRED_ARTIFACTS: dict[LocalizationReadinessCategory, tuple[str, ...]] = {
    "locale_copy": (
        "Approved source copy with string keys, context notes, character limits, and screenshot references.",
        "Locale ownership and fallback behavior for every changed user-facing string.",
    ),
    "translation": (
        "Updated translation resources or vendor handoff package with stable keys and context.",
        "Fallback locale and missing-translation handling documented for changed flows.",
    ),
    "date_time_currency_formatting": (
        "Locale-aware date, time, number, and currency formatting examples for target locales.",
        "Timezone, rounding, currency-code, and parsing expectations documented for changed data.",
    ),
    "rtl_layout": (
        "RTL screenshots or layout review for affected views, including mirrored icons and directional spacing.",
        "Bidirectional text handling documented for mixed-language content and user input.",
    ),
    "pluralization": (
        "Plural rule coverage for zero, one, few, many, and other forms required by target locales.",
        "ICU or equivalent message fixtures with count-variable examples.",
    ),
    "regional_compliance_copy": (
        "Legal or policy approval for region-specific notices, consent, tax, privacy, or terms copy.",
        "Market applicability and locale fallback rules for regulated copy.",
    ),
    "localized_qa": (
        "Localized QA checklist covering pseudo-localization, copy expansion, screenshots, and regression scope.",
        "Named locale reviewers, test locales, devices, and sign-off evidence.",
    ),
}
_FOLLOW_UP_QUESTIONS: dict[LocalizationReadinessCategory, tuple[str, ...]] = {
    "locale_copy": (
        "Which locales need copy review before launch, and who approves source-string context?",
    ),
    "translation": (
        "Are translation keys frozen, and what is the cutoff for vendor or in-house localization?",
    ),
    "date_time_currency_formatting": (
        "Which locales, currencies, timezones, and numeric formats must be validated for this launch?",
    ),
    "rtl_layout": (
        "Which RTL locales are in scope, and are mirrored layouts validated with real translated content?",
    ),
    "pluralization": (
        "Which count ranges and locale plural forms need fixtures and translator context?",
    ),
    "regional_compliance_copy": (
        "Which regions require legal approval or alternate disclosure copy before release?",
    ),
    "localized_qa": (
        "Who owns localized QA sign-off, and which pseudo-localized or translated builds are required?",
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
    for field_name in (
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if _any_signal(key_text):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _any_signal(key_text):
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


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _TEXT_PATTERNS.values())


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
    "LocalizationReadinessCategory",
    "LocalizationReadinessSeverity",
    "PlanLocalizationReadinessMatrix",
    "PlanLocalizationReadinessRow",
    "build_plan_localization_readiness_matrix",
    "plan_localization_readiness_matrix_to_dict",
    "plan_localization_readiness_matrix_to_markdown",
]
