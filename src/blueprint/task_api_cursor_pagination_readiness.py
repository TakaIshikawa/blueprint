"""Plan cursor-based API pagination readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CursorPaginationSignal = Literal[
    "cursor_token",
    "next_previous_links",
    "stable_ordering",
    "page_size_limits",
    "filtering_interaction",
    "deleted_row_handling",
    "backward_pagination",
]
CursorPaginationSafeguard = Literal[
    "deterministic_sort_tests",
    "cursor_expiry_handling",
    "malformed_cursor_tests",
    "max_page_size_enforcement",
    "cursor_documentation",
]
CursorPaginationReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[CursorPaginationReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[CursorPaginationSignal, ...] = (
    "cursor_token",
    "next_previous_links",
    "stable_ordering",
    "page_size_limits",
    "filtering_interaction",
    "deleted_row_handling",
    "backward_pagination",
)
_SAFEGUARD_ORDER: tuple[CursorPaginationSafeguard, ...] = (
    "deterministic_sort_tests",
    "cursor_expiry_handling",
    "malformed_cursor_tests",
    "max_page_size_enforcement",
    "cursor_documentation",
)
_SIGNAL_PATTERNS: dict[CursorPaginationSignal, re.Pattern[str]] = {
    "cursor_token": re.compile(
        r"\b(?:cursor|cursors|cursor token|cursor-based|cursor pagination|opaque cursor|"
        r"continuation token|page token|next_token|nextToken|after cursor|before cursor)\b",
        re.I,
    ),
    "next_previous_links": re.compile(
        r"\b(?:next links?|previous links?|prev links?|next url|previous url|prev url|next page link|"
        r"previous page link|pagination links?|next/previous|next/prev|has_more|hasMore)\b",
        re.I,
    ),
    "stable_ordering": re.compile(
        r"\b(?:stable order|stable ordering|stable sort|deterministic order|deterministic ordering|"
        r"deterministic sort|consistent order|order by|sort by|created_at|updated_at|tie[- ]?breaker|"
        r"secondary sort|compound sort|multi-column sort)\b",
        re.I,
    ),
    "page_size_limits": re.compile(
        r"\b(?:page size|page_size|per_page|limit|max page size|max_page_size|min page size|"
        r"min_page_size|default limit|page limit|bounded limit|page size cap|limit cap)\b",
        re.I,
    ),
    "filtering_interaction": re.compile(
        r"\b(?:filter|filters|filtering|where clause|query param|search|search param|"
        r"filter param|filter criteria|filter interaction|filter and cursor|cursor and filter)\b",
        re.I,
    ),
    "deleted_row_handling": re.compile(
        r"\b(?:deleted row|soft delete|tombstone|deleted record|deleted item|missing row|"
        r"missing record|row deletion|record deletion|deleted entity|deleted entities)\b",
        re.I,
    ),
    "backward_pagination": re.compile(
        r"\b(?:backward paginat|reverse paginat|previous page|prev page|before cursor|"
        r"backwards|bi-directional|bidirectional paginat|forward and backward)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[CursorPaginationSignal, re.Pattern[str]] = {
    "cursor_token": re.compile(r"cursor|token|continuation", re.I),
    "next_previous_links": re.compile(r"next|prev(?:ious)?|links?|paginat", re.I),
    "stable_ordering": re.compile(r"sort|order|determin", re.I),
    "page_size_limits": re.compile(r"page[_-]?size|limit|per[_-]?page", re.I),
    "filtering_interaction": re.compile(r"filter|query|search", re.I),
    "deleted_row_handling": re.compile(r"delet|tombstone|soft[_-]?delet", re.I),
    "backward_pagination": re.compile(r"backward|reverse|prev(?:ious)?|bidirection", re.I),
}
_SAFEGUARD_PATTERNS: dict[CursorPaginationSafeguard, re.Pattern[str]] = {
    "deterministic_sort_tests": re.compile(
        r"\b(?:(?:deterministic|stable|consistent).{0,80}(?:sort|order).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:deterministic|stable|consistent).{0,80}(?:sort|order)|"
        r"sort tests?|ordering tests?|tie[- ]?breaker tests?)\b",
        re.I,
    ),
    "cursor_expiry_handling": re.compile(
        r"\b(?:cursor expir(?:y|ation|es|ing|ed)?|token expir(?:y|ation|es|ing|ed)?|expired cursor|expired token|stale cursor|stale token|"
        r"cursor ttl|token ttl|cursor timeout|token timeout|cursor lifetime|token lifetime)\b",
        re.I,
    ),
    "malformed_cursor_tests": re.compile(
        r"\b(?:(?:malformed|invalid|corrupt|bad).{0,80}(?:cursor|token).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:malformed|invalid|corrupt|bad).{0,80}(?:cursor|token)|"
        r"cursor validation|token validation|cursor decode|token decode)\b",
        re.I,
    ),
    "max_page_size_enforcement": re.compile(
        r"\b(?:enforce|enforcement|validate|validation).{0,80}(?:max|maximum).{0,80}(?:page size|limit|per_page)|"
        r"(?:max|maximum).{0,80}(?:page size|limit|per_page).{0,80}(?:enforce|enforcement|validate|validation)|"
        r"page size cap|limit cap|bounded limit|page size bound\b",
        re.I,
    ),
    "cursor_documentation": re.compile(
        r"\b(?:cursor.{0,80}(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?)|"
        r"(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?).{0,80}cursor|"
        r"pagination.{0,80}(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?)|"
        r"(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?).{0,80}pagination)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:cursor|pagination|paginat)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[CursorPaginationSafeguard, str] = {
    "deterministic_sort_tests": "Add tests verifying deterministic sort order with tie-breakers to prevent missing or duplicate records across pages.",
    "cursor_expiry_handling": "Define cursor or token expiry handling, TTL behavior, and expired-cursor error responses.",
    "malformed_cursor_tests": "Add tests for malformed, invalid, corrupt, or tampered cursor tokens with proper validation and error responses.",
    "max_page_size_enforcement": "Enforce maximum page size limits, validate page size parameters, and return bounded limit errors for oversized requests.",
    "cursor_documentation": "Document cursor encoding, pagination usage, cursor expiry, page size limits, and client integration examples.",
}


@dataclass(frozen=True, slots=True)
class TaskApiCursorPaginationReadinessFinding:
    """API cursor pagination readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[CursorPaginationSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[CursorPaginationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[CursorPaginationSafeguard, ...] = field(default_factory=tuple)
    readiness: CursorPaginationReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actionable_remediations: tuple[str, ...] = field(default_factory=tuple)

    @property
    def actionable_gaps(self) -> tuple[str, ...]:
        """Compatibility view for readiness modules that expose gaps."""
        return self.actionable_remediations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "actionable_remediations": list(self.actionable_remediations),
        }


@dataclass(frozen=True, slots=True)
class TaskApiCursorPaginationReadinessPlan:
    """Plan-level API cursor pagination readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiCursorPaginationReadinessFinding, ...] = field(default_factory=tuple)
    cursor_pagination_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiCursorPaginationReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "cursor_pagination_task_ids": list(self.cursor_pagination_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    def to_markdown(self) -> str:
        """Render cursor pagination readiness as deterministic Markdown."""
        title = "# Task API Cursor Pagination Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('total_task_count', 0)}",
            f"- Cursor pagination task count: {self.summary.get('cursor_pagination_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: " + ", ".join(f"{readiness} {readiness_counts.get(readiness, 0)}" for readiness in _READINESS_ORDER),
        ]
        if not self.findings:
            lines.extend(["", "No cursor pagination readiness findings were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Present Safeguards | Missing Safeguards | Remediation | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for finding in self.findings:
            lines.append(
                "| "
                f"`{_markdown_cell(finding.task_id)}` | "
                f"{_markdown_cell(finding.title)} | "
                f"{finding.readiness} | "
                f"{_markdown_cell(', '.join(finding.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.actionable_remediations) or 'none')} | "
                f"{_markdown_cell('; '.join(finding.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_api_cursor_pagination_readiness_plan(source: Any) -> TaskApiCursorPaginationReadinessPlan:
    """Build API cursor pagination readiness findings for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_finding_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    findings = tuple(
        sorted(
            (finding for finding in candidates if finding is not None),
            key=lambda finding: (_READINESS_ORDER[finding.readiness], finding.task_id, finding.title.casefold()),
        )
    )
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskApiCursorPaginationReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        cursor_pagination_task_ids=tuple(finding.task_id for finding in findings),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_api_cursor_pagination_readiness(source: Any) -> TaskApiCursorPaginationReadinessPlan:
    """Compatibility alias for building API cursor pagination readiness plans."""
    return build_task_api_cursor_pagination_readiness_plan(source)


def summarize_task_api_cursor_pagination_readiness(source: Any) -> TaskApiCursorPaginationReadinessPlan:
    """Compatibility alias for building API cursor pagination readiness plans."""
    return build_task_api_cursor_pagination_readiness_plan(source)


def extract_task_api_cursor_pagination_readiness(source: Any) -> TaskApiCursorPaginationReadinessPlan:
    """Compatibility alias for extracting API cursor pagination readiness plans."""
    return build_task_api_cursor_pagination_readiness_plan(source)


def generate_task_api_cursor_pagination_readiness(source: Any) -> TaskApiCursorPaginationReadinessPlan:
    """Compatibility alias for generating API cursor pagination readiness plans."""
    return build_task_api_cursor_pagination_readiness_plan(source)


def task_api_cursor_pagination_readiness_plan_to_dict(result: TaskApiCursorPaginationReadinessPlan) -> dict[str, Any]:
    """Serialize an API cursor pagination readiness plan to a plain dictionary."""
    return result.to_dict()


task_api_cursor_pagination_readiness_plan_to_dict.__test__ = False


def task_api_cursor_pagination_readiness_plan_to_dicts(
    result: TaskApiCursorPaginationReadinessPlan | Iterable[TaskApiCursorPaginationReadinessFinding],
) -> list[dict[str, Any]]:
    """Serialize API cursor pagination readiness findings to plain dictionaries."""
    if isinstance(result, TaskApiCursorPaginationReadinessPlan):
        return result.to_dicts()
    return [finding.to_dict() for finding in result]


task_api_cursor_pagination_readiness_plan_to_dicts.__test__ = False


def task_api_cursor_pagination_readiness_plan_to_markdown(result: TaskApiCursorPaginationReadinessPlan) -> str:
    """Render an API cursor pagination readiness plan as Markdown."""
    return result.to_markdown()


task_api_cursor_pagination_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[CursorPaginationSignal, ...] = field(default_factory=tuple)
    safeguards: tuple[CursorPaginationSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskApiCursorPaginationReadinessFinding | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None

    missing: tuple[CursorPaginationSafeguard, ...] = tuple(
        safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.safeguards
    )
    task_id = _task_id(task, index)
    return TaskApiCursorPaginationReadinessFinding(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        detected_signals=signals.signals,
        present_safeguards=signals.safeguards,
        missing_safeguards=missing,
        readiness=_readiness_level(missing),
        evidence=signals.evidence,
        actionable_remediations=tuple(_REMEDIATIONS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[CursorPaginationSignal] = set()
    safeguard_hits: set[CursorPaginationSafeguard] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        matched = False
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        matched = False
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _readiness_level(
    missing: tuple[CursorPaginationSafeguard, ...],
) -> CursorPaginationReadiness:
    if not missing:
        return "strong"
    missing_set = set(missing)
    critical = {"deterministic_sort_tests", "cursor_expiry_handling", "malformed_cursor_tests"}
    # Weak if missing 4 or more safeguards, or missing all 3 critical safeguards
    if len(missing) >= 4:
        return "weak"
    if len(critical & missing_set) >= 3:
        return "weak"
    return "partial"


def _summary(
    findings: tuple[TaskApiCursorPaginationReadinessFinding, ...],
    *,
    total_task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "cursor_pagination_task_count": len(findings),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(finding.missing_safeguards) for finding in findings),
        "readiness_counts": {
            readiness: sum(1 for finding in findings if finding.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


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
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
        "validation_commands",
        "test_commands",
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
    return str(PurePosixPath(value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")))


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> tuple[_T, ...]:
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
    return tuple(deduped)


__all__ = [
    "CursorPaginationReadiness",
    "CursorPaginationSafeguard",
    "CursorPaginationSignal",
    "TaskApiCursorPaginationReadinessFinding",
    "TaskApiCursorPaginationReadinessPlan",
    "analyze_task_api_cursor_pagination_readiness",
    "build_task_api_cursor_pagination_readiness_plan",
    "extract_task_api_cursor_pagination_readiness",
    "generate_task_api_cursor_pagination_readiness",
    "summarize_task_api_cursor_pagination_readiness",
    "task_api_cursor_pagination_readiness_plan_to_dict",
    "task_api_cursor_pagination_readiness_plan_to_dicts",
    "task_api_cursor_pagination_readiness_plan_to_markdown",
]
