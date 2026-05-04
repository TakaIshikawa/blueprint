"""Identify task-level API pagination and cursor impact risks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar, cast

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


PaginationSignal = Literal[
    "list_endpoint",
    "cursor_pagination",
    "offset_pagination",
    "page_size_limit",
    "next_token",
    "infinite_scroll",
    "bulk_listing",
]
PaginationImpactLevel = Literal["high", "medium", "low"]
PaginationSafeguard = Literal[
    "stable_sort_order",
    "cursor_compatibility",
    "max_page_size_handling",
    "empty_page_behavior",
    "backwards_compatibility",
    "boundary_page_tests",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: dict[PaginationSignal, int] = {
    "list_endpoint": 0,
    "cursor_pagination": 1,
    "offset_pagination": 2,
    "page_size_limit": 3,
    "next_token": 4,
    "infinite_scroll": 5,
    "bulk_listing": 6,
}
_IMPACT_ORDER: dict[PaginationImpactLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SAFEGUARD_ORDER: dict[PaginationSafeguard, int] = {
    "stable_sort_order": 0,
    "cursor_compatibility": 1,
    "max_page_size_handling": 2,
    "empty_page_behavior": 3,
    "backwards_compatibility": 4,
    "boundary_page_tests": 5,
}
_SIGNAL_PATTERNS: dict[PaginationSignal, re.Pattern[str]] = {
    "list_endpoint": re.compile(
        r"\b(?:list endpoint|listing endpoint|list api|listing api|collection endpoint|"
        r"index endpoint|GET\s+/[^\s]+|list (?:users|orders|items|records|resources|"
        r"customers|tasks|events)|bulk listing api)\b",
        re.I,
    ),
    "cursor_pagination": re.compile(
        r"\b(?:cursor pagination|cursor-based pagination|cursor based pagination|cursor\b|"
        r"after cursor|before cursor|opaque cursor)\b",
        re.I,
    ),
    "offset_pagination": re.compile(
        r"\b(?:offset pagination|offset\b|limit/offset|limit and offset|page number|"
        r"page=\d*|page parameter)\b",
        re.I,
    ),
    "page_size_limit": re.compile(
        r"\b(?:page size|page-size|page_size|per_page|per page|limit parameter|"
        r"max(?:imum)? limit|max(?:imum)? page size|default page size)\b",
        re.I,
    ),
    "next_token": re.compile(
        r"\b(?:next token|next_token|next page token|next_page_token|page token|"
        r"page_token|continuation token|pagination token)\b",
        re.I,
    ),
    "infinite_scroll": re.compile(
        r"\b(?:infinite scroll|infinite-scroll|endless scroll|load more|feed backend)\b",
        re.I,
    ),
    "bulk_listing": re.compile(
        r"\b(?:bulk list|bulk listing|bulk listing api|bulk export|bulk download|"
        r"export all|list all|full export|data export|backfill listing)\b",
        re.I,
    ),
}
_API_RE = re.compile(
    r"\b(?:api|apis|endpoint|endpoints|route|routes|rest|http|graphql|openapi|"
    r"controller|handler|client|sdk|webhook|request|response)\b",
    re.I,
)
_CLIENT_FACING_RE = re.compile(
    r"\b(?:public api|external api|partner api|customer[- ]facing|client[- ]facing|"
    r"mobile clients?|sdk|third[- ]party|existing clients?|consumers?|integrations?)\b",
    re.I,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrate|backwards? compatible|compatibility|non[- ]breaking|"
    r"breaking|deprecation|existing clients?|legacy|old cursor|previous cursor|rollout)\b",
    re.I,
)
_BULK_EXPORT_RE = re.compile(
    r"\b(?:bulk|export|download|backfill|sync|all records|millions|large dataset)\b",
    re.I,
)
_SAFEGUARD_PATTERNS: dict[PaginationSafeguard, re.Pattern[str]] = {
    "stable_sort_order": re.compile(
        r"\b(?:stable sort|deterministic sort|stable order|consistent order|"
        r"sort order|ordering|order by|created_at|id tie[- ]?breaker)\b",
        re.I,
    ),
    "cursor_compatibility": re.compile(
        r"\b(?:cursor compat|compatible cursor|old cursor|legacy cursor|versioned cursor|"
        r"opaque cursor|cursor version|cursor migration)\b",
        re.I,
    ),
    "max_page_size_handling": re.compile(
        r"\b(?:max(?:imum)? page size|max(?:imum)? limit|clamp|cap page size|"
        r"page size limit|default page size|limit boundary)\b",
        re.I,
    ),
    "empty_page_behavior": re.compile(
        r"\b(?:empty page|empty result|empty results|no results|last page|end of list|"
        r"exhausted cursor|has_more false|no next token)\b",
        re.I,
    ),
    "backwards_compatibility": re.compile(
        r"\b(?:backwards? compatible|compatibility|non[- ]breaking|existing clients?|"
        r"legacy clients?|deprecation|versioning|old response)\b",
        re.I,
    ),
    "boundary_page_tests": re.compile(
        r"\b(?:boundary page|page boundary|first page|last page|boundary tests?|"
        r"page size tests?|limit tests?|offset boundary|cursor boundary)\b",
        re.I,
    ),
}
_SIGNAL_KEYS = (
    "pagination_signals",
    "api_pagination_signals",
    "pagination_categories",
    "impact_signals",
    "signals",
)
_SAFEGUARD_KEYS = (
    "pagination_safeguards",
    "safeguards",
    "recommended_safeguards",
    "checks",
    "test_coverage",
)
_CHECKS: dict[PaginationSafeguard, str] = {
    "stable_sort_order": "Verify stable, deterministic ordering with a unique tie-breaker across pages.",
    "cursor_compatibility": "Verify cursor or next-token compatibility for existing clients and deployed cursors.",
    "max_page_size_handling": "Verify default, minimum, maximum, and oversized page-size boundary behavior.",
    "empty_page_behavior": "Verify empty result sets, exhausted cursors, and final-page responses.",
    "backwards_compatibility": "Verify response shape and pagination parameters remain compatible for existing clients.",
    "boundary_page_tests": "Add tests for first page, middle page, last page, and page-size boundary pages.",
}


@dataclass(frozen=True, slots=True)
class TaskApiPaginationImpactRecord:
    """Pagination guidance for one execution task."""

    task_id: str
    title: str
    matched_signals: tuple[PaginationSignal, ...]
    impact_level: PaginationImpactLevel
    missing_safeguards: tuple[PaginationSafeguard, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_signals": list(self.matched_signals),
            "impact_level": self.impact_level,
            "missing_safeguards": list(self.missing_safeguards),
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskApiPaginationImpactPlan:
    """Plan-level API pagination and cursor impact review."""

    plan_id: str | None = None
    records: tuple[TaskApiPaginationImpactRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskApiPaginationImpactRecord, ...]:
        """Compatibility view matching planners that name task records findings."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return API pagination impact records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the API pagination impact plan as deterministic Markdown."""
        title = "# Task API Pagination Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        signal_counts = self.summary.get("signal_counts", {})
        impact_counts = self.summary.get("impact_level_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('impacted_task_count', 0)} impacted tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(no impact: {self.summary.get('no_impact_task_count', 0)})."
            ),
            (
                "Signals: "
                f"list_endpoint {signal_counts.get('list_endpoint', 0)}, "
                f"cursor_pagination {signal_counts.get('cursor_pagination', 0)}, "
                f"offset_pagination {signal_counts.get('offset_pagination', 0)}, "
                f"page_size_limit {signal_counts.get('page_size_limit', 0)}, "
                f"next_token {signal_counts.get('next_token', 0)}, "
                f"infinite_scroll {signal_counts.get('infinite_scroll', 0)}, "
                f"bulk_listing {signal_counts.get('bulk_listing', 0)}."
            ),
            (
                "Impact: "
                f"high {impact_counts.get('high', 0)}, "
                f"medium {impact_counts.get('medium', 0)}, "
                f"low {impact_counts.get('low', 0)}."
            ),
        ]
        if not self.records:
            lines.extend(["", "No API pagination impact records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Impact | Matched Signals | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.impact_level} | "
                f"{_markdown_cell(', '.join(record.matched_signals))} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_api_pagination_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskApiPaginationImpactPlan:
    """Build task-level API pagination and cursor impact guidance."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _record(task, index)) is not None
            ),
            key=lambda record: (
                _IMPACT_ORDER[record.impact_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    impacted_task_ids = tuple(record.task_id for record in records)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in impacted_task_ids
    )
    signal_counts = {
        signal: sum(1 for record in records if signal in record.matched_signals)
        for signal in _SIGNAL_ORDER
    }
    impact_counts = {
        impact: sum(1 for record in records if record.impact_level == impact)
        for impact in _IMPACT_ORDER
    }

    return TaskApiPaginationImpactPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=impacted_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary={
            "task_count": len(tasks),
            "record_count": len(records),
            "impacted_task_count": len(impacted_task_ids),
            "no_impact_task_count": len(no_impact_task_ids),
            "signal_counts": signal_counts,
            "impact_level_counts": impact_counts,
            "impacted_task_ids": list(impacted_task_ids),
            "no_impact_task_ids": list(no_impact_task_ids),
        },
    )


def derive_task_api_pagination_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskApiPaginationImpactPlan:
    """Compatibility alias for building API pagination impact plans."""
    return build_task_api_pagination_impact_plan(source)


def summarize_task_api_pagination_impact(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskApiPaginationImpactPlan:
    """Compatibility alias matching other task-level planners."""
    return build_task_api_pagination_impact_plan(source)


def summarize_task_api_pagination_impacts(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskApiPaginationImpactPlan:
    """Compatibility alias matching plural task impact helper names."""
    return build_task_api_pagination_impact_plan(source)


def task_api_pagination_impact_plan_to_dict(
    result: TaskApiPaginationImpactPlan,
) -> dict[str, Any]:
    """Serialize an API pagination impact plan to a plain dictionary."""
    return result.to_dict()


task_api_pagination_impact_plan_to_dict.__test__ = False


def task_api_pagination_impact_plan_to_markdown(
    result: TaskApiPaginationImpactPlan,
) -> str:
    """Render an API pagination impact plan as Markdown."""
    return result.to_markdown()


task_api_pagination_impact_plan_to_markdown.__test__ = False


def _record(task: Mapping[str, Any], index: int) -> TaskApiPaginationImpactRecord | None:
    signals, evidence = _signals(task)
    if not signals:
        return None

    context = _context(task)
    task_id = _task_id(task, index)
    safeguards = _present_safeguards(task)
    missing = tuple(
        safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in safeguards
    )
    return TaskApiPaginationImpactRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        matched_signals=signals,
        impact_level=_impact_level(signals, context),
        missing_safeguards=missing,
        recommended_checks=tuple(_CHECKS[safeguard] for safeguard in _SAFEGUARD_ORDER),
        evidence=evidence,
    )


def _signals(task: Mapping[str, Any]) -> tuple[tuple[PaginationSignal, ...], tuple[str, ...]]:
    signals: set[PaginationSignal] = set()
    evidence: list[str] = []
    metadata = task.get("metadata")

    if isinstance(metadata, Mapping):
        for signal in _metadata_signals(metadata):
            signals.add(signal)
            evidence.append(f"metadata.pagination_signals: {signal}")

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_signals = _path_signals(normalized, path_text)
        if path_signals:
            signals.update(path_signals)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        matched = _text_signals(text)
        if matched:
            signals.update(matched)
            evidence.append(_evidence_snippet(source_field, text))

    for command in _validation_commands(task):
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = {*_text_signals(command), *_text_signals(command_text)}
        if matched:
            signals.update(matched)
            evidence.append(_evidence_snippet("validation_commands", command))

    return (
        tuple(signal for signal in _SIGNAL_ORDER if signal in signals),
        tuple(_dedupe(evidence)),
    )


def _metadata_signals(metadata: Mapping[str, Any]) -> tuple[PaginationSignal, ...]:
    signals: list[PaginationSignal] = []
    for key in _SIGNAL_KEYS:
        for value in _strings(metadata.get(key)):
            normalized = value.casefold().replace("-", "_").replace(" ", "_").replace("/", "_")
            aliases = {
                "cursor": "cursor_pagination",
                "cursor_based": "cursor_pagination",
                "offset": "offset_pagination",
                "limit_offset": "offset_pagination",
                "page_size": "page_size_limit",
                "page_limit": "page_size_limit",
                "token": "next_token",
                "page_token": "next_token",
                "infinite": "infinite_scroll",
                "bulk": "bulk_listing",
                "list": "list_endpoint",
                "listing": "list_endpoint",
            }
            normalized = aliases.get(normalized, normalized)
            if normalized in _SIGNAL_ORDER:
                signals.append(cast(PaginationSignal, normalized))
    return tuple(_dedupe(signals))


def _path_signals(normalized: str, path_text: str) -> tuple[PaginationSignal, ...]:
    path = PurePosixPath(normalized.casefold())
    parts = set(path.parts)
    name = path.name
    signals: set[PaginationSignal] = set()
    if {"api", "apis", "controllers", "handlers", "routes", "endpoints"} & parts and re.search(
        r"\b(?:list|index|collection|feed|search|query)\b", path_text, re.I
    ):
        signals.add("list_endpoint")
    if re.search(r"\b(?:pagination|paginate|cursor|page_token|next_token)\b", path_text, re.I):
        signals.add("cursor_pagination")
    if re.search(r"\b(?:offset|limit_offset|page_number)\b", path_text, re.I):
        signals.add("offset_pagination")
    if re.search(r"\b(?:page_size|per_page|max_limit|limit)\b", path_text, re.I):
        signals.add("page_size_limit")
    if re.search(r"\b(?:next_token|next_page_token|page_token|continuation_token)\b", path_text, re.I):
        signals.add("next_token")
    if "infinite_scroll" in path_text or "feed" in parts or "feed" in name:
        signals.add("infinite_scroll")
    if re.search(r"\b(?:bulk|export|download|backfill|list_all)\b", path_text, re.I):
        signals.add("bulk_listing")
    return tuple(signal for signal in _SIGNAL_ORDER if signal in signals)


def _text_signals(text: str) -> tuple[PaginationSignal, ...]:
    matched = {
        signal
        for signal, pattern in _SIGNAL_PATTERNS.items()
        if pattern.search(text) and _signal_is_relevant(signal, text)
    }
    if "list_endpoint" not in matched and _API_RE.search(text) and re.search(
        r"\b(?:pagination|paginate|cursor|offset|page size|page token|next token|per_page)\b",
        text,
        re.I,
    ):
        matched.add("list_endpoint")
    return tuple(signal for signal in _SIGNAL_ORDER if signal in matched)


def _signal_is_relevant(signal: PaginationSignal, text: str) -> bool:
    if signal == "cursor_pagination":
        return bool(re.search(r"\b(?:cursor pagination|cursor-based|cursor based|after cursor|before cursor|opaque cursor|pagination cursor)\b", text, re.I))
    if signal == "offset_pagination":
        return bool(re.search(r"\b(?:offset pagination|limit/offset|limit and offset|offset parameter|page number|page parameter)\b", text, re.I))
    if signal == "page_size_limit":
        return bool(re.search(r"\b(?:page size|page-size|page_size|per_page|per page|max(?:imum)? limit|max(?:imum)? page size|default page size)\b", text, re.I))
    if signal == "next_token":
        return bool(re.search(r"\b(?:next token|next_token|next page token|next_page_token|page token|page_token|continuation token)\b", text, re.I))
    return True


def _present_safeguards(task: Mapping[str, Any]) -> set[PaginationSafeguard]:
    safeguards: set[PaginationSafeguard] = set()
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in _SAFEGUARD_KEYS:
            for value in _strings(metadata.get(key)):
                safeguards.update(_safeguards_from_text(value))
    for _, text in _candidate_texts(task):
        safeguards.update(_safeguards_from_text(text))
    for command in _validation_commands(task):
        safeguards.update(_safeguards_from_text(command))
    return safeguards


def _safeguards_from_text(text: str) -> set[PaginationSafeguard]:
    normalized = text.casefold().replace("-", "_").replace(" ", "_").replace("/", "_")
    aliases = {
        "stable_sort": "stable_sort_order",
        "sort_order": "stable_sort_order",
        "cursor_compat": "cursor_compatibility",
        "compatibility": "backwards_compatibility",
        "backward_compatibility": "backwards_compatibility",
        "backwards_compatible": "backwards_compatibility",
        "max_page_size": "max_page_size_handling",
        "page_size": "max_page_size_handling",
        "empty_page": "empty_page_behavior",
        "boundary_pages": "boundary_page_tests",
    }
    if normalized in aliases:
        normalized = aliases[normalized]
    safeguards = {
        safeguard
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items()
        if pattern.search(text) or normalized == safeguard
    }
    return safeguards


def _impact_level(
    signals: tuple[PaginationSignal, ...],
    context: str,
) -> PaginationImpactLevel:
    if _CLIENT_FACING_RE.search(context) and (_MIGRATION_RE.search(context) or _BULK_EXPORT_RE.search(context)):
        return "high"
    if _MIGRATION_RE.search(context) and ("cursor_pagination" in signals or "next_token" in signals):
        return "high"
    if "bulk_listing" in signals and (_API_RE.search(context) or _BULK_EXPORT_RE.search(context)):
        return "high"
    if _CLIENT_FACING_RE.search(context):
        return "high"
    if _API_RE.search(context) or {"list_endpoint", "next_token", "infinite_scroll"} & set(signals):
        return "medium"
    return "low"


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
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "test_commands",
        "validation_command",
        "validation_commands",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
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
    for field_name in ("files_or_modules", "files", "acceptance_criteria", "depends_on", "dependencies", "tags", "labels", "notes"):
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
                if _metadata_key_has_signal(key_text):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_has_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _metadata_key_has_signal(key_text):
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


def _metadata_key_has_signal(text: str) -> bool:
    return bool(
        any(pattern.search(text) for pattern in _SIGNAL_PATTERNS.values())
        or any(pattern.search(text) for pattern in _SAFEGUARD_PATTERNS.values())
        or text in {key.replace("_", " ") for key in (*_SIGNAL_KEYS, *_SAFEGUARD_KEYS)}
    )


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


def _context(task: Mapping[str, Any]) -> str:
    values = [text for _, text in _candidate_texts(task)]
    values.extend(_validation_commands(task))
    return " ".join(values)


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
    "PaginationImpactLevel",
    "PaginationSafeguard",
    "PaginationSignal",
    "TaskApiPaginationImpactPlan",
    "TaskApiPaginationImpactRecord",
    "build_task_api_pagination_impact_plan",
    "derive_task_api_pagination_impact_plan",
    "summarize_task_api_pagination_impact",
    "summarize_task_api_pagination_impacts",
    "task_api_pagination_impact_plan_to_dict",
    "task_api_pagination_impact_plan_to_markdown",
]
