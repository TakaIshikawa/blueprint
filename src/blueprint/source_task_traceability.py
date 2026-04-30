"""Build source-to-task traceability links for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief, SourceBrief


CoverageStatus = Literal["covered", "uncovered"]
_T = TypeVar("_T")

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_COVERED_THRESHOLD = 0.6
_MIN_OVERLAP_TOKENS = 2
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "be",
    "by",
    "can",
    "for",
    "from",
    "if",
    "in",
    "into",
    "is",
    "it",
    "may",
    "of",
    "on",
    "or",
    "that",
    "the",
    "then",
    "to",
    "with",
}
_SOURCE_PAYLOAD_FIELDS = {
    "scope",
    "integration_points",
    "risks",
    "validation_plan",
    "definition_of_done",
}


@dataclass(frozen=True, slots=True)
class SourceTaskTraceLink:
    """Traceability coverage for one upstream requirement anchor."""

    requirement_id: str
    source_field: str
    requirement_text: str
    matched_task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence_snippets: tuple[str, ...] = field(default_factory=tuple)
    coverage_status: CoverageStatus = "uncovered"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_id": self.requirement_id,
            "source_field": self.source_field,
            "requirement_text": self.requirement_text,
            "matched_task_ids": list(self.matched_task_ids),
            "evidence_snippets": list(self.evidence_snippets),
            "coverage_status": self.coverage_status,
        }


@dataclass(frozen=True, slots=True)
class _RequirementAnchor:
    requirement_id: str
    source_field: str
    text: str
    tokens: frozenset[str]


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task_id: str
    snippets: tuple[tuple[str, str], ...]


def build_source_task_traceability_map(
    source_brief: Mapping[str, Any] | SourceBrief,
    implementation_brief: Mapping[str, Any] | ImplementationBrief,
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> tuple[SourceTaskTraceLink, ...]:
    """Link source and implementation requirement anchors to execution tasks."""
    source = _source_payload(source_brief)
    brief = _implementation_payload(implementation_brief)
    plan = _plan_payload(execution_plan)
    tasks = _task_records(_task_payloads(plan.get("tasks")))

    links: list[SourceTaskTraceLink] = []
    for anchor in _requirement_anchors(source, brief):
        matches = _task_matches(anchor, tasks)
        task_ids = tuple(match[0] for match in matches)
        evidence = tuple(_dedupe(snippet for _, snippets in matches for snippet in snippets))
        links.append(
            SourceTaskTraceLink(
                requirement_id=anchor.requirement_id,
                source_field=anchor.source_field,
                requirement_text=anchor.text,
                matched_task_ids=task_ids,
                evidence_snippets=evidence,
                coverage_status="covered" if task_ids else "uncovered",
            )
        )

    return tuple(links)


def source_task_traceability_to_dicts(
    links: tuple[SourceTaskTraceLink, ...] | list[SourceTaskTraceLink],
) -> list[dict[str, Any]]:
    """Serialize source-to-task trace links to plain dictionaries."""
    return [link.to_dict() for link in links]


source_task_traceability_to_dicts.__test__ = False


def _requirement_anchors(
    source: Mapping[str, Any],
    brief: Mapping[str, Any],
) -> list[_RequirementAnchor]:
    anchors: list[_RequirementAnchor] = []
    anchors.extend(_field_anchors("source", source, ("title", "summary")))

    source_payload = source.get("source_payload")
    if isinstance(source_payload, Mapping):
        anchors.extend(
            _field_anchors(
                "source.source_payload",
                source_payload,
                tuple(sorted(_SOURCE_PAYLOAD_FIELDS)),
            )
        )

    anchors.extend(
        _field_anchors(
            "implementation",
            brief,
            (
                "title",
                "summary",
                "scope",
                "integration_points",
                "risks",
                "validation_plan",
                "definition_of_done",
            ),
        )
    )
    return anchors


def _field_anchors(
    prefix: str,
    payload: Mapping[str, Any],
    field_names: tuple[str, ...],
) -> list[_RequirementAnchor]:
    anchors: list[_RequirementAnchor] = []
    for field_name in field_names:
        raw_value = payload.get(field_name)
        values = _strings(raw_value)
        if not values:
            continue
        if not isinstance(raw_value, (list, tuple, set)):
            anchors.append(_anchor(prefix, field_name, values[0], None))
            continue
        for index, value in enumerate(values, start=1):
            anchors.append(_anchor(prefix, field_name, value, index))
    return anchors


def _anchor(
    prefix: str,
    field_name: str,
    text: str,
    index: int | None,
) -> _RequirementAnchor:
    source_field = f"{prefix}.{field_name}"
    indexed_field = f"{source_field}.{index:03d}" if index is not None else source_field
    slug = _slug(text)
    requirement_id = f"req-{_slug(indexed_field)}"
    if slug:
        requirement_id = f"{requirement_id}-{slug}"
    return _RequirementAnchor(
        requirement_id=requirement_id,
        source_field=indexed_field,
        text=text,
        tokens=frozenset(_normalized_tokens(text)),
    )


def _task_matches(
    anchor: _RequirementAnchor,
    tasks: list[_TaskRecord],
) -> list[tuple[str, list[str]]]:
    matches: list[tuple[str, list[str]]] = []
    for task in tasks:
        snippets = _matching_snippets(anchor, task)
        if snippets:
            matches.append((task.task_id, snippets))
    return matches


def _matching_snippets(anchor: _RequirementAnchor, task: _TaskRecord) -> list[str]:
    snippets: list[str] = []
    for field_name, text in task.snippets:
        score = _match_score(anchor.text, anchor.tokens, text)
        if score < _COVERED_THRESHOLD:
            continue
        snippets.append(f"{task.task_id} {field_name}: {_evidence_text(text)}")
    return snippets[:3]


def _match_score(requirement: str, requirement_tokens: frozenset[str], text: str) -> float:
    requirement_phrase = _normalized_phrase(requirement)
    text_phrase = _normalized_phrase(text)
    if (
        requirement_phrase
        and requirement_phrase in text_phrase
        and (
            len(requirement_tokens) > 1
            or _strong_single_token(next(iter(requirement_tokens)))
        )
    ):
        return 1.0

    if not requirement_tokens:
        return 0.0
    task_tokens = set(_normalized_tokens(text))
    if not task_tokens:
        return 0.0

    overlap = requirement_tokens & task_tokens
    if len(requirement_tokens) == 1:
        if overlap == requirement_tokens and _strong_single_token(next(iter(overlap))):
            return 1.0
        return 0.0
    if len(overlap) < min(_MIN_OVERLAP_TOKENS, len(requirement_tokens)):
        return 0.0
    return len(overlap) / len(requirement_tokens)


def _strong_single_token(token: str) -> bool:
    return len(token) >= 6 and token not in {"build", "create", "update", "support", "review"}


def _task_records(tasks: list[dict[str, Any]]) -> list[_TaskRecord]:
    records: list[_TaskRecord] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        snippets: list[tuple[str, str]] = []
        for field_name in ("title", "description", "acceptance_criteria", "files_or_modules"):
            for value in _strings(task.get(field_name)):
                snippets.append((field_name, value))
        records.append(_TaskRecord(task_id=task_id, snippets=tuple(snippets)))
    return records


def _source_payload(source: Mapping[str, Any] | SourceBrief) -> dict[str, Any]:
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = SourceBrief.model_validate(source).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(source, Mapping):
            return dict(source)
    return {}


def _implementation_payload(
    brief: Mapping[str, Any] | ImplementationBrief,
) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        value = brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ImplementationBrief.model_validate(brief).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(brief, Mapping):
            return dict(brief)
    return {}


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        values: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            values.extend(_strings(value[key]))
        return values
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        values: list[str] = []
        for item in items:
            values.extend(_strings(item))
        return values
    text = _text(value)
    return [text] if text else []


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _normalized_phrase(value: str) -> str:
    return " ".join(_normalized_tokens(value))


def _normalized_tokens(value: str) -> list[str]:
    tokens: list[str] = []
    for raw_token in _TOKEN_RE.findall(value.lower()):
        token = _normalize_token(raw_token)
        if token and token not in _STOPWORDS:
            tokens.append(token)
    return tokens


def _normalize_token(token: str) -> str:
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def _slug(value: Any) -> str:
    return "-".join(_normalized_tokens(str(value)))[:80].strip("-")


def _evidence_text(value: str) -> str:
    text = _text(value)
    return text if len(text) <= 160 else text[:157].rstrip() + "..."


def _dedupe(values: list[_T] | tuple[_T, ...] | Any) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "CoverageStatus",
    "SourceTaskTraceLink",
    "build_source_task_traceability_map",
    "source_task_traceability_to_dicts",
]
