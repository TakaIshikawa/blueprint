"""Shared deterministic keyword extraction for source requirement reports."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


Confidence = str
RequirementKey = str


@dataclass(frozen=True, slots=True)
class KeywordRequirementSpec:
    key: RequirementKey
    pattern: re.Pattern[str]
    missing_details: tuple[str, ...]
    detail_patterns: Mapping[str, re.Pattern[str]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class KeywordRequirement:
    source_brief_id: str | None
    requirement_type: str
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: Confidence = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "missing_details": list(self.missing_details),
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class KeywordRequirementsReport:
    source_id: str | None = None
    requirements: tuple[KeywordRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)
    title: str = "Source Requirements Report"

    @property
    def records(self) -> tuple[KeywordRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        records = [requirement.to_dict() for requirement in self.requirements]
        return {
            "source_id": self.source_id,
            "requirements": records,
            "summary": dict(self.summary),
            "records": records,
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        title = f"# {self.title}"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        counts = self.summary.get("type_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement type counts: "
            + ", ".join(
                f"{key} {counts.get(key, 0)}"
                for key in self.summary.get("requirement_type_order", ())
            ),
        ]
        if self.summary.get("missing_detail_flags"):
            lines.append(
                "- Missing detail flags: "
                + ", ".join(self.summary.get("missing_detail_flags", ()))
            )
        if not self.requirements:
            lines.extend(["", "No matching source requirements were found."])
            return "\n".join(lines)
        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Type | Confidence | Missing Details | Evidence |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.requirement_type} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.missing_details))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|cannot ship|policy|compliance)\b",
    re.I,
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "created_by",
    "updated_by",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
}


def build_keyword_requirements_report(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | Iterable[Any] | str | object,
    *,
    title: str,
    specs: tuple[KeywordRequirementSpec, ...],
    context_pattern: re.Pattern[str],
    structured_field_pattern: re.Pattern[str],
    negated_pattern: re.Pattern[str],
    summary_flag_groups: Mapping[str, tuple[str, ...]] | None = None,
    unrelated_pattern: re.Pattern[str] | None = None,
) -> KeywordRequirementsReport:
    brief_payloads = _source_payloads(source)
    candidates: dict[tuple[str | None, str], list[KeywordRequirement]] = {}
    for source_brief_id, payload in brief_payloads:
        segments = _candidate_segments(payload)
        has_negated_scope = any(negated_pattern.search(segment) for _, segment in segments)
        for field, segment in segments:
            if has_negated_scope and field in {"title", "summary"}:
                continue
            if negated_pattern.search(segment):
                continue
            field_text = field.replace("_", " ").replace("-", " ")
            has_context = bool(context_pattern.search(segment) or context_pattern.search(field_text))
            if not has_context:
                continue
            if unrelated_pattern and unrelated_pattern.search(segment) and not any(
                spec.pattern.search(segment) for spec in specs
            ):
                continue
            for spec in specs:
                if not (spec.pattern.search(segment) or (spec.pattern.search(field_text) and has_context)):
                    continue
                evidence = _evidence_snippet(field, segment)
                item = KeywordRequirement(
                    source_brief_id=source_brief_id,
                    requirement_type=spec.key,
                    missing_details=_missing_details(spec, segment),
                    confidence=_confidence(segment, field, structured_field_pattern),
                    evidence=(evidence,),
                )
                candidates.setdefault((source_brief_id, spec.key), []).append(item)

    requirements = tuple(
        sorted(
            (_merge(items) for items in candidates.values()),
            key=lambda requirement: (
                requirement.source_brief_id or "",
                _spec_index(specs, requirement.requirement_type),
                {"high": 0, "medium": 1, "low": 2}.get(requirement.confidence, 3),
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return KeywordRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads), specs, summary_flag_groups or {}),
        title=title,
    )


def _source_payloads(source: Any) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Any) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    return None, {
        key: getattr(source, key)
        for key in dir(source)
        if not key.startswith("_") and not callable(getattr(source, key))
    }


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    preferred = (
        "title",
        "summary",
        "body",
        "description",
        "problem_statement",
        "goal",
        "goals",
        "mvp_goal",
        "requirements",
        "constraints",
        "scope",
        "non_goals",
        "assumptions",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "compliance",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    for field_name in preferred:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            values.append((child_field, key_text))
            _append_value(values, child_field, value[key])
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        values.extend((source_field, segment) for segment in _segments(text))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for sentence in parts:
            segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _missing_details(spec: KeywordRequirementSpec, text: str) -> tuple[str, ...]:
    missing = list(spec.missing_details)
    for detail, pattern in spec.detail_patterns.items():
        if pattern.search(text) and detail in missing:
            missing.remove(detail)
    return tuple(missing)


def _confidence(text: str, field: str, structured_field_pattern: re.Pattern[str]) -> str:
    if _REQUIRED_RE.search(text):
        return "high"
    if structured_field_pattern.search(field):
        return "medium"
    return "low"


def _merge(items: list[KeywordRequirement]) -> KeywordRequirement:
    first = items[0]
    confidence = min(items, key=lambda item: {"high": 0, "medium": 1, "low": 2}.get(item.confidence, 3)).confidence
    common_missing = set(first.missing_details)
    for item in items[1:]:
        common_missing.intersection_update(item.missing_details)
    evidence = sorted(
        _dedupe(evidence for item in items for evidence in item.evidence),
        key=lambda value: value.casefold(),
    )[:5]
    return KeywordRequirement(
        source_brief_id=first.source_brief_id,
        requirement_type=first.requirement_type,
        missing_details=tuple(sorted(common_missing, key=str.casefold)),
        confidence=confidence,
        evidence=tuple(evidence),
    )


def _summary(
    requirements: tuple[KeywordRequirement, ...],
    source_count: int,
    specs: tuple[KeywordRequirementSpec, ...],
    summary_flag_groups: Mapping[str, tuple[str, ...]],
) -> dict[str, Any]:
    missing_detail_flags = tuple(
        flag
        for flag, details in summary_flag_groups.items()
        if any(detail in requirement.missing_details for requirement in requirements for detail in details)
    )
    type_order = tuple(spec.key for spec in specs)
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "type_counts": {
            key: sum(1 for requirement in requirements if requirement.requirement_type == key)
            for key in type_order
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in ("high", "medium", "low")
        },
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "requirement_type_order": type_order,
        "missing_detail_flags": list(missing_detail_flags),
        "status": "no_requirements"
        if not requirements
        else ("needs_detail" if missing_detail_flags else "complete"),
    }


def _spec_index(specs: tuple[KeywordRequirementSpec, ...], key: str) -> int:
    for index, spec in enumerate(specs):
        if spec.key == key:
            return index
    return len(specs)


def _evidence_snippet(source_field: str, text: str) -> str:
    snippet = _clean_text(text)
    if len(snippet) > 180:
        snippet = f"{snippet[:177].rstrip()}..."
    return f"{source_field}: {snippet}"


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip(" -\t\r\n.")


def _dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.casefold()
        if key not in seen:
            seen.add(key)
            result.append(value)
    return result


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")
