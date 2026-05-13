"""Extract source-level API idempotency key requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint._source_requirement_utils import (
    dedupe,
    evidence_snippet,
    markdown_cell,
    optional_text,
    segments,
    source_id,
    source_payloads,
)


APIIdempotencyKeyCategory = Literal[
    "idempotency_key",
    "replay_window",
    "request_fingerprint",
    "conflict_response",
    "persistence_ttl",
    "retry_semantics",
    "observability",
]
APIIdempotencyKeyConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[APIIdempotencyKeyCategory, ...] = (
    "idempotency_key",
    "replay_window",
    "request_fingerprint",
    "conflict_response",
    "persistence_ttl",
    "retry_semantics",
    "observability",
)
_CONFIDENCE_ORDER: dict[APIIdempotencyKeyConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
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
    "api",
    "idempotency",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CONTEXT_RE = re.compile(
    r"\b(?:idempotency|idempotent|idempotency-key|idempotency key|"
    r"deduplicat(?:e|ion)|duplicate request|request replay|replay window|safe retry)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"support|allow|provide|return|persist|store|expire|ttl|retry|replay|conflict|"
    r"fingerprint|hash|observe|metric|log|trace|alert|acceptance|done when)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}"
    r"\b(?:idempotency|idempotent|idempotency-key|idempotency key|replay|safe retry)\b|"
    r"\b(?:idempotency|idempotent|idempotency-key|idempotency key|replay|safe retry)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|excluded|no changes?)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:idempotent migration script|idempotent seed|idempotent deploy|"
    r"idempotent terraform|idempotent ansible|idempotent css)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[APIIdempotencyKeyCategory, re.Pattern[str]] = {
    "idempotency_key": re.compile(
        r"\b(?:idempotency-key|idempotency key|idempotency header|client(?: supplied|-supplied)? key|"
        r"dedupe key|request key|unique request id)\b",
        re.I,
    ),
    "replay_window": re.compile(
        r"\b(?:replay window|replay period|duplicate window|dedupe window|within \d+\s*(?:minutes?|hours?|days?)|"
        r"same key within|request replay)\b",
        re.I,
    ),
    "request_fingerprint": re.compile(
        r"\b(?:request fingerprint|fingerprint|payload hash|body hash|request hash|"
        r"parameter hash|method and path|same payload|mismatched payload)\b",
        re.I,
    ),
    "conflict_response": re.compile(
        r"\b(?:409 conflict|conflict response|idempotency conflict|mismatched request|"
        r"key reuse conflict|unprocessable duplicate|error on mismatch)\b",
        re.I,
    ),
    "persistence_ttl": re.compile(
        r"\b(?:persist|persistence|store|storage|ttl|expire|expiration|retention|"
        r"cached response|dedupe record|idempotency record)\b",
        re.I,
    ),
    "retry_semantics": re.compile(
        r"\b(?:retry|safe retry|retry semantics|network retry|timeout retry|"
        r"same response|replay response|duplicate submission)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|monitor|monitoring|metric|metrics|log|logging|trace|tracing|"
        r"alert|dashboard|duplicate rate|conflict rate)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[APIIdempotencyKeyCategory, tuple[str, ...]] = {
    "idempotency_key": ("api_platform", "backend"),
    "replay_window": ("api_platform", "backend"),
    "request_fingerprint": ("api_platform", "backend"),
    "conflict_response": ("api_platform", "backend"),
    "persistence_ttl": ("api_platform", "storage"),
    "retry_semantics": ("api_platform", "client_sdk"),
    "observability": ("api_platform", "observability"),
}
_PLANNING_NOTES: dict[APIIdempotencyKeyCategory, tuple[str, ...]] = {
    "idempotency_key": ("Define accepted Idempotency-Key header format, scope, uniqueness, and required endpoints.",),
    "replay_window": ("Specify replay window duration and how duplicate requests are recognized inside that window.",),
    "request_fingerprint": ("Define request fingerprint inputs and mismatch detection for reused keys.",),
    "conflict_response": ("Document conflict response status, error payload, and client remediation guidance.",),
    "persistence_ttl": ("Plan persistence, TTL, cleanup, and cached response storage for idempotency records.",),
    "retry_semantics": ("Define retry semantics for timeouts, duplicate submissions, and response replay.",),
    "observability": ("Add metrics, logs, traces, and alerts for key reuse, replays, conflicts, and store failures.",),
}
_MISSING_DETAIL_MESSAGES: dict[str, str] = {
    "missing_replay_window": "Specify replay window duration for retained idempotency keys.",
    "missing_fingerprint": "Define request fingerprinting and mismatch behavior for key reuse.",
    "missing_persistence": "Define idempotency record persistence, TTL, and cleanup ownership.",
}


@dataclass(frozen=True, slots=True)
class SourceAPIIdempotencyKeyRequirement:
    """One source-backed API idempotency key requirement."""

    category: APIIdempotencyKeyCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: APIIdempotencyKeyConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> APIIdempotencyKeyCategory:
        return self.category

    @property
    def concern(self) -> APIIdempotencyKeyCategory:
        return self.category

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "planning_notes": list(self.planning_notes),
            "gap_messages": list(self.gap_messages),
        }


@dataclass(frozen=True, slots=True)
class SourceAPIIdempotencyKeyRequirementsReport:
    """Source-level API idempotency key requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceAPIIdempotencyKeyRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAPIIdempotencyKeyRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAPIIdempotencyKeyRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source API Idempotency Key Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source API idempotency key requirements were inferred."])
            return "\n".join(lines)
        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{markdown_cell(requirement.source_field)} | "
                f"{markdown_cell(', '.join(requirement.suggested_owners))} | "
                f"{markdown_cell('; '.join(requirement.evidence))} | "
                f"{markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{markdown_cell('; '.join(requirement.gap_messages))} |"
            )
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: APIIdempotencyKeyCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: APIIdempotencyKeyConfidence


def build_source_api_idempotency_key_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | Iterable[Any] | str | object,
) -> SourceAPIIdempotencyKeyRequirementsReport:
    """Build an API idempotency key requirements report from brief-shaped input."""
    payloads = source_payloads(source)
    candidates: list[_Candidate] = []
    for _, payload in payloads:
        if _has_no_scope(payload):
            continue
        candidates.extend(_candidates(payload))
    requirements = tuple(_merge_candidates(candidates, _gap_messages(candidates)))
    ids = dedupe(source_id(payload) for _, payload in payloads)
    return SourceAPIIdempotencyKeyRequirementsReport(
        brief_id=ids[0] if len(ids) == 1 else None,
        title=optional_text(payloads[0][1].get("title")) if payloads else None,
        requirements=requirements,
        summary=_summary(requirements, len(payloads)),
    )


def summarize_source_api_idempotency_key_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | SourceAPIIdempotencyKeyRequirementsReport | Iterable[Any] | str | object,
) -> dict[str, Any]:
    if isinstance(source, SourceAPIIdempotencyKeyRequirementsReport):
        return dict(source.summary)
    return build_source_api_idempotency_key_requirements(source).summary


def derive_source_api_idempotency_key_requirements(source: Any) -> SourceAPIIdempotencyKeyRequirementsReport:
    return build_source_api_idempotency_key_requirements(source)


def generate_source_api_idempotency_key_requirements(source: Any) -> SourceAPIIdempotencyKeyRequirementsReport:
    return build_source_api_idempotency_key_requirements(source)


def extract_source_api_idempotency_key_requirements(source: Any) -> tuple[SourceAPIIdempotencyKeyRequirement, ...]:
    return build_source_api_idempotency_key_requirements(source).requirements


def source_api_idempotency_key_requirements_to_dict(report: SourceAPIIdempotencyKeyRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_api_idempotency_key_requirements_to_dict.__test__ = False


def source_api_idempotency_key_requirements_to_dicts(
    requirements: tuple[SourceAPIIdempotencyKeyRequirement, ...]
    | list[SourceAPIIdempotencyKeyRequirement]
    | SourceAPIIdempotencyKeyRequirementsReport,
) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceAPIIdempotencyKeyRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_idempotency_key_requirements_to_dicts.__test__ = False


def source_api_idempotency_key_requirements_to_markdown(report: SourceAPIIdempotencyKeyRequirementsReport) -> str:
    return report.to_markdown()


source_api_idempotency_key_requirements_to_markdown.__test__ = False


def _candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_field, text in segments(payload, _SCANNED_FIELDS):
        searchable = f"{source_field.replace('_', ' ')} {text}"
        if not _is_requirement(searchable):
            continue
        for category in _categories(searchable):
            candidates.append(
                _Candidate(
                    category=category,
                    value=_value(category, text),
                    source_field=source_field,
                    evidence=evidence_snippet(source_field, text),
                    confidence=_confidence(source_field, searchable),
                )
            )
    return candidates


def _is_requirement(text: str) -> bool:
    return bool(_CONTEXT_RE.search(text) and _REQUIREMENT_RE.search(text) and not _NEGATED_RE.search(text) and not _UNRELATED_RE.search(text))


def _has_no_scope(payload: Mapping[str, Any]) -> bool:
    return any(_NEGATED_RE.search(text) for _, text in segments(payload, _SCANNED_FIELDS))


def _categories(text: str) -> list[APIIdempotencyKeyCategory]:
    return [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(text)]


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_messages: tuple[str, ...],
) -> list[SourceAPIIdempotencyKeyRequirement]:
    grouped: dict[APIIdempotencyKeyCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)
    requirements: list[SourceAPIIdempotencyKeyRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        requirements.append(
            SourceAPIIdempotencyKeyRequirement(
                category=category,
                source_field=sorted({item.source_field for item in items}, key=str.casefold)[0],
                evidence=tuple(sorted(dedupe(item.evidence for item in items), key=str.casefold))[:5],
                confidence=confidence,
                value=_best_value(items),
                suggested_owners=_OWNER_SUGGESTIONS[category],
                planning_notes=_PLANNING_NOTES[category],
                gap_messages=gap_messages,
            )
        )
    return requirements


def _gap_messages(candidates: list[_Candidate]) -> tuple[str, ...]:
    if not candidates:
        return ()
    categories = {candidate.category for candidate in candidates}
    flags: list[str] = []
    if "replay_window" not in categories:
        flags.append("missing_replay_window")
    if "request_fingerprint" not in categories:
        flags.append("missing_fingerprint")
    if "persistence_ttl" not in categories:
        flags.append("missing_persistence")
    return tuple(_MISSING_DETAIL_MESSAGES[flag] for flag in flags)


def _summary(requirements: tuple[SourceAPIIdempotencyKeyRequirement, ...], source_count: int) -> dict[str, Any]:
    category_counts = {category: 0 for category in _CATEGORY_ORDER}
    confidence_counts = {confidence: 0 for confidence in _CONFIDENCE_ORDER}
    for requirement in requirements:
        category_counts[requirement.category] += 1
        confidence_counts[requirement.confidence] += 1
    gap_messages = dedupe(message for requirement in requirements for message in requirement.gap_messages)
    missing_flags = [
        flag
        for flag, message in _MISSING_DETAIL_MESSAGES.items()
        if message in gap_messages
    ]
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "categories": [category for category in _CATEGORY_ORDER if category_counts[category]],
        "category_counts": category_counts,
        "confidence_counts": confidence_counts,
        "missing_detail_flags": missing_flags,
        "gap_messages": gap_messages,
        "owner_suggestions": dedupe(owner for requirement in requirements for owner in requirement.suggested_owners),
        "status": "no_api_idempotency_key_language"
        if not requirements
        else ("needs_idempotency_key_details" if missing_flags else "ready_for_planning"),
    }


def _confidence(source_field: str, text: str) -> APIIdempotencyKeyConfidence:
    if re.search(r"\b(?:idempotency-key|idempotency key|409 conflict|ttl|fingerprint)\b", text, re.I):
        return "high"
    if source_field.startswith(("title", "summary", "requirements", "acceptance", "source_payload")):
        return "medium"
    return "low"


def _value(category: APIIdempotencyKeyCategory, text: str) -> str | None:
    match = _CATEGORY_PATTERNS[category].search(text)
    return optional_text(match.group(0).casefold()) if match else None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = dedupe(item.value for item in items)
    return values[0] if values else None
