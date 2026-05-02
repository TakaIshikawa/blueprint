"""Extract source-level rate limit and quota requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


RateLimitRequirementType = Literal[
    "rate_limit",
    "quota",
    "throttling",
    "burst",
    "retry_after",
    "tenant_scope",
    "user_scope",
]
RateLimitConfidence = Literal["high", "medium", "low"]

_REQUIREMENT_ORDER: tuple[RateLimitRequirementType, ...] = (
    "rate_limit",
    "quota",
    "throttling",
    "burst",
    "retry_after",
    "tenant_scope",
    "user_scope",
)
_CONFIDENCE_ORDER: dict[RateLimitConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_LIMIT_VALUE_RE = re.compile(
    r"\b(?P<value>(?:\d+(?:[,.]\d+)?\s*(?:k|m)?|unlimited)\s*"
    r"(?:requests?|reqs?|calls?|api calls?|tokens?|messages?|jobs?|events?|writes?|reads?|"
    r"uploads?|exports?|credits?|units?)\s*"
    r"(?:/|per\s+)(?:second|sec|s|minute|min|m|hour|hr|h|day|d|month|mo|tenant|user|account|workspace))\b",
    re.I,
)
_RETRY_VALUE_RE = re.compile(
    r"\b(?P<value>(?:retry[- ]after|back\s*off|backoff)\s*(?:header|seconds?|window)?"
    r"(?:\s*(?:of|:|=|is|for)?\s*\d+(?:\.\d+)?\s*(?:ms|milliseconds?|seconds?|secs?|s|minutes?|mins?|m))?)\b",
    re.I,
)
_SCOPE_RE = re.compile(
    r"\b(?P<scope>per[- ]tenant|per tenant|tenant[- ]level|tenant scoped|by tenant|"
    r"per[- ]user|per user|user[- ]level|user scoped|by user|per account|account[- ]level|"
    r"per workspace|workspace[- ]level)\b",
    re.I,
)
_RATE_LIMIT_CONTEXT_RE = re.compile(
    r"\b(?:rate[- ]limits?|rate limiting|request limits?|api limits?|limits? requests?|"
    r"quota|quotas|allowance|throttle|throttling|throttled|burst|bursts|bursting|"
    r"retry[- ]after|backoff|back off|too many requests|http\s*429|429s?|"
    r"requests? per|calls? per|api calls? per)\b",
    re.I,
)
_REQUIREMENT_PATTERNS: dict[RateLimitRequirementType, re.Pattern[str]] = {
    "rate_limit": re.compile(
        r"\b(?:rate[- ]limits?|rate limiting|request limits?|api limits?|limits? requests?|"
        r"requests? per|calls? per|api calls? per|too many requests|http\s*429|429s?)\b",
        re.I,
    ),
    "quota": re.compile(
        r"\b(?:quota|quotas|allowance|allocation|included usage|usage cap|cap usage|"
        r"daily cap|monthly cap|limit budget|credits?)\b",
        re.I,
    ),
    "throttling": re.compile(
        r"\b(?:throttle|throttles|throttled|throttling|shed load|slow down clients?|"
        r"backpressure|too many requests|http\s*429|429s?)\b",
        re.I,
    ),
    "burst": re.compile(r"\b(?:burst|bursts|bursting|burst capacity|burst window|spikes?)\b", re.I),
    "retry_after": re.compile(
        r"\b(?:retry[- ]after|retry after|backoff|back off|exponential backoff|"
        r"retry window|cooldown|cool[- ]down)\b",
        re.I,
    ),
    "tenant_scope": re.compile(
        r"\b(?:per[- ]tenant|per tenant|tenant[- ]level|tenant scoped|by tenant|"
        r"tenant quota|tenant limit|per account|account[- ]level|per workspace|workspace[- ]level)\b",
        re.I,
    ),
    "user_scope": re.compile(
        r"\b(?:per[- ]user|per user|user[- ]level|user scoped|by user|user quota|user limit)\b",
        re.I,
    ),
}
_PLANNING_NOTES: dict[RateLimitRequirementType, str] = {
    "rate_limit": "Define server-side enforcement, client messaging, and observability for limit hits.",
    "quota": "Define quota accounting, reset windows, upgrade or exception paths, and customer-visible balances.",
    "throttling": "Plan throttled responses, client retry behavior, load-shedding thresholds, and alerts.",
    "burst": "Document burst capacity, burst window, smoothing behavior, and abuse safeguards.",
    "retry_after": "Specify Retry-After or backoff semantics, response headers, and client retry tests.",
    "tenant_scope": "Confirm tenant/account/workspace limit attribution, isolation, and admin visibility.",
    "user_scope": "Confirm user-level attribution, fairness expectations, and support diagnostics.",
}
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
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "risks",
    "api",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)


@dataclass(frozen=True, slots=True)
class SourceRateLimitRequirement:
    """One source-backed rate limit, quota, throttling, or scope requirement."""

    requirement_type: RateLimitRequirementType
    value: str | None = None
    limit_scope: str | None = None
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: RateLimitConfidence = "medium"
    planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "value": self.value,
            "limit_scope": self.limit_scope,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceRateLimitRequirementsReport:
    """Source-level rate limit requirements report."""

    source_brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceRateLimitRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceRateLimitRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return rate limit requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Rate Limit Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("requirement_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
            "- Requirement type counts: "
            + ", ".join(f"{key} {type_counts.get(key, 0)}" for key in _REQUIREMENT_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source rate limit requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Value | Scope | Confidence | Source Field | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{_markdown_cell(requirement.limit_scope or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} |"
            )
        return "\n".join(lines)


def build_source_rate_limit_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceRateLimitRequirementsReport:
    """Build a rate limit requirements report from a source brief-like payload."""
    source_brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceRateLimitRequirementsReport(
        source_brief_id=source_brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_rate_limit_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceRateLimitRequirementsReport:
    """Compatibility helper for callers that use summarize_* naming."""
    return build_source_rate_limit_requirements(source)


def derive_source_rate_limit_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceRateLimitRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_rate_limit_requirements(source)


def generate_source_rate_limit_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceRateLimitRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_rate_limit_requirements(source)


def extract_source_rate_limit_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceRateLimitRequirement, ...]:
    """Return rate limit requirement records from brief-shaped input."""
    return build_source_rate_limit_requirements(source).requirements


def source_rate_limit_requirements_to_dict(
    report: SourceRateLimitRequirementsReport,
) -> dict[str, Any]:
    """Serialize a rate limit requirements report to a plain dictionary."""
    return report.to_dict()


source_rate_limit_requirements_to_dict.__test__ = False


def source_rate_limit_requirements_to_dicts(
    requirements: (
        tuple[SourceRateLimitRequirement, ...]
        | list[SourceRateLimitRequirement]
        | SourceRateLimitRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize rate limit requirement records to dictionaries."""
    if isinstance(requirements, SourceRateLimitRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_rate_limit_requirements_to_dicts.__test__ = False


def source_rate_limit_requirements_to_markdown(
    report: SourceRateLimitRequirementsReport,
) -> str:
    """Render a rate limit requirements report as Markdown."""
    return report.to_markdown()


source_rate_limit_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: RateLimitRequirementType
    value: str | None
    limit_scope: str | None
    source_field: str
    evidence: str
    confidence: RateLimitConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        try:
            value = SourceBrief.model_validate(source).model_dump(mode="python")
            payload = dict(value)
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
        return _source_id(payload), payload
    return None, _object_payload(source)


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_field, segment in _candidate_segments(payload):
        if not _is_rate_limit_signal(segment):
            continue
        requirement_types = _requirement_types(segment)
        if not requirement_types:
            continue
        value = _value(segment)
        scope = _scope(segment)
        if source_field == "title" and not value and not scope:
            continue
        evidence = _evidence_snippet(source_field, segment)
        confidence = _confidence(segment, source_field, value, scope)
        for requirement_type in requirement_types:
            candidates.append(
                _Candidate(
                    requirement_type=requirement_type,
                    value=value if requirement_type not in {"tenant_scope", "user_scope"} else None,
                    limit_scope=scope,
                    source_field=source_field,
                    evidence=evidence,
                    confidence=confidence,
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceRateLimitRequirement]:
    grouped: dict[RateLimitRequirementType, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.requirement_type, []).append(candidate)

    requirements: list[SourceRateLimitRequirement] = []
    for requirement_type, items in grouped.items():
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda item: item.casefold(),
        )[0]
        confidence = min(
            (item.confidence for item in items), key=lambda item: _CONFIDENCE_ORDER[item]
        )
        value = _best_value(items)
        limit_scope = _best_scope(items)
        requirements.append(
            SourceRateLimitRequirement(
                requirement_type=requirement_type,
                value=value,
                limit_scope=limit_scope,
                source_field=source_field,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:4],
                confidence=confidence,
                planning_note=_PLANNING_NOTES[requirement_type],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CONFIDENCE_ORDER[requirement.confidence],
            _REQUIREMENT_ORDER.index(requirement.requirement_type),
            requirement.limit_scope or "",
            requirement.value or "",
            requirement.source_field.casefold(),
            requirement.evidence,
        ),
    )


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (0 if re.search(r"\d", value) else 1, len(value), value.casefold()),
    )
    return values[0] if values else None


def _best_scope(items: Iterable[_Candidate]) -> str | None:
    scopes = sorted({item.limit_scope for item in items if item.limit_scope})
    return scopes[0] if scopes else None


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and key not in {
            "id",
            "source_brief_id",
            "source_id",
            "source_project",
            "source_entity_type",
            "created_at",
            "updated_at",
            "source_links",
        }:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _is_rate_limit_signal(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, child)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            values.append((source_field, segment))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for sentence in _SENTENCE_SPLIT_RE.split(value):
        segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _requirement_types(text: str) -> tuple[RateLimitRequirementType, ...]:
    return tuple(
        requirement_type
        for requirement_type in _REQUIREMENT_ORDER
        if _REQUIREMENT_PATTERNS[requirement_type].search(text)
    )


def _is_rate_limit_signal(text: str) -> bool:
    return (
        _RATE_LIMIT_CONTEXT_RE.search(text) is not None or _LIMIT_VALUE_RE.search(text) is not None
    )


def _value(text: str) -> str | None:
    if retry := _RETRY_VALUE_RE.search(text):
        return _clean_text(retry.group("value"))
    if limit := _LIMIT_VALUE_RE.search(text):
        return _clean_text(limit.group("value"))
    return None


def _scope(text: str) -> str | None:
    if scope := _SCOPE_RE.search(text):
        value = _clean_text(scope.group("scope")).casefold().replace("-", " ")
        if "tenant" in value:
            return "tenant"
        if "user" in value:
            return "user"
        if "account" in value:
            return "account"
        if "workspace" in value:
            return "workspace"
    return None


def _confidence(
    text: str, source_field: str, value: str | None, scope: str | None
) -> RateLimitConfidence:
    normalized_field = source_field.replace("-", "_").casefold()
    if value and (
        re.search(
            r"\b(?:must|shall|required|requires?|enforce|reject|return|respond)\b", text, re.I
        )
        or any(
            marker in normalized_field
            for marker in ("success_criteria", "acceptance_criteria", "constraint")
        )
    ):
        return "high"
    if (
        value
        or scope
        or re.search(r"\b(?:must|shall|required|http\s*429|retry[- ]after)\b", text, re.I)
    ):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceRateLimitRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "requirement_type_counts": {
            requirement_type: sum(
                1
                for requirement in requirements
                if requirement.requirement_type == requirement_type
            )
            for requirement_type in _REQUIREMENT_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "scopes": sorted(
            {requirement.limit_scope for requirement in requirements if requirement.limit_scope}
        ),
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
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
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "risks",
        "api",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return sorted(deduped, key=lambda item: item.casefold())


__all__ = [
    "RateLimitConfidence",
    "RateLimitRequirementType",
    "SourceRateLimitRequirement",
    "SourceRateLimitRequirementsReport",
    "build_source_rate_limit_requirements",
    "derive_source_rate_limit_requirements",
    "extract_source_rate_limit_requirements",
    "generate_source_rate_limit_requirements",
    "summarize_source_rate_limit_requirements",
    "source_rate_limit_requirements_to_dict",
    "source_rate_limit_requirements_to_dicts",
    "source_rate_limit_requirements_to_markdown",
]
