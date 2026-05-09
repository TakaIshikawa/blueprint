"""Extract rate limiting requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


RateLimitingRequirementType = Literal[
    "rate_limits",
    "quota_types",
    "time_windows",
    "burst_allowances",
    "enforcement_mechanisms",
    "backoff_strategies",
    "client_communication",
    "distributed_limiting",
]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[RateLimitingRequirementType, ...] = (
    "rate_limits",
    "quota_types",
    "time_windows",
    "burst_allowances",
    "enforcement_mechanisms",
    "backoff_strategies",
    "client_communication",
    "distributed_limiting",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "integration_points",
    "integrations",
    "constraints",
    "metadata",
)

_TYPE_PATTERNS: dict[RateLimitingRequirementType, re.Pattern[str]] = {
    "rate_limits": re.compile(
        r"\b(?:rate limit(?:s|ing)?|requests? per (?:second|minute|hour|day)|"
        r"(?:\d+)\s*(?:req(?:uest)?s?|calls?|hits?)\s*per\s*(?:sec(?:ond)?|min(?:ute)?|hr|hour|day)|"
        r"limit(?:s|ing)?\s*(?:per|to)\s*(?:user|ip|api[- ]?key|account|endpoint)|"
        r"throttl(?:e|ing)|rps|rpm|tps)\b",
        re.I,
    ),
    "quota_types": re.compile(
        r"\b(?:per[- ](?:user|ip|api[- ]?key|account|tenant|organization|endpoint|service|client)|"
        r"(?:user|ip|account|tenant|organization|endpoint|service|client)[- ](?:level|based|specific) (?:limit|quota|rate)|"
        r"global (?:rate )?limit|shared (?:rate )?limit|quota scope)\b",
        re.I,
    ),
    "time_windows": re.compile(
        r"\b(?:per[- ](?:second|minute|hour|day|month)|"
        r"\d+[- ](?:second|minute|hour|day|month) window|"
        r"(?:second|minute|hour|day|month)ly (?:limit|rate|window)|"
        r"sliding window|rolling window|fixed window|tumbling window|"
        r"time window|rate window|window (?:size|period)|token bucket|leaky bucket)\b",
        re.I,
    ),
    "burst_allowances": re.compile(
        r"\b(?:burst(?:ing)?|burst (?:limit|capacity|allowance|handling)|"
        r"burst mode|allow(?:ed)? burst|temporary (?:burst|spike)|"
        r"spike handling|traffic spike|bursty traffic|burst tolerance)\b",
        re.I,
    ),
    "enforcement_mechanisms": re.compile(
        r"\b(?:enforce(?:ment)?|rate limit enforce(?:ment)?|"
        r"(?:429|http 429|status 429)|too many requests|"
        r"block(?:ing)?|reject(?:ing)?|drop(?:ping)?|queue(?:ing)?|"
        r"rate limit(?:ing)? (?:enforcement|mechanism|implementation|strategy)|"
        r"limit enforce(?:ment)?|quota enforce(?:ment)?)\b",
        re.I,
    ),
    "backoff_strategies": re.compile(
        r"\b(?:backoff|back[- ]off|retry[- ]after|"
        r"exponential backoff|linear backoff|jitter|"
        r"retry (?:strategy|policy|logic|mechanism)|"
        r"retry[- ]after header|backoff (?:strategy|policy|algorithm)|"
        r"rate limit retry)\b",
        re.I,
    ),
    "client_communication": re.compile(
        r"\b(?:rate limit (?:header|headers|response|feedback)|"
        r"x[- ]rate[- ]limit|x[- ]ratelimit|"
        r"remaining|reset|retry[- ]after|"
        r"rate limit (?:status|info|information|details)|"
        r"communicate (?:limit|rate)|limit (?:visibility|transparency)|"
        r"client (?:notification|feedback|communication))\b",
        re.I,
    ),
    "distributed_limiting": re.compile(
        r"\b(?:distributed (?:rate limit(?:ing)?|limiting)|"
        r"centralized (?:rate limit(?:ing)?|limiting)|"
        r"cluster[- ]wide (?:rate limit(?:ing)?|limiting)|"
        r"multi[- ](?:node|instance|server) (?:rate limit(?:ing)?|limiting)|"
        r"shared (?:counter|state)|redis (?:rate limit(?:ing)?|limiting)|"
        r"coordinated (?:rate limit(?:ing)?|limiting))\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[RateLimitingRequirementType, tuple[str, ...]] = {
    "rate_limits": (
        "What specific rate limits should be enforced (requests per time unit)?",
        "Should different endpoints or resources have different limits?",
    ),
    "quota_types": (
        "What quota scopes are required (per-user, per-IP, per-API-key, global)?",
        "Should quotas be isolated or shared across resources?",
    ),
    "time_windows": (
        "What time windows should be used (per-second, per-minute, per-hour)?",
        "Should windows be sliding/rolling or fixed?",
    ),
    "burst_allowances": (
        "Should temporary burst traffic be allowed above the steady-state limit?",
        "What burst capacity and duration should be supported?",
    ),
    "enforcement_mechanisms": (
        "How should rate limit violations be handled (block, queue, throttle)?",
        "What HTTP status codes and response format should be used?",
    ),
    "backoff_strategies": (
        "What retry strategy should clients use (exponential backoff, Retry-After header)?",
        "Should jitter be recommended to prevent thundering herd?",
    ),
    "client_communication": (
        "What rate limit information should be communicated to clients?",
        "Which HTTP headers should expose limit, remaining, and reset values?",
    ),
    "distributed_limiting": (
        "How should rate limits be enforced in a distributed system?",
        "What coordination mechanism should be used (Redis, distributed cache)?",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceRateLimitingRequirement:
    """One source-backed rate limiting requirement."""

    requirement_type: RateLimitingRequirementType
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "follow_up_questions": list(self.follow_up_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceRateLimitingRequirementsReport:
    """Source-level rate limiting requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceRateLimitingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceRateLimitingRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return rate limiting requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Rate Limiting Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("type_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Source count: {self.summary.get('source_count', 1)}",
            f"- Abuse prevention coverage: {self.summary.get('abuse_prevention_coverage', 0)}%",
            f"- Client experience coverage: {self.summary.get('client_experience_coverage', 0)}%",
            f"- Scalability coverage: {self.summary.get('scalability_coverage', 0)}%",
            "- Requirement type counts: "
            + ", ".join(
                f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source rate limiting requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Source Field Paths | Evidence | Follow-up Questions |",
                "| --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(requirement.follow_up_questions) or 'none')} |"
            )
        return "\n".join(lines)


def build_source_rate_limiting_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceRateLimitingRequirementsReport:
    """Extract rate limiting requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    grouped = _group_requirements(payload)
    requirements = _merge_requirements(grouped, source_brief_id)
    return SourceRateLimitingRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_rate_limiting_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceRateLimitingRequirement, ...]:
    """Return rate limiting requirement records extracted from brief-shaped input."""
    return build_source_rate_limiting_requirements(source).requirements


def summarize_source_rate_limiting_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | SourceRateLimitingRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic rate limiting requirements summary."""
    if isinstance(source_or_result, SourceRateLimitingRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_rate_limiting_requirements(source_or_result).summary


def source_rate_limiting_requirements_to_dict(
    report: SourceRateLimitingRequirementsReport,
) -> dict[str, Any]:
    """Serialize a rate limiting requirements report to a plain dictionary."""
    return report.to_dict()


source_rate_limiting_requirements_to_dict.__test__ = False


def source_rate_limiting_requirements_to_dicts(
    requirements: (
        tuple[SourceRateLimitingRequirement, ...]
        | list[SourceRateLimitingRequirement]
        | SourceRateLimitingRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source rate limiting requirement records to dictionaries."""
    if isinstance(requirements, SourceRateLimitingRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_rate_limiting_requirements_to_dicts.__test__ = False


def source_rate_limiting_requirements_to_markdown(
    report: SourceRateLimitingRequirementsReport,
) -> str:
    """Render a rate limiting requirements report as Markdown."""
    return report.to_markdown()


source_rate_limiting_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: RateLimitingRequirementType
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_brief_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _source_brief_id(payload), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
            return _source_brief_id(payload), payload
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
            return _source_brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_brief_id(payload), payload
    return None, {}


def _source_brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _group_requirements(payload: Mapping[str, Any]) -> dict[RateLimitingRequirementType, list[_Candidate]]:
    grouped: dict[RateLimitingRequirementType, list[_Candidate]] = {}
    for source_field, text in _candidate_texts(payload):
        for segment in _segments(text):
            for requirement_type in _matched_requirement_types(segment):
                candidate = _Candidate(
                    requirement_type=requirement_type,
                    evidence=_evidence_snippet(source_field, segment),
                    source_field_path=source_field,
                    matched_terms=_matched_terms(requirement_type, segment),
                )
                grouped.setdefault(requirement_type, []).append(candidate)
    return grouped


def _merge_requirements(
    grouped: dict[RateLimitingRequirementType, list[_Candidate]],
    source_brief_id: str | None,
) -> tuple[SourceRateLimitingRequirement, ...]:
    requirements: list[SourceRateLimitingRequirement] = []
    for requirement_type in _TYPE_ORDER:
        candidates = grouped.get(requirement_type, [])
        if not candidates:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in candidates))[:5]
        source_field_paths = tuple(
            sorted(_dedupe(item.source_field_path for item in candidates), key=str.casefold)
        )
        matched_terms = tuple(
            sorted(
                _dedupe(term for item in candidates for term in item.matched_terms),
                key=str.casefold,
            )
        )
        questions = _follow_up_questions(requirement_type, " ".join(evidence))
        requirements.append(
            SourceRateLimitingRequirement(
                requirement_type=requirement_type,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                follow_up_questions=questions,
            )
        )
    return tuple(requirements)


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in _SCANNED_FIELDS:
        value = payload.get(field_name)
        if field_name == "metadata":
            texts.extend(_nested_texts(value, field_name))
            continue
        for index, text in enumerate(_strings(value)):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))

    if isinstance(payload.get("source_payload"), Mapping):
        for field_name in _SCANNED_FIELDS:
            if field_name in payload["source_payload"]:
                texts.extend(_nested_texts(payload["source_payload"][field_name], f"source_payload.{field_name}"))
    return texts


def _nested_texts(value: Any, prefix: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(item, field))
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


def _segments(text: str) -> list[str]:
    segments: list[str] = []
    for raw_segment in _SENTENCE_SPLIT_RE.split(text):
        segment = _clean_text(raw_segment)
        if segment:
            segments.append(segment)
    return segments


def _matched_requirement_types(text: str) -> tuple[RateLimitingRequirementType, ...]:
    return tuple(
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    )


def _matched_terms(
    requirement_type: RateLimitingRequirementType,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)) for match in _TYPE_PATTERNS[requirement_type].finditer(text)
        )
    )


def _follow_up_questions(
    requirement_type: RateLimitingRequirementType,
    evidence_text: str,
) -> tuple[str, ...]:
    questions = list(_BASE_QUESTIONS[requirement_type])
    # Reduce questions if evidence already provides specific answers
    if requirement_type == "rate_limits" and re.search(
        r"\b\d+\s*(?:req(?:uest)?s?|calls?)\s*per\s*(?:sec(?:ond)?|min(?:ute)?|hour|day)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Specific limit mentioned
    if requirement_type == "quota_types" and re.search(
        r"\b(?:per[- ](?:user|ip|api[- ]?key|account)|global)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Scope mentioned
    if requirement_type == "time_windows" and re.search(
        r"\b(?:per[- ](?:second|minute|hour|day)|sliding|rolling|fixed|tumbling)\s*(?:window)?\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Window type or time unit mentioned
    if requirement_type == "enforcement_mechanisms" and re.search(
        r"\b(?:429|block|reject|queue|throttle)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Mechanism mentioned
    return tuple(_dedupe(questions))


def _summary(requirements: tuple[SourceRateLimitingRequirement, ...]) -> dict[str, Any]:
    # Calculate coverage metrics
    abuse_prevention = {"rate_limits", "quota_types", "enforcement_mechanisms"}
    client_experience = {"backoff_strategies", "client_communication"}
    scalability = {"time_windows", "burst_allowances", "distributed_limiting"}

    req_types = {req.requirement_type for req in requirements}
    abuse_prevention_coverage = int(100 * len(req_types & abuse_prevention) / len(abuse_prevention)) if abuse_prevention else 0
    client_experience_coverage = int(100 * len(req_types & client_experience) / len(client_experience)) if client_experience else 0
    scalability_coverage = int(100 * len(req_types & scalability) / len(scalability)) if scalability else 0

    return {
        "requirement_count": len(requirements),
        "source_count": 1,
        "type_counts": {
            req_type: sum(1 for req in requirements if req.requirement_type == req_type)
            for req_type in _TYPE_ORDER
        },
        "requirement_types": [req.requirement_type for req in requirements],
        "follow_up_question_count": sum(
            len(req.follow_up_questions) for req in requirements
        ),
        "abuse_prevention_coverage": abuse_prevention_coverage,
        "client_experience_coverage": client_experience_coverage,
        "scalability_coverage": scalability_coverage,
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
        "requirements",
        "acceptance_criteria",
        "acceptance",
        "constraints",
        "integration_points",
        "integrations",
        "metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _BULLET_RE.sub("", text.strip())
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


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
    return deduped


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
    "RateLimitingRequirementType",
    "SourceRateLimitingRequirement",
    "SourceRateLimitingRequirementsReport",
    "build_source_rate_limiting_requirements",
    "extract_source_rate_limiting_requirements",
    "source_rate_limiting_requirements_to_dict",
    "source_rate_limiting_requirements_to_dicts",
    "source_rate_limiting_requirements_to_markdown",
    "summarize_source_rate_limiting_requirements",
]
