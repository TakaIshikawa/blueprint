"""Extract API WebSocket requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


WebSocketRequirementType = Literal[
    "connection_lifecycle",
    "message_framing",
    "authentication",
    "rate_limiting",
    "reconnection_strategy",
    "message_ordering",
    "ping_pong_heartbeat",
    "subprotocol_negotiation",
]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[WebSocketRequirementType, ...] = (
    "connection_lifecycle",
    "message_framing",
    "authentication",
    "rate_limiting",
    "reconnection_strategy",
    "message_ordering",
    "ping_pong_heartbeat",
    "subprotocol_negotiation",
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

_TYPE_PATTERNS: dict[WebSocketRequirementType, re.Pattern[str]] = {
    "connection_lifecycle": re.compile(
        r"\b(?:websocket|ws|wss)\b.{0,100}\b(?:connection|handshake|upgrade|establish|connect|disconnect|close frame|closing handshake|connection lifecycle)\b|"
        r"\b(?:connection|handshake|upgrade|establish|connect|disconnect|close frame|closing handshake|connection lifecycle)\b.{0,100}\b(?:websocket|ws|wss)\b",
        re.I,
    ),
    "message_framing": re.compile(
        r"\b(?:message fram(?:e|ing)|text frame|binary frame|text message|binary message|message format|frame type|opcode|payload|fragmentation|message segmentation)\b",
        re.I,
    ),
    "authentication": re.compile(
        r"\b(?:websocket|ws|wss|connection|frame|message)\b.{0,100}\b(?:auth(?:entication|orization)?|token|bearer|jwt|api[- ]?key|credential)\b|"
        r"\b(?:auth(?:entication|orization)?|bearer|jwt|api[- ]?key|credential)\b.{0,100}\b(?:websocket|ws|wss|connection|frame|message)\b|"
        r"\b(?:jwt|bearer token|api[- ]?key|credential)\b",
        re.I,
    ),
    "rate_limiting": re.compile(
        r"\b(?:websocket|ws|wss|connection)\b.{0,100}\b(?:rate limit(?:ing)?|throttl(?:e|ing)|backpressure|flow control|message (?:rate|limit)|concurrent connection|max(?:imum)? connection)\b|"
        r"\b(?:rate limit(?:ing)?|throttl(?:e|ing)|backpressure|flow control)\b.{0,100}\b(?:websocket|ws|wss|connection|message)\b|"
        r"\b(?:concurrent connection|max(?:imum)? connection)\b",
        re.I,
    ),
    "reconnection_strategy": re.compile(
        r"\b(?:websocket|ws|wss|connection|socket\.io)\b.{0,100}\b(?:reconnect(?:ion)?|reconnect strategy|automatic reconnect|exponential backoff|connection recovery|resume connection)\b|"
        r"\b(?:reconnect(?:ion)?|reconnect strategy|automatic reconnect|exponential backoff|connection recovery|resume connection)\b.{0,100}\b(?:websocket|ws|wss|connection|socket\.io)\b|"
        r"\b(?:automatic reconnect(?:ion)?|exponential backoff|reconnect(?:ion)? strategy)\b",
        re.I,
    ),
    "message_ordering": re.compile(
        r"\b(?:message order(?:ing)?|sequential message|message sequence|delivery (?:guarantee|order)|ordered delivery|fifo|in[- ]order delivery|message queue|maintain(?:ing)? (?:delivery )?order)\b",
        re.I,
    ),
    "ping_pong_heartbeat": re.compile(
        r"\b(?:ping[/-]?pong|heartbeat|keep[- ]?alive|connection alive|ping frames?|pong frames?|health check)\b",
        re.I,
    ),
    "subprotocol_negotiation": re.compile(
        r"\b(?:subprotocol|sub[- ]protocol|protocol negotiat(?:ion|e)|sec[- ]websocket[- ]protocol|stomp|mqtt|wamp|socket\.io|custom protocol)\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[WebSocketRequirementType, tuple[str, ...]] = {
    "connection_lifecycle": (
        "What are the connection establishment and handshake requirements?",
        "How should connection closure and cleanup be handled?",
    ),
    "message_framing": (
        "Which message frame types are supported (text, binary, both)?",
        "Are there size limits or fragmentation requirements for large messages?",
    ),
    "authentication": (
        "What authentication method should be used for WebSocket connections?",
        "Should authentication occur during handshake or after connection establishment?",
    ),
    "rate_limiting": (
        "What are the rate limits for messages and connections?",
        "How should backpressure and flow control be implemented?",
    ),
    "reconnection_strategy": (
        "What retry strategy should be used for failed connections?",
        "How should connection state be restored after reconnection?",
    ),
    "message_ordering": (
        "Are messages guaranteed to be delivered in order?",
        "How should out-of-order or duplicate messages be handled?",
    ),
    "ping_pong_heartbeat": (
        "What is the heartbeat interval and timeout configuration?",
        "Should ping/pong be automatic or manual?",
    ),
    "subprotocol_negotiation": (
        "Which WebSocket subprotocols are required or supported?",
        "How should subprotocol negotiation failures be handled?",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceApiWebSocketRequirement:
    """One source-backed API WebSocket requirement."""

    requirement_type: WebSocketRequirementType
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
class SourceApiWebSocketRequirementsReport:
    """Source-level API WebSocket requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceApiWebSocketRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiWebSocketRequirement, ...]:
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
        """Return API WebSocket requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API WebSocket Requirements Report"
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
            f"- Connection coverage: {self.summary.get('connection_coverage', 0)}%",
            f"- Security coverage: {self.summary.get('security_coverage', 0)}%",
            f"- Reliability coverage: {self.summary.get('reliability_coverage', 0)}%",
            "- Requirement type counts: "
            + ", ".join(
                f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source API WebSocket requirements were inferred."])
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


def build_source_api_websocket_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceApiWebSocketRequirementsReport:
    """Extract API WebSocket requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    grouped = _group_requirements(payload)
    requirements = _merge_requirements(grouped, source_brief_id)
    return SourceApiWebSocketRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_api_websocket_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceApiWebSocketRequirement, ...]:
    """Return API WebSocket requirement records extracted from brief-shaped input."""
    return build_source_api_websocket_requirements(source).requirements


def summarize_source_api_websocket_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | SourceApiWebSocketRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic API WebSocket requirements summary."""
    if isinstance(source_or_result, SourceApiWebSocketRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_api_websocket_requirements(source_or_result).summary


def source_api_websocket_requirements_to_dict(
    report: SourceApiWebSocketRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API WebSocket requirements report to a plain dictionary."""
    return report.to_dict()


source_api_websocket_requirements_to_dict.__test__ = False


def source_api_websocket_requirements_to_dicts(
    requirements: (
        tuple[SourceApiWebSocketRequirement, ...]
        | list[SourceApiWebSocketRequirement]
        | SourceApiWebSocketRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source API WebSocket requirement records to dictionaries."""
    if isinstance(requirements, SourceApiWebSocketRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_websocket_requirements_to_dicts.__test__ = False


def source_api_websocket_requirements_to_markdown(
    report: SourceApiWebSocketRequirementsReport,
) -> str:
    """Render an API WebSocket requirements report as Markdown."""
    return report.to_markdown()


source_api_websocket_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: WebSocketRequirementType
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


def _group_requirements(payload: Mapping[str, Any]) -> dict[WebSocketRequirementType, list[_Candidate]]:
    grouped: dict[WebSocketRequirementType, list[_Candidate]] = {}
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
    grouped: dict[WebSocketRequirementType, list[_Candidate]],
    source_brief_id: str | None,
) -> tuple[SourceApiWebSocketRequirement, ...]:
    requirements: list[SourceApiWebSocketRequirement] = []
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
            SourceApiWebSocketRequirement(
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


def _matched_requirement_types(text: str) -> tuple[WebSocketRequirementType, ...]:
    return tuple(
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    )


def _matched_terms(
    requirement_type: WebSocketRequirementType,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)) for match in _TYPE_PATTERNS[requirement_type].finditer(text)
        )
    )


def _follow_up_questions(
    requirement_type: WebSocketRequirementType,
    evidence_text: str,
) -> tuple[str, ...]:
    questions = list(_BASE_QUESTIONS[requirement_type])
    # Reduce questions if evidence already provides specific answers
    if requirement_type == "authentication" and re.search(
        r"\b(?:jwt|bearer|token|api[- ]?key)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Auth method mentioned
    if requirement_type == "message_framing" and re.search(
        r"\b(?:text|binary|both)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Frame type mentioned
    if requirement_type == "ping_pong_heartbeat" and re.search(
        r"\b(?:\d+\s*(?:second|minute|ms)|interval|automatic|every)\b", evidence_text, re.I
    ):
        questions = []  # Interval or automatic mentioned
    return tuple(_dedupe(questions))


def _summary(requirements: tuple[SourceApiWebSocketRequirement, ...]) -> dict[str, Any]:
    # Calculate coverage metrics
    connection = {"connection_lifecycle", "reconnection_strategy"}
    security = {"authentication", "rate_limiting"}
    reliability = {"ping_pong_heartbeat", "message_ordering"}

    req_types = {req.requirement_type for req in requirements}
    connection_coverage = int(100 * len(req_types & connection) / len(connection)) if connection else 0
    security_coverage = int(100 * len(req_types & security) / len(security)) if security else 0
    reliability_coverage = int(100 * len(req_types & reliability) / len(reliability)) if reliability else 0

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
        "connection_coverage": connection_coverage,
        "security_coverage": security_coverage,
        "reliability_coverage": reliability_coverage,
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
    "WebSocketRequirementType",
    "SourceApiWebSocketRequirement",
    "SourceApiWebSocketRequirementsReport",
    "build_source_api_websocket_requirements",
    "extract_source_api_websocket_requirements",
    "source_api_websocket_requirements_to_dict",
    "source_api_websocket_requirements_to_dicts",
    "source_api_websocket_requirements_to_markdown",
    "summarize_source_api_websocket_requirements",
]
