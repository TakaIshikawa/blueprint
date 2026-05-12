"""Extract source-level Model Context Protocol integration requirements."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


McpRequirementType = Literal[
    "server_role",
    "client_role",
    "tool_exposure",
    "resource_access",
    "auth",
    "consent",
    "sandboxing",
    "rate_limits",
    "audit_logging",
    "prompt_context_boundaries",
    "fallback_behavior",
]
McpRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[McpRequirementType, ...] = (
    "server_role",
    "client_role",
    "tool_exposure",
    "resource_access",
    "auth",
    "consent",
    "sandboxing",
    "rate_limits",
    "audit_logging",
    "prompt_context_boundaries",
    "fallback_behavior",
)
_CONFIDENCE_ORDER: dict[McpRequirementConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_MCP_CONTEXT_RE = re.compile(r"\b(?:model context protocol|mcp|mcp server|mcp client|tools?|resources?)\b", re.I)
_NO_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:mcp|model context protocol|tool exposure|resource access)\b"
    r".{0,100}\b(?:scope|required|needed|changes?|impact)\b",
    re.I,
)
_SCANNED_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "constraints",
    "risks",
    "security",
    "integration_points",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_TYPE_PATTERNS: dict[McpRequirementType, re.Pattern[str]] = {
    "server_role": re.compile(r"\b(?:mcp server|model context protocol server|server role|serve tools|expose an mcp endpoint)\b", re.I),
    "client_role": re.compile(r"\b(?:mcp client|model context protocol client|client role|connect to mcp|consume mcp)\b", re.I),
    "tool_exposure": re.compile(r"\b(?:tool exposure|expose tools?|tool catalog|tool definitions?|tool invocation|available tools?)\b", re.I),
    "resource_access": re.compile(r"\b(?:resource access|mcp resources?|read resources?|resource uri|files?|database records?|workspace resources?)\b", re.I),
    "auth": re.compile(r"\b(?:auth|oauth|api key|token|credential|authorization|authentication|scopes?)\b", re.I),
    "consent": re.compile(r"\b(?:consent|user approval|approve tool|human approval|permission prompt|confirmation)\b", re.I),
    "sandboxing": re.compile(r"\b(?:sandbox|sandboxing|isolation|restricted filesystem|container|egress restriction|allowlist|denylist)\b", re.I),
    "rate_limits": re.compile(r"\b(?:rate limit|quota|throttle|concurrency limit|tool call limit|request limit)\b", re.I),
    "audit_logging": re.compile(r"\b(?:audit logs?|audit logging|audit trail|log tool calls?|invocation log|access log|security event)\b", re.I),
    "prompt_context_boundaries": re.compile(r"\b(?:prompt boundary|context boundary|context injection|prompt injection|system prompt|context window|data boundary|redaction)\b", re.I),
    "fallback_behavior": re.compile(r"\b(?:fallback|degrade gracefully|tool unavailable|server unavailable|timeout behavior|retry later|manual path)\b", re.I),
}
_VALUE_RE = re.compile(r"\b(?:mcp server|mcp client|oauth|api key|token|sandbox|rate limit|audit log|prompt injection|\d+\s*(?:requests?|calls?)|fallback)\b", re.I)
_NOTES: dict[McpRequirementType, str] = {
    "server_role": "Define whether the integration hosts an MCP server and which capabilities it serves.",
    "client_role": "Define which MCP servers the client connects to and how sessions are managed.",
    "tool_exposure": "Inventory exposed tools, schemas, inputs, side effects, and ownership.",
    "resource_access": "Describe accessible resources, URI patterns, authorization, and data minimization.",
    "auth": "Specify authentication, authorization scopes, credential storage, and token rotation.",
    "consent": "Define consent and approval prompts before sensitive tool or resource access.",
    "sandboxing": "Constrain tool execution with sandboxing, filesystem, network, and process limits.",
    "rate_limits": "Set request, tool-call, and concurrency limits for MCP operations.",
    "audit_logging": "Log tool invocation, resource access, actor, result, and timestamp.",
    "prompt_context_boundaries": "Prevent prompt/context leakage and separate trusted instructions from retrieved context.",
    "fallback_behavior": "Define behavior when MCP servers, tools, or resources are unavailable.",
}


@dataclass(frozen=True, slots=True)
class SourceModelContextProtocolRequirement:
    requirement_type: McpRequirementType
    source_field: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: McpRequirementConfidence = "medium"
    value: str | None = None
    planning_notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def category(self) -> McpRequirementType:
        return self.requirement_type

    def to_dict(self) -> dict[str, Any]:
        return {
            "requirement_type": self.requirement_type,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "planning_notes": list(self.planning_notes),
        }


@dataclass(frozen=True, slots=True)
class SourceModelContextProtocolRequirementsReport:
    source_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceModelContextProtocolRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceModelContextProtocolRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceModelContextProtocolRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [item.to_dict() for item in self.requirements],
            "records": [item.to_dict() for item in self.records],
            "findings": [item.to_dict() for item in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source Model Context Protocol Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Role coverage: {self.summary.get('role_coverage', 0)}%",
            f"- Capability coverage: {self.summary.get('capability_coverage', 0)}%",
            f"- Security coverage: {self.summary.get('security_coverage', 0)}%",
            f"- Fallback coverage: {self.summary.get('fallback_coverage', 0)}%",
        ]
        if not self.requirements:
            lines.extend(["", "No Model Context Protocol requirements were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Type | Confidence | Source | Evidence |", "| --- | --- | --- | --- |"])
        for item in self.requirements:
            lines.append(f"| {item.requirement_type} | {item.confidence} | {_markdown_cell(item.source_field)} | {_markdown_cell('; '.join(item.evidence))} |")
        return "\n".join(lines)


def build_source_model_context_protocol_requirements(source: Any) -> SourceModelContextProtocolRequirementsReport:
    source_id, title, payload = _source_payload(source)
    requirements = tuple(_merge(_group(payload)))
    return SourceModelContextProtocolRequirementsReport(
        source_id=source_id,
        title=title,
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_model_context_protocol_requirements(source: Any) -> tuple[SourceModelContextProtocolRequirement, ...]:
    return build_source_model_context_protocol_requirements(source).requirements


def derive_source_model_context_protocol_requirements(source: Any) -> SourceModelContextProtocolRequirementsReport:
    return build_source_model_context_protocol_requirements(source)


def generate_source_model_context_protocol_requirements(source: Any) -> SourceModelContextProtocolRequirementsReport:
    return build_source_model_context_protocol_requirements(source)


def summarize_source_model_context_protocol_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceModelContextProtocolRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_model_context_protocol_requirements(source_or_report).summary


def source_model_context_protocol_requirements_to_dict(report: SourceModelContextProtocolRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_model_context_protocol_requirements_to_dict.__test__ = False


def source_model_context_protocol_requirements_to_dicts(items: Any) -> list[dict[str, Any]]:
    if isinstance(items, SourceModelContextProtocolRequirementsReport):
        return items.to_dicts()
    return [item.to_dict() for item in items]


source_model_context_protocol_requirements_to_dicts.__test__ = False


def source_model_context_protocol_requirements_to_markdown(report: SourceModelContextProtocolRequirementsReport) -> str:
    return report.to_markdown()


source_model_context_protocol_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: McpRequirementType
    source_field: str
    evidence: str
    confidence: McpRequirementConfidence
    value: str | None


def _source_payload(source: Any) -> tuple[str | None, str | None, Mapping[str, Any]]:
    if isinstance(source, str):
        return None, None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), _optional_text(payload.get("title")), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _source_id(payload), _optional_text(payload.get("title")), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
        return _source_id(payload), _optional_text(payload.get("title")), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = {name: getattr(source, name) for name in _SCANNED_FIELDS + ("id", "source_id", "source_brief_id") if hasattr(source, name)}
        return _source_id(payload), _optional_text(payload.get("title")), payload
    return None, None, {}


def _group(payload: Mapping[str, Any]) -> dict[McpRequirementType, list[_Candidate]]:
    grouped: dict[McpRequirementType, list[_Candidate]] = {}
    for field, text in _candidate_texts(payload):
        for segment in _segments(text):
            if _NO_SCOPE_RE.search(segment):
                continue
            matched = [name for name in _TYPE_ORDER if _TYPE_PATTERNS[name].search(segment)]
            if not matched:
                continue
            confidence: McpRequirementConfidence = "high" if _MCP_CONTEXT_RE.search(segment) else "medium"
            for requirement_type in matched:
                grouped.setdefault(requirement_type, []).append(
                    _Candidate(requirement_type, field, _evidence(field, segment), confidence, _value(segment))
                )
    return grouped


def _merge(grouped: Mapping[McpRequirementType, list[_Candidate]]) -> list[SourceModelContextProtocolRequirement]:
    records: list[SourceModelContextProtocolRequirement] = []
    for requirement_type in _TYPE_ORDER:
        candidates = grouped.get(requirement_type, [])
        if not candidates:
            continue
        fields = sorted(_dedupe(candidate.source_field for candidate in candidates), key=str.casefold)
        confidence = sorted((candidate.confidence for candidate in candidates), key=lambda item: _CONFIDENCE_ORDER[item])[0]
        values = _dedupe(candidate.value for candidate in candidates if candidate.value)
        records.append(
            SourceModelContextProtocolRequirement(
                requirement_type=requirement_type,
                source_field=fields[0],
                evidence=tuple(_dedupe_evidence(candidate.evidence for candidate in candidates))[:5],
                confidence=confidence,
                value=values[0] if values else None,
                planning_notes=(_NOTES[requirement_type],),
            )
        )
    return records


def _summary(requirements: tuple[SourceModelContextProtocolRequirement, ...]) -> dict[str, Any]:
    types = {item.requirement_type for item in requirements}
    role = {"server_role", "client_role"}
    capability = {"tool_exposure", "resource_access", "rate_limits"}
    security = {"auth", "consent", "sandboxing", "audit_logging", "prompt_context_boundaries"}
    fallback = {"fallback_behavior"}
    return {
        "requirement_count": len(requirements),
        "requirement_types": [item.requirement_type for item in requirements],
        "type_counts": {name: sum(1 for item in requirements if item.requirement_type == name) for name in _TYPE_ORDER},
        "confidence_counts": {name: sum(1 for item in requirements if item.confidence == name) for name in _CONFIDENCE_ORDER},
        "security_sensitive_types": [name for name in _TYPE_ORDER if name in types & security],
        "role_coverage": int(100 * len(types & role) / len(role)),
        "capability_coverage": int(100 * len(types & capability) / len(capability)),
        "security_coverage": int(100 * len(types & security) / len(security)),
        "fallback_coverage": 100 if fallback <= types else 0,
        "status": "ready_for_planning" if requirements else "no_mcp_language",
    }


def _candidate_texts(payload: Mapping[str, Any], prefix: str | None = None) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for key in sorted(payload, key=str):
        if prefix is None and key not in _SCANNED_FIELDS:
            continue
        value = payload[key]
        field = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, str):
            if text := _optional_text(value):
                texts.append((field, text))
        elif isinstance(value, Mapping):
            texts.extend(_candidate_texts(value, field))
        elif isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
            for index, item in enumerate(value):
                if isinstance(item, Mapping):
                    texts.extend(_candidate_texts(item, f"{field}[{index}]"))
                elif text := _optional_text(item):
                    texts.append((f"{field}[{index}]", text))
    return texts


def _segments(text: str) -> list[str]:
    return [cleaned for part in _SPLIT_RE.split(text) if (cleaned := _clean(part))]


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return _optional_text(payload.get("id") or payload.get("source_brief_id") or payload.get("source_id"))


def _value(text: str) -> str | None:
    match = _VALUE_RE.search(text)
    return _clean(match.group(0)).casefold() if match else None


def _evidence(field: str, text: str) -> str:
    cleaned = _clean(text)
    if len(cleaned) > 220:
        cleaned = f"{cleaned[:217].rstrip()}..."
    return f"{field}: {cleaned}"


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean(str(value))
    return text or None


def _clean(value: str) -> str:
    return _SPACE_RE.sub(" ", _BULLET_RE.sub("", str(value).strip())).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[str] = set()
    for value in values:
        key = str(value).casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        _, _, statement = value.partition(": ")
        key = _clean(statement or value).casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return sorted(result, key=str.casefold)


def _markdown_cell(value: str) -> str:
    return _clean(value).replace("|", "\\|").replace("\n", " ")


__all__ = [
    "SourceModelContextProtocolRequirement",
    "SourceModelContextProtocolRequirementsReport",
    "build_source_model_context_protocol_requirements",
    "derive_source_model_context_protocol_requirements",
    "extract_source_model_context_protocol_requirements",
    "generate_source_model_context_protocol_requirements",
    "source_model_context_protocol_requirements_to_dict",
    "source_model_context_protocol_requirements_to_dicts",
    "source_model_context_protocol_requirements_to_markdown",
    "summarize_source_model_context_protocol_requirements",
]
