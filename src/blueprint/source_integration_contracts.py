"""Extract integration contract requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


IntegrationContractType = Literal[
    "api",
    "webhook",
    "event",
    "schema",
    "oauth",
    "sso",
    "file_import_export",
    "graphql",
    "rest",
    "message_queue",
]
IntegrationContractDirection = Literal["inbound", "outbound", "bidirectional", "unknown"]

_CONTRACT_ORDER: tuple[IntegrationContractType, ...] = (
    "api",
    "rest",
    "graphql",
    "webhook",
    "event",
    "message_queue",
    "oauth",
    "sso",
    "file_import_export",
    "schema",
)
_CONTRACT_PATTERNS: dict[IntegrationContractType, re.Pattern[str]] = {
    "api": re.compile(r"\b(?:api|apis|endpoint|endpoints|sdk|integration client)\b", re.I),
    "webhook": re.compile(r"\b(?:webhook|webhooks|callback url|callback endpoint|callback)\b", re.I),
    "event": re.compile(r"\b(?:event|events|domain event|event stream|emit|emits|subscribe|subscribes)\b", re.I),
    "schema": re.compile(r"\b(?:schema|schemas|payload|contract|openapi|swagger|json schema|required fields?)\b", re.I),
    "oauth": re.compile(r"\b(?:oauth|oauth2|authorization code|refresh token|access token|client secret|scopes?)\b", re.I),
    "sso": re.compile(r"\b(?:sso|single sign[- ]on|saml|oidc|openid connect|identity provider|idp)\b", re.I),
    "file_import_export": re.compile(
        r"\b(?:import|imports|export|exports|csv|tsv|sftp|file feed|flat file|batch file|data feed)\b",
        re.I,
    ),
    "graphql": re.compile(r"\b(?:graphql|gql|query|mutation|resolver|graph api)\b", re.I),
    "rest": re.compile(r"\b(?:rest|restful|http endpoint|https endpoint|GET|POST|PUT|PATCH|DELETE)\b", re.I),
    "message_queue": re.compile(
        r"\b(?:queue|queues|message bus|message broker|kafka|sqs|sns|rabbitmq|pub/sub|pubsub|topic|"
        r"consumer|producer|dead letter|dlq)\b",
        re.I,
    ),
}
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_SPACE_RE = re.compile(r"\s+")
_FIELD_LIST_RE = re.compile(
    r"\b(?:fields?|attributes?|properties?|columns?|headers?|claims?|scopes?)\b\s*"
    r"(?:include|includes|including|are|:)\s*(?P<items>[^.;\n]+)",
    re.I,
)
_REQUESTED_FIELD_LIST_RE = re.compile(
    r"\b(?:request|requests|require|requires|with|using)\s+"
    r"(?:fields?|attributes?|properties?|columns?|headers?|claims?|scopes?)\s+(?P<items>[^.;\n]+)",
    re.I,
)
_EVENT_LIST_RE = re.compile(
    r"\b(?:events?|topics?)\b\s*(?:include|includes|including|are|:)\s*(?P<items>[^.;\n]+)",
    re.I,
)
_TOKEN_RE = re.compile(r"(?:`([^`]+)`|'([^']+)'|\"([^\"]+)\"|\b([a-z][a-z0-9_]*(?:\.[a-z0-9_]+)+)\b)")
_PROVIDER_NAME_PATTERN = r"[A-Z][A-Za-z0-9&_.-]*(?:\s+[A-Z][A-Za-z0-9&_.-]*){0,4}"
_KEY_VALUE_PROVIDER_RE = re.compile(
    r"\b(?:provider|vendor|system|service|platform|partner|idp|identity provider)\b\s*(?:is|=|:)\s*"
    rf"(?P<name>{_PROVIDER_NAME_PATTERN})"
)
_NEARBY_PROVIDER_RE = re.compile(
    r"\b(?:with|from|to|via|against|for|through|using|into)\s+"
    rf"(?P<name>{_PROVIDER_NAME_PATTERN})\s+"
    r"(?:API|APIs|REST|GraphQL|webhook|webhooks|OAuth|SSO|SAML|OIDC|queue|topic|event|events|schema|"
    r"import|export|SFTP|CSV|integration)\b"
)
_KEYWORD_PROVIDER_RE = re.compile(
    r"\b(?:API|APIs|REST|GraphQL|webhook|webhooks|OAuth|SSO|SAML|OIDC|queue|topic|event|events|schema|"
    r"import|export|SFTP|CSV|integration)\s+"
    r"(?:with|from|to|via|against|for|through|using|into)\s+"
    rf"(?P<name>{_PROVIDER_NAME_PATTERN})\b",
    re.I,
)
_VERB_PROVIDER_RE = re.compile(
    r"\b(?:call|calls|query|queries|post to|send to|publish to|export to|import from|receive from|consume from)\s+"
    rf"(?P<name>{_PROVIDER_NAME_PATTERN})\s+"
    r"(?:API|APIs|REST|GraphQL|webhook|webhooks|OAuth|SSO|SAML|OIDC|queue|topic|event|events|schema|"
    r"import|export|SFTP|CSV|integration)\b",
    re.I,
)
_PREFIX_PROVIDER_RE = re.compile(
    rf"\b(?P<name>{_PROVIDER_NAME_PATTERN})\s+"
    r"(?:API|APIs|REST|GraphQL|webhook|webhooks|OAuth|SSO|SAML|OIDC|queue|topic|event|events|schema|"
    r"import|export|SFTP|CSV|integration)\b"
)
_PROVIDER_STOPWORDS = {
    "add",
    "admin",
    "api",
    "checkout",
    "external",
    "general",
    "internal",
    "partner",
    "source",
    "the",
    "use",
}
_INBOUND_RE = re.compile(
    r"\b(?:inbound|incoming|receive|receives|received|ingest|ingests|consume|consumes|accept|accepts|"
    r"import from|webhook from|callback from|posted by|sent by)\b|\bfrom\s+[A-Z][A-Za-z0-9&_.-]+",
    re.I,
)
_OUTBOUND_RE = re.compile(
    r"\b(?:outbound|outgoing|send|sends|sent to|call|calls|push|pushes|post to|publish|publishes|emit|"
    r"emits|export to|write to|notify|notifies)\b",
    re.I,
)
_BIDIRECTIONAL_RE = re.compile(
    r"\b(?:bidirectional|bi-directional|two[- ]way|both directions|read/write|read and write|sync both ways|"
    r"import and export|export and import)\b",
    re.I,
)
_SOURCE_PAYLOAD_FIELDS = (
    "body",
    "description",
    "markdown",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "integration_points",
    "integrations",
    "architecture",
    "architecture_notes",
    "data_requirements",
    "constraints",
    "metadata",
)


@dataclass(frozen=True, slots=True)
class SourceIntegrationContract:
    """One source-backed integration contract requirement candidate."""

    provider_or_system: str = ""
    contract_type: IntegrationContractType = "api"
    required_fields_or_events: tuple[str, ...] = field(default_factory=tuple)
    direction: IntegrationContractDirection = "unknown"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    open_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "provider_or_system": self.provider_or_system,
            "contract_type": self.contract_type,
            "required_fields_or_events": list(self.required_fields_or_events),
            "direction": self.direction,
            "evidence": list(self.evidence),
            "open_questions": list(self.open_questions),
        }


def extract_source_integration_contracts(
    source_brief: Mapping[str, Any] | SourceBrief | object,
) -> tuple[SourceIntegrationContract, ...]:
    """Return integration contract requirements from one SourceBrief-shaped record."""
    brief = _source_brief_payload(source_brief)
    if not brief:
        return ()

    grouped: dict[tuple[IntegrationContractType, str, IntegrationContractDirection], dict[str, list[str]]] = {}
    for source_field, value in _candidate_values(brief):
        for segment in _segments(value):
            contract_types = _contract_types(segment)
            if not contract_types:
                continue
            provider = _provider_or_system(segment) or _provider_from_metadata_field(source_field, value)
            direction = _direction(segment)
            fields_or_events = _required_fields_or_events(segment)
            for contract_type in contract_types:
                key = (contract_type, provider, direction)
                bucket = grouped.setdefault(key, {"fields_or_events": [], "evidence": []})
                bucket["fields_or_events"].extend(fields_or_events)
                bucket["evidence"].append(_evidence_snippet(source_field, segment))

    records = [
        SourceIntegrationContract(
            provider_or_system=provider,
            contract_type=contract_type,
            required_fields_or_events=tuple(_dedupe(values["fields_or_events"])),
            direction=direction,
            evidence=tuple(_dedupe(values["evidence"])),
            open_questions=_open_questions(contract_type, provider, direction, values["fields_or_events"]),
        )
        for (contract_type, provider, direction), values in grouped.items()
    ]
    return tuple(
        sorted(
            records,
            key=lambda record: (
                _CONTRACT_ORDER.index(record.contract_type),
                record.provider_or_system.casefold(),
                record.direction,
                record.evidence,
            ),
        )
    )


def source_integration_contracts_to_dicts(
    records: tuple[SourceIntegrationContract, ...] | list[SourceIntegrationContract],
) -> list[dict[str, Any]]:
    """Serialize integration contract records to dictionaries."""
    return [record.to_dict() for record in records]


def _source_brief_payload(source_brief: Mapping[str, Any] | SourceBrief | object) -> dict[str, Any]:
    if hasattr(source_brief, "model_dump"):
        value = source_brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = SourceBrief.model_validate(source_brief).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(source_brief) if isinstance(source_brief, Mapping) else {}


def _candidate_values(brief: Mapping[str, Any]) -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = []
    for field_name in ("title", "domain", "summary"):
        if field_name in brief:
            candidates.append((field_name, brief[field_name]))

    payload = brief.get("source_payload")
    if isinstance(payload, Mapping):
        visited: set[str] = set()
        for field_name in _SOURCE_PAYLOAD_FIELDS:
            if field_name in payload:
                source_field = f"source_payload.{field_name}"
                candidates.extend(_flatten_value(payload[field_name], source_field))
                visited.add(source_field)
        for source_field, value in _flatten_value(payload, "source_payload"):
            if not _is_under_visited_field(source_field, visited):
                candidates.append((source_field, value))

    links = brief.get("source_links")
    if isinstance(links, Mapping):
        for source_field, value in _flatten_value(links, "source_links"):
            candidates.append((source_field, value))
    return candidates


def _flatten_value(value: Any, source_field: str) -> list[tuple[str, Any]]:
    if isinstance(value, Mapping):
        flattened: list[tuple[str, Any]] = []
        for key in sorted(value, key=lambda item: str(item)):
            flattened.extend(_flatten_value(value[key], f"{source_field}.{key}"))
        return flattened
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        flattened = []
        for index, item in enumerate(items):
            flattened.extend(_flatten_value(item, f"{source_field}[{index}]"))
        return flattened
    return [(source_field, value)]


def _segments(value: Any) -> list[str]:
    text = _optional_text(value)
    if text is None:
        return []
    segments: list[str] = []
    for part in _SENTENCE_SPLIT_RE.split(text):
        cleaned = _clean_text(part)
        if cleaned:
            segments.append(cleaned)
    return segments


def _contract_types(text: str) -> tuple[IntegrationContractType, ...]:
    return tuple(contract_type for contract_type in _CONTRACT_ORDER if _CONTRACT_PATTERNS[contract_type].search(text))


def _provider_or_system(text: str) -> str:
    for pattern in (
        _KEY_VALUE_PROVIDER_RE,
        _VERB_PROVIDER_RE,
        _NEARBY_PROVIDER_RE,
        _KEYWORD_PROVIDER_RE,
        _PREFIX_PROVIDER_RE,
    ):
        match = pattern.search(text)
        if match is not None:
            provider = _clean_provider(match.group("name"))
            if _valid_provider(provider):
                return provider
    return ""


def _provider_from_metadata_field(source_field: str, value: Any) -> str:
    if not re.search(r"(?:provider|vendor|system|service|platform|partner|idp)", source_field, re.I):
        return ""
    text = _optional_text(value)
    if text is None or _contract_types(text):
        return ""
    return _clean_provider(text)


def _direction(text: str) -> IntegrationContractDirection:
    if _BIDIRECTIONAL_RE.search(text):
        return "bidirectional"
    inbound = bool(_INBOUND_RE.search(text))
    outbound = bool(_OUTBOUND_RE.search(text))
    if inbound and outbound:
        return "bidirectional"
    if inbound:
        return "inbound"
    if outbound:
        return "outbound"
    return "unknown"


def _required_fields_or_events(text: str) -> tuple[str, ...]:
    values: list[str] = []
    for pattern in (_FIELD_LIST_RE, _REQUESTED_FIELD_LIST_RE, _EVENT_LIST_RE):
        for match in pattern.finditer(text):
            values.extend(_split_items(match.group("items")))
    for match in _TOKEN_RE.finditer(text):
        values.append(next(group for group in match.groups() if group))
    return tuple(_dedupe(values))


def _split_items(value: str) -> list[str]:
    items = re.split(r",|\band\b", value)
    return [_clean_text(item) for item in items if _clean_text(item)]


def _open_questions(
    contract_type: IntegrationContractType,
    provider: str,
    direction: IntegrationContractDirection,
    fields_or_events: list[str],
) -> tuple[str, ...]:
    questions: list[str] = []
    if not provider:
        questions.append("Confirm the provider or internal system that owns this contract.")
    if direction == "unknown":
        questions.append("Confirm whether the integration is inbound, outbound, or bidirectional.")
    if not fields_or_events:
        questions.append("List required fields, events, claims, topics, or file columns for this contract.")
    if contract_type in {"api", "rest", "graphql", "webhook"}:
        questions.append("Confirm versioning, authentication, error handling, and retry/idempotency expectations.")
    if contract_type in {"oauth", "sso"}:
        questions.append("Confirm identity scopes, claims, token/session lifetime, and security review requirements.")
    if contract_type in {"event", "message_queue"}:
        questions.append("Confirm ordering, delivery guarantees, retry, dead-letter, and replay expectations.")
    if contract_type == "file_import_export":
        questions.append("Confirm file format, transport, cadence, validation, and failure handling.")
    if contract_type == "schema":
        questions.append("Confirm the canonical schema source and compatibility rules.")
    return tuple(_dedupe(questions))


def _clean_provider(value: str) -> str:
    text = _clean_text(value)
    text = re.sub(r"\b(?:the|a|an|our|internal|external|third[- ]party)\b\s*", "", text, flags=re.I)
    text = re.sub(
        r"\s+(?:API|APIs|REST|GraphQL|webhook|webhooks|OAuth|SSO|SAML|OIDC|queue|topic|event|events|"
        r"schema|import|export|SFTP|CSV|integration)\b.*$",
        "",
        text,
        flags=re.I,
    )
    text = re.sub(
        r"\s+(?:must|should|needs?|will|uses?|requires?|requests?|to|from|for|using|via)\b.*$",
        "",
        text,
        flags=re.I,
    )
    return text.strip(" .,:;-")


def _valid_provider(value: str) -> bool:
    if not value:
        return False
    if not any(char.isupper() for char in value):
        return False
    return value.casefold() not in _PROVIDER_STOPWORDS


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _SPACE_RE.sub(" ", str(value)).strip()
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _dedupe(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(str(value))
        key = text.casefold()
        if text and key not in seen:
            seen.add(key)
            result.append(text)
    return result


def _is_under_visited_field(source_field: str, visited_fields: set[str]) -> bool:
    return any(
        source_field == visited
        or source_field.startswith(f"{visited}.")
        or source_field.startswith(f"{visited}[")
        for visited in visited_fields
    )


__all__ = [
    "IntegrationContractDirection",
    "IntegrationContractType",
    "SourceIntegrationContract",
    "extract_source_integration_contracts",
    "source_integration_contracts_to_dicts",
]
