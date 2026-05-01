"""Extract likely API breaking-change signals from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from blueprint.domain.models import SourceBrief


ApiBreakingChangeType = Literal[
    "removed_field",
    "renamed_field",
    "response_shape_change",
    "required_parameter_change",
    "authentication_change",
    "status_code_change",
    "pagination_change",
    "webhook_payload_change",
    "versioning_requirement",
]
ApiBreakingChangeSeverity = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_ENDPOINT_RE = re.compile(
    r"(?:\b(?:GET|POST|PUT|PATCH|DELETE)\s+)?`?(/(?:[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=-]+))`?"
)
_FIELD_RE = re.compile(r"(?:`([^`]+)`|'([^']+)'|\"([^\"]+)\")")
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "problem_statement",
    "context",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "data_requirements",
    "integration_points",
    "risks",
    "constraints",
    "metadata",
)
_MAX_EVIDENCE_PER_RECORD = 5
_CHANGE_ORDER: dict[ApiBreakingChangeType, int] = {
    "removed_field": 0,
    "authentication_change": 1,
    "webhook_payload_change": 2,
    "renamed_field": 3,
    "response_shape_change": 4,
    "required_parameter_change": 5,
    "status_code_change": 6,
    "pagination_change": 7,
    "versioning_requirement": 8,
}
_SEVERITY_ORDER: dict[ApiBreakingChangeSeverity, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_HIGH_SEVERITY_TYPES: frozenset[ApiBreakingChangeType] = frozenset(
    {"removed_field", "authentication_change", "webhook_payload_change"}
)

_PATTERNS: dict[ApiBreakingChangeType, tuple[re.Pattern[str], ...]] = {
    "removed_field": (
        re.compile(
            r"\b(?:remove|removes|removed|drop|drops|dropped|delete|deleted|deprecate without fallback)\b"
            r".{0,80}\b(?:field|fields|property|properties|attribute|attributes|key|keys|claim|claims|column|columns)\b",
            re.I,
        ),
        re.compile(
            r"\b(?:field|fields|property|properties|attribute|attributes|key|keys|claim|claims|column|columns)\b"
            r".{0,80}\b(?:is|are|will be|must be)?\s*(?:removed|dropped|deleted|no longer returned|no longer included)\b",
            re.I,
        ),
    ),
    "renamed_field": (
        re.compile(
            r"\b(?:rename|renames|renamed|renaming)\b.{0,100}\b(?:field|property|attribute|key|claim|column)s?\b",
            re.I,
        ),
        re.compile(
            r"\b(?:field|property|attribute|key|claim|column)s?\b.{0,80}\b(?:renamed|becomes|changed from)\b",
            re.I,
        ),
    ),
    "response_shape_change": (
        re.compile(
            r"\b(?:response shape|response schema|response body|payload shape|json shape|object shape)\b"
            r".{0,100}\b(?:change|changes|changed|changing|flatten|nested|array|object)\b",
            re.I,
        ),
        re.compile(
            r"\b(?:return|returns|returned|respond|responds)\b.{0,80}\b(?:array|object|nested|flattened|envelope|wrapper)\b",
            re.I,
        ),
    ),
    "required_parameter_change": (
        re.compile(
            r"\b(?:new|required|mandatory)\b.{0,80}\b(?:parameter|param|query param|header|field|argument|body field)s?\b",
            re.I,
        ),
        re.compile(
            r"\b(?:parameter|param|query param|header|field|argument|body field)s?\b"
            r".{0,80}\b(?:now required|required|mandatory|must be supplied|must be provided)\b",
            re.I,
        ),
    ),
    "authentication_change": (
        re.compile(
            r"\b(?:auth|authentication|authorization|oauth|oauth2|oidc|sso|api key|bearer token|jwt|scope|scopes|"
            r"permission|permissions|token|tokens|client secret|mfa)\b"
            r".{0,100}\b(?:change|changes|changed|require|required|requires|migrate|migration|rotate|replace|enforce)\b",
            re.I,
        ),
        re.compile(
            r"\b(?:require|required|requires|enforce|migrate|rotate|replace)\b.{0,100}"
            r"\b(?:oauth|oauth2|bearer token|jwt|api key|scope|scopes|permission|permissions|auth|authentication)\b",
            re.I,
        ),
    ),
    "status_code_change": (
        re.compile(
            r"\b(?:status code|status codes|http status|response code|error code|error codes)\b"
            r".{0,100}\b(?:change|changes|changed|changing|return|returns|now|instead)\b",
            re.I,
        ),
        re.compile(r"\b(?:200|201|202|204|301|302|400|401|403|404|409|422|429|500|503)\b.{0,40}\b(?:instead of|replaces|instead)\b", re.I),
    ),
    "pagination_change": (
        re.compile(
            r"\b(?:pagination|paginated|cursor|cursor-based|offset|page token|next_token|next token|limit|page size)\b"
            r".{0,100}\b(?:change|changes|changed|required|requires|migrate|replace|instead)\b",
            re.I,
        ),
        re.compile(
            r"\b(?:replace|replaces|migrate|switch)\b.{0,80}\b(?:offset|cursor|pagination|page token|next token)\b",
            re.I,
        ),
    ),
    "webhook_payload_change": (
        re.compile(
            r"\b(?:webhook|webhooks|callback|callbacks)\b.{0,120}"
            r"\b(?:payload|schema|body|event|events|signature|signatures|field|fields|shape)\b.{0,80}"
            r"\b(?:change|changes|changed|remove|removed|rename|renamed|required|version)\b",
            re.I,
        ),
        re.compile(
            r"\b(?:payload|schema|body|event|events|signature|signatures)\b.{0,80}"
            r"\b(?:change|changes|changed|remove|removed|rename|renamed)\b.{0,80}\b(?:webhook|webhooks|callback|callbacks)\b",
            re.I,
        ),
    ),
    "versioning_requirement": (
        re.compile(
            r"\b(?:api version|api versions|versioned endpoint|version header|versioning)\b"
            r".{0,100}\b(?:required|require|requires|migrate|migration|upgrade|pin|send|use)\b",
            re.I,
        ),
        re.compile(
            r"\b(?:require|required|requires|migrate|upgrade|pin|send|use)\b.{0,100}"
            r"\b(?:api version|version header|versioning)\b",
            re.I,
        ),
    ),
}

_MIGRATION_QUESTIONS: dict[ApiBreakingChangeType, tuple[str, ...]] = {
    "removed_field": (
        "Which consumers still read the removed field, and what replacement or fallback do they need?",
    ),
    "renamed_field": (
        "Can old and new field names be accepted during a compatibility window?",
    ),
    "response_shape_change": (
        "Which clients, serializers, and contract tests assume the previous response shape?",
    ),
    "required_parameter_change": (
        "How will existing callers supply the newly required parameter or header?",
    ),
    "authentication_change": (
        "Which credentials, scopes, token flows, and rollout windows must be migrated before enforcement?",
    ),
    "status_code_change": (
        "Which clients branch on the old status or error code and need updated handling?",
    ),
    "pagination_change": (
        "How will callers migrate cursors, limits, and backfills from the old pagination contract?",
    ),
    "webhook_payload_change": (
        "Which webhook receivers need schema, signature, replay, and idempotency updates before rollout?",
    ),
    "versioning_requirement": (
        "Which callers must pin, send, or upgrade API versions, and what is the default for omitted versions?",
    ),
}
_PLAN_TAGS: dict[ApiBreakingChangeType, tuple[str, ...]] = {
    "removed_field": ("api-contract", "compatibility", "migration"),
    "renamed_field": ("api-contract", "compatibility", "client-update"),
    "response_shape_change": ("api-contract", "contract-tests", "client-update"),
    "required_parameter_change": ("api-contract", "client-update", "validation"),
    "authentication_change": ("api-contract", "auth-migration", "security"),
    "status_code_change": ("api-contract", "error-handling", "contract-tests"),
    "pagination_change": ("api-contract", "pagination", "client-update"),
    "webhook_payload_change": ("api-contract", "webhook-migration", "contract-tests"),
    "versioning_requirement": ("api-contract", "versioning", "migration"),
}


@dataclass(frozen=True, slots=True)
class SourceApiBreakingChangeRecord:
    """One likely source-backed API breaking-change signal."""

    change_type: ApiBreakingChangeType
    affected_surface: str
    severity: ApiBreakingChangeSeverity
    evidence: tuple[str, ...] = field(default_factory=tuple)
    migration_questions: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_tags: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "change_type": self.change_type,
            "affected_surface": self.affected_surface,
            "severity": self.severity,
            "evidence": list(self.evidence),
            "migration_questions": list(self.migration_questions),
            "suggested_plan_tags": list(self.suggested_plan_tags),
        }


@dataclass(frozen=True, slots=True)
class SourceApiBreakingChangeReport:
    """API breaking-change report for one source brief payload."""

    source_brief_id: str | None = None
    title: str | None = None
    records: tuple[SourceApiBreakingChangeRecord, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return breaking-change records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Breaking Changes"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        lines = [title, "", "## Summary", ""]
        lines.extend(
            [
                f"- Change count: {self.summary.get('change_count', 0)}",
                f"- High severity count: {self.summary.get('high_severity_count', 0)}",
                f"- Change types: {', '.join(self.summary.get('change_types', [])) or 'none'}",
            ]
        )
        if not self.records:
            lines.extend(["", "No source API breaking-change signals were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Changes",
                "",
                "| Change Type | Surface | Severity | Evidence | Migration Questions | Suggested Tags |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{record.change_type} | "
                f"{_markdown_cell(record.affected_surface)} | "
                f"{record.severity} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(record.migration_questions) or 'none')} | "
                f"{_markdown_cell(', '.join(record.suggested_plan_tags) or 'none')} |"
            )
        return "\n".join(lines)


def build_source_api_breaking_change_report(
    source: Mapping[str, Any] | SourceBrief | Any,
) -> SourceApiBreakingChangeReport:
    """Build a normalized API breaking-change report for a source brief-like payload."""
    brief = _payload_dict(source)
    source_brief_id = _optional_text(brief.get("id")) or _optional_text(brief.get("source_id"))
    title = _optional_text(brief.get("title"))
    grouped: dict[tuple[ApiBreakingChangeType, str], list[str]] = {}

    for source_field, text in _candidate_texts(brief):
        for segment in _segments(text):
            for change_type in _matched_change_types(segment):
                surface = _affected_surface(segment, source_field)
                key = (change_type, surface)
                grouped.setdefault(key, []).append(_evidence_snippet(source_field, segment))

    records = tuple(
        sorted(
            (
                SourceApiBreakingChangeRecord(
                    change_type=change_type,
                    affected_surface=surface,
                    severity=_severity(change_type),
                    evidence=tuple(_dedupe(evidence)[:_MAX_EVIDENCE_PER_RECORD]),
                    migration_questions=_MIGRATION_QUESTIONS[change_type],
                    suggested_plan_tags=_PLAN_TAGS[change_type],
                )
                for (change_type, surface), evidence in grouped.items()
                if _dedupe(evidence)
            ),
            key=_record_sort_key,
        )
    )
    return SourceApiBreakingChangeReport(
        source_brief_id=source_brief_id,
        title=title,
        records=records,
        summary=_summary(records),
    )


def summarize_source_api_breaking_changes(
    source: Mapping[str, Any] | SourceBrief | Any,
) -> SourceApiBreakingChangeReport:
    """Compatibility alias for building source API breaking-change reports."""
    return build_source_api_breaking_change_report(source)


def source_api_breaking_change_report_to_dict(
    report: SourceApiBreakingChangeReport,
) -> dict[str, Any]:
    """Serialize a source API breaking-change report to a plain dictionary."""
    return report.to_dict()


source_api_breaking_change_report_to_dict.__test__ = False


def source_api_breaking_change_report_to_markdown(
    report: SourceApiBreakingChangeReport,
) -> str:
    """Render a source API breaking-change report as Markdown."""
    return report.to_markdown()


source_api_breaking_change_report_to_markdown.__test__ = False


def _summary(records: tuple[SourceApiBreakingChangeRecord, ...]) -> dict[str, Any]:
    return {
        "change_count": len(records),
        "high_severity_count": sum(1 for record in records if record.severity == "high"),
        "change_types": sorted({record.change_type for record in records}, key=lambda value: _CHANGE_ORDER[value]),
        "severity_counts": {
            severity: sum(1 for record in records if record.severity == severity)
            for severity in ("high", "medium", "low")
        },
        "change_type_counts": {
            change_type: sum(1 for record in records if record.change_type == change_type)
            for change_type in _CHANGE_ORDER
        },
    }


def _payload_dict(source: Mapping[str, Any] | SourceBrief | Any) -> dict[str, Any]:
    if isinstance(source, SourceBrief):
        return source.model_dump(mode="python")
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    if isinstance(source, Mapping):
        return dict(source)
    return {}


def _candidate_texts(brief: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in _SCANNED_FIELDS:
        value = brief.get(field_name)
        if field_name == "metadata":
            texts.extend(_nested_texts(value, field_name))
            continue
        for index, text in enumerate(_strings(value)):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))

    if isinstance(brief.get("source_payload"), Mapping):
        for field_name in _SCANNED_FIELDS:
            if field_name in brief["source_payload"]:
                texts.extend(_nested_texts(brief["source_payload"][field_name], f"source_payload.{field_name}"))
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
    for raw_segment in re.split(r"(?<=[.!?])\s+|\n+", text):
        segment = _text(raw_segment)
        if segment:
            segments.append(segment)
    return segments


def _matched_change_types(text: str) -> tuple[ApiBreakingChangeType, ...]:
    matches = [
        change_type
        for change_type, patterns in _PATTERNS.items()
        if any(pattern.search(text) for pattern in patterns)
    ]
    if "webhook_payload_change" in matches and "removed_field" in matches:
        matches.remove("removed_field")
    return tuple(matches)


def _affected_surface(text: str, source_field: str) -> str:
    if endpoint := _endpoint(text):
        return endpoint
    if field := _quoted_field(text):
        return field
    field_hint = source_field.removeprefix("source_payload.")
    if "." in field_hint:
        field_hint = field_hint.split(".", 1)[0]
    return field_hint or "api_contract"


def _endpoint(text: str) -> str | None:
    match = _ENDPOINT_RE.search(text)
    if not match:
        return None
    value = match.group(1).rstrip(".,;)")
    return value if value != "/" else None


def _quoted_field(text: str) -> str | None:
    match = _FIELD_RE.search(text)
    if not match:
        return None
    for group in match.groups():
        if group:
            return group.strip()
    return None


def _severity(change_type: ApiBreakingChangeType) -> ApiBreakingChangeSeverity:
    if change_type in _HIGH_SEVERITY_TYPES:
        return "high"
    return "medium"


def _record_sort_key(record: SourceApiBreakingChangeRecord) -> tuple[Any, ...]:
    return (
        _SEVERITY_ORDER[record.severity],
        _CHANGE_ORDER[record.change_type],
        record.affected_surface.casefold(),
        record.evidence,
    )


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


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


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "ApiBreakingChangeSeverity",
    "ApiBreakingChangeType",
    "SourceApiBreakingChangeRecord",
    "SourceApiBreakingChangeReport",
    "build_source_api_breaking_change_report",
    "source_api_breaking_change_report_to_dict",
    "source_api_breaking_change_report_to_markdown",
    "summarize_source_api_breaking_changes",
]
