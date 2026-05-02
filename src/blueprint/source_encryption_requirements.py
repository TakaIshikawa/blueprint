"""Extract encryption and key-management requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


SourceEncryptionRequirementCategory = Literal[
    "at_rest_encryption",
    "in_transit_encryption",
    "tls",
    "kms",
    "customer_managed_keys",
    "key_rotation",
    "secrets_management",
    "token_encryption",
    "envelope_encryption",
    "field_level_encryption",
]
SourceEncryptionRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SourceEncryptionRequirementCategory, ...] = (
    "at_rest_encryption",
    "in_transit_encryption",
    "tls",
    "kms",
    "customer_managed_keys",
    "key_rotation",
    "secrets_management",
    "token_encryption",
    "envelope_encryption",
    "field_level_encryption",
)
_CONFIDENCE_ORDER: dict[SourceEncryptionRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|compliance|policy|cannot ship|block(?:er|ing)?)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:encryption|tls|kms|keys?|secrets?|token).*?"
    r"\b(?:in scope|required|needed|changes?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:encrypt|encryption|crypto|cryptographic|tls|ssl|https|kms|keys?|cmk|byok|"
    r"hyok|secrets?|tokens?|security|compliance|requirements?|constraints?|controls?|"
    r"acceptance|metadata)",
    re.I,
)
_ENCRYPTION_CONTEXT_RE = re.compile(
    r"\b(?:encrypt(?:ion|ed|s)?|cryptograph(?:y|ic)|ciphertext|tls|ssl|https|"
    r"kms|key management|customer[- ]managed keys?|cmk|byok|hyok|key rotation|"
    r"secrets?|tokens?|envelope encryption|field[- ]level encryption)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[SourceEncryptionRequirementCategory, re.Pattern[str]] = {
    "at_rest_encryption": re.compile(
        r"\b(?:encrypt(?:ed|ion)? at rest|at[- ]rest encryption|data at rest|"
        r"database encryption|disk encryption|storage encryption|s3 encryption|"
        r"encrypted storage|encrypted database)\b",
        re.I,
    ),
    "in_transit_encryption": re.compile(
        r"\b(?:encrypt(?:ed|ion)? in transit|in[- ]transit encryption|data in transit|"
        r"over the wire|network encryption|secure transport|encrypted transport)\b",
        re.I,
    ),
    "tls": re.compile(
        r"\b(?:tls(?:\s*1\.[23])?|mtls|mTLS|mutual tls|ssl|https|certificate pinning|"
        r"certificates?|cipher suites?)\b",
        re.I,
    ),
    "kms": re.compile(
        r"\b(?:kms|key management service|aws kms|azure key vault|gcp cloud kms|"
        r"google cloud kms|vault transit|hsm|hardware security module|key vault)\b",
        re.I,
    ),
    "customer_managed_keys": re.compile(
        r"\b(?:customer[- ]managed keys?|customer managed encryption keys?|cmk|cmek|"
        r"bring your own key|byok|hold your own key|hyok|customer supplied keys?)\b",
        re.I,
    ),
    "key_rotation": re.compile(
        r"\b(?:key rotation|rotate keys?|rotating keys?|rotation schedule|key rollover|"
        r"certificate rotation|secret rotation|automatic rotation)\b",
        re.I,
    ),
    "secrets_management": re.compile(
        r"\b(?:secrets? management|secrets? manager|vault|api keys?|client secrets?|"
        r"credentials?|passwords?|private keys?|secret storage|encrypted secrets?)\b",
        re.I,
    ),
    "token_encryption": re.compile(
        r"\b(?:token encryption|encrypted tokens?|encrypt tokens?|refresh tokens?|"
        r"tokens?.{0,40}\bencrypt(?:ed|ion)?|access tokens?|id tokens?|jwt(?:s)?|"
        r"session tokens?)\b",
        re.I,
    ),
    "envelope_encryption": re.compile(r"\b(?:envelope encryption|data encryption keys?|dek|key encryption keys?|kek)\b", re.I),
    "field_level_encryption": re.compile(
        r"\b(?:field[- ]level encryption|column[- ]level encryption|cell[- ]level encryption|"
        r"encrypt fields?|encrypted fields?|pii fields?|sensitive fields?)\b",
        re.I,
    ),
}
_SCOPE_RE = re.compile(
    r"\b(?:customer|tenant|user|account|workspace|organization|payment|billing|invoice|"
    r"client|cardholder|pci|pii|phi|hipaa|personal|sensitive|confidential|database|storage|"
    r"object storage|files?|uploads?|backups?|logs?|audit logs?|tokens?|sessions?|"
    r"api keys?|credentials?|secrets?|webhooks?|events?|messages?|metadata|traffic|"
    r"internal services?|external api|admin|production|staging)"
    r"(?:[- ](?:data|records?|fields?|files?|uploads?|backups?|logs?|tokens?|sessions?|"
    r"secrets?|credentials?|traffic|services?|apis?|keys?|metadata|payloads?))*\b",
    re.I,
)
_IGNORED_FIELDS = {
    "id",
    "source_id",
    "source_brief_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
}
_MISSING_INPUTS: dict[SourceEncryptionRequirementCategory, tuple[str, ...]] = {
    "at_rest_encryption": ("data scope", "storage systems", "key owner", "rotation policy"),
    "in_transit_encryption": ("system scope", "protocol versions", "certificate owner"),
    "tls": ("system scope", "minimum TLS version", "certificate owner", "rotation policy"),
    "kms": ("key provider", "key owner", "key access policy", "rotation policy"),
    "customer_managed_keys": ("customer key ownership model", "key provider", "rotation policy", "revocation behavior"),
    "key_rotation": ("rotation interval", "key owner", "rollback behavior"),
    "secrets_management": ("secret inventory", "secret owner", "rotation policy", "storage system"),
    "token_encryption": ("token scope", "key owner", "rotation policy"),
    "envelope_encryption": ("data key scope", "key encryption key provider", "rotation policy"),
    "field_level_encryption": ("field inventory", "data scope", "key owner", "rotation policy"),
}


@dataclass(frozen=True, slots=True)
class SourceEncryptionRequirement:
    """One source-backed encryption or key-management requirement."""

    source_brief_id: str | None
    category: SourceEncryptionRequirementCategory
    scope: str | None = None
    missing_inputs: tuple[str, ...] = field(default_factory=tuple)
    confidence: SourceEncryptionRequirementConfidence = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "scope": self.scope,
            "missing_inputs": list(self.missing_inputs),
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceEncryptionRequirementsReport:
    """Source-level encryption requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceEncryptionRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceEncryptionRequirement, ...]:
        """Compatibility view matching extractors that name findings records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return encryption requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Encryption Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Confidence counts: "
            f"high {confidence_counts.get('high', 0)}, "
            f"medium {confidence_counts.get('medium', 0)}, "
            f"low {confidence_counts.get('low', 0)}",
            "- Category counts: "
            + (", ".join(f"{key} {category_counts[key]}" for key in sorted(category_counts)) or "none"),
        ]
        if not self.requirements:
            lines.extend(["", "No encryption requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Scope | Confidence | Missing Inputs | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.category} | "
                f"{_markdown_cell(requirement.scope or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.missing_inputs))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_encryption_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceEncryptionRequirementsReport:
    """Extract encryption requirement records from SourceBrief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _category_index(requirement.category),
                requirement.scope or "",
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceEncryptionRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_encryption_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceEncryptionRequirementsReport:
    """Compatibility alias for building an encryption requirements report."""
    return build_source_encryption_requirements(source)


def generate_source_encryption_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceEncryptionRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_encryption_requirements(source)


def summarize_source_encryption_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | SourceEncryptionRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted encryption requirements."""
    if isinstance(source_or_result, SourceEncryptionRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_encryption_requirements(source_or_result).summary


def source_encryption_requirements_to_dict(
    report: SourceEncryptionRequirementsReport,
) -> dict[str, Any]:
    """Serialize an encryption requirements report to a plain dictionary."""
    return report.to_dict()


source_encryption_requirements_to_dict.__test__ = False


def source_encryption_requirements_to_dicts(
    requirements: (
        tuple[SourceEncryptionRequirement, ...]
        | list[SourceEncryptionRequirement]
        | SourceEncryptionRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize encryption requirement records to dictionaries."""
    if isinstance(requirements, SourceEncryptionRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_encryption_requirements_to_dicts.__test__ = False


def source_encryption_requirements_to_markdown(
    report: SourceEncryptionRequirementsReport,
) -> str:
    """Render an encryption requirements report as Markdown."""
    return report.to_markdown()


source_encryption_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: SourceEncryptionRequirementCategory
    scope: str | None
    missing_inputs: tuple[str, ...]
    confidence: SourceEncryptionRequirementConfidence
    evidence: str


def _source_payloads(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief)) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Mapping[str, Any] | SourceBrief | str | object) -> tuple[str | None, dict[str, Any]]:
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


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for source_field, segment in _candidate_segments(payload):
            categories = _categories(segment, source_field)
            if not categories:
                continue
            evidence = _evidence_snippet(source_field, segment)
            for category in categories:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        scope=_scope(segment, source_field),
                        missing_inputs=_missing_inputs(category, segment),
                        confidence=_confidence(category, segment, source_field),
                        evidence=evidence,
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceEncryptionRequirement]:
    grouped: dict[tuple[str | None, SourceEncryptionRequirementCategory, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.source_brief_id, candidate.category, _dedupe_key(candidate.scope)),
            [],
        ).append(candidate)

    requirements: list[SourceEncryptionRequirement] = []
    for (source_brief_id, category, _), items in grouped.items():
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        scope = next((item.scope for item in items if item.scope), None)
        common_missing = set(items[0].missing_inputs)
        for item in items[1:]:
            common_missing.intersection_update(item.missing_inputs)
        requirements.append(
            SourceEncryptionRequirement(
                source_brief_id=source_brief_id,
                category=category,
                scope=scope,
                missing_inputs=tuple(sorted(_dedupe(common_missing), key=str.casefold)),
                confidence=confidence,
                evidence=tuple(sorted(_dedupe(item.evidence for item in items), key=str.casefold))[:6],
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "requirements",
        "constraints",
        "risks",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "security",
        "compliance",
        "encryption",
        "keys",
        "secrets",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
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
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _any_signal(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, child)
            if _any_signal(key_text) and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    values.extend((child_field, segment) for segment in _segments(f"{key_text}: {text}"))
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


def _categories(text: str, source_field: str) -> tuple[SourceEncryptionRequirementCategory, ...]:
    if _NEGATED_SCOPE_RE.search(text):
        return ()
    categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(text)]
    field_text = source_field.replace("_", " ").replace("-", " ")
    for category in _CATEGORY_ORDER:
        if (
            category not in categories
            and _CATEGORY_PATTERNS[category].search(field_text)
            and _encryption_context(text, source_field)
        ):
            categories.append(category)
    if "tls" in categories and "in_transit_encryption" not in categories:
        categories.insert(_CATEGORY_ORDER.index("in_transit_encryption"), "in_transit_encryption")
    if (
        "kms" in categories
        and "key_rotation" not in categories
        and _CATEGORY_PATTERNS["key_rotation"].search(text)
    ):
        categories.append("key_rotation")
    return tuple(_dedupe(categories))


def _scope(text: str, source_field: str) -> str | None:
    generic_scopes = {
        "customer",
        "tenant",
        "user",
        "account",
        "workspace",
        "organization",
        "database",
        "storage",
        "metadata",
        "traffic",
        "production",
        "staging",
    }
    matches = [
        _clean_scope(match.group(0))
        for match in _SCOPE_RE.finditer(text)
        if _clean_scope(match.group(0)) not in generic_scopes
    ]
    if matches:
        return _dedupe(matches)[0]
    field_tail = source_field.rsplit(".", 1)[-1].replace("_", " ").replace("-", " ")
    field_matches = [
        _clean_scope(match.group(0))
        for match in _SCOPE_RE.finditer(field_tail)
        if _clean_scope(match.group(0)) not in generic_scopes
    ]
    return _dedupe(field_matches)[0] if field_matches else None


def _missing_inputs(category: SourceEncryptionRequirementCategory, text: str) -> tuple[str, ...]:
    missing = list(_MISSING_INPUTS[category])
    if _scope(text, ""):
        _remove(missing, "data scope")
        _remove(missing, "system scope")
        _remove(missing, "token scope")
        _remove(missing, "data key scope")
        _remove(missing, "field inventory")
        _remove(missing, "secret inventory")
    if _key_owner_present(text):
        _remove(missing, "key owner")
        _remove(missing, "secret owner")
        _remove(missing, "certificate owner")
        _remove(missing, "customer key ownership model")
    if _rotation_present(text):
        _remove(missing, "rotation policy")
        _remove(missing, "rotation interval")
    if _provider_present(text):
        _remove(missing, "key provider")
        _remove(missing, "key encryption key provider")
        _remove(missing, "storage system")
    if re.search(r"\b(?:tls\s*1\.[23]|minimum tls|tls version)\b", text, re.I):
        _remove(missing, "minimum TLS version")
        _remove(missing, "protocol versions")
    if re.search(r"\b(?:kms policy|iam|rbac|key access|grants?|permissions?)\b", text, re.I):
        _remove(missing, "key access policy")
    if re.search(r"\b(?:revoke|revocation|disable key|key disabled|delete key)\b", text, re.I):
        _remove(missing, "revocation behavior")
    return tuple(missing)


def _confidence(
    category: SourceEncryptionRequirementCategory,
    text: str,
    source_field: str,
) -> SourceEncryptionRequirementConfidence:
    structured_field = bool(_STRUCTURED_FIELD_RE.search(source_field))
    has_detail = bool(_scope(text, source_field) or _provider_present(text) or _rotation_present(text))
    if _REQUIRED_RE.search(text) or (structured_field and has_detail):
        return "high"
    if structured_field or category in {
        "kms",
        "customer_managed_keys",
        "key_rotation",
        "secrets_management",
        "envelope_encryption",
        "field_level_encryption",
    }:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceEncryptionRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "categories": [requirement.category for requirement in requirements],
    }


def _encryption_context(text: str, source_field: str) -> bool:
    return bool(
        _ENCRYPTION_CONTEXT_RE.search(text)
        or _STRUCTURED_FIELD_RE.search(source_field.replace("-", "_"))
    )


def _key_owner_present(text: str) -> bool:
    return bool(re.search(r"\b(?:owned by|owner|key admin|security team|platform team|customer owned)\b", text, re.I))


def _rotation_present(text: str) -> bool:
    return bool(_CATEGORY_PATTERNS["key_rotation"].search(text) or re.search(r"\b(?:every|each)\s+\d+\s+(?:days?|months?|years?)\b", text, re.I))


def _provider_present(text: str) -> bool:
    return bool(_CATEGORY_PATTERNS["kms"].search(text) or re.search(r"\b(?:secrets manager|vault|database|s3|storage)\b", text, re.I))


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _CATEGORY_PATTERNS.values())


def _category_index(category: SourceEncryptionRequirementCategory) -> int:
    return _CATEGORY_ORDER.index(category)


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
        "constraints",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
        "source_links",
        "acceptance_criteria",
        "implementation_notes",
        "validation_plan",
        "security",
        "compliance",
        "encryption",
        "keys",
        "secrets",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _clean_scope(text: str) -> str:
    return _SPACE_RE.sub(" ", text.strip(" .,:;")).casefold()


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe_key(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", _clean_text(value).casefold()).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[Any] = set()
    for value in values:
        key = value.casefold() if isinstance(value, str) else value
        if not value or key in seen:
            continue
        result.append(value)
        seen.add(key)
    return result


def _remove(items: list[str], value: str) -> None:
    if value in items:
        items.remove(value)


__all__ = [
    "SourceEncryptionRequirement",
    "SourceEncryptionRequirementCategory",
    "SourceEncryptionRequirementConfidence",
    "SourceEncryptionRequirementsReport",
    "build_source_encryption_requirements",
    "extract_source_encryption_requirements",
    "generate_source_encryption_requirements",
    "source_encryption_requirements_to_dict",
    "source_encryption_requirements_to_dicts",
    "source_encryption_requirements_to_markdown",
    "summarize_source_encryption_requirements",
]
