"""Extract source-level API authentication requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ApiAuthenticationCategory = Literal[
    "api_key",
    "bearer_token",
    "oauth_client_credentials",
    "token_expiry",
    "credential_storage",
    "credential_rotation_revocation",
    "auth_failure_response",
    "test_coverage",
]
ApiAuthenticationMissingDetail = Literal["missing_credential_hashing_or_encryption", "missing_rotation_or_revocation"]
ApiAuthenticationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[ApiAuthenticationCategory, ...] = (
    "api_key",
    "bearer_token",
    "oauth_client_credentials",
    "token_expiry",
    "credential_storage",
    "credential_rotation_revocation",
    "auth_failure_response",
    "test_coverage",
)
_MISSING_DETAIL_ORDER: tuple[ApiAuthenticationMissingDetail, ...] = (
    "missing_credential_hashing_or_encryption",
    "missing_rotation_or_revocation",
)
_CONFIDENCE_ORDER: dict[ApiAuthenticationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_API_AUTH_CONTEXT_RE = re.compile(
    r"\b(?:api auth(?:entication)?|api keys?|bearer tokens?|oauth|client credentials|"
    r"access tokens?|refresh tokens?|token expiry|token expiration|"
    r"credential storage|credential hashing|credential encryption|"
    r"api key rotation|token rotation|credential rotation|revocation|revoke|"
    r"authentication failures?|401|unauthorized|unauthenticated)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:api|auth(?:entication)?|keys?|tokens?|bearer|oauth|client[_ -]?credentials|"
    r"credentials?|expiry|expiration|ttl|storage|hashing|encryption|bcrypt|argon2|"
    r"rotation|revocation|revoke|unauthorized|401|security|requirements?|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|create|generate|issue|provision|verify|validate|"
    r"hash|encrypt|store|rotate|rotation|revoke|revocation|expire|expiry|"
    r"401|unauthorized|reject|fail|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:api auth(?:entication)?|api keys?|bearer tokens?|oauth|client credentials|"
    r"access tokens?|credential storage|credential rotation|token rotation|revocation)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:api auth(?:entication)?|api keys?|bearer tokens?|oauth|client credentials|"
    r"access tokens?|credential storage|credential rotation|token rotation|revocation)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_API_AUTH_RE = re.compile(
    r"\b(?:no api auth(?:entication)?|api authentication is out of scope|"
    r"api auth is out of scope|no api auth work|public api|unauthenticated api)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:keyboard key|translation key|cache key|primary key|foreign key|key value|"
    r"legend key|map key|license key copy|key result|object key|sort key|"
    r"user login|password auth|session auth|cookie auth|web auth|browser auth)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:minutes?|hours?|days?|weeks?|months?|years?)|"
    r"api[- ]?key|bearer[- ]?token|oauth|client[- ]?credentials|"
    r"bcrypt|argon2|sha[- ]?256|aes[- ]?256|encrypted|hashed|"
    r"401|unauthorized|rotation|revocation|revoke)\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:minutes?|hours?|days?|weeks?|months?|years?)\b", re.I)
_HASHING_ENCRYPTION_RE = re.compile(
    r"\b(?:hash(?:ed|ing)?|encrypt(?:ed|ion)?|bcrypt|argon2|scrypt|pbkdf2|"
    r"aes[- ]?256|secure storage|protected|secret manager|vault)\b",
    re.I,
)
_ROTATION_REVOCATION_RE = re.compile(
    r"\b(?:rotate|rotation|rotated|regenerate|reissue|rollover|"
    r"revoke|revocation|revoked|invalidate|deactivate|expire manually|"
    r"force expiry|compromised credential)\b",
    re.I,
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
    "id",
    "source_id",
    "source_brief_id",
    "status",
    "created_by",
    "updated_by",
    "owner",
    "last_editor",
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
    "authentication",
    "auth_requirements",
    "security",
    "authorization",
    "integrations",
    "api",
    "api_keys",
    "access_keys",
    "developer_keys",
    "credentials",
    "tokens",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[ApiAuthenticationCategory, re.Pattern[str]] = {
    "api_key": re.compile(
        r"\b(?:api keys?|api[- ]?key auth(?:entication)?|developer keys?|"
        r"access keys?|client keys?|secret keys?|public keys?|private keys?)\b",
        re.I,
    ),
    "bearer_token": re.compile(
        r"\b(?:bearer tokens?|bearer auth(?:entication)?|access tokens?|"
        r"jwt|json web tokens?|authorization: bearer|authorization header)\b",
        re.I,
    ),
    "oauth_client_credentials": re.compile(
        r"\b(?:oauth|oauth2|oauth 2\.0|client credentials|client[- ]?id|client[- ]?secret|"
        r"grant[- ]?type|token endpoint|authorization flow)\b",
        re.I,
    ),
    "token_expiry": re.compile(
        r"\b(?:token expiry|token expiration|ttl|time to live|expires?(?:d| at| in)?|"
        r"lifetime|validity period|refresh tokens?|short[- ]?lived|long[- ]?lived)\b",
        re.I,
    ),
    "credential_storage": re.compile(
        r"\b(?:credential storage|store credentials?|hash(?:ed|ing)?|encrypt(?:ed|ion)?|"
        r"bcrypt|argon2|scrypt|pbkdf2|aes[- ]?256|secret manager|vault|"
        r"secure storage|protected storage|one[- ]?way hash)\b",
        re.I,
    ),
    "credential_rotation_revocation": re.compile(
        r"\b(?:rotate|rotation|rotated|regenerate|reissue|rollover|roll credentials?|"
        r"revoke|revocation|revoked|invalidate|deactivate|"
        r"compromised credential|force expiry|manual revocation)\b",
        re.I,
    ),
    "auth_failure_response": re.compile(
        r"\b(?:401|unauthorized|unauthenticated|authentication failed|invalid token|"
        r"invalid api[- ]?key|missing token|missing api[- ]?key|expired token|"
        r"www[- ]?authenticate header|error response|auth error)\b",
        re.I,
    ),
    "test_coverage": re.compile(
        r"\b(?:tests?|test coverage|unit tests?|integration tests?|auth tests?|"
        r"authentication tests?|token tests?|api[- ]?key tests?|401 tests?|"
        r"expiry tests?|rotation tests?|revocation tests?)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[ApiAuthenticationCategory, tuple[str, ...]] = {
    "api_key": ("security", "api_platform"),
    "bearer_token": ("security", "api_platform"),
    "oauth_client_credentials": ("security", "api_platform"),
    "token_expiry": ("security", "platform"),
    "credential_storage": ("security", "infrastructure"),
    "credential_rotation_revocation": ("security", "api_platform"),
    "auth_failure_response": ("security", "api_platform"),
    "test_coverage": ("qa", "security"),
}
_PLANNING_NOTES: dict[ApiAuthenticationCategory, tuple[str, ...]] = {
    "api_key": ("Define API key creation, prefix format, customer visibility, and endpoint access scoping.",),
    "bearer_token": ("Specify bearer token format, JWT claims or opaque tokens, signature verification, and header parsing.",),
    "oauth_client_credentials": ("Plan OAuth client ID/secret issuance, token endpoint flow, scope validation, and grant type enforcement.",),
    "token_expiry": ("Define token lifetime, expiry timestamps, refresh token behavior, and time-to-live checks.",),
    "credential_storage": ("Specify credential hashing algorithm, encryption at rest, secret manager usage, and one-way storage.",),
    "credential_rotation_revocation": ("Describe credential rotation workflow, manual revocation, compromised key response, and customer migration.",),
    "auth_failure_response": ("Define 401 Unauthorized responses, WWW-Authenticate headers, error details, and auth failure logging.",),
    "test_coverage": ("Add tests for auth success, 401 failures, token expiry, rotation, revocation, and invalid credentials.",),
}
_GAP_MESSAGES: dict[ApiAuthenticationMissingDetail, str] = {
    "missing_credential_hashing_or_encryption": "Specify credential hashing, encryption, or secure storage mechanism.",
    "missing_rotation_or_revocation": "Specify credential rotation or manual revocation workflow.",
}


@dataclass(frozen=True, slots=True)
class SourceApiAuthenticationRequirement:
    """One source-backed API authentication requirement."""

    category: ApiAuthenticationCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: ApiAuthenticationConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> ApiAuthenticationCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> ApiAuthenticationCategory:
        """Compatibility view for extractors that expose concern naming."""
        return self.category

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        """Compatibility view matching adjacent source extractors."""
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
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
class SourceApiAuthenticationRequirementsReport:
    """Source-level API authentication requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceApiAuthenticationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiAuthenticationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceApiAuthenticationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return API authentication requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Authentication Requirements Report"
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
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source API authentication requirements were inferred."])
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
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell(', '.join(requirement.suggested_owners))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{_markdown_cell('; '.join(requirement.gap_messages))} |"
            )
        return "\n".join(lines)


def build_source_api_authentication_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiAuthenticationRequirementsReport:
    """Build an API authentication requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    gap_flags = tuple(_missing_detail_flags(candidate.evidence for candidate in candidates))
    requirements = tuple(_merge_candidates(candidates, gap_flags))
    return SourceApiAuthenticationRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements, gap_flags if requirements else ()),
    )


def summarize_source_api_authentication_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceApiAuthenticationRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted API authentication requirements."""
    if isinstance(source, SourceApiAuthenticationRequirementsReport):
        return dict(source.summary)
    return build_source_api_authentication_requirements(source).summary


def derive_source_api_authentication_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiAuthenticationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_api_authentication_requirements(source)


def generate_source_api_authentication_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiAuthenticationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_authentication_requirements(source)


def extract_source_api_authentication_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceApiAuthenticationRequirement, ...]:
    """Return API authentication requirement records from brief-shaped input."""
    return build_source_api_authentication_requirements(source).requirements


def source_api_authentication_requirements_to_dict(
    report: SourceApiAuthenticationRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API authentication requirements report to a plain dictionary."""
    return report.to_dict()


source_api_authentication_requirements_to_dict.__test__ = False


def source_api_authentication_requirements_to_dicts(
    requirements: (
        tuple[SourceApiAuthenticationRequirement, ...]
        | list[SourceApiAuthenticationRequirement]
        | SourceApiAuthenticationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize API authentication requirement records to dictionaries."""
    if isinstance(requirements, SourceApiAuthenticationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_authentication_requirements_to_dicts.__test__ = False


def source_api_authentication_requirements_to_markdown(
    report: SourceApiAuthenticationRequirementsReport,
) -> str:
    """Render an API authentication requirements report as Markdown."""
    return report.to_markdown()


source_api_authentication_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: ApiAuthenticationCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: ApiAuthenticationConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _brief_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _brief_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _brief_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _brief_id(payload), payload
    return None, {}


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        if not _is_requirement(segment):
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = _categories(searchable)
        for category in _dedupe(categories):
            candidates.append(
                _Candidate(
                    category=category,
                    value=_value(category, segment.text),
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    confidence=_confidence(segment),
                )
            )
    return candidates


def _has_global_no_scope(payload: Mapping[str, Any]) -> bool:
    for segment in _candidate_segments(payload):
        if segment.source_field.split("[", 1)[0].split(".", 1)[0] not in {
            "title",
            "summary",
            "body",
            "description",
            "scope",
            "non_goals",
            "constraints",
            "source_payload",
        }:
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        if _NO_API_AUTH_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_flags: tuple[ApiAuthenticationMissingDetail, ...],
) -> list[SourceApiAuthenticationRequirement]:
    grouped: dict[ApiAuthenticationCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceApiAuthenticationRequirement] = []
    gap_messages = tuple(_GAP_MESSAGES[flag] for flag in gap_flags)
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(_CONFIDENCE_ORDER[item.confidence] for item in items if item.source_field == field),
                _field_category_rank(category, field),
                field.casefold(),
            ),
        )[0]
        requirements.append(
            SourceApiAuthenticationRequirement(
                category=category,
                source_field=source_field,
                evidence=tuple(
                    sorted(
                        _dedupe_evidence(
                            item.evidence
                            for item in sorted(
                                items,
                                key=lambda item: (
                                    _field_category_rank(category, item.source_field),
                                    item.source_field.casefold(),
                                ),
                            )
                        ),
                        key=str.casefold,
                    )
                )[:5],
                confidence=confidence,
                value=_best_value(items),
                suggested_owners=_OWNER_SUGGESTIONS[category],
                planning_notes=_PLANNING_NOTES[category],
                gap_messages=gap_messages,
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.value or "",
            requirement.source_field.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            if str(key) in _IGNORED_FIELDS:
                continue
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _API_AUTH_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        raw_text = str(value) if isinstance(value, str) else text
        for segment_text, segment_context in _segments(raw_text, field_context):
            segments.append(_Segment(source_field, segment_text, segment_context))


def _segments(value: str, inherited_context: bool) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    section_context = inherited_context
    for raw_line in value.splitlines() or [value]:
        line = raw_line.strip()
        if not line:
            continue
        heading = _HEADING_RE.match(line)
        if heading:
            title = _clean_text(heading.group("title"))
            section_context = inherited_context or bool(
                _API_AUTH_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = (
                [part]
                if _NEGATED_SCOPE_RE.search(part) and _API_AUTH_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_words = _field_words(segment.source_field)
    if _NO_API_AUTH_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _UNRELATED_RE.search(searchable) and not _API_AUTH_CONTEXT_RE.search(searchable):
        return False
    if not (_API_AUTH_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _API_AUTH_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:created|generated|issued|verified|validated|hashed|encrypted|stored|rotated|revoked|rejected)\b",
            segment.text,
            re.I,
        )
    )


def _categories(searchable: str) -> list[ApiAuthenticationCategory]:
    categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
    if "api_key" in categories and "bearer_token" in categories:
        if re.search(r"\bbearers?\b", searchable, re.I):
            categories.remove("api_key")
        elif re.search(r"\bapi[- ]?keys?\b", searchable, re.I):
            categories.remove("bearer_token")
    return categories


def _missing_detail_flags(evidence_values: Iterable[str]) -> list[ApiAuthenticationMissingDetail]:
    text = " ".join(evidence_values)
    if not text:
        return []
    flags: list[ApiAuthenticationMissingDetail] = []
    if not _HASHING_ENCRYPTION_RE.search(text):
        flags.append("missing_credential_hashing_or_encryption")
    if not _ROTATION_REVOCATION_RE.search(text):
        flags.append("missing_rotation_or_revocation")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(category: ApiAuthenticationCategory, text: str) -> str | None:
    if category == "token_expiry":
        if match := _DURATION_RE.search(text):
            return _clean_text(match.group(0)).casefold()
        if match := re.search(r"\b(?P<value>token expiry|ttl|time to live|expires?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "credential_storage":
        if match := re.search(r"\b(?P<value>bcrypt|argon2|scrypt|pbkdf2|aes[- ]?256|sha[- ]?256)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(
            r"\b(?P<value>hash(?:ed|ing)?|encrypt(?:ed|ion)?|secret manager|vault|secure storage)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category in {"api_key", "bearer_token"}:
        if match := re.search(r"\b(?P<value>api[- ]?key|bearer[- ]?token|jwt)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "oauth_client_credentials":
        if match := re.search(r"\b(?P<value>oauth|oauth2|client credentials|client[- ]?id|client[- ]?secret)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "credential_rotation_revocation":
        if match := re.search(r"\b(?P<value>rotate|rotation|revoke|revocation|regenerate|invalidate)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "auth_failure_response":
        if match := re.search(r"\b(?P<value>401|unauthorized)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(r"\b(?P<value>401|unauthorized|unauthenticated|invalid token|expired token)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "test_coverage":
        if match := re.search(r"\b(?P<value>auth tests?|authentication tests?|token tests?|api[- ]?key tests?|401 tests?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    ranked_values = sorted(
        ((index, item.value) for index, item in enumerate(items) if item.value),
        key=lambda indexed_value: (
            0 if re.search(r"\d", indexed_value[1]) else 1,
            0 if _VALUE_RE.search(indexed_value[1]) or _DURATION_RE.search(indexed_value[1]) else 1,
            indexed_value[0],
            len(indexed_value[1]),
            indexed_value[1].casefold(),
        ),
    )
    values = _dedupe(value for _, value in ranked_values)
    return values[0] if values else None


def _confidence(segment: _Segment) -> ApiAuthenticationConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _REQUIREMENT_RE.search(segment.text) and (
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "authentication",
                "auth_requirements",
                "security",
                "api",
                "credentials",
                "tokens",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _API_AUTH_CONTEXT_RE.search(searchable):
        return "medium"
    if _API_AUTH_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceApiAuthenticationRequirement, ...],
    gap_flags: tuple[ApiAuthenticationMissingDetail, ...],
) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "categories": [requirement.category for requirement in requirements],
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "missing_detail_flags": list(gap_flags),
        "missing_detail_counts": {
            flag: sum(1 for requirement in requirements if _GAP_MESSAGES[flag] in requirement.gap_messages)
            for flag in _MISSING_DETAIL_ORDER
        },
        "gap_messages": [_GAP_MESSAGES[flag] for flag in gap_flags],
        "status": "ready_for_planning" if requirements and not gap_flags else "needs_api_auth_details" if requirements else "no_api_auth_language",
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
        "scope",
        "non_goals",
        "assumptions",
        "acceptance",
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "authentication",
        "auth_requirements",
        "security",
        "authorization",
        "integrations",
        "api",
        "api_keys",
        "access_keys",
        "developer_keys",
        "credentials",
        "tokens",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: ApiAuthenticationCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[ApiAuthenticationCategory, tuple[str, ...]] = {
        "api_key": ("api key", "api keys", "developer key", "access key"),
        "bearer_token": ("bearer", "token", "jwt", "access token"),
        "oauth_client_credentials": ("oauth", "client credentials", "client id", "client secret"),
        "token_expiry": ("expiry", "expiration", "ttl", "lifetime", "expires"),
        "credential_storage": ("storage", "hash", "encrypt", "bcrypt", "argon2", "vault"),
        "credential_rotation_revocation": ("rotation", "rotate", "revocation", "revoke", "regenerate"),
        "auth_failure_response": ("failure", "401", "unauthorized", "error"),
        "test_coverage": ("test", "tests", "coverage"),
    }
    return 0 if any(marker in field_words for marker in markers[category]) else 1


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
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
    "ApiAuthenticationCategory",
    "ApiAuthenticationConfidence",
    "ApiAuthenticationMissingDetail",
    "SourceApiAuthenticationRequirement",
    "SourceApiAuthenticationRequirementsReport",
    "build_source_api_authentication_requirements",
    "derive_source_api_authentication_requirements",
    "extract_source_api_authentication_requirements",
    "generate_source_api_authentication_requirements",
    "summarize_source_api_authentication_requirements",
    "source_api_authentication_requirements_to_dict",
    "source_api_authentication_requirements_to_dicts",
    "source_api_authentication_requirements_to_markdown",
]
