"""Extract third-party integration authentication requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


IntegrationAuthFindingType = Literal[
    "auth_mechanism",
    "secret_handling",
    "scope_constraints",
    "rotation_expiry",
]
IntegrationAuthMechanism = Literal[
    "oauth",
    "api_key",
    "bearer_token",
    "webhook_signature",
    "service_account",
    "refresh_token",
]
IntegrationAuthReadiness = Literal["ready_for_planning", "needs_clarification"]
IntegrationAuthConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[IntegrationAuthFindingType, ...] = (
    "auth_mechanism",
    "secret_handling",
    "scope_constraints",
    "rotation_expiry",
)
_MECHANISM_ORDER: tuple[IntegrationAuthMechanism, ...] = (
    "oauth",
    "api_key",
    "bearer_token",
    "webhook_signature",
    "service_account",
    "refresh_token",
)
_CONFIDENCE_ORDER: dict[IntegrationAuthConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_DIRECTIVE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|use|uses|using|store|stored|encrypt|encrypted|rotate|expire|expires|"
    r"expiry|expiration|validate|verify|sign|signed|least privilege|only asks?|"
    r"limited to|before launch|acceptance|done when|cannot ship)\b",
    re.I,
)
_INTEGRATION_CONTEXT_RE = re.compile(
    r"\b(?:third[- ]party|external|partner|vendor|integration|connected app|marketplace app|"
    r"webhooks?|callback|provider|slack|stripe|google|salesforce|github|shopify|"
    r"hubspot|zendesk|twilio|sendgrid|crm|api)\b",
    re.I,
)
_AUTH_CONTEXT_RE = re.compile(
    r"\b(?:auth(?:entication|orization)?|credential|secret|token|oauth|oauth2|oauth 2\.0|"
    r"api key|access key|bearer|service account|scope|permission|grant|refresh token|"
    r"client secret|hmac|signature|signed webhook|workload identity)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:integration[_ -]?auth|auth(?:entication|orization)?|oauth|api[_ -]?key|"
    r"access[_ -]?key|bearer|tokens?|credentials?|secrets?|service[_ -]?account|"
    r"webhook|signature|hmac|scopes?|permissions?|grants?|rotation|expiry|"
    r"expiration|source[_ -]?payload|metadata|requirements?|acceptance|criteria)",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:external|third[- ]party|partner|vendor|"
    r"integration|oauth|api keys?|bearer tokens?|service accounts?|webhooks?|"
    r"credential|secret|auth(?:entication|orization)?)\b.{0,100}"
    r"\b(?:required|needed|in scope|changes?|work|support)\b|"
    r"\b(?:no|not|without)\b.{0,100}\b(?:auth(?:entication|orization)?|"
    r"credentials?|secrets?|tokens?)\b.{0,100}\b(?:required|needed|in scope|changes?|work)\b",
    re.I,
)
_GENERIC_RE = re.compile(
    r"(?:general\s+)?(?:connected app\s+)?(?:integration\s+)?auth(?:entication|orization)?\s+requirements?\.?",
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
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "data_requirements",
    "integration_points",
    "risks",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_MECHANISM_PATTERNS: dict[IntegrationAuthMechanism, re.Pattern[str]] = {
    "oauth": re.compile(
        r"\b(?:oauth(?:\s*2(?:\.0)?)?|openid connect|oidc|authorization code|"
        r"client credentials|oauth app|connected app)\b",
        re.I,
    ),
    "api_key": re.compile(r"\b(?:api keys?|access keys?|developer keys?|partner keys?)\b", re.I),
    "bearer_token": re.compile(
        r"\b(?:bearer tokens?|access tokens?|api tokens?|personal access tokens?|pat)\b",
        re.I,
    ),
    "webhook_signature": re.compile(
        r"\b(?:webhook.{0,50}(?:signature|signing secret|hmac|signed)|"
        r"(?:signature|signing secret|hmac|signed).{0,50}webhook|hmac signature)\b",
        re.I,
    ),
    "service_account": re.compile(
        r"\b(?:service accounts?|service account keys?|workload identity|machine account|"
        r"machine[- ]to[- ]machine|m2m|json key)\b",
        re.I,
    ),
    "refresh_token": re.compile(
        r"\b(?:refresh tokens?|offline_access|offline access|long[- ]lived tokens?)\b",
        re.I,
    ),
}
_TYPE_PATTERNS: dict[IntegrationAuthFindingType, re.Pattern[str]] = {
    "auth_mechanism": re.compile(
        r"\b(?:oauth(?:\s*2(?:\.0)?)?|openid connect|oidc|authorization code|"
        r"api keys?|access keys?|bearer tokens?|access tokens?|personal access tokens?|"
        r"service accounts?|workload identity|webhook signatures?|hmac signature|"
        r"signed hmac|signed webhooks?|signing secret|refresh tokens?)\b",
        re.I,
    ),
    "secret_handling": re.compile(
        r"\b(?:secrets? manager|secret storage|stored? encrypted|encrypted storage|"
        r"vault|kms|keychain|credential store|client secret|signing secret|"
        r"never logged|redact(?:ed)?|mask(?:ed)?|environment variable|env var|"
        r"store.{0,50}(?:secret|credential|token|api key|key)|"
        r"(?:secret|credential|token|api key|key).{0,50}store)\b",
        re.I,
    ),
    "scope_constraints": re.compile(
        r"\b(?:oauth scopes?|scopes?|permissions?|grants?|least privilege|least-privilege|"
        r"minimal scopes?|minimum scopes?|read[- ]only|read/write|admin consent|"
        r"limited to|only asks? for|only request|consent screen)\b",
        re.I,
    ),
    "rotation_expiry": re.compile(
        r"\b(?:rotate|rotation|rotated|expiry|expire|expires|expiration|ttl|lifetime|"
        r"refresh tokens?|token refresh|revoke|revocation|disconnect app|"
        r"every\s+\d+\s+(?:days?|weeks?|months?|years?)|after\s+\d+\s+(?:days?|weeks?|months?|years?))\b",
        re.I,
    ),
}
_SCOPE_VALUE_RE = re.compile(
    r"\b(?:[a-z][a-z0-9_.-]+[:.][a-z0-9_*.-]+|offline_access|read[-_:][a-z0-9_.:-]+|"
    r"write[-_:][a-z0-9_.:-]+|[a-z0-9_.:-]+\.read|[a-z0-9_.:-]+\.write)\b",
    re.I,
)
_ROTATION_WINDOW_RE = re.compile(
    r"\b(?:(?:every|after|within|before|for)\s+)?(?:\d+(?:\.\d+)?|one|two|three|"
    r"six|twelve|thirty|sixty|ninety)\s+(?:hours?|days?|weeks?|months?|years?)\b",
    re.I,
)
_BASE_QUESTIONS: dict[IntegrationAuthFindingType, tuple[str, ...]] = {
    "auth_mechanism": ("Which third-party auth mechanism must each integration use?",),
    "secret_handling": ("Where must integration credentials be stored and redacted?",),
    "scope_constraints": ("Which exact OAuth scopes, grants, or permissions are allowed?",),
    "rotation_expiry": ("What rotation, expiry, refresh, or revocation schedule is required?",),
}


@dataclass(frozen=True, slots=True)
class SourceIntegrationAuthRequirement:
    """One source-backed integration authentication requirement or concern."""

    source_brief_id: str | None
    finding_type: IntegrationAuthFindingType
    auth_mechanisms: tuple[IntegrationAuthMechanism, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)
    confidence: IntegrationAuthConfidence = "medium"
    readiness: IntegrationAuthReadiness = "needs_clarification"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "finding_type": self.finding_type,
            "auth_mechanisms": list(self.auth_mechanisms),
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "follow_up_questions": list(self.follow_up_questions),
            "confidence": self.confidence,
            "readiness": self.readiness,
        }


@dataclass(frozen=True, slots=True)
class SourceIntegrationAuthRequirementsReport:
    """Source-level integration authentication requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceIntegrationAuthRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceIntegrationAuthRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceIntegrationAuthRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
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
        """Return integration auth requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Integration Auth Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Source count: {self.summary.get('source_count', 0)}",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Finding type counts: "
            + ", ".join(
                f"{finding_type} {type_counts.get(finding_type, 0)}"
                for finding_type in _TYPE_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}"
                for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source integration auth requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Mechanisms | Confidence | Readiness | Source Field Paths | Evidence | Follow-up Questions |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.finding_type} | "
                f"{_markdown_cell(', '.join(requirement.auth_mechanisms))} | "
                f"{requirement.confidence} | "
                f"{requirement.readiness} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.follow_up_questions))} |"
            )
        return "\n".join(lines)


def build_source_integration_auth_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceIntegrationAuthRequirementsReport:
    """Extract third-party integration auth requirement signals from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _TYPE_ORDER.index(requirement.finding_type),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceIntegrationAuthRequirementsReport(
        source_brief_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_integration_auth_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceIntegrationAuthRequirementsReport:
    """Compatibility alias for building an integration auth requirements report."""
    return build_source_integration_auth_requirements(source)


def generate_source_integration_auth_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceIntegrationAuthRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_integration_auth_requirements(source)


def derive_source_integration_auth_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceIntegrationAuthRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_integration_auth_requirements(source)


def summarize_source_integration_auth_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceIntegrationAuthRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic integration auth requirements summary."""
    if isinstance(source_or_result, SourceIntegrationAuthRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_integration_auth_requirements(source_or_result).summary


def source_integration_auth_requirements_to_dict(
    report: SourceIntegrationAuthRequirementsReport,
) -> dict[str, Any]:
    """Serialize an integration auth requirements report to a plain dictionary."""
    return report.to_dict()


source_integration_auth_requirements_to_dict.__test__ = False


def source_integration_auth_requirements_to_dicts(
    requirements: (
        tuple[SourceIntegrationAuthRequirement, ...]
        | list[SourceIntegrationAuthRequirement]
        | SourceIntegrationAuthRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source integration auth requirement records to dictionaries."""
    if isinstance(requirements, SourceIntegrationAuthRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_integration_auth_requirements_to_dicts.__test__ = False


def source_integration_auth_requirements_to_markdown(
    report: SourceIntegrationAuthRequirementsReport,
) -> str:
    """Render an integration auth requirements report as Markdown."""
    return report.to_markdown()


source_integration_auth_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    finding_type: IntegrationAuthFindingType
    auth_mechanisms: tuple[IntegrationAuthMechanism, ...]
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]
    confidence: IntegrationAuthConfidence


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(
        source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)
    ) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
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
    if isinstance(source, (bytes, bytearray)):
        return None, {}
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
        for source_field, text in _candidate_segments(payload):
            for finding_type in _finding_types(source_field, text):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        finding_type=finding_type,
                        auth_mechanisms=_mechanisms(source_field, text),
                        evidence=_evidence_snippet(source_field, text),
                        source_field_path=source_field,
                        matched_terms=_matched_terms(finding_type, source_field, text),
                        confidence=_confidence(finding_type, source_field, text),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceIntegrationAuthRequirement]:
    grouped: dict[tuple[str | None, IntegrationAuthFindingType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.finding_type), []).append(candidate)

    requirements: list[SourceIntegrationAuthRequirement] = []
    for (source_brief_id, finding_type), items in grouped.items():
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=lambda item: item.casefold())
        )[:5]
        source_field_paths = tuple(
            sorted(_dedupe(item.source_field_path for item in items), key=str.casefold)
        )
        matched_terms = tuple(
            sorted(
                _dedupe(term for item in items for term in item.matched_terms),
                key=str.casefold,
            )
        )
        mechanisms = tuple(
            mechanism
            for mechanism in _MECHANISM_ORDER
            if any(mechanism in item.auth_mechanisms for item in items)
        )
        confidence = min(
            (item.confidence for item in items), key=lambda item: _CONFIDENCE_ORDER[item]
        )
        questions = _follow_up_questions(finding_type, " ".join(evidence), matched_terms)
        requirements.append(
            SourceIntegrationAuthRequirement(
                source_brief_id=source_brief_id,
                finding_type=finding_type,
                auth_mechanisms=mechanisms,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                follow_up_questions=questions,
                confidence=confidence,
                readiness="needs_clarification" if questions else "ready_for_planning",
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            child = value[key]
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            if field_context or _STRUCTURED_FIELD_RE.search(key_text):
                if not isinstance(child, (Mapping, list, tuple, set)):
                    if text := _optional_text(child):
                        values.append((child_field, _clean_text(f"{key_text}: {text}")))
                    continue
            _append_value(values, child_field, child)
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
    for raw_line in value.splitlines() or [value]:
        cleaned = _clean_text(raw_line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(raw_line) or _CHECKBOX_RE.match(raw_line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            clauses = [part] if _NEGATED_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _finding_types(source_field: str, text: str) -> tuple[IntegrationAuthFindingType, ...]:
    if _NEGATED_RE.search(text) or _GENERIC_RE.fullmatch(_clean_text(text)):
        return ()
    searchable = _searchable_text(source_field, text)
    field_words = _field_words(source_field)
    has_auth_context = bool(_AUTH_CONTEXT_RE.search(searchable))
    has_integration_context = bool(_INTEGRATION_CONTEXT_RE.search(searchable))
    has_structured_context = bool(_STRUCTURED_FIELD_RE.search(field_words))
    if not (
        (has_auth_context and (has_integration_context or has_structured_context))
        or any(pattern.search(searchable) for pattern in _MECHANISM_PATTERNS.values())
    ):
        return ()

    finding_types: list[IntegrationAuthFindingType] = []
    for finding_type in _TYPE_ORDER:
        if finding_type == "scope_constraints" and not _scope_is_auth_related(
            searchable, field_words
        ):
            continue
        if _TYPE_PATTERNS[finding_type].search(searchable):
            finding_types.append(finding_type)
    if finding_types and not (
        _DIRECTIVE_RE.search(searchable)
        or has_structured_context
        or any(pattern.search(searchable) for pattern in _MECHANISM_PATTERNS.values())
    ):
        return ()
    return tuple(_dedupe(finding_types))


def _scope_is_auth_related(searchable: str, field_words: str) -> bool:
    if re.search(r"\b(?:project|release|task|feature|page)\s+scope\b", searchable, re.I):
        return False
    return bool(
        re.search(
            r"\b(?:oauth|consent screen|permission|grant|least privilege|auth|token|"
            r"credential|integration|api)\b",
            searchable,
            re.I,
        )
        or re.search(r"\b(?:oauth|auth|permission|grant|scope)\b", field_words, re.I)
        or _SCOPE_VALUE_RE.search(searchable)
    )


def _mechanisms(source_field: str, text: str) -> tuple[IntegrationAuthMechanism, ...]:
    searchable = _searchable_text(source_field, text)
    return tuple(
        mechanism
        for mechanism in _MECHANISM_ORDER
        if _MECHANISM_PATTERNS[mechanism].search(searchable)
    )


def _matched_terms(
    finding_type: IntegrationAuthFindingType,
    source_field: str,
    text: str,
) -> tuple[str, ...]:
    searchable = _searchable_text(source_field, text)
    terms: list[str] = [
        _clean_text(match.group(0)) for match in _TYPE_PATTERNS[finding_type].finditer(searchable)
    ]
    if finding_type == "scope_constraints":
        terms.extend(_clean_text(match.group(0)) for match in _SCOPE_VALUE_RE.finditer(searchable))
    if finding_type == "rotation_expiry":
        terms.extend(_clean_text(match.group(0)) for match in _ROTATION_WINDOW_RE.finditer(searchable))
    for mechanism in _mechanisms(source_field, text):
        terms.extend(
            _clean_text(match.group(0))
            for match in _MECHANISM_PATTERNS[mechanism].finditer(searchable)
        )
    return tuple(_dedupe(terms))


def _confidence(
    finding_type: IntegrationAuthFindingType,
    source_field: str,
    text: str,
) -> IntegrationAuthConfidence:
    searchable = _searchable_text(source_field, text)
    has_directive = bool(_DIRECTIVE_RE.search(searchable))
    has_structured_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    has_mechanism = bool(_mechanisms(source_field, text))
    has_detail = bool(
        (finding_type == "scope_constraints" and _SCOPE_VALUE_RE.search(searchable))
        or (finding_type == "rotation_expiry" and _ROTATION_WINDOW_RE.search(searchable))
        or (finding_type == "secret_handling" and _TYPE_PATTERNS["secret_handling"].search(searchable))
    )
    if has_directive and (has_detail or has_structured_context or has_mechanism):
        return "high"
    if has_directive or has_detail or has_structured_context or has_mechanism:
        return "medium"
    return "low"


def _follow_up_questions(
    finding_type: IntegrationAuthFindingType,
    evidence_text: str,
    matched_terms: tuple[str, ...],
) -> tuple[str, ...]:
    questions = list(_BASE_QUESTIONS[finding_type])
    if finding_type == "auth_mechanism" and matched_terms:
        questions = []
    if finding_type == "secret_handling" and re.search(
        r"\b(?:secrets? manager|vault|kms|encrypted|never logged|redact|mask|env(?:ironment)? var)\b",
        evidence_text,
        re.I,
    ):
        questions = []
    if finding_type == "scope_constraints" and (
        _SCOPE_VALUE_RE.search(evidence_text)
        or re.search(r"\bleast privilege|limited to|only asks? for\b", evidence_text, re.I)
    ):
        questions = []
    if finding_type == "rotation_expiry" and (
        _ROTATION_WINDOW_RE.search(evidence_text)
        or re.search(r"\b(?:expire|expires|expiry|refresh token|rotation|rotate|revoke)\b", evidence_text, re.I)
    ):
        questions = []
    return tuple(_dedupe(questions))


def _summary(
    requirements: tuple[SourceIntegrationAuthRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    status = "ready_for_planning" if requirements else "no_integration_auth_language"
    if any(requirement.readiness == "needs_clarification" for requirement in requirements):
        status = "needs_clarification"
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "type_counts": {
            finding_type: sum(
                1 for requirement in requirements if requirement.finding_type == finding_type
            )
            for finding_type in _TYPE_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "readiness_counts": {
            readiness: sum(1 for requirement in requirements if requirement.readiness == readiness)
            for readiness in ("ready_for_planning", "needs_clarification")
        },
        "finding_types": [
            finding_type
            for finding_type in _TYPE_ORDER
            if any(requirement.finding_type == finding_type for requirement in requirements)
        ],
        "auth_mechanisms": [
            mechanism
            for mechanism in _MECHANISM_ORDER
            if any(mechanism in requirement.auth_mechanisms for requirement in requirements)
        ],
        "follow_up_question_count": sum(
            len(requirement.follow_up_questions) for requirement in requirements
        ),
        "status": status,
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "target_user",
        "buyer",
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
        "product_surface",
        "requirements",
        "constraints",
        "scope",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "data_requirements",
        "integration_points",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _searchable_text(source_field: str, text: str) -> str:
    return f"{_field_words(source_field)} {text}".replace("_", " ").replace("-", " ")


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
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = _clean_text(str(value)).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


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


__all__ = [
    "IntegrationAuthConfidence",
    "IntegrationAuthFindingType",
    "IntegrationAuthMechanism",
    "IntegrationAuthReadiness",
    "SourceIntegrationAuthRequirement",
    "SourceIntegrationAuthRequirementsReport",
    "build_source_integration_auth_requirements",
    "derive_source_integration_auth_requirements",
    "extract_source_integration_auth_requirements",
    "generate_source_integration_auth_requirements",
    "source_integration_auth_requirements_to_dict",
    "source_integration_auth_requirements_to_dicts",
    "source_integration_auth_requirements_to_markdown",
    "summarize_source_integration_auth_requirements",
]
