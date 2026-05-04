"""Extract source-level API password policy requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


PasswordPolicyCategory = Literal[
    "password_complexity_rules",
    "password_length_requirements",
    "password_history_tracking",
    "password_expiration_policies",
    "password_reset_workflows",
    "breach_password_detection",
    "password_strength_meter",
    "password_hashing_algorithms",
]
PasswordPolicyMissingDetail = Literal["missing_policy_enforcement", "missing_hashing_algorithm"]
PasswordPolicyConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[PasswordPolicyCategory, ...] = (
    "password_complexity_rules",
    "password_length_requirements",
    "password_history_tracking",
    "password_expiration_policies",
    "password_reset_workflows",
    "breach_password_detection",
    "password_strength_meter",
    "password_hashing_algorithms",
)
_MISSING_DETAIL_ORDER: tuple[PasswordPolicyMissingDetail, ...] = (
    "missing_policy_enforcement",
    "missing_hashing_algorithm",
)
_CONFIDENCE_ORDER: dict[PasswordPolicyConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_PASSWORD_POLICY_CONTEXT_RE = re.compile(
    r"\b(?:password policy|password policies|password rule|password requirement|"
    r"password strength|password security|credential strength|credential policy|"
    r"password complexity|password length|password history|password expir|"
    r"password reset|password recovery|breach password|pwned password|"
    r"password hash|password meter|password validation|password enforcement|"
    r"password standard|password quality|weak password|strong password)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:password|credential|policy|policies|security|auth|authentication|"
    r"hash|hashing|strength|complexity|validation|"
    r"header|headers?|api|rest|requirements?)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|include|return|expose|follow|implement|"
    r"password|credential|policy|hash|strength|complexity|"
    r"acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:password policy|password policies|password rule|password requirement|"
    r"password complexity|password history|password expiration|breach detection|"
    r"password hash|password strength meter)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:password policy|password policies|password rule|password requirement|"
    r"password complexity|password history|password expiration|breach detection|"
    r"password hash|password strength meter)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_PASSWORD_POLICY_RE = re.compile(
    r"\b(?:no password policy|no password policies|no password requirement|"
    r"password policy is out of scope|password policies are out of scope|"
    r"no password rule|no credential policy)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:password field|password input|password textbox|password form|"
    r"password database|password table)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:complexity|length|history|expiration|reset|breach|pwned|"
    r"strength meter|bcrypt|argon2|scrypt|pbkdf2|sha256|sha512)\b",
    re.I,
)
_POLICY_ENFORCEMENT_DETAIL_RE = re.compile(
    r"\b(?:enforce|enforcement|validate|validation|check|verify|"
    r"reject|prevent|block|disallow|comply|compliance)\b",
    re.I,
)
_HASHING_ALGORITHM_DETAIL_RE = re.compile(
    r"\b(?:bcrypt|argon2|argon2id|scrypt|pbkdf2|sha256|sha512|"
    r"hash algorithm|hashing algorithm|cryptographic hash|salt|salted)\b",
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
    "api",
    "rest",
    "password",
    "credential",
    "security",
    "authentication",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[PasswordPolicyCategory, re.Pattern[str]] = {
    "password_complexity_rules": re.compile(
        r"\b(?:password complexity|complexity rule|complexity requirement|"
        r"character requirement|uppercase|lowercase|digit|special character|"
        r"mixed case|alphanumeric|character type|character class|"
        r"complexity check|complexity validation|complexity policy)\b",
        re.I,
    ),
    "password_length_requirements": re.compile(
        r"\b(?:password length|minimum length|maximum length|length requirement|"
        r"length policy|character count|minimum character|maximum character|"
        r"length validation|length check|password size|min length|max length)\b",
        re.I,
    ),
    "password_history_tracking": re.compile(
        r"\b(?:password history|history tracking|previous password|"
        r"password reuse|reuse prevention|password change history|"
        r"historical password|password record|password archive|"
        r"prevent reuse|block reuse|history count|history limit)\b",
        re.I,
    ),
    "password_expiration_policies": re.compile(
        r"\b(?:password expiration|password expiry|expiration policy|expiration period|"
        r"password age|password lifetime|password validity|max age|"
        r"password rotation|rotation policy|forced change|periodic change|"
        r"expiry date|expiry period|expiry notification|expiry warning)\b",
        re.I,
    ),
    "password_reset_workflows": re.compile(
        r"\b(?:password reset|reset workflow|reset process|reset link|"
        r"password recovery|account recovery|forgot password|"
        r"reset token|reset email|reset request|self-service reset|"
        r"password change|change password|update password)\b",
        re.I,
    ),
    "breach_password_detection": re.compile(
        r"\b(?:breach password|pwned password|compromised password|"
        r"breach detection|breach check|haveibeenpwned|hibp|"
        r"leaked password|known breach|breach database|breach list|"
        r"password blacklist|common password|weak password list)\b",
        re.I,
    ),
    "password_strength_meter": re.compile(
        r"\b(?:password strength|strength meter|strength indicator|"
        r"strength calculation|strength score|strength rating|"
        r"strength feedback|strength assessment|strength visualization|"
        r"password quality|quality meter|quality indicator|"
        r"weak|strong|very strong|password entropy)\b",
        re.I,
    ),
    "password_hashing_algorithms": re.compile(
        r"\b(?:password hash|hashing algorithm|hash algorithm|"
        r"bcrypt|argon2|argon2id|scrypt|pbkdf2|sha256|sha512|"
        r"cryptographic hash|salted hash|salt|salting|"
        r"hash function|hash storage|password storage|secure storage)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[PasswordPolicyCategory, tuple[str, ...]] = {
    "password_complexity_rules": ("security", "backend", "api_platform"),
    "password_length_requirements": ("security", "backend", "api_platform"),
    "password_history_tracking": ("security", "backend"),
    "password_expiration_policies": ("security", "backend"),
    "password_reset_workflows": ("security", "backend", "frontend"),
    "breach_password_detection": ("security", "backend"),
    "password_strength_meter": ("security", "frontend", "backend"),
    "password_hashing_algorithms": ("security", "backend"),
}
_PLANNING_NOTES: dict[PasswordPolicyCategory, tuple[str, ...]] = {
    "password_complexity_rules": ("Define complexity rules (uppercase, lowercase, digits, special characters) and validation logic.",),
    "password_length_requirements": ("Specify minimum and maximum password length requirements and enforcement points.",),
    "password_history_tracking": ("Plan password history storage, reuse prevention logic, and history retention policy.",),
    "password_expiration_policies": ("Document password expiration period, rotation enforcement, and user notification workflow.",),
    "password_reset_workflows": ("Design password reset flow, token generation, expiration handling, and security measures.",),
    "breach_password_detection": ("Integrate breach password database (e.g., HaveIBeenPwned), check workflow, and update strategy.",),
    "password_strength_meter": ("Implement password strength calculation, visualization feedback, and user guidance.",),
    "password_hashing_algorithms": ("Select secure hashing algorithm (bcrypt, argon2, scrypt), salting strategy, and storage approach.",),
}
_GAP_MESSAGES: dict[PasswordPolicyMissingDetail, str] = {
    "missing_policy_enforcement": "Specify password policy enforcement mechanisms (validation points, rejection handling, compliance checks).",
    "missing_hashing_algorithm": "Define password hashing algorithm (bcrypt, argon2, scrypt, PBKDF2) and salting strategy.",
}


@dataclass(frozen=True, slots=True)
class SourceAPIPasswordPolicyRequirement:
    """One source-backed API password policy requirement."""

    category: PasswordPolicyCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: PasswordPolicyConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> PasswordPolicyCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> PasswordPolicyCategory:
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
class SourceAPIPasswordPolicyRequirementsReport:
    """Source-level API password policy requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceAPIPasswordPolicyRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAPIPasswordPolicyRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAPIPasswordPolicyRequirement, ...]:
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
        """Return API password policy requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Password Policy Requirements Report"
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
            lines.extend(["", "No source API password policy requirements were inferred."])
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


def build_source_api_password_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPIPasswordPolicyRequirementsReport:
    """Build an API password policy requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    gap_flags = tuple(_missing_detail_flags(candidate.evidence for candidate in candidates))
    requirements = tuple(_merge_candidates(candidates, gap_flags))
    return SourceAPIPasswordPolicyRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements, gap_flags if requirements else ()),
    )


def summarize_source_api_password_policy_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAPIPasswordPolicyRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted API password policy requirements."""
    if isinstance(source, SourceAPIPasswordPolicyRequirementsReport):
        return dict(source.summary)
    return build_source_api_password_policy_requirements(source).summary


def derive_source_api_password_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPIPasswordPolicyRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_api_password_policy_requirements(source)


def generate_source_api_password_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPIPasswordPolicyRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_password_policy_requirements(source)


def extract_source_api_password_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceAPIPasswordPolicyRequirement, ...]:
    """Return API password policy requirement records from brief-shaped input."""
    return build_source_api_password_policy_requirements(source).requirements


def source_api_password_policy_requirements_to_dict(
    report: SourceAPIPasswordPolicyRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API password policy requirements report to a plain dictionary."""
    return report.to_dict()


source_api_password_policy_requirements_to_dict.__test__ = False


def source_api_password_policy_requirements_to_dicts(
    requirements: (
        tuple[SourceAPIPasswordPolicyRequirement, ...]
        | list[SourceAPIPasswordPolicyRequirement]
        | SourceAPIPasswordPolicyRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize API password policy requirement records to dictionaries."""
    if isinstance(requirements, SourceAPIPasswordPolicyRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_password_policy_requirements_to_dicts.__test__ = False


def source_api_password_policy_requirements_to_markdown(
    report: SourceAPIPasswordPolicyRequirementsReport,
) -> str:
    """Render an API password policy requirements report as Markdown."""
    return report.to_markdown()


source_api_password_policy_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: PasswordPolicyCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: PasswordPolicyConfidence


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
        if _NO_PASSWORD_POLICY_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_flags: tuple[PasswordPolicyMissingDetail, ...],
) -> list[SourceAPIPasswordPolicyRequirement]:
    grouped: dict[PasswordPolicyCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceAPIPasswordPolicyRequirement] = []
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
            SourceAPIPasswordPolicyRequirement(
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
                _STRUCTURED_FIELD_RE.search(key_text) or _PASSWORD_POLICY_CONTEXT_RE.search(key_text)
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
                _PASSWORD_POLICY_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _PASSWORD_POLICY_CONTEXT_RE.search(part)
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
    if _NO_PASSWORD_POLICY_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _UNRELATED_RE.search(searchable) and not _PASSWORD_POLICY_CONTEXT_RE.search(searchable):
        return False
    if not (_PASSWORD_POLICY_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _PASSWORD_POLICY_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:include|included|return|returned|expose|exposed|follow|followed|implement|implemented)\b",
            segment.text,
            re.I,
        )
    )


def _categories(searchable: str) -> list[PasswordPolicyCategory]:
    categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
    return categories


def _missing_detail_flags(evidence_values: Iterable[str]) -> list[PasswordPolicyMissingDetail]:
    text = " ".join(evidence_values)
    if not text:
        return []
    flags: list[PasswordPolicyMissingDetail] = []
    if not _POLICY_ENFORCEMENT_DETAIL_RE.search(text):
        flags.append("missing_policy_enforcement")
    if not _HASHING_ALGORITHM_DETAIL_RE.search(text):
        flags.append("missing_hashing_algorithm")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(category: PasswordPolicyCategory, text: str) -> str | None:
    if category == "password_complexity_rules":
        if match := re.search(r"\b(?P<value>complexity|mixed case|alphanumeric|character type|character class)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "password_length_requirements":
        if match := re.search(r"\b(?P<value>minimum length|maximum length|length|min length|max length|\d+\s*character)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "password_history_tracking":
        if match := re.search(r"\b(?P<value>history|password history|reuse prevention|previous password)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "password_expiration_policies":
        if match := re.search(r"\b(?P<value>expiration|expiry|rotation|max age|password age)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "password_reset_workflows":
        if match := re.search(r"\b(?P<value>reset|recovery|forgot password|password change)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "breach_password_detection":
        if match := re.search(r"\b(?P<value>breach|pwned|compromised|leaked|haveibeenpwned|hibp)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "password_strength_meter":
        if match := re.search(r"\b(?P<value>strength meter|strength indicator|strength|quality meter|weak|strong)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "password_hashing_algorithms":
        if match := re.search(r"\b(?P<value>bcrypt|argon2|argon2id|scrypt|pbkdf2|sha256|sha512|hash|salted hash)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    ranked_values = sorted(
        ((index, item.value) for index, item in enumerate(items) if item.value),
        key=lambda indexed_value: (
            0 if _VALUE_RE.search(indexed_value[1]) else 1,
            indexed_value[0],
            len(indexed_value[1]),
            indexed_value[1].casefold(),
        ),
    )
    values = _dedupe(value for _, value in ranked_values)
    return values[0] if values else None


def _confidence(segment: _Segment) -> PasswordPolicyConfidence:
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
                "api",
                "rest",
                "password",
                "credential",
                "security",
                "authentication",
                "requirements",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _PASSWORD_POLICY_CONTEXT_RE.search(searchable):
        return "medium"
    if _PASSWORD_POLICY_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceAPIPasswordPolicyRequirement, ...],
    gap_flags: tuple[PasswordPolicyMissingDetail, ...],
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
        "status": "ready_for_planning" if requirements and not gap_flags else "needs_password_policy_details" if requirements else "no_password_policy_language",
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
        "api",
        "rest",
        "password",
        "credential",
        "security",
        "authentication",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: PasswordPolicyCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[PasswordPolicyCategory, tuple[str, ...]] = {
        "password_complexity_rules": ("complexity", "character requirement", "mixed case"),
        "password_length_requirements": ("length", "minimum length", "maximum length"),
        "password_history_tracking": ("history", "reuse", "previous password"),
        "password_expiration_policies": ("expiration", "expiry", "rotation", "age"),
        "password_reset_workflows": ("reset", "recovery", "forgot password"),
        "breach_password_detection": ("breach", "pwned", "compromised", "leaked"),
        "password_strength_meter": ("strength", "meter", "indicator", "quality"),
        "password_hashing_algorithms": ("hash", "bcrypt", "argon2", "scrypt", "salt"),
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
    "PasswordPolicyCategory",
    "PasswordPolicyConfidence",
    "PasswordPolicyMissingDetail",
    "SourceAPIPasswordPolicyRequirement",
    "SourceAPIPasswordPolicyRequirementsReport",
    "build_source_api_password_policy_requirements",
    "derive_source_api_password_policy_requirements",
    "extract_source_api_password_policy_requirements",
    "generate_source_api_password_policy_requirements",
    "summarize_source_api_password_policy_requirements",
    "source_api_password_policy_requirements_to_dict",
    "source_api_password_policy_requirements_to_dicts",
    "source_api_password_policy_requirements_to_markdown",
]
