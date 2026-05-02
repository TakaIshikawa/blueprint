"""Extract source-level age verification and minor access requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


AgeVerificationRequirementType = Literal[
    "age_verification",
    "parental_consent",
    "minor_account",
    "coppa",
    "age_gated_access",
]
AgeVerificationSeverity = Literal["blocker", "high", "medium"]
AgeVerificationReadiness = Literal["ready_for_planning", "needs_clarification"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[AgeVerificationRequirementType, ...] = (
    "age_verification",
    "parental_consent",
    "minor_account",
    "coppa",
    "age_gated_access",
)
_SEVERITY_ORDER: dict[AgeVerificationSeverity, int] = {
    "blocker": 0,
    "high": 1,
    "medium": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_AGE_THRESHOLD_RE = re.compile(
    r"\b(?:under|over|younger than|older than|age(?:d)?|ages?|minimum age|at least)\s+"
    r"(?:\d{1,2}|thirteen|sixteen|eighteen)\b|"
    r"\b(?:u13|13\+|16\+|18\+|under-13|under 13|under-16|under 16|under-18|under 18)\b",
    re.I,
)
_SURFACE_RE = re.compile(
    r"\b(?:signup|sign-up|registration|register|onboarding|login|account creation|"
    r"profile|checkout|purchase|content|feature|api|mobile|web|app|admin|"
    r"settings|moderation|community|messaging)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|need(?:s)?|should|ensure|enforce|block|prevent|"
    r"restrict|allow only|verify|collect|capture|obtain|before launch|acceptance|"
    r"done when|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:age[- ]?)?(?:verification|gate|gating|minor|"
    r"parental consent|coppa).*?\b(?:required|needed|in scope|changes?|impact)\b",
    re.I,
)
_CONTEXT_RE = re.compile(
    r"\b(?:age verification|age check|verify age|date of birth|dob|birthdate|"
    r"parental consent|guardian consent|minor(?:s)?|child(?:ren)?|teen(?:s)?|"
    r"underage|coppa|children'?s online privacy|age[- ]gate|age[- ]gated|"
    r"minimum age|adult only|restricted content)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:title|summary|requirements?|acceptance|criteria|constraints?|data[-_ ]?requirements|"
    r"compliance|privacy|legal|trust|safety|identity|registration|onboarding|"
    r"access|age|minor|parent|guardian|coppa|metadata|source[-_ ]?payload)",
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
_TYPE_PATTERNS: dict[AgeVerificationRequirementType, re.Pattern[str]] = {
    "age_verification": re.compile(
        r"\b(?:age verification|age check|verify (?:the )?user'?s age|verify age|"
        r"age assurance|date of birth|dob|birthdate|birth date|minimum age)\b",
        re.I,
    ),
    "parental_consent": re.compile(
        r"\b(?:parental consent|parent consent|guardian consent|legal guardian consent|"
        r"parent approval|guardian approval|verifiable parental consent|vpc)\b",
        re.I,
    ),
    "minor_account": re.compile(
        r"\b(?:minor account|minor accounts|minor user|minor users|children'?s account|"
        r"child account|teen account|underage account|accounts? for minors?)\b",
        re.I,
    ),
    "coppa": re.compile(
        r"\b(?:coppa|children'?s online privacy protection|children'?s privacy)\b",
        re.I,
    ),
    "age_gated_access": re.compile(
        r"\b(?:age[- ]?gat(?:e|ed|ing)|age restricted|age-restricted|adult only|"
        r"adult-only|restricted content|restrict(?:ed)? access by age|block underage)\b",
        re.I,
    ),
}
_BASE_QUESTIONS: dict[AgeVerificationRequirementType, tuple[str, ...]] = {
    "age_verification": (
        "What minimum age threshold or jurisdiction-specific threshold should be enforced?",
    ),
    "parental_consent": ("Who is authorized to grant and revoke consent for a minor account?",),
    "minor_account": ("Which account capabilities differ for minors versus standard users?",),
    "coppa": (
        "Which COPPA compliance owner should review collection, notice, consent, and retention behavior?",
    ),
    "age_gated_access": ("Which product surfaces should enforce age-gated access?",),
}


@dataclass(frozen=True, slots=True)
class SourceAgeVerificationRequirement:
    """One source-backed age verification or minor access requirement."""

    source_brief_id: str | None
    requirement_type: AgeVerificationRequirementType
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)
    severity: AgeVerificationSeverity = "high"
    readiness: AgeVerificationReadiness = "needs_clarification"
    matched_terms: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "follow_up_questions": list(self.follow_up_questions),
            "severity": self.severity,
            "readiness": self.readiness,
            "matched_terms": list(self.matched_terms),
        }


@dataclass(frozen=True, slots=True)
class SourceAgeVerificationRequirementsReport:
    """Source-level age verification requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceAgeVerificationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAgeVerificationRequirement, ...]:
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
        """Return age verification requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Age Verification Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("type_counts", {})
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement type counts: "
            + ", ".join(
                f"{requirement_type} {type_counts.get(requirement_type, 0)}"
                for requirement_type in _TYPE_ORDER
            ),
            "- Severity counts: "
            + ", ".join(
                f"{severity} {severity_counts.get(severity, 0)}" for severity in _SEVERITY_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(
                [
                    "",
                    "No age verification or minor access requirements were found in the source brief.",
                ]
            )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Severity | Readiness | Source Field Paths | Evidence | Follow-up Questions |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_type} | "
                f"{requirement.severity} | "
                f"{requirement.readiness} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.follow_up_questions))} |"
            )
        return "\n".join(lines)


def build_source_age_verification_requirements(
    source: Mapping[str, Any] | SourceBrief | object,
) -> SourceAgeVerificationRequirementsReport:
    """Extract age verification and minor access requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload), source_brief_id))
    return SourceAgeVerificationRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def generate_source_age_verification_requirements(
    source: Mapping[str, Any] | SourceBrief | object,
) -> SourceAgeVerificationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_age_verification_requirements(source)


def extract_source_age_verification_requirements(
    source: Mapping[str, Any] | SourceBrief | object,
) -> tuple[SourceAgeVerificationRequirement, ...]:
    """Return age verification requirement records extracted from brief-shaped input."""
    return build_source_age_verification_requirements(source).requirements


def summarize_source_age_verification_requirements(
    source_or_result: (
        Mapping[str, Any] | SourceBrief | SourceAgeVerificationRequirementsReport | object
    ),
) -> dict[str, Any]:
    """Return the deterministic age verification requirements summary."""
    if isinstance(source_or_result, SourceAgeVerificationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_age_verification_requirements(source_or_result).summary


def source_age_verification_requirements_to_dict(
    report: SourceAgeVerificationRequirementsReport,
) -> dict[str, Any]:
    """Serialize an age verification requirements report to a plain dictionary."""
    return report.to_dict()


source_age_verification_requirements_to_dict.__test__ = False


def source_age_verification_requirements_to_dicts(
    requirements: (
        tuple[SourceAgeVerificationRequirement, ...]
        | list[SourceAgeVerificationRequirement]
        | SourceAgeVerificationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source age verification requirement records to dictionaries."""
    if isinstance(requirements, SourceAgeVerificationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_age_verification_requirements_to_dicts.__test__ = False


def source_age_verification_requirements_to_markdown(
    report: SourceAgeVerificationRequirementsReport,
) -> str:
    """Render an age verification requirements report as Markdown."""
    return report.to_markdown()


source_age_verification_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: AgeVerificationRequirementType
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]
    severity: AgeVerificationSeverity


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | object
) -> tuple[str | None, dict[str, Any]]:
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
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
        return _source_brief_id(payload), payload
    if not isinstance(source, (str, bytes, bytearray)):
        payload = _object_payload(source)
        return _source_brief_id(payload), payload
    return None, {}


def _source_brief_id(payload: Mapping[str, Any]) -> str | None:
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
        requirement_types = [
            requirement_type
            for requirement_type in _TYPE_ORDER
            if _TYPE_PATTERNS[requirement_type].search(searchable)
        ]
        for requirement_type in requirement_types:
            candidates.append(
                _Candidate(
                    requirement_type=requirement_type,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    source_field_path=segment.source_field,
                    matched_terms=_matched_terms(requirement_type, searchable),
                    severity=_severity(requirement_type, segment.text),
                )
            )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
    source_brief_id: str | None,
) -> list[SourceAgeVerificationRequirement]:
    by_type: dict[AgeVerificationRequirementType, list[_Candidate]] = {}
    for candidate in candidates:
        by_type.setdefault(candidate.requirement_type, []).append(candidate)

    requirements: list[SourceAgeVerificationRequirement] = []
    for requirement_type in _TYPE_ORDER:
        items = by_type.get(requirement_type, [])
        if not items:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in items))[:5]
        source_field_paths = tuple(
            sorted(_dedupe(item.source_field_path for item in items), key=str.casefold)
        )
        matched_terms = tuple(
            sorted(
                _dedupe(term for item in items for term in item.matched_terms),
                key=str.casefold,
            )
        )
        severity = min((item.severity for item in items), key=lambda item: _SEVERITY_ORDER[item])
        questions = _follow_up_questions(requirement_type, " ".join(evidence))
        requirements.append(
            SourceAgeVerificationRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                evidence=evidence,
                source_field_paths=source_field_paths,
                follow_up_questions=questions,
                severity=severity,
                readiness="needs_clarification" if questions else "ready_for_planning",
                matched_terms=matched_terms,
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "requirements",
        "acceptance_criteria",
        "acceptance",
        "constraints",
        "data_requirements",
        "privacy",
        "compliance",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(
    segments: list[_Segment],
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text in _segments(text):
            segments.append(_Segment(source_field, segment_text, field_context))


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
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _is_requirement(segment: _Segment) -> bool:
    if _NEGATED_SCOPE_RE.search(segment.text):
        return False
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if not _CONTEXT_RE.search(searchable):
        return False
    if segment.section_context:
        return True
    return bool(_REQUIREMENT_RE.search(segment.text))


def _matched_terms(
    requirement_type: AgeVerificationRequirementType,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)) for match in _TYPE_PATTERNS[requirement_type].finditer(text)
        )
    )


def _severity(
    requirement_type: AgeVerificationRequirementType,
    text: str,
) -> AgeVerificationSeverity:
    if requirement_type in {"coppa", "parental_consent"}:
        return "blocker"
    if requirement_type in {"age_verification", "age_gated_access"}:
        return "high"
    if _REQUIREMENT_RE.search(text):
        return "high"
    return "medium"


def _follow_up_questions(
    requirement_type: AgeVerificationRequirementType,
    evidence_text: str,
) -> tuple[str, ...]:
    questions = list(_BASE_QUESTIONS[requirement_type])
    if _AGE_THRESHOLD_RE.search(evidence_text):
        questions = [question for question in questions if "minimum age threshold" not in question]
    elif requirement_type != "age_verification":
        questions.append("What age threshold distinguishes minors, children, or restricted users?")
    if requirement_type != "age_gated_access" and not _SURFACE_RE.search(evidence_text):
        questions.append("Which signup, account, content, or workflow surfaces must enforce this?")
    if requirement_type == "age_gated_access" and _SURFACE_RE.search(evidence_text):
        questions = [question for question in questions if "Which product surfaces" not in question]
    return tuple(_dedupe(questions))


def _summary(requirements: tuple[SourceAgeVerificationRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "type_counts": {
            requirement_type: sum(
                1
                for requirement in requirements
                if requirement.requirement_type == requirement_type
            )
            for requirement_type in _TYPE_ORDER
        },
        "severity_counts": {
            severity: sum(1 for requirement in requirements if requirement.severity == severity)
            for severity in _SEVERITY_ORDER
        },
        "readiness_counts": {
            readiness: sum(1 for requirement in requirements if requirement.readiness == readiness)
            for readiness in ("ready_for_planning", "needs_clarification")
        },
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "follow_up_question_count": sum(
            len(requirement.follow_up_questions) for requirement in requirements
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
        "requirements",
        "acceptance_criteria",
        "acceptance",
        "constraints",
        "data_requirements",
        "privacy",
        "compliance",
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
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


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
    "AgeVerificationReadiness",
    "AgeVerificationRequirementType",
    "AgeVerificationSeverity",
    "SourceAgeVerificationRequirement",
    "SourceAgeVerificationRequirementsReport",
    "build_source_age_verification_requirements",
    "extract_source_age_verification_requirements",
    "generate_source_age_verification_requirements",
    "source_age_verification_requirements_to_dict",
    "source_age_verification_requirements_to_dicts",
    "source_age_verification_requirements_to_markdown",
    "summarize_source_age_verification_requirements",
]
