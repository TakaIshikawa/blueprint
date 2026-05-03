"""Extract source-level API security headers requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ApiSecurityHeadersCategory = Literal[
    "hsts",
    "csp",
    "frame_options",
    "content_type_options",
    "referrer_policy",
    "permissions_policy",
    "cors_headers",
    "secure_cookies",
    "test_coverage",
]
ApiSecurityHeadersMissingDetail = Literal["missing_csp_directives", "missing_cookie_security"]
ApiSecurityHeadersConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[ApiSecurityHeadersCategory, ...] = (
    "hsts",
    "csp",
    "frame_options",
    "content_type_options",
    "referrer_policy",
    "permissions_policy",
    "cors_headers",
    "secure_cookies",
    "test_coverage",
)
_MISSING_DETAIL_ORDER: tuple[ApiSecurityHeadersMissingDetail, ...] = (
    "missing_csp_directives",
    "missing_cookie_security",
)
_CONFIDENCE_ORDER: dict[ApiSecurityHeadersConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SECURITY_HEADERS_CONTEXT_RE = re.compile(
    r"\b(?:security headers?|http headers?|response headers?|hsts|"
    r"strict[- ]?transport[- ]?security|content[- ]?security[- ]?policy|csp|"
    r"x[- ]?frame[- ]?options|x[- ]?content[- ]?type[- ]?options|"
    r"referrer[- ]?policy|permissions[- ]?policy|cors|"
    r"cross[- ]?origin|secure cookies?|httponly|samesite)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:security|headers?|hsts|csp|cors|cookies?|frame|content[- ]?type|referrer|permissions|"
    r"requirements?|source[_ -]?payload|api)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|create|configure|set|include|enforce|add|define|"
    r"implement|block|disable|restrict|"
    r"acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:security headers?|hsts|csp|content[- ]?security[- ]?policy|"
    r"x[- ]?frame[- ]?options|cors|secure cookies?)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:security headers?|hsts|csp|content[- ]?security[- ]?policy|"
    r"x[- ]?frame[- ]?options|cors|secure cookies?)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_SECURITY_HEADERS_RE = re.compile(
    r"\b(?:no security headers?|security headers? out of scope|"
    r"no security header work|no http headers?|skip security headers?)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:email headers?|csv headers?|table headers?|column headers?|"
    r"request headers? only|header row|heading|title)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:strict[- ]?transport[- ]?security|hsts|max[- ]?age|preload|includesubdomains?|"
    r"content[- ]?security[- ]?policy|csp|default[- ]?src|script[- ]?src|style[- ]?src|"
    r"x[- ]?frame[- ]?options|deny|sameorigin|"
    r"x[- ]?content[- ]?type[- ]?options|nosniff|"
    r"referrer[- ]?policy|no[- ]?referrer|strict[- ]?origin|"
    r"permissions[- ]?policy|geolocation|camera|microphone|"
    r"access[- ]?control[- ]?allow[- ]?origin|cors|"
    r"httponly|secure|samesite|strict|lax|none)\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:seconds?|minutes?|hours?|days?|weeks?|months?|years?)\b", re.I)
_CSP_DIRECTIVE_RE = re.compile(
    r"\b(?:default[- ]?src|script[- ]?src|style[- ]?src|img[- ]?src|font[- ]?src|"
    r"connect[- ]?src|frame[- ]?src|object[- ]?src|media[- ]?src|worker[- ]?src|"
    r"manifest[- ]?src|base[- ]?uri|form[- ]?action|frame[- ]?ancestors|"
    r"upgrade[- ]?insecure[- ]?requests|block[- ]?all[- ]?mixed[- ]?content)\b",
    re.I,
)
_COOKIE_SECURITY_RE = re.compile(
    r"\b(?:httponly|secure|samesite|strict|lax|none|cookie security|secure cookies?)\b",
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
    "security",
    "security_requirements",
    "headers",
    "api",
    "cors",
    "cookies",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[ApiSecurityHeadersCategory, re.Pattern[str]] = {
    "hsts": re.compile(
        r"\b(?:hsts|strict[- ]?transport[- ]?security|http strict transport|"
        r"sts header|hsts preload)\b",
        re.I,
    ),
    "csp": re.compile(
        r"\b(?:csp|content[- ]?security[- ]?policy|content security|"
        r"default[- ]?src|script[- ]?src|style[- ]?src|img[- ]?src|"
        r"frame[- ]?ancestors|upgrade[- ]?insecure[- ]?requests|nonce[- ]?\{|hash[- ]?based)\b",
        re.I,
    ),
    "frame_options": re.compile(
        r"\b(?:x[- ]?frame[- ]?options|frame options|clickjacking|"
        r"(?:frame.{0,20})?deny|(?:frame.{0,20})?sameorigin|allow[- ]?from)\b",
        re.I,
    ),
    "content_type_options": re.compile(
        r"\b(?:x[- ]?content[- ]?type[- ]?options|content type options|"
        r"nosniff|mime[- ]?type sniffing|mime sniff)\b",
        re.I,
    ),
    "referrer_policy": re.compile(
        r"\b(?:referrer[- ]?policy|referer policy|no[- ]?referrer|"
        r"strict[- ]?origin(?:[- ]when[- ]cross[- ]origin)?|same[- ]?origin)\b",
        re.I,
    ),
    "permissions_policy": re.compile(
        r"\b(?:permissions[- ]?policy|feature[- ]?policy)\b|"
        r"\b(?:restrict|disable|block).{0,30}(?:geolocation|camera|microphone|payment|usb|midi|accelerometer|gyroscope)\b",
        re.I,
    ),
    "cors_headers": re.compile(
        r"\b(?:cors|cross[- ]?origin resource sharing|cross origin|"
        r"access[- ]?control[- ]?allow[- ]?origin|access[- ]?control[- ]?allow[- ]?methods?|"
        r"access[- ]?control[- ]?allow[- ]?headers?|access[- ]?control[- ]?expose[- ]?headers?|"
        r"access[- ]?control[- ]?max[- ]?age|access[- ]?control[- ]?allow[- ]?credentials)\b",
        re.I,
    ),
    "secure_cookies": re.compile(
        r"\b(?:secure cookies?|cookie security|httponly|samesite|"
        r"cookie attributes?|set[- ]?cookie)\b",
        re.I,
    ),
    "test_coverage": re.compile(
        r"\b(?:tests?|test coverage|unit tests?|integration tests?|security tests?|"
        r"header tests?|hsts tests?|csp tests?|cors tests?|cookie tests?)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[ApiSecurityHeadersCategory, tuple[str, ...]] = {
    "hsts": ("security", "platform"),
    "csp": ("security", "frontend"),
    "frame_options": ("security", "platform"),
    "content_type_options": ("security", "platform"),
    "referrer_policy": ("security", "privacy"),
    "permissions_policy": ("security", "platform"),
    "cors_headers": ("security", "api_platform"),
    "secure_cookies": ("security", "platform"),
    "test_coverage": ("qa", "security"),
}
_PLANNING_NOTES: dict[ApiSecurityHeadersCategory, tuple[str, ...]] = {
    "hsts": ("Configure Strict-Transport-Security header with max-age, includeSubDomains, and preload directives.",),
    "csp": ("Define Content-Security-Policy with default-src, script-src, style-src, and other directives to prevent XSS.",),
    "frame_options": ("Set X-Frame-Options to DENY or SAMEORIGIN to prevent clickjacking attacks.",),
    "content_type_options": ("Set X-Content-Type-Options to nosniff to prevent MIME type sniffing.",),
    "referrer_policy": ("Configure Referrer-Policy to control referrer information leakage across origins.",),
    "permissions_policy": ("Define Permissions-Policy to restrict access to browser features like geolocation and camera.",),
    "cors_headers": ("Configure CORS headers (Access-Control-Allow-Origin, Allow-Methods, Allow-Headers) for cross-origin requests.",),
    "secure_cookies": ("Enforce HttpOnly, Secure, and SameSite attributes on cookies to prevent XSS and CSRF attacks.",),
    "test_coverage": ("Add tests for security header presence, values, CSP violations, CORS preflight, and cookie attributes.",),
}
_GAP_MESSAGES: dict[ApiSecurityHeadersMissingDetail, str] = {
    "missing_csp_directives": "Specify CSP directives (default-src, script-src, style-src) and nonce/hash strategy.",
    "missing_cookie_security": "Specify cookie attributes (HttpOnly, Secure, SameSite) and enforcement policy.",
}


@dataclass(frozen=True, slots=True)
class SourceApiSecurityHeadersRequirement:
    """One source-backed API security headers requirement."""

    category: ApiSecurityHeadersCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: ApiSecurityHeadersConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> ApiSecurityHeadersCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> ApiSecurityHeadersCategory:
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
class SourceApiSecurityHeadersRequirementsReport:
    """Source-level API security headers requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceApiSecurityHeadersRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiSecurityHeadersRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceApiSecurityHeadersRequirement, ...]:
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
        """Return API security headers requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Security Headers Requirements Report"
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
            lines.extend(["", "No source API security headers requirements were inferred."])
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


def build_source_api_security_headers_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiSecurityHeadersRequirementsReport:
    """Build an API security headers requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    gap_flags = tuple(_missing_detail_flags(candidate.evidence for candidate in candidates))
    requirements = tuple(_merge_candidates(candidates, gap_flags))
    return SourceApiSecurityHeadersRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements, gap_flags if requirements else ()),
    )


def summarize_source_api_security_headers_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceApiSecurityHeadersRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted API security headers requirements."""
    if isinstance(source, SourceApiSecurityHeadersRequirementsReport):
        return dict(source.summary)
    return build_source_api_security_headers_requirements(source).summary


def derive_source_api_security_headers_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiSecurityHeadersRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_api_security_headers_requirements(source)


def generate_source_api_security_headers_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiSecurityHeadersRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_security_headers_requirements(source)


def extract_source_api_security_headers_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceApiSecurityHeadersRequirement, ...]:
    """Return API security headers requirement records from brief-shaped input."""
    return build_source_api_security_headers_requirements(source).requirements


def source_api_security_headers_requirements_to_dict(
    report: SourceApiSecurityHeadersRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API security headers requirements report to a plain dictionary."""
    return report.to_dict()


source_api_security_headers_requirements_to_dict.__test__ = False


def source_api_security_headers_requirements_to_dicts(
    requirements: (
        tuple[SourceApiSecurityHeadersRequirement, ...]
        | list[SourceApiSecurityHeadersRequirement]
        | SourceApiSecurityHeadersRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize API security headers requirement records to dictionaries."""
    if isinstance(requirements, SourceApiSecurityHeadersRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_security_headers_requirements_to_dicts.__test__ = False


def source_api_security_headers_requirements_to_markdown(
    report: SourceApiSecurityHeadersRequirementsReport,
) -> str:
    """Render an API security headers requirements report as Markdown."""
    return report.to_markdown()


source_api_security_headers_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: ApiSecurityHeadersCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: ApiSecurityHeadersConfidence


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
        if _NO_SECURITY_HEADERS_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_flags: tuple[ApiSecurityHeadersMissingDetail, ...],
) -> list[SourceApiSecurityHeadersRequirement]:
    grouped: dict[ApiSecurityHeadersCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceApiSecurityHeadersRequirement] = []
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
            SourceApiSecurityHeadersRequirement(
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
                _STRUCTURED_FIELD_RE.search(key_text) or _SECURITY_HEADERS_CONTEXT_RE.search(key_text)
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
                _SECURITY_HEADERS_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _SECURITY_HEADERS_CONTEXT_RE.search(part)
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
    if _NO_SECURITY_HEADERS_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _UNRELATED_RE.search(searchable) and not _SECURITY_HEADERS_CONTEXT_RE.search(searchable):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    # If we have a category match and a requirement keyword, that's enough
    if _REQUIREMENT_RE.search(segment.text):
        return True
    # Otherwise, need security context or structured field
    if not (_SECURITY_HEADERS_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _SECURITY_HEADERS_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:configured|set|enabled|enforced|included|provided|required|specified)\b",
            segment.text,
            re.I,
        )
    )


def _categories(searchable: str) -> list[ApiSecurityHeadersCategory]:
    categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
    return categories


def _missing_detail_flags(evidence_values: Iterable[str]) -> list[ApiSecurityHeadersMissingDetail]:
    text = " ".join(evidence_values)
    if not text:
        return []
    flags: list[ApiSecurityHeadersMissingDetail] = []
    if "csp" in text.lower() or "content-security-policy" in text.lower():
        if not _CSP_DIRECTIVE_RE.search(text):
            flags.append("missing_csp_directives")
    if "cookie" in text.lower() and "secure" in text.lower():
        # Only flag as missing if it's vague like "make cookies secure" without specifying attributes
        if not re.search(r"\b(?:httponly|samesite|strict|lax)\b", text, re.I):
            flags.append("missing_cookie_security")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(category: ApiSecurityHeadersCategory, text: str) -> str | None:
    if category == "hsts":
        # Check for numeric durations first (most specific)
        if match := _DURATION_RE.search(text):
            return _clean_text(match.group(0)).casefold()
        # Then check for "max-age" with or without a number following it
        if match := re.search(r"\bmax[- ]?age\s*\d+", text, re.I):
            return _clean_text(match.group(0)).casefold()
        # Check for "preload" (more specific than generic "hsts")
        if "preload" in text.lower():
            return "preload"
        # Then check for specific directives before generic "hsts"
        if match := re.search(
            r"\b(?P<value>max[- ]?age|includesubdomains?|strict[- ]?transport[- ]?security)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
        # Finally fallback to generic "hsts"
        if "hsts" in text.lower():
            return "hsts"
    if category == "csp":
        if match := re.search(
            r"\b(?P<value>default[- ]?src|script[- ]?src|style[- ]?src|img[- ]?src|"
            r"frame[- ]?ancestors|upgrade[- ]?insecure[- ]?requests)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(r"\b(?P<value>content[- ]?security[- ]?policy|csp)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "frame_options":
        if match := re.search(r"\b(?P<value>deny|sameorigin|allow[- ]?from)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(r"\b(?P<value>x[- ]?frame[- ]?options)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "content_type_options":
        if match := re.search(r"\b(?P<value>nosniff)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "referrer_policy":
        if match := re.search(
            r"\b(?P<value>no[- ]?referrer|strict[- ]?origin|origin[- ]?when[- ]?cross[- ]?origin|same[- ]?origin)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category == "permissions_policy":
        # Prioritize specific feature names over generic "permissions-policy"
        if match := re.search(
            r"\b(?P<value>geolocation|camera|microphone|payment|usb|midi|accelerometer|gyroscope)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
        # Fallback to permissions-policy header name if no specific feature found
        if match := re.search(r"\b(?P<value>permissions[- ]?policy|feature[- ]?policy)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "cors_headers":
        if match := re.search(
            r"\b(?P<value>access[- ]?control[- ]?allow[- ]?origin|access[- ]?control[- ]?allow[- ]?methods?|"
            r"access[- ]?control[- ]?allow[- ]?headers?)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(r"\b(?P<value>cors|cross[- ]?origin)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "secure_cookies":
        # Check for "Lax", "Strict", "None" when associated with cookies/SameSite
        if match := re.search(r"\b(?P<value>strict|lax|none)\b", text, re.I):
            if re.search(r"\b(?:samesite|cookie)", text, re.I):
                return _clean_text(match.group("value")).casefold()
        if match := re.search(r"\b(?P<value>httponly|samesite|secure)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "test_coverage":
        if match := re.search(
            r"\b(?P<value>security tests?|header tests?|hsts tests?|csp tests?|cors tests?)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    ranked_values = sorted(
        ((index, item.value) for index, item in enumerate(items) if item.value),
        key=lambda indexed_value: (
            # Prefer values with numbers (like "max-age", durations, or "31536000")
            0 if re.search(r"\d", indexed_value[1]) else 1,
            # Prefer specific values over generic header names
            0 if indexed_value[1] not in ("hsts", "csp", "cors", "permissions-policy", "feature-policy") else 1,
            # Prefer values that match our value patterns
            0 if _VALUE_RE.search(indexed_value[1]) or _DURATION_RE.search(indexed_value[1]) else 1,
            # Then order by length (longer = more specific)
            -len(indexed_value[1]),
            indexed_value[0],
            indexed_value[1].casefold(),
        ),
    )
    values = _dedupe(value for _, value in ranked_values)
    return values[0] if values else None


def _confidence(segment: _Segment) -> ApiSecurityHeadersConfidence:
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
                "security",
                "security_requirements",
                "headers",
                "api",
                "cors",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _SECURITY_HEADERS_CONTEXT_RE.search(searchable):
        return "medium"
    if _SECURITY_HEADERS_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceApiSecurityHeadersRequirement, ...],
    gap_flags: tuple[ApiSecurityHeadersMissingDetail, ...],
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
        "status": (
            "ready_for_planning"
            if requirements and not gap_flags
            else "needs_security_headers_details"
            if requirements
            else "no_security_headers_language"
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
        "security",
        "security_requirements",
        "headers",
        "api",
        "cors",
        "cookies",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: ApiSecurityHeadersCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[ApiSecurityHeadersCategory, tuple[str, ...]] = {
        "hsts": ("hsts", "strict transport", "max age", "preload"),
        "csp": ("csp", "content security", "script src", "style src"),
        "frame_options": ("frame", "clickjacking", "x frame options"),
        "content_type_options": ("content type", "nosniff", "mime"),
        "referrer_policy": ("referrer", "referer", "no referrer"),
        "permissions_policy": ("permissions", "feature policy", "geolocation"),
        "cors_headers": ("cors", "cross origin", "access control"),
        "secure_cookies": ("cookie", "httponly", "samesite", "secure"),
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
    "ApiSecurityHeadersCategory",
    "ApiSecurityHeadersConfidence",
    "ApiSecurityHeadersMissingDetail",
    "SourceApiSecurityHeadersRequirement",
    "SourceApiSecurityHeadersRequirementsReport",
    "build_source_api_security_headers_requirements",
    "derive_source_api_security_headers_requirements",
    "extract_source_api_security_headers_requirements",
    "generate_source_api_security_headers_requirements",
    "summarize_source_api_security_headers_requirements",
    "source_api_security_headers_requirements_to_dict",
    "source_api_security_headers_requirements_to_dicts",
    "source_api_security_headers_requirements_to_markdown",
]
