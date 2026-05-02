"""Extract source-level transactional email template requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


EmailTemplateConcern = Literal[
    "subject",
    "variables",
    "localization",
    "legal_footer",
    "plain_text_fallback",
    "sender_identity",
    "approval_workflow",
    "preview_text",
    "dark_mode",
    "reply_to",
    "template_ownership",
]
EmailTemplateConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CONCERN_ORDER: tuple[EmailTemplateConcern, ...] = (
    "subject",
    "variables",
    "localization",
    "legal_footer",
    "plain_text_fallback",
    "sender_identity",
    "approval_workflow",
    "preview_text",
    "dark_mode",
    "reply_to",
    "template_ownership",
)
_CONFIDENCE_ORDER: dict[EmailTemplateConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|define|document|include|contain|render|review|approve|"
    r"owned by|owner|sign[- ]?off|acceptance|before launch)\b",
    re.I,
)
_TEMPLATE_CONTEXT_RE = re.compile(
    r"\b(?:transactional email|email template|notification template|message template|template copy|"
    r"email copy|email content|notification content|subject line|preheader|preview text|"
    r"plain[- ]text|html email|sender|from name|reply[- ]to|legal footer|unsubscribe|"
    r"locali[sz]ation|locale|personalization|merge fields?|template variables?|dark mode)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:email|notification|template|copy|content|subject|preheader|preview|variable|merge|"
    r"locale|locali[sz]ation|translation|footer|unsubscribe|legal|plain[_ -]?text|"
    r"sender|from|reply[_ -]?to|owner|approval|review|dark[_ -]?mode|requirements?|"
    r"acceptance|criteria|definition[_ -]?of[_ -]?done|metadata|source[_ -]?payload)",
    re.I,
)
_DELIVERABILITY_ONLY_RE = re.compile(
    r"\b(?:spf|dkim|dmarc|dns|mx|bounces?|bounce handling|hard bounce|soft bounce|"
    r"suppression list|spam complaint|complaint rate|reputation|ip warm(?:ing|up)|"
    r"dedicated ip|mail transfer agent|mta|smtp retry|delivery rate|deliverability|"
    r"inbox placement|feedback loop)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,100}"
    r"\b(?:email templates?|notification templates?|template copy|email content)\b"
    r".{0,100}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:email templates?|notification templates?|template copy|email content)\b"
    r".{0,100}\b(?:out of scope|not required|not needed|no support|unsupported|no work|non-goal|non goal)\b",
    re.I,
)
_LOCALE_VALUE_RE = re.compile(
    r"\b(?:en[-_ ]US|en[-_ ]GB|fr[-_ ]FR|fr[-_ ]CA|es[-_ ]ES|es[-_ ]MX|de[-_ ]DE|"
    r"ja[-_ ]JP|pt[-_ ]BR|it[-_ ]IT|nl[-_ ]NL|zh[-_ ]CN|ko[-_ ]KR|"
    r"English|French|Spanish|German|Japanese|Portuguese|Italian|Dutch|Chinese|Korean)\b",
    re.I,
)
_VARIABLE_VALUE_RE = re.compile(
    r"(?:\{\{\s*([a-z][\w.:-]*)\s*\}\}|\{([a-z][\w.:-]*)\}|%([a-z][\w.:-]*)%|\b(?:variables?|merge fields?|tokens?)\b\s*:\s*([^.;\n]+))",
    re.I,
)
_EMAIL_ADDRESS_RE = re.compile(r"\b[\w.+-]+@[\w.-]+\.[a-z]{2,}\b", re.I)
_QUOTED_RE = re.compile(r"[\"'“”‘’](?P<value>[^\"'“”‘’]{3,100})[\"'“”‘’]")
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
    "non_goals",
    "assumptions",
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "notifications",
    "notification",
    "email",
    "emails",
    "templates",
    "template",
    "content",
    "copy",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_CONCERN_PATTERNS: dict[EmailTemplateConcern, re.Pattern[str]] = {
    "subject": re.compile(r"\b(?:subject line|email subject|message subject|subject must|subject should|subject:)\b", re.I),
    "variables": re.compile(r"\b(?:variables?|merge fields?|template fields?|personalization|personalized|tokens?|placeholders?|\{\{[^}]+\}\})\b", re.I),
    "localization": re.compile(r"\b(?:locales?|localized|locali[sz]ation|translations?|languages?|i18n|en[-_ ]US|fr[-_ ]FR|es[-_ ]ES|de[-_ ]DE|ja[-_ ]JP|Japanese|French|Spanish|German)\b", re.I),
    "legal_footer": re.compile(r"\b(?:legal footer|footer copy|unsubscribe|physical mailing address|postal address|company address|terms link|privacy link|compliance footer|can[- ]spam|gdpr footer)\b", re.I),
    "plain_text_fallback": re.compile(r"\b(?:plain[- ]text fallback|plain text version|text fallback|multipart alternative|text/plain|non-html|html fallback)\b", re.I),
    "sender_identity": re.compile(r"\b(?:sender identity|from name|from address|sender name|sender address|send as|from:|friendly from)\b", re.I),
    "approval_workflow": re.compile(r"\b(?:approval workflow|copy approval|legal approval|brand approval|marketing approval|review workflow|sign[- ]off|approved by|approval required|final approval)\b", re.I),
    "preview_text": re.compile(r"\b(?:preview text|preheader|pre-header|snippet text|inbox preview)\b", re.I),
    "dark_mode": re.compile(r"\b(?:dark mode|dark theme|dark-mode|prefers-color-scheme|inverted colors)\b", re.I),
    "reply_to": re.compile(r"\b(?:reply[- ]to|reply to|reply address|replies go to|no[- ]reply|support replies)\b", re.I),
    "template_ownership": re.compile(r"\b(?:template owner|template ownership|owned by|content owner|copy owner|maintained by|owner team|responsible team)\b", re.I),
}
_PLAN_IMPACTS: dict[EmailTemplateConcern, tuple[str, ...]] = {
    "subject": ("Define approved subject line copy and any dynamic subject variants.",),
    "variables": ("Model required template variables, defaults, escaping, and missing-value behavior.",),
    "localization": ("Plan locale-specific copy, translation ownership, and fallback language behavior.",),
    "legal_footer": ("Include required footer, unsubscribe, address, terms, and privacy content in the template.",),
    "plain_text_fallback": ("Create and test a plain-text or multipart fallback alongside the HTML template.",),
    "sender_identity": ("Define From name and From address for each transactional template.",),
    "approval_workflow": ("Add copy, legal, brand, or product approval steps before template launch.",),
    "preview_text": ("Define inbox preview or preheader text and how it changes with dynamic content.",),
    "dark_mode": ("Verify dark-mode rendering for colors, logos, links, and legibility.",),
    "reply_to": ("Define Reply-To behavior, monitored inboxes, and no-reply handling.",),
    "template_ownership": ("Assign template ownership for content updates, reviews, and future changes.",),
}


@dataclass(frozen=True, slots=True)
class SourceEmailTemplateRequirement:
    """One source-backed transactional email template requirement."""

    concern: EmailTemplateConcern
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: EmailTemplateConfidence = "medium"
    value: str | None = None
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> EmailTemplateConcern:
        """Compatibility view for extractors that expose category naming."""
        return self.concern

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "concern": self.concern,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "missing_details": list(self.missing_details),
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourceEmailTemplateRequirementsReport:
    """Source-level transactional email template requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceEmailTemplateRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceEmailTemplateRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceEmailTemplateRequirement, ...]:
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
        """Return email template requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Email Template Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        concern_counts = self.summary.get("concern_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Concern counts: "
            + ", ".join(f"{concern} {concern_counts.get(concern, 0)}" for concern in _CONCERN_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source email template requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Concern | Value | Missing Details | Confidence | Source Field | Evidence | Suggested Plan Impacts |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.concern} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{_markdown_cell('; '.join(requirement.missing_details))} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.suggested_plan_impacts))} |"
            )
        return "\n".join(lines)


def build_source_email_template_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceEmailTemplateRequirementsReport:
    """Build an email template requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceEmailTemplateRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_email_template_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceEmailTemplateRequirementsReport
        | str
        | object
    ),
) -> SourceEmailTemplateRequirementsReport | dict[str, Any]:
    """Compatibility helper for callers that use summarize_* naming."""
    if isinstance(source, SourceEmailTemplateRequirementsReport):
        return dict(source.summary)
    return build_source_email_template_requirements(source)


def derive_source_email_template_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceEmailTemplateRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_email_template_requirements(source)


def generate_source_email_template_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceEmailTemplateRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_email_template_requirements(source)


def extract_source_email_template_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceEmailTemplateRequirement, ...]:
    """Return email template requirement records from brief-shaped input."""
    return build_source_email_template_requirements(source).requirements


def source_email_template_requirements_to_dict(
    report: SourceEmailTemplateRequirementsReport,
) -> dict[str, Any]:
    """Serialize an email template requirements report to a plain dictionary."""
    return report.to_dict()


source_email_template_requirements_to_dict.__test__ = False


def source_email_template_requirements_to_dicts(
    requirements: (
        tuple[SourceEmailTemplateRequirement, ...]
        | list[SourceEmailTemplateRequirement]
        | SourceEmailTemplateRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize email template requirement records to dictionaries."""
    if isinstance(requirements, SourceEmailTemplateRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_email_template_requirements_to_dicts.__test__ = False


def source_email_template_requirements_to_markdown(
    report: SourceEmailTemplateRequirementsReport,
) -> str:
    """Render an email template requirements report as Markdown."""
    return report.to_markdown()


source_email_template_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    concern: EmailTemplateConcern
    value: str | None
    missing_details: tuple[str, ...]
    source_field: str
    evidence: str
    confidence: EmailTemplateConfidence


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
        concerns = [
            concern for concern in _CONCERN_ORDER if _CONCERN_PATTERNS[concern].search(searchable)
        ]
        for concern in _dedupe(concerns):
            candidates.append(
                _Candidate(
                    concern=concern,
                    value=_value(concern, segment.text),
                    missing_details=_missing_details(concern, segment.text),
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    confidence=_confidence(segment),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceEmailTemplateRequirement]:
    grouped: dict[EmailTemplateConcern, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.concern, []).append(candidate)

    requirements: list[SourceEmailTemplateRequirement] = []
    for concern in _CONCERN_ORDER:
        items = grouped.get(concern, [])
        if not items:
            continue
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(
                    _CONFIDENCE_ORDER[item.confidence]
                    for item in items
                    if item.source_field == field
                ),
                field.casefold(),
            ),
        )[0]
        value = _best_value(concern, items)
        requirements.append(
            SourceEmailTemplateRequirement(
                concern=concern,
                source_field=source_field,
                evidence=tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:5],
                confidence=confidence,
                value=value,
                missing_details=_merged_missing_details(concern, items, value),
                suggested_plan_impacts=_PLAN_IMPACTS[concern],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CONCERN_ORDER.index(requirement.concern),
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
                _STRUCTURED_FIELD_RE.search(key_text) or _TEMPLATE_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text, segment_context in _segments(text, field_context):
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
                _TEMPLATE_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if (
                    _NEGATED_SCOPE_RE.search(part) and _TEMPLATE_CONTEXT_RE.search(part)
                )
                or _LOCALE_VALUE_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _DELIVERABILITY_ONLY_RE.search(searchable) and not _TEMPLATE_CONTEXT_RE.search(searchable):
        return False
    has_concern = any(pattern.search(searchable) for pattern in _CONCERN_PATTERNS.values())
    if not has_concern:
        return False
    if not (_TEMPLATE_CONTEXT_RE.search(searchable) or segment.section_context):
        return False
    if _DELIVERABILITY_ONLY_RE.search(searchable) and not any(
        _CONCERN_PATTERNS[concern].search(searchable)
        for concern in ("subject", "variables", "localization", "legal_footer", "plain_text_fallback", "sender_identity", "approval_workflow", "preview_text", "dark_mode", "reply_to", "template_ownership")
    ):
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return True
    return bool(
        re.search(
            r"\b(?:include|contains?|uses?|renders?|approved|owned|localized|subject line|preview text|preheader)\b",
            segment.text,
            re.I,
        )
    )


def _value(concern: EmailTemplateConcern, text: str) -> str | None:
    if concern in {"subject", "preview_text"}:
        if match := _QUOTED_RE.search(text):
            return _clean_text(match.group("value"))
        if match := re.search(r"\b(?:subject line|subject|preview text|preheader|pre-header)\b[:\s-]+(?P<value>[^.;\n]+)", text, re.I):
            return _clean_text(match.group("value"))
    if concern == "variables":
        variables = _variables(text)
        if variables:
            return ", ".join(variables)
    if concern == "localization":
        locales = _locales(text)
        if locales:
            return ", ".join(locales)
    if concern in {"sender_identity", "reply_to"}:
        if match := _EMAIL_ADDRESS_RE.search(text):
            return _clean_text(match.group(0)).casefold()
        if match := re.search(r"\b(?:from name|sender name|from address|sender address|reply[- ]to|reply to)\b[:\s-]+(?P<value>[^.;\n]+)", text, re.I):
            return _clean_text(match.group("value"))
    if concern == "approval_workflow":
        if match := re.search(r"\b(?P<value>(?:legal|brand|marketing|product|content|security)\s+approval|copy approval|sign[- ]off|review workflow)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if concern == "template_ownership":
        if match := re.search(r"\b(?:owned by|owner(?: team)?|content owner|template owner|maintained by)\b[:\s-]*(?P<value>[^.;\n]+)", text, re.I):
            return _clean_text(match.group("value"))
    if concern == "legal_footer":
        if match := re.search(r"\b(?P<value>unsubscribe|physical mailing address|postal address|company address|terms link|privacy link|legal footer|compliance footer)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if concern == "plain_text_fallback":
        if match := re.search(r"\b(?P<value>plain[- ]text fallback|plain text version|multipart alternative|text/plain)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if concern == "dark_mode":
        if match := re.search(r"\b(?P<value>dark mode|dark theme|prefers-color-scheme|inverted colors)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    return None


def _variables(text: str) -> list[str]:
    values: list[str] = []
    for match in _VARIABLE_VALUE_RE.finditer(text):
        explicit = next((group for group in match.groups()[:3] if group), None)
        if explicit:
            values.append(_clean_text(explicit).casefold())
            continue
        tail = match.group(4)
        if tail and ":" in match.group(0):
            for item in re.split(r"\s*,\s*|\s+and\s+", tail):
                cleaned = _clean_text(item).strip(" .")
                if cleaned and len(cleaned) <= 40:
                    values.append(cleaned.casefold())
    return _dedupe(values)


def _locales(text: str) -> list[str]:
    return _dedupe(_clean_text(match.group(0)).replace(" ", "-") for match in _LOCALE_VALUE_RE.finditer(text))


def _missing_details(concern: EmailTemplateConcern, text: str) -> tuple[str, ...]:
    if concern == "variables" and not _variables(text):
        return ("required variable names",)
    if concern == "localization" and not _locales(text):
        return ("locale list",)
    if concern == "subject" and not _value("subject", text):
        return ("exact subject copy",)
    if concern == "preview_text" and not _value("preview_text", text):
        return ("exact preview text",)
    if concern == "sender_identity" and not _value("sender_identity", text):
        return ("from name or address",)
    if concern == "reply_to" and not _value("reply_to", text):
        return ("reply-to address or handling",)
    if concern == "template_ownership" and not _value("template_ownership", text):
        return ("owner team",)
    if concern == "approval_workflow" and not _value("approval_workflow", text):
        return ("approver roles",)
    return ()


def _merged_missing_details(
    concern: EmailTemplateConcern,
    items: Iterable[_Candidate],
    value: str | None,
) -> tuple[str, ...]:
    if value:
        return ()
    return tuple(_dedupe(detail for item in items for detail in item.missing_details))


def _best_value(concern: EmailTemplateConcern, items: Iterable[_Candidate]) -> str | None:
    if concern in {"variables", "localization"}:
        combined: list[str] = []
        for item in items:
            if item.value:
                combined.extend(_clean_text(part) for part in item.value.split(","))
        values = _dedupe(value for value in combined if value)
        return ", ".join(values) if values else None
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (len(value), value.casefold()),
    )
    return values[0] if values else None


def _confidence(segment: _Segment) -> EmailTemplateConfidence:
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
                "template",
                "email",
                "notification",
                "content",
                "copy",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _TEMPLATE_CONTEXT_RE.search(searchable):
        return "medium"
    if _TEMPLATE_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(requirements: tuple[SourceEmailTemplateRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "concerns": [requirement.concern for requirement in requirements],
        "concern_counts": {
            concern: sum(1 for requirement in requirements if requirement.concern == concern)
            for concern in _CONCERN_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "status": "ready_for_planning" if requirements else "no_email_template_language",
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
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "notifications",
        "notification",
        "email",
        "emails",
        "templates",
        "template",
        "content",
        "copy",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


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
    "EmailTemplateConcern",
    "EmailTemplateConfidence",
    "SourceEmailTemplateRequirement",
    "SourceEmailTemplateRequirementsReport",
    "build_source_email_template_requirements",
    "derive_source_email_template_requirements",
    "extract_source_email_template_requirements",
    "generate_source_email_template_requirements",
    "summarize_source_email_template_requirements",
    "source_email_template_requirements_to_dict",
    "source_email_template_requirements_to_dicts",
    "source_email_template_requirements_to_markdown",
]
