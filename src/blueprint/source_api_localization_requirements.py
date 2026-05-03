"""Extract source-level API localization requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


LocalizationCategory = Literal[
    "multi_language_support",
    "locale_based_content",
    "accept_language_header",
    "translation_management",
    "currency_number_formatting",
    "datetime_localization",
    "rtl_language_support",
    "locale_fallback_strategy",
]
LocalizationMissingDetail = Literal["missing_locale_detection", "missing_translation_strategy"]
LocalizationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[LocalizationCategory, ...] = (
    "multi_language_support",
    "locale_based_content",
    "accept_language_header",
    "translation_management",
    "currency_number_formatting",
    "datetime_localization",
    "rtl_language_support",
    "locale_fallback_strategy",
)
_MISSING_DETAIL_ORDER: tuple[LocalizationMissingDetail, ...] = (
    "missing_locale_detection",
    "missing_translation_strategy",
)
_CONFIDENCE_ORDER: dict[LocalizationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_LOCALIZATION_CONTEXT_RE = re.compile(
    r"\b(?:localization|localisation|localize|localise|"
    r"internationalization|internationalisation|i18n|l10n|"
    r"multi-?language|multilingual|language support|"
    r"accept-language|locale|translation|translate|"
    r"currency|number format|date format|time format|timezone|"
    r"rtl|right-to-left|bidirectional|locale fallback)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:localiz|internationali|i18n|l10n|language|locale|translation|"
    r"currency|format|rtl|header|headers?|api|rest|requirements?)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|include|return|expose|follow|implement|"
    r"localization|locale|translation|accept-language|currency|format|"
    r"rtl|fallback|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:localization|localisation|internationalization|i18n|l10n|"
    r"multi-?language|translation|locale|accept-language|currency format|rtl)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:localization|localisation|internationalization|i18n|l10n|"
    r"multi-?language|translation|locale|accept-language|currency format|rtl)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_LOCALIZATION_RE = re.compile(
    r"\b(?:no localization|no localisation|no internationalization|"
    r"localization is out of scope|i18n is out of scope|"
    r"no multi-?language|no translation support)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:file location|physical location|data location|server location|"
    r"geolocation|gps location|location tracking|location services)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:locale|language|translation|accept-language|currency|format|"
    r"rtl|fallback|en-US|fr-FR|es-ES|de-DE|ja-JP|zh-CN|ar-SA)\b",
    re.I,
)
_LOCALE_DETECTION_DETAIL_RE = re.compile(
    r"\b(?:accept-language|locale detection|language detection|user locale|"
    r"browser locale|request locale|locale negotiation)\b",
    re.I,
)
_TRANSLATION_STRATEGY_DETAIL_RE = re.compile(
    r"\b(?:translation file|translation key|translation service|translation api|"
    r"message catalog|resource bundle|translation management|localization file)\b",
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
    "localization",
    "i18n",
    "l10n",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[LocalizationCategory, re.Pattern[str]] = {
    "multi_language_support": re.compile(
        r"\b(?:multi-?language|multilingual|multiple language(?:s)?|support multiple language(?:s)?|"
        r"multi-?lingual support|language-specific|language selection|language switcher|"
        r"language preference|(?<!rtl\s)language support(?!\s+must\s+handle))\b",
        re.I,
    ),
    "locale_based_content": re.compile(
        r"\b(?:locale-based content|locale-specific content|localized content|"
        r"content localization|region-specific content|locale content delivery|"
        r"locale-aware content|content by locale|locale variant)\b",
        re.I,
    ),
    "accept_language_header": re.compile(
        r"\b(?:accept-language|accept language header|language header|"
        r"accept-language handling|accept-language parsing|language negotiation|"
        r"content negotiation|locale negotiation|language priority)\b",
        re.I,
    ),
    "translation_management": re.compile(
        r"\b(?:translation management|translation file|translation key|"
        r"translation service|translation api|message catalog|resource bundle|"
        r"translation storage|translation update|translation workflow|"
        r"localization file|translation database)\b",
        re.I,
    ),
    "currency_number_formatting": re.compile(
        r"\b(?:currency format(?:ting)?|number format(?:ting)?|currency display|number display|"
        r"locale-specific format|decimal format|thousand separator|"
        r"currency symbol|currency code|monetary format|numeric format)\b",
        re.I,
    ),
    "datetime_localization": re.compile(
        r"\b(?:date format|time format|datetime format|date localization|"
        r"time localization|timezone|time zone|locale-specific date|"
        r"locale-specific time|date display|time display|calendar format)\b",
        re.I,
    ),
    "rtl_language_support": re.compile(
        r"\b(?:rtl(?:\s+language)?(?:\s+support)?|right-to-left|bidirectional text|"
        r"bidi|rtl layout|ltr|left-to-right|text direction|"
        r"arabic support|hebrew support|bidirectional support)\b",
        re.I,
    ),
    "locale_fallback_strategy": re.compile(
        r"\b(?:locale fallback|fallback locale|default locale|language fallback|"
        r"fallback strategy|locale hierarchy|locale chain|fallback language|"
        r"locale resolution|missing translation|untranslated content)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[LocalizationCategory, tuple[str, ...]] = {
    "multi_language_support": ("api_platform", "backend", "frontend"),
    "locale_based_content": ("api_platform", "backend", "content"),
    "accept_language_header": ("api_platform", "backend"),
    "translation_management": ("api_platform", "backend", "content"),
    "currency_number_formatting": ("api_platform", "backend", "frontend"),
    "datetime_localization": ("api_platform", "backend", "frontend"),
    "rtl_language_support": ("frontend", "design"),
    "locale_fallback_strategy": ("api_platform", "backend"),
}
_PLANNING_NOTES: dict[LocalizationCategory, tuple[str, ...]] = {
    "multi_language_support": ("Define supported languages, language detection strategy, and language preference storage.",),
    "locale_based_content": ("Specify locale-based content delivery, locale-specific routing, and content variant management.",),
    "accept_language_header": ("Plan Accept-Language header parsing, locale negotiation algorithm, and quality value handling.",),
    "translation_management": ("Document translation file structure, translation key management, and translation update workflow.",),
    "currency_number_formatting": ("Define currency and number formatting rules, locale-specific format patterns, and format customization.",),
    "datetime_localization": ("Specify date/time formatting, timezone handling, and locale-specific calendar support.",),
    "rtl_language_support": ("Plan RTL layout support, bidirectional text handling, and RTL-specific UI adjustments.",),
    "locale_fallback_strategy": ("Document locale fallback hierarchy, missing translation handling, and default locale configuration.",),
}
_GAP_MESSAGES: dict[LocalizationMissingDetail, str] = {
    "missing_locale_detection": "Specify locale detection logic (Accept-Language parsing, user preferences, browser locale) and locale negotiation strategy.",
    "missing_translation_strategy": "Define translation management approach (file structure, translation keys, translation services) and update workflow.",
}


@dataclass(frozen=True, slots=True)
class SourceAPILocalizationRequirement:
    """One source-backed API localization requirement."""

    category: LocalizationCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: LocalizationConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> LocalizationCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> LocalizationCategory:
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
class SourceAPILocalizationRequirementsReport:
    """Source-level API localization requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceAPILocalizationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAPILocalizationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAPILocalizationRequirement, ...]:
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
        """Return API localization requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Localization Requirements Report"
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
            lines.extend(["", "No source API localization requirements were inferred."])
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


def build_source_api_localization_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPILocalizationRequirementsReport:
    """Build an API localization requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    gap_flags = tuple(_missing_detail_flags(candidate.evidence for candidate in candidates))
    requirements = tuple(_merge_candidates(candidates, gap_flags))
    return SourceAPILocalizationRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements, gap_flags if requirements else ()),
    )


def summarize_source_api_localization_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAPILocalizationRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted API localization requirements."""
    if isinstance(source, SourceAPILocalizationRequirementsReport):
        return dict(source.summary)
    return build_source_api_localization_requirements(source).summary


def derive_source_api_localization_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPILocalizationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_api_localization_requirements(source)


def generate_source_api_localization_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPILocalizationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_localization_requirements(source)


def extract_source_api_localization_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceAPILocalizationRequirement, ...]:
    """Return API localization requirement records from brief-shaped input."""
    return build_source_api_localization_requirements(source).requirements


def source_api_localization_requirements_to_dict(
    report: SourceAPILocalizationRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API localization requirements report to a plain dictionary."""
    return report.to_dict()


source_api_localization_requirements_to_dict.__test__ = False


def source_api_localization_requirements_to_dicts(
    requirements: (
        tuple[SourceAPILocalizationRequirement, ...]
        | list[SourceAPILocalizationRequirement]
        | SourceAPILocalizationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize API localization requirement records to dictionaries."""
    if isinstance(requirements, SourceAPILocalizationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_localization_requirements_to_dicts.__test__ = False


def source_api_localization_requirements_to_markdown(
    report: SourceAPILocalizationRequirementsReport,
) -> str:
    """Render an API localization requirements report as Markdown."""
    return report.to_markdown()


source_api_localization_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: LocalizationCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: LocalizationConfidence


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
        if _NO_LOCALIZATION_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_flags: tuple[LocalizationMissingDetail, ...],
) -> list[SourceAPILocalizationRequirement]:
    grouped: dict[LocalizationCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceAPILocalizationRequirement] = []
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
            SourceAPILocalizationRequirement(
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
                _STRUCTURED_FIELD_RE.search(key_text) or _LOCALIZATION_CONTEXT_RE.search(key_text)
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
                _LOCALIZATION_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _LOCALIZATION_CONTEXT_RE.search(part)
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
    if _NO_LOCALIZATION_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _UNRELATED_RE.search(searchable) and not _LOCALIZATION_CONTEXT_RE.search(searchable):
        return False
    if not (_LOCALIZATION_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _LOCALIZATION_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:include|included|return|returned|expose|exposed|follow|followed|implement|implemented)\b",
            segment.text,
            re.I,
        )
    )


def _categories(searchable: str) -> list[LocalizationCategory]:
    categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
    return categories


def _missing_detail_flags(evidence_values: Iterable[str]) -> list[LocalizationMissingDetail]:
    text = " ".join(evidence_values)
    if not text:
        return []
    flags: list[LocalizationMissingDetail] = []
    if not _LOCALE_DETECTION_DETAIL_RE.search(text):
        flags.append("missing_locale_detection")
    if not _TRANSLATION_STRATEGY_DETAIL_RE.search(text):
        flags.append("missing_translation_strategy")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(category: LocalizationCategory, text: str) -> str | None:
    if category == "multi_language_support":
        if match := re.search(r"\b(?P<value>multi-?language|multilingual|multiple language(?:s)?|language support)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "locale_based_content":
        if match := re.search(r"\b(?P<value>locale-based|locale-specific|localized|locale)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "accept_language_header":
        if match := re.search(r"\b(?P<value>accept-language|language header|negotiation)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "translation_management":
        if match := re.search(r"\b(?P<value>translation|translation file|translation key|translation service)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "currency_number_formatting":
        if match := re.search(r"\b(?P<value>currency|number format|currency format|decimal)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "datetime_localization":
        if match := re.search(r"\b(?P<value>date format|time format|datetime|timezone)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "rtl_language_support":
        if match := re.search(r"\b(?P<value>rtl|right-to-left|bidirectional|bidi)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "locale_fallback_strategy":
        if match := re.search(r"\b(?P<value>fallback|fallback locale|default locale|locale hierarchy)\b", text, re.I):
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


def _confidence(segment: _Segment) -> LocalizationConfidence:
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
                "localization",
                "i18n",
                "l10n",
                "requirements",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _LOCALIZATION_CONTEXT_RE.search(searchable):
        return "medium"
    if _LOCALIZATION_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceAPILocalizationRequirement, ...],
    gap_flags: tuple[LocalizationMissingDetail, ...],
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
        "status": "ready_for_planning" if requirements and not gap_flags else "needs_localization_details" if requirements else "no_localization_language",
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
        "localization",
        "i18n",
        "l10n",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: LocalizationCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[LocalizationCategory, tuple[str, ...]] = {
        "multi_language_support": ("multi language", "multilingual", "language support"),
        "locale_based_content": ("locale based", "locale specific", "localized content"),
        "accept_language_header": ("accept language", "language header", "negotiation"),
        "translation_management": ("translation", "translation file", "translation key"),
        "currency_number_formatting": ("currency", "number format", "currency format"),
        "datetime_localization": ("date format", "time format", "datetime", "timezone"),
        "rtl_language_support": ("rtl", "right to left", "bidirectional"),
        "locale_fallback_strategy": ("fallback", "fallback locale", "default locale"),
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
    "LocalizationCategory",
    "LocalizationConfidence",
    "LocalizationMissingDetail",
    "SourceAPILocalizationRequirement",
    "SourceAPILocalizationRequirementsReport",
    "build_source_api_localization_requirements",
    "derive_source_api_localization_requirements",
    "extract_source_api_localization_requirements",
    "generate_source_api_localization_requirements",
    "summarize_source_api_localization_requirements",
    "source_api_localization_requirements_to_dict",
    "source_api_localization_requirements_to_dicts",
    "source_api_localization_requirements_to_markdown",
]
