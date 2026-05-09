"""Extract internationalization (i18n) requirements from source brief data."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for internationalization concepts
_TARGET_LOCALES_RE = re.compile(
    r"\b(?:target[_\s]+(?:locales?|languages?|markets?)|"
    r"supported[_\s]+(?:locales?|languages?)|"
    r"translations?[_\s]+(?:for|in)|locales?[_\s]+supported|"
    r"language[_\s]+support|multi[_\s-]*(?:language|lingual)|"
    r"(?:en|es|fr|de|ja|zh|pt|it|ko|ru|ar)[_\s-]?(?:US|GB|CA|ES|FR|DE|JP|CN|BR|IT|KR|RU)|"
    r"i18n[_\s]+(?:locales?|languages?|implementation)?|internationali[sz]ation)\b",
    re.I,
)
_TRANSLATION_WORKFLOW_RE = re.compile(
    r"\b(?:translation[_\s]+(?:workflow|process|pipeline|management)|"
    r"translate[_\s]+(?:content|strings?|text)|"
    r"translation[_\s]+(?:service|provider|vendor|platform)|"
    r"crowdin|lokalise|phrase|transifex|"
    r"translation[_\s]+(?:memory|database)|professional[_\s]+translation|"
    r"machine[_\s]+translation|human[_\s]+translation)\b",
    re.I,
)
_RTL_SUPPORT_RE = re.compile(
    r"\b(?:rtl[_\s]+(?:support|layout)|right[_\s-]*to[_\s-]*left|"
    r"bidirectional[_\s]+(?:text|support)|bidi[_\s]+support|"
    r"arabic[_\s]+(?:support|layout)|hebrew[_\s]+(?:support|layout)|"
    r"rtl[_\s]+(?:languages?|locales?)|text[_\s]+direction)\b",
    re.I,
)
_LOCALE_FORMATTING_RE = re.compile(
    r"\b(?:locale[_\s-]*specific[_\s]+formatting|"
    r"date[_\s]+(?:formatting|localization|format)|"
    r"time[_\s]+(?:formatting|localization|format)|"
    r"currency[_\s]+(?:formatting|localization|format)|"
    r"number[_\s]+(?:formatting|localization|format)|"
    r"decimal[_\s]+separator|thousand[_\s]+separator|"
    r"date[_\s]+(?:picker|selector)[_\s]+locali[sz]ation|"
    r"format[_\s]+(?:date|time|currency|number)[_\s]+for[_\s]+locale)\b",
    re.I,
)
_STRING_EXTERNALIZATION_RE = re.compile(
    r"(?:\b(?:string[_\s]+externali[sz]ation|externali[sz]e[_\s]+strings?|"
    r"extract[_\s]+strings?|resource[_\s]+(?:files?|bundles?)|"
    r"translation[_\s]+(?:keys?|files?|resources?)|"
    r"message[_\s]+(?:catalog|bundle)|locali[sz]ation[_\s]+files?|"
    r"in[_\s]+\.(?:properties|resx|po|pot)[_\s]+files?)\b|"
    r"\.(?:properties|resx|po|pot)|\.json\s+(?:i18n|translation))",
    re.I,
)
_PLURALIZATION_RE = re.compile(
    r"\b(?:plurali[sz]ation|plural[_\s]+(?:forms?|rules?)|"
    r"plurals?[_\s]+support|singular[_\s]+(?:and|vs)[_\s]+plural|"
    r"icu[_\s]+(?:message|plural)|intl[_\s]+plural[_\s]+rules?|"
    r"plural[_\s]+(?:categories?|formatting))\b",
    re.I,
)
_CONTENT_MANAGEMENT_RE = re.compile(
    r"\b(?:content[_\s]+management[_\s]+(?:system|for[_\s]+i18n)?|"
    r"cms[_\s]+(?:i18n|internationali[sz]ation|translation)|"
    r"dynamic[_\s]+content[_\s]+translation|"
    r"user[_\s-]*generated[_\s]+content[_\s]+translation|"
    r"translate[_\s]+user[_\s-]*generated[_\s]+content|"
    r"content[_\s]+(?:locali[sz]ation|translation)[_\s]+(?:workflow|pipeline))\b",
    re.I,
)
_TRANSLATION_TESTING_RE = re.compile(
    r"\b(?:translation[_\s]+testing|test[_\s]+translations?|"
    r"i18n[_\s]+testing|locali[sz]ation[_\s]+testing|"
    r"pseudo[_\s-]*locali[sz]ation|test[_\s]+locales?|"
    r"qa[_\s]+translation|translation[_\s]+quality|"
    r"linguistic[_\s]+(?:testing|validation))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class InternationalizationRequirements:
    """Internationalization (i18n) requirements extracted from source brief."""

    target_locales_defined: bool = False
    translation_workflow_specified: bool = False
    rtl_support_required: bool = False
    locale_formatting_addressed: bool = False
    string_externalization_planned: bool = False
    pluralization_handled: bool = False
    content_management_specified: bool = False
    translation_testing_included: bool = False

    @property
    def completeness_score(self) -> float:
        """Calculate completeness score (0.0 to 1.0)."""
        total_checks = 8
        passed_checks = sum([
            self.target_locales_defined,
            self.translation_workflow_specified,
            self.rtl_support_required,
            self.locale_formatting_addressed,
            self.string_externalization_planned,
            self.pluralization_handled,
            self.content_management_specified,
            self.translation_testing_included,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "target_locales_defined": self.target_locales_defined,
            "translation_workflow_specified": self.translation_workflow_specified,
            "rtl_support_required": self.rtl_support_required,
            "locale_formatting_addressed": self.locale_formatting_addressed,
            "string_externalization_planned": self.string_externalization_planned,
            "pluralization_handled": self.pluralization_handled,
            "content_management_specified": self.content_management_specified,
            "translation_testing_included": self.translation_testing_included,
            "completeness_score": self.completeness_score,
        }


def extract_internationalization_requirements(source_data: Mapping[str, Any]) -> InternationalizationRequirements:
    """
    Extract internationalization requirements from source brief data.

    Args:
        source_data: A mapping containing source brief information with fields like
                    'title', 'description', 'requirements', etc.

    Returns:
        InternationalizationRequirements with boolean flags for each aspect and overall score.
    """
    if not isinstance(source_data, Mapping):
        return InternationalizationRequirements()

    searchable_text = _extract_searchable_text(source_data)

    return InternationalizationRequirements(
        target_locales_defined=bool(_TARGET_LOCALES_RE.search(searchable_text)),
        translation_workflow_specified=bool(_TRANSLATION_WORKFLOW_RE.search(searchable_text)),
        rtl_support_required=bool(_RTL_SUPPORT_RE.search(searchable_text)),
        locale_formatting_addressed=bool(_LOCALE_FORMATTING_RE.search(searchable_text)),
        string_externalization_planned=bool(_STRING_EXTERNALIZATION_RE.search(searchable_text)),
        pluralization_handled=bool(_PLURALIZATION_RE.search(searchable_text)),
        content_management_specified=bool(_CONTENT_MANAGEMENT_RE.search(searchable_text)),
        translation_testing_included=bool(_TRANSLATION_TESTING_RE.search(searchable_text)),
    )


def _extract_searchable_text(source_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the source data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "summary", "rationale"):
        value = source_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("requirements", "acceptance_criteria", "constraints", "notes", "definition_of_done"):
        value = source_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "InternationalizationRequirements",
    "extract_internationalization_requirements",
]
