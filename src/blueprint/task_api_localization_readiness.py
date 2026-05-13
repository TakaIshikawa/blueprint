"""Assess task-level readiness for API localization implementation work."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._task_safeguard_readiness import (
    TaskSafeguardReadinessPlan,
    TaskSafeguardReadinessRecord,
    build_task_safeguard_readiness_plan,
)


TaskApiLocalizationReadinessRecord = TaskSafeguardReadinessRecord
TaskApiLocalizationReadinessPlan = TaskSafeguardReadinessPlan

_SIGNALS = {
    "api_localization": re.compile(r"\b(?:api localization|localized api|localise api|localize api|i18n api)\b", re.I),
    "locale_negotiation": re.compile(r"\b(?:locale negotiation|negotiate locale|locale resolver|locale preference|preferred locale)\b", re.I),
    "accept_language": re.compile(r"\b(?:accept[- ]language|accept_language|language header|http language)\b", re.I),
    "translation_catalog": re.compile(r"\b(?:translation catalogs?|message catalogs?|locale catalogs?|resource bundles?|translations?)\b", re.I),
    "fallback_locale": re.compile(r"\b(?:fallback locale|default locale|locale fallback|fallback language|default language)\b", re.I),
    "localized_formatting": re.compile(r"\b(?:currency formatting|date formatting|number formatting|localized formatting|pluralization|time format)\b", re.I),
    "rtl_support": re.compile(r"\b(?:rtl|right[- ]to[- ]left|bidirectional|bidi|arabic|hebrew)\b", re.I),
}
_PATH_SIGNALS = {
    "api_localization": re.compile(r"api.*(?:locali[sz]ation|i18n)|(?:locali[sz]ation|i18n).*api", re.I),
    "locale_negotiation": re.compile(r"locale|language|i18n", re.I),
    "accept_language": re.compile(r"accept[_-]?language|language[_-]?header", re.I),
    "translation_catalog": re.compile(r"translations?|catalogs?|messages?|resource[_-]?bundle", re.I),
    "fallback_locale": re.compile(r"fallback|default[_-]?locale", re.I),
    "localized_formatting": re.compile(r"format|currency|date|number|plural", re.I),
    "rtl_support": re.compile(r"rtl|bidi|right[_-]?to[_-]?left", re.I),
}
_SAFEGUARDS = {
    "accept_language_tests": re.compile(
        r"\b(?:accept[-_ ]language(?: header)? tests?|test[-_ ]accept[-_ ]language|language header tests?|"
        r"header parsing tests?|accept[-_ ]language header)\b",
        re.I,
    ),
    "locale_fallback_policy": re.compile(
        r"\b(?:fallback locale policy|fallback locale|locale fallback|default locale|unsupported locale|"
        r"missing translation fallback)\b",
        re.I,
    ),
    "catalog_validation": re.compile(r"\b(?:catalog validation|translation validation|missing keys?|message key coverage|catalog completeness)\b", re.I),
    "formatting_tests": re.compile(r"\b(?:currency formatting tests?|date formatting tests?|number formatting tests?|pluralization tests?|formatting coverage)\b", re.I),
    "rtl_coverage": re.compile(r"\b(?:rtl tests?|right[- ]to[- ]left coverage|bidi tests?|rtl coverage)\b", re.I),
    "cache_vary_headers": re.compile(r"\b(?:vary: accept-language|vary header|language cache key|locale cache key|cache varies by language)\b", re.I),
}
_GUIDANCE = {
    "accept_language_tests": "Add tests for Accept-Language parsing, q-values, invalid tags, and explicit locale overrides.",
    "locale_fallback_policy": "Define fallback locale behavior for unsupported locales and missing translations.",
    "catalog_validation": "Validate translation catalog completeness, message key drift, and missing localized strings.",
    "formatting_tests": "Cover locale-specific currency, date, number, timezone, and plural formatting behavior.",
    "rtl_coverage": "Verify RTL and bidirectional language responses where localized API payloads expose direction-sensitive content.",
    "cache_vary_headers": "Ensure localized API responses vary cache keys and HTTP Vary headers by language or locale.",
}
_HIGH_IMPACT = {"api_localization", "locale_negotiation", "accept_language", "translation_catalog"}


def build_task_api_localization_readiness_plan(source: Any) -> TaskApiLocalizationReadinessPlan:
    return build_task_safeguard_readiness_plan(
        source,
        title="Task API Localization Readiness",
        task_count_label="localization_task_count",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        safeguard_patterns=_SAFEGUARDS,
        safeguard_guidance=_GUIDANCE,
        high_impact_signals=_HIGH_IMPACT,
    )


def analyze_task_api_localization_readiness(source: Any) -> TaskApiLocalizationReadinessPlan:
    return build_task_api_localization_readiness_plan(source)


def extract_task_api_localization_readiness(source: Any) -> TaskApiLocalizationReadinessPlan:
    return build_task_api_localization_readiness_plan(source)


def generate_task_api_localization_readiness(source: Any) -> TaskApiLocalizationReadinessPlan:
    return build_task_api_localization_readiness_plan(source)


def derive_task_api_localization_readiness(source: Any) -> TaskApiLocalizationReadinessPlan:
    return build_task_api_localization_readiness_plan(source)


def summarize_task_api_localization_readiness(source: Any) -> TaskApiLocalizationReadinessPlan:
    return build_task_api_localization_readiness_plan(source)


def recommend_task_api_localization_readiness(source: Any) -> TaskApiLocalizationReadinessPlan:
    return build_task_api_localization_readiness_plan(source)


def task_api_localization_readiness_plan_to_dict(report: TaskApiLocalizationReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_api_localization_readiness_plan_to_dict.__test__ = False


def task_api_localization_readiness_plan_to_dicts(
    report: TaskApiLocalizationReadinessPlan | Iterable[TaskApiLocalizationReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(report, TaskSafeguardReadinessPlan):
        return report.to_dicts()
    return [record.to_dict() for record in report]


task_api_localization_readiness_plan_to_dicts.__test__ = False
task_api_localization_readiness_to_dicts = task_api_localization_readiness_plan_to_dicts
task_api_localization_readiness_to_dicts.__test__ = False


def task_api_localization_readiness_plan_to_markdown(report: TaskApiLocalizationReadinessPlan) -> str:
    return report.to_markdown()


task_api_localization_readiness_plan_to_markdown.__test__ = False
