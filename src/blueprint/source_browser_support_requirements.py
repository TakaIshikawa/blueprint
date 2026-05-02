"""Extract browser, device, and client support requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


BrowserSupportClientTarget = Literal[
    "chrome",
    "safari",
    "firefox",
    "edge",
    "mobile_browser",
    "webview",
    "legacy_browser",
    "responsive",
    "tablet",
    "desktop",
    "screen_size",
    "progressive_enhancement",
    "polyfill",
]
BrowserSupportRequirementLevel = Literal["required", "recommended", "mentioned"]
_T = TypeVar("_T")

_CLIENT_TARGET_ORDER: tuple[BrowserSupportClientTarget, ...] = (
    "chrome",
    "safari",
    "firefox",
    "edge",
    "mobile_browser",
    "webview",
    "legacy_browser",
    "responsive",
    "tablet",
    "desktop",
    "screen_size",
    "progressive_enhancement",
    "polyfill",
)
_REQUIREMENT_LEVEL_ORDER: dict[BrowserSupportRequirementLevel, int] = {
    "required": 0,
    "recommended": 1,
    "mentioned": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|need(?:s)?|support(?:s|ed)?|ensure|"
    r"compatible with|works? (?:on|in|across)|has to|acceptance|done when|"
    r"before launch|block(?:er|ing)|minimum target|target browsers?)\b",
    re.I,
)
_RECOMMENDED_RE = re.compile(
    r"\b(?:should|prefer(?:red)?|recommend(?:ed)?|ideally|nice to have|where possible|"
    r"best effort|consider|optimi[sz]e for|compatible where feasible|graceful(?:ly)?)\b",
    re.I,
)
_TARGET_PATTERNS: dict[BrowserSupportClientTarget, re.Pattern[str]] = {
    "chrome": re.compile(r"\b(?:chrome|chromium|google chrome)\b", re.I),
    "safari": re.compile(r"\b(?:safari|mobile safari|ios safari)\b", re.I),
    "firefox": re.compile(r"\b(?:firefox|mozilla firefox)\b", re.I),
    "edge": re.compile(r"\b(?:edge|microsoft edge|edge chromium)\b", re.I),
    "mobile_browser": re.compile(
        r"\b(?:mobile browsers?|mobile web|ios browser|android browser|mobile safari|"
        r"chrome for android|in-app browser|phone browsers?)\b",
        re.I,
    ),
    "webview": re.compile(
        r"\b(?:webviews?|web views?|android webview|ios webview|wkwebview|uiwebview|"
        r"in-app webview|embedded browser)\b",
        re.I,
    ),
    "legacy_browser": re.compile(
        r"\b(?:legacy browsers?|older browsers?|old browsers?|internet explorer|ie11|ie 11|"
        r"ie10|ie 10|es5 browsers?|non evergreen browsers?)\b",
        re.I,
    ),
    "responsive": re.compile(
        r"\b(?:responsive|responsive layout|responsive design|adaptive layout|fluid layout|"
        r"breakpoints?|mobile-first|mobile first)\b",
        re.I,
    ),
    "tablet": re.compile(r"\b(?:tablet|tablets|ipad|android tablet|tablet viewport)\b", re.I),
    "desktop": re.compile(
        r"\b(?:desktop|desktop browser|desktop web|laptop|large screens?|wide screens?)\b",
        re.I,
    ),
    "screen_size": re.compile(
        r"\b(?:screen sizes?|viewport sizes?|viewports?|breakpoints?|"
        r"\d{3,4}\s*(?:px|pixels)|small screens?|large screens?|wide screens?)\b",
        re.I,
    ),
    "progressive_enhancement": re.compile(
        r"\b(?:progressive enhancement|graceful degradation|degrade gracefully|"
        r"baseline experience|enhanced experience|no-js fallback|without javascript)\b",
        re.I,
    ),
    "polyfill": re.compile(
        r"\b(?:polyfill(?:s|ed)?|ponyfill(?:s)?|transpil(?:e|ed|ing)|babel|core-js|"
        r"feature detection)\b",
        re.I,
    ),
}
_RECOMMENDED_VALIDATION: dict[BrowserSupportClientTarget, tuple[str, ...]] = {
    "chrome": ("Run cross-browser tests in current Chrome.",),
    "safari": ("Run cross-browser tests in Safari and Mobile Safari where relevant.",),
    "firefox": ("Run cross-browser tests in current Firefox.",),
    "edge": ("Run cross-browser tests in current Microsoft Edge.",),
    "mobile_browser": ("Run mobile browser checks on iOS and Android.",),
    "webview": ("Validate the experience inside the target native app WebView container.",),
    "legacy_browser": (
        "Run legacy-browser compatibility tests and verify fallback behavior for unsupported APIs.",
    ),
    "responsive": ("Run responsive viewport checks across the defined breakpoints.",),
    "tablet": ("Run tablet viewport checks in portrait and landscape orientations.",),
    "desktop": ("Run desktop viewport checks across standard and wide layouts.",),
    "screen_size": ("Run device viewport checks for the named screen sizes and breakpoints.",),
    "progressive_enhancement": (
        "Verify baseline functionality and graceful fallback when enhanced browser features are unavailable.",
    ),
    "polyfill": (
        "Verify required polyfills or transpilation output in the lowest supported clients.",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceBrowserSupportRequirement:
    """One browser, device, or client support requirement found in source evidence."""

    source_brief_id: str | None
    client_target: BrowserSupportClientTarget
    requirement_level: BrowserSupportRequirementLevel
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_validation: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "client_target": self.client_target,
            "requirement_level": self.requirement_level,
            "matched_terms": list(self.matched_terms),
            "evidence": list(self.evidence),
            "recommended_validation": list(self.recommended_validation),
        }


@dataclass(frozen=True, slots=True)
class SourceBrowserSupportRequirementsReport:
    """Source-level browser support requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceBrowserSupportRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceBrowserSupportRequirement, ...]:
        """Compatibility view matching reports that expose rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Browser Support Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        target_counts = self.summary.get("client_target_counts", {})
        level_counts = self.summary.get("requirement_level_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement level counts: "
            f"required {level_counts.get('required', 0)}, "
            f"recommended {level_counts.get('recommended', 0)}, "
            f"mentioned {level_counts.get('mentioned', 0)}",
            "- Client target counts: "
            + (", ".join(f"{key} {target_counts[key]}" for key in sorted(target_counts)) or "none"),
        ]
        if not self.requirements:
            lines.extend(["", "No browser, device, or client support requirements were found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Client Target | Level | Matched Terms | Evidence | Recommended Validation |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.client_target} | "
                f"{requirement.requirement_level} | "
                f"{_markdown_cell('; '.join(requirement.matched_terms))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.recommended_validation))} |"
            )
        return "\n".join(lines)


def build_source_browser_support_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceBrowserSupportRequirementsReport:
    """Extract browser, device, and client support requirement records from source briefs."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _client_target_index(requirement.client_target),
                _REQUIREMENT_LEVEL_ORDER[requirement.requirement_level],
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceBrowserSupportRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def generate_source_browser_support_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceBrowserSupportRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_browser_support_requirements(source)


def extract_source_browser_support_requirements(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> tuple[SourceBrowserSupportRequirement, ...]:
    """Return browser support requirement records from brief-shaped input."""
    return build_source_browser_support_requirements(source).requirements


def source_browser_support_requirements_to_dict(
    report: SourceBrowserSupportRequirementsReport,
) -> dict[str, Any]:
    """Serialize a browser support requirements report to a plain dictionary."""
    return report.to_dict()


source_browser_support_requirements_to_dict.__test__ = False


def source_browser_support_requirements_to_dicts(
    requirements: (
        tuple[SourceBrowserSupportRequirement, ...] | list[SourceBrowserSupportRequirement]
    ),
) -> list[dict[str, Any]]:
    """Serialize browser support requirement records to dictionaries."""
    return [requirement.to_dict() for requirement in requirements]


source_browser_support_requirements_to_dicts.__test__ = False


def source_browser_support_requirements_to_markdown(
    report: SourceBrowserSupportRequirementsReport,
) -> str:
    """Render a browser support requirements report as Markdown."""
    return report.to_markdown()


source_browser_support_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    client_target: BrowserSupportClientTarget
    matched_term: str
    evidence: str
    requirement_level: BrowserSupportRequirementLevel


def _source_payloads(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief)) or hasattr(
        source, "model_dump"
    ):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
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
            target_terms = _target_terms(segment)
            if not target_terms:
                continue
            requirement_level = _requirement_level(segment, source_field)
            evidence = _evidence_snippet(source_field, segment)
            for client_target, matched_terms in target_terms.items():
                for matched_term in matched_terms:
                    candidates.append(
                        _Candidate(
                            source_brief_id=source_brief_id,
                            client_target=client_target,
                            matched_term=matched_term,
                            evidence=evidence,
                            requirement_level=requirement_level,
                        )
                    )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceBrowserSupportRequirement]:
    grouped: dict[tuple[str | None, BrowserSupportClientTarget], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.client_target), []).append(
            candidate
        )

    requirements: list[SourceBrowserSupportRequirement] = []
    for (source_brief_id, client_target), items in grouped.items():
        requirement_level = min(
            (item.requirement_level for item in items),
            key=lambda value: _REQUIREMENT_LEVEL_ORDER[value],
        )
        requirements.append(
            SourceBrowserSupportRequirement(
                source_brief_id=source_brief_id,
                client_target=client_target,
                requirement_level=requirement_level,
                matched_terms=tuple(
                    sorted(
                        _dedupe(item.matched_term for item in items),
                        key=lambda item: item.casefold(),
                    )
                ),
                evidence=tuple(
                    sorted(
                        _dedupe(item.evidence for item in items), key=lambda item: item.casefold()
                    )
                ),
                recommended_validation=_RECOMMENDED_VALIDATION[client_target],
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
        "design_notes",
        "user_experience",
        "architecture_notes",
        "implementation_notes",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited:
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
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            values.append((source_field, segment))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for sentence in _SENTENCE_SPLIT_RE.split(value):
        segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _target_terms(text: str) -> dict[BrowserSupportClientTarget, tuple[str, ...]]:
    return {
        client_target: tuple(_dedupe(match.group(0).casefold() for match in pattern.finditer(text)))
        for client_target, pattern in _TARGET_PATTERNS.items()
        if pattern.search(text)
    }


def _requirement_level(text: str, source_field: str) -> BrowserSupportRequirementLevel:
    del source_field
    if _REQUIRED_RE.search(text):
        return "required"
    if _RECOMMENDED_RE.search(text):
        return "recommended"
    return "mentioned"


def _summary(
    requirements: tuple[SourceBrowserSupportRequirement, ...], source_count: int
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "client_target_counts": {
            client_target: sum(
                1 for requirement in requirements if requirement.client_target == client_target
            )
            for client_target in _CLIENT_TARGET_ORDER
        },
        "requirement_level_counts": {
            requirement_level: sum(
                1
                for requirement in requirements
                if requirement.requirement_level == requirement_level
            )
            for requirement_level in _REQUIREMENT_LEVEL_ORDER
        },
    }


def _client_target_index(client_target: BrowserSupportClientTarget) -> int:
    return _CLIENT_TARGET_ORDER.index(client_target)


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
        "design_notes",
        "user_experience",
        "architecture_notes",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _TARGET_PATTERNS.values())


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


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


__all__ = [
    "BrowserSupportClientTarget",
    "BrowserSupportRequirementLevel",
    "SourceBrowserSupportRequirement",
    "SourceBrowserSupportRequirementsReport",
    "build_source_browser_support_requirements",
    "extract_source_browser_support_requirements",
    "generate_source_browser_support_requirements",
    "source_browser_support_requirements_to_dict",
    "source_browser_support_requirements_to_dicts",
    "source_browser_support_requirements_to_markdown",
]
