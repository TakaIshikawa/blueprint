"""Extract UI requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


UiRequirementType = Literal[
    "component_specification",
    "interaction_pattern",
    "visual_design",
    "responsive_breakpoints",
    "animations",
    "design_system_alignment",
    "component_reusability",
    "state_management",
    "accessibility_compliance",
    "browser_support",
]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[UiRequirementType, ...] = (
    "component_specification",
    "interaction_pattern",
    "visual_design",
    "responsive_breakpoints",
    "animations",
    "design_system_alignment",
    "component_reusability",
    "state_management",
    "accessibility_compliance",
    "browser_support",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "integration_points",
    "integrations",
    "constraints",
    "metadata",
)

_TYPE_PATTERNS: dict[UiRequirementType, re.Pattern[str]] = {
    "component_specification": re.compile(
        r"\b(?:component(?:s)?|widget(?:s)?|ui element(?:s)?|interface element(?:s)?|"
        r"button(?:s)?|input(?:s)?|form(?:s)?|modal(?:s)?|dialog(?:s)?|dropdown(?:s)?|"
        r"card(?:s)?|table(?:s)?|list(?:s)?|navigation|menu(?:s)?|header|footer|sidebar)\b",
        re.I,
    ),
    "interaction_pattern": re.compile(
        r"\b(?:interaction(?:s)?|user (?:interaction|flow|journey)|click|tap|hover|"
        r"drag(?:ging)?|drop(?:ping)?|scroll(?:ing)?|swipe|gesture(?:s)?|keyboard (?:navigation|shortcut)|"
        r"touch|mouse|focus|blur|submit|cancel|confirm)\b",
        re.I,
    ),
    "visual_design": re.compile(
        r"\b(?:visual design|design|styling|style(?:s)?|color(?:s)?|palette|theme|"
        r"typography|font(?:s)?|spacing|padding|margin|border(?:s)?|shadow(?:s)?|"
        r"layout|grid|flexbox|alignment|icon(?:s)?|image(?:s)?|background)\b",
        re.I,
    ),
    "responsive_breakpoints": re.compile(
        r"\b(?:responsive|breakpoint(?:s)?|mobile|tablet|desktop|screen size(?:s)?|"
        r"media quer(?:y|ies)|viewport|adaptive|fluid|320px|768px|1024px|1440px|"
        r"xs|sm|md|lg|xl|xxl)\b",
        re.I,
    ),
    "animations": re.compile(
        r"\b(?:animation(?:s)?|transition(?:s)?|fade|slide|zoom|rotate|scale|"
        r"spring|easing|duration|delay|keyframe(?:s)?|motion|microinteraction(?:s)?)\b",
        re.I,
    ),
    "design_system_alignment": re.compile(
        r"\b(?:design system|style guide|ui (?:kit|library)|component library|"
        r"material ui|mui|chakra|tailwind|bootstrap|ant design|semantic ui|"
        r"design token(?:s)?|brand guideline(?:s)?|consistency)\b",
        re.I,
    ),
    "component_reusability": re.compile(
        r"\b(?:reusabl(?:e|ility)|shared component(?:s)?|common component(?:s)?|"
        r"composable|modular|atomic design|composition|props|configurable)\b",
        re.I,
    ),
    "state_management": re.compile(
        r"\b(?:state (?:management|handling)|useState|useReducer|redux|mobx|zustand|"
        r"recoil|jotai|context|global state|local state|form state|ui state)\b",
        re.I,
    ),
    "accessibility_compliance": re.compile(
        r"\b(?:accessibilit(?:y|ies)|a11y|wcag|aria|screen reader|keyboard (?:accessible|navigation)|"
        r"tab index|focus (?:indicator|management)|alt text|semantic html|contrast ratio|"
        r"508 compliance|ada compliance)\b",
        re.I,
    ),
    "browser_support": re.compile(
        r"\b(?:browser (?:support|compatibility)|cross[- ]?browser|ie11|edge|chrome|"
        r"firefox|safari|polyfill(?:s)?|autoprefixer|css (?:prefix|vendor)|"
        r"feature detection|caniuse)\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[UiRequirementType, tuple[str, ...]] = {
    "component_specification": (
        "What are the specific component requirements and specifications?",
        "Which UI components need to be built or customized?",
    ),
    "interaction_pattern": (
        "What are the expected user interaction patterns?",
        "How should users interact with UI elements?",
    ),
    "visual_design": (
        "What are the visual design requirements?",
        "Which design tokens, colors, and typography should be used?",
    ),
    "responsive_breakpoints": (
        "What are the responsive breakpoints?",
        "How should the UI adapt across different screen sizes?",
    ),
    "animations": (
        "What animations and transitions are required?",
        "What are the animation timing and easing requirements?",
    ),
    "design_system_alignment": (
        "Which design system or component library should be used?",
        "How should components align with the design system?",
    ),
    "component_reusability": (
        "Which components should be reusable across the application?",
        "How should components be structured for maximum reusability?",
    ),
    "state_management": (
        "How should UI state be managed?",
        "Which state management approach should be used?",
    ),
    "accessibility_compliance": (
        "What are the accessibility requirements (WCAG level)?",
        "Which ARIA attributes and semantic HTML are needed?",
    ),
    "browser_support": (
        "Which browsers and versions need to be supported?",
        "Are polyfills or vendor prefixes required?",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceUiRequirement:
    """One source-backed UI requirement."""

    requirement_type: UiRequirementType
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "follow_up_questions": list(self.follow_up_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceUiRequirementsReport:
    """Source-level UI requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceUiRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceUiRequirement, ...]:
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
        """Return UI requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]


def extract_source_ui_requirements(source: Any) -> SourceUiRequirementsReport:
    """Extract UI requirements from source brief."""
    brief_id, payload = _source_payload(source)
    scanned = _scanned_texts(payload)
    requirements: list[SourceUiRequirement] = []

    for requirement_type in _TYPE_ORDER:
        pattern = _TYPE_PATTERNS[requirement_type]
        evidence_list: list[str] = []
        field_paths: list[str] = []
        matched_terms: set[str] = set()

        for field_path, text in scanned:
            if pattern.search(text):
                evidence_list.append(_snippet(text))
                field_paths.append(field_path)
                for match in pattern.finditer(text):
                    matched_terms.add(match.group().strip())

        if evidence_list:
            requirements.append(
                SourceUiRequirement(
                    requirement_type=requirement_type,
                    evidence=tuple(_dedupe(evidence_list)),
                    source_field_paths=tuple(_dedupe(field_paths)),
                    matched_terms=tuple(sorted(matched_terms, key=str.casefold)),
                    follow_up_questions=_BASE_QUESTIONS[requirement_type],
                )
            )

    return SourceUiRequirementsReport(
        source_brief_id=brief_id,
        requirements=tuple(requirements),
        summary=_summary(requirements),
    )


def _source_payload(source: Any) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, SourceBrief):
        return source.id, source.model_dump(mode="python")
    if isinstance(source, Mapping):
        return source.get("id"), dict(source)
    if hasattr(source, "model_dump"):
        payload = source.model_dump(mode="python")
        return payload.get("id"), payload
    if hasattr(source, "id"):
        payload = {field: getattr(source, field) for field in _SCANNED_FIELDS if hasattr(source, field)}
        return getattr(source, "id", None), payload
    return None, {}


def _scanned_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field in _SCANNED_FIELDS:
        value = payload.get(field)
        if value:
            for field_path, text in _field_texts(field, value):
                texts.append((field_path, text))
    return texts


def _field_texts(field: str, value: Any, prefix: str = "") -> list[tuple[str, str]]:
    field_path = f"{prefix}.{field}" if prefix else field
    if isinstance(value, str):
        return [(field_path, value)]
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key, val in value.items():
            texts.extend(_field_texts(str(key), val, field_path))
        return texts
    if isinstance(value, (list, tuple)):
        texts = []
        for index, item in enumerate(value):
            texts.extend(_field_texts(f"[{index}]", item, field_path))
        return texts
    if value is not None:
        return [(field_path, str(value))]
    return []


def _snippet(text: str, max_length: int = 180) -> str:
    cleaned = _SPACE_RE.sub(" ", str(text)).strip()
    if len(cleaned) <= max_length:
        return cleaned
    return f"{cleaned[:max_length - 3].rstrip()}..."


def _summary(requirements: list[SourceUiRequirement]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "type_counts": {
            req_type: sum(1 for req in requirements if req.requirement_type == req_type)
            for req_type in _TYPE_ORDER
        },
    }


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
