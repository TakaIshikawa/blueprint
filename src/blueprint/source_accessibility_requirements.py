"""Extract source-level accessibility requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceAccessibilitySignal = Literal[
    "keyboard",
    "screen_reader",
    "contrast",
    "reduced_motion",
    "captions",
    "focus_management",
    "wcag_conformance",
    "aria_labels",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: dict[SourceAccessibilitySignal, int] = {
    "keyboard": 0,
    "screen_reader": 1,
    "contrast": 2,
    "reduced_motion": 3,
    "captions": 4,
    "focus_management": 5,
    "wcag_conformance": 6,
    "aria_labels": 7,
}
_SIGNAL_PATTERNS: dict[SourceAccessibilitySignal, re.Pattern[str]] = {
    "keyboard": re.compile(
        r"\b(?:keyboard(?: navigation| accessible| support)?|tab(?:bing)?|tab order|"
        r"shortcut keys?|hotkeys?|enter key|escape key|space(?:bar)? key|arrow keys?|"
        r"without (?:a )?mouse|cannot use (?:a )?mouse|no mouse|mouse[- ]free|"
        r"pointer[- ]free|non[- ]pointer)\b",
        re.I,
    ),
    "screen_reader": re.compile(
        r"\b(?:screen reader|assistive technolog(?:y|ies)|voiceover|nvda|jaws|talkback|"
        r"narrator|read(?:able)? by screen readers?|announce(?:d|ment)?s?)\b",
        re.I,
    ),
    "contrast": re.compile(
        r"\b(?:contrast|color contrast|colour contrast|high contrast|text contrast|"
        r"4\.5:1|3:1|contrast ratio|low vision)\b",
        re.I,
    ),
    "reduced_motion": re.compile(
        r"\b(?:reduced motion|prefers-reduced-motion|reduce motion|motion sensitivity|"
        r"disable animations?|skip animations?|no animations?|vestibular)\b",
        re.I,
    ),
    "captions": re.compile(
        r"\b(?:captions?|closed captions?|subtitles?|transcripts?|audio description|"
        r"media alternatives?|video alternatives?)\b",
        re.I,
    ),
    "focus_management": re.compile(
        r"\b(?:focus management|focus order|focus trap|focus ring|focus indicator|focus states?|"
        r"initial focus|restore focus|return focus|visible focus|skip link|skip to content)\b",
        re.I,
    ),
    "wcag_conformance": re.compile(
        r"\b(?:wcag|web content accessibility guidelines|a11y|accessibility conformance|"
        r"accessibility compliance|aa compliant|aaa compliant|level aa|level aaa|"
        r"level a conformance|level aa conformance|level aaa conformance|section 508|ada compliant)\b",
        re.I,
    ),
    "aria_labels": re.compile(
        r"\b(?:aria[- ]label(?:s|ledby)?|aria labels?|accessible names?|alt text|"
        r"image descriptions?|semantic labels?|labelled controls?)\b",
        re.I,
    ),
}
_STRUCTURED_CONFIDENCE_FIELDS = {
    "accessibility",
    "accessibility_requirements",
    "acceptance_criteria",
    "criteria",
    "definition_of_done",
    "constraints",
    "non_functional_requirements",
    "requirements",
}
_BRIEF_TEXT_FIELDS = (
    "title",
    "summary",
    "domain",
    "target_user",
    "buyer",
    "workflow_context",
    "problem_statement",
    "mvp_goal",
    "product_surface",
    "architecture_notes",
    "data_requirements",
    "validation_plan",
    "generation_prompt",
)
_BRIEF_LIST_FIELDS = (
    "personas",
    "constraints",
    "acceptance_criteria",
    "criteria",
    "notes",
    "scope",
    "non_goals",
    "assumptions",
    "risks",
    "definition_of_done",
    "integration_points",
)
_PAYLOAD_FIELDS = ("source_payload", "metadata", "payload")
_SUGGESTED_ACCEPTANCE_CRITERIA: dict[SourceAccessibilitySignal, str] = {
    "keyboard": "Keyboard users can reach and operate all interactive controls without a pointer.",
    "screen_reader": "Screen reader users receive meaningful roles, names, states, and status announcements.",
    "contrast": "Text, icons, and focus indicators meet the required color contrast ratios.",
    "reduced_motion": "Motion-heavy UI respects reduced-motion preferences without losing essential context.",
    "captions": "Audio and video content includes captions, transcripts, or equivalent alternatives.",
    "focus_management": "Focus order, initial focus, trapped focus, and restored focus are predictable.",
    "wcag_conformance": "Accessibility acceptance testing verifies the stated WCAG conformance target.",
    "aria_labels": "Icon-only and ambiguous controls expose accurate accessible names.",
}


@dataclass(frozen=True, slots=True)
class SourceAccessibilityRequirement:
    """One accessibility requirement inferred from source brief evidence."""

    signal: SourceAccessibilitySignal
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_acceptance_criterion: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "signal": self.signal,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "suggested_acceptance_criterion": self.suggested_acceptance_criterion,
        }


@dataclass(frozen=True, slots=True)
class SourceAccessibilityRequirementInventory:
    """Inventory of accessibility requirements found in a source brief."""

    source_id: str | None = None
    requirements: tuple[SourceAccessibilityRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAccessibilityRequirement, ...]:
        """Compatibility view matching inventories that name rows records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return accessibility requirements as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]


def build_source_accessibility_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief,
) -> SourceAccessibilityRequirementInventory:
    """Build an accessibility requirement inventory from a source or implementation brief."""
    payload = _source_payload(source)
    source_id = _source_id(payload)
    detected: dict[SourceAccessibilitySignal, list[tuple[str, float]]] = {}

    for source_field, text in _candidate_texts(payload):
        confidence = _base_confidence(source_field)
        evidence = _evidence_snippet(source_field, text)
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text):
                detected.setdefault(signal, []).append((evidence, confidence))

    requirements = tuple(
        SourceAccessibilityRequirement(
            signal=signal,
            confidence=_confidence(evidence_confidences),
            evidence=tuple(_dedupe(evidence for evidence, _ in evidence_confidences)),
            suggested_acceptance_criterion=_SUGGESTED_ACCEPTANCE_CRITERIA[signal],
        )
        for signal, evidence_confidences in sorted(detected.items(), key=lambda item: _SIGNAL_ORDER[item[0]])
    )
    return SourceAccessibilityRequirementInventory(
        source_id=source_id,
        requirements=requirements,
        summary=_summary(requirements, evidence_count=sum(len(items) for items in detected.values())),
    )


def extract_source_accessibility_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief,
) -> SourceAccessibilityRequirementInventory:
    """Compatibility alias for building a source accessibility requirement inventory."""
    return build_source_accessibility_requirements(source)


def source_accessibility_requirements_to_dict(
    result: SourceAccessibilityRequirementInventory,
) -> dict[str, Any]:
    """Serialize a source accessibility requirement inventory to a plain dictionary."""
    return result.to_dict()


source_accessibility_requirements_to_dict.__test__ = False


def _source_payload(source: Mapping[str, Any] | SourceBrief | ImplementationBrief) -> dict[str, Any]:
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        return source.model_dump(mode="python")
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                value = model.model_validate(source).model_dump(mode="python")
                return dict(value) if isinstance(value, Mapping) else {}
            except (TypeError, ValueError, ValidationError):
                continue
        return dict(source)
    return {}


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in _BRIEF_TEXT_FIELDS:
        if text := _optional_text(payload.get(field_name)):
            texts.append((field_name, text))
    for field_name in _BRIEF_LIST_FIELDS:
        for source_field, text in _nested_texts(payload.get(field_name), field_name):
            texts.append((source_field, text))
    for field_name in _PAYLOAD_FIELDS:
        for source_field, text in _nested_texts(payload.get(field_name), field_name):
            texts.append((source_field, text))
    return texts


def _nested_texts(value: Any, prefix: str) -> list[tuple[str, str]]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            key_matches = any(pattern.search(key_text) for pattern in _SIGNAL_PATTERNS.values())
            if isinstance(child, Mapping) or _list_like(child):
                if key_matches and _truthy_signal_value(child):
                    texts.append((field, key_text))
                texts.extend(_nested_texts(child, field))
            elif text := _optional_text(child):
                if key_matches and _truthy_signal_value(child):
                    texts.append((field, f"{key_text}: {text}"))
                else:
                    texts.append((field, text))
            elif key_matches and _truthy_signal_value(child):
                texts.append((field, key_text))
        return texts
    if _list_like(value):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            texts.extend(_nested_texts(item, f"{prefix}[{index}]"))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _list_like(value: Any) -> bool:
    return isinstance(value, (list, tuple, set))


def _truthy_signal_value(value: Any) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, str) and value.strip().casefold() in {"", "false", "no", "none", "n/a"}:
        return False
    return True


def _base_confidence(source_field: str) -> float:
    field = source_field.split("[", 1)[0].split(".", 1)[0]
    if field in _STRUCTURED_CONFIDENCE_FIELDS:
        return 0.9
    if source_field.startswith(_PAYLOAD_FIELDS):
        return 0.84
    if field in {"summary", "problem_statement", "workflow_context", "mvp_goal", "validation_plan"}:
        return 0.8
    return 0.72


def _confidence(evidence_confidences: list[tuple[str, float]]) -> float:
    unique_evidence_count = len(_dedupe(evidence for evidence, _ in evidence_confidences))
    best = max((confidence for _, confidence in evidence_confidences), default=0.0)
    boosted = best + min(0.08, max(0, unique_evidence_count - 1) * 0.04)
    return round(min(0.99, boosted), 2)


def _summary(
    requirements: tuple[SourceAccessibilityRequirement, ...],
    *,
    evidence_count: int,
) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "signals": [signal for signal in _SIGNAL_ORDER if any(item.signal == signal for item in requirements)],
        "signal_counts": {
            signal: sum(1 for item in requirements if item.signal == signal)
            for signal in _SIGNAL_ORDER
        },
        "evidence_count": evidence_count,
    }


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


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
    "SourceAccessibilityRequirement",
    "SourceAccessibilityRequirementInventory",
    "SourceAccessibilitySignal",
    "build_source_accessibility_requirements",
    "extract_source_accessibility_requirements",
    "source_accessibility_requirements_to_dict",
]
