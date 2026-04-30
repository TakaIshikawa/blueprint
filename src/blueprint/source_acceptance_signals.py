"""Extract acceptance-like signals from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


AcceptanceSignalType = Literal["checklist", "must", "should", "done_when", "acceptance"]

_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+(?P<text>.+?)\s*$")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_DONE_WHEN_RE = re.compile(r"\bdone\s+when\b", re.IGNORECASE)
_MUST_RE = re.compile(r"\bmust\b", re.IGNORECASE)
_SHOULD_RE = re.compile(r"\bshould\b", re.IGNORECASE)
_ACCEPTANCE_RE = re.compile(r"\bacceptance\b", re.IGNORECASE)

_DIRECT_PAYLOAD_FIELDS = (
    "body",
    "description",
    "checklist",
    "acceptance",
    "acceptance_criteria",
    "criteria",
    "requirements",
)


@dataclass(frozen=True, slots=True)
class SourceAcceptanceSignal:
    """One acceptance-like statement found in a source brief."""

    source_brief_id: str
    text: str
    signal_type: AcceptanceSignalType
    confidence: float
    source_field: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "text": self.text,
            "signal_type": self.signal_type,
            "confidence": self.confidence,
            "source_field": self.source_field,
        }


def extract_source_acceptance_signals(
    source_brief: Mapping[str, Any] | SourceBrief,
) -> tuple[SourceAcceptanceSignal, ...]:
    """Return acceptance-like signals from one SourceBrief-shaped record."""
    brief = _source_brief_payload(source_brief)
    source_brief_id = _text(brief.get("id")) or _text(brief.get("source_id")) or "source-brief"
    payload = _mapping(brief.get("source_payload"))

    candidates: list[tuple[str, str]] = []
    _append_text(candidates, "title", brief.get("title"))
    _append_text(candidates, "summary", brief.get("summary"))

    visited_payload_fields: set[str] = set()
    for field_name in _DIRECT_PAYLOAD_FIELDS:
        if field_name in payload:
            source_field = f"source_payload.{field_name}"
            _append_payload_value(candidates, payload[field_name], source_field)
            visited_payload_fields.add(source_field)

    if "normalized" in payload:
        _append_payload_value(candidates, payload["normalized"], "source_payload.normalized")
        visited_payload_fields.add("source_payload.normalized")

    for source_field, value in _flatten_payload(payload, prefix="source_payload"):
        if source_field in visited_payload_fields or _is_under_visited_field(
            source_field, visited_payload_fields
        ):
            continue
        _append_text(candidates, source_field, value)

    signals: list[SourceAcceptanceSignal] = []
    seen_texts: set[str] = set()
    for source_field, value in candidates:
        for text, signal_type in _signals_from_text(value):
            dedupe_key = _dedupe_key(text)
            if dedupe_key in seen_texts:
                continue
            seen_texts.add(dedupe_key)
            signals.append(
                SourceAcceptanceSignal(
                    source_brief_id=source_brief_id,
                    text=text,
                    signal_type=signal_type,
                    confidence=_confidence(signal_type),
                    source_field=source_field,
                )
            )

    return tuple(signals)


def build_source_acceptance_signals(
    source_briefs: list[Mapping[str, Any] | SourceBrief]
    | tuple[Mapping[str, Any] | SourceBrief, ...],
) -> tuple[SourceAcceptanceSignal, ...]:
    """Return acceptance-like signals from multiple SourceBrief-shaped records."""
    signals: list[SourceAcceptanceSignal] = []
    for source_brief in source_briefs:
        signals.extend(extract_source_acceptance_signals(source_brief))
    return tuple(signals)


def source_acceptance_signals_to_dicts(
    signals: tuple[SourceAcceptanceSignal, ...] | list[SourceAcceptanceSignal],
) -> list[dict[str, Any]]:
    """Serialize source acceptance signals to dictionaries."""
    return [signal.to_dict() for signal in signals]


def _source_brief_payload(source_brief: Mapping[str, Any] | SourceBrief) -> dict[str, Any]:
    if hasattr(source_brief, "model_dump"):
        return source_brief.model_dump(mode="python")
    try:
        return SourceBrief.model_validate(source_brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        if isinstance(source_brief, Mapping):
            return dict(source_brief)
    return {}


def _append_payload_value(candidates: list[tuple[str, str]], value: Any, source_field: str) -> None:
    if isinstance(value, Mapping):
        for child_field, child_value in _flatten_payload(value, prefix=source_field):
            _append_text(candidates, child_field, child_value)
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _append_payload_value(candidates, item, f"{source_field}[{index}]")
        return
    _append_text(candidates, source_field, value)


def _append_text(candidates: list[tuple[str, str]], source_field: str, value: Any) -> None:
    text = _source_text(value)
    if text is not None:
        candidates.append((source_field, text))


def _flatten_payload(value: Any, *, prefix: str) -> list[tuple[str, Any]]:
    flattened: list[tuple[str, Any]] = []

    def append(current: Any, path: str) -> None:
        if isinstance(current, Mapping):
            for key, child in current.items():
                append(child, f"{path}.{key}")
            return
        if isinstance(current, (list, tuple)):
            for index, item in enumerate(current):
                append(item, f"{path}[{index}]")
            return
        flattened.append((path, current))

    append(value, prefix)
    return flattened


def _signals_from_text(value: str) -> list[tuple[str, AcceptanceSignalType]]:
    signals: list[tuple[str, AcceptanceSignalType]] = []
    for line in value.splitlines():
        checkbox = _CHECKBOX_RE.match(line)
        if checkbox is not None:
            text = _clean_text(checkbox.group("text"))
            if text:
                signals.append((text, "checklist"))
            continue
        for segment in _sentence_segments(line):
            signal_type = _classify(segment)
            if signal_type is not None:
                signals.append((_clean_text(segment), signal_type))
    return signals


def _sentence_segments(value: str) -> list[str]:
    segments: list[str] = []
    for part in _SENTENCE_SPLIT_RE.split(value):
        text = _clean_text(part)
        if text:
            segments.append(text)
    return segments


def _classify(text: str) -> AcceptanceSignalType | None:
    if _DONE_WHEN_RE.search(text):
        return "done_when"
    if _MUST_RE.search(text):
        return "must"
    if _SHOULD_RE.search(text):
        return "should"
    if _ACCEPTANCE_RE.search(text):
        return "acceptance"
    return None


def _confidence(signal_type: AcceptanceSignalType) -> float:
    return {
        "checklist": 0.95,
        "done_when": 0.9,
        "must": 0.88,
        "acceptance": 0.86,
        "should": 0.82,
    }[signal_type]


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return " ".join(text.split())


def _dedupe_key(text: str) -> str:
    return _clean_text(text).casefold()


def _is_under_visited_field(source_field: str, visited_fields: set[str]) -> bool:
    return any(
        source_field.startswith(f"{visited}.") or source_field.startswith(f"{visited}[")
        for visited in visited_fields
    )


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _source_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


__all__ = [
    "AcceptanceSignalType",
    "SourceAcceptanceSignal",
    "build_source_acceptance_signals",
    "extract_source_acceptance_signals",
    "source_acceptance_signals_to_dicts",
]
