"""Extract deterministic glossary entries from implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


_ACRONYM_RE = re.compile(r"\b[A-Z][A-Z0-9]{1,}(?:-[A-Z0-9]{2,})?\b")
_CAPITALIZED_TERM_RE = re.compile(
    r"\b(?:[A-Z][a-z0-9]+|[A-Z][A-Z0-9]{1,})"
    r"(?:\s+(?:[A-Z][a-z0-9]+|[A-Z][A-Z0-9]{1,})){1,3}\b"
)
_DEFINITION_RE = re.compile(
    r"^\s*(?:[-*]\s*)?"
    r"([A-Za-z][A-Za-z0-9][A-Za-z0-9 /&().'-]{1,80})"
    r":\s+(.+?)\s*$"
)
_QUOTED_TERM_RE = re.compile(r"[\"']([A-Za-z][A-Za-z0-9][A-Za-z0-9 /&().'-]{1,80})[\"']")
_SPLIT_RE = re.compile(r"(?:\r?\n|;)+")
_WORD_RE = re.compile(r"[a-z0-9]+")

_BRIEF_FIELDS = (
    "problem_statement",
    "mvp_goal",
    "scope",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "validation_plan",
    "definition_of_done",
)
_STOP_TERMS: set[str] = set()
_LOWERCASE_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}


@dataclass(frozen=True, slots=True)
class BriefGlossaryEntry:
    """One glossary entry inferred from a brief."""

    term: str
    definition: str | None = None
    sources: tuple[str, ...] = field(default_factory=tuple)
    occurrence_count: int = 0
    confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "term": self.term,
            "definition": self.definition,
            "sources": list(self.sources),
            "occurrence_count": self.occurrence_count,
            "confidence": self.confidence,
        }


def extract_implementation_brief_glossary(
    implementation_brief: Mapping[str, Any] | ImplementationBrief,
    source_brief: Mapping[str, Any] | SourceBrief | None = None,
) -> tuple[BriefGlossaryEntry, ...]:
    """Extract likely domain glossary entries from brief text."""
    brief_payload = _brief_payload(implementation_brief)
    text_fields = _implementation_text_fields(brief_payload)
    if source_brief is not None:
        text_fields.extend(_source_text_fields(_source_payload(source_brief)))

    candidates: dict[str, dict[str, Any]] = {}
    for source, text in text_fields:
        _add_explicit_definitions(candidates, source, text)
        _add_quoted_terms(candidates, source, text)
        _add_acronyms(candidates, source, text)
        _add_capitalized_terms(candidates, source, text)

    entries: list[BriefGlossaryEntry] = []
    for candidate in candidates.values():
        count = _occurrence_count(candidate["term"], text_fields)
        if count < candidate["minimum_occurrences"]:
            continue
        sources = _term_sources(candidate["term"], text_fields)
        confidence = _confidence(candidate["confidence"], count, bool(candidate["definition"]))
        entries.append(
            BriefGlossaryEntry(
                term=candidate["term"],
                definition=candidate["definition"],
                sources=tuple(sources),
                occurrence_count=count,
                confidence=confidence,
            )
        )

    return tuple(
        sorted(
            entries,
            key=lambda entry: (-entry.confidence, entry.term.casefold()),
        )
    )


def brief_glossary_to_dict(
    glossary: tuple[BriefGlossaryEntry, ...] | list[BriefGlossaryEntry],
) -> list[dict[str, Any]]:
    """Serialize glossary entries to dictionaries."""
    return [entry.to_dict() for entry in glossary]


brief_glossary_to_dict.__test__ = False


def _add_explicit_definitions(
    candidates: dict[str, dict[str, Any]],
    source: str,
    text: str,
) -> None:
    for part in _SPLIT_RE.split(text):
        match = _DEFINITION_RE.match(part)
        if match is None:
            continue
        term = _clean_term(match.group(1))
        definition = _optional_text(match.group(2))
        if term and definition and _is_domain_term(term):
            _merge_candidate(
                candidates,
                term=term,
                source=source,
                definition=definition,
                confidence=1.0,
                minimum_occurrences=1,
            )


def _add_quoted_terms(candidates: dict[str, dict[str, Any]], source: str, text: str) -> None:
    for match in _QUOTED_TERM_RE.finditer(text):
        term = _clean_term(match.group(1))
        if term and _is_domain_term(term):
            _merge_candidate(
                candidates,
                term=term,
                source=source,
                confidence=0.85,
                minimum_occurrences=1,
            )


def _add_acronyms(candidates: dict[str, dict[str, Any]], source: str, text: str) -> None:
    for match in _ACRONYM_RE.finditer(text):
        term = _clean_term(match.group(0))
        if term and term.casefold() not in _STOP_TERMS:
            _merge_candidate(
                candidates,
                term=term,
                source=source,
                confidence=0.8,
                minimum_occurrences=1,
            )


def _add_capitalized_terms(
    candidates: dict[str, dict[str, Any]],
    source: str,
    text: str,
) -> None:
    for match in _CAPITALIZED_TERM_RE.finditer(text):
        term = _clean_term(match.group(0))
        if term and _is_domain_term(term):
            _merge_candidate(
                candidates,
                term=term,
                source=source,
                confidence=0.68,
                minimum_occurrences=2,
            )


def _merge_candidate(
    candidates: dict[str, dict[str, Any]],
    *,
    term: str,
    source: str,
    confidence: float,
    minimum_occurrences: int,
    definition: str | None = None,
) -> None:
    key = _term_key(term)
    existing = candidates.get(key)
    if existing is None:
        candidates[key] = {
            "term": term,
            "definition": definition,
            "sources": {source},
            "confidence": confidence,
            "minimum_occurrences": minimum_occurrences,
        }
        return

    existing["sources"].add(source)
    if definition and not existing["definition"]:
        existing["definition"] = definition
        existing["term"] = term
    if confidence > existing["confidence"]:
        existing["confidence"] = confidence
    existing["minimum_occurrences"] = min(existing["minimum_occurrences"], minimum_occurrences)
    if _preferred_term(term, existing["term"]) == term:
        existing["term"] = term


def _confidence(base_confidence: float, occurrence_count: int, has_definition: bool) -> float:
    if has_definition:
        return 1.0
    occurrence_bonus = min(max(occurrence_count - 1, 0) * 0.04, 0.12)
    return round(min(base_confidence + occurrence_bonus, 0.95), 2)


def _occurrence_count(term: str, text_fields: list[tuple[str, str]]) -> int:
    pattern = _term_pattern(term)
    return sum(len(pattern.findall(text)) for _, text in text_fields)


def _term_sources(term: str, text_fields: list[tuple[str, str]]) -> list[str]:
    pattern = _term_pattern(term)
    return [source for source, text in text_fields if pattern.search(text)]


def _term_pattern(term: str) -> re.Pattern[str]:
    escaped = re.escape(term)
    escaped = escaped.replace(r"\ ", r"\s+")
    return re.compile(rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])", re.IGNORECASE)


def _implementation_text_fields(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    for field_name in _BRIEF_FIELDS:
        fields.extend(_text_fields(payload.get(field_name), field_name))
    return fields


def _source_text_fields(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    fields.extend(_text_fields(payload.get("summary"), "source_brief.summary"))
    fields.extend(_text_fields(payload.get("source_payload"), "source_brief.source_payload"))
    return fields


def _text_fields(value: Any, source: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        fields: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            text_key = _optional_text(key)
            if text_key:
                fields.extend(_text_fields(value[key], f"{source}.{text_key}"))
        return fields
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        fields = []
        for index, item in enumerate(items, start=1):
            fields.extend(_text_fields(item, f"{source}.{index}"))
        return fields

    text = _field_text(value)
    return [(source, text)] if text else []


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(brief)


def _source_payload(source_brief: Mapping[str, Any] | SourceBrief) -> dict[str, Any]:
    if hasattr(source_brief, "model_dump"):
        return source_brief.model_dump(mode="python")
    try:
        return SourceBrief.model_validate(source_brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(source_brief)


def _clean_term(value: str) -> str | None:
    term = " ".join(value.strip(" \t\r\n-*:.,;()[]{}").split())
    if not term:
        return None
    words = term.split()
    return " ".join(_canonical_word(word) for word in words)


def _canonical_word(word: str) -> str:
    if word.isupper():
        return word
    if word.casefold() in _LOWERCASE_WORDS:
        return word.casefold()
    return word[:1].upper() + word[1:]


def _preferred_term(candidate: str, existing: str) -> str:
    if candidate.isupper() and not existing.isupper():
        return candidate
    if len(candidate) < len(existing):
        return candidate
    return existing


def _is_domain_term(term: str) -> bool:
    key = _term_key(term)
    if key in _STOP_TERMS:
        return False
    tokens = _WORD_RE.findall(key)
    return bool(tokens) and not all(token in _LOWERCASE_WORDS for token in tokens)


def _term_key(term: str) -> str:
    return " ".join(_WORD_RE.findall(term.casefold()))


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _field_text(value: Any) -> str | None:
    if value is None:
        return None
    lines = [" ".join(line.split()) for line in str(value).splitlines()]
    text = "\n".join(line for line in lines if line)
    return text or None


__all__ = [
    "BriefGlossaryEntry",
    "brief_glossary_to_dict",
    "extract_implementation_brief_glossary",
]
