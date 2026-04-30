"""Build compact evidence indexes from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


EvidenceKind = Literal[
    "title",
    "summary",
    "domain",
    "timestamp",
    "source_payload",
    "source_link",
]


@dataclass(frozen=True, slots=True)
class SourceEvidenceEntry:
    """One normalized source evidence value."""

    evidence_id: str
    source_brief_id: str
    kind: EvidenceKind
    label: str
    value: Any
    confidence: float
    link: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "evidence_id": self.evidence_id,
            "source_brief_id": self.source_brief_id,
            "kind": self.kind,
            "label": self.label,
            "value": self.value,
            "confidence": self.confidence,
            "link": self.link,
        }


def build_source_evidence_index(
    source_briefs: list[Mapping[str, Any] | SourceBrief]
    | tuple[Mapping[str, Any] | SourceBrief, ...],
) -> tuple[SourceEvidenceEntry, ...]:
    """Extract deterministic evidence entries from SourceBrief-shaped records."""
    entries: list[SourceEvidenceEntry] = []
    for index, source_brief in enumerate(source_briefs, start=1):
        brief = _source_brief_payload(source_brief)
        source_brief_id = _text(brief.get("id")) or f"source-brief-{index}"
        link = _primary_link(_mapping(brief.get("source_links")))
        draft_entries = [
            *_top_level_entries(brief, source_brief_id=source_brief_id, link=link),
            *_nested_entries(
                _mapping(brief.get("source_payload")),
                kind="source_payload",
                source_brief_id=source_brief_id,
                link=link,
                confidence=0.8,
            ),
            *_nested_entries(
                _mapping(brief.get("source_links")),
                kind="source_link",
                source_brief_id=source_brief_id,
                link=link,
                confidence=0.95,
            ),
        ]
        entries.extend(_dedupe_source_entries(draft_entries))

    return tuple(sorted(entries, key=_entry_sort_key))


def source_evidence_index_to_dicts(
    entries: tuple[SourceEvidenceEntry, ...] | list[SourceEvidenceEntry],
) -> list[dict[str, Any]]:
    """Serialize source evidence entries to dictionaries."""
    return [entry.to_dict() for entry in entries]


def _top_level_entries(
    brief: Mapping[str, Any], *, source_brief_id: str, link: str | None
) -> list[SourceEvidenceEntry]:
    entries: list[SourceEvidenceEntry] = []
    for kind, confidence in (
        ("title", 0.9),
        ("summary", 0.85),
        ("domain", 0.75),
    ):
        value = _scalar_value(brief.get(kind))
        if value is not None:
            entries.append(
                _entry(
                    source_brief_id=source_brief_id,
                    kind=kind,
                    label=kind,
                    value=value,
                    confidence=confidence,
                    link=link,
                )
            )

    for field_name in ("created_at", "updated_at"):
        value = _scalar_value(brief.get(field_name))
        if value is not None:
            entries.append(
                _entry(
                    source_brief_id=source_brief_id,
                    kind="timestamp",
                    label=field_name,
                    value=value,
                    confidence=0.7,
                    link=link,
                )
            )
    return entries


def _nested_entries(
    value: Mapping[str, Any],
    *,
    kind: Literal["source_payload", "source_link"],
    source_brief_id: str,
    link: str | None,
    confidence: float,
) -> list[SourceEvidenceEntry]:
    entries: list[SourceEvidenceEntry] = []
    for label, scalar in _flatten(value, prefix=kind):
        entry_link = _text(scalar) if kind == "source_link" and _looks_like_link(scalar) else link
        entries.append(
            _entry(
                source_brief_id=source_brief_id,
                kind=kind,
                label=label,
                value=scalar,
                confidence=confidence,
                link=entry_link,
            )
        )
    return entries


def _entry(
    *,
    source_brief_id: str,
    kind: EvidenceKind,
    label: str,
    value: Any,
    confidence: float,
    link: str | None,
) -> SourceEvidenceEntry:
    return SourceEvidenceEntry(
        evidence_id=_evidence_id(source_brief_id, kind, label, value),
        source_brief_id=source_brief_id,
        kind=kind,
        label=label,
        value=value,
        confidence=confidence,
        link=link,
    )


def _source_brief_payload(source_brief: Mapping[str, Any] | SourceBrief) -> dict[str, Any]:
    if hasattr(source_brief, "model_dump"):
        return source_brief.model_dump(mode="python")
    try:
        return SourceBrief.model_validate(source_brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(source_brief)


def _flatten(value: Any, *, prefix: str) -> list[tuple[str, Any]]:
    flattened: list[tuple[str, Any]] = []

    def append(current: Any, path: str) -> None:
        if isinstance(current, Mapping):
            for key in sorted(current, key=str):
                append(current[key], f"{path}.{key}")
            return
        if isinstance(current, (list, tuple)):
            for index, item in enumerate(current):
                append(item, f"{path}[{index}]")
            return

        scalar = _scalar_value(current)
        if scalar is not None:
            flattened.append((path, scalar))

    append(value, prefix)
    return flattened


def _scalar_value(value: Any) -> Any:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value
    return None


def _dedupe_source_entries(entries: list[SourceEvidenceEntry]) -> list[SourceEvidenceEntry]:
    deduped: list[SourceEvidenceEntry] = []
    seen_values: set[tuple[str, str]] = set()
    for entry in entries:
        value_key = (entry.source_brief_id, _value_fingerprint(entry.value))
        if value_key in seen_values:
            continue
        deduped.append(entry)
        seen_values.add(value_key)
    return deduped


def _evidence_id(source_brief_id: str, kind: str, label: str, value: Any) -> str:
    payload = {
        "source_brief_id": source_brief_id,
        "kind": kind,
        "label": label,
        "value": value,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()[:12]
    return f"evidence-{digest}"


def _entry_sort_key(entry: SourceEvidenceEntry) -> tuple[Any, ...]:
    return (
        entry.source_brief_id,
        _kind_rank(entry.kind),
        entry.label,
        _value_fingerprint(entry.value),
    )


def _kind_rank(kind: EvidenceKind) -> int:
    return {
        "title": 0,
        "summary": 1,
        "domain": 2,
        "timestamp": 3,
        "source_payload": 4,
        "source_link": 5,
    }[kind]


def _primary_link(source_links: Mapping[str, Any]) -> str | None:
    preferred_keys = ("html_url", "url", "source", "spec", "file_path", "path")
    for key in preferred_keys:
        value = _text(source_links.get(key))
        if value:
            return value
    for key in sorted(source_links, key=str):
        value = source_links[key]
        if isinstance(value, list):
            for item in value:
                text = _text(item)
                if text:
                    return text
        else:
            text = _text(value)
            if text:
                return text
    return None


def _looks_like_link(value: Any) -> bool:
    text = _text(value)
    return bool(text) and (
        "://" in text
        or text.startswith("/")
        or text.startswith("./")
        or text.startswith("../")
        or "/" in text
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


def _value_fingerprint(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


__all__ = [
    "EvidenceKind",
    "SourceEvidenceEntry",
    "build_source_evidence_index",
    "source_evidence_index_to_dicts",
]
