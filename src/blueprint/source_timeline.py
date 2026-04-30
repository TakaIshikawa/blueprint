"""Extract deterministic timeline events from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


_ARRAY_TIMELINE_KEYS = ("timeline", "events", "milestones")
_DATE_FIELD_KEYS = (
    "date",
    "datetime",
    "timestamp",
    "time",
    "due_date",
    "deadline",
    "target_date",
    "start_date",
    "end_date",
)
_DEADLINE_KEYS = ("due_date", "deadline", "target_date")
_LABEL_KEYS = ("label", "title", "name", "summary", "description")


@dataclass(frozen=True)
class SourceTimelineEvent:
    """One timeline event extracted from a SourceBrief."""

    label: str
    date: str | None
    source_key: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "label": self.label,
            "date": self.date,
            "source_key": self.source_key,
            "metadata": self.metadata,
        }


def extract_source_timeline(
    source_brief: Mapping[str, Any] | SourceBrief,
) -> tuple[SourceTimelineEvent, ...]:
    """Return chronological events found in SourceBrief timestamps and payload."""
    brief = _source_brief_payload(source_brief)
    source_payload = _mapping(brief.get("source_payload"))
    events: list[tuple[int, SourceTimelineEvent]] = []

    _append_timestamp_event(events, brief, "created_at", "Source brief created")
    _append_timestamp_event(events, brief, "updated_at", "Source brief updated")

    sequence = len(events)
    for source_key in _ARRAY_TIMELINE_KEYS:
        for item_index, item in enumerate(_list(source_payload.get(source_key))):
            event = _array_item_event(item, source_key, item_index)
            if event is not None:
                events.append((sequence, event))
                sequence += 1

    for source_key in _DEADLINE_KEYS:
        event = _deadline_event(source_payload.get(source_key), source_key)
        if event is not None:
            events.append((sequence, event))
            sequence += 1

    return tuple(event for _, event in sorted(events, key=_event_sort_key))


def source_timeline_to_dicts(
    events: tuple[SourceTimelineEvent, ...] | list[SourceTimelineEvent],
) -> list[dict[str, Any]]:
    """Serialize source timeline events to dictionaries."""
    return [event.to_dict() for event in events]


def _append_timestamp_event(
    events: list[tuple[int, SourceTimelineEvent]],
    brief: Mapping[str, Any],
    source_key: str,
    label: str,
) -> None:
    parsed = _parse_iso_temporal(brief.get(source_key))
    if parsed is None:
        raw_value = brief.get(source_key)
        if raw_value in (None, ""):
            return
        events.append(
            (
                len(events),
                SourceTimelineEvent(
                    label=label,
                    date=None,
                    source_key=source_key,
                    metadata={"raw_date": raw_value},
                ),
            )
        )
        return

    events.append(
        (
            len(events),
            SourceTimelineEvent(
                label=label,
                date=parsed,
                source_key=source_key,
                metadata={},
            ),
        )
    )


def _array_item_event(item: Any, source_key: str, item_index: int) -> SourceTimelineEvent | None:
    item_source_key = f"{source_key}[{item_index}]"
    if isinstance(item, Mapping):
        payload = dict(item)
        label = _first_text(payload, _LABEL_KEYS) or _humanize_key(source_key)
        date_key, raw_date = _first_present(payload, _DATE_FIELD_KEYS)
        parsed = _parse_iso_temporal(raw_date)
        metadata = _event_metadata(payload, date_key)
        if raw_date not in (None, "") and parsed is None:
            metadata["raw_date"] = raw_date
        return SourceTimelineEvent(
            label=label,
            date=parsed,
            source_key=item_source_key,
            metadata=metadata,
        )

    if isinstance(item, str) and item.strip():
        return SourceTimelineEvent(
            label=item.strip(),
            date=None,
            source_key=item_source_key,
            metadata={},
        )
    return None


def _deadline_event(value: Any, source_key: str) -> SourceTimelineEvent | None:
    if value in (None, ""):
        return None
    parsed = _parse_iso_temporal(value)
    metadata: dict[str, Any] = {}
    if parsed is None:
        metadata["raw_date"] = value
    return SourceTimelineEvent(
        label=_humanize_key(source_key),
        date=parsed,
        source_key=source_key,
        metadata=metadata,
    )


def _event_metadata(payload: Mapping[str, Any], date_key: str | None) -> dict[str, Any]:
    omitted = set(_LABEL_KEYS)
    if date_key is not None:
        omitted.add(date_key)
    return {
        str(key): payload[key]
        for key in sorted(payload, key=str)
        if key not in omitted and payload[key] is not None
    }


def _source_brief_payload(source_brief: Mapping[str, Any] | SourceBrief) -> dict[str, Any]:
    if hasattr(source_brief, "model_dump"):
        return source_brief.model_dump(mode="python")
    try:
        return SourceBrief.model_validate(source_brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(source_brief)


def _parse_iso_temporal(value: Any) -> str | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = f"{candidate[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            try:
                return date.fromisoformat(candidate).isoformat()
            except ValueError:
                return None
    else:
        return None

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.isoformat()


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _first_present(payload: Mapping[str, Any], keys: tuple[str, ...]) -> tuple[str | None, Any]:
    for key in keys:
        if key in payload:
            return key, payload[key]
    return None, None


def _first_text(payload: Mapping[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _humanize_key(value: str) -> str:
    return value.replace("_", " ").capitalize()


def _event_sort_key(item: tuple[int, SourceTimelineEvent]) -> tuple[Any, ...]:
    sequence, event = item
    return (
        event.date is None,
        event.date or "",
        sequence,
        event.source_key,
        event.label,
    )


__all__ = [
    "SourceTimelineEvent",
    "extract_source_timeline",
    "source_timeline_to_dicts",
]
