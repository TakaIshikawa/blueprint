"""Shared helpers for deterministic source requirement extractors."""

from __future__ import annotations

import re
from typing import Any, Iterable, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SPACE_RE = re.compile(r"\s+")
SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")


def source_payloads(source: Any) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(source, "model_dump"):
        return [source_payload(source)]
    if isinstance(source, Iterable):
        return [source_payload(item) for item in source]
    return [source_payload(source)]


def source_payload(source: Any) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return source_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return source_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = {
            key: getattr(source, key)
            for key in dir(source)
            if not key.startswith("_") and not callable(getattr(source, key, None))
        }
        return source_id(payload), payload
    return None, {}


def source_id(payload: Mapping[str, Any]) -> str | None:
    return optional_text(payload.get("id")) or optional_text(payload.get("source_brief_id")) or optional_text(payload.get("source_id"))


def segments(payload: Mapping[str, Any], scanned_fields: tuple[str, ...], prefix: str | None = None) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    items = payload.items() if prefix else ((key, payload[key]) for key in scanned_fields if key in payload)
    for key, value in items:
        field = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, str):
            for part in SPLIT_RE.split(value):
                if text := clean(part):
                    found.append((field, text))
        elif isinstance(value, Mapping):
            found.extend(segments(value, scanned_fields, field))
        elif isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, str)):
            for index, item in enumerate(value):
                child = f"{field}[{index}]"
                if isinstance(item, str):
                    if text := clean(item):
                        found.append((child, text))
                elif isinstance(item, Mapping):
                    found.extend(segments(item, scanned_fields, child))
    return found


def evidence_snippet(source_field: str, text: str) -> str:
    value = clean(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = clean(str(value))
    return text or None


def clean(value: str) -> str:
    return SPACE_RE.sub(" ", value.strip(" \t\r\n-*+")).strip()


def dedupe(values: Iterable[str | None]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def markdown_cell(value: str) -> str:
    return clean(value).replace("|", "\\|").replace("\n", " ")
