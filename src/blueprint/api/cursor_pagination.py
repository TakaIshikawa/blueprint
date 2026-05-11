"""Opaque cursor pagination helpers."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Iterable, Literal, Sequence

from pydantic import BaseModel, ConfigDict, Field

Direction = Literal["next", "previous"]
SortDirection = Literal["asc", "desc"]


class PaginationError(ValueError):
    """Raised when cursor pagination inputs are malformed."""


class CursorPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sort_value: Any
    direction: Direction = "next"
    sort_key: str = "id"
    sort_direction: SortDirection = "asc"


class PaginationMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    has_next: bool = False
    has_previous: bool = False
    next_cursor: str | None = None
    previous_cursor: str | None = None
    limit: int = Field(ge=1)
    total_count: int | None = Field(default=None, ge=0)


@dataclass(frozen=True, slots=True)
class PaginationResult:
    items: tuple[Any, ...]
    has_next: bool
    has_previous: bool
    next_cursor: str | None
    previous_cursor: str | None
    limit: int
    total_count: int | None = None

    @property
    def metadata(self) -> PaginationMetadata:
        return PaginationMetadata(
            has_next=self.has_next,
            has_previous=self.has_previous,
            next_cursor=self.next_cursor,
            previous_cursor=self.previous_cursor,
            limit=self.limit,
            total_count=self.total_count,
        )


def encode_cursor(
    sort_value: Any,
    *,
    direction: Direction = "next",
    sort_key: str = "id",
    sort_direction: SortDirection = "asc",
) -> str:
    payload = CursorPayload(
        sort_value=sort_value,
        direction=direction,
        sort_key=sort_key,
        sort_direction=sort_direction,
    ).model_dump()
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str) -> CursorPayload:
    if not cursor or not isinstance(cursor, str):
        raise PaginationError("Cursor must be a non-empty opaque string")
    try:
        padded = cursor + ("=" * (-len(cursor) % 4))
        raw = base64.urlsafe_b64decode(padded.encode())
        payload = json.loads(raw.decode())
        return CursorPayload(**payload)
    except Exception as exc:  # noqa: BLE001 - normalize parser/model errors
        raise PaginationError("Invalid or malformed pagination cursor") from exc


def clamp_limit(requested: int | None, *, default: int = 50, minimum: int = 1, maximum: int = 100) -> int:
    value = default if requested is None else requested
    return max(minimum, min(maximum, value))


def stable_sort(items: Iterable[Any], *, sort_key: str = "id", reverse: bool = False) -> list[Any]:
    def value(item: Any) -> Any:
        if isinstance(item, dict):
            return (item.get(sort_key), item.get("id"))
        return (getattr(item, sort_key, None), getattr(item, "id", None))

    return sorted(items, key=value, reverse=reverse)


def paginate_items(
    items: Sequence[Any],
    *,
    limit: int | None = None,
    cursor: str | None = None,
    sort_key: str = "id",
    sort_direction: SortDirection = "asc",
    min_limit: int = 1,
    max_limit: int = 100,
    total_count: int | None = None,
) -> PaginationResult:
    page_limit = clamp_limit(limit, minimum=min_limit, maximum=max_limit)
    reverse = sort_direction == "desc"
    ordered = stable_sort(items, sort_key=sort_key, reverse=reverse)
    start = 0
    if cursor:
        payload = decode_cursor(cursor)
        for index, item in enumerate(ordered):
            value = item.get(sort_key) if isinstance(item, dict) else getattr(item, sort_key, None)
            if value == payload.sort_value:
                start = index + (1 if payload.direction == "next" else -page_limit)
                break
        start = max(0, start)
    page = ordered[start : start + page_limit]
    has_previous = start > 0
    has_next = start + page_limit < len(ordered)
    next_cursor = None
    previous_cursor = None
    if page and has_next:
        last = page[-1]
        next_cursor = encode_cursor(
            last.get(sort_key) if isinstance(last, dict) else getattr(last, sort_key, None),
            direction="next",
            sort_key=sort_key,
            sort_direction=sort_direction,
        )
    if page and has_previous:
        first = page[0]
        previous_cursor = encode_cursor(
            first.get(sort_key) if isinstance(first, dict) else getattr(first, sort_key, None),
            direction="previous",
            sort_key=sort_key,
            sort_direction=sort_direction,
        )
    return PaginationResult(
        items=tuple(page),
        has_next=has_next,
        has_previous=has_previous,
        next_cursor=next_cursor,
        previous_cursor=previous_cursor,
        limit=page_limit,
        total_count=total_count,
    )

