import pytest

from blueprint.api.cursor_pagination import (
    PaginationError,
    clamp_limit,
    decode_cursor,
    encode_cursor,
    paginate_items,
)


def test_cursor_round_trips_sort_value_and_direction():
    cursor = encode_cursor("2026-01-01", direction="previous", sort_key="created_at")
    payload = decode_cursor(cursor)

    assert payload.sort_value == "2026-01-01"
    assert payload.direction == "previous"
    assert payload.sort_key == "created_at"


def test_limit_is_clamped_to_bounds():
    assert clamp_limit(500, maximum=100) == 100
    assert clamp_limit(0, minimum=1) == 1
    assert clamp_limit(None, default=25) == 25


def test_paginate_items_exposes_metadata_and_cursors():
    result = paginate_items(
        [{"id": "a"}, {"id": "b"}, {"id": "c"}],
        limit=2,
        total_count=3,
    )

    assert [item["id"] for item in result.items] == ["a", "b"]
    assert result.has_next is True
    assert result.has_previous is False
    assert result.next_cursor
    assert result.metadata.total_count == 3


def test_invalid_cursor_raises_clear_error():
    with pytest.raises(PaginationError):
        decode_cursor("not-a-json-cursor")

