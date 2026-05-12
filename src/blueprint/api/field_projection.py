"""Framework-neutral sparse field projection helpers for API responses."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence


class FieldProjectionError(ValueError):
    """Raised when sparse field selectors are malformed or unsupported."""


@dataclass(frozen=True, slots=True)
class FieldSelector:
    """One sparse field selector, optionally using dot notation."""

    path: tuple[str, ...]

    @property
    def top_level(self) -> str:
        return self.path[0]

    @property
    def query_value(self) -> str:
        return ".".join(self.path)


def parse_field_selectors(
    value: str | None,
    *,
    allowed_fields: Iterable[str] | None = None,
) -> tuple[FieldSelector, ...]:
    """Parse comma-separated sparse field selectors.

    Allowed-field validation applies to top-level fields so nested selectors
    like ``owner.email`` can be authorized by allowing ``owner``.
    """

    if value is None or not value.strip():
        return ()

    allowed = _allowed_field_set(allowed_fields)
    selectors: list[FieldSelector] = []
    seen: set[str] = set()
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            raise FieldProjectionError("Field selector contains an empty field")
        path = tuple(segment.strip() for segment in part.split("."))
        if any(not segment for segment in path):
            raise FieldProjectionError(f"Invalid field selector: {part}")
        selector = FieldSelector(path)
        if allowed is not None and selector.top_level not in allowed:
            raise FieldProjectionError(f"Unknown field selector: {selector.top_level}")
        if selector.query_value in seen:
            raise FieldProjectionError(f"Duplicate field selector: {selector.query_value}")
        seen.add(selector.query_value)
        selectors.append(selector)
    return tuple(selectors)


def project_fields(
    data: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    selectors: str | Sequence[FieldSelector],
    *,
    allowed_fields: Iterable[str] | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Project a dictionary or list of dictionaries to selected fields."""

    parsed = (
        parse_field_selectors(selectors, allowed_fields=allowed_fields)
        if isinstance(selectors, str)
        else tuple(selectors)
    )
    if isinstance(data, Mapping):
        return _project_mapping(data, parsed)
    return [_project_mapping(item, parsed) for item in data]


def field_selectors_to_query(selectors: Sequence[FieldSelector]) -> str:
    """Serialize field selectors back to a stable API query value."""

    return ",".join(selector.query_value for selector in selectors)


def _allowed_field_set(allowed_fields: Iterable[str] | None) -> frozenset[str] | None:
    if allowed_fields is None:
        return None
    allowed = frozenset(field.strip() for field in allowed_fields if field.strip())
    if not allowed:
        raise FieldProjectionError("Allowed field selectors must include at least one field")
    return allowed


def _project_mapping(
    item: Mapping[str, Any],
    selectors: Sequence[FieldSelector],
) -> dict[str, Any]:
    if not selectors:
        return dict(item)

    projected: dict[str, Any] = {}
    for selector in selectors:
        value = _get_nested(item, selector.path)
        if value is _MISSING:
            continue
        _set_nested(projected, selector.path, value)
    return projected


_MISSING = object()


def _get_nested(item: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = item
    for segment in path:
        if not isinstance(current, Mapping) or segment not in current:
            return _MISSING
        current = current[segment]
    return current


def _set_nested(target: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    current = target
    for segment in path[:-1]:
        child = current.setdefault(segment, {})
        if not isinstance(child, dict):
            child = {}
            current[segment] = child
        current = child
    current[path[-1]] = value

