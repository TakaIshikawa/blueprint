"""Framework-neutral API sort parameter helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal, Sequence

SortDirection = Literal["asc", "desc"]


class SortParameterError(ValueError):
    """Raised when an API sort parameter is malformed or unsupported."""


@dataclass(frozen=True, slots=True)
class SortCriterion:
    """One ordered sort field and direction."""

    field: str
    direction: SortDirection = "asc"

    def __post_init__(self) -> None:
        field = self.field.strip()
        if not field:
            raise SortParameterError("Sort field must be non-empty")
        if self.direction not in {"asc", "desc"}:
            raise SortParameterError("Sort direction must be 'asc' or 'desc'")
        object.__setattr__(self, "field", field)


def parse_sort_parameter(
    value: str | None,
    *,
    allowed_fields: Iterable[str] | None = None,
    default: Sequence[SortCriterion | str] | None = None,
) -> tuple[SortCriterion, ...]:
    """Parse a comma-separated API sort value into ordered criteria.

    A leading ``-`` marks descending order. Empty or missing values return the
    deterministic default criteria, when provided.
    """

    allowed = _allowed_field_set(allowed_fields)
    if value is None or not value.strip():
        return _normalize_criteria(default or (), allowed_fields=allowed)

    criteria: list[SortCriterion] = []
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            raise SortParameterError("Sort parameter contains an empty field")
        direction: SortDirection = "desc" if part.startswith("-") else "asc"
        field = part[1:].strip() if direction == "desc" else part
        criteria.append(SortCriterion(field=field, direction=direction))

    return _normalize_criteria(criteria, allowed_fields=allowed)


def default_sort_criteria(*fields: SortCriterion | str) -> tuple[SortCriterion, ...]:
    """Build stable default sort criteria from field names or criteria."""

    return _normalize_criteria(fields, allowed_fields=None)


def sort_criteria_to_query(criteria: Sequence[SortCriterion | str]) -> str:
    """Serialize sort criteria back to a stable API query value."""

    normalized = _normalize_criteria(criteria, allowed_fields=None)
    return ",".join(
        f"-{criterion.field}" if criterion.direction == "desc" else criterion.field
        for criterion in normalized
    )


def _allowed_field_set(allowed_fields: Iterable[str] | None) -> frozenset[str] | None:
    if allowed_fields is None:
        return None
    allowed = frozenset(field.strip() for field in allowed_fields if field.strip())
    if not allowed:
        raise SortParameterError("Allowed sort fields must include at least one field")
    return allowed


def _normalize_criteria(
    criteria: Sequence[SortCriterion | str],
    *,
    allowed_fields: frozenset[str] | None,
) -> tuple[SortCriterion, ...]:
    normalized: list[SortCriterion] = []
    seen: set[str] = set()
    for item in criteria:
        criterion = _coerce_criterion(item)
        if allowed_fields is not None and criterion.field not in allowed_fields:
            raise SortParameterError(f"Unknown sort field: {criterion.field}")
        if criterion.field in seen:
            raise SortParameterError(f"Duplicate sort field: {criterion.field}")
        seen.add(criterion.field)
        normalized.append(criterion)
    return tuple(normalized)


def _coerce_criterion(value: SortCriterion | str) -> SortCriterion:
    if isinstance(value, SortCriterion):
        return value
    if not isinstance(value, str):
        raise SortParameterError("Sort criteria must be strings or SortCriterion objects")
    direction: SortDirection = "desc" if value.strip().startswith("-") else "asc"
    field = value.strip()[1:].strip() if direction == "desc" else value.strip()
    return SortCriterion(field=field, direction=direction)

