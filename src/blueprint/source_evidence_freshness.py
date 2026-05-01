"""Score freshness of source brief evidence without external lookups."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


FreshnessBand = Literal["fresh", "current", "aging", "stale", "missing_date", "malformed_date"]

_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}(?:[T ][0-9:.+-]+Z?)?\b")
_DATE_FIELD_RE = re.compile(
    r"(?:^|_)(?:date|time|timestamp|created_at|updated_at|published_at|"
    r"reported_at|submitted_at|requested_at|reviewed_at|observed_at|captured_at|"
    r"collected_at)$",
    re.IGNORECASE,
)
_EVIDENCE_FIELDS = (
    "evidence",
    "references",
    "timestamps",
    "metadata",
    "created_at",
    "updated_at",
)


@dataclass(frozen=True, slots=True)
class SourceEvidenceFreshnessItem:
    """Freshness assessment for one evidence-like value."""

    source_brief_id: str
    label: str
    value: Any
    band: FreshnessBand
    score: int
    evidence_date: str | None = None
    age_days: int | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "label": self.label,
            "value": self.value,
            "band": self.band,
            "score": self.score,
            "evidence_date": self.evidence_date,
            "age_days": self.age_days,
            "error": self.error,
        }


@dataclass(frozen=True, slots=True)
class SourceEvidenceFreshnessScore:
    """Aggregate source evidence freshness score and remediation hints."""

    score: int
    band: Literal["fresh", "current", "aging", "stale", "missing"]
    evidence: tuple[SourceEvidenceFreshnessItem, ...] = field(default_factory=tuple)
    stale_evidence: tuple[SourceEvidenceFreshnessItem, ...] = field(default_factory=tuple)
    missing_date_evidence: tuple[SourceEvidenceFreshnessItem, ...] = field(default_factory=tuple)
    malformed_date_evidence: tuple[SourceEvidenceFreshnessItem, ...] = field(default_factory=tuple)
    remediation_notes: tuple[str, ...] = field(default_factory=tuple)
    reference_date: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "score": self.score,
            "band": self.band,
            "evidence": [item.to_dict() for item in self.evidence],
            "stale_evidence": [item.to_dict() for item in self.stale_evidence],
            "missing_date_evidence": [item.to_dict() for item in self.missing_date_evidence],
            "malformed_date_evidence": [
                item.to_dict() for item in self.malformed_date_evidence
            ],
            "remediation_notes": list(self.remediation_notes),
            "reference_date": self.reference_date,
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return per-evidence freshness records as plain dictionaries."""
        return [item.to_dict() for item in self.evidence]


def score_source_evidence_freshness(
    source_briefs: (
        Mapping[str, Any]
        | SourceBrief
        | Iterable[Mapping[str, Any] | SourceBrief]
    ),
    *,
    today: date | datetime | str | None = None,
) -> SourceEvidenceFreshnessScore:
    """Score dated source evidence, treating stale, missing, and malformed dates as risks."""
    reference_date = _reference_date(today)
    items = [
        item
        for index, source_brief in enumerate(_source_briefs(source_briefs), start=1)
        for item in _freshness_items(source_brief, index=index, today=reference_date)
    ]
    items.sort(key=_item_sort_key)

    if not items:
        return SourceEvidenceFreshnessScore(
            score=0,
            band="missing",
            remediation_notes=("Add dated evidence or source references before planning.",),
            reference_date=reference_date.isoformat(),
        )

    score = round(sum(item.score for item in items) / len(items))
    stale = tuple(item for item in items if item.band == "stale")
    missing = tuple(item for item in items if item.band == "missing_date")
    malformed = tuple(item for item in items if item.band == "malformed_date")

    return SourceEvidenceFreshnessScore(
        score=score,
        band=_overall_band(score),
        evidence=tuple(items),
        stale_evidence=stale,
        missing_date_evidence=missing,
        malformed_date_evidence=malformed,
        remediation_notes=tuple(_remediation_notes(stale, missing, malformed)),
        reference_date=reference_date.isoformat(),
    )


def build_source_evidence_freshness_score(
    source_briefs: (
        Mapping[str, Any]
        | SourceBrief
        | Iterable[Mapping[str, Any] | SourceBrief]
    ),
    *,
    today: date | datetime | str | None = None,
) -> SourceEvidenceFreshnessScore:
    """Compatibility alias for scoring source evidence freshness."""
    return score_source_evidence_freshness(source_briefs, today=today)


def source_evidence_freshness_score_to_dict(
    result: SourceEvidenceFreshnessScore,
) -> dict[str, Any]:
    """Serialize a freshness score to a plain dictionary."""
    return result.to_dict()


source_evidence_freshness_score_to_dict.__test__ = False


def _freshness_items(
    source_brief: Mapping[str, Any], *, index: int, today: date
) -> list[SourceEvidenceFreshnessItem]:
    source_brief_id = _text(source_brief.get("id")) or _text(source_brief.get("source_id"))
    source_brief_id = source_brief_id or f"source-brief-{index}"
    candidates = _evidence_candidates(source_brief)
    if not candidates:
        candidates = [("source_brief", _brief_label(source_brief))]

    return [
        _freshness_item(source_brief_id, label, value, today=today)
        for label, value in candidates
    ]


def _freshness_item(
    source_brief_id: str,
    label: str,
    value: Any,
    *,
    today: date,
) -> SourceEvidenceFreshnessItem:
    parsed = _date_from_value(value)
    malformed = _malformed_date_value(value)
    if parsed is None and (malformed is not None or _date_field_relevant(label)):
        text = malformed or _text(value)
        if text:
            return SourceEvidenceFreshnessItem(
                source_brief_id=source_brief_id,
                label=label,
                value=_scalar_value(value),
                band="malformed_date",
                score=0,
                error=f"Could not parse date value: {text}",
            )
    if parsed is None:
        return SourceEvidenceFreshnessItem(
            source_brief_id=source_brief_id,
            label=label,
            value=_scalar_value(value),
            band="missing_date",
            score=0,
        )

    age_days = max(0, (today - parsed).days)
    band, score = _date_score(age_days)
    return SourceEvidenceFreshnessItem(
        source_brief_id=source_brief_id,
        label=label,
        value=_scalar_value(value),
        band=band,
        score=score,
        evidence_date=parsed.isoformat(),
        age_days=age_days,
    )


def _evidence_candidates(source_brief: Mapping[str, Any]) -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = []
    for field_name in _EVIDENCE_FIELDS:
        if field_name not in source_brief:
            continue
        value = source_brief.get(field_name)
        if field_name in {"created_at", "updated_at"}:
            scalar = _scalar_value(value)
            if scalar is not None:
                candidates.append((field_name, value))
            continue
        candidates.extend(_flatten(value, prefix=field_name))
    return _dedupe_candidates(candidates)


def _flatten(value: Any, *, prefix: str) -> list[tuple[str, Any]]:
    flattened: list[tuple[str, Any]] = []

    def append(current: Any, path: str) -> None:
        if isinstance(current, Mapping):
            if _is_evidence_record(current):
                flattened.append((path, dict(current)))
                return
            for key in sorted(current, key=str):
                append(current[key], f"{path}.{key}")
            return
        if isinstance(current, (list, tuple)):
            for index, item in enumerate(current):
                append(item, f"{path}[{index}]")
            return

        scalar = _scalar_value(current)
        if scalar is not None:
            flattened.append((path, current))

    append(value, prefix)
    return flattened


def _is_evidence_record(value: Mapping[str, Any]) -> bool:
    text_keys = {"claim", "summary", "title", "reference", "url", "link", "source"}
    keys = {str(key) for key in value}
    return bool(keys & text_keys)


def _date_from_value(value: Any) -> date | None:
    direct = _parse_date(value)
    if direct is not None:
        return direct
    if isinstance(value, Mapping):
        for key in sorted(value, key=str):
            child = value[key]
            if _date_field_relevant(str(key)):
                parsed = _parse_date(child)
                if parsed is not None:
                    return parsed
            if isinstance(child, Mapping | list | tuple):
                parsed = _date_from_value(child)
                if parsed is not None:
                    return parsed
    if isinstance(value, (list, tuple)):
        for item in value:
            parsed = _date_from_value(item)
            if parsed is not None:
                return parsed
    return None


def _malformed_date_value(value: Any) -> str | None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=str):
            child = value[key]
            if _date_field_relevant(str(key)) and _text(child) and _parse_date(child) is None:
                return _text(child)
            if isinstance(child, Mapping | list | tuple):
                malformed = _malformed_date_value(child)
                if malformed is not None:
                    return malformed
    if isinstance(value, (list, tuple)):
        for item in value:
            malformed = _malformed_date_value(item)
            if malformed is not None:
                return malformed
    return None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None
    for match in _DATE_RE.finditer(text):
        candidate = match.group(0)
        if candidate.endswith("Z"):
            candidate = f"{candidate[:-1]}+00:00"
        try:
            return datetime.fromisoformat(candidate).date()
        except ValueError:
            try:
                return date.fromisoformat(candidate[:10])
            except ValueError:
                continue
    return None


def _reference_date(value: date | datetime | str | None) -> date:
    if value is None:
        return datetime.now(timezone.utc).date()
    parsed = _parse_date(value)
    if parsed is None:
        raise ValueError(f"today must be a date, datetime, or ISO date string: {value!r}")
    return parsed


def _date_score(age_days: int) -> tuple[FreshnessBand, int]:
    if age_days <= 30:
        return "fresh", 100
    if age_days <= 90:
        return "current", 75
    if age_days <= 180:
        return "aging", 45
    return "stale", 15


def _overall_band(score: int) -> Literal["fresh", "current", "aging", "stale", "missing"]:
    if score >= 85:
        return "fresh"
    if score >= 65:
        return "current"
    if score >= 35:
        return "aging"
    if score > 0:
        return "stale"
    return "missing"


def _remediation_notes(
    stale: tuple[SourceEvidenceFreshnessItem, ...],
    missing: tuple[SourceEvidenceFreshnessItem, ...],
    malformed: tuple[SourceEvidenceFreshnessItem, ...],
) -> list[str]:
    notes: list[str] = []
    if stale:
        notes.append("Refresh stale evidence before marking the brief execution-ready.")
    if missing:
        notes.append("Add collection, publication, or last-reviewed dates to undated evidence.")
    if malformed:
        notes.append("Correct malformed date values so freshness can be scored deterministically.")
    if not notes:
        notes.append("Evidence dates are fresh enough for planning.")
    return notes


def _source_briefs(
    source: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief],
) -> list[dict[str, Any]]:
    if isinstance(source, SourceBrief):
        return [source.model_dump(mode="python")]
    if isinstance(source, Mapping):
        if "source_briefs" in source:
            return _source_briefs(source.get("source_briefs"))  # type: ignore[arg-type]
        return [_source_brief_payload(source)]

    try:
        iterator = iter(source)
    except TypeError:
        return []

    briefs: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, SourceBrief):
            briefs.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            briefs.append(_source_brief_payload(item))
    return briefs


def _source_brief_payload(source_brief: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return SourceBrief.model_validate(source_brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(source_brief)


def _dedupe_candidates(candidates: list[tuple[str, Any]]) -> list[tuple[str, Any]]:
    deduped: list[tuple[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for label, value in candidates:
        key = (label, str(_scalar_value(value)))
        if key in seen:
            continue
        seen.add(key)
        deduped.append((label, value))
    return deduped


def _date_field_relevant(label: str) -> bool:
    return any(_DATE_FIELD_RE.search(part) for part in label.split("."))


def _brief_label(source_brief: Mapping[str, Any]) -> str:
    return _text(source_brief.get("title")) or _text(source_brief.get("summary")) or "source_brief"


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
    if isinstance(value, Mapping):
        return {
            str(key): _scalar_value(value[key])
            for key in sorted(value, key=str)
            if _scalar_value(value[key]) is not None
        }
    if isinstance(value, (list, tuple)):
        return [
            scalar for item in value if (scalar := _scalar_value(item)) is not None
        ]
    return None


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _item_sort_key(item: SourceEvidenceFreshnessItem) -> tuple[Any, ...]:
    return (item.source_brief_id, item.label, item.evidence_date or "", str(item.value))


__all__ = [
    "SourceEvidenceFreshnessItem",
    "SourceEvidenceFreshnessScore",
    "build_source_evidence_freshness_score",
    "score_source_evidence_freshness",
    "source_evidence_freshness_score_to_dict",
]
