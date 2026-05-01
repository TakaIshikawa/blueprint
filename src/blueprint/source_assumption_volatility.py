"""Score volatile SourceBrief assumptions before implementation planning."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


AssumptionVolatility = Literal["high", "medium", "low"]

_SIGNAL_RE = re.compile(
    r"\b(?:assum(?:e|es|ed|ing|ption|ptions)|depends?\s+on|pending|subject\s+to|"
    r"tbd|estimate|estimated|vendor|deadline|legal|pricing|api\s+limit|"
    r"rate\s+limit|staffing)\b",
    re.IGNORECASE,
)
_EXPLICIT_ASSUMPTION_RE = re.compile(
    r"\b(?:assum(?:e|es|ed|ing|ption|ptions)|estimate|estimated)\b",
    re.IGNORECASE,
)
_PENDING_RE = re.compile(
    r"\b(?:pending|subject\s+to|tbd|todo|waiting|unresolved|unknown|confirm|"
    r"clarify|blocked|blocker|deadline)\b",
    re.IGNORECASE,
)
_EXTERNAL_RE = re.compile(
    r"\b(?:vendor|partner|external|third[- ]party|legal|pricing|api\s+limit|"
    r"rate\s+limit|contract|approval|deadline|staffing)\b",
    re.IGNORECASE,
)
_QUESTION_FIELD_RE = re.compile(
    r"(?:^|\.)(?:open_)?(?:questions?|unresolved_questions|unknowns|ambiguities)(?:$|\.|\[)",
    re.IGNORECASE,
)
_ASSUMPTION_FIELD_RE = re.compile(
    r"(?:^|\.)(?:assumptions?|metadata_assumptions)(?:$|\.|\[)",
    re.IGNORECASE,
)
_METADATA_FIELD_RE = re.compile(r"(?:^|\.)metadata(?:$|\.|\[)", re.IGNORECASE)
_DATE_FIELD_RE = re.compile(
    r"(?:^|_)(?:date|time|timestamp|created_at|updated_at|published_at|"
    r"reported_at|submitted_at|reviewed_at|observed_at)$",
    re.IGNORECASE,
)
_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}(?:[T ][0-9:.+-]+Z?)?\b")
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|(?:\r?\n|;)+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)]|\[[ xX]\])\s+")
_PRIORITY = {"high": 0, "medium": 1, "low": 2}


@dataclass(frozen=True, slots=True)
class SourceAssumptionVolatilityRecord:
    """One volatile assumption-like source signal."""

    source_brief_id: str
    assumption: str
    volatility: AssumptionVolatility
    score: int
    evidence_type: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_fields: tuple[str, ...] = field(default_factory=tuple)
    follow_up_actions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "assumption": self.assumption,
            "volatility": self.volatility,
            "score": self.score,
            "evidence_type": self.evidence_type,
            "evidence": list(self.evidence),
            "source_fields": list(self.source_fields),
            "follow_up_actions": list(self.follow_up_actions),
        }


@dataclass(frozen=True, slots=True)
class SourceAssumptionVolatilityReport:
    """Volatility report for assumptions found in one or more source briefs."""

    records: tuple[SourceAssumptionVolatilityRecord, ...] = field(default_factory=tuple)
    source_count: int = 0

    @property
    def summary_counts(self) -> dict[str, int]:
        """Return counts by volatility band in stable key order."""
        return {
            "high": sum(1 for record in self.records if record.volatility == "high"),
            "medium": sum(1 for record in self.records if record.volatility == "medium"),
            "low": sum(1 for record in self.records if record.volatility == "low"),
        }

    @property
    def assumption_count(self) -> int:
        """Return the number of volatility records."""
        return len(self.records)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_count": self.source_count,
            "assumption_count": self.assumption_count,
            "summary_counts": self.summary_counts,
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return volatility records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        lines = ["# Source Assumption Volatility"]
        lines.extend(
            [
                "",
                (
                    "Summary: "
                    f"{self.summary_counts['high']} high, "
                    f"{self.summary_counts['medium']} medium, "
                    f"{self.summary_counts['low']} low."
                ),
            ]
        )
        if not self.records:
            lines.extend(["", "No volatile source assumptions found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Volatility | Score | Source | Assumption | Evidence | Follow-up |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"{record.volatility} | "
                f"{record.score} | "
                f"{_markdown_cell(record.source_brief_id)} | "
                f"{_markdown_cell(record.assumption)} | "
                f"{_markdown_cell('; '.join(record.evidence))} | "
                f"{_markdown_cell('; '.join(record.follow_up_actions))} |"
            )
        return "\n".join(lines)


def build_source_assumption_volatility_report(
    source: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief],
    *,
    today: date | datetime | str | None = None,
) -> SourceAssumptionVolatilityReport:
    """Score assumption volatility for SourceBrief, dict, or iterable inputs."""
    briefs = _source_briefs(source)
    reference_date = _reference_date(today)
    records: list[SourceAssumptionVolatilityRecord] = []
    for index, brief in enumerate(briefs, start=1):
        records.extend(_brief_records(brief, index=index, today=reference_date))
    records.sort(key=_record_sort_key)
    return SourceAssumptionVolatilityReport(records=tuple(records), source_count=len(briefs))


def source_assumption_volatility_report_to_dict(
    report: SourceAssumptionVolatilityReport,
) -> dict[str, Any]:
    """Serialize an assumption volatility report to a plain dictionary."""
    return report.to_dict()


source_assumption_volatility_report_to_dict.__test__ = False


def source_assumption_volatility_report_to_markdown(
    report: SourceAssumptionVolatilityReport,
) -> str:
    """Render an assumption volatility report as Markdown."""
    return report.to_markdown()


source_assumption_volatility_report_to_markdown.__test__ = False


def _brief_records(
    brief: Mapping[str, Any], *, index: int, today: date
) -> list[SourceAssumptionVolatilityRecord]:
    source_brief_id = (
        _optional_text(brief.get("id"))
        or _optional_text(brief.get("source_id"))
        or f"source-brief-{index}"
    )
    candidates: dict[str, _Candidate] = {}
    freshness_evidence = _freshness_evidence(brief, today=today)

    for source_field, value in _brief_values(brief):
        for text in _assumption_texts(value, source_field):
            key = _dedupe_key(text)
            existing = candidates.get(key)
            if existing is None:
                candidates[key] = _Candidate(text, [source_field])
            elif source_field not in existing.source_fields:
                existing.source_fields.append(source_field)

    return [
        _record(source_brief_id, candidate, freshness_evidence=freshness_evidence)
        for candidate in candidates.values()
    ]


@dataclass(slots=True)
class _Candidate:
    text: str
    source_fields: list[str]


def _record(
    source_brief_id: str,
    candidate: _Candidate,
    *,
    freshness_evidence: tuple[str, ...],
) -> SourceAssumptionVolatilityRecord:
    text = candidate.text
    fields = tuple(sorted(candidate.source_fields))
    signals = _signals(text, fields)
    score = 1
    score += 3 if "explicit assumption" in signals else 0
    score += 4 if "external dependency" in signals else 0
    score += 3 if "unresolved question" in signals else 0
    score += 3 if "pending signal" in signals else 0
    score += 2 if freshness_evidence else 0
    score = min(score, 15)
    volatility = _volatility(score)
    evidence = tuple([*signals, *freshness_evidence, f"Source field: {fields[0]}"])
    return SourceAssumptionVolatilityRecord(
        source_brief_id=source_brief_id,
        assumption=text,
        volatility=volatility,
        score=score,
        evidence_type=_evidence_type(signals),
        evidence=evidence,
        source_fields=fields,
        follow_up_actions=tuple(_follow_up_actions(text, volatility, signals)),
    )


def _assumption_texts(value: Any, source_field: str) -> list[str]:
    texts = _text_segments(value)
    if _QUESTION_FIELD_RE.search(source_field):
        return texts
    if _ASSUMPTION_FIELD_RE.search(source_field):
        return texts
    if _METADATA_FIELD_RE.search(source_field):
        return [text for text in texts if _SIGNAL_RE.search(text)]
    return [text for text in texts if _SIGNAL_RE.search(text)]


def _text_segments(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        if "question" in value:
            return _text_segments(value.get("question"))
        if "text" in value:
            return _text_segments(value.get("text"))
        if "assumption" in value:
            return _text_segments(value.get("assumption"))
        return []
    if isinstance(value, (list, tuple)):
        return [segment for item in value for segment in _text_segments(item)]
    if isinstance(value, str):
        text = value.strip()
    else:
        text = _optional_text(value)
        if text is None:
            return []

    segments: list[str] = []
    for part in _SPLIT_RE.split(text):
        cleaned = _clean_text(part)
        if cleaned:
            segments.append(cleaned)
    return segments


def _brief_values(brief: Mapping[str, Any]) -> list[tuple[str, Any]]:
    values: list[tuple[str, Any]] = []
    for field in ("summary", "metadata", "source_payload"):
        if field in brief:
            values.extend(_flatten(brief[field], prefix=field))
    return values


def _flatten(value: Any, *, prefix: str) -> list[tuple[str, Any]]:
    flattened: list[tuple[str, Any]] = []

    def append(current: Any, path: str) -> None:
        if isinstance(current, Mapping):
            for key in sorted(current, key=str):
                append(current[key], f"{path}.{key}")
            return
        if isinstance(current, (list, tuple)):
            for item_index, item in enumerate(current):
                append(item, f"{path}[{item_index}]")
            return
        if _optional_text(current) is not None:
            flattened.append((path, current))

    append(value, prefix)
    return flattened


def _signals(text: str, source_fields: tuple[str, ...]) -> tuple[str, ...]:
    signals: list[str] = []
    field_text = " ".join(source_fields)
    combined = f"{text} {field_text}"
    if _EXPLICIT_ASSUMPTION_RE.search(combined) or any(
        _ASSUMPTION_FIELD_RE.search(field) for field in source_fields
    ):
        signals.append("explicit assumption")
    if _EXTERNAL_RE.search(combined):
        signals.append("external dependency")
    if _QUESTION_FIELD_RE.search(field_text) or "?" in text:
        signals.append("unresolved question")
    if _PENDING_RE.search(combined):
        signals.append("pending signal")
    return tuple(signals or ("volatility keyword",))


def _freshness_evidence(brief: Mapping[str, Any], *, today: date) -> tuple[str, ...]:
    dates = [
        parsed
        for label, value in _flatten(brief, prefix="source_brief")
        if _DATE_FIELD_RE.search(label.rsplit(".", 1)[-1])
        for parsed in [_parse_date(value)]
        if parsed is not None
    ]
    if not dates:
        return ()
    newest = max(dates)
    age_days = max(0, (today - newest).days)
    if age_days > 180:
        return (f"Freshness metadata is stale ({age_days} days old).",)
    if age_days > 90:
        return (f"Freshness metadata is aging ({age_days} days old).",)
    return ()


def _follow_up_actions(
    text: str, volatility: AssumptionVolatility, signals: tuple[str, ...]
) -> list[str]:
    if volatility != "high":
        return []
    actions = ["Confirm or replace the assumption with dated source evidence before planning."]
    combined = f"{text} {' '.join(signals)}"
    if "external dependency" in signals:
        actions.append("Get written confirmation from the external owner or vendor.")
    if "unresolved question" in signals or "pending signal" in signals:
        actions.append("Resolve the pending question and record the decision owner.")
    if re.search(r"\b(?:pricing|legal|deadline|staffing)\b", combined, re.IGNORECASE):
        actions.append("Escalate commercial, legal, schedule, or staffing risk to the owner.")
    return actions


def _evidence_type(signals: tuple[str, ...]) -> str:
    if "unresolved question" in signals:
        return "unresolved_question"
    if "external dependency" in signals:
        return "external_dependency"
    if "pending signal" in signals:
        return "pending_signal"
    if "explicit assumption" in signals:
        return "explicit_assumption"
    return "keyword_signal"


def _volatility(score: int) -> AssumptionVolatility:
    if score >= 10:
        return "high"
    if score >= 5:
        return "medium"
    return "low"


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
    for match in _DATE_RE.finditer(value.strip()):
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


def _record_sort_key(record: SourceAssumptionVolatilityRecord) -> tuple[Any, ...]:
    return (
        _PRIORITY[record.volatility],
        -record.score,
        record.source_brief_id,
        record.source_fields,
        _dedupe_key(record.assumption),
    )


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return " ".join(text.split())


def _dedupe_key(value: str) -> str:
    return _clean_text(value).casefold()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "AssumptionVolatility",
    "SourceAssumptionVolatilityRecord",
    "SourceAssumptionVolatilityReport",
    "build_source_assumption_volatility_report",
    "source_assumption_volatility_report_to_dict",
    "source_assumption_volatility_report_to_markdown",
]
