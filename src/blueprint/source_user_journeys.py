"""Extract user journey and workflow signals from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


SourceUserJourneyConfidence = Literal["high", "medium", "low"]

_CONFIDENCE_ORDER: dict[SourceUserJourneyConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_FIELD_ORDER = {
    "title": 0,
    "summary": 1,
    "source_payload.body": 2,
    "source_payload.goals": 3,
    "source_payload.constraints": 4,
    "source_payload.metadata": 5,
}
_TOP_LEVEL_FIELDS = (
    "title",
    "summary",
    "body",
    "goals",
    "constraints",
    "metadata",
)
_SOURCE_PAYLOAD_FIELDS = (
    "body",
    "description",
    "goals",
    "constraints",
    "metadata",
    "journeys",
    "user_journeys",
    "workflows",
    "flows",
    "flow",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)]|\[[ xX]\])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_WORKFLOW_FIELD_RE = re.compile(r"(?:journey|workflow|flow|handoff|step)", re.I)
_CAN_RE = re.compile(
    r"\b(?P<actor>users?|admins?|administrators?|customers?|clients?|buyers?|shoppers?|"
    r"support agents?|agents?|operators?|managers?|reviewers?|approvers?)\s+can\s+"
    r"(?P<outcome>[A-Za-z0-9][A-Za-z0-9 ,&/_.+'-]{2,160})",
    re.I,
)
_FLOW_RE = re.compile(
    r"\b(?P<label>customer journey|onboarding flow|checkout flow|support workflow|"
    r"approval flow|handoff steps?|handoff flow)\b",
    re.I,
)
_HANDOFF_RE = re.compile(
    r"\bhandoff(?:\s+steps?|\s+flow)?\s+"
    r"(?:from\s+(?P<from>[A-Za-z][A-Za-z0-9 &/'-]{1,60})\s+)?"
    r"(?:to|into)\s+(?P<to>[A-Za-z][A-Za-z0-9 &/'-]{1,60})",
    re.I,
)
_TRIGGER_RE = re.compile(
    r"\b(?:when|after|before|once|upon|if|as soon as)\s+"
    r"(?P<trigger>[A-Za-z0-9][A-Za-z0-9 ,&/_.+'-]{2,140})",
    re.I,
)
_OUTCOME_RE = re.compile(
    r"\b(?:so that|until|ending with|ends with|resulting in)\s+"
    r"(?P<outcome>[A-Za-z0-9][A-Za-z0-9 ,&/_.+'-]{2,160})",
    re.I,
)
_TRIM_RE = re.compile(
    r"\b(?:when|after|before|once|upon|if|as soon as|so that|to|until|and then|then|"
    r"while|with|without|because)\b",
    re.I,
)
_ACTOR_FROM_LABEL = {
    "customer journey": "customer",
    "onboarding flow": "new user",
    "checkout flow": "customer",
    "support workflow": "support agent",
    "approval flow": "approver",
    "handoff steps": "team",
    "handoff step": "team",
    "handoff flow": "team",
}


@dataclass(frozen=True, slots=True)
class SourceUserJourney:
    """One source-backed user journey or workflow signal."""

    journey_name: str
    actor: str
    trigger: str
    expected_outcome: str
    source_field: str
    confidence: SourceUserJourneyConfidence
    evidence: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "journey_name": self.journey_name,
            "actor": self.actor,
            "trigger": self.trigger,
            "expected_outcome": self.expected_outcome,
            "source_field": self.source_field,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


def extract_source_user_journeys(
    source_brief: Mapping[str, Any] | SourceBrief,
) -> tuple[SourceUserJourney, ...]:
    """Return user journey and workflow signals from one SourceBrief-shaped record."""
    brief = _source_brief_payload(source_brief)
    if not brief:
        return ()

    records: list[SourceUserJourney] = []
    for source_field, value in _candidate_values(brief):
        if _workflow_field(source_field):
            records.extend(_structured_value_records(source_field, value))
            continue
        for segment in _segments(value):
            records.extend(_text_records(source_field, segment))

    return tuple(sorted(_dedupe_records(records), key=_record_sort_key))


def source_user_journeys_to_dicts(
    records: tuple[SourceUserJourney, ...] | list[SourceUserJourney],
) -> list[dict[str, Any]]:
    """Serialize user journey records to dictionaries."""
    return [record.to_dict() for record in records]


def summarize_source_user_journeys(
    records_or_source: (
        Mapping[str, Any] | SourceBrief | tuple[SourceUserJourney, ...] | list[SourceUserJourney]
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted user journey signals."""
    if _looks_like_records(records_or_source):
        records = tuple(records_or_source)  # type: ignore[arg-type]
    else:
        records = extract_source_user_journeys(records_or_source)  # type: ignore[arg-type]
    return {
        "journey_count": len(records),
        "confidence_counts": {
            confidence: sum(1 for record in records if record.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "actors": list(_dedupe_text(record.actor for record in records)),
        "journey_names": [record.journey_name for record in records],
    }


def _candidate_values(brief: Mapping[str, Any]) -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = []
    for field_name in _TOP_LEVEL_FIELDS:
        if field_name in brief:
            _append_value(candidates, field_name, brief[field_name])

    payload = brief.get("source_payload")
    if isinstance(payload, Mapping):
        visited: set[str] = set()
        for field_name in _SOURCE_PAYLOAD_FIELDS:
            if field_name in payload:
                source_field = f"source_payload.{field_name}"
                if _workflow_field(source_field):
                    candidates.append((source_field, payload[field_name]))
                else:
                    _append_value(candidates, source_field, payload[field_name])
                visited.add(source_field)
        for source_field, value in _flatten_payload(payload, prefix="source_payload"):
            if _is_under_visited_field(source_field, visited):
                continue
            _append_value(candidates, source_field, value)
    return candidates


def _append_value(candidates: list[tuple[str, Any]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for child_field, child_value in _flatten_payload(value, prefix=source_field):
            _append_value(candidates, child_field, child_value)
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _append_value(candidates, f"{source_field}[{index}]", item)
        return
    candidates.append((source_field, value))


def _structured_value_records(source_field: str, value: Any) -> list[SourceUserJourney]:
    if isinstance(value, Mapping):
        return _structured_records(source_field, value)
    if isinstance(value, (list, tuple)):
        records: list[SourceUserJourney] = []
        for index, item in enumerate(value):
            records.extend(_structured_value_records(f"{source_field}[{index}]", item))
        return records
    records: list[SourceUserJourney] = []
    for segment in _segments(value):
        records.extend(_text_records(source_field, segment))
    return records


def _structured_records(source_field: str, value: Mapping[str, Any]) -> list[SourceUserJourney]:
    if _looks_like_journey_mapping(value):
        record = _record(
            journey_name=_first_text(
                value,
                ("journey_name", "name", "title", "workflow", "flow", "label"),
            )
            or _journey_name_from_field(source_field),
            actor=_first_text(value, ("actor", "persona", "user", "role", "owner")) or "user",
            trigger=_first_text(value, ("trigger", "entry_point", "start", "starts_when"))
            or "source brief workflow signal",
            expected_outcome=_first_text(
                value,
                ("expected_outcome", "outcome", "goal", "result", "end_state"),
            )
            or "complete the workflow",
            source_field=source_field,
            confidence="high",
            evidence=_snippet(
                _first_text(value, ("evidence", "description", "summary", "body"))
                or _first_text(value, ("journey_name", "name", "title", "workflow", "flow", "label"))
                or source_field
            ),
        )
        return [record]

    records: list[SourceUserJourney] = []
    for child_field, child_value in _flatten_payload(value, prefix=source_field):
        for segment in _segments(child_value):
            records.extend(_text_records(child_field, segment))
    return records


def _text_records(source_field: str, text: str) -> list[SourceUserJourney]:
    records: list[SourceUserJourney] = []
    cleaned = _clean_text(text)
    if not cleaned:
        return records

    for match in _CAN_RE.finditer(cleaned):
        actor = _clean_actor(match.group("actor"))
        outcome = _clean_phrase(match.group("outcome"))
        if not outcome:
            continue
        records.append(
            _record(
                journey_name=f"{_title_actor(actor)} can {_short_name(outcome)}",
                actor=actor,
                trigger=_trigger(cleaned),
                expected_outcome=outcome,
                source_field=source_field,
                confidence=_confidence(source_field, cleaned, explicit=True),
                evidence=_snippet(cleaned),
            )
        )

    for match in _FLOW_RE.finditer(cleaned):
        label = _clean_label(match.group("label"))
        actor = _ACTOR_FROM_LABEL.get(label.casefold(), "user")
        records.append(
            _record(
                journey_name=_title_phrase(label),
                actor=actor,
                trigger=_trigger(cleaned),
                expected_outcome=_outcome(cleaned) or _default_outcome(label),
                source_field=source_field,
                confidence=_confidence(source_field, cleaned, explicit=True),
                evidence=_snippet(cleaned),
            )
        )

    for match in _HANDOFF_RE.finditer(cleaned):
        actor = _clean_actor(match.group("from") or "team")
        target = _clean_phrase(match.group("to"))
        if target:
            records.append(
                _record(
                    journey_name=f"Handoff to {_title_phrase(target)}",
                    actor=actor,
                    trigger=_trigger(cleaned),
                    expected_outcome=f"handoff to {target}",
                    source_field=source_field,
                    confidence=_confidence(source_field, cleaned, explicit=True),
                    evidence=_snippet(cleaned),
                )
            )

    return records


def _record(
    *,
    journey_name: str,
    actor: str,
    trigger: str,
    expected_outcome: str,
    source_field: str,
    confidence: SourceUserJourneyConfidence,
    evidence: str,
) -> SourceUserJourney:
    return SourceUserJourney(
        journey_name=_clean_label(journey_name),
        actor=_clean_actor(actor),
        trigger=_clean_phrase(trigger) or "source brief workflow signal",
        expected_outcome=_clean_phrase(expected_outcome) or "complete the workflow",
        source_field=source_field,
        confidence=confidence,
        evidence=(_snippet(evidence),),
    )


def _dedupe_records(records: list[SourceUserJourney]) -> tuple[SourceUserJourney, ...]:
    best_by_key: dict[tuple[str, str, str], SourceUserJourney] = {}
    for record in records:
        if not record.journey_name or not record.expected_outcome:
            continue
        key = (
            _dedupe_key(record.journey_name),
            _dedupe_key(record.actor),
            _dedupe_key(record.expected_outcome),
        )
        current = best_by_key.get(key)
        if current is None or _record_precedence(record) < _record_precedence(current):
            best_by_key[key] = record
    return tuple(best_by_key.values())


def _record_precedence(record: SourceUserJourney) -> tuple[int, int, int, str]:
    structured = 0 if _workflow_field(record.source_field) else 1
    return (
        _CONFIDENCE_ORDER[record.confidence],
        structured,
        _field_rank(record.source_field),
        record.source_field,
    )


def _record_sort_key(record: SourceUserJourney) -> tuple[Any, ...]:
    return (
        _CONFIDENCE_ORDER[record.confidence],
        _field_rank(record.source_field),
        _dedupe_key(record.journey_name),
        _dedupe_key(record.actor),
        record.source_field,
    )


def _field_rank(source_field: str) -> int:
    if source_field in _FIELD_ORDER:
        return _FIELD_ORDER[source_field]
    for field, rank in _FIELD_ORDER.items():
        if source_field.startswith(f"{field}.") or source_field.startswith(f"{field}["):
            return rank
    return 100


def _confidence(
    source_field: str,
    text: str,
    *,
    explicit: bool,
) -> SourceUserJourneyConfidence:
    if _workflow_field(source_field):
        return "high"
    if explicit and _FLOW_RE.search(text):
        return "high"
    if explicit:
        return "medium"
    return "low"


def _trigger(text: str) -> str:
    match = _TRIGGER_RE.search(text)
    if match is None:
        return "source brief workflow signal"
    return _clean_phrase(match.group("trigger"))


def _outcome(text: str) -> str | None:
    match = _OUTCOME_RE.search(text)
    if match is None:
        return None
    return _clean_phrase(match.group("outcome"))


def _default_outcome(label: str) -> str:
    lowered = label.casefold()
    return {
        "customer journey": "complete the customer journey",
        "onboarding flow": "complete onboarding",
        "checkout flow": "complete checkout",
        "support workflow": "resolve the support request",
        "approval flow": "complete approval",
        "handoff step": "complete the handoff",
        "handoff steps": "complete the handoff",
        "handoff flow": "complete the handoff",
    }.get(lowered, "complete the workflow")


def _segments(value: Any) -> tuple[str, ...]:
    text = _source_text(value)
    if text is None:
        return ()
    segments: list[str] = []
    for line in text.splitlines():
        line_text = _clean_text(line)
        if not line_text:
            continue
        if _BULLET_RE.match(line):
            segments.append(line_text)
            continue
        for part in _SENTENCE_SPLIT_RE.split(line):
            cleaned = _clean_text(part)
            if cleaned:
                segments.append(cleaned)
    return tuple(segments)


def _flatten_payload(value: Any, *, prefix: str) -> list[tuple[str, Any]]:
    flattened: list[tuple[str, Any]] = []

    def append(current: Any, path: str) -> None:
        if isinstance(current, Mapping):
            for key in sorted(current):
                append(current[key], f"{path}.{key}")
            return
        if isinstance(current, (list, tuple)):
            for index, item in enumerate(current):
                append(item, f"{path}[{index}]")
            return
        flattened.append((path, current))

    append(value, prefix)
    return flattened


def _source_brief_payload(source_brief: Mapping[str, Any] | SourceBrief) -> dict[str, Any]:
    if hasattr(source_brief, "model_dump"):
        return source_brief.model_dump(mode="python")
    try:
        return SourceBrief.model_validate(source_brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        if isinstance(source_brief, Mapping):
            return dict(source_brief)
    return {}


def _source_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, (int, float, bool)):
        return str(value)
    return None


def _first_text(value: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        text = _source_text(value.get(key))
        if text:
            return _clean_text(text)
    return None


def _looks_like_journey_mapping(value: Mapping[str, Any]) -> bool:
    return any(
        key in value
        for key in (
            "journey_name",
            "name",
            "workflow",
            "flow",
            "actor",
            "persona",
            "trigger",
            "expected_outcome",
            "outcome",
        )
    )


def _workflow_field(source_field: str) -> bool:
    return bool(_WORKFLOW_FIELD_RE.search(source_field))


def _journey_name_from_field(source_field: str) -> str:
    name = re.sub(r"^source_payload\.", "", source_field)
    name = re.sub(r"\[\d+\]", "", name)
    name = name.rsplit(".", maxsplit=1)[-1]
    return _title_phrase(name.replace("_", " "))


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _clean_phrase(value: str | None) -> str:
    if not value:
        return ""
    text = _clean_text(value)
    text = _TRIM_RE.split(text, maxsplit=1)[0]
    text = re.split(r"[:.!?()\[\]]", text, maxsplit=1)[0]
    return text.strip(" ,;:-")


def _clean_label(value: str) -> str:
    return _clean_text(value).strip(" ,;:-")


def _clean_actor(value: str) -> str:
    text = _clean_phrase(value).casefold()
    text = re.sub(r"\b(?:the|all|a|an)\s+", "", text)
    text = {
        "users": "user",
        "admins": "admin",
        "administrators": "administrator",
        "customers": "customer",
        "clients": "client",
        "buyers": "buyer",
        "shoppers": "shopper",
        "support agents": "support agent",
        "agents": "agent",
        "operators": "operator",
        "managers": "manager",
        "reviewers": "reviewer",
        "approvers": "approver",
    }.get(text, text)
    return text or "user"


def _title_actor(value: str) -> str:
    return _title_phrase(value)


def _title_phrase(value: str) -> str:
    return " ".join(part.capitalize() for part in _clean_phrase(value).split())


def _short_name(value: str) -> str:
    words = _clean_phrase(value).split()
    return " ".join(words[:6])


def _dedupe_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _clean_text(value).casefold()).strip()


def _snippet(text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) <= 180:
        return cleaned
    return f"{cleaned[:177].rstrip()}..."


def _dedupe_text(values: Any) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(str(value))
        key = text.casefold()
        if text and key not in seen:
            seen.add(key)
            result.append(text)
    return tuple(result)


def _is_under_visited_field(source_field: str, visited_fields: set[str]) -> bool:
    return any(
        source_field == visited
        or source_field.startswith(f"{visited}.")
        or source_field.startswith(f"{visited}[")
        for visited in visited_fields
    )


def _looks_like_records(value: Any) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    return all(isinstance(item, SourceUserJourney) for item in value)


__all__ = [
    "SourceUserJourney",
    "SourceUserJourneyConfidence",
    "extract_source_user_journeys",
    "source_user_journeys_to_dicts",
    "summarize_source_user_journeys",
]
