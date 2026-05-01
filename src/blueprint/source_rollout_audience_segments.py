"""Extract rollout audience segments from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


RolloutAudienceSegmentType = Literal[
    "persona",
    "customer_tier",
    "region",
    "role",
    "platform",
    "plan",
    "cohort",
    "beta_group",
    "internal_user",
]
RolloutAudienceInclusionStatus = Literal["included", "excluded"]
RolloutAudienceConfidence = Literal["high", "medium", "low"]

_TYPE_ORDER: tuple[RolloutAudienceSegmentType, ...] = (
    "persona",
    "customer_tier",
    "region",
    "role",
    "platform",
    "plan",
    "beta_group",
    "cohort",
    "internal_user",
)
_STATUS_ORDER: dict[RolloutAudienceInclusionStatus, int] = {"included": 0, "excluded": 1}
_CONFIDENCE_ORDER: dict[RolloutAudienceConfidence, int] = {"high": 0, "medium": 1, "low": 2}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)]|\[[ xX]\])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_LIST_SPLIT_RE = re.compile(r"\s*(?:,|;|/|\band\b|\bor\b)\s+", re.I)
_AUDIENCE_FIELD_RE = re.compile(
    r"(?:audience|segment|persona|target|customer|tier|region|market|geo|role|"
    r"platform|plan|cohort|beta|early_access|internal|employee|staff|exclude)",
    re.I,
)
_EXCLUSION_FIELD_RE = re.compile(r"(?:exclude|excluded|exclusion|not_for|out_of_scope)", re.I)
_EXCLUSION_RE = re.compile(
    r"\b(?:not\s+for|exclude|excluding|excluded|except|no\s+access\s+for|do\s+not\s+roll\s+out\s+to|"
    r"do\s+not\s+launch\s+to|without)\s+(?P<label>[A-Za-z0-9][A-Za-z0-9 &/_.+-]{1,80})",
    re.I,
)
_INCLUSION_RE = re.compile(
    r"\b(?:for|to|target(?:ing)?|roll(?:\s|-)?out\s+to|launch\s+to|available\s+to|"
    r"enable\s+for|include|including|pilot\s+with|beta\s+for)\s+"
    r"(?P<label>[A-Za-z0-9][A-Za-z0-9 &/_.+-]{1,100})",
    re.I,
)
_LABEL_STOP_RE = re.compile(
    r"\b(?:first|initially|only|before|after|during|when|with|without|while|unless|on|"
    r"using|on\s+the|via|who|that|where|because|from|until|and\s+then)\b",
    re.I,
)

_STRUCTURED_TYPE_HINTS: tuple[tuple[RolloutAudienceSegmentType, re.Pattern[str]], ...] = (
    ("beta_group", re.compile(r"(?:beta|early_access|early access|preview)", re.I)),
    ("internal_user", re.compile(r"(?:internal|employee|staff|dogfood|workspace_user)", re.I)),
    (
        "customer_tier",
        re.compile(r"(?:tier|customer_segment|customer segment|account_segment)", re.I),
    ),
    ("region", re.compile(r"(?:region|market|geo|country|locale|territor)", re.I)),
    ("role", re.compile(r"(?:role|rbac|permission)", re.I)),
    ("platform", re.compile(r"(?:platform|device|client|browser|os)", re.I)),
    ("plan", re.compile(r"(?:plan|package|subscription|sku|edition)", re.I)),
    ("cohort", re.compile(r"(?:cohort|pilot|canary|wave|phase)", re.I)),
    (
        "persona",
        re.compile(r"(?:persona|audience|target_user|target user|user_segment|user segment)", re.I),
    ),
)

_TEXT_PATTERNS: tuple[tuple[RolloutAudienceSegmentType, re.Pattern[str]], ...] = (
    (
        "region",
        re.compile(
            r"\b(?:EU|European Union|UK|United Kingdom|US|U\.S\.|United States|Canada|APAC|EMEA|LATAM|Japan|Australia)\b",
            re.I,
        ),
    ),
    (
        "platform",
        re.compile(
            r"\b(?:iOS|Android|web|mobile|desktop|tablet|Chrome|Safari|Firefox|Edge)\b", re.I
        ),
    ),
    (
        "customer_tier",
        re.compile(
            r"\b(?:enterprise|mid[- ]market|smb|small business|strategic|paid|free[- ]tier|self[- ]serve)(?:\s+(?:customers?|accounts?|tenants?|tier))?\b",
            re.I,
        ),
    ),
    (
        "plan",
        re.compile(
            r"\b(?:free|starter|basic|pro|premium|business|enterprise)\s+(?:plan|package|subscription|edition|sku)\b",
            re.I,
        ),
    ),
    (
        "role",
        re.compile(
            r"\b(?:admins?|administrators?|owners?|workspace owners?|tenant owners?|operators?|developers?|agents?)\b",
            re.I,
        ),
    ),
    (
        "beta_group",
        re.compile(
            r"\b(?:beta users?|beta group|early access(?: group| users?)?|preview users?)\b", re.I
        ),
    ),
    (
        "internal_user",
        re.compile(
            r"\b(?:internal users?|employees?|staff|support agents?|customer success|dogfood users?)\b",
            re.I,
        ),
    ),
    (
        "cohort",
        re.compile(
            r"\b(?:pilot cohort|canary cohort|cohort [A-Za-z0-9_-]+|wave \d+|phase \d+)\b", re.I
        ),
    ),
)


@dataclass(frozen=True, slots=True)
class SourceRolloutAudienceSegment:
    """One rollout audience segment inferred from a source brief."""

    segment_label: str
    segment_type: RolloutAudienceSegmentType
    inclusion_status: RolloutAudienceInclusionStatus
    source_field: str
    confidence: RolloutAudienceConfidence
    evidence: str
    rollout_implication: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "segment_label": self.segment_label,
            "segment_type": self.segment_type,
            "inclusion_status": self.inclusion_status,
            "source_field": self.source_field,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "rollout_implication": self.rollout_implication,
        }


def extract_source_rollout_audience_segments(
    source_brief: Mapping[str, Any] | SourceBrief,
) -> tuple[SourceRolloutAudienceSegment, ...]:
    """Return rollout audience segments from one SourceBrief-shaped record."""
    brief = _source_brief_payload(source_brief)
    if not brief:
        return ()

    candidates: list[SourceRolloutAudienceSegment] = []
    for source_field, value in _candidate_values(brief):
        if _structured_audience_field(source_field):
            candidates.extend(_structured_records(source_field, value))
            continue
        for text in _text_segments(value):
            candidates.extend(_text_records(source_field, text))

    return tuple(sorted(_dedupe_records(candidates), key=_record_sort_key))


def source_rollout_audience_segments_to_dicts(
    records: tuple[SourceRolloutAudienceSegment, ...] | list[SourceRolloutAudienceSegment],
) -> list[dict[str, Any]]:
    """Serialize rollout audience segment records to dictionaries."""
    return [record.to_dict() for record in records]


def summarize_source_rollout_audience_segments(
    records_or_source: (
        Mapping[str, Any]
        | SourceBrief
        | tuple[SourceRolloutAudienceSegment, ...]
        | list[SourceRolloutAudienceSegment]
    ),
) -> dict[str, Any]:
    """Return deterministic counts and rollout implications for extracted audience segments."""
    if _looks_like_records(records_or_source):
        records = tuple(records_or_source)  # type: ignore[arg-type]
    else:
        records = extract_source_rollout_audience_segments(records_or_source)  # type: ignore[arg-type]

    return {
        "segment_count": len(records),
        "included_count": sum(1 for record in records if record.inclusion_status == "included"),
        "excluded_count": sum(1 for record in records if record.inclusion_status == "excluded"),
        "type_counts": {
            segment_type: sum(1 for record in records if record.segment_type == segment_type)
            for segment_type in _TYPE_ORDER
            if any(record.segment_type == segment_type for record in records)
        },
        "confidence_counts": {
            confidence: sum(1 for record in records if record.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "rollout_implications": list(
            _dedupe_text(record.rollout_implication for record in records)
        ),
    }


def _candidate_values(brief: Mapping[str, Any]) -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = []
    for field_name in ("title", "summary", "domain"):
        if field_name in brief:
            candidates.append((field_name, brief[field_name]))
    payload = brief.get("source_payload")
    if isinstance(payload, Mapping):
        visited: set[str] = set()
        for key in sorted(payload):
            source_field = f"source_payload.{key}"
            if _structured_audience_field(source_field):
                candidates.append((source_field, payload[key]))
                visited.add(source_field)
        for source_field, value in _flatten_payload(payload, prefix="source_payload"):
            if _is_under_visited_field(source_field, visited):
                continue
            candidates.append((source_field, value))
    return candidates


def _structured_records(source_field: str, value: Any) -> list[SourceRolloutAudienceSegment]:
    records: list[SourceRolloutAudienceSegment] = []
    if isinstance(value, Mapping):
        if _looks_like_segment_mapping(value):
            label = _segment_label_from_mapping(value)
            if label:
                segment_type = _segment_type(source_field, label, value.get("type"))
                status = _status(source_field, value)
                records.append(_record(label, segment_type, status, source_field, "high", label))
            return records
        for child_field, child_value in _flatten_payload(value, prefix=source_field):
            if _structured_audience_field(child_field):
                records.extend(_structured_records(child_field, child_value))
        return records
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            records.extend(_structured_records(f"{source_field}[{index}]", item))
        return records
    text = _source_text(value)
    if text is None:
        return records
    for label in _split_labels(text):
        segment_type = _segment_type(source_field, label)
        records.append(
            _record(
                label,
                segment_type,
                "excluded" if _EXCLUSION_FIELD_RE.search(source_field) else "included",
                source_field,
                "high",
                label,
            )
        )
    return records


def _text_records(source_field: str, text: str) -> list[SourceRolloutAudienceSegment]:
    records: list[SourceRolloutAudienceSegment] = []
    for match in _EXCLUSION_RE.finditer(text):
        for label in _split_labels(_trim_label(match.group("label"))):
            segment_type = _segment_type(source_field, label)
            records.append(_record(label, segment_type, "excluded", source_field, "medium", text))

    for segment_type, pattern in _TEXT_PATTERNS:
        for match in pattern.finditer(text):
            label = _clean_label(match.group(0))
            if label:
                status: RolloutAudienceInclusionStatus = (
                    "excluded" if _has_near_exclusion(text, match.start()) else "included"
                )
                records.append(_record(label, segment_type, status, source_field, "medium", text))

    for match in _INCLUSION_RE.finditer(text):
        if _has_near_exclusion(text, match.start()):
            continue
        label_text = _trim_label(match.group("label"))
        for label in _split_labels(label_text):
            segment_type = _segment_type(source_field, label)
            if segment_type == "persona" and len(label.split()) > 5:
                continue
            records.append(_record(label, segment_type, "included", source_field, "low", text))
    return records


def _record(
    label: str,
    segment_type: RolloutAudienceSegmentType,
    status: RolloutAudienceInclusionStatus,
    source_field: str,
    confidence: RolloutAudienceConfidence,
    evidence: str,
) -> SourceRolloutAudienceSegment:
    label = _clean_label(label)
    if confidence == "low" and segment_type != "persona":
        confidence = "medium"
    return SourceRolloutAudienceSegment(
        segment_label=label,
        segment_type=segment_type,
        inclusion_status=status,
        source_field=source_field,
        confidence=confidence,
        evidence=_snippet(evidence),
        rollout_implication=_rollout_implication(segment_type, label, status),
    )


def _rollout_implication(
    segment_type: RolloutAudienceSegmentType,
    label: str,
    status: RolloutAudienceInclusionStatus,
) -> str:
    if status == "excluded":
        return f"Keep {label} out of rollout targeting, eligibility checks, and validation cohorts."
    guidance = {
        "persona": "Validate launch behavior with representative users from this persona.",
        "customer_tier": "Stage rollout and success metrics by this customer tier.",
        "region": "Gate availability, compliance checks, localization, and monitoring by region.",
        "role": "Confirm permissions, navigation, and support guidance for this role.",
        "platform": "Include platform-specific QA, telemetry, and release gating.",
        "plan": "Tie feature eligibility and billing or entitlement checks to this plan.",
        "cohort": "Use this cohort as an explicit launch wave or validation sample.",
        "beta_group": "Treat this group as an early-access or beta validation audience.",
        "internal_user": "Use this audience for internal dogfood, support readiness, or staff validation.",
    }[segment_type]
    return f"{guidance} Segment: {label}."


def _segment_type(
    source_field: str,
    label: str,
    explicit_type: Any = None,
) -> RolloutAudienceSegmentType:
    type_hint = _source_text(explicit_type)
    if type_hint:
        for segment_type, pattern in _STRUCTURED_TYPE_HINTS:
            if pattern.search(type_hint):
                return segment_type
    for segment_type, pattern in _STRUCTURED_TYPE_HINTS:
        if segment_type != "persona" and pattern.search(source_field):
            return segment_type
    if re.search(
        r"(?:persona|target_user|target user|user_segment|user segment)", source_field, re.I
    ):
        return "persona"
    for segment_type, pattern in _TEXT_PATTERNS:
        if pattern.fullmatch(label) or pattern.search(label):
            return segment_type
    if re.search(r"(?:audience|segment|target)", source_field, re.I):
        return "persona"
    return "persona"


def _status(source_field: str, value: Mapping[str, Any]) -> RolloutAudienceInclusionStatus:
    status = _source_text(
        value.get("status") or value.get("inclusion_status") or value.get("included")
    )
    if status and re.search(r"\b(?:exclude|excluded|false|no|out)\b", status, re.I):
        return "excluded"
    if _EXCLUSION_FIELD_RE.search(source_field):
        return "excluded"
    return "included"


def _looks_like_segment_mapping(value: Mapping[str, Any]) -> bool:
    return any(key in value for key in ("label", "name", "segment", "audience", "persona", "value"))


def _segment_label_from_mapping(value: Mapping[str, Any]) -> str | None:
    for key in ("label", "name", "segment", "audience", "persona", "value"):
        text = _source_text(value.get(key))
        if text:
            return text
    return None


def _dedupe_records(
    records: list[SourceRolloutAudienceSegment],
) -> tuple[SourceRolloutAudienceSegment, ...]:
    best_by_key: dict[tuple[str, str, str], SourceRolloutAudienceSegment] = {}
    for record in records:
        if not record.segment_label:
            continue
        key = (
            record.segment_type,
            record.inclusion_status,
            _label_key(record.segment_label, record.segment_type),
        )
        current = best_by_key.get(key)
        if current is None or _record_precedence(record) < _record_precedence(current):
            best_by_key[key] = record
    return tuple(best_by_key.values())


def _record_precedence(record: SourceRolloutAudienceSegment) -> tuple[int, int, str]:
    structured = 0 if _structured_audience_field(record.source_field) else 1
    return (_CONFIDENCE_ORDER[record.confidence], structured, record.source_field)


def _record_sort_key(record: SourceRolloutAudienceSegment) -> tuple[Any, ...]:
    return (
        _STATUS_ORDER[record.inclusion_status],
        _TYPE_ORDER.index(record.segment_type),
        _CONFIDENCE_ORDER[record.confidence],
        _label_key(record.segment_label, record.segment_type),
        record.source_field,
    )


def _structured_audience_field(source_field: str) -> bool:
    return bool(_AUDIENCE_FIELD_RE.search(source_field))


def _is_under_visited_field(source_field: str, visited_fields: set[str]) -> bool:
    return any(
        source_field == visited
        or source_field.startswith(f"{visited}.")
        or source_field.startswith(f"{visited}[")
        for visited in visited_fields
    )


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


def _text_segments(value: Any) -> tuple[str, ...]:
    text = _source_text(value)
    if text is None:
        return ()
    segments: list[str] = []
    for line in text.splitlines():
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        if _BULLET_RE.match(line):
            segments.append(cleaned)
            continue
        segments.extend(
            part for part in (_clean_text(part) for part in _SENTENCE_SPLIT_RE.split(line)) if part
        )
    return tuple(segments)


def _split_labels(value: str) -> tuple[str, ...]:
    labels = []
    for part in _LIST_SPLIT_RE.split(value):
        label = _clean_label(_trim_label(part))
        if label and not _too_generic(label):
            labels.append(label)
    return tuple(_dedupe_text(labels))


def _trim_label(value: str) -> str:
    text = _LABEL_STOP_RE.split(value, maxsplit=1)[0]
    text = re.split(r"[:.!?()\[\]]", text, maxsplit=1)[0]
    return text


def _has_near_exclusion(text: str, start: int) -> bool:
    window = text[max(0, start - 24) : start].casefold()
    return bool(
        re.search(r"(?:not for|exclude|excluding|except|without|no access for)\s*$", window)
    )


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
        text = _clean_text(value)
        return text or None
    if isinstance(value, (int, float, bool)):
        return str(value)
    return None


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _clean_label(value: str) -> str:
    text = _clean_text(value)
    text = re.sub(r"^(?:the|all|only|new|existing)\s+", "", text, flags=re.I)
    text = re.sub(r"\s+(?:only|first|initially)$", "", text, flags=re.I)
    return text.strip(" ,;:-")


def _label_key(label: str, segment_type: RolloutAudienceSegmentType) -> str:
    key = re.sub(r"[^a-z0-9]+", " ", _clean_label(label).casefold()).strip()
    key = re.sub(r"\b(?:customers?|users?|accounts?|tenants?|audiences?|segments?)\b", "", key)
    if segment_type in {"plan", "customer_tier"}:
        key = re.sub(r"\b(?:plan|tier|package|subscription|edition|sku)\b", "", key)
    return _SPACE_RE.sub(" ", key).strip()


def _too_generic(label: str) -> bool:
    return _label_key(label, "persona") in {
        "",
        "user",
        "users",
        "customer",
        "customers",
        "audience",
    }


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


def _looks_like_records(value: Any) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    return all(isinstance(item, SourceRolloutAudienceSegment) for item in value)


__all__ = [
    "RolloutAudienceConfidence",
    "RolloutAudienceInclusionStatus",
    "RolloutAudienceSegmentType",
    "SourceRolloutAudienceSegment",
    "extract_source_rollout_audience_segments",
    "source_rollout_audience_segments_to_dicts",
    "summarize_source_rollout_audience_segments",
]
