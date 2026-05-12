"""Extract source-level webhook ordering requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping


WebhookOrderingRequirementType = Literal[
    "ordering_scope",
    "sequence_metadata",
    "duplicate_out_of_order_handling",
    "replay_backfill_behavior",
    "consumer_compatibility",
    "monitoring_evidence",
]

_TYPE_ORDER: tuple[WebhookOrderingRequirementType, ...] = (
    "ordering_scope",
    "sequence_metadata",
    "duplicate_out_of_order_handling",
    "replay_backfill_behavior",
    "consumer_compatibility",
    "monitoring_evidence",
)
_SPACE_RE = re.compile(r"\s+")
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_WEBHOOK_RE = re.compile(r"\bwebhooks?\b|\bwebhook deliveries?\b|\bwebhook events?\b", re.I)
_ORDERING_RE = re.compile(
    r"\b(?:ordering|ordered|sequence|sequencing|out[- ]of[- ]order|in order|causal order|fifo|"
    r"per[- ](?:entity|tenant|account|aggregate|customer)|aggregate order|version order)\b",
    re.I,
)
_NO_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:webhook ordering|webhook sequencing|ordered webhook|out[- ]of[- ]order)\b"
    r".{0,100}\b(?:scope|required|needed|changes?|impact)\b",
    re.I,
)
_RETRY_SIGNING_ONLY_RE = re.compile(
    r"\b(?:retry policy|webhook retries|backoff|max attempts|signing secret|signature verification|hmac|rotate secret)\b",
    re.I,
)
_FIELD_CONTEXT_RE = re.compile(r"(?:webhook|ordering|sequence|delivery|replay|consumer|requirements?|acceptance)", re.I)
_TYPE_PATTERNS: dict[WebhookOrderingRequirementType, re.Pattern[str]] = {
    "ordering_scope": re.compile(
        r"\b(?:ordering scope|per[- ](?:entity|tenant|account|aggregate|customer)|aggregate|entity|tenant|account|"
        r"causal order|fifo|within each|same customer|same subscription)\b",
        re.I,
    ),
    "sequence_metadata": re.compile(
        r"\b(?:sequence(?: number| id| key)?|version|revision|monotonic|watermark|event id|"
        r"aggregate id|ordering key|partition key|created_at|occurred_at)\b",
        re.I,
    ),
    "duplicate_out_of_order_handling": re.compile(
        r"\b(?:duplicate|dedupe|idempotent|out[- ]of[- ]order|late event|stale event|gap|buffer|discard|ignore older)\b",
        re.I,
    ),
    "replay_backfill_behavior": re.compile(
        r"\b(?:replay|backfill|redeliver|historical|resend|catch[- ]up|rebuild|reprocess)\b",
        re.I,
    ),
    "consumer_compatibility": re.compile(
        r"\b(?:consumer compatibility|backward compatible|existing consumers?|subscriber|receiver|endpoint|"
        r"contract|schema version|non[- ]breaking|migration)\b",
        re.I,
    ),
    "monitoring_evidence": re.compile(
        r"\b(?:monitor|monitoring|observability|metric|dashboard|alert|audit|evidence|log|trace|"
        r"out[- ]of[- ]order rate|sequence gap)\b",
        re.I,
    ),
}
_NOTES: dict[WebhookOrderingRequirementType, str] = {
    "ordering_scope": "Define the aggregate or tenant boundary where ordering is guaranteed.",
    "sequence_metadata": "Require stable sequence, version, timestamp, or partition metadata in each event.",
    "duplicate_out_of_order_handling": "Specify duplicate, late, stale, and out-of-order event handling.",
    "replay_backfill_behavior": "Document replay and backfill ordering semantics for historical delivery.",
    "consumer_compatibility": "Preserve compatibility expectations for existing webhook consumers.",
    "monitoring_evidence": "Capture observability for sequence gaps, out-of-order delivery, and replay outcomes.",
}


@dataclass(frozen=True, slots=True)
class SourceWebhookOrderingRequirement:
    requirement_type: WebhookOrderingRequirementType
    source_field: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: Literal["high", "medium"] = "high"
    planning_note: str = ""

    @property
    def category(self) -> WebhookOrderingRequirementType:
        return self.requirement_type

    def to_dict(self) -> dict[str, Any]:
        return {
            "requirement_type": self.requirement_type,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceWebhookOrderingRequirementsReport:
    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceWebhookOrderingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceWebhookOrderingRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceWebhookOrderingRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [item.to_dict() for item in self.requirements],
            "records": [item.to_dict() for item in self.records],
            "findings": [item.to_dict() for item in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source Webhook Ordering Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        lines = [title, "", "## Summary", "", f"- Requirements found: {self.summary.get('requirement_count', 0)}"]
        if not self.requirements:
            lines.extend(["", "No webhook ordering requirements were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Type | Confidence | Source | Evidence |", "| --- | --- | --- | --- |"])
        for item in self.requirements:
            lines.append(
                f"| {item.requirement_type} | {item.confidence} | {_markdown_cell(item.source_field)} | "
                f"{_markdown_cell('; '.join(item.evidence))} |"
            )
        return "\n".join(lines)


def build_source_webhook_ordering_requirements(source: Any) -> SourceWebhookOrderingRequirementsReport:
    brief_id, title, payload = _payload(source)
    buckets: dict[WebhookOrderingRequirementType, list[tuple[str, str]]] = {kind: [] for kind in _TYPE_ORDER}
    saw_ordering_context = False

    for field, text in _texts(payload):
        if _NO_SCOPE_RE.search(text):
            return SourceWebhookOrderingRequirementsReport(brief_id=brief_id, title=title, summary=_summary(()))
        searchable = f"{field.replace('_', ' ')} {text}"
        has_webhook = bool(_WEBHOOK_RE.search(searchable) or "webhook" in field.casefold())
        has_ordering = bool(_ORDERING_RE.search(searchable))
        if has_webhook and has_ordering:
            saw_ordering_context = True
        contextual_followup = saw_ordering_context and (
            _FIELD_CONTEXT_RE.search(field)
            or any(pattern.search(searchable) for pattern in _TYPE_PATTERNS.values())
        )
        if not ((has_webhook and (has_ordering or saw_ordering_context or _FIELD_CONTEXT_RE.search(field))) or contextual_followup):
            continue
        for kind, pattern in _TYPE_PATTERNS.items():
            if pattern.search(searchable):
                buckets[kind].append((field, text))

    if not saw_ordering_context and not any(buckets.values()):
        return SourceWebhookOrderingRequirementsReport(brief_id=brief_id, title=title, summary=_summary(()))
    if not saw_ordering_context and all(_RETRY_SIGNING_ONLY_RE.search(text) for _, text in _texts(payload)):
        return SourceWebhookOrderingRequirementsReport(brief_id=brief_id, title=title, summary=_summary(()))

    requirements = tuple(
        SourceWebhookOrderingRequirement(
            requirement_type=kind,
            source_field=evidence[0][0],
            evidence=tuple(_dedupe(f"{field}: {text}" for field, text in evidence))[:5],
            confidence="high" if saw_ordering_context else "medium",
            planning_note=_NOTES[kind],
        )
        for kind in _TYPE_ORDER
        if (evidence := buckets[kind])
    )
    return SourceWebhookOrderingRequirementsReport(brief_id=brief_id, title=title, requirements=requirements, summary=_summary(requirements))


def extract_source_webhook_ordering_requirements(source: Any) -> tuple[SourceWebhookOrderingRequirement, ...]:
    return build_source_webhook_ordering_requirements(source).requirements


def derive_source_webhook_ordering_requirements(source: Any) -> SourceWebhookOrderingRequirementsReport:
    return build_source_webhook_ordering_requirements(source)


def generate_source_webhook_ordering_requirements(source: Any) -> SourceWebhookOrderingRequirementsReport:
    return build_source_webhook_ordering_requirements(source)


def summarize_source_webhook_ordering_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceWebhookOrderingRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_webhook_ordering_requirements(source_or_report).summary


def source_webhook_ordering_requirements_to_dict(report: SourceWebhookOrderingRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_webhook_ordering_requirements_to_dict.__test__ = False


def source_webhook_ordering_requirements_to_dicts(
    result: SourceWebhookOrderingRequirementsReport | Iterable[SourceWebhookOrderingRequirement],
) -> list[dict[str, Any]]:
    if isinstance(result, SourceWebhookOrderingRequirementsReport):
        return result.to_dicts()
    return [item.to_dict() for item in result]


source_webhook_ordering_requirements_to_dicts.__test__ = False


def source_webhook_ordering_requirements_to_markdown(report: SourceWebhookOrderingRequirementsReport) -> str:
    return report.to_markdown()


source_webhook_ordering_requirements_to_markdown.__test__ = False


def _payload(source: Any) -> tuple[str | None, str | None, Mapping[str, Any]]:
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
    elif isinstance(source, Mapping):
        payload = dict(source)
    elif not isinstance(source, (str, bytes, bytearray)):
        payload = {key: getattr(source, key) for key in dir(source) if not key.startswith("_") and not callable(getattr(source, key))}
    else:
        payload = {"body": source}
    return _optional(payload.get("id") or payload.get("source_id")), _optional(payload.get("title")), payload


def _texts(payload: Mapping[str, Any], prefix: str | None = None) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    for key in sorted(payload, key=str):
        field = f"{prefix}.{key}" if prefix else str(key)
        value = payload[key]
        if isinstance(value, Mapping):
            values.extend(_texts(value, field))
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                if isinstance(item, Mapping):
                    values.extend(_texts(item, f"{field}[{index}]"))
                elif text := _optional(item):
                    values.extend((f"{field}[{index}]", part) for part in _parts(text))
        elif text := _optional(value):
            values.extend((field, part) for part in _parts(text))
    return values


def _parts(text: str) -> list[str]:
    return [part for raw in _SPLIT_RE.split(text) if (part := _clean(raw))]


def _optional(value: Any) -> str | None:
    if value is None:
        return None
    text = _clean(str(value))
    return text or None


def _clean(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip(" -\t\r\n.")


def _dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _summary(requirements: tuple[SourceWebhookOrderingRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "types": [item.requirement_type for item in requirements],
        "type_counts": {kind: sum(1 for item in requirements if item.requirement_type == kind) for kind in _TYPE_ORDER},
        "has_ordering_requirements": bool(requirements),
    }


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")
