"""Extract source-level queue dead-letter handling requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint._source_requirement_utils import (
    dedupe,
    evidence_snippet,
    markdown_cell,
    optional_text,
    segments,
    source_id,
    source_payloads,
)


QueueDeadLetterCategory = Literal[
    "dlq_routing",
    "poison_message",
    "retry_exhaustion",
    "replay_tooling",
    "quarantine_review",
    "alerting",
    "retention",
    "ownership",
]
QueueDeadLetterConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[QueueDeadLetterCategory, ...] = (
    "dlq_routing",
    "poison_message",
    "retry_exhaustion",
    "replay_tooling",
    "quarantine_review",
    "alerting",
    "retention",
    "ownership",
)
_CONFIDENCE_ORDER: dict[QueueDeadLetterConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SCANNED_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "scope",
    "non_goals",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "queue",
    "messaging",
    "dead_letter",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CONTEXT_RE = re.compile(
    r"\b(?:dead[- ]?letter|dlq|dead letter queue|poison message|retry exhaustion|"
    r"failed messages?|quarantine|message replay|queue replay)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"route|send|move|classify|retry|exhaust|replay|review|quarantine|alert|monitor|"
    r"retain|retention|owner|ownership|runbook|support|provide|acceptance|done when)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}"
    r"\b(?:dead[- ]?letter|dlq|poison message|queue replay|retry exhaustion)\b|"
    r"\b(?:dead[- ]?letter|dlq|poison message|queue replay|retry exhaustion)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|excluded|no changes?)\b",
    re.I,
)
_UNRELATED_RE = re.compile(r"\b(?:letter template|cover letter|dead link|dead code)\b", re.I)
_CATEGORY_PATTERNS: dict[QueueDeadLetterCategory, re.Pattern[str]] = {
    "dlq_routing": re.compile(r"\b(?:dead[- ]?letter queue|dead[- ]?letter|dlq|failure queue|route failed|move failed)\b", re.I),
    "poison_message": re.compile(r"\b(?:poison message|poison pill|malformed message|permanent failure|non[- ]?retryable)\b", re.I),
    "retry_exhaustion": re.compile(r"\b(?:retry exhaustion|retries exhausted|max retries|retry limit|after \d+ retries|final attempt)\b", re.I),
    "replay_tooling": re.compile(r"\b(?:replay tooling|replay tool|redrive|reprocess|manual replay|bulk replay|message replay)\b", re.I),
    "quarantine_review": re.compile(r"\b(?:quarantine|review queue|manual review|triage|inspect failed|operator review)\b", re.I),
    "alerting": re.compile(r"\b(?:alert|alerting|monitor|monitoring|metric|metrics|dashboard|pager|on-call|alarm)\b", re.I),
    "retention": re.compile(r"\b(?:retention|retain|ttl|expire|expiration|purge|archive|keep failed)\b", re.I),
    "ownership": re.compile(r"\b(?:owner|ownership|runbook|on-call|responsible team|operational owner|support team)\b", re.I),
}
_OWNER_SUGGESTIONS = {
    "dlq_routing": ("backend", "platform"),
    "poison_message": ("backend", "data_quality"),
    "retry_exhaustion": ("backend", "platform"),
    "replay_tooling": ("platform", "operations"),
    "quarantine_review": ("operations", "support"),
    "alerting": ("observability", "operations"),
    "retention": ("platform", "compliance"),
    "ownership": ("operations", "service_owner"),
}
_PLANNING_NOTES = {
    "dlq_routing": ("Define which failures route to the DLQ and preserve message metadata for recovery.",),
    "poison_message": ("Classify poison messages and distinguish retryable from permanent failures.",),
    "retry_exhaustion": ("Specify retry limits, backoff, and transition behavior after attempts are exhausted.",),
    "replay_tooling": ("Plan replay tooling, validation, throttling, and audit trail for redrives.",),
    "quarantine_review": ("Define quarantine review workflow, triage fields, and manual disposition states.",),
    "alerting": ("Add alerts and dashboards for DLQ depth, failure rate, replay failures, and age.",),
    "retention": ("Set retention, purge, and archive rules for failed messages and payloads.",),
    "ownership": ("Assign operational owners, runbooks, and escalation path for DLQ handling.",),
}
_MISSING_DETAIL_MESSAGES = {
    "missing_replay": "Specify replay or redrive tooling for dead-letter messages.",
    "missing_alerting": "Define alerting or monitoring for dead-letter queue growth and age.",
    "missing_owner": "Assign operational ownership and runbook coverage for DLQ handling.",
}


@dataclass(frozen=True, slots=True)
class SourceQueueDeadLetterRequirement:
    category: QueueDeadLetterCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: QueueDeadLetterConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> QueueDeadLetterCategory:
        return self.category

    @property
    def concern(self) -> QueueDeadLetterCategory:
        return self.category

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "planning_notes": list(self.planning_notes),
            "gap_messages": list(self.gap_messages),
        }


@dataclass(frozen=True, slots=True)
class SourceQueueDeadLetterRequirementsReport:
    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceQueueDeadLetterRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceQueueDeadLetterRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceQueueDeadLetterRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source Queue Dead Letter Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        category_counts = self.summary.get("category_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Category counts: " + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
        ]
        if not self.requirements:
            lines.extend(["", "No source queue dead-letter requirements were inferred."])
            return "\n".join(lines)
        lines.extend(["", "## Requirements", "", "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |", "| --- | --- | --- | --- | --- | --- | --- | --- |"])
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | {markdown_cell(requirement.value or '')} | {requirement.confidence} | "
                f"{markdown_cell(requirement.source_field)} | {markdown_cell(', '.join(requirement.suggested_owners))} | "
                f"{markdown_cell('; '.join(requirement.evidence))} | {markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{markdown_cell('; '.join(requirement.gap_messages))} |"
            )
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: QueueDeadLetterCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: QueueDeadLetterConfidence


def build_source_queue_dead_letter_requirements(source: Mapping[str, Any] | SourceBrief | ImplementationBrief | Iterable[Any] | str | object) -> SourceQueueDeadLetterRequirementsReport:
    payloads = source_payloads(source)
    candidates: list[_Candidate] = []
    for _, payload in payloads:
        if not _has_no_scope(payload):
            candidates.extend(_candidates(payload))
    requirements = tuple(_merge(candidates, _gap_messages(candidates)))
    ids = dedupe(source_id(payload) for _, payload in payloads)
    return SourceQueueDeadLetterRequirementsReport(
        brief_id=ids[0] if len(ids) == 1 else None,
        title=optional_text(payloads[0][1].get("title")) if payloads else None,
        requirements=requirements,
        summary=_summary(requirements, len(payloads)),
    )


def extract_source_queue_dead_letter_requirements(source: Any) -> tuple[SourceQueueDeadLetterRequirement, ...]:
    return build_source_queue_dead_letter_requirements(source).requirements


def derive_source_queue_dead_letter_requirements(source: Any) -> SourceQueueDeadLetterRequirementsReport:
    return build_source_queue_dead_letter_requirements(source)


def generate_source_queue_dead_letter_requirements(source: Any) -> SourceQueueDeadLetterRequirementsReport:
    return build_source_queue_dead_letter_requirements(source)


def summarize_source_queue_dead_letter_requirements(source: Any) -> dict[str, Any]:
    if isinstance(source, SourceQueueDeadLetterRequirementsReport):
        return dict(source.summary)
    return build_source_queue_dead_letter_requirements(source).summary


def source_queue_dead_letter_requirements_to_dict(report: SourceQueueDeadLetterRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_queue_dead_letter_requirements_to_dict.__test__ = False


def source_queue_dead_letter_requirements_to_dicts(requirements: tuple[SourceQueueDeadLetterRequirement, ...] | list[SourceQueueDeadLetterRequirement] | SourceQueueDeadLetterRequirementsReport) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceQueueDeadLetterRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_queue_dead_letter_requirements_to_dicts.__test__ = False


def source_queue_dead_letter_requirements_to_markdown(report: SourceQueueDeadLetterRequirementsReport) -> str:
    return report.to_markdown()


source_queue_dead_letter_requirements_to_markdown.__test__ = False


def _candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    found: list[_Candidate] = []
    for source_field, text in segments(payload, _SCANNED_FIELDS):
        if source_field == "title" and re.search(r"\brequirements?\b\.?$", text, re.I):
            continue
        searchable = f"{source_field.replace('_', ' ')} {text}"
        if not (_CONTEXT_RE.search(searchable) and _REQUIREMENT_RE.search(searchable)) or _NEGATED_RE.search(searchable) or _UNRELATED_RE.search(searchable):
            continue
        for category in _CATEGORY_ORDER:
            if _CATEGORY_PATTERNS[category].search(searchable):
                found.append(_Candidate(category, _value(category, text), source_field, evidence_snippet(source_field, text), _confidence(searchable)))
    return found


def _has_no_scope(payload: Mapping[str, Any]) -> bool:
    return any(_NEGATED_RE.search(text) for _, text in segments(payload, _SCANNED_FIELDS))


def _merge(candidates: Iterable[_Candidate], gap_messages: tuple[str, ...]) -> list[SourceQueueDeadLetterRequirement]:
    grouped: dict[QueueDeadLetterCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)
    result: list[SourceQueueDeadLetterRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        result.append(
            SourceQueueDeadLetterRequirement(
                category=category,
                source_field=sorted({item.source_field for item in items}, key=str.casefold)[0],
                evidence=tuple(sorted(dedupe(item.evidence for item in items), key=str.casefold))[:5],
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                value=dedupe(item.value for item in items)[0],
                suggested_owners=_OWNER_SUGGESTIONS[category],
                planning_notes=_PLANNING_NOTES[category],
                gap_messages=gap_messages,
            )
        )
    return result


def _gap_messages(candidates: list[_Candidate]) -> tuple[str, ...]:
    if not candidates:
        return ()
    categories = {candidate.category for candidate in candidates}
    flags = []
    if "replay_tooling" not in categories:
        flags.append("missing_replay")
    if "alerting" not in categories:
        flags.append("missing_alerting")
    if "ownership" not in categories:
        flags.append("missing_owner")
    return tuple(_MISSING_DETAIL_MESSAGES[flag] for flag in flags)


def _summary(requirements: tuple[SourceQueueDeadLetterRequirement, ...], source_count: int) -> dict[str, Any]:
    category_counts = {category: 0 for category in _CATEGORY_ORDER}
    confidence_counts = {confidence: 0 for confidence in _CONFIDENCE_ORDER}
    for requirement in requirements:
        category_counts[requirement.category] += 1
        confidence_counts[requirement.confidence] += 1
    gap_messages = dedupe(message for requirement in requirements for message in requirement.gap_messages)
    missing_flags = [flag for flag, message in _MISSING_DETAIL_MESSAGES.items() if message in gap_messages]
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "categories": [category for category in _CATEGORY_ORDER if category_counts[category]],
        "category_counts": category_counts,
        "confidence_counts": confidence_counts,
        "missing_detail_flags": missing_flags,
        "gap_messages": gap_messages,
        "owner_suggestions": dedupe(owner for requirement in requirements for owner in requirement.suggested_owners),
        "status": "no_queue_dead_letter_language" if not requirements else ("needs_dead_letter_details" if missing_flags else "ready_for_planning"),
    }


def _confidence(text: str) -> QueueDeadLetterConfidence:
    return "high" if re.search(r"\b(?:dlq|dead[- ]?letter|poison message|retry exhaustion|redrive)\b", text, re.I) else "medium"


def _value(category: QueueDeadLetterCategory, text: str) -> str | None:
    match = _CATEGORY_PATTERNS[category].search(text)
    return optional_text(match.group(0).casefold()) if match else None
