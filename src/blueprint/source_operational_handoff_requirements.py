"""Extract source-level operational handoff requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal

from blueprint._source_requirement_utils import dedupe, evidence_snippet, markdown_cell, segments, source_payloads

OperationalHandoffRequirementType = Literal["support_handoff", "on_call_ownership", "runbook_location", "escalation_path", "launch_checklist", "training_materials", "post_launch_review"]
OperationalHandoffConfidence = Literal["high", "medium", "low"]
_TYPE_ORDER: tuple[OperationalHandoffRequirementType, ...] = ("support_handoff", "on_call_ownership", "runbook_location", "escalation_path", "launch_checklist", "training_materials", "post_launch_review")
_CONFIDENCE_ORDER = {"high": 0, "medium": 1, "low": 2}
_LABELS = {
    "support_handoff": "Support handoff",
    "on_call_ownership": "On-call ownership",
    "runbook_location": "Runbook location",
    "escalation_path": "Escalation path",
    "launch_checklist": "Launch checklist",
    "training_materials": "Training materials",
    "post_launch_review": "Post-launch review",
}
_MISSING = {
    "support_handoff": ("support_owner", "handoff_channel", "handoff_timing"),
    "on_call_ownership": ("on_call_owner", "schedule", "backup"),
    "runbook_location": ("runbook_url", "owner", "update_cadence"),
    "escalation_path": ("levels", "contacts", "severity_rules"),
    "launch_checklist": ("checklist_items", "approver", "completion_evidence"),
    "training_materials": ("audience", "materials", "training_date"),
    "post_launch_review": ("review_date", "participants", "success_metrics"),
}
_PATTERNS: dict[OperationalHandoffRequirementType, re.Pattern[str]] = {
    "support_handoff": re.compile(r"\b(?:support handoff|handoff to support|support readiness|cs handoff|support enablement)\b", re.I),
    "on_call_ownership": re.compile(r"\b(?:on[- ]?call owner|on[- ]?call ownership|pager owner|rotation|primary on[- ]?call)\b", re.I),
    "runbook_location": re.compile(r"\b(?:runbook|playbook|ops guide|operational guide)\b", re.I),
    "escalation_path": re.compile(r"\b(?:escalation path|escalation policy|tier escalation|sev[ -]?\d|page security|page sre)\b", re.I),
    "launch_checklist": re.compile(r"\b(?:launch checklist|go[- ]live checklist|release checklist|pre[- ]launch checklist)\b", re.I),
    "training_materials": re.compile(r"\b(?:training materials?|enablement deck|support training|agent training|knowledge base|kb article)\b", re.I),
    "post_launch_review": re.compile(r"\b(?:post[- ]launch review|launch retrospective|hypercare review|after action review|postmortem)\b", re.I),
}
_DETAILS = {
    "support_owner": re.compile(r"\b(?:support owner|support team|cs owner|customer support|agent)\b", re.I),
    "handoff_channel": re.compile(r"\b(?:slack|channel|ticket|handoff doc|meeting)\b", re.I),
    "handoff_timing": re.compile(r"\b(?:before launch|by|date|eod|during launch|at launch)\b", re.I),
    "on_call_owner": re.compile(r"\b(?:on-call owner|primary|pager owner|sre|ops owner)\b", re.I),
    "schedule": re.compile(r"\b(?:schedule|rotation|calendar|pagerduty|opsgenie)\b", re.I),
    "backup": re.compile(r"\b(?:backup|secondary|delegate)\b", re.I),
    "runbook_url": re.compile(r"\b(?:url|link|confluence|notion|wiki|https?://)\b", re.I),
    "owner": re.compile(r"\b(?:owner|owned by|team)\b", re.I),
    "update_cadence": re.compile(r"\b(?:update|review cadence|monthly|quarterly|before launch)\b", re.I),
    "levels": re.compile(r"\b(?:tier|level|sev|severity|l1|l2|l3)\b", re.I),
    "contacts": re.compile(r"\b(?:contact|pager|slack|email|team)\b", re.I),
    "severity_rules": re.compile(r"\b(?:severity|sev|threshold|priority)\b", re.I),
    "checklist_items": re.compile(r"\b(?:checklist|smoke test|monitoring|rollback|verification)\b", re.I),
    "approver": re.compile(r"\b(?:approver|approval|sign-off|captain|owner)\b", re.I),
    "completion_evidence": re.compile(r"\b(?:evidence|ticket|checkbox|record|done)\b", re.I),
    "audience": re.compile(r"\b(?:support|agents?|on-call|ops|sre|training audience)\b", re.I),
    "materials": re.compile(r"\b(?:deck|kb|article|materials|docs|guide)\b", re.I),
    "training_date": re.compile(r"\b(?:date|by|before launch|session|training)\b", re.I),
    "review_date": re.compile(r"\b(?:date|after launch|post-launch|within \d+ days?|scheduled)\b", re.I),
    "participants": re.compile(r"\b(?:participants|support|sre|product|engineering|ops)\b", re.I),
    "success_metrics": re.compile(r"\b(?:metrics|incidents|tickets|sla|success|error rate)\b", re.I),
}
_REQ_RE = re.compile(r"\b(?:must|shall|required|requires?|need(?:ed|s)?|should|define|own|publish|train|review|handoff|escalate)\b", re.I)
_NEGATED_RE = re.compile(r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}\b(?:operational handoff|support handoff|runbook|on-call|training)\b.{0,120}\b(?:required|needed|planned|in scope|work)\b|\b(?:operational handoff|support handoff|runbook|on-call|training)\b.{0,120}\b(?:not required|not needed|out of scope|no work)\b", re.I)
_SCANNED_FIELDS = ("title", "summary", "body", "description", "requirements", "scope", "acceptance_criteria", "definition_of_done", "operations", "support", "metadata", "source_payload")


@dataclass(frozen=True, slots=True)
class SourceOperationalHandoffRequirement:
    source_brief_id: str | None
    requirement_type: OperationalHandoffRequirementType
    requirement_text: str
    label: str
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: OperationalHandoffConfidence = "medium"

    @property
    def category(self) -> OperationalHandoffRequirementType:
        return self.requirement_type

    @property
    def requirement_category(self) -> OperationalHandoffRequirementType:
        return self.requirement_type

    @property
    def missing_detail_guidance(self) -> str | None:
        return "; ".join(self.missing_details) if self.missing_details else None

    def to_dict(self) -> dict[str, Any]:
        return {"source_brief_id": self.source_brief_id, "requirement_type": self.requirement_type, "requirement_category": self.requirement_category, "requirement_text": self.requirement_text, "label": self.label, "source_field": self.source_field, "evidence": list(self.evidence), "missing_details": list(self.missing_details), "missing_detail_guidance": self.missing_detail_guidance, "confidence": self.confidence}


@dataclass(frozen=True, slots=True)
class SourceOperationalHandoffRequirementsReport:
    source_id: str | None = None
    requirements: tuple[SourceOperationalHandoffRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceOperationalHandoffRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceOperationalHandoffRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {"source_id": self.source_id, "requirements": [item.to_dict() for item in self.requirements], "summary": dict(self.summary), "records": [item.to_dict() for item in self.records], "findings": [item.to_dict() for item in self.findings]}

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.requirements]

    def to_markdown(self) -> str:
        lines = [f"# Source Operational Handoff Requirements{': ' + self.source_id if self.source_id else ''}", "", f"Requirements found: {self.summary.get('requirement_count', 0)}"]
        if not self.requirements:
            return "\n".join([*lines, "", "No operational handoff requirements were inferred."])
        lines.extend(["", "| Type | Requirement | Missing Details | Evidence |", "| --- | --- | --- | --- |"])
        for item in self.requirements:
            lines.append(f"| {markdown_cell(item.requirement_type)} | {markdown_cell(item.requirement_text)} | {markdown_cell('; '.join(item.missing_details))} | {markdown_cell('; '.join(item.evidence))} |")
        return "\n".join(lines)


def build_source_operational_handoff_requirements(source: Any) -> SourceOperationalHandoffRequirementsReport:
    payloads = source_payloads(source)
    records = tuple(_merge(_candidates(payloads)))
    ids = dedupe(source_id for source_id, _ in payloads if source_id)
    return SourceOperationalHandoffRequirementsReport(ids[0] if len(ids) == 1 else None, records, _summary(records, len(payloads)))


extract_source_operational_handoff_requirements = build_source_operational_handoff_requirements
generate_source_operational_handoff_requirements = build_source_operational_handoff_requirements
derive_source_operational_handoff_requirements = build_source_operational_handoff_requirements


def summarize_source_operational_handoff_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceOperationalHandoffRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_operational_handoff_requirements(source_or_report).summary


def source_operational_handoff_requirements_to_dict(report: SourceOperationalHandoffRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_operational_handoff_requirements_to_dict.__test__ = False


def source_operational_handoff_requirements_to_dicts(items: SourceOperationalHandoffRequirementsReport | Iterable[SourceOperationalHandoffRequirement]) -> list[dict[str, Any]]:
    if isinstance(items, SourceOperationalHandoffRequirementsReport):
        return items.to_dicts()
    return [item.to_dict() for item in items]


source_operational_handoff_requirements_to_dicts.__test__ = False


def source_operational_handoff_requirements_to_markdown(report: SourceOperationalHandoffRequirementsReport) -> str:
    return report.to_markdown()


source_operational_handoff_requirements_to_markdown.__test__ = False


def _candidates(payloads: Iterable[tuple[str | None, dict[str, Any]]]) -> list[SourceOperationalHandoffRequirement]:
    out: list[SourceOperationalHandoffRequirement] = []
    for source_id, payload in payloads:
        for field_name, text in segments(payload, _SCANNED_FIELDS):
            searchable = f"{field_name} {text}"
            if _NEGATED_RE.search(searchable) or not _REQ_RE.search(text):
                continue
            for requirement_type, pattern in _PATTERNS.items():
                if pattern.search(searchable):
                    missing = tuple(detail for detail in _MISSING[requirement_type] if not _DETAILS[detail].search(searchable))
                    out.append(SourceOperationalHandoffRequirement(source_id, requirement_type, text, _LABELS[requirement_type], field_name, (evidence_snippet(field_name, text),), missing, "high"))
    return out


def _merge(candidates: Iterable[SourceOperationalHandoffRequirement]) -> list[SourceOperationalHandoffRequirement]:
    grouped: dict[OperationalHandoffRequirementType, list[SourceOperationalHandoffRequirement]] = {}
    for item in candidates:
        grouped.setdefault(item.requirement_type, []).append(item)
    records = []
    for requirement_type in _TYPE_ORDER:
        items = grouped.get(requirement_type, [])
        if items:
            best = min(items, key=lambda item: (len(item.missing_details), item.source_field or ""))
            missing = tuple(detail for detail in _MISSING[requirement_type] if all(detail in item.missing_details for item in items))
            records.append(SourceOperationalHandoffRequirement(best.source_brief_id, requirement_type, best.requirement_text, best.label, best.source_field, tuple(dedupe(ev for item in items for ev in item.evidence))[:5], missing, "high"))
    return records


def _summary(records: tuple[SourceOperationalHandoffRequirement, ...], source_count: int) -> dict[str, Any]:
    counts = {item: sum(1 for record in records if record.requirement_type == item) for item in _TYPE_ORDER}
    return {"source_count": source_count, "requirement_count": len(records), "requirement_type_counts": counts, "confidence_counts": {level: sum(1 for record in records if record.confidence == level) for level in _CONFIDENCE_ORDER}, "missing_detail_count": sum(len(record.missing_details) for record in records), "requirement_types": [item for item in _TYPE_ORDER if counts[item]]}


__all__ = [name for name in globals() if name.startswith(("SourceOperationalHandoff", "build_source_operational", "extract_source_operational", "generate_source_operational", "derive_source_operational", "summarize_source_operational", "source_operational"))] + ["OperationalHandoffRequirementType", "OperationalHandoffConfidence"]
