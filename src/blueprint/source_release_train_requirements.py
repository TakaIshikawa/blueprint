"""Extract source-level release train requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal

from blueprint._source_requirement_utils import dedupe, evidence_snippet, markdown_cell, segments, source_payloads


ReleaseTrainRequirementType = Literal[
    "train_cadence",
    "freeze_window",
    "branch_cutoff",
    "environment_promotion",
    "release_captain_ownership",
    "rollback_window",
    "stakeholder_signoff",
]
ReleaseTrainConfidence = Literal["high", "medium", "low"]

_TYPE_ORDER: tuple[ReleaseTrainRequirementType, ...] = (
    "train_cadence",
    "freeze_window",
    "branch_cutoff",
    "environment_promotion",
    "release_captain_ownership",
    "rollback_window",
    "stakeholder_signoff",
)
_CONFIDENCE_ORDER = {"high": 0, "medium": 1, "low": 2}
_LABELS = {
    "train_cadence": "Train cadence",
    "freeze_window": "Freeze window",
    "branch_cutoff": "Branch cutoff",
    "environment_promotion": "Environment promotion",
    "release_captain_ownership": "Release captain ownership",
    "rollback_window": "Rollback window",
    "stakeholder_signoff": "Stakeholder sign-off",
}
_GUIDANCE = {
    "train_cadence": ("cadence", "calendar", "owner"),
    "freeze_window": ("freeze_start", "freeze_end", "exception_policy"),
    "branch_cutoff": ("cutoff_time", "branch_policy", "approver"),
    "environment_promotion": ("environment_sequence", "promotion_gate", "validation_owner"),
    "release_captain_ownership": ("captain", "backup", "handoff_channel"),
    "rollback_window": ("rollback_window", "rollback_trigger", "owner"),
    "stakeholder_signoff": ("stakeholders", "approval_channel", "deadline"),
}
_PATTERNS: dict[ReleaseTrainRequirementType, re.Pattern[str]] = {
    "train_cadence": re.compile(r"\b(?:release train|train cadence|weekly release|biweekly release|monthly release|scheduled release|release calendar)\b", re.I),
    "freeze_window": re.compile(r"\b(?:freeze window|code freeze|release freeze|change freeze|freeze starts?|freeze ends?)\b", re.I),
    "branch_cutoff": re.compile(r"\b(?:branch cutoff|cutoff branch|merge cutoff|branch cut|release branch|cutoff time)\b", re.I),
    "environment_promotion": re.compile(r"\b(?:environment promotion|promote to (?:staging|prod|production)|staging to prod|dev to staging|promotion gate)\b", re.I),
    "release_captain_ownership": re.compile(r"\b(?:release captain|release owner|captain owns|release manager|ship captain)\b", re.I),
    "rollback_window": re.compile(r"\b(?:rollback window|rollback trigger|backout window|revert window|rollback policy)\b", re.I),
    "stakeholder_signoff": re.compile(r"\b(?:stakeholder sign[- ]?off|sign[- ]?off|approval|go/no[- ]?go|business approval|product approval)\b", re.I),
}
_DETAILS = {
    "cadence": re.compile(r"\b(?:daily|weekly|biweekly|monthly|quarterly|every \d+|calendar)\b", re.I),
    "calendar": re.compile(r"\b(?:calendar|schedule|every|cron|date)\b", re.I),
    "owner": re.compile(r"\b(?:owner|captain|manager|on-call|release team)\b", re.I),
    "freeze_start": re.compile(r"\b(?:start|starts|begin|from|before)\b.{0,50}\b(?:freeze|code freeze|release)\b|\bfreeze\b.{0,50}\b(?:start|starts|begin|from)\b", re.I),
    "freeze_end": re.compile(r"\b(?:end|ends|until|lift)\b.{0,50}\b(?:freeze|code freeze|release)\b|\bfreeze\b.{0,50}\b(?:end|ends|until|lift)\b", re.I),
    "exception_policy": re.compile(r"\b(?:exception|hotfix|approval|override)\b", re.I),
    "cutoff_time": re.compile(r"\b(?:cutoff|by|before|at)\b.{0,40}\b(?:am|pm|utc|\d{1,2}:\d{2}|eod|noon)\b", re.I),
    "branch_policy": re.compile(r"\b(?:branch|merge|cherry-pick|release branch|main)\b", re.I),
    "approver": re.compile(r"\b(?:approver|approval|approved by|lead|captain)\b", re.I),
    "environment_sequence": re.compile(r"\b(?:dev|qa|staging|stage|prod|production)\b.{0,80}\b(?:dev|qa|staging|stage|prod|production)\b", re.I),
    "promotion_gate": re.compile(r"\b(?:gate|criteria|validation|smoke|canary|approval)\b", re.I),
    "validation_owner": re.compile(r"\b(?:qa|sre|owner|captain|release team)\b", re.I),
    "captain": re.compile(r"\b(?:captain|release manager|release owner)\b", re.I),
    "backup": re.compile(r"\b(?:backup|delegate|secondary)\b", re.I),
    "handoff_channel": re.compile(r"\b(?:slack|channel|handoff|war room|room)\b", re.I),
    "rollback_window": re.compile(r"\b(?:\d+\s*(?:hours?|days?)|window|within)\b", re.I),
    "rollback_trigger": re.compile(r"\b(?:trigger|threshold|error rate|slo|incident|rollback criteria)\b", re.I),
    "stakeholders": re.compile(r"\b(?:stakeholder|product|legal|support|sales|security|business)\b", re.I),
    "approval_channel": re.compile(r"\b(?:jira|slack|ticket|approval|sign-off|email)\b", re.I),
    "deadline": re.compile(r"\b(?:deadline|by|before|due|eod|date)\b", re.I),
}
_CONTEXT_RE = re.compile(r"\b(?:release train|scheduled release|code freeze|branch cutoff|release captain|environment promotion|rollback|sign[- ]?off)\b", re.I)
_REQUIREMENT_RE = re.compile(r"\b(?:must|shall|required|requires?|need(?:ed|s)?|should|define|own|approve|promote|freeze|cutoff|rollback|sign[- ]?off)\b", re.I)
_NEGATED_RE = re.compile(r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}\b(?:release train|release coordination|code freeze|branch cutoff|sign[- ]?off)\b.{0,120}\b(?:required|needed|planned|in scope|work)\b|\b(?:release train|release coordination|code freeze|branch cutoff|sign[- ]?off)\b.{0,120}\b(?:not required|not needed|out of scope|no work)\b", re.I)
_SCANNED_FIELDS = ("title", "summary", "body", "description", "requirements", "scope", "acceptance_criteria", "definition_of_done", "constraints", "risks", "metadata", "brief_metadata", "source_payload")


@dataclass(frozen=True, slots=True)
class SourceReleaseTrainRequirement:
    source_brief_id: str | None
    requirement_type: ReleaseTrainRequirementType
    requirement_text: str
    label: str
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: ReleaseTrainConfidence = "medium"

    @property
    def category(self) -> ReleaseTrainRequirementType:
        return self.requirement_type

    @property
    def requirement_category(self) -> ReleaseTrainRequirementType:
        return self.requirement_type

    @property
    def missing_detail_guidance(self) -> str | None:
        return "; ".join(self.missing_details) if self.missing_details else None

    def to_dict(self) -> dict[str, Any]:
        return {"source_brief_id": self.source_brief_id, "requirement_type": self.requirement_type, "requirement_category": self.requirement_category, "requirement_text": self.requirement_text, "label": self.label, "source_field": self.source_field, "evidence": list(self.evidence), "missing_details": list(self.missing_details), "missing_detail_guidance": self.missing_detail_guidance, "confidence": self.confidence}


@dataclass(frozen=True, slots=True)
class SourceReleaseTrainRequirementsReport:
    source_id: str | None = None
    requirements: tuple[SourceReleaseTrainRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceReleaseTrainRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceReleaseTrainRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {"source_id": self.source_id, "requirements": [item.to_dict() for item in self.requirements], "summary": dict(self.summary), "records": [item.to_dict() for item in self.records], "findings": [item.to_dict() for item in self.findings]}

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.requirements]

    def to_markdown(self) -> str:
        lines = [f"# Source Release Train Requirements{': ' + self.source_id if self.source_id else ''}", "", f"Requirements found: {self.summary.get('requirement_count', 0)}"]
        if not self.requirements:
            return "\n".join([*lines, "", "No release train requirements were inferred."])
        lines.extend(["", "| Type | Requirement | Missing Details | Evidence |", "| --- | --- | --- | --- |"])
        for item in self.requirements:
            lines.append(f"| {markdown_cell(item.requirement_type)} | {markdown_cell(item.requirement_text)} | {markdown_cell('; '.join(item.missing_details))} | {markdown_cell('; '.join(item.evidence))} |")
        return "\n".join(lines)


def build_source_release_train_requirements(source: Any) -> SourceReleaseTrainRequirementsReport:
    payloads = source_payloads(source)
    records = tuple(_merge(_candidates(payloads)))
    ids = dedupe(source_id for source_id, _ in payloads if source_id)
    return SourceReleaseTrainRequirementsReport(ids[0] if len(ids) == 1 else None, records, _summary(records, len(payloads)))


extract_source_release_train_requirements = build_source_release_train_requirements
generate_source_release_train_requirements = build_source_release_train_requirements
derive_source_release_train_requirements = build_source_release_train_requirements


def summarize_source_release_train_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceReleaseTrainRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_release_train_requirements(source_or_report).summary


def source_release_train_requirements_to_dict(report: SourceReleaseTrainRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_release_train_requirements_to_dict.__test__ = False


def source_release_train_requirements_to_dicts(items: SourceReleaseTrainRequirementsReport | Iterable[SourceReleaseTrainRequirement]) -> list[dict[str, Any]]:
    if isinstance(items, SourceReleaseTrainRequirementsReport):
        return items.to_dicts()
    return [item.to_dict() for item in items]


source_release_train_requirements_to_dicts.__test__ = False


def source_release_train_requirements_to_markdown(report: SourceReleaseTrainRequirementsReport) -> str:
    return report.to_markdown()


source_release_train_requirements_to_markdown.__test__ = False


def _candidates(payloads: Iterable[tuple[str | None, dict[str, Any]]]) -> list[SourceReleaseTrainRequirement]:
    out: list[SourceReleaseTrainRequirement] = []
    for source_id, payload in payloads:
        for field_name, text in segments(payload, _SCANNED_FIELDS):
            searchable = f"{field_name} {text}"
            if _NEGATED_RE.search(searchable) or not _CONTEXT_RE.search(searchable) or not _REQUIREMENT_RE.search(text):
                continue
            for requirement_type, pattern in _PATTERNS.items():
                if pattern.search(searchable):
                    missing = tuple(detail for detail in _GUIDANCE[requirement_type] if not _DETAILS[detail].search(searchable))
                    out.append(SourceReleaseTrainRequirement(source_id, requirement_type, text, _LABELS[requirement_type], field_name, (evidence_snippet(field_name, text),), missing, "high" if _REQUIREMENT_RE.search(text) else "medium"))
    return out


def _merge(candidates: Iterable[SourceReleaseTrainRequirement]) -> list[SourceReleaseTrainRequirement]:
    by_type: dict[ReleaseTrainRequirementType, list[SourceReleaseTrainRequirement]] = {}
    for item in candidates:
        by_type.setdefault(item.requirement_type, []).append(item)
    merged: list[SourceReleaseTrainRequirement] = []
    for requirement_type in _TYPE_ORDER:
        items = by_type.get(requirement_type, [])
        if not items:
            continue
        best = min(items, key=lambda item: (_CONFIDENCE_ORDER[item.confidence], len(item.missing_details), item.source_field or ""))
        missing = tuple(detail for detail in _GUIDANCE[requirement_type] if all(detail in item.missing_details for item in items))
        merged.append(SourceReleaseTrainRequirement(best.source_brief_id, requirement_type, best.requirement_text, best.label, best.source_field, tuple(dedupe(ev for item in items for ev in item.evidence))[:5], missing, min((item.confidence for item in items), key=_CONFIDENCE_ORDER.__getitem__)))
    return merged


def _summary(records: tuple[SourceReleaseTrainRequirement, ...], source_count: int) -> dict[str, Any]:
    counts = {item: sum(1 for record in records if record.requirement_type == item) for item in _TYPE_ORDER}
    return {"source_count": source_count, "requirement_count": len(records), "requirement_type_counts": counts, "confidence_counts": {level: sum(1 for record in records if record.confidence == level) for level in _CONFIDENCE_ORDER}, "missing_detail_count": sum(len(record.missing_details) for record in records), "requirement_types": [item for item in _TYPE_ORDER if counts[item]]}


__all__ = [name for name in globals() if name.startswith(("SourceReleaseTrain", "build_source_release", "extract_source_release", "generate_source_release", "derive_source_release", "summarize_source_release", "source_release"))] + ["ReleaseTrainRequirementType", "ReleaseTrainConfidence"]
