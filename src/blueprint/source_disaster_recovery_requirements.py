"""Extract source-level disaster recovery requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal

from blueprint._source_requirement_utils import dedupe, evidence_snippet, markdown_cell, segments, source_payloads

DisasterRecoveryRequirementType = Literal["rto", "rpo", "failover_mode", "backup_dependency", "recovery_ownership", "validation_drill", "customer_communication", "regional_constraint"]
DisasterRecoveryConfidence = Literal["high", "medium", "low"]
DisasterRecoveryReadiness = Literal["ready", "needs_detail"]
_TYPE_ORDER: tuple[DisasterRecoveryRequirementType, ...] = ("rto", "rpo", "failover_mode", "backup_dependency", "recovery_ownership", "validation_drill", "customer_communication", "regional_constraint")
_CONFIDENCE_ORDER = {"high": 0, "medium": 1, "low": 2}
_READINESS_ORDER = ("ready", "needs_detail")
_LABELS = {"rto": "Recovery time objective", "rpo": "Recovery point objective", "failover_mode": "Failover mode", "backup_dependency": "Backup dependency", "recovery_ownership": "Recovery ownership", "validation_drill": "Validation drill", "customer_communication": "Customer communication", "regional_constraint": "Regional constraint"}
_MISSING = {
    "rto": ("target_time", "service_scope", "measurement"),
    "rpo": ("data_loss_window", "data_scope", "measurement"),
    "failover_mode": ("mode", "trigger", "rollback"),
    "backup_dependency": ("backup_source", "restore_path", "retention"),
    "recovery_ownership": ("owner", "backup_owner", "escalation"),
    "validation_drill": ("drill_cadence", "scenario", "evidence"),
    "customer_communication": ("audience", "channel", "timing"),
    "regional_constraint": ("regions", "residency_rule", "failover_target"),
}
_PATTERNS: dict[DisasterRecoveryRequirementType, re.Pattern[str]] = {
    "rto": re.compile(r"\b(?:rto|recovery time objective|restore within|recover within|time to recover)\b", re.I),
    "rpo": re.compile(r"\b(?:rpo|recovery point objective|data loss window|point objective|lose no more than)\b", re.I),
    "failover_mode": re.compile(r"\b(?:failover|fail over|active[- ]?active|active[- ]?passive|warm standby|cold standby|manual failover|automatic failover)\b", re.I),
    "backup_dependency": re.compile(r"\b(?:backup|snapshot|restore|point[- ]?in[- ]?time|pit r|replica|archive)\b", re.I),
    "recovery_ownership": re.compile(r"\b(?:recovery owner|incident commander|dr owner|sre|platform owner|on[- ]?call|escalation)\b", re.I),
    "validation_drill": re.compile(r"\b(?:restore drill|dr drill|recovery drill|game day|tabletop|validation drill|failover test|restore test)\b", re.I),
    "customer_communication": re.compile(r"\b(?:customer communication|status page|customer notice|support macro|incident update|notify customers?)\b", re.I),
    "regional_constraint": re.compile(r"\b(?:region|regional|multi[- ]?region|cross[- ]?region|data residency|residency|eu|us|apac|failover target)\b", re.I),
}
_DETAILS = {
    "target_time": re.compile(r"\b(?:within \d+|under \d+|\d+\s*(?:minutes?|hours?|days?))\b", re.I),
    "service_scope": re.compile(r"\b(?:service|api|database|tenant|region|workflow|customer|critical path)\b", re.I),
    "measurement": re.compile(r"\b(?:measure|metric|objective|sla|verified|tracked|reported)\b", re.I),
    "data_loss_window": re.compile(r"\b(?:\d+\s*(?:minutes?|hours?|days?)|zero data loss|no data loss|rpo|lose no more than)\b", re.I),
    "data_scope": re.compile(r"\b(?:data|database|object storage|queue|events?|transactions?|records?)\b", re.I),
    "mode": re.compile(r"\b(?:manual|automatic|active[- ]?active|active[- ]?passive|warm standby|cold standby)\b", re.I),
    "trigger": re.compile(r"\b(?:trigger|threshold|incident|region outage|health check|declared)\b", re.I),
    "rollback": re.compile(r"\b(?:rollback|failback|backout|return to primary)\b", re.I),
    "backup_source": re.compile(r"\b(?:backup|snapshot|replica|archive|point[- ]?in[- ]?time|pit r)\b", re.I),
    "restore_path": re.compile(r"\b(?:restore|replay|recover|runbook|procedure)\b", re.I),
    "retention": re.compile(r"\b(?:retention|retain|kept for|\d+\s*(?:days?|weeks?|months?))\b", re.I),
    "owner": re.compile(r"\b(?:owner|owned by|sre|platform|incident commander|on-call)\b", re.I),
    "backup_owner": re.compile(r"\b(?:backup owner|secondary|delegate|fallback)\b", re.I),
    "escalation": re.compile(r"\b(?:escalation|page|pagerduty|sev|incident)\b", re.I),
    "drill_cadence": re.compile(r"\b(?:monthly|quarterly|annually|each release|cadence|every \d+)\b", re.I),
    "scenario": re.compile(r"\b(?:scenario|region outage|database loss|dependency failure|restore|failover)\b", re.I),
    "evidence": re.compile(r"\b(?:evidence|ticket|report|log|sign-off|results?)\b", re.I),
    "audience": re.compile(r"\b(?:customers?|admins?|support|status page subscribers|stakeholders)\b", re.I),
    "channel": re.compile(r"\b(?:status page|email|slack|support macro|in-app|channel)\b", re.I),
    "timing": re.compile(r"\b(?:within \d+|before|after|during|every \d+|minutes?|hours?|timing)\b", re.I),
    "regions": re.compile(r"\b(?:us|eu|apac|region|primary|secondary|multi[- ]?region|cross[- ]?region)\b", re.I),
    "residency_rule": re.compile(r"\b(?:residency|data stays|same region|sovereign|compliance)\b", re.I),
    "failover_target": re.compile(r"\b(?:failover target|secondary region|backup region|target region|paired region)\b", re.I),
}
_REQ_RE = re.compile(r"\b(?:must|shall|required|requires?|need(?:ed|s)?|should|define|support|ensure|recover|restore|failover|notify|validate|drill)\b", re.I)
_NEGATED_RE = re.compile(r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}\b(?:disaster recovery|dr|rto|rpo|failover|backup|restore drill)\b.{0,140}\b(?:required|needed|planned|in scope|work|support)\b|\b(?:disaster recovery|dr|rto|rpo|failover|backup|restore drill)\b.{0,140}\b(?:out of scope|not required|not needed|no work|no changes?)\b", re.I)
_SCANNED_FIELDS = ("title", "summary", "body", "description", "requirements", "constraints", "scope", "acceptance_criteria", "definition_of_done", "reliability", "operations", "infrastructure", "risks", "metadata", "source_payload")


@dataclass(frozen=True, slots=True)
class SourceDisasterRecoveryRequirement:
    source_brief_id: str | None
    requirement_type: DisasterRecoveryRequirementType
    requirement_text: str
    label: str
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: DisasterRecoveryConfidence = "medium"
    readiness: DisasterRecoveryReadiness = "needs_detail"

    @property
    def category(self) -> DisasterRecoveryRequirementType:
        return self.requirement_type

    @property
    def requirement_category(self) -> DisasterRecoveryRequirementType:
        return self.requirement_type

    @property
    def missing_detail_guidance(self) -> str | None:
        return "; ".join(self.missing_details) if self.missing_details else None

    def to_dict(self) -> dict[str, Any]:
        return {"source_brief_id": self.source_brief_id, "requirement_type": self.requirement_type, "requirement_category": self.requirement_category, "requirement_text": self.requirement_text, "label": self.label, "source_field": self.source_field, "evidence": list(self.evidence), "missing_details": list(self.missing_details), "missing_detail_guidance": self.missing_detail_guidance, "confidence": self.confidence, "readiness": self.readiness}


@dataclass(frozen=True, slots=True)
class SourceDisasterRecoveryRequirementsReport:
    source_id: str | None = None
    requirements: tuple[SourceDisasterRecoveryRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceDisasterRecoveryRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceDisasterRecoveryRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {"source_id": self.source_id, "requirements": [item.to_dict() for item in self.requirements], "summary": dict(self.summary), "records": [item.to_dict() for item in self.records], "findings": [item.to_dict() for item in self.findings]}

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.requirements]

    def to_markdown(self) -> str:
        lines = [f"# Source Disaster Recovery Requirements{': ' + self.source_id if self.source_id else ''}", "", f"Requirements found: {self.summary.get('requirement_count', 0)}"]
        if not self.requirements:
            return "\n".join([*lines, "", "No disaster recovery requirements were inferred."])
        lines.extend(["", "| Type | Requirement | Missing Details | Readiness | Evidence |", "| --- | --- | --- | --- | --- |"])
        for item in self.requirements:
            lines.append(f"| {markdown_cell(item.requirement_type)} | {markdown_cell(item.requirement_text)} | {markdown_cell('; '.join(item.missing_details))} | {item.readiness} | {markdown_cell('; '.join(item.evidence))} |")
        return "\n".join(lines)


def build_source_disaster_recovery_requirements(source: Any) -> SourceDisasterRecoveryRequirementsReport:
    payloads = source_payloads(source)
    records = tuple(_merge(_candidates(payloads)))
    ids = dedupe(source_id for source_id, _ in payloads if source_id)
    return SourceDisasterRecoveryRequirementsReport(ids[0] if len(ids) == 1 else None, records, _summary(records, len(payloads)))


extract_source_disaster_recovery_requirements = build_source_disaster_recovery_requirements
generate_source_disaster_recovery_requirements = build_source_disaster_recovery_requirements
derive_source_disaster_recovery_requirements = build_source_disaster_recovery_requirements


def summarize_source_disaster_recovery_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceDisasterRecoveryRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_disaster_recovery_requirements(source_or_report).summary


def source_disaster_recovery_requirements_to_dict(report: SourceDisasterRecoveryRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_disaster_recovery_requirements_to_dict.__test__ = False


def source_disaster_recovery_requirements_to_dicts(items: SourceDisasterRecoveryRequirementsReport | Iterable[SourceDisasterRecoveryRequirement]) -> list[dict[str, Any]]:
    if isinstance(items, SourceDisasterRecoveryRequirementsReport):
        return items.to_dicts()
    return [item.to_dict() for item in items]


source_disaster_recovery_requirements_to_dicts.__test__ = False


def source_disaster_recovery_requirements_to_markdown(report: SourceDisasterRecoveryRequirementsReport) -> str:
    return report.to_markdown()


source_disaster_recovery_requirements_to_markdown.__test__ = False


def _candidates(payloads: Iterable[tuple[str | None, dict[str, Any]]]) -> list[SourceDisasterRecoveryRequirement]:
    out: list[SourceDisasterRecoveryRequirement] = []
    for source_id, payload in payloads:
        for field_name, text in segments(payload, _SCANNED_FIELDS):
            searchable = f"{field_name} {text}"
            if _NEGATED_RE.search(searchable) or not _REQ_RE.search(text):
                continue
            for requirement_type, pattern in _PATTERNS.items():
                if pattern.search(searchable):
                    missing = tuple(detail for detail in _MISSING[requirement_type] if not _DETAILS[detail].search(searchable))
                    readiness: DisasterRecoveryReadiness = "ready" if not missing else "needs_detail"
                    out.append(SourceDisasterRecoveryRequirement(source_id, requirement_type, text, _LABELS[requirement_type], field_name, (evidence_snippet(field_name, text),), missing, "high", readiness))
    return out


def _merge(candidates: Iterable[SourceDisasterRecoveryRequirement]) -> list[SourceDisasterRecoveryRequirement]:
    grouped: dict[DisasterRecoveryRequirementType, list[SourceDisasterRecoveryRequirement]] = {}
    for item in candidates:
        grouped.setdefault(item.requirement_type, []).append(item)
    records: list[SourceDisasterRecoveryRequirement] = []
    for requirement_type in _TYPE_ORDER:
        items = grouped.get(requirement_type, [])
        if not items:
            continue
        best = min(items, key=lambda item: (len(item.missing_details), item.source_field or ""))
        missing = tuple(detail for detail in _MISSING[requirement_type] if all(detail in item.missing_details for item in items))
        readiness: DisasterRecoveryReadiness = "ready" if not missing else "needs_detail"
        records.append(SourceDisasterRecoveryRequirement(best.source_brief_id, requirement_type, best.requirement_text, best.label, best.source_field, tuple(dedupe(ev for item in items for ev in item.evidence))[:5], missing, "high", readiness))
    return records


def _summary(records: tuple[SourceDisasterRecoveryRequirement, ...], source_count: int) -> dict[str, Any]:
    counts = {item: sum(1 for record in records if record.requirement_type == item) for item in _TYPE_ORDER}
    return {"source_count": source_count, "requirement_count": len(records), "requirement_type_counts": counts, "category_counts": counts, "readiness_counts": {item: sum(1 for record in records if record.readiness == item) for item in _READINESS_ORDER}, "confidence_counts": {level: sum(1 for record in records if record.confidence == level) for level in _CONFIDENCE_ORDER}, "missing_detail_count": sum(len(record.missing_details) for record in records), "requirement_types": [item for item in _TYPE_ORDER if counts[item]]}


__all__ = [name for name in globals() if name.startswith(("SourceDisasterRecovery", "build_source_disaster", "extract_source_disaster", "generate_source_disaster", "derive_source_disaster", "summarize_source_disaster", "source_disaster"))] + ["DisasterRecoveryRequirementType", "DisasterRecoveryConfidence", "DisasterRecoveryReadiness"]
