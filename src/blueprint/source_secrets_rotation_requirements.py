"""Extract source-level secrets rotation requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal

from blueprint._source_requirement_utils import dedupe, evidence_snippet, markdown_cell, segments, source_payloads

SecretsRotationRequirementType = Literal[
    "secret_types",
    "rotation_cadence",
    "ownership",
    "storage_backend",
    "rollout_coordination",
    "emergency_revocation",
    "audit_evidence",
    "consumers",
]
SecretsRotationConfidence = Literal["high", "medium", "low"]
SecretsRotationReadiness = Literal["ready", "needs_detail"]

_TYPE_ORDER: tuple[SecretsRotationRequirementType, ...] = (
    "secret_types",
    "rotation_cadence",
    "ownership",
    "storage_backend",
    "rollout_coordination",
    "emergency_revocation",
    "audit_evidence",
    "consumers",
)
_CONFIDENCE_ORDER = {"high": 0, "medium": 1, "low": 2}
_READINESS_ORDER = ("ready", "needs_detail")
_LABELS = {
    "secret_types": "Secret types",
    "rotation_cadence": "Rotation cadence",
    "ownership": "Ownership",
    "storage_backend": "Storage backend",
    "rollout_coordination": "Rollout coordination",
    "emergency_revocation": "Emergency revocation",
    "audit_evidence": "Audit evidence",
    "consumers": "Consumers",
}
_MISSING = {
    "secret_types": ("secret_type", "scope", "environment"),
    "rotation_cadence": ("cadence", "trigger", "grace_period"),
    "ownership": ("owner", "approver", "backup_owner"),
    "storage_backend": ("backend", "access_policy", "environment"),
    "rollout_coordination": ("overlap_window", "consumer_cutover", "rollback"),
    "emergency_revocation": ("revocation_trigger", "responder", "propagation_sla"),
    "audit_evidence": ("audit_log", "ticket", "actor"),
    "consumers": ("consumer_list", "notification_channel", "dependency_owner"),
}
_PATTERNS: dict[SecretsRotationRequirementType, re.Pattern[str]] = {
    "secret_types": re.compile(r"\b(?:api keys?|tokens?|certificates?|client secrets?|webhook secrets?|passwords?|credentials?)\b", re.I),
    "rotation_cadence": re.compile(r"\b(?:rotation cadence|rotate|rotated|rotation|every \d+|daily|weekly|monthly|quarterly|annually|ttl|expiry|expires?)\b", re.I),
    "ownership": re.compile(r"\b(?:owner|owned by|approver|approval|security team|platform team|sre|devops|backup owner)\b", re.I),
    "storage_backend": re.compile(r"\b(?:vault|kms|key management service|aws secrets manager|secret manager|key vault|hsm|sealed secret|parameter store)\b", re.I),
    "rollout_coordination": re.compile(r"\b(?:dual[- ]?write|overlap window|rollout|cutover|consumer cutover|grace period|rollback|phased|blue green)\b", re.I),
    "emergency_revocation": re.compile(r"\b(?:emergency revocation|revoke|revocation|disable|kill switch|compromised|incident|break[- ]?glass)\b", re.I),
    "audit_evidence": re.compile(r"\b(?:audit|evidence|log|ticket|change record|approval trail|rotation record|actor|timestamp)\b", re.I),
    "consumers": re.compile(r"\b(?:consumers?|services?|clients?|integrations?|downstream|upstream|dependency owners?|notification channel)\b", re.I),
}
_DETAILS = {
    "secret_type": _PATTERNS["secret_types"],
    "scope": re.compile(r"\b(?:service|integration|environment|tenant|workspace|prod|staging|sandbox|region)\b", re.I),
    "environment": re.compile(r"\b(?:prod|production|staging|dev|sandbox|environment|region)\b", re.I),
    "cadence": re.compile(r"\b(?:every \d+|daily|weekly|monthly|quarterly|annually|ttl|expiry|expires?|cadence|90 days?|30 days?)\b", re.I),
    "trigger": re.compile(r"\b(?:on incident|compromise|employee exit|vendor change|release|scheduled|manual trigger|automated)\b", re.I),
    "grace_period": re.compile(r"\b(?:grace period|overlap|dual[- ]?write|valid for|window|ttl)\b", re.I),
    "owner": re.compile(r"\b(?:owner|owned by|security|platform|sre|devops|team)\b", re.I),
    "approver": re.compile(r"\b(?:approver|approval|approved by|security sign-off|change approval)\b", re.I),
    "backup_owner": re.compile(r"\b(?:backup owner|secondary|delegate|fallback owner)\b", re.I),
    "backend": _PATTERNS["storage_backend"],
    "access_policy": re.compile(r"\b(?:access policy|rbac|iam|least privilege|role|permission)\b", re.I),
    "overlap_window": re.compile(r"\b(?:overlap window|dual[- ]?write|grace period|parallel|both secrets)\b", re.I),
    "consumer_cutover": re.compile(r"\b(?:consumer cutover|clients? cut over|services? migrate|rollout|phased)\b", re.I),
    "rollback": re.compile(r"\b(?:rollback|backout|restore old|fallback)\b", re.I),
    "revocation_trigger": re.compile(r"\b(?:compromise|incident|leak|emergency|break[- ]?glass|employee exit)\b", re.I),
    "responder": re.compile(r"\b(?:responder|on-call|security|sre|incident commander|owner)\b", re.I),
    "propagation_sla": re.compile(r"\b(?:immediate(?:ly)?|within \d+|sla|minutes?|hours?|propagat)\b", re.I),
    "audit_log": re.compile(r"\b(?:audit log|log|logged|siem|cloudtrail)\b", re.I),
    "ticket": re.compile(r"\b(?:ticket|change record|jira|incident|approval trail)\b", re.I),
    "actor": re.compile(r"\b(?:actor|who|rotated_by|approved_by|timestamp|user)\b", re.I),
    "consumer_list": re.compile(r"\b(?:consumer list|services?|clients?|integrations?|downstream|dependencies)\b", re.I),
    "notification_channel": re.compile(r"\b(?:slack|email|pagerduty|notification|channel|webhook)\b", re.I),
    "dependency_owner": re.compile(r"\b(?:dependency owner|service owner|consumer owner|integration owner|team)\b", re.I),
}
_REQ_RE = re.compile(r"\b(?:must|shall|required|requires?|need(?:ed|s)?|should|define|support|ensure|rotate|revoke|store|audit|notify|coordinate)\b", re.I)
_NEGATED_RE = re.compile(r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}\b(?:secret|credential|token|key|certificate|rotation|revocation|vault|kms)\b.{0,140}\b(?:required|needed|planned|in scope|work|support)\b|\b(?:secret|credential|token|key|certificate|rotation|revocation|vault|kms)\b.{0,140}\b(?:out of scope|not required|not needed|no work|no changes?)\b", re.I)
_SCANNED_FIELDS = ("title", "summary", "body", "description", "requirements", "constraints", "scope", "acceptance_criteria", "definition_of_done", "security", "infrastructure", "integrations", "operations", "metadata", "source_payload")


@dataclass(frozen=True, slots=True)
class SourceSecretsRotationRequirement:
    source_brief_id: str | None
    requirement_type: SecretsRotationRequirementType
    requirement_text: str
    label: str
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: SecretsRotationConfidence = "medium"
    readiness: SecretsRotationReadiness = "needs_detail"

    @property
    def category(self) -> SecretsRotationRequirementType:
        return self.requirement_type

    @property
    def requirement_category(self) -> SecretsRotationRequirementType:
        return self.requirement_type

    @property
    def missing_detail_guidance(self) -> str | None:
        return "; ".join(self.missing_details) if self.missing_details else None

    def to_dict(self) -> dict[str, Any]:
        return {"source_brief_id": self.source_brief_id, "requirement_type": self.requirement_type, "requirement_category": self.requirement_category, "requirement_text": self.requirement_text, "label": self.label, "source_field": self.source_field, "evidence": list(self.evidence), "missing_details": list(self.missing_details), "missing_detail_guidance": self.missing_detail_guidance, "confidence": self.confidence, "readiness": self.readiness}


@dataclass(frozen=True, slots=True)
class SourceSecretsRotationRequirementsReport:
    source_id: str | None = None
    requirements: tuple[SourceSecretsRotationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSecretsRotationRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceSecretsRotationRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {"source_id": self.source_id, "requirements": [item.to_dict() for item in self.requirements], "summary": dict(self.summary), "records": [item.to_dict() for item in self.records], "findings": [item.to_dict() for item in self.findings]}

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.requirements]

    def to_markdown(self) -> str:
        lines = [f"# Source Secrets Rotation Requirements{': ' + self.source_id if self.source_id else ''}", "", f"Requirements found: {self.summary.get('requirement_count', 0)}"]
        if not self.requirements:
            return "\n".join([*lines, "", "No secrets rotation requirements were inferred."])
        lines.extend(["", "| Type | Requirement | Missing Details | Readiness | Evidence |", "| --- | --- | --- | --- | --- |"])
        for item in self.requirements:
            lines.append(f"| {markdown_cell(item.requirement_type)} | {markdown_cell(item.requirement_text)} | {markdown_cell('; '.join(item.missing_details))} | {item.readiness} | {markdown_cell('; '.join(item.evidence))} |")
        return "\n".join(lines)


def build_source_secrets_rotation_requirements(source: Any) -> SourceSecretsRotationRequirementsReport:
    payloads = source_payloads(source)
    records = tuple(_merge(_candidates(payloads)))
    ids = dedupe(source_id for source_id, _ in payloads if source_id)
    return SourceSecretsRotationRequirementsReport(ids[0] if len(ids) == 1 else None, records, _summary(records, len(payloads)))


extract_source_secrets_rotation_requirements = build_source_secrets_rotation_requirements
generate_source_secrets_rotation_requirements = build_source_secrets_rotation_requirements
derive_source_secrets_rotation_requirements = build_source_secrets_rotation_requirements


def summarize_source_secrets_rotation_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceSecretsRotationRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_secrets_rotation_requirements(source_or_report).summary


def source_secrets_rotation_requirements_to_dict(report: SourceSecretsRotationRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_secrets_rotation_requirements_to_dict.__test__ = False


def source_secrets_rotation_requirements_to_dicts(items: SourceSecretsRotationRequirementsReport | Iterable[SourceSecretsRotationRequirement]) -> list[dict[str, Any]]:
    if isinstance(items, SourceSecretsRotationRequirementsReport):
        return items.to_dicts()
    return [item.to_dict() for item in items]


source_secrets_rotation_requirements_to_dicts.__test__ = False


def source_secrets_rotation_requirements_to_markdown(report: SourceSecretsRotationRequirementsReport) -> str:
    return report.to_markdown()


source_secrets_rotation_requirements_to_markdown.__test__ = False


def _candidates(payloads: Iterable[tuple[str | None, dict[str, Any]]]) -> list[SourceSecretsRotationRequirement]:
    out: list[SourceSecretsRotationRequirement] = []
    for source_id, payload in payloads:
        for field_name, text in segments(payload, _SCANNED_FIELDS):
            searchable = f"{field_name} {text}"
            if _NEGATED_RE.search(searchable) or not _REQ_RE.search(text):
                continue
            for requirement_type, pattern in _PATTERNS.items():
                if pattern.search(searchable):
                    missing = tuple(detail for detail in _MISSING[requirement_type] if not _DETAILS[detail].search(searchable))
                    readiness: SecretsRotationReadiness = "ready" if not missing else "needs_detail"
                    out.append(SourceSecretsRotationRequirement(source_id, requirement_type, text, _LABELS[requirement_type], field_name, (evidence_snippet(field_name, text),), missing, "high", readiness))
    return out


def _merge(candidates: Iterable[SourceSecretsRotationRequirement]) -> list[SourceSecretsRotationRequirement]:
    grouped: dict[SecretsRotationRequirementType, list[SourceSecretsRotationRequirement]] = {}
    for item in candidates:
        grouped.setdefault(item.requirement_type, []).append(item)
    records: list[SourceSecretsRotationRequirement] = []
    for requirement_type in _TYPE_ORDER:
        items = grouped.get(requirement_type, [])
        if not items:
            continue
        best = min(items, key=lambda item: (len(item.missing_details), item.source_field or ""))
        missing = tuple(detail for detail in _MISSING[requirement_type] if all(detail in item.missing_details for item in items))
        readiness: SecretsRotationReadiness = "ready" if not missing else "needs_detail"
        records.append(SourceSecretsRotationRequirement(best.source_brief_id, requirement_type, best.requirement_text, best.label, best.source_field, tuple(dedupe(ev for item in items for ev in item.evidence))[:5], missing, "high", readiness))
    return records


def _summary(records: tuple[SourceSecretsRotationRequirement, ...], source_count: int) -> dict[str, Any]:
    counts = {item: sum(1 for record in records if record.requirement_type == item) for item in _TYPE_ORDER}
    readiness_counts = {item: sum(1 for record in records if record.readiness == item) for item in _READINESS_ORDER}
    return {"source_count": source_count, "requirement_count": len(records), "requirement_type_counts": counts, "category_counts": counts, "readiness_counts": readiness_counts, "confidence_counts": {level: sum(1 for record in records if record.confidence == level) for level in _CONFIDENCE_ORDER}, "missing_detail_count": sum(len(record.missing_details) for record in records), "missing_field_counts": {item: sum(1 for record in records if item in record.missing_details) for fields in _MISSING.values() for item in fields}, "requirement_types": [item for item in _TYPE_ORDER if counts[item]]}


__all__ = [name for name in globals() if name.startswith(("SourceSecretsRotation", "build_source_secrets", "extract_source_secrets", "generate_source_secrets", "derive_source_secrets", "summarize_source_secrets", "source_secrets"))] + ["SecretsRotationRequirementType", "SecretsRotationConfidence", "SecretsRotationReadiness"]
