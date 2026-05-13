"""Extract source-level data loss prevention requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


DataLossPreventionRequirementType = Literal[
    "sensitive_data_discovery",
    "outbound_sharing_controls",
    "clipboard_download_restrictions",
    "watermarking",
    "alerting",
    "exception_approval",
]
DataLossPreventionConfidence = Literal["high", "medium", "low"]
DataLossPreventionReadiness = Literal["ready", "needs_detail"]

_TYPE_ORDER: tuple[DataLossPreventionRequirementType, ...] = (
    "sensitive_data_discovery",
    "outbound_sharing_controls",
    "clipboard_download_restrictions",
    "watermarking",
    "alerting",
    "exception_approval",
)
_CONFIDENCE_ORDER: dict[DataLossPreventionConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_LABELS: dict[DataLossPreventionRequirementType, str] = {
    "sensitive_data_discovery": "Sensitive data discovery",
    "outbound_sharing_controls": "Outbound sharing controls",
    "clipboard_download_restrictions": "Clipboard and download restrictions",
    "watermarking": "Watermarking",
    "alerting": "Alerting",
    "exception_approval": "Exception approval",
}
_PLANNING_NOTES: dict[DataLossPreventionRequirementType, str] = {
    "sensitive_data_discovery": "Classify protected data types, scan locations, match confidence, and remediation ownership.",
    "outbound_sharing_controls": "Define channels, recipients, policy actions, user messaging, and audit evidence.",
    "clipboard_download_restrictions": "Define copy, paste, export, print, screenshot, and download restrictions by role and data class.",
    "watermarking": "Define visible and forensic watermark content, placement, scope, and exception behavior.",
    "alerting": "Define DLP alert destinations, severity, thresholds, escalation, and triage ownership.",
    "exception_approval": "Define exception request, approver, expiry, evidence, and review cadence.",
}
_MISSING_BY_TYPE: dict[DataLossPreventionRequirementType, tuple[str, ...]] = {
    "sensitive_data_discovery": ("data_classes", "scan_scope", "owner"),
    "outbound_sharing_controls": ("channels", "policy_action", "allowed_recipients"),
    "clipboard_download_restrictions": ("restricted_actions", "roles", "enforcement_surface"),
    "watermarking": ("watermark_content", "surfaces", "exception_behavior"),
    "alerting": ("alert_destination", "severity", "triage_owner"),
    "exception_approval": ("approver", "expiry", "audit_evidence"),
}
_PATTERNS: dict[DataLossPreventionRequirementType, re.Pattern[str]] = {
    "sensitive_data_discovery": re.compile(
        r"\b(?:sensitive data discovery|discover sensitive data|data discovery|classif(?:y|ication)|"
        r"detect (?:pii|phi|pci|secrets?|credentials?)|scan for (?:pii|phi|pci|secrets?|credentials?)|"
        r"scan (?:uploads?|documents?|repositories?|databases?) for (?:pii|phi|pci|secrets?|credentials?))\b",
        re.I,
    ),
    "outbound_sharing_controls": re.compile(
        r"\b(?:outbound sharing|external sharing|public sharing|share links?|egress|exfiltrat(?:e|ion)|"
        r"send outside|external recipients?|block sharing|sharing controls?|prevent data loss)\b",
        re.I,
    ),
    "clipboard_download_restrictions": re.compile(
        r"\b(?:clipboard|copy/paste|copy paste|disable copy|restrict downloads?|download restrictions?|"
        r"clipboard and download restrictions|block (?:copy|paste|download|export|print|screenshot)|"
        r"disable (?:copy|paste|download|export|print|screenshot)|prevent (?:copy|paste|download|export|print|screenshot))\b",
        re.I,
    ),
    "watermarking": re.compile(r"\b(?:watermark(?:s|ing)?|forensic mark|visible mark|user stamp)\b", re.I),
    "alerting": re.compile(
        r"\b(?:dlp alert(?:s|ing)?|security alert(?:s)?|alert on|notify security|incident alert|"
        r"siem|pagerduty|slack alert|escalat(?:e|ion))\b",
        re.I,
    ),
    "exception_approval": re.compile(
        r"\b(?:exception approval|approved exception|exception request|temporary exception|policy exception|"
        r"override approval|allowlist approval|break[- ]?glass)\b",
        re.I,
    ),
}
_DETAIL_PATTERNS: dict[str, re.Pattern[str]] = {
    "data_classes": re.compile(r"\b(?:pii|phi|pci|secrets?|credentials?|ssn|email addresses?|credit cards?|personal data)\b", re.I),
    "scan_scope": re.compile(r"\b(?:scan|classify|discover|bucket|drive|workspace|repository|database|document|attachment|uploads?)\b", re.I),
    "owner": re.compile(r"\b(?:owner|owned by|reviewed by|triage owner|security|privacy|compliance|legal)\b", re.I),
    "channels": re.compile(r"\b(?:email|slack|share links?|external recipients?|download|export|api|webhook|public link)\b", re.I),
    "policy_action": re.compile(r"\b(?:block|quarantine|warn|redact|mask|allow|deny|restrict|prevent)\b", re.I),
    "allowed_recipients": re.compile(r"\b(?:allowed recipients?|approved domains?|trusted domains?|internal only|allowlist|whitelist)\b", re.I),
    "restricted_actions": re.compile(r"\b(?:copy|paste|clipboard|download|export|print|screenshot|screen capture)\b", re.I),
    "roles": re.compile(r"\b(?:role|admin|viewer|agent|contractor|employee|support|by group|rbac)\b", re.I),
    "enforcement_surface": re.compile(r"\b(?:browser|desktop|mobile|admin console|app|endpoint|workspace|portal)\b", re.I),
    "watermark_content": re.compile(r"\b(?:user id|email|timestamp|ip address|account id|tenant id|case id)\b", re.I),
    "surfaces": re.compile(r"\b(?:pdf|export|download|screen|preview|viewer|report|document|attachment)\b", re.I),
    "exception_behavior": re.compile(r"\b(?:exception|override|allowlist|expires?|fallback|bypass)\b", re.I),
    "alert_destination": re.compile(r"\b(?:siem|slack|pagerduty|email|security queue|incident queue|webhook)\b", re.I),
    "severity": re.compile(r"\b(?:severity|critical|high|medium|low|threshold|priority)\b", re.I),
    "triage_owner": re.compile(r"\b(?:triage owner|security|privacy|compliance|soc|on-call|incident commander)\b", re.I),
    "approver": re.compile(r"\b(?:approver|approved by|security approval|legal approval|manager approval|compliance approval)\b", re.I),
    "expiry": re.compile(r"\b(?:expire|expiry|ttl|until|temporary|review cadence|renewal)\b", re.I),
    "audit_evidence": re.compile(r"\b(?:audit|evidence|log|ticket|record|approval trail|change history)\b", re.I),
}
_CONTEXT_RE = re.compile(
    r"\b(?:dlp|data loss prevention|sensitive data|pii|phi|pci|secrets?|exfiltrat|outbound sharing|"
    r"clipboard|download|watermark(?:s|ing)?|security alert|exception approval)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|define|support|"
    r"block|restrict|prevent|detect|classify|scan|watermark|alert|approve|approval|audit|done when)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}\b(?:dlp|data loss prevention|"
    r"sensitive data|outbound sharing|clipboard|download|watermark|exception approval)\b.{0,120}"
    r"\b(?:required|needed|in scope|planned|changes?|work|support)\b|"
    r"\b(?:dlp|data loss prevention|sensitive data|outbound sharing|clipboard|download|watermark|"
    r"exception approval)\b.{0,120}\b(?:out of scope|not required|not needed|no work|no changes?|non[- ]?goal)\b",
    re.I,
)
_SCANNED_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "problem_statement",
    "mvp_goal",
    "workflow_context",
    "requirements",
    "constraints",
    "scope",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "risks",
    "security",
    "privacy",
    "compliance",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_SPACE_RE = re.compile(r"\s+")
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")


@dataclass(frozen=True, slots=True)
class SourceDataLossPreventionRequirement:
    """One source-backed data loss prevention requirement."""

    source_brief_id: str | None
    requirement_type: DataLossPreventionRequirementType
    requirement_text: str
    label: str
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: DataLossPreventionConfidence = "medium"
    readiness: DataLossPreventionReadiness = "needs_detail"
    planning_note: str = ""

    @property
    def category(self) -> DataLossPreventionRequirementType:
        return self.requirement_type

    @property
    def requirement_category(self) -> DataLossPreventionRequirementType:
        return self.requirement_type

    @property
    def data_loss_prevention_category(self) -> DataLossPreventionRequirementType:
        return self.requirement_type

    @property
    def missing_detail_guidance(self) -> str | None:
        return "; ".join(self.missing_details) if self.missing_details else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "requirement_category": self.requirement_category,
            "requirement_text": self.requirement_text,
            "label": self.label,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "missing_details": list(self.missing_details),
            "missing_detail_guidance": self.missing_detail_guidance,
            "confidence": self.confidence,
            "readiness": self.readiness,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceDataLossPreventionRequirementsReport:
    """Source-level data loss prevention requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceDataLossPreventionRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceDataLossPreventionRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceDataLossPreventionRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source Data Loss Prevention Requirements"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("requirement_type_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement type counts: "
            + ", ".join(f"{item} {type_counts.get(item, 0)}" for item in _TYPE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No data loss prevention requirements were inferred."])
            return "\n".join(lines)
        lines.extend(
            [
                "",
                "| Type | Requirement | Missing Details | Confidence | Readiness | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.requirement_type)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell('; '.join(requirement.missing_details))} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.readiness)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_data_loss_prevention_requirements(source: Any) -> SourceDataLossPreventionRequirementsReport:
    """Extract source-level data loss prevention requirement records from brief-shaped input."""
    payloads = _source_payloads(source)
    requirements = tuple(_merge(_candidates(payloads)))
    source_ids = _dedupe(source_id for source_id, _ in payloads if source_id)
    return SourceDataLossPreventionRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(payloads)),
    )


def extract_source_data_loss_prevention_requirements(source: Any) -> SourceDataLossPreventionRequirementsReport:
    return build_source_data_loss_prevention_requirements(source)


def generate_source_data_loss_prevention_requirements(source: Any) -> SourceDataLossPreventionRequirementsReport:
    return build_source_data_loss_prevention_requirements(source)


def derive_source_data_loss_prevention_requirements(source: Any) -> SourceDataLossPreventionRequirementsReport:
    return build_source_data_loss_prevention_requirements(source)


def summarize_source_data_loss_prevention_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceDataLossPreventionRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_data_loss_prevention_requirements(source_or_result).summary


def source_data_loss_prevention_requirements_to_dict(
    report: SourceDataLossPreventionRequirementsReport,
) -> dict[str, Any]:
    return report.to_dict()


source_data_loss_prevention_requirements_to_dict.__test__ = False


def source_data_loss_prevention_requirements_to_dicts(
    requirements: SourceDataLossPreventionRequirementsReport | Iterable[SourceDataLossPreventionRequirement],
) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceDataLossPreventionRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_data_loss_prevention_requirements_to_dicts.__test__ = False


def source_data_loss_prevention_requirements_to_markdown(
    report: SourceDataLossPreventionRequirementsReport,
) -> str:
    return report.to_markdown()


source_data_loss_prevention_requirements_to_markdown.__test__ = False


def _source_payloads(source: Any) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Any) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = {
            key: getattr(source, key)
            for key in dir(source)
            if not key.startswith("_") and not callable(getattr(source, key, None))
        }
        return _source_id(payload), payload
    return None, {}


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return _optional_text(payload.get("id")) or _optional_text(payload.get("source_brief_id")) or _optional_text(payload.get("source_id"))


def _candidates(payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[SourceDataLossPreventionRequirement]:
    found: list[SourceDataLossPreventionRequirement] = []
    for source_id, payload in payloads:
        for source_field, text in _segments(payload):
            searchable = f"{source_field} {text}"
            if _NEGATED_RE.search(searchable) or not _CONTEXT_RE.search(searchable):
                continue
            for requirement_type, pattern in _PATTERNS.items():
                if not pattern.search(searchable):
                    continue
                missing = tuple(detail for detail in _MISSING_BY_TYPE[requirement_type] if not _DETAIL_PATTERNS[detail].search(searchable))
                found.append(
                    SourceDataLossPreventionRequirement(
                        source_brief_id=source_id,
                        requirement_type=requirement_type,
                        requirement_text=text,
                        label=_LABELS[requirement_type],
                        source_field=source_field,
                        evidence=(_evidence_snippet(source_field, text),),
                        missing_details=missing,
                        confidence="high" if _REQUIREMENT_RE.search(text) else "medium",
                        readiness="ready" if not missing else "needs_detail",
                        planning_note=_PLANNING_NOTES[requirement_type],
                    )
                )
    return found


def _merge(candidates: Iterable[SourceDataLossPreventionRequirement]) -> list[SourceDataLossPreventionRequirement]:
    grouped: dict[DataLossPreventionRequirementType, list[SourceDataLossPreventionRequirement]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.requirement_type, []).append(candidate)
    merged: list[SourceDataLossPreventionRequirement] = []
    for requirement_type in _TYPE_ORDER:
        items = grouped.get(requirement_type, [])
        if not items:
            continue
        best = min(items, key=lambda item: (_CONFIDENCE_ORDER[item.confidence], len(item.missing_details), item.source_field or ""))
        all_missing = tuple(detail for detail in _MISSING_BY_TYPE[requirement_type] if all(detail in item.missing_details for item in items))
        merged.append(
            SourceDataLossPreventionRequirement(
                source_brief_id=best.source_brief_id,
                requirement_type=requirement_type,
                requirement_text=best.requirement_text,
                label=best.label,
                source_field=best.source_field,
                evidence=tuple(_dedupe(evidence for item in items for evidence in item.evidence))[:5],
                missing_details=all_missing,
                confidence=min((item.confidence for item in items), key=_CONFIDENCE_ORDER.__getitem__),
                readiness="ready" if not all_missing else "needs_detail",
                planning_note=best.planning_note,
            )
        )
    return merged


def _segments(payload: Mapping[str, Any], prefix: str | None = None) -> list[tuple[str, str]]:
    segments: list[tuple[str, str]] = []
    items = payload.items() if prefix else ((key, payload[key]) for key in _SCANNED_FIELDS if key in payload)
    for key, value in items:
        field = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, str):
            for part in _SPLIT_RE.split(value):
                if text := _clean(part):
                    segments.append((field, text))
        elif isinstance(value, Mapping):
            segments.extend(_segments(value, field))
        elif isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, str)):
            for index, item in enumerate(value):
                child_field = f"{field}[{index}]"
                if isinstance(item, str):
                    if text := _clean(item):
                        segments.append((child_field, text))
                elif isinstance(item, Mapping):
                    segments.extend(_segments(item, child_field))
    return segments


def _summary(requirements: tuple[SourceDataLossPreventionRequirement, ...], source_count: int) -> dict[str, Any]:
    type_counts = {item: 0 for item in _TYPE_ORDER}
    confidence_counts = {item: 0 for item in _CONFIDENCE_ORDER}
    readiness_counts = {"ready": 0, "needs_detail": 0}
    for requirement in requirements:
        type_counts[requirement.requirement_type] += 1
        confidence_counts[requirement.confidence] += 1
        readiness_counts[requirement.readiness] += 1
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_type_counts": type_counts,
        "confidence_counts": confidence_counts,
        "readiness_counts": readiness_counts,
        "requirement_types": [item for item in _TYPE_ORDER if type_counts[item]],
        "missing_detail_count": sum(len(requirement.missing_details) for requirement in requirements),
    }


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _clean(str(value))
    return text or None


def _clean(value: str) -> str:
    return _SPACE_RE.sub(" ", value.strip(" \t\r\n-*+")).strip()


def _dedupe(values: Iterable[str | None]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _markdown_cell(value: str) -> str:
    return _clean(value).replace("|", "\\|").replace("\n", " ")


__all__ = [
    "DataLossPreventionConfidence",
    "DataLossPreventionReadiness",
    "DataLossPreventionRequirementType",
    "SourceDataLossPreventionRequirement",
    "SourceDataLossPreventionRequirementsReport",
    "build_source_data_loss_prevention_requirements",
    "derive_source_data_loss_prevention_requirements",
    "extract_source_data_loss_prevention_requirements",
    "generate_source_data_loss_prevention_requirements",
    "source_data_loss_prevention_requirements_to_dict",
    "source_data_loss_prevention_requirements_to_dicts",
    "source_data_loss_prevention_requirements_to_markdown",
    "summarize_source_data_loss_prevention_requirements",
]
