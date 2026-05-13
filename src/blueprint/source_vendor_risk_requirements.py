"""Extract source-level third-party vendor-risk requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping


VendorRiskRequirementCategory = Literal[
    "security_review",
    "data_processing_terms",
    "compliance_attestation",
    "uptime_sla_dependency",
    "exit_plan",
    "subprocessors",
    "procurement_approval",
    "incident_notification",
]

_CATEGORY_ORDER: tuple[VendorRiskRequirementCategory, ...] = (
    "security_review",
    "data_processing_terms",
    "compliance_attestation",
    "uptime_sla_dependency",
    "exit_plan",
    "subprocessors",
    "procurement_approval",
    "incident_notification",
)
_SPACE_RE = re.compile(r"\s+")
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CONTEXT_RE = re.compile(
    r"\b(?:vendor risk|third[- ]party risk|supplier risk|vendor review|vendor assessment|"
    r"processor|subprocessor|external vendor|third[- ]party vendor|data processing agreement|dpa|"
    r"soc ?2|iso ?27001|security questionnaire|sla|service level|exit plan|contingency|"
    r"procurement approval|incident notification)\b",
    re.I,
)
_FIELD_CONTEXT_RE = re.compile(
    r"(?:vendor|third[-_ ]?party|supplier|processor|subprocessor|risk|procurement|security|compliance|"
    r"privacy|dpa|sla|incident|requirements?|constraints?|acceptance|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|before launch|"
    r"before go[- ]?live|cannot launch|cannot ship|blocked|blocks?|gate|gated|approval|approve|"
    r"review|attestation|signed|contract|notify|notification|plan|contingency|exit)\b",
    re.I,
)
_NO_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope)\b.{0,120}\b(?:vendor risk|third[- ]party risk|vendor review|"
    r"security review|dpa|subprocessor|procurement approval|sla|exit plan)\b"
    r".{0,120}\b(?:required|needed|scope|changes?|work|planned)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[VendorRiskRequirementCategory, re.Pattern[str]] = {
    "security_review": re.compile(
        r"\b(?:security review|infosec review|security questionnaire|security assessment|vendor risk review|"
        r"risk assessment|threat review|penetration test|pen test|security approval)\b",
        re.I,
    ),
    "data_processing_terms": re.compile(
        r"\b(?:data processing agreement|dpa|data protection agreement|privacy addendum|processor agreement|"
        r"data processing terms|standard contractual clauses|sccs?|data transfer terms|dtia)\b",
        re.I,
    ),
    "compliance_attestation": re.compile(
        r"\b(?:soc ?2|iso ?27001|hipaa|pci|gdpr|ccpa|compliance attestation|attestation|certificate|"
        r"certification|audit report|bridge letter)\b",
        re.I,
    ),
    "uptime_sla_dependency": re.compile(
        r"\b(?:sla|service level agreement|uptime|availability|support response|response time|service credit|"
        r"rto|rpo|vendor outage|dependency uptime)\b",
        re.I,
    ),
    "exit_plan": re.compile(
        r"\b(?:exit plan|exit strategy|contingency plan|fallback provider|vendor replacement|offboarding|"
        r"termination plan|transition plan|data return|data deletion on termination)\b",
        re.I,
    ),
    "subprocessors": re.compile(
        r"\b(?:subprocessor|subprocessors|downstream processor|processor list|subcontractor|vendor subprocessors)\b",
        re.I,
    ),
    "procurement_approval": re.compile(
        r"\b(?:procurement approval|procurement review|supplier approval|vendor onboarding|purchase approval|"
        r"finance approval|legal approval|approval gate|vendor approved)\b",
        re.I,
    ),
    "incident_notification": re.compile(
        r"\b(?:incident notification|breach notification|security incident|vendor incident|notify within|"
        r"notification window|incident notice|subprocessor incident)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[VendorRiskRequirementCategory, str] = {
    "security_review": "security",
    "data_processing_terms": "legal_privacy",
    "compliance_attestation": "compliance",
    "uptime_sla_dependency": "vendor_management",
    "exit_plan": "product_operations",
    "subprocessors": "privacy",
    "procurement_approval": "procurement",
    "incident_notification": "security_operations",
}
_NOTE_BY_CATEGORY: dict[VendorRiskRequirementCategory, str] = {
    "security_review": "Capture security review scope, questionnaire evidence, risk owner, and approval gate.",
    "data_processing_terms": "Resolve DPA, processor terms, transfer terms, and privacy review before data sharing.",
    "compliance_attestation": "Collect required SOC 2, ISO, PCI, HIPAA, or equivalent compliance evidence.",
    "uptime_sla_dependency": "Treat vendor uptime, support response, RTO/RPO, and service credits as planning constraints.",
    "exit_plan": "Define contingency, vendor replacement, data export, and termination handling.",
    "subprocessors": "Review subprocessor list, approval workflow, notification terms, and data locations.",
    "procurement_approval": "Track procurement, supplier onboarding, finance, and legal approval dependencies.",
    "incident_notification": "Document breach or security-incident notice window, recipient, and escalation path.",
}


@dataclass(frozen=True, slots=True)
class SourceVendorRiskRequirement:
    """One source-backed vendor-risk requirement."""

    category: VendorRiskRequirementCategory
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_owner: str = ""
    suggested_planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "suggested_owner": self.suggested_owner,
            "suggested_planning_note": self.suggested_planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceVendorRiskRequirementsReport:
    """Source-level vendor-risk requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceVendorRiskRequirement, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceVendorRiskRequirement, ...]:
        return self.requirements

    @property
    def gaps(self) -> tuple[str, ...]:
        return self.gap_messages

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "gap_messages": list(self.gap_messages),
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "gaps": list(self.gaps),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source Vendor Risk Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Gap count: {self.summary.get('gap_count', 0)}",
        ]
        if self.gap_messages:
            lines.extend(["", "## Gaps", ""])
            lines.extend(f"- {message}" for message in self.gap_messages)
        if not self.requirements:
            lines.extend(["", "No source vendor risk requirements were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Category | Confidence | Owner | Evidence | Planning Note |", "| --- | --- | --- | --- | --- |"])
        for requirement in self.requirements:
            lines.append(
                f"| {requirement.category} | {requirement.confidence:.2f} | {requirement.suggested_owner} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.suggested_planning_note)} |"
            )
        return "\n".join(lines)


def build_source_vendor_risk_requirements(source: Any) -> SourceVendorRiskRequirementsReport:
    source_id, payload = _payload(source)
    buckets: dict[VendorRiskRequirementCategory, list[str]] = {category: [] for category in _CATEGORY_ORDER}
    had_vendor_risk_context = False
    for field, text in _texts(payload):
        if _NO_SCOPE_RE.search(text):
            return SourceVendorRiskRequirementsReport(source_id=source_id, summary=_summary((), ()))
        searchable = f"{field.replace('_', ' ')} {text}"
        has_context = bool(_CONTEXT_RE.search(searchable) or _FIELD_CONTEXT_RE.search(field))
        has_requirement = bool(_REQUIREMENT_RE.search(searchable))
        if not (has_context and has_requirement):
            continue
        had_vendor_risk_context = True
        for category, pattern in _CATEGORY_PATTERNS.items():
            if pattern.search(searchable):
                buckets[category].append(f"{field}: {text}")

    requirements = tuple(
        SourceVendorRiskRequirement(
            category=category,
            confidence=0.95 if any(_CONTEXT_RE.search(item) for item in evidence) else 0.8,
            evidence=tuple(_dedupe(evidence))[:5],
            suggested_owner=_OWNER_BY_CATEGORY[category],
            suggested_planning_note=_NOTE_BY_CATEGORY[category],
        )
        for category in _CATEGORY_ORDER
        if (evidence := buckets[category])
    )
    gap_messages = _gap_messages(requirements, had_vendor_risk_context)
    return SourceVendorRiskRequirementsReport(
        source_id=source_id,
        requirements=requirements,
        gap_messages=gap_messages,
        summary=_summary(requirements, gap_messages),
    )


def generate_source_vendor_risk_requirements(source: Any) -> SourceVendorRiskRequirementsReport:
    return build_source_vendor_risk_requirements(source)


def derive_source_vendor_risk_requirements(source: Any) -> SourceVendorRiskRequirementsReport:
    return build_source_vendor_risk_requirements(source)


def extract_source_vendor_risk_requirements(source: Any) -> tuple[SourceVendorRiskRequirement, ...]:
    return build_source_vendor_risk_requirements(source).requirements


def summarize_source_vendor_risk_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceVendorRiskRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_vendor_risk_requirements(source_or_report).summary


def source_vendor_risk_requirements_to_dict(report: SourceVendorRiskRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_vendor_risk_requirements_to_dict.__test__ = False


def source_vendor_risk_requirements_to_dicts(
    requirements: SourceVendorRiskRequirementsReport | Iterable[SourceVendorRiskRequirement],
) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceVendorRiskRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_vendor_risk_requirements_to_dicts.__test__ = False


def source_vendor_risk_requirements_to_markdown(report: SourceVendorRiskRequirementsReport) -> str:
    return report.to_markdown()


source_vendor_risk_requirements_to_markdown.__test__ = False


def _payload(source: Any) -> tuple[str | None, Mapping[str, Any]]:
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
    elif isinstance(source, Mapping):
        payload = dict(source)
    elif not isinstance(source, (str, bytes, bytearray)):
        payload = {key: getattr(source, key) for key in dir(source) if not key.startswith("_") and not callable(getattr(source, key))}
    elif isinstance(source, str):
        payload = {"body": source}
    else:
        payload = {}
    return _optional(payload.get("id") or payload.get("source_id") or payload.get("source_brief_id")), payload


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


def _gap_messages(
    requirements: tuple[SourceVendorRiskRequirement, ...],
    had_vendor_risk_context: bool,
) -> tuple[str, ...]:
    categories = {requirement.category for requirement in requirements}
    messages: list[str] = []
    if not requirements:
        return ()
    if had_vendor_risk_context and not ({"security_review", "compliance_attestation"} & categories):
        messages.append("Missing security or compliance review details for the vendor risk requirement.")
    if "exit_plan" not in categories:
        messages.append("Missing exit or contingency strategy for the vendor dependency.")
    return tuple(messages)


def _summary(
    requirements: tuple[SourceVendorRiskRequirement, ...],
    gap_messages: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "categories": [requirement.category for requirement in requirements],
        "category_counts": {category: sum(1 for requirement in requirements if requirement.category == category) for category in _CATEGORY_ORDER},
        "high_confidence_count": sum(1 for requirement in requirements if requirement.confidence >= 0.9),
        "gap_count": len(gap_messages),
        "gap_messages": list(gap_messages),
    }


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")
