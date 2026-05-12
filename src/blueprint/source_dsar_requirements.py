"""Extract source-level DSAR requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping


DsarRequirementCategory = Literal[
    "requester_verification",
    "fulfillment_sla",
    "data_scope",
    "delivery_format",
    "audit_evidence",
    "exception_redaction",
]

_CATEGORY_ORDER: tuple[DsarRequirementCategory, ...] = (
    "requester_verification",
    "fulfillment_sla",
    "data_scope",
    "delivery_format",
    "audit_evidence",
    "exception_redaction",
)
_SPACE_RE = re.compile(r"\s+")
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_DSAR_RE = re.compile(
    r"\b(?:dsar|data subject access request|subject access request|access request|right of access|"
    r"privacy request|personal data request|export my data|download my data|data export request|"
    r"copy of (?:my|their) data|gdpr access|ccpa access)\b",
    re.I,
)
_NO_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,90}\b(?:dsar|data subject access|access request|privacy request|data export)\b"
    r".{0,90}\b(?:scope|required|needed|changes?|impact)\b",
    re.I,
)
_FIELD_CONTEXT_RE = re.compile(r"(?:privacy|dsar|access|export|support|compliance|request|requirements?|acceptance)", re.I)
_CATEGORY_PATTERNS: dict[DsarRequirementCategory, re.Pattern[str]] = {
    "requester_verification": re.compile(
        r"\b(?:verify|verification|authenticate|identity proof|requester identity|authorized agent|"
        r"account ownership|email confirmation|government id)\b",
        re.I,
    ),
    "fulfillment_sla": re.compile(
        r"\b(?:sla|within|deadline|due within|fulfill(?:ment)?|respond within|complete within|"
        r"\d+\s*(?:calendar\s*)?(?:days?|hours?)|30 days|45 days)\b",
        re.I,
    ),
    "data_scope": re.compile(
        r"\b(?:data scope|scope of data|personal data|profile data|account data|billing data|"
        r"support tickets|activity logs|include|exclude|systems of record)\b",
        re.I,
    ),
    "delivery_format": re.compile(
        r"\b(?:delivery|deliver|export format|download link|secure link|csv|json|pdf|zip|"
        r"machine[- ]readable|encrypted archive|email)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit|evidence|case log|request log|proof of fulfillment|timestamp|receipt|"
        r"support ticket|compliance record)\b",
        re.I,
    ),
    "exception_redaction": re.compile(
        r"\b(?:exception|exemption|redact|redaction|withhold|deny|legal hold|fraud|security|"
        r"third[- ]party|trade secret|sensitive)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[DsarRequirementCategory, str] = {
    "requester_verification": "privacy_operations",
    "fulfillment_sla": "privacy_operations",
    "data_scope": "data_platform",
    "delivery_format": "product_engineering",
    "audit_evidence": "compliance",
    "exception_redaction": "legal_privacy",
}
_NOTE_BY_CATEGORY: dict[DsarRequirementCategory, str] = {
    "requester_verification": "Define identity and authorized-agent checks before disclosing personal data.",
    "fulfillment_sla": "Capture DSAR response deadlines, extension rules, and operational ownership.",
    "data_scope": "Inventory included systems, data classes, exclusions, and ownership boundaries.",
    "delivery_format": "Specify secure export packaging, format, and delivery channel.",
    "audit_evidence": "Record request intake, decisions, fulfillment timestamps, and reviewer evidence.",
    "exception_redaction": "Define exception, denial, and redaction handling with legal review.",
}


@dataclass(frozen=True, slots=True)
class SourceDsarRequirement:
    """One source-backed DSAR requirement."""

    category: DsarRequirementCategory
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
class SourceDsarRequirementsReport:
    """Source-level DSAR requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceDsarRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceDsarRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source DSAR Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        lines = [title, "", "## Summary", "", f"- Requirements found: {self.summary.get('requirement_count', 0)}"]
        if not self.requirements:
            lines.extend(["", "No DSAR requirements were found in the source brief."])
            return "\n".join(lines)
        lines.extend(["", "| Category | Confidence | Evidence |", "| --- | --- | --- |"])
        for requirement in self.requirements:
            lines.append(
                f"| {requirement.category} | {requirement.confidence:.2f} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_dsar_requirements(source: Any) -> SourceDsarRequirementsReport:
    source_id, payload = _payload(source)
    buckets: dict[DsarRequirementCategory, list[str]] = {category: [] for category in _CATEGORY_ORDER}
    for field, text in _texts(payload):
        if _NO_SCOPE_RE.search(text):
            return SourceDsarRequirementsReport(source_id=source_id, summary=_summary(()))
        searchable = f"{field.replace('_', ' ')} {text}"
        has_context = bool(_DSAR_RE.search(searchable) or _FIELD_CONTEXT_RE.search(field))
        if not has_context:
            continue
        for category, pattern in _CATEGORY_PATTERNS.items():
            if pattern.search(searchable):
                buckets[category].append(f"{field}: {text}")

    requirements = tuple(
        SourceDsarRequirement(
            category=category,
            confidence=0.95 if any(_DSAR_RE.search(item) for item in evidence) else 0.8,
            evidence=tuple(_dedupe(evidence))[:5],
            suggested_owner=_OWNER_BY_CATEGORY[category],
            suggested_planning_note=_NOTE_BY_CATEGORY[category],
        )
        for category in _CATEGORY_ORDER
        if (evidence := buckets[category])
    )
    return SourceDsarRequirementsReport(source_id=source_id, requirements=requirements, summary=_summary(requirements))


def generate_source_dsar_requirements(source: Any) -> SourceDsarRequirementsReport:
    return build_source_dsar_requirements(source)


def derive_source_dsar_requirements(source: Any) -> SourceDsarRequirementsReport:
    return build_source_dsar_requirements(source)


def extract_source_dsar_requirements(source: Any) -> tuple[SourceDsarRequirement, ...]:
    return build_source_dsar_requirements(source).requirements


def summarize_source_dsar_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceDsarRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_dsar_requirements(source_or_report).summary


def source_dsar_requirements_to_dict(report: SourceDsarRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_dsar_requirements_to_dict.__test__ = False


def source_dsar_requirements_to_dicts(
    requirements: SourceDsarRequirementsReport | Iterable[SourceDsarRequirement],
) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceDsarRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_dsar_requirements_to_dicts.__test__ = False


def source_dsar_requirements_to_markdown(report: SourceDsarRequirementsReport) -> str:
    return report.to_markdown()


source_dsar_requirements_to_markdown.__test__ = False


def _payload(source: Any) -> tuple[str | None, Mapping[str, Any]]:
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
    elif isinstance(source, Mapping):
        payload = dict(source)
    elif not isinstance(source, (str, bytes, bytearray)):
        payload = {key: getattr(source, key) for key in dir(source) if not key.startswith("_") and not callable(getattr(source, key))}
    else:
        payload = {"body": source}
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


def _summary(requirements: tuple[SourceDsarRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "categories": [requirement.category for requirement in requirements],
        "category_counts": {category: sum(1 for requirement in requirements if requirement.category == category) for category in _CATEGORY_ORDER},
        "high_confidence_count": sum(1 for requirement in requirements if requirement.confidence >= 0.9),
    }


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")
