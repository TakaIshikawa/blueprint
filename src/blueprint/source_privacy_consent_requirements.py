"""Extract source-level privacy consent requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


PrivacyConsentRequirementType = Literal[
    "consent_collection",
    "withdrawal",
    "purpose_binding",
    "regional_policy",
    "audit_evidence",
]
PrivacyConsentConfidence = Literal["high", "medium", "low"]

_TYPE_ORDER: tuple[PrivacyConsentRequirementType, ...] = (
    "consent_collection",
    "withdrawal",
    "purpose_binding",
    "regional_policy",
    "audit_evidence",
)
_CONFIDENCE_ORDER: dict[PrivacyConsentConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_LABELS: dict[PrivacyConsentRequirementType, str] = {
    "consent_collection": "Consent collection",
    "withdrawal": "Withdrawal and revocation",
    "purpose_binding": "Purpose binding",
    "regional_policy": "Regional policy",
    "audit_evidence": "Audit evidence",
}
_GUIDANCE: dict[PrivacyConsentRequirementType, str] = {
    "consent_collection": "Confirm where consent is collected, the affirmative action, and default state.",
    "withdrawal": "Confirm how users withdraw consent and how revocation propagates.",
    "purpose_binding": "Confirm each processing purpose and how consent is bound to that purpose.",
    "regional_policy": "Confirm jurisdiction-specific consent rules, policy copy, and fallback behavior.",
    "audit_evidence": "Confirm evidence schema, retention, export, and compliance review ownership.",
}
_TYPE_PATTERNS: dict[PrivacyConsentRequirementType, re.Pattern[str]] = {
    "consent_collection": re.compile(
        r"\b(?:collect|capture|obtain|request|ask for|record|gather|prompt for)\b.{0,80}"
        r"\b(?:consent|permission|authorization|opt[- ]?in)\b|"
        r"\b(?:consent|permission|authorization|opt[- ]?in)\b.{0,80}"
        r"\b(?:checkbox|toggle|form|modal|prompt|affirmative|explicit|collected|captured|recorded)\b",
        re.I,
    ),
    "withdrawal": re.compile(
        r"\b(?:withdraw(?:al)?|revoke|revocation|remove consent|opt[- ]?out|unsubscribe|"
        r"stop processing|change consent|consent cancellation)\b",
        re.I,
    ),
    "purpose_binding": re.compile(
        r"\b(?:purpose[- ]specific|processing purpose|purpose binding|purpose limitation|"
        r"per[- ]purpose|by purpose|granular consent|consent purpose)\b|"
        r"\b(?:consent|permission|opt[- ]?in)\b.{0,80}\b(?:analytics|marketing|tracking|"
        r"profiling|personalization|promotional|communications?|data sharing|research|processing purpose)\b|"
        r"\b(?:analytics|marketing|tracking|profiling|personalization|promotional|communications?|"
        r"data sharing|research)\b.{0,80}\b(?:consent|permission|opt[- ]?in)\b",
        re.I,
    ),
    "regional_policy": re.compile(
        r"\b(?:gdpr|eprivacy|eea|eu|uk gdpr|ccpa|cpra|california|lgpd|brazil|quebec|"
        r"jurisdiction|regional|region[- ]specific|country[- ]specific|locale[- ]specific|"
        r"privacy law|policy regime)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit evidence|proof of consent|consent receipt|consent log|audit log|audit trail|"
        r"consent history|evidence of consent|timestamp|policy version|export consent|"
        r"compliance evidence|attestation)\b",
        re.I,
    ),
}
_CONSENT_CONTEXT_RE = re.compile(
    r"\b(?:privacy consent|consent|permission|authorization|opt[- ]?in|opt[- ]?out|"
    r"withdrawal|revocation|preference center|privacy preferences?|gdpr|ccpa|cpra|lgpd|"
    r"proof of consent|consent receipt|consent history)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|support|"
    r"allow|honou?r|respect|collect|capture|obtain|record|store|log|export|retain|"
    r"withdraw|revoke|bind|policy|compliance|acceptance|done when)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}"
    r"\b(?:consent|permission|opt[- ]?in|withdrawal|privacy consent)\b"
    r".{0,120}\b(?:required|needed|in scope|support|planned|changes?|impact)\b|"
    r"\b(?:consent|permission|opt[- ]?in|withdrawal|privacy consent)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|no work|no impact)\b",
    re.I,
)
_SCANNED_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "constraints",
    "privacy",
    "compliance",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_SPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class SourcePrivacyConsentRequirement:
    """One source-backed privacy consent requirement."""

    source_brief_id: str | None
    requirement_type: PrivacyConsentRequirementType
    requirement_text: str
    label: str
    source_field: str | None = None
    source_fields: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: PrivacyConsentConfidence = "medium"
    missing_detail_guidance: str | None = None

    @property
    def category(self) -> PrivacyConsentRequirementType:
        return self.requirement_type

    @property
    def requirement_category(self) -> PrivacyConsentRequirementType:
        return self.requirement_type

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "requirement_text": self.requirement_text,
            "label": self.label,
            "source_field": self.source_field,
            "source_fields": list(self.source_fields),
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "missing_detail_guidance": self.missing_detail_guidance,
        }


@dataclass(frozen=True, slots=True)
class SourcePrivacyConsentMissingDetail:
    """A missing planning detail for a privacy consent brief."""

    identifier: PrivacyConsentRequirementType
    label: str
    guidance: str

    def to_dict(self) -> dict[str, str]:
        return {"identifier": self.identifier, "label": self.label, "guidance": self.guidance}


@dataclass(frozen=True, slots=True)
class SourcePrivacyConsentRequirementsReport:
    """Source-level privacy consent requirements report."""

    source_id: str | None = None
    requirements: tuple[SourcePrivacyConsentRequirement, ...] = field(default_factory=tuple)
    missing_details: tuple[SourcePrivacyConsentMissingDetail, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourcePrivacyConsentRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourcePrivacyConsentRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "missing_details": [detail.to_dict() for detail in self.missing_details],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source Privacy Consent Requirements"
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
            lines.extend(["", "No source privacy consent requirements were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Type | Label | Requirement | Missing Detail Guidance | Evidence |", "| --- | --- | --- | --- | --- |"])
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.requirement_type)} | "
                f"{_markdown_cell(requirement.label)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.missing_detail_guidance or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        if self.missing_details:
            lines.extend(["", "## Missing Details", ""])
            for detail in self.missing_details:
                lines.append(f"- `{detail.identifier}` {detail.label}: {detail.guidance}")
        return "\n".join(lines)


def build_source_privacy_consent_requirements(source: Any) -> SourcePrivacyConsentRequirementsReport:
    """Extract source-level privacy consent requirement records from brief-shaped input."""
    payloads = _source_payloads(source)
    requirements = tuple(_merge(_candidates(payloads)))
    present = {requirement.requirement_type for requirement in requirements}
    missing = tuple(
        SourcePrivacyConsentMissingDetail(item, _LABELS[item], _GUIDANCE[item])
        for item in _TYPE_ORDER
        if item not in present and requirements
    )
    source_ids = _dedupe(source_id for source_id, _ in payloads if source_id)
    return SourcePrivacyConsentRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        missing_details=missing,
        summary=_summary(requirements, missing, len(payloads)),
    )


def extract_source_privacy_consent_requirements(source: Any) -> SourcePrivacyConsentRequirementsReport:
    return build_source_privacy_consent_requirements(source)


def generate_source_privacy_consent_requirements(source: Any) -> SourcePrivacyConsentRequirementsReport:
    return build_source_privacy_consent_requirements(source)


def derive_source_privacy_consent_requirements(source: Any) -> SourcePrivacyConsentRequirementsReport:
    return build_source_privacy_consent_requirements(source)


def summarize_source_privacy_consent_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourcePrivacyConsentRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_privacy_consent_requirements(source_or_result).summary


def source_privacy_consent_requirements_to_dict(report: SourcePrivacyConsentRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_privacy_consent_requirements_to_dict.__test__ = False


def source_privacy_consent_requirements_to_dicts(
    requirements: SourcePrivacyConsentRequirementsReport | Iterable[SourcePrivacyConsentRequirement],
) -> list[dict[str, Any]]:
    if isinstance(requirements, SourcePrivacyConsentRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_privacy_consent_requirements_to_dicts.__test__ = False


def source_privacy_consent_requirements_to_markdown(report: SourcePrivacyConsentRequirementsReport) -> str:
    return report.to_markdown()


source_privacy_consent_requirements_to_markdown.__test__ = False


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


def _candidates(payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[SourcePrivacyConsentRequirement]:
    found: list[SourcePrivacyConsentRequirement] = []
    for source_id, payload in payloads:
        if _NEGATED_RE.search(" ".join(text for _, text in _segments(payload))):
            continue
        for source_field, text in _segments(payload):
            if not _CONSENT_CONTEXT_RE.search(f"{source_field} {text}") or not _REQUIREMENT_RE.search(text):
                continue
            if _NEGATED_RE.search(text):
                continue
            for requirement_type, pattern in _TYPE_PATTERNS.items():
                if not pattern.search(text):
                    continue
                found.append(
                    SourcePrivacyConsentRequirement(
                        source_brief_id=source_id,
                        requirement_type=requirement_type,
                        requirement_text=text,
                        label=_LABELS[requirement_type],
                        source_field=source_field,
                        source_fields=(source_field,),
                        evidence=(f"{source_field}: {text}",),
                        matched_terms=tuple(_dedupe(match.group(0).strip() for match in pattern.finditer(text))),
                        confidence="high" if _CONSENT_CONTEXT_RE.search(text) and _REQUIREMENT_RE.search(text) else "medium",
                        missing_detail_guidance=None,
                    )
                )
    return found


def _merge(candidates: Iterable[SourcePrivacyConsentRequirement]) -> list[SourcePrivacyConsentRequirement]:
    grouped: dict[PrivacyConsentRequirementType, list[SourcePrivacyConsentRequirement]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.requirement_type, []).append(candidate)
    merged: list[SourcePrivacyConsentRequirement] = []
    for requirement_type in _TYPE_ORDER:
        items = grouped.get(requirement_type, [])
        if not items:
            continue
        first = items[0]
        merged.append(
            SourcePrivacyConsentRequirement(
                source_brief_id=first.source_brief_id,
                requirement_type=requirement_type,
                requirement_text=first.requirement_text,
                label=first.label,
                source_field=first.source_field,
                source_fields=tuple(_dedupe(field for item in items for field in item.source_fields)),
                evidence=tuple(_dedupe(evidence for item in items for evidence in item.evidence)),
                matched_terms=tuple(_dedupe(term for item in items for term in item.matched_terms)),
                confidence=min((item.confidence for item in items), key=_CONFIDENCE_ORDER.__getitem__),
                missing_detail_guidance=None,
            )
        )
    return merged


def _segments(payload: Mapping[str, Any], prefix: str | None = None) -> list[tuple[str, str]]:
    segments: list[tuple[str, str]] = []
    for key, value in payload.items():
        field = f"{prefix}.{key}" if prefix else str(key)
        if prefix is None and key not in _SCANNED_FIELDS:
            continue
        if isinstance(value, str):
            for part in _SPLIT_RE.split(value):
                text = _clean(part)
                if text:
                    segments.append((field, text))
        elif isinstance(value, Mapping):
            segments.extend(_segments(value, field))
        elif isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, str)):
            for index, item in enumerate(value):
                if isinstance(item, str):
                    text = _clean(item)
                    if text:
                        segments.append((f"{field}[{index}]", text))
                elif isinstance(item, Mapping):
                    segments.extend(_segments(item, f"{field}[{index}]"))
    return segments


def _summary(
    requirements: tuple[SourcePrivacyConsentRequirement, ...],
    missing: tuple[SourcePrivacyConsentMissingDetail, ...],
    source_count: int,
) -> dict[str, Any]:
    counts = {item: 0 for item in _TYPE_ORDER}
    confidence_counts = {item: 0 for item in _CONFIDENCE_ORDER}
    for requirement in requirements:
        counts[requirement.requirement_type] += 1
        confidence_counts[requirement.confidence] += 1
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_type_counts": counts,
        "confidence_counts": confidence_counts,
        "missing_detail_count": len(missing),
        "missing_detail_identifiers": [detail.identifier for detail in missing],
        "status": "ready_for_privacy_consent_planning" if requirements and not missing else ("needs_privacy_consent_planning" if requirements else "no_privacy_consent_language"),
    }


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
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")
