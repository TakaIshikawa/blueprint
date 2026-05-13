"""Extract source-level privacy impact requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from blueprint._source_requirement_utils import (
    dedupe,
    evidence_snippet,
    markdown_cell,
    optional_text,
    segments,
    source_payload,
)


PrivacyImpactRequirementType = Literal[
    "data_categories",
    "legal_basis_consent",
    "data_subject_rights",
    "minimization",
    "retention_reference",
    "third_party_sharing",
    "dpa_dpia_evidence",
    "owner",
    "unresolved_gap",
]
PrivacyImpactConfidence = Literal["high", "medium", "low"]

_TYPE_ORDER: tuple[PrivacyImpactRequirementType, ...] = (
    "data_categories",
    "legal_basis_consent",
    "data_subject_rights",
    "minimization",
    "retention_reference",
    "third_party_sharing",
    "dpa_dpia_evidence",
    "owner",
    "unresolved_gap",
)
_CONFIDENCE_ORDER: dict[PrivacyImpactConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SCANNED_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "constraints",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "data_requirements",
    "privacy",
    "risks",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_PATTERNS: dict[PrivacyImpactRequirementType, re.Pattern[str]] = {
    "data_categories": re.compile(
        r"\b(?:data categor(?:y|ies)|personal data|pii|email addresses?|phone numbers?|"
        r"names?|addresses?|location data|payment data|health data|special category|sensitive data)\b",
        re.I,
    ),
    "legal_basis_consent": re.compile(
        r"\b(?:legal basis|consent|opt[- ]?in|legitimate interest|contractual necessity|"
        r"permission to process|gdpr basis|withdraw consent)\b",
        re.I,
    ),
    "data_subject_rights": re.compile(
        r"\b(?:data subject rights?|dsar|access request|delete request|erasure|rectification|"
        r"portability|objection|right to be forgotten)\b",
        re.I,
    ),
    "minimization": re.compile(
        r"\b(?:data minimization|minimi[sz]e data|minimum necessary|collect only|"
        r"only necessary|avoid collecting|no extra pii|privacy by default)\b",
        re.I,
    ),
    "retention_reference": re.compile(
        r"\b(?:retention|retain|purge|delete after|ttl|expiration|data lifecycle|retention schedule)\b",
        re.I,
    ),
    "third_party_sharing": re.compile(
        r"\b(?:third[- ]party sharing|processor|subprocessor|vendor|share with|external provider|"
        r"data transfer|cross[- ]border transfer|third party receives)\b",
        re.I,
    ),
    "dpa_dpia_evidence": re.compile(
        r"\b(?:dpa|data processing agreement|dpia|privacy impact assessment|pia|ropa|"
        r"privacy review|legal review|vendor assessment)\b",
        re.I,
    ),
    "owner": re.compile(
        r"\b(?:privacy owner|data owner|dpo|legal owner|product owner|security owner|owning team|dri|owner)\b",
        re.I,
    ),
    "unresolved_gap": re.compile(
        r"\b(?:tbd|todo|unresolved|unknown|not yet defined|needs decision|open question|gap|missing)\b",
        re.I,
    ),
}
_NOTES: dict[PrivacyImpactRequirementType, str] = {
    "data_categories": "Inventory personal data categories, sensitivity, and source systems.",
    "legal_basis_consent": "Record legal basis, consent capture, and withdrawal behavior.",
    "data_subject_rights": "Confirm DSAR, deletion, rectification, portability, and objection handling.",
    "minimization": "Limit collection, processing, logs, and analytics to minimum necessary data.",
    "retention_reference": "Link the workflow to retention, purge, and lifecycle requirements.",
    "third_party_sharing": "Document processors, subprocessors, transfers, and sharing constraints.",
    "dpa_dpia_evidence": "Attach DPA, DPIA, PIA, ROPA, or privacy review evidence.",
    "owner": "Name the accountable privacy, legal, product, or data owner.",
    "unresolved_gap": "Resolve open privacy impact questions before implementation planning proceeds.",
}


@dataclass(frozen=True, slots=True)
class SourcePrivacyImpactRequirement:
    """One source-backed privacy impact requirement."""

    requirement_type: PrivacyImpactRequirementType
    source_field: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: PrivacyImpactConfidence = "medium"
    planning_note: str = ""
    owner: str | None = None
    unresolved_gaps: tuple[str, ...] = field(default_factory=tuple)

    @property
    def category(self) -> PrivacyImpactRequirementType:
        return self.requirement_type

    def to_dict(self) -> dict[str, Any]:
        return {
            "requirement_type": self.requirement_type,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
            "owner": self.owner,
            "unresolved_gaps": list(self.unresolved_gaps),
        }


@dataclass(frozen=True, slots=True)
class SourcePrivacyImpactRequirementsReport:
    """Source-level privacy impact requirements report."""

    source_brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourcePrivacyImpactRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourcePrivacyImpactRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [item.to_dict() for item in self.requirements],
            "records": [item.to_dict() for item in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source Privacy Impact Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Unresolved gap count: {len(self.summary.get('unresolved_gaps', []))}",
        ]
        if not self.requirements:
            lines.extend(["", "No source privacy impact requirements were inferred."])
            return "\n".join(lines)
        lines.extend(["", "| Type | Confidence | Source | Evidence |", "| --- | --- | --- | --- |"])
        for item in self.requirements:
            lines.append(
                f"| {item.requirement_type} | {item.confidence} | {markdown_cell(item.source_field)} | "
                f"{markdown_cell('; '.join(item.evidence))} |"
            )
        return "\n".join(lines)


def build_source_privacy_impact_requirements(source: Any) -> SourcePrivacyImpactRequirementsReport:
    """Build a privacy impact requirements report from source brief-like input."""
    source_brief_id, payload = source_payload(source)
    requirements = tuple(_merge(_candidates(payload)))
    return SourcePrivacyImpactRequirementsReport(
        source_brief_id=source_brief_id,
        title=optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_privacy_impact_requirements(source: Any) -> SourcePrivacyImpactRequirementsReport:
    return build_source_privacy_impact_requirements(source)


def derive_source_privacy_impact_requirements(source: Any) -> SourcePrivacyImpactRequirementsReport:
    return build_source_privacy_impact_requirements(source)


def generate_source_privacy_impact_requirements(source: Any) -> SourcePrivacyImpactRequirementsReport:
    return build_source_privacy_impact_requirements(source)


def extract_source_privacy_impact_requirements(source: Any) -> tuple[SourcePrivacyImpactRequirement, ...]:
    return build_source_privacy_impact_requirements(source).requirements


def source_privacy_impact_requirements_to_dict(report: SourcePrivacyImpactRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_privacy_impact_requirements_to_dict.__test__ = False


def source_privacy_impact_requirements_to_dicts(items: Any) -> list[dict[str, Any]]:
    if isinstance(items, SourcePrivacyImpactRequirementsReport):
        return items.to_dicts()
    return [item.to_dict() for item in items]


source_privacy_impact_requirements_to_dicts.__test__ = False


def source_privacy_impact_requirements_to_markdown(report: SourcePrivacyImpactRequirementsReport) -> str:
    return report.to_markdown()


source_privacy_impact_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: PrivacyImpactRequirementType
    source_field: str
    evidence: str
    confidence: PrivacyImpactConfidence
    owner: str | None = None


def _candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    found: list[_Candidate] = []
    for source_field, text in segments(payload, _SCANNED_FIELDS):
        matched_types = [name for name in _TYPE_ORDER if _PATTERNS[name].search(text)]
        if not matched_types:
            continue
        confidence = _confidence(source_field, text)
        owner = _owner(text)
        evidence = evidence_snippet(source_field, text)
        for requirement_type in matched_types:
            found.append(_Candidate(requirement_type, source_field, evidence, confidence, owner))
    return found


def _merge(candidates: list[_Candidate]) -> list[SourcePrivacyImpactRequirement]:
    grouped: dict[PrivacyImpactRequirementType, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.requirement_type, []).append(candidate)
    result: list[SourcePrivacyImpactRequirement] = []
    for requirement_type, items in grouped.items():
        evidence = tuple(dedupe(item.evidence for item in items))[:4]
        result.append(
            SourcePrivacyImpactRequirement(
                requirement_type=requirement_type,
                source_field=sorted({item.source_field for item in items}, key=str.casefold)[0],
                evidence=evidence,
                confidence=min((item.confidence for item in items), key=_CONFIDENCE_ORDER.get),
                planning_note=_NOTES[requirement_type],
                owner=_best_owner(items),
                unresolved_gaps=evidence if requirement_type == "unresolved_gap" else (),
            )
        )
    return sorted(
        result,
        key=lambda item: (
            _CONFIDENCE_ORDER[item.confidence],
            _TYPE_ORDER.index(item.requirement_type),
            item.source_field.casefold(),
        ),
    )


def _summary(requirements: tuple[SourcePrivacyImpactRequirement, ...]) -> dict[str, Any]:
    present = {item.requirement_type for item in requirements}
    expected = tuple(name for name in _TYPE_ORDER if name != "unresolved_gap")
    unresolved = [_NOTES[name] for name in expected if requirements and name not in present]
    unresolved.extend(gap for item in requirements for gap in item.unresolved_gaps)
    return {
        "requirement_count": len(requirements),
        "requirement_types": [item.requirement_type for item in requirements],
        "requirement_type_counts": {
            name: sum(1 for item in requirements if item.requirement_type == name)
            for name in _TYPE_ORDER
        },
        "confidence_counts": {
            name: sum(1 for item in requirements if item.confidence == name)
            for name in _CONFIDENCE_ORDER
        },
        "owners": sorted({item.owner for item in requirements if item.owner}),
        "unresolved_gaps": list(dedupe(unresolved)),
    }


def _confidence(source_field: str, text: str) -> PrivacyImpactConfidence:
    field = source_field.casefold()
    if any(marker in field for marker in ("acceptance", "definition_of_done", "constraints")):
        return "high"
    if re.search(r"\b(?:must|shall|required|requires?|ensure|document|retain|delete)\b", text, re.I):
        return "high"
    if re.search(r"\b(?:privacy|personal data|pii|consent|retention|third[- ]party)\b", text, re.I):
        return "medium"
    return "low"


def _owner(text: str) -> str | None:
    if match := re.search(
        r"\b(?:privacy owner|data owner|legal owner|product owner|security owner|owner|dri)\s*(?:is|:|-)?\s*(?P<owner>[A-Z][A-Za-z0-9 _/-]{1,50})",
        text,
        re.I,
    ):
        return optional_text(match.group("owner").rstrip("."))
    return None


def _best_owner(items: list[_Candidate]) -> str | None:
    owners = sorted({item.owner for item in items if item.owner}, key=str.casefold)
    return owners[0] if owners else None
