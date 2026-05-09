"""Extract A/B testing requirements from source brief-shaped inputs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


AbTestingRequirementType = Literal[
    "experiment_design",
    "variant_specification",
    "success_metrics",
    "sample_size",
    "duration",
    "randomization",
    "statistical_significance",
    "metric_selection",
    "segment_definition",
    "bias_prevention",
]

_SPACE_RE = re.compile(r"\s+")
_SCANNED_FIELDS: tuple[str, ...] = (
    "summary",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
    "constraints",
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "risks",
    "metadata",
)
_REQUIREMENT_ORDER: dict[AbTestingRequirementType, int] = {
    "experiment_design": 0,
    "variant_specification": 1,
    "success_metrics": 2,
    "sample_size": 3,
    "duration": 4,
    "randomization": 5,
    "statistical_significance": 6,
    "metric_selection": 7,
    "segment_definition": 8,
    "bias_prevention": 9,
}
_REQUIREMENT_PATTERNS: dict[AbTestingRequirementType, tuple[re.Pattern[str], ...]] = {
    "experiment_design": (
        re.compile(
            r"\b(?:experiment design|a/?b test design|test design|"
            r"experimental design|hypothesis)\b",
            re.I,
        ),
    ),
    "variant_specification": (
        re.compile(
            r"\b(?:variant|variation|treatment|control|test group|"
            r"control group|a/?b variant)\b",
            re.I,
        ),
    ),
    "success_metrics": (
        re.compile(
            r"\b(?:success metrics?|key metrics?|target metrics?|"
            r"conversion|click[- ]?through|retention|engagement)\b",
            re.I,
        ),
    ),
    "sample_size": (
        re.compile(
            r"\b(?:sample size|sample|users? count|participant count|"
            r"test population|minimum detectable effect)\b",
            re.I,
        ),
    ),
    "duration": (
        re.compile(
            r"\b(?:test duration|experiment duration|run duration|"
            r"test period|time frame|duration estimate)\b",
            re.I,
        ),
    ),
    "randomization": (
        re.compile(
            r"\b(?:randomization|random assignment|random allocation|"
            r"randomize|shuffl(?:e|ing)|stratified)\b",
            re.I,
        ),
    ),
    "statistical_significance": (
        re.compile(
            r"\b(?:statistical significance|significance level|"
            r"confidence level|p[- ]?value|alpha level|"
            r"statistical validity|statistical rigor)\b",
            re.I,
        ),
    ),
    "metric_selection": (
        re.compile(
            r"\b(?:metric selection|select metrics?|choose metrics?|"
            r"primary metric|secondary metric|guardrail metric)\b",
            re.I,
        ),
    ),
    "segment_definition": (
        re.compile(
            r"\b(?:segment|segmentation|user segment|cohort|"
            r"target audience|target group|population)\b",
            re.I,
        ),
    ),
    "bias_prevention": (
        re.compile(
            r"\b(?:bias prevention|bias|selection bias|"
            r"survivorship bias|simpson[']?s paradox|confounding)\b",
            re.I,
        ),
    ),
}
_FOLLOW_UPS: dict[AbTestingRequirementType, str] = {
    "experiment_design": "Confirm hypothesis, expected outcomes, and experimental methodology.",
    "variant_specification": "Define all variants, their characteristics, and allocation percentages.",
    "success_metrics": "Establish metric definitions, tracking implementation, and success thresholds.",
    "sample_size": "Calculate required sample size for desired effect size and power.",
    "duration": "Determine test duration based on traffic, seasonality, and statistical requirements.",
    "randomization": "Specify randomization strategy, unit of randomization, and consistency guarantees.",
    "statistical_significance": "Define significance levels, multiple testing corrections, and stopping rules.",
    "metric_selection": "Document primary vs secondary metrics, guardrail metrics, and metric trade-offs.",
    "segment_definition": "Specify target segments, exclusion criteria, and segment size estimation.",
    "bias_prevention": "Identify potential biases and mitigation strategies.",
}


@dataclass(frozen=True, slots=True)
class AbTestingRequirement:
    """One A/B testing requirement found in source brief evidence."""

    requirement_type: AbTestingRequirementType
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: float = 0.0
    recommended_follow_up: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "recommended_follow_up": self.recommended_follow_up,
        }


@dataclass(frozen=True, slots=True)
class AbTestingRequirementsReport:
    """Source-level A/B testing requirements report."""

    source_brief_id: str | None = None
    title: str | None = None
    requirements: tuple[AbTestingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[AbTestingRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]


def build_ab_testing_requirements_report(
    source: Mapping[str, Any] | SourceBrief | object,
) -> AbTestingRequirementsReport:
    """Build an A/B testing requirements report from a source brief-like payload."""
    source_brief_id, payload = _source_payload(source)
    requirements = tuple(_find_requirements(payload))
    return AbTestingRequirementsReport(
        source_brief_id=source_brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def derive_ab_testing_requirements(
    source: Mapping[str, Any] | SourceBrief | object,
) -> tuple[AbTestingRequirement, ...]:
    """Return A/B testing requirement records from brief-shaped input."""
    return build_ab_testing_requirements_report(source).requirements


def extract_ab_testing_requirements(
    source: Mapping[str, Any] | SourceBrief | object,
) -> tuple[AbTestingRequirement, ...]:
    """Alias for callers that use extract_* naming."""
    return derive_ab_testing_requirements(source)


def _source_payload(source: Mapping[str, Any] | SourceBrief | object) -> tuple[str | None, dict[str, Any]]:
    """Extract source brief ID and payload from various input types."""
    if isinstance(source, Mapping):
        return source.get("id"), dict(source)

    if isinstance(source, SourceBrief):
        try:
            return source.id, source.model_dump()
        except (AttributeError, ValidationError):
            return getattr(source, "id", None), {}

    # Object-like input
    try:
        data = {}
        for field in _SCANNED_FIELDS:
            if hasattr(source, field):
                data[field] = getattr(source, field)
        return getattr(source, "id", None), data
    except Exception:
        return None, {}


def _optional_text(value: Any) -> str | None:
    """Extract optional text value."""
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _find_requirements(payload: dict[str, Any]) -> list[AbTestingRequirement]:
    """Find all A/B testing requirements in the payload."""
    requirements: list[AbTestingRequirement] = []
    seen_types: set[AbTestingRequirementType] = set()

    for field in _SCANNED_FIELDS:
        value = payload.get(field)
        if not value:
            continue

        text = _extract_text(value)
        if not text:
            continue

        for req_type, patterns in _REQUIREMENT_PATTERNS.items():
            if req_type in seen_types:
                continue

            for pattern in patterns:
                if pattern.search(text):
                    requirements.append(
                        AbTestingRequirement(
                            requirement_type=req_type,
                            source_field=field,
                            evidence=(text[:200],),
                            confidence=1.0,
                            recommended_follow_up=_FOLLOW_UPS.get(req_type, ""),
                        )
                    )
                    seen_types.add(req_type)
                    break

    # Sort by requirement order
    requirements.sort(key=lambda r: _REQUIREMENT_ORDER[r.requirement_type])
    return requirements


def _extract_text(value: Any) -> str:
    """Extract searchable text from value."""
    if isinstance(value, str):
        return _SPACE_RE.sub(" ", value).strip()
    if isinstance(value, (list, tuple)):
        return " ".join(str(item) for item in value if item)
    return ""


def _summary(requirements: tuple[AbTestingRequirement, ...]) -> dict[str, Any]:
    """Generate summary statistics."""
    requirement_counts: dict[str, int] = {}
    for req in requirements:
        requirement_counts[req.requirement_type] = requirement_counts.get(req.requirement_type, 0) + 1

    return {
        "total_requirements": len(requirements),
        "requirement_types_found": len(set(r.requirement_type for r in requirements)),
        "completeness_score": len(requirements) / len(_REQUIREMENT_ORDER),
        "by_type": requirement_counts,
    }


__all__ = [
    "AbTestingRequirement",
    "AbTestingRequirementsReport",
    "AbTestingRequirementType",
    "build_ab_testing_requirements_report",
    "derive_ab_testing_requirements",
    "extract_ab_testing_requirements",
]
