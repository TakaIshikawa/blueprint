"""Extract CI/CD pipeline requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


CicdRequirementType = Literal[
    "build_steps",
    "test_stages",
    "deployment_strategy",
    "environment_promotion",
    "approval_gates",
    "build_optimization",
    "test_parallelization",
    "artifact_management",
    "deployment_automation",
    "rollback_procedures",
]

_TYPE_ORDER: tuple[CicdRequirementType, ...] = (
    "build_steps",
    "test_stages",
    "deployment_strategy",
    "environment_promotion",
    "approval_gates",
    "build_optimization",
    "test_parallelization",
    "artifact_management",
    "deployment_automation",
    "rollback_procedures",
)

_SPACE_RE = re.compile(r"\s+")
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "integration_points",
    "integrations",
    "constraints",
    "metadata",
)

_TYPE_PATTERNS: dict[CicdRequirementType, re.Pattern[str]] = {
    "build_steps": re.compile(
        r"\b(?:build\s+(?:steps?|stages?|process|pipeline)|"
        r"compilation|compile\s+(?:code|sources?)|"
        r"(?:npm|yarn|maven|gradle|make)\s+build|"
        r"build\s+(?:configuration|script|automation)|"
        r"(?:docker|container)\s+build|image\s+build)\b",
        re.I,
    ),
    "test_stages": re.compile(
        r"\b(?:test\s+(?:stages?|phases?|pipeline)|"
        r"(?:unit|integration|e2e|end[- ]to[- ]end)\s+tests?|"
        r"test\s+(?:suite|automation|execution|coverage)|"
        r"(?:run|execute)\s+tests?|automated\s+testing)\b",
        re.I,
    ),
    "deployment_strategy": re.compile(
        r"\b(?:deployment\s+strateg(?:y|ies)|deploy(?:ment)?\s+(?:approach|process|pipeline)|"
        r"(?:blue[- ]green|canary|rolling)\s+deploy(?:ment)?|"
        r"deploy(?:ment)?\s+(?:automation|orchestration)|"
        r"continuous\s+deploy(?:ment)?|cd\s+pipeline)\b",
        re.I,
    ),
    "environment_promotion": re.compile(
        r"\b(?:environment\s+promotion|promote\s+to\s+(?:staging|production)|"
        r"(?:dev|staging|prod(?:uction)?)\s+environment|"
        r"multi[- ]environment\s+deploy(?:ment)?|"
        r"environment\s+(?:pipeline|progression|flow))\b",
        re.I,
    ),
    "approval_gates": re.compile(
        r"\b(?:approval\s+gates?|manual\s+approval|"
        r"require(?:s|d)?\s+approval|approval\s+(?:process|workflow|step)|"
        r"(?:sign[- ]?off|gate(?:way|keeper)?)\s+approval|"
        r"review\s+(?:before|prior\s+to)\s+deploy(?:ment)?)\b",
        re.I,
    ),
    "build_optimization": re.compile(
        r"\b(?:build\s+optimization|optimize\s+build|"
        r"(?:fast|faster|quick(?:er)?)\s+build|build\s+(?:caching|cache)|"
        r"parallel\s+build|incremental\s+build|"
        r"build\s+(?:performance|speed|time))\b",
        re.I,
    ),
    "test_parallelization": re.compile(
        r"\b(?:test\s+parallelization|parallel\s+tests?|"
        r"parallelize\s+tests?|concurrent\s+tests?|"
        r"(?:test\s+)?(?:matrix|sharding)|distributed\s+testing)\b",
        re.I,
    ),
    "artifact_management": re.compile(
        r"\b(?:artifact\s+(?:management|storage|repository)|"
        r"(?:store|publish|upload)\s+artifacts?|"
        r"(?:docker|container)\s+(?:registry|repository)|"
        r"(?:npm|maven|pypi)\s+(?:registry|repository)|"
        r"build\s+artifacts?|package\s+(?:management|registry))\b",
        re.I,
    ),
    "deployment_automation": re.compile(
        r"\b(?:deployment\s+automation|automate(?:d)?\s+deploy(?:ment)?|"
        r"(?:automated|automatic)\s+(?:release|rollout)|"
        r"(?:ci[/]?cd|continuous\s+(?:integration|deployment))\s+pipeline|"
        r"(?:conditional|branch[- ]based)\s+deploy(?:ment)?|"
        r"(?:jenkins|github\s+actions|gitlab\s+ci|circle\s?ci|travis)\s+pipeline)\b",
        re.I,
    ),
    "rollback_procedures": re.compile(
        r"\b(?:rollback\s+(?:procedure|process|plan|strategy)|"
        r"(?:automated|automatic|manual)\s+rollback|"
        r"rollback\s+(?:capability|support|automation)|"
        r"revert\s+deploy(?:ment)?|deployment\s+rollback)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class SourceCicdRequirement:
    """One source-backed CI/CD pipeline requirement."""

    requirement_type: CicdRequirementType
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
        }


@dataclass(frozen=True, slots=True)
class SourceCicdRequirementsReport:
    """Source-level CI/CD pipeline requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceCicdRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceCicdRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return CI/CD requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source CI/CD Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("type_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Source count: {self.summary.get('source_count', 1)}",
            f"- Pipeline coverage: {self.summary.get('pipeline_coverage', 0)}%",
            f"- Automation coverage: {self.summary.get('automation_coverage', 0)}%",
            "- Requirement type counts: "
            + ", ".join(
                f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source CI/CD requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Source Field Paths | Evidence |",
                "| --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def extract_cicd_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceCicdRequirementsReport:
    """Extract CI/CD pipeline requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    requirements = _extract_requirements(payload)
    return SourceCicdRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def _source_payload(source: Mapping[str, Any] | SourceBrief | str | object) -> tuple[str | None, Mapping[str, Any]]:
    """Extract source brief ID and searchable payload from input."""
    if isinstance(source, SourceBrief):
        return source.id, source.model_dump()
    if isinstance(source, Mapping):
        brief_id = source.get("id") or source.get("source_brief_id")
        return str(brief_id) if brief_id else None, source
    if isinstance(source, str):
        try:
            brief = SourceBrief.model_validate_json(source)
            return brief.id, brief.model_dump()
        except (ValidationError, ValueError):
            return None, {}
    if hasattr(source, "model_dump"):
        try:
            payload = source.model_dump()
            brief_id = payload.get("id") or payload.get("source_brief_id")
            return str(brief_id) if brief_id else None, payload
        except Exception:
            return None, {}
    return None, {}


def _extract_requirements(payload: Mapping[str, Any]) -> tuple[SourceCicdRequirement, ...]:
    """Extract CI/CD requirements from payload."""
    requirements: list[SourceCicdRequirement] = []

    for req_type, pattern in _TYPE_PATTERNS.items():
        for field_name in _SCANNED_FIELDS:
            field_value = payload.get(field_name)
            if not field_value:
                continue

            text = _extract_text(field_value)
            if not text:
                continue

            matches = pattern.findall(text)
            if matches:
                evidence = tuple(match.strip() for match in matches[:3])
                requirements.append(
                    SourceCicdRequirement(
                        requirement_type=req_type,
                        evidence=evidence,
                        source_field_paths=(field_name,),
                        matched_terms=evidence,
                    )
                )
                break

    return tuple(requirements)


def _extract_text(value: Any) -> str:
    """Extract searchable text from various value types."""
    if isinstance(value, str):
        return _SPACE_RE.sub(" ", value).strip()
    if isinstance(value, (list, tuple)):
        parts = [_extract_text(item) for item in value]
        return " ".join(part for part in parts if part)
    if isinstance(value, Mapping):
        parts = [_extract_text(v) for v in value.values()]
        return " ".join(part for part in parts if part)
    return str(value) if value else ""


def _summary(requirements: tuple[SourceCicdRequirement, ...]) -> dict[str, Any]:
    """Calculate summary statistics for CI/CD requirements."""
    type_counts: dict[str, int] = {}
    for requirement in requirements:
        type_counts[requirement.requirement_type] = type_counts.get(requirement.requirement_type, 0) + 1

    pipeline_types = {"build_steps", "test_stages", "deployment_strategy"}
    automation_types = {"deployment_automation", "build_optimization", "test_parallelization"}

    pipeline_coverage = (
        len([r for r in requirements if r.requirement_type in pipeline_types]) / len(pipeline_types) * 100
        if requirements else 0
    )
    automation_coverage = (
        len([r for r in requirements if r.requirement_type in automation_types]) / len(automation_types) * 100
        if requirements else 0
    )

    return {
        "requirement_count": len(requirements),
        "source_count": 1,
        "pipeline_coverage": int(pipeline_coverage),
        "automation_coverage": int(automation_coverage),
        "type_counts": type_counts,
    }


def _markdown_cell(text: str) -> str:
    """Escape text for safe Markdown table cell rendering."""
    return text.replace("|", "\\|").replace("\n", " ")


# Compatibility aliases
generate_cicd_requirements = extract_cicd_requirements
analyze_cicd_requirements = extract_cicd_requirements
derive_cicd_requirements = extract_cicd_requirements
summarize_cicd_requirements = extract_cicd_requirements


__all__ = [
    "CicdRequirementType",
    "SourceCicdRequirement",
    "SourceCicdRequirementsReport",
    "extract_cicd_requirements",
    "generate_cicd_requirements",
    "analyze_cicd_requirements",
    "derive_cicd_requirements",
    "summarize_cicd_requirements",
]
