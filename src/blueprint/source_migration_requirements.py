"""Extract data migration requirements from source brief data."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for data migration concepts
_SOURCE_SYSTEMS_RE = re.compile(
    r"\b(?:source[_\s]+system(?:s)?|migrate[_\s]+from|"
    r"legacy[_\s]+(?:system|database|platform)|"
    r"existing[_\s]+(?:system|database|data)|"
    r"current[_\s]+(?:system|platform|database)|"
    r"origin[_\s]+(?:system|database))\b",
    re.I,
)
_DATA_VOLUME_RE = re.compile(
    r"\b(?:data[_\s]+volume|number[_\s]+of[_\s]+records|"
    r"(?:migrate|transfer)[_\s]+\d+|"
    r"(?:millions?|thousands?|billions?)[_\s]+of[_\s]+records|"
    r"(?:TB|GB|MB)[_\s]+of[_\s]+data|"
    r"data[_\s]+size[_\s]+estimate|volume[_\s]+estimate)\b",
    re.I,
)
_TRANSFORMATION_RULES_RE = re.compile(
    r"\b(?:transformation[_\s]+(?:rules?|logic|mapping)|"
    r"data[_\s]+transformation|transform[_\s]+data|"
    r"field[_\s]+mapping|schema[_\s]+mapping|"
    r"convert[_\s]+data|data[_\s]+conversion|"
    r"(?:map|mapping)[_\s]+(?:fields?|columns?|schema))\b",
    re.I,
)
_VALIDATION_CRITERIA_RE = re.compile(
    r"\b(?:validation[_\s]+(?:criteria|rules?|logic|strategy)|"
    r"data[_\s]+validation|validate[_\s]+(?:data|migration)|"
    r"(?:verify|check)[_\s]+data[_\s]+(?:integrity|correctness)|"
    r"reconciliation|data[_\s]+(?:quality|integrity)[_\s]+check)\b",
    re.I,
)
_CUTOVER_STRATEGY_RE = re.compile(
    r"\b(?:cutover[_\s]+(?:strategy|plan|approach|timeline)|"
    r"migration[_\s]+cutover|switch[_\s]+over|"
    r"go[_\s-]*live[_\s]+(?:strategy|plan)|"
    r"production[_\s]+cutover|final[_\s]+cutover)\b",
    re.I,
)
_DATA_QUALITY_RE = re.compile(
    r"\b(?:data[_\s]+quality|quality[_\s]+(?:check(?:s)?|assurance|validation)|"
    r"data[_\s]+(?:cleansing|cleaning|scrubbing)|"
    r"(?:ensure|maintain)[_\s]+data[_\s]+quality|"
    r"quality[_\s]+gate(?:s)?)\b",
    re.I,
)
_DEDUPLICATION_RE = re.compile(
    r"\b(?:deduplication|de[_\s-]*duplication|"
    r"dedupe|remove[_\s]+duplicates?|"
    r"duplicate[_\s]+(?:detection|removal|handling)|"
    r"(?:identify|eliminate)[_\s]+duplicate(?:s)?)\b",
    re.I,
)
_RECONCILIATION_RE = re.compile(
    r"\b(?:reconciliation|reconcile[_\s]+data|"
    r"data[_\s]+reconciliation|"
    r"compare[_\s]+(?:source|data)[_\s]+(?:and|with)[_\s]+(?:target|destination)|"
    r"verify[_\s]+data[_\s]+(?:match(?:es)?|parity)|"
    r"validation[_\s]+report)\b",
    re.I,
)
_ROLLBACK_PROCEDURES_RE = re.compile(
    r"\b(?:rollback[_\s]+(?:procedure(?:s)?|plan|strategy)|"
    r"migration[_\s]+rollback|revert[_\s]+migration|"
    r"fallback[_\s]+(?:plan|procedure)|"
    r"(?:undo|reverse)[_\s]+migration|"
    r"rollback[_\s]+(?:if|on)[_\s]+failure)\b",
    re.I,
)
_DUAL_RUN_PERIOD_RE = re.compile(
    r"\b(?:dual[_\s-]*run(?:[_\s]+period)?|parallel[_\s]+run|"
    r"run[_\s]+in[_\s]+parallel|shadow[_\s]+mode|"
    r"(?:both|old[_\s]+and[_\s]+new)[_\s]+system(?:s)?[_\s]+running|"
    r"grace[_\s]+period|cutover[_\s]+period)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class MigrationRequirements:
    """Data migration requirements extracted from source brief."""

    source_systems_identified: bool = False
    data_volume_estimated: bool = False
    transformation_rules_defined: bool = False
    validation_criteria_specified: bool = False
    cutover_strategy_planned: bool = False
    data_quality_addressed: bool = False
    deduplication_planned: bool = False
    reconciliation_included: bool = False
    rollback_procedures_defined: bool = False
    dual_run_period_planned: bool = False

    @property
    def completeness_score(self) -> float:
        """Calculate completeness score (0.0 to 1.0)."""
        total_checks = 10
        passed_checks = sum([
            self.source_systems_identified,
            self.data_volume_estimated,
            self.transformation_rules_defined,
            self.validation_criteria_specified,
            self.cutover_strategy_planned,
            self.data_quality_addressed,
            self.deduplication_planned,
            self.reconciliation_included,
            self.rollback_procedures_defined,
            self.dual_run_period_planned,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "source_systems_identified": self.source_systems_identified,
            "data_volume_estimated": self.data_volume_estimated,
            "transformation_rules_defined": self.transformation_rules_defined,
            "validation_criteria_specified": self.validation_criteria_specified,
            "cutover_strategy_planned": self.cutover_strategy_planned,
            "data_quality_addressed": self.data_quality_addressed,
            "deduplication_planned": self.deduplication_planned,
            "reconciliation_included": self.reconciliation_included,
            "rollback_procedures_defined": self.rollback_procedures_defined,
            "dual_run_period_planned": self.dual_run_period_planned,
            "completeness_score": self.completeness_score,
        }


def extract_migration_requirements(source_data: Mapping[str, Any]) -> MigrationRequirements:
    """
    Extract migration requirements from source brief data.

    Args:
        source_data: A mapping containing source brief information with fields like
                    'title', 'description', 'requirements', etc.

    Returns:
        MigrationRequirements with boolean flags for each aspect and overall score.
    """
    if not isinstance(source_data, Mapping):
        return MigrationRequirements()

    searchable_text = _extract_searchable_text(source_data)

    return MigrationRequirements(
        source_systems_identified=bool(_SOURCE_SYSTEMS_RE.search(searchable_text)),
        data_volume_estimated=bool(_DATA_VOLUME_RE.search(searchable_text)),
        transformation_rules_defined=bool(_TRANSFORMATION_RULES_RE.search(searchable_text)),
        validation_criteria_specified=bool(_VALIDATION_CRITERIA_RE.search(searchable_text)),
        cutover_strategy_planned=bool(_CUTOVER_STRATEGY_RE.search(searchable_text)),
        data_quality_addressed=bool(_DATA_QUALITY_RE.search(searchable_text)),
        deduplication_planned=bool(_DEDUPLICATION_RE.search(searchable_text)),
        reconciliation_included=bool(_RECONCILIATION_RE.search(searchable_text)),
        rollback_procedures_defined=bool(_ROLLBACK_PROCEDURES_RE.search(searchable_text)),
        dual_run_period_planned=bool(_DUAL_RUN_PERIOD_RE.search(searchable_text)),
    )


def _extract_searchable_text(source_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the source data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale", "context"):
        value = source_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("requirements", "acceptance_criteria", "notes", "constraints", "objectives"):
        value = source_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "MigrationRequirements",
    "extract_migration_requirements",
]
