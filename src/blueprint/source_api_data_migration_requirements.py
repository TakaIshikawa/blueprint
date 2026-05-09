"""Extract API data migration and schema evolution requirements from source brief data."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for data migration concepts
_MIGRATION_SCRIPTS_RE = re.compile(
    r"\b(?:migration[_\s]+scripts?|migrate[_\s]+data|data[_\s]+migration[_\s]+script|"
    r"schema[_\s]+migration|alembic|flyway|liquibase|"
    r"knex[_\s]+migrate|migration[_\s]+file)\b",
    re.I,
)
_SCHEMA_VERSIONING_RE = re.compile(
    r"\b(?:schema[_\s]+version(?:ing)?|schema[_\s]+evolution|"
    r"schema[_\s]+registry|schema[_\s]+compatibility|"
    r"backward[_\s]+compatible[_\s]+schema|"
    r"schema[_\s]+migration[_\s]+version)\b",
    re.I,
)
_DATA_TRANSFORMATION_RE = re.compile(
    r"\b(?:data[_\s]+transformation|transform[_\s]+data|"
    r"ETL|data[_\s]+mapping|field[_\s]+mapping|"
    r"column[_\s]+mapping|data[_\s]+conversion|"
    r"type[_\s]+conversion|data[_\s]+normalization)\b",
    re.I,
)
_ROLLBACK_PROCEDURES_RE = re.compile(
    r"\b(?:migration[_\s]+rollback|rollback[_\s]+migration|"
    r"downgrade[_\s]+migration|reverse[_\s]+migration|"
    r"undo[_\s]+migration|migration[_\s]+revert|"
    r"rollback[_\s]+procedure|rollback[_\s]+script)\b",
    re.I,
)
_ZERO_DOWNTIME_RE = re.compile(
    r"\b(?:zero[_\s]+downtime|online[_\s]+migration|"
    r"live[_\s]+migration|rolling[_\s]+migration|"
    r"blue[_\s]+green[_\s]+migration|shadow[_\s]+migration|"
    r"parallel[_\s]+migration|dual[_\s]+write|dual[_\s]+read)\b",
    re.I,
)
_POST_MIGRATION_VALIDATION_RE = re.compile(
    r"\b(?:post[_\s]+migration[_\s]+validation|validate[_\s]+migration|"
    r"migration[_\s]+verification|data[_\s]+integrity[_\s]+check|"
    r"migration[_\s]+test|verify[_\s]+migration|"
    r"validate[_\s]+data[_\s]+after[_\s]+migration|"
    r"migration[_\s]+smoke[_\s]+test)\b",
    re.I,
)
_PROGRESS_TRACKING_RE = re.compile(
    r"\b(?:migration[_\s]+progress|migration[_\s]+status|"
    r"migration[_\s]+tracking|migration[_\s]+monitoring|"
    r"migration[_\s]+dashboard|track[_\s]+migration|"
    r"migration[_\s]+log|migration[_\s]+audit)\b",
    re.I,
)
_MIGRATION_TESTING_RE = re.compile(
    r"\b(?:migration[_\s]+test|test[_\s]+migration|"
    r"migration[_\s]+integration[_\s]+test|migration[_\s]+dry[_\s]+run|"
    r"migration[_\s]+staging|migration[_\s]+rehearsal|"
    r"migration[_\s]+simulation|test[_\s]+data[_\s]+migration)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class DataMigrationRequirements:
    """API data migration and schema evolution requirements extracted from source brief."""

    migration_scripts_specified: bool = False
    schema_versioning_defined: bool = False
    data_transformation_rules_specified: bool = False
    rollback_procedures_defined: bool = False
    zero_downtime_strategy_specified: bool = False
    post_migration_validation_planned: bool = False
    progress_tracking_included: bool = False
    migration_testing_specified: bool = False

    @property
    def completeness_score(self) -> float:
        """Calculate completeness score (0.0 to 1.0)."""
        total_checks = 8
        passed_checks = sum([
            self.migration_scripts_specified,
            self.schema_versioning_defined,
            self.data_transformation_rules_specified,
            self.rollback_procedures_defined,
            self.zero_downtime_strategy_specified,
            self.post_migration_validation_planned,
            self.progress_tracking_included,
            self.migration_testing_specified,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "migration_scripts_specified": self.migration_scripts_specified,
            "schema_versioning_defined": self.schema_versioning_defined,
            "data_transformation_rules_specified": self.data_transformation_rules_specified,
            "rollback_procedures_defined": self.rollback_procedures_defined,
            "zero_downtime_strategy_specified": self.zero_downtime_strategy_specified,
            "post_migration_validation_planned": self.post_migration_validation_planned,
            "progress_tracking_included": self.progress_tracking_included,
            "migration_testing_specified": self.migration_testing_specified,
            "completeness_score": self.completeness_score,
        }


def extract_data_migration_requirements(source_data: Mapping[str, Any]) -> DataMigrationRequirements:
    """
    Extract data migration requirements from source brief data.

    Args:
        source_data: A mapping containing source brief information with fields like
                    'title', 'description', 'requirements', etc.

    Returns:
        DataMigrationRequirements with boolean flags for each aspect and overall score.
    """
    if not isinstance(source_data, Mapping):
        return DataMigrationRequirements()

    searchable_text = _extract_searchable_text(source_data)

    return DataMigrationRequirements(
        migration_scripts_specified=bool(_MIGRATION_SCRIPTS_RE.search(searchable_text)),
        schema_versioning_defined=bool(_SCHEMA_VERSIONING_RE.search(searchable_text)),
        data_transformation_rules_specified=bool(_DATA_TRANSFORMATION_RE.search(searchable_text)),
        rollback_procedures_defined=bool(_ROLLBACK_PROCEDURES_RE.search(searchable_text)),
        zero_downtime_strategy_specified=bool(_ZERO_DOWNTIME_RE.search(searchable_text)),
        post_migration_validation_planned=bool(_POST_MIGRATION_VALIDATION_RE.search(searchable_text)),
        progress_tracking_included=bool(_PROGRESS_TRACKING_RE.search(searchable_text)),
        migration_testing_specified=bool(_MIGRATION_TESTING_RE.search(searchable_text)),
    )


def _extract_searchable_text(source_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the source data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "summary", "rationale"):
        value = source_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("requirements", "acceptance_criteria", "constraints", "notes", "definition_of_done"):
        value = source_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "DataMigrationRequirements",
    "extract_data_migration_requirements",
]
