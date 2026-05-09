"""Analyze data migration readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for data migration concepts
_MIGRATION_SCRIPT_QUALITY_RE = re.compile(
    r"\b(?:migration[_\s]+script[_\s]+quality|"
    r"script[_\s]+validation|"
    r"migration[_\s]+script[_\s]+testing|"
    r"migration[_\s]+script[_\s]+review|"
    r"validate[_\s]+migration[_\s]+script|"
    r"migration[_\s]+code[_\s]+review)\b",
    re.I,
)
_SCHEMA_VERSION_TRACKING_RE = re.compile(
    r"\b(?:schema[_\s]+version[_\s]+tracking|"
    r"version[_\s]+tracking|"
    r"schema[_\s]+registry|"
    r"schema[_\s]+catalog|"
    r"version[_\s]+control[_\s]+schema|"
    r"track[_\s]+schema[_\s]+version)\b",
    re.I,
)
_TRANSFORMATION_LOGIC_RE = re.compile(
    r"\b(?:transformation[_\s]+logic|"
    r"data[_\s]+mapping[_\s]+verified|"
    r"field[_\s]+mapping[_\s]+tested|"
    r"conversion[_\s]+logic[_\s]+validated|"
    r"ETL[_\s]+logic[_\s]+tested|"
    r"transformation[_\s]+testing)\b",
    re.I,
)
_ROLLBACK_PROCEDURE_RE = re.compile(
    r"\b(?:rollback[_\s]+tested|"
    r"test[_\s]+rollback|"
    r"rollback[_\s]+procedure[_\s]+verified|"
    r"rollback[_\s]+drill|"
    r"rollback[_\s]+rehearsal|"
    r"migration[_\s]+rollback[_\s]+test)\b",
    re.I,
)
_ZERO_DOWNTIME_RE = re.compile(
    r"\b(?:zero[_\s]+downtime[_\s]+implemented|"
    r"online[_\s]+migration[_\s]+configured|"
    r"rolling[_\s]+migration[_\s]+setup|"
    r"dual[_\s]+write[_\s]+enabled|"
    r"live[_\s]+migration[_\s]+ready|"
    r"blue[_\s]+green[_\s]+migration[_\s]+configured)\b",
    re.I,
)
_POST_MIGRATION_VALIDATION_RE = re.compile(
    r"\b(?:post[_\s]+migration[_\s]+validation[_\s]+configured|"
    r"migration[_\s]+validation[_\s]+automated|"
    r"data[_\s]+integrity[_\s]+verified|"
    r"migration[_\s]+smoke[_\s]+test|"
    r"verify[_\s]+data[_\s]+after[_\s]+migration)\b",
    re.I,
)
_PROGRESS_TRACKING_RE = re.compile(
    r"\b(?:progress[_\s]+tracking[_\s]+configured|"
    r"migration[_\s]+monitoring[_\s]+setup|"
    r"migration[_\s]+dashboard[_\s]+configured|"
    r"migration[_\s]+status[_\s]+reporting|"
    r"track[_\s]+migration[_\s]+progress)\b",
    re.I,
)
_MIGRATION_TESTING_COVERAGE_RE = re.compile(
    r"\b(?:migration[_\s]+test[_\s]+coverage|"
    r"migration[_\s]+integration[_\s]+tests|"
    r"migration[_\s]+dry[_\s]+run[_\s]+completed|"
    r"migration[_\s]+staging[_\s]+tested|"
    r"migration[_\s]+rehearsal[_\s]+done|"
    r"end[_\s]+to[_\s]+end[_\s]+migration[_\s]+test)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class DataMigrationReadiness:
    """Data migration readiness analysis for a task."""

    migration_script_quality_assessed: bool = False
    schema_version_tracking_configured: bool = False
    transformation_logic_verified: bool = False
    rollback_procedure_tested: bool = False
    zero_downtime_implemented: bool = False
    post_migration_validation_configured: bool = False
    progress_tracking_configured: bool = False
    migration_testing_coverage_adequate: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        total_checks = 8
        passed_checks = sum([
            self.migration_script_quality_assessed,
            self.schema_version_tracking_configured,
            self.transformation_logic_verified,
            self.rollback_procedure_tested,
            self.zero_downtime_implemented,
            self.post_migration_validation_configured,
            self.progress_tracking_configured,
            self.migration_testing_coverage_adequate,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "migration_script_quality_assessed": self.migration_script_quality_assessed,
            "schema_version_tracking_configured": self.schema_version_tracking_configured,
            "transformation_logic_verified": self.transformation_logic_verified,
            "rollback_procedure_tested": self.rollback_procedure_tested,
            "zero_downtime_implemented": self.zero_downtime_implemented,
            "post_migration_validation_configured": self.post_migration_validation_configured,
            "progress_tracking_configured": self.progress_tracking_configured,
            "migration_testing_coverage_adequate": self.migration_testing_coverage_adequate,
            "readiness_score": self.readiness_score,
        }


def analyze_data_migration_readiness(task_data: Mapping[str, Any]) -> DataMigrationReadiness:
    """
    Analyze data migration readiness from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        DataMigrationReadiness with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return DataMigrationReadiness()

    searchable_text = _extract_searchable_text(task_data)

    return DataMigrationReadiness(
        migration_script_quality_assessed=bool(_MIGRATION_SCRIPT_QUALITY_RE.search(searchable_text)),
        schema_version_tracking_configured=bool(_SCHEMA_VERSION_TRACKING_RE.search(searchable_text)),
        transformation_logic_verified=bool(_TRANSFORMATION_LOGIC_RE.search(searchable_text)),
        rollback_procedure_tested=bool(_ROLLBACK_PROCEDURE_RE.search(searchable_text)),
        zero_downtime_implemented=bool(_ZERO_DOWNTIME_RE.search(searchable_text)),
        post_migration_validation_configured=bool(_POST_MIGRATION_VALIDATION_RE.search(searchable_text)),
        progress_tracking_configured=bool(_PROGRESS_TRACKING_RE.search(searchable_text)),
        migration_testing_coverage_adequate=bool(_MIGRATION_TESTING_COVERAGE_RE.search(searchable_text)),
    )


def _extract_searchable_text(task_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the task data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = task_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = task_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = task_data.get("validation_command") or task_data.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "DataMigrationReadiness",
    "analyze_data_migration_readiness",
]
