"""Extract data export requirements from source brief data."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for export requirements concepts
_EXPORT_FORMATS_RE = re.compile(
    r"(?:export[_\s]+(?:formats?|to)|"
    r"(?:csv|json|excel|xlsx|xls|pdf|xml|parquet|avro)[_\s]+(?:export|format)|"
    r"(?:export|download)[_\s]+(?:as|to|in)[_\s]+(?:csv|json|excel|pdf|xml)|"
    r"file[_\s]+formats?|output[_\s]+formats?|"
    r"(?:support|provide)[_\s]+(?:multiple[_\s]+)?(?:export[_\s]+)?formats?)",
    re.I,
)
_DATA_SCOPE_RE = re.compile(
    r"(?:data[_\s]+scope|export[_\s]+scope|"
    r"(?:full|partial|selective)[_\s]+(?:export|data)|"
    r"(?:all|selected|filtered)[_\s]+(?:records?|data|rows?)|"
    r"scope[_\s]+of[_\s]+export|"
    r"(?:what|which)[_\s]+data[_\s]+to[_\s]+export|"
    r"export[_\s]+(?:criteria|selection|filter))",
    re.I,
)
_FILTERING_OPTIONS_RE = re.compile(
    r"(?:filtering[_\s]+options?|export[_\s]+filters?|"
    r"(?:filter|select)[_\s]+(?:data|records?)(?:[_\s]+(?:for|before)[_\s]+export)?|"
    r"(?:custom|advanced)[_\s]+filters?|"
    r"filter[_\s]+criteria|search[_\s]+(?:and[_\s]+)?(?:filter|export)|"
    r"(?:date|time)[_\s]+range[_\s]+(?:filter|selection))",
    re.I,
)
_SCHEDULING_CAPABILITIES_RE = re.compile(
    r"(?:scheduled?[_\s]+exports?|export[_\s]+scheduling|"
    r"(?:automatic|automated)[_\s]+exports?|"
    r"(?:recurring|periodic)[_\s]+exports?|"
    r"(?:daily|weekly|monthly)[_\s]+exports?|"
    r"schedule[_\s]+(?:export|data[_\s]+export)|"
    r"export[_\s]+(?:automation|on[_\s]+schedule))",
    re.I,
)
_LARGE_DATASET_HANDLING_RE = re.compile(
    r"(?:large[_\s]+datasets?|(?:big|huge|massive)[_\s]+(?:data|files?|exports?)|"
    r"(?:handle|process|export)[_\s]+large[_\s]+(?:volumes?|amounts?|datasets?)|"
    r"(?:millions?|billions?)[_\s]+of[_\s]+records?|"
    r"(?:chunked?|batch|paginated?)[_\s]+export|"
    r"streaming[_\s]+export|"
    r"(?:memory|performance)[_\s]+(?:efficient|optimization)[_\s]+export|"
    r"export[_\s]+(?:pagination|in[_\s]+chunks?))",
    re.I,
)
_INCREMENTAL_EXPORTS_RE = re.compile(
    r"(?:incremental[_\s]+exports?|delta[_\s]+exports?|"
    r"export[_\s]+(?:changes|deltas?|increments?|updates?)|"
    r"(?:only|just)[_\s]+(?:changed?|new|updated?)[_\s]+(?:data|records?)|"
    r"(?:change|delta)[_\s]+detection|"
    r"(?:track|export)[_\s]+modifications?|"
    r"since[_\s]+last[_\s]+export)",
    re.I,
)
_DATA_MASKING_RE = re.compile(
    r"(?:data[_\s]+masking|mask[_\s]+(?:sensitive[_\s]+)?data|"
    r"(?:redact|anonymize|obfuscate)[_\s]+(?:data|pii|sensitive)|"
    r"(?:sensitive|confidential)[_\s]+data[_\s]+(?:protection|handling)|"
    r"(?:pii|personal[_\s]+data)[_\s]+(?:masking|protection|redaction)|"
    r"data[_\s]+(?:anonymization|obfuscation))",
    re.I,
)
_FORMAT_COMPATIBILITY_RE = re.compile(
    r"(?:format[_\s]+compatibility|compatible[_\s]+formats?|"
    r"compatible[_\s]+with[_\s]+(?:excel|csv|json|pdf)|"
    r"(?:cross|multi)[_\s-]*platform[_\s]+(?:format|compatibility)|"
    r"standard[_\s]+formats?|industry[_\s]+standard|"
    r"(?:ensure|maintain)[_\s]+(?:format[_\s]+)?compatibility)",
    re.I,
)
_DELIVERY_METHODS_RE = re.compile(
    r"(?:delivery[_\s]+methods?|export[_\s]+delivery|"
    r"(?:download|email|ftp|sftp|s3|cloud)[_\s]+(?:export|delivery)|"
    r"(?:send|deliver|transfer)[_\s]+export|"
    r"(?:upload|save|store)[_\s]+(?:to|in)[_\s]+(?:s3|cloud|server|ftp)|"
    r"delivery[_\s]+(?:options?|mechanisms?))",
    re.I,
)
_USER_EXPERIENCE_RE = re.compile(
    r"(?:user[_\s]+experience|export[_\s]+(?:ui|interface|experience)|"
    r"(?:easy|simple|intuitive)[_\s]+(?:export|to[_\s]+use)|"
    r"progress[_\s]+(?:indicator|bar|tracking)|"
    r"(?:export|download)[_\s]+progress|"
    r"(?:user[_\s]+)?friendly[_\s]+(?:export|interface)|"
    r"export[_\s]+(?:wizard|flow|process))",
    re.I,
)


@dataclass(frozen=True, slots=True)
class ExportRequirements:
    """Data export requirements extracted from source brief."""

    export_formats_defined: bool = False
    data_scope_specified: bool = False
    filtering_options_planned: bool = False
    scheduling_capabilities_planned: bool = False
    large_dataset_handling_addressed: bool = False
    incremental_exports_supported: bool = False
    data_masking_planned: bool = False
    format_compatibility_ensured: bool = False
    delivery_methods_defined: bool = False
    user_experience_considered: bool = False

    @property
    def completeness_score(self) -> float:
        """Calculate completeness score (0.0 to 1.0)."""
        total_checks = 10
        passed_checks = sum([
            self.export_formats_defined,
            self.data_scope_specified,
            self.filtering_options_planned,
            self.scheduling_capabilities_planned,
            self.large_dataset_handling_addressed,
            self.incremental_exports_supported,
            self.data_masking_planned,
            self.format_compatibility_ensured,
            self.delivery_methods_defined,
            self.user_experience_considered,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "export_formats_defined": self.export_formats_defined,
            "data_scope_specified": self.data_scope_specified,
            "filtering_options_planned": self.filtering_options_planned,
            "scheduling_capabilities_planned": self.scheduling_capabilities_planned,
            "large_dataset_handling_addressed": self.large_dataset_handling_addressed,
            "incremental_exports_supported": self.incremental_exports_supported,
            "data_masking_planned": self.data_masking_planned,
            "format_compatibility_ensured": self.format_compatibility_ensured,
            "delivery_methods_defined": self.delivery_methods_defined,
            "user_experience_considered": self.user_experience_considered,
            "completeness_score": self.completeness_score,
        }


def extract_export_requirements(source_data: Mapping[str, Any]) -> ExportRequirements:
    """
    Extract export requirements from source brief data.

    Args:
        source_data: A mapping containing source brief information with fields like
                    'title', 'description', 'requirements', etc.

    Returns:
        ExportRequirements with boolean flags for each aspect and overall score.
    """
    if not isinstance(source_data, Mapping):
        return ExportRequirements()

    searchable_text = _extract_searchable_text(source_data)

    return ExportRequirements(
        export_formats_defined=bool(_EXPORT_FORMATS_RE.search(searchable_text)),
        data_scope_specified=bool(_DATA_SCOPE_RE.search(searchable_text)),
        filtering_options_planned=bool(_FILTERING_OPTIONS_RE.search(searchable_text)),
        scheduling_capabilities_planned=bool(_SCHEDULING_CAPABILITIES_RE.search(searchable_text)),
        large_dataset_handling_addressed=bool(_LARGE_DATASET_HANDLING_RE.search(searchable_text)),
        incremental_exports_supported=bool(_INCREMENTAL_EXPORTS_RE.search(searchable_text)),
        data_masking_planned=bool(_DATA_MASKING_RE.search(searchable_text)),
        format_compatibility_ensured=bool(_FORMAT_COMPATIBILITY_RE.search(searchable_text)),
        delivery_methods_defined=bool(_DELIVERY_METHODS_RE.search(searchable_text)),
        user_experience_considered=bool(_USER_EXPERIENCE_RE.search(searchable_text)),
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
    for field in ("requirements", "acceptance_criteria", "notes", "features", "specifications"):
        value = source_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "ExportRequirements",
    "extract_export_requirements",
]
