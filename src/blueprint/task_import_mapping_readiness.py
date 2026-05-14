"""Assess readiness for import mapping and ingestion tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskImportMappingReadinessPlan = SimpleReadinessPlan
TaskImportMappingReadinessRecord = SimpleReadinessRecord
TaskImportMappingReadinessFinding = SimpleReadinessRecord
TaskImportMappingReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "import_mapping": re.compile(r"\b(?:import mapping|field mapping|source to target|source-to-target|column mapping|csv mapping|ingestion mapping)\b", re.I),
    "csv_import": re.compile(r"\b(?:csv import|csv ingestion|spreadsheet import|bulk import|file import|upload import|import job)\b", re.I),
    "data_ingestion": re.compile(r"\b(?:data ingestion|ingest|etl|load file|parser|mapping template|sample fixture)\b", re.I),
}
_PATH_SIGNALS = {
    "import_mapping": re.compile(r"mapping|field[_-]?map|source[_-]?target|columns?", re.I),
    "csv_import": re.compile(r"csv|imports?|ingest|upload|spreadsheet", re.I),
    "data_ingestion": re.compile(r"fixtures?|samples?|testdata|etl|parser|schema", re.I),
}
_CRITERIA = {
    "field_mapping": re.compile(r"\b(?:field mapping|column mapping|source to target|source-to-target|target fields?|mapping table|mapping template)\b", re.I),
    "transform_rules": re.compile(r"\b(?:transform rules?|transformation|normalize|format conversion|derive|trim|uppercase|lowercase|mapping logic)\b", re.I),
    "required_fields": re.compile(r"\b(?:required fields?|mandatory fields?|missing required|nullable|default values?|required column)\b", re.I),
    "type_coercion": re.compile(r"\b(?:type coercion|type conversion|cast|parse date|date format|number format|boolean conversion|enum conversion)\b", re.I),
    "identifier_mapping": re.compile(r"\b(?:identifier mapping|external id|foreign key|lookup key|natural key|id mapping|dedupe key|match key)\b", re.I),
    "validation_errors": re.compile(r"\b(?:validation errors?|error report|reject file|row errors?|invalid rows?|error handling|failure reason)\b", re.I),
    "sample_fixtures": re.compile(r"\b(?:sample fixtures?|sample files?|example csv|golden file|test fixture|fixture file|sample data)\b", re.I),
}
_GUIDANCE = {
    "field_mapping": "Document source-to-target field or column mappings.",
    "transform_rules": "Specify transform, normalization, derivation, or formatting rules.",
    "required_fields": "Define required-field handling, defaults, nullability, and missing-field errors.",
    "type_coercion": "Describe type coercion for dates, numbers, booleans, enums, and invalid values.",
    "identifier_mapping": "Define identifier, external ID, lookup, foreign-key, dedupe, or match-key mapping.",
    "validation_errors": "Specify validation error reporting, rejected rows, failure reasons, and handling.",
    "sample_fixtures": "Provide sample import files, fixtures, golden files, or sample data.",
}
_NO_IMPACT = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:import|ingestion|csv|mapping)\b.{0,80}\b(?:changes?|required|planned|impact)\b", re.I)


def build_task_import_mapping_readiness_plan(source: Any) -> TaskImportMappingReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Import Mapping Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_import_mapping_readiness = build_task_import_mapping_readiness_plan
extract_task_import_mapping_readiness = build_task_import_mapping_readiness_plan
generate_task_import_mapping_readiness = build_task_import_mapping_readiness_plan
derive_task_import_mapping_readiness = build_task_import_mapping_readiness_plan
summarize_task_import_mapping_readiness = build_task_import_mapping_readiness_plan
summarize_task_import_mapping_readiness_plan = build_task_import_mapping_readiness_plan


def recommend_task_import_mapping_readiness(source: Any) -> tuple[TaskImportMappingReadinessRecord, ...]:
    return build_task_import_mapping_readiness_plan(source).records


def task_import_mapping_readiness_plan_to_dict(plan: TaskImportMappingReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_import_mapping_readiness_plan_to_dict.__test__ = False


def task_import_mapping_readiness_plan_to_dicts(plan: TaskImportMappingReadinessPlan | Iterable[TaskImportMappingReadinessRecord]) -> list[dict[str, Any]]:
    if isinstance(plan, SimpleReadinessPlan):
        return plan.to_dicts()
    return [record.to_dict() for record in plan]


task_import_mapping_readiness_plan_to_dicts.__test__ = False
task_import_mapping_readiness_to_dicts = task_import_mapping_readiness_plan_to_dicts
task_import_mapping_readiness_to_dicts.__test__ = False


def task_import_mapping_readiness_plan_to_markdown(plan: TaskImportMappingReadinessPlan) -> str:
    return plan.to_markdown()


task_import_mapping_readiness_plan_to_markdown.__test__ = False

