"""Assess task-level readiness for data lineage implementation work."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._task_safeguard_readiness import (
    TaskSafeguardReadinessPlan,
    TaskSafeguardReadinessRecord,
    build_task_safeguard_readiness_plan,
)


TaskDataLineageReadinessRecord = TaskSafeguardReadinessRecord
TaskDataLineageReadinessPlan = TaskSafeguardReadinessPlan

_SIGNALS = {
    "data_lineage": re.compile(r"\b(?:data lineage|lineage graph|lineage tracking|dataset lineage)\b", re.I),
    "provenance": re.compile(r"\b(?:provenance|source provenance|transformation provenance|derived from)\b", re.I),
    "pipeline": re.compile(r"\b(?:pipeline|etl|elt|data job|workflow|dag|orchestration)\b", re.I),
    "transformation": re.compile(r"\b(?:transformation|transform|mapper|aggregation|normalization|derivation)\b", re.I),
    "dataset_dependency": re.compile(r"\b(?:upstream dataset|downstream dataset|dataset dependency|dependency graph|source dataset)\b", re.I),
    "backfill": re.compile(r"\b(?:backfill|historical reload|reprocess history|bootstrap lineage)\b", re.I),
    "job_run": re.compile(r"\b(?:job id|run id|job run|workflow run|execution id|batch id)\b", re.I),
    "schema_version": re.compile(r"\b(?:schema version|dataset version|contract version|versioned schema)\b", re.I),
}
_PATH_SIGNALS = {
    "data_lineage": re.compile(r"lineage", re.I),
    "provenance": re.compile(r"provenance|source", re.I),
    "pipeline": re.compile(r"pipeline|etl|elt|dag|job|workflow", re.I),
    "transformation": re.compile(r"transform|mapper|aggregation|normalize", re.I),
    "dataset_dependency": re.compile(r"dataset|dependency|upstream|downstream", re.I),
    "backfill": re.compile(r"backfill|reload|reprocess|bootstrap", re.I),
    "job_run": re.compile(r"run|execution|batch|job", re.I),
    "schema_version": re.compile(r"schema|version|contract", re.I),
}
_SAFEGUARDS = {
    "lineage_metadata": re.compile(
        r"\b(?:lineage metadata|metadata capture|source and target metadata|upstream and downstream metadata)\b",
        re.I,
    ),
    "owner_mapping": re.compile(r"\b(?:owner mapping|data owner|dataset owner|steward|ownership)\b", re.I),
    "validation_checks": re.compile(r"\b(?:validation checks?|lineage validation|validate lineage|integrity checks?|contract tests?)\b", re.I),
    "schema_version_tracking": re.compile(
        r"\b(?:schema version tracking|schema version|dataset version|contract version|version tracking)\b",
        re.I,
    ),
    "observability": re.compile(r"\b(?:observability|monitoring|metrics?|alerts?|dashboard|tracing|lineage freshness)\b", re.I),
    "backfill_handling": re.compile(r"\b(?:backfill handling|backfill plan|historical reload|reprocess history|bootstrap lineage)\b", re.I),
    "run_id_tracking": re.compile(r"\b(?:run id tracking|job id|run id|job run|execution id|batch id)\b", re.I),
}
_GUIDANCE = {
    "lineage_metadata": "Capture lineage metadata for upstream sources, transformations, outputs, and downstream consumers.",
    "owner_mapping": "Map datasets and lineage edges to data owners or stewards for review and escalation.",
    "validation_checks": "Add validation checks that prove lineage edges, dependencies, and transformations are correct.",
    "schema_version_tracking": "Track schema or contract versions for every lineage-producing and lineage-consuming dataset.",
    "observability": "Monitor lineage freshness, missing edges, failed captures, and pipeline/run correlation.",
    "backfill_handling": "Define backfill handling for historical data, replayed jobs, and bootstrap lineage records.",
    "run_id_tracking": "Persist job IDs, run IDs, execution IDs, or batch IDs with lineage records.",
}
_HIGH_IMPACT = {"data_lineage", "provenance", "pipeline", "dataset_dependency", "backfill"}


def build_task_data_lineage_readiness_plan(source: Any) -> TaskDataLineageReadinessPlan:
    return build_task_safeguard_readiness_plan(
        source,
        title="Task Data Lineage Readiness",
        task_count_label="lineage_task_count",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        safeguard_patterns=_SAFEGUARDS,
        safeguard_guidance=_GUIDANCE,
        high_impact_signals=_HIGH_IMPACT,
    )


def analyze_task_data_lineage_readiness(source: Any) -> TaskDataLineageReadinessPlan:
    return build_task_data_lineage_readiness_plan(source)


def extract_task_data_lineage_readiness(source: Any) -> TaskDataLineageReadinessPlan:
    return build_task_data_lineage_readiness_plan(source)


def generate_task_data_lineage_readiness(source: Any) -> TaskDataLineageReadinessPlan:
    return build_task_data_lineage_readiness_plan(source)


def derive_task_data_lineage_readiness(source: Any) -> TaskDataLineageReadinessPlan:
    return build_task_data_lineage_readiness_plan(source)


def summarize_task_data_lineage_readiness(source: Any) -> TaskDataLineageReadinessPlan:
    return build_task_data_lineage_readiness_plan(source)


def recommend_task_data_lineage_readiness(source: Any) -> TaskDataLineageReadinessPlan:
    return build_task_data_lineage_readiness_plan(source)


def task_data_lineage_readiness_plan_to_dict(report: TaskDataLineageReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_data_lineage_readiness_plan_to_dict.__test__ = False


def task_data_lineage_readiness_plan_to_dicts(
    report: TaskDataLineageReadinessPlan | Iterable[TaskDataLineageReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(report, TaskSafeguardReadinessPlan):
        return report.to_dicts()
    return [record.to_dict() for record in report]


task_data_lineage_readiness_plan_to_dicts.__test__ = False
task_data_lineage_readiness_to_dicts = task_data_lineage_readiness_plan_to_dicts
task_data_lineage_readiness_to_dicts.__test__ = False


def task_data_lineage_readiness_plan_to_markdown(report: TaskDataLineageReadinessPlan) -> str:
    return report.to_markdown()


task_data_lineage_readiness_plan_to_markdown.__test__ = False
