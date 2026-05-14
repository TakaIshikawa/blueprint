"""Assess readiness for deterministic task data seeding work."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import (
    SimpleReadinessPlan,
    SimpleReadinessRecord,
    build_simple_readiness_plan,
)


TaskDataSeedingReadinessPlan = SimpleReadinessPlan
TaskDataSeedingReadinessRecord = SimpleReadinessRecord
TaskDataSeedingReadinessFinding = SimpleReadinessRecord
TaskDataSeedingReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "data_seeding": re.compile(
        r"\b(?:data seeding|seed data|seed database|seed fixtures?|seed records?|"
        r"seed(?:\s+\w+){0,3}\s+accounts?|database seed|initial data|reference data load|"
        r"bootstrap data|demo data)\b",
        re.I,
    ),
    "fixture_load": re.compile(
        r"\b(?:fixtures?|fixture load|test data|sample data|sample-data|golden data|"
        r"example data|mock records?)\b",
        re.I,
    ),
    "migration_seed": re.compile(
        r"\b(?:migration seed|seed migration|post-migration seed|migrate and seed|"
        r"backfill seed|baseline data)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "data_seeding": re.compile(r"(?:^|/)(?:db/)?seeds?(?:/|$)|seed(?:s|ing)?|bootstrap[_-]?data", re.I),
    "fixture_load": re.compile(r"fixtures?|sample\s*data|test\s*data|demo\s*data|golden\s*data", re.I),
    "migration_seed": re.compile(r"migrations?.*(?:seed|reference|baseline)|(?:seed|reference|baseline).*migrations?", re.I),
}
_CRITERIA = {
    "seed_source": re.compile(
        r"\b(?:seed source|source data|source file|fixture source|canonical source|"
        r"reference dataset|csv|json|yaml|sql dump|snapshot|sample-data file)\b",
        re.I,
    ),
    "idempotency": re.compile(
        r"\b(?:idempotent|idempotency|safe to rerun|rerunnable|upsert|on conflict|"
        r"dedupe|de-?duplicate|natural key|unique constraint|duplicate guard)\b",
        re.I,
    ),
    "environment_targeting": re.compile(
        r"\b(?:environment target|target environment|dev|development|staging|sandbox|"
        r"test environment|non-production|production guard|env var|environment flag)\b",
        re.I,
    ),
    "cleanup_or_rollback": re.compile(
        r"\b(?:cleanup|clean up|rollback|roll back|remove seeded|delete seeded|undo|"
        r"restore|snapshot|truncate|teardown|revert)\b",
        re.I,
    ),
    "owner": re.compile(
        r"\b(?:owner|owned by|responsible team|dri|data steward|maintainer|on-call|"
        r"approver|accountable)\b",
        re.I,
    ),
    "validation_checks": re.compile(
        r"\b(?:validation|validate|verify|smoke test|assertion|row count|record count|"
        r"checksum|referential integrity|foreign key|post-run check|acceptance check)\b",
        re.I,
    ),
    "sensitive_data_masking": re.compile(
        r"\b(?:mask|masked|masking|redact|redacted|anonymi[sz]e|scrub|saniti[sz]e|"
        r"synthetic|fake pii|no pii|no personal data|sensitive data)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "seed_source": "Identify the canonical seed source such as a fixture, dump, CSV, JSON, or reference dataset.",
    "idempotency": "Make seed execution safe to rerun using upserts, unique keys, dedupe guards, or equivalent controls.",
    "environment_targeting": "Specify target environments and production guards for the seeding run.",
    "cleanup_or_rollback": "Document cleanup, rollback, restore, truncate, or teardown steps for seeded records.",
    "owner": "Name the owner, DRI, responsible team, approver, or data steward for the seed data.",
    "validation_checks": "Add validation checks such as counts, smoke tests, checksums, or integrity assertions.",
    "sensitive_data_masking": "Confirm sensitive data is masked, synthetic, anonymized, scrubbed, or explicitly absent.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:seed data|data seeding|fixtures?|sample data)\b"
    r".{0,80}\b(?:required|needed|planned|scope|impact|changes?)\b",
    re.I,
)


def build_task_data_seeding_readiness_plan(source: Any) -> TaskDataSeedingReadinessPlan:
    """Build data seeding readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Data Seeding Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_data_seeding_readiness(source: Any) -> TaskDataSeedingReadinessPlan:
    return build_task_data_seeding_readiness_plan(source)


def extract_task_data_seeding_readiness(source: Any) -> TaskDataSeedingReadinessPlan:
    return build_task_data_seeding_readiness_plan(source)


def generate_task_data_seeding_readiness(source: Any) -> TaskDataSeedingReadinessPlan:
    return build_task_data_seeding_readiness_plan(source)


def derive_task_data_seeding_readiness(source: Any) -> TaskDataSeedingReadinessPlan:
    return build_task_data_seeding_readiness_plan(source)


def summarize_task_data_seeding_readiness(source: Any) -> TaskDataSeedingReadinessPlan:
    return build_task_data_seeding_readiness_plan(source)


def summarize_task_data_seeding_readiness_plan(source: Any) -> TaskDataSeedingReadinessPlan:
    return build_task_data_seeding_readiness_plan(source)


def recommend_task_data_seeding_readiness(source: Any) -> tuple[TaskDataSeedingReadinessRecord, ...]:
    return build_task_data_seeding_readiness_plan(source).records


def task_data_seeding_readiness_plan_to_dict(result: TaskDataSeedingReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_data_seeding_readiness_plan_to_dict.__test__ = False


def task_data_seeding_readiness_plan_to_dicts(
    result: TaskDataSeedingReadinessPlan | Iterable[TaskDataSeedingReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_data_seeding_readiness_plan_to_dicts.__test__ = False
task_data_seeding_readiness_to_dicts = task_data_seeding_readiness_plan_to_dicts
task_data_seeding_readiness_to_dicts.__test__ = False


def task_data_seeding_readiness_plan_to_markdown(result: TaskDataSeedingReadinessPlan) -> str:
    return result.to_markdown()


task_data_seeding_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskDataSeedingReadinessFinding",
    "TaskDataSeedingReadinessPlan",
    "TaskDataSeedingReadinessRecord",
    "TaskDataSeedingReadinessRecommendation",
    "analyze_task_data_seeding_readiness",
    "build_task_data_seeding_readiness_plan",
    "derive_task_data_seeding_readiness",
    "extract_task_data_seeding_readiness",
    "generate_task_data_seeding_readiness",
    "recommend_task_data_seeding_readiness",
    "summarize_task_data_seeding_readiness",
    "summarize_task_data_seeding_readiness_plan",
    "task_data_seeding_readiness_plan_to_dict",
    "task_data_seeding_readiness_plan_to_dicts",
    "task_data_seeding_readiness_plan_to_markdown",
    "task_data_seeding_readiness_to_dicts",
]
