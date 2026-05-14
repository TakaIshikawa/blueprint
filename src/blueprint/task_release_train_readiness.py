"""Assess readiness for release train execution tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskReleaseTrainReadinessPlan = SimpleReadinessPlan
TaskReleaseTrainReadinessRecord = SimpleReadinessRecord
TaskReleaseTrainReadinessFinding = SimpleReadinessRecord
TaskReleaseTrainReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "release_train": re.compile(r"\b(?:release train|train release|train promotion|release cadence)\b", re.I),
    "scheduled_release": re.compile(r"\b(?:scheduled releases?|release calendar|release schedule|calendar release)\b", re.I),
    "train_cutoff": re.compile(r"\b(?:train cutoff|scope cutoff|code cutoff|release cutoff)\b", re.I),
    "branch_freeze": re.compile(r"\b(?:branch freeze|code freeze|version freeze|freeze window)\b", re.I),
}
_PATH_SIGNALS = {
    "release_train": re.compile(r"(?:release[_-]?train|train[_-]?promotion|release[_-]?cadence)", re.I),
    "scheduled_release": re.compile(r"(?:release[_-]?calendar|release[_-]?schedule|scheduled[_-]?release)", re.I),
    "train_cutoff": re.compile(r"(?:train[_-]?cutoff|scope[_-]?cutoff|release[_-]?cutoff)", re.I),
    "branch_freeze": re.compile(r"(?:branch|version|release|freeze)", re.I),
}
_CRITERIA = {
    "train_schedule": re.compile(r"\b(?:train schedule|release schedule|release calendar|cadence|scheduled release|calendar date|release date)\b", re.I),
    "scope_cutoff": re.compile(r"\b(?:scope cutoff|train cutoff|code cutoff|feature cutoff|cutoff date|merge cutoff|inclusion deadline)\b", re.I),
    "branch_version_strategy": re.compile(r"\b(?:branch strategy|version strategy|release branch|version branch|tagging strategy|semver|versioning)\b", re.I),
    "approval_gates": re.compile(r"\b(?:approval gates?|release gates?|go/no[- ]go|sign[- ]off|approver|release approval|quality gate)\b", re.I),
    "rollback_hold_process": re.compile(r"\b(?:rollback|hold process|release hold|stop ship|backout|revert|hotfix|pause promotion)\b", re.I),
    "dependency_coordination": re.compile(r"\b(?:dependency coordination|dependent teams?|cross[- ]team|upstream|downstream|dependency owners?|integration dependency)\b", re.I),
    "communication_plan": re.compile(r"\b(?:communication plan|release notes|stakeholder communication|customer communication|announcement|status update)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|pytest|unit tests?|integration tests?|release train tests?|promotion tests?|gate tests?)\b", re.I),
}
_GUIDANCE = {
    "train_schedule": "Define the train schedule, release calendar, cadence, release date, or scheduled-release timeline.",
    "scope_cutoff": "Specify scope, train, code, feature, merge, or inclusion cutoffs.",
    "branch_version_strategy": "Document branch, version, release-branch, tag, semver, or versioning strategy.",
    "approval_gates": "Add approval gates, release gates, go/no-go, sign-off, approvers, or quality gates.",
    "rollback_hold_process": "Define rollback, hold, stop-ship, backout, revert, hotfix, or promotion-pause process.",
    "dependency_coordination": "Coordinate dependent teams, upstream/downstream systems, and dependency owners.",
    "communication_plan": "Add a communication plan, release notes, announcements, or stakeholder/customer updates.",
    "tests": "Add unit, integration, release train, promotion, or gate tests.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:release train|scheduled releases?|train cutoffs?|release calendars?|branch freezes?|train promotion)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b",
    re.I,
)


def build_task_release_train_readiness_plan(source: Any) -> TaskReleaseTrainReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Release Train Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_release_train_readiness = build_task_release_train_readiness_plan
extract_task_release_train_readiness = build_task_release_train_readiness_plan
generate_task_release_train_readiness = build_task_release_train_readiness_plan
derive_task_release_train_readiness = build_task_release_train_readiness_plan
summarize_task_release_train_readiness = build_task_release_train_readiness_plan
summarize_task_release_train_readiness_plan = build_task_release_train_readiness_plan


def recommend_task_release_train_readiness(source: Any) -> tuple[TaskReleaseTrainReadinessRecord, ...]:
    return build_task_release_train_readiness_plan(source).records


def task_release_train_readiness_plan_to_dict(plan: TaskReleaseTrainReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_release_train_readiness_plan_to_dict.__test__ = False


def task_release_train_readiness_plan_to_dicts(
    plan: TaskReleaseTrainReadinessPlan | Iterable[TaskReleaseTrainReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(plan, SimpleReadinessPlan):
        return plan.to_dicts()
    return [record.to_dict() for record in plan]


task_release_train_readiness_plan_to_dicts.__test__ = False
task_release_train_readiness_to_dicts = task_release_train_readiness_plan_to_dicts
task_release_train_readiness_to_dicts.__test__ = False


def task_release_train_readiness_plan_to_markdown(plan: TaskReleaseTrainReadinessPlan) -> str:
    return plan.to_markdown()


task_release_train_readiness_plan_to_markdown.__test__ = False
