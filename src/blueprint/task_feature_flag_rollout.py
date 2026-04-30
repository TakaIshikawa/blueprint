"""Infer feature-flag rollout guidance for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


RolloutPhase = Literal["introduce", "guard", "monitor", "cleanup"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SLUG_RE = re.compile(r"[a-z0-9]+")
_STRONG_FLAG_RE = re.compile(
    r"\b(?:feature[- ]?flags?|release[- ]?flags?|feature[- ]?toggles?|toggles?|"
    r"rollouts?|experiments?|beta|kill switch(?:es)?)\b",
    re.IGNORECASE,
)
_BARE_FLAG_RE = re.compile(r"\bflags?\b", re.IGNORECASE)
_RELEASE_CONTEXT_RE = re.compile(
    r"\b(?:feature|release|rollout|deploy|deployment|beta|experiment|toggle|"
    r"kill switch|enable|disable|segment|cohort|percentage|gradual|dark launch)\b",
    re.IGNORECASE,
)
_ORDINARY_FLAG_RE = re.compile(
    r"\b(?:cli|command line|argparse|click|typer|option|argument|parser|"
    r"lint|compiler|country|status|fraud|red flag|warning flag)\b",
    re.IGNORECASE,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|disable|turn off|kill switch|revert|fallback|backout)\b",
    re.IGNORECASE,
)
_MONITORING_RE = re.compile(
    r"\b(?:monitor|metric|metrics|alert|dashboard|log|logging|observe|observability|"
    r"telemetry|slo|error rate)\b",
    re.IGNORECASE,
)
_CLEANUP_RE = re.compile(
    r"\b(?:cleanup|clean up|remove|delete|retire|decommission|sunset|remove flag|"
    r"delete flag)\b",
    re.IGNORECASE,
)
_GUARD_RE = re.compile(
    r"\b(?:guard|gate|gated|disable|kill switch|rollback|fallback|off switch|"
    r"protect|default off)\b",
    re.IGNORECASE,
)
_CONFIG_PATH_PARTS = {
    "config",
    "configs",
    "configuration",
    "settings",
    "feature_flags",
    "feature-flags",
    "flags",
    "environments",
}
_CONFIG_PATH_NAMES = {
    "feature_flags.yaml",
    "feature_flags.yml",
    "feature-flags.yaml",
    "feature-flags.yml",
    "flags.yaml",
    "flags.yml",
}
_STOP_WORDS = {
    "a",
    "add",
    "and",
    "beta",
    "build",
    "create",
    "enable",
    "feature",
    "flag",
    "for",
    "guard",
    "implement",
    "introduce",
    "release",
    "rollout",
    "task",
    "the",
    "to",
    "toggle",
    "under",
    "update",
}


@dataclass(frozen=True, slots=True)
class TaskFeatureFlagRolloutRecommendation:
    """Rollout guidance for one task that appears to need a feature flag."""

    task_id: str
    title: str
    flag_name: str
    rollout_phase: RolloutPhase
    required_checks: tuple[str, ...] = field(default_factory=tuple)
    missing_evidence: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "flag_name": self.flag_name,
            "rollout_phase": self.rollout_phase,
            "required_checks": list(self.required_checks),
            "missing_evidence": list(self.missing_evidence),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskFeatureFlagRolloutPlan:
    """Feature-flag rollout recommendations for an execution plan."""

    plan_id: str | None = None
    recommendations: tuple[TaskFeatureFlagRolloutRecommendation, ...] = field(
        default_factory=tuple
    )
    flagged_task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [
                recommendation.to_dict() for recommendation in self.recommendations
            ],
            "flagged_task_ids": list(self.flagged_task_ids),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return rollout recommendation records as plain dictionaries."""
        return [recommendation.to_dict() for recommendation in self.recommendations]

    def to_markdown(self) -> str:
        """Render the rollout plan as deterministic Markdown."""
        title = "# Task Feature Flag Rollout Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.recommendations:
            lines.extend(["", "No feature-flag rollout guidance was derived."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Flag | Phase | Required Checks | Missing Evidence |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for recommendation in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(recommendation.task_id)}` | "
                f"`{_markdown_cell(recommendation.flag_name)}` | "
                f"{recommendation.rollout_phase} | "
                f"{_markdown_cell('; '.join(recommendation.required_checks) or 'None')} | "
                f"{_markdown_cell('; '.join(recommendation.missing_evidence) or 'None')} |"
            )
        return "\n".join(lines)


def build_task_feature_flag_rollout_plan(
    source: Mapping[str, Any] | ExecutionPlan,
) -> TaskFeatureFlagRolloutPlan:
    """Recommend rollout checks for tasks likely to require feature flags."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    cleanup_flag_names = {
        _flag_name(task, index)
        for index, task in enumerate(tasks, start=1)
        if _is_cleanup_task(task)
    }

    recommendations = tuple(
        recommendation
        for index, task in enumerate(tasks, start=1)
        if (recommendation := _recommendation(task, index, cleanup_flag_names)) is not None
    )
    return TaskFeatureFlagRolloutPlan(
        plan_id=_optional_text(plan.get("id")),
        recommendations=recommendations,
        flagged_task_ids=tuple(recommendation.task_id for recommendation in recommendations),
    )


def task_feature_flag_rollout_plan_to_dict(
    result: TaskFeatureFlagRolloutPlan,
) -> dict[str, Any]:
    """Serialize a feature-flag rollout plan to a plain dictionary."""
    return result.to_dict()


task_feature_flag_rollout_plan_to_dict.__test__ = False


def task_feature_flag_rollout_plan_to_markdown(
    result: TaskFeatureFlagRolloutPlan,
) -> str:
    """Render a feature-flag rollout plan as Markdown."""
    return result.to_markdown()


task_feature_flag_rollout_plan_to_markdown.__test__ = False


def _recommendation(
    task: Mapping[str, Any],
    index: int,
    cleanup_flag_names: set[str],
) -> TaskFeatureFlagRolloutRecommendation | None:
    evidence = _flag_evidence(task)
    if not evidence:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    flag_name = _flag_name(task, index)
    phase = _rollout_phase(task)
    task_text = _task_context(task)
    required_checks = _required_checks(phase)
    missing_evidence = _missing_evidence(
        task_text=task_text,
        phase=phase,
        flag_name=flag_name,
        cleanup_flag_names=cleanup_flag_names,
    )
    return TaskFeatureFlagRolloutRecommendation(
        task_id=task_id,
        title=title,
        flag_name=flag_name,
        rollout_phase=phase,
        required_checks=required_checks,
        missing_evidence=missing_evidence,
        evidence=tuple(_dedupe(evidence)),
    )


def _flag_evidence(task: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _is_flag_config_path(path):
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _task_texts(task):
        if _is_release_flag_text(text):
            evidence.append(f"{source_field}: {text}")

    for source_field, text in _metadata_texts(task.get("metadata")):
        if _is_release_flag_text(f"{source_field} {text}"):
            evidence.append(f"{source_field}: {text}")

    for source_field, text in _tag_texts(task):
        if _is_release_flag_text(text):
            evidence.append(f"{source_field}: {text}")

    return evidence


def _is_release_flag_text(text: str) -> bool:
    if _STRONG_FLAG_RE.search(text):
        return True
    return bool(
        _BARE_FLAG_RE.search(text)
        and _RELEASE_CONTEXT_RE.search(text)
        and not _ORDINARY_FLAG_RE.search(text)
    )


def _is_flag_config_path(value: str) -> bool:
    path = PurePosixPath(_normalized_path(value).lower())
    parts = set(path.parts)
    return (
        path.name in _CONFIG_PATH_NAMES
        or bool(parts & _CONFIG_PATH_PARTS)
        and (path.suffix in {".env", ".yaml", ".yml", ".json", ".toml"} or "flag" in path.name)
    )


def _rollout_phase(task: Mapping[str, Any]) -> RolloutPhase:
    context = _task_context(task)
    if _CLEANUP_RE.search(context):
        return "cleanup"
    if _MONITORING_RE.search(context):
        return "monitor"
    if _GUARD_RE.search(context):
        return "guard"
    return "introduce"


def _required_checks(phase: RolloutPhase) -> tuple[str, ...]:
    checks = {
        "introduce": (
            "Flag has a documented default and owner.",
            "Rollout starts disabled or scoped to a safe cohort.",
            "Rollback path is captured in acceptance criteria.",
            "Monitoring signal is captured in acceptance criteria.",
            "Cleanup or retirement task exists for the flag.",
        ),
        "guard": (
            "Guarded code path defaults to the current behavior.",
            "Disable or rollback behavior is validated.",
            "Monitoring signal is captured in acceptance criteria.",
            "Cleanup or retirement task exists for the flag.",
        ),
        "monitor": (
            "Rollout metrics, alerts, or dashboards are defined.",
            "Rollback threshold is documented.",
            "Cleanup or retirement task exists for the flag.",
        ),
        "cleanup": (
            "Flag removal covers enabled and disabled code paths.",
            "Configuration and documentation references are removed.",
            "Post-cleanup regression validation is defined.",
        ),
    }[phase]
    return checks


def _missing_evidence(
    *,
    task_text: str,
    phase: RolloutPhase,
    flag_name: str,
    cleanup_flag_names: set[str],
) -> tuple[str, ...]:
    missing: list[str] = []
    if phase != "cleanup" and not _ROLLBACK_RE.search(task_text):
        missing.append("no rollback criterion")
    if phase != "cleanup" and not _MONITORING_RE.search(task_text):
        missing.append("no monitoring criterion")
    if phase != "cleanup" and flag_name not in cleanup_flag_names:
        missing.append("no cleanup task")
    if phase == "cleanup" and not re.search(
        r"\b(?:regression|test|validate|validation)\b",
        task_text,
        re.IGNORECASE,
    ):
        missing.append("no cleanup validation criterion")
    return tuple(missing)


def _is_cleanup_task(task: Mapping[str, Any]) -> bool:
    return bool(_flag_evidence(task) and _rollout_phase(task) == "cleanup")


def _flag_name(task: Mapping[str, Any], index: int) -> str:
    metadata = task.get("metadata")
    for value in (
        task.get("flag_name"),
        task.get("feature_flag"),
        metadata.get("flag_name") if isinstance(metadata, Mapping) else None,
        metadata.get("feature_flag") if isinstance(metadata, Mapping) else None,
    ):
        if text := _optional_text(value):
            return _slug(text, drop_generic=False)

    title = _optional_text(task.get("title")) or _optional_text(task.get("id")) or f"task-{index}"
    return _slug(title)


def _slug(value: str, *, drop_generic: bool = True) -> str:
    words = _SLUG_RE.findall(value.casefold())
    if drop_generic:
        words = [word for word in words if word not in _STOP_WORDS]
    return "_".join(words[:5]) or "feature_flag"


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "milestone", "owner_type", "suggested_engine"):
        text = _optional_text(task.get(field_name))
        if text:
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    return texts


def _tag_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _task_context(task: Mapping[str, Any]) -> str:
    values = [text for _, text in _task_texts(task)]
    values.extend(_strings(task.get("metadata")))
    values.extend(text for _, text in _tag_texts(task))
    return " ".join(values)


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "RolloutPhase",
    "TaskFeatureFlagRolloutPlan",
    "TaskFeatureFlagRolloutRecommendation",
    "build_task_feature_flag_rollout_plan",
    "task_feature_flag_rollout_plan_to_dict",
    "task_feature_flag_rollout_plan_to_markdown",
]
