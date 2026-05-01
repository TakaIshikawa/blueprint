"""Plan feature-flag sunset work for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FeatureFlagSunsetTrigger = Literal[
    "validation_complete",
    "rollout_complete",
    "experiment_concluded",
]
FeatureFlagCategory = Literal["rollout", "experiment", "configuration"]
_T = TypeVar("_T")

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SPACE_RE = re.compile(r"\s+")
_EXPLICIT_FLAG_RE = re.compile(
    r"\b(?:feature[- ]?flags?|release[- ]?flags?|feature[- ]?toggles?|toggles?|"
    r"a/b tests?|ab tests?|kill switch(?:es)?)\b",
    re.IGNORECASE,
)
_BARE_FLAG_RE = re.compile(r"\bflags?\b", re.IGNORECASE)
_ROLLOUT_EXPERIMENT_RE = re.compile(r"\b(?:rollouts?|experiments?|beta)\b", re.IGNORECASE)
_CONTEXT_RE = re.compile(
    r"\b(?:feature|release|rollout|deploy|deployment|beta|experiment|toggle|"
    r"cohort|segment|percentage|gradual|dark launch|configuration|config|settings|"
    r"enabled|disabled|remove|cleanup|sunset|retire)\b",
    re.IGNORECASE,
)
_NEGATED_IMPACT_RE = re.compile(
    r"\b(?:no|without)\s+(?:rollout|experiment|feature flag|toggle|flag)\b",
    re.IGNORECASE,
)
_ORDINARY_FLAG_RE = re.compile(
    r"\b(?:cli|command line|argparse|click|typer|option|argument|parser|"
    r"lint|compiler|country|status|fraud|red flag|warning flag)\b",
    re.IGNORECASE,
)
_EXPERIMENT_RE = re.compile(
    r"\b(?:experiment|experiments|a/b|ab test|split test|variant|cohort|"
    r"conversion|hypothesis|winner|concluded)\b",
    re.IGNORECASE,
)
_ROLLOUT_RE = re.compile(
    r"\b(?:rollout|release flag|beta|percentage|gradual|cohort|segment|"
    r"dark launch|enable|disable|kill switch)\b",
    re.IGNORECASE,
)
_CONFIG_RE = re.compile(
    r"\b(?:config|configuration|settings|environment|env var|environment variable|"
    r"feature_flags?|flags?\.ya?ml|flags?\.json|flags?\.toml)\b",
    re.IGNORECASE,
)
_VALIDATION_RE = re.compile(r"\b(?:test|tests|pytest|validate|validation|verify|verified)\b", re.I)
_ROLLOUT_COMPLETE_RE = re.compile(
    r"\b(?:100%|fully rolled out|rollout complete|all users|general availability|ga|enabled for all)\b",
    re.IGNORECASE,
)
_EXPERIMENT_CONCLUDED_RE = re.compile(
    r"\b(?:experiment concluded|winner selected|results reviewed|hypothesis decided|"
    r"statistically significant|decision made)\b",
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
    "flags.json",
    "flags.toml",
}
_STOP_WORDS = {
    "a",
    "add",
    "and",
    "beta",
    "build",
    "cleanup",
    "config",
    "configuration",
    "create",
    "enable",
    "feature",
    "flag",
    "for",
    "implement",
    "introduce",
    "release",
    "remove",
    "rollout",
    "sunset",
    "task",
    "the",
    "to",
    "toggle",
    "under",
    "update",
}


@dataclass(frozen=True, slots=True)
class TaskFeatureFlagSunsetRecommendation:
    """Sunset guidance for one task that appears to introduce or manage a flag."""

    task_id: str
    title: str
    flag_name: str
    category: FeatureFlagCategory
    cleanup_trigger: FeatureFlagSunsetTrigger
    owner_role: str
    validation_command: str
    suggested_acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "flag_name": self.flag_name,
            "category": self.category,
            "cleanup_trigger": self.cleanup_trigger,
            "owner_role": self.owner_role,
            "validation_command": self.validation_command,
            "suggested_acceptance_criteria": list(self.suggested_acceptance_criteria),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskFeatureFlagSunsetPlan:
    """Feature-flag sunset recommendations for a plan or task collection."""

    plan_id: str | None = None
    recommendations: tuple[TaskFeatureFlagSunsetRecommendation, ...] = field(
        default_factory=tuple
    )
    flagged_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [
                recommendation.to_dict() for recommendation in self.recommendations
            ],
            "flagged_task_ids": list(self.flagged_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return sunset recommendation records as plain dictionaries."""
        return [recommendation.to_dict() for recommendation in self.recommendations]

    def to_markdown(self) -> str:
        """Render the sunset plan as deterministic Markdown."""
        title = "# Task Feature Flag Sunset Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.recommendations:
            lines.extend(["", "No feature-flag sunset recommendations were derived."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Flag | Category | Trigger | Owner | Validation Command |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for recommendation in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(recommendation.task_id)}` | "
                f"`{_markdown_cell(recommendation.flag_name)}` | "
                f"{recommendation.category} | "
                f"{recommendation.cleanup_trigger} | "
                f"{_markdown_cell(recommendation.owner_role)} | "
                f"`{_markdown_cell(recommendation.validation_command)}` |"
            )
        return "\n".join(lines)


def build_task_feature_flag_sunset_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskFeatureFlagSunsetPlan:
    """Recommend explicit flag-cleanup work for tasks likely to affect flags."""
    plan_id, tasks = _source_payload(source)
    recommendations = tuple(
        recommendation
        for index, task in enumerate(tasks, start=1)
        if (recommendation := _recommendation(task, index)) is not None
    )
    flagged_task_ids = tuple(recommendation.task_id for recommendation in recommendations)
    all_task_ids = tuple(
        _optional_text(task.get("id")) or f"task-{index}"
        for index, task in enumerate(tasks, start=1)
    )
    return TaskFeatureFlagSunsetPlan(
        plan_id=plan_id,
        recommendations=recommendations,
        flagged_task_ids=flagged_task_ids,
        no_impact_task_ids=tuple(task_id for task_id in all_task_ids if task_id not in flagged_task_ids),
    )


def task_feature_flag_sunset_plan_to_dict(
    result: TaskFeatureFlagSunsetPlan,
) -> dict[str, Any]:
    """Serialize a feature-flag sunset plan to a plain dictionary."""
    return result.to_dict()


task_feature_flag_sunset_plan_to_dict.__test__ = False


def task_feature_flag_sunset_plan_to_markdown(
    result: TaskFeatureFlagSunsetPlan,
) -> str:
    """Render a feature-flag sunset plan as Markdown."""
    return result.to_markdown()


task_feature_flag_sunset_plan_to_markdown.__test__ = False


def _recommendation(
    task: Mapping[str, Any],
    index: int,
) -> TaskFeatureFlagSunsetRecommendation | None:
    evidence = _flag_evidence(task)
    if not evidence:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    category = _category(task)
    flag_name = _flag_name(task, index)
    cleanup_trigger = _cleanup_trigger(task, category)
    validation_command = _validation_command(task, flag_name)
    return TaskFeatureFlagSunsetRecommendation(
        task_id=task_id,
        title=title,
        flag_name=flag_name,
        category=category,
        cleanup_trigger=cleanup_trigger,
        owner_role=_owner_role(task, category),
        validation_command=validation_command,
        suggested_acceptance_criteria=_suggested_acceptance_criteria(
            flag_name,
            cleanup_trigger,
            validation_command,
        ),
        evidence=tuple(_dedupe(evidence)),
    )


def _flag_evidence(task: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    context = _task_context(task)
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _is_flag_path(path):
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _task_texts(task):
        if _is_flag_text(text, context=context):
            evidence.append(f"{source_field}: {text}")

    for source_field, text in _metadata_texts(task.get("metadata")):
        if _is_flag_metadata_field(source_field) or _is_flag_text(
            f"{source_field} {text}",
            context=context,
        ):
            evidence.append(f"{source_field}: {text}")

    return evidence


def _is_flag_text(text: str, *, context: str = "") -> bool:
    normalized = text.replace("_", " ")
    normalized_context = context.replace("_", " ")
    if _NEGATED_IMPACT_RE.search(normalized):
        return False
    if _EXPLICIT_FLAG_RE.search(normalized):
        if _ORDINARY_FLAG_RE.search(normalized) and not _CONTEXT_RE.search(normalized):
            return False
        return True
    if _BARE_FLAG_RE.search(normalized):
        return bool(_CONTEXT_RE.search(normalized_context) and not _ORDINARY_FLAG_RE.search(normalized_context))
    if not _ROLLOUT_EXPERIMENT_RE.search(normalized):
        return False
    if _ORDINARY_FLAG_RE.search(normalized):
        return False
    return bool(
        re.search(
            r"\b(?:behind|gated|guarded|cohort|percentage|gradual|variant|a/b|ab test)\b",
            normalized,
            re.IGNORECASE,
        )
    )


def _is_flag_metadata_field(source_field: str) -> bool:
    normalized = source_field.replace("_", " ").casefold()
    return any(
        marker in normalized
        for marker in (
            "metadata.feature flag",
            "metadata.flag name",
            "metadata.rollout",
            "metadata.experiment",
            "metadata.toggle",
        )
    )


def _is_flag_path(value: str) -> bool:
    path = PurePosixPath(_normalized_path(value).lower())
    parts = set(path.parts)
    return (
        path.name in _CONFIG_PATH_NAMES
        or "feature_flag" in path.name
        or "feature-flag" in path.name
        or bool(parts & _CONFIG_PATH_PARTS)
        and (path.suffix in {".env", ".yaml", ".yml", ".json", ".toml", ".py", ".ts", ".tsx"})
    )


def _category(task: Mapping[str, Any]) -> FeatureFlagCategory:
    context = _task_context(task)
    if _EXPERIMENT_RE.search(context):
        return "experiment"
    if _CONFIG_RE.search(context) or any(
        _is_flag_path(path) for path in _strings(task.get("files_or_modules") or task.get("files"))
    ):
        return "configuration"
    return "rollout"


def _cleanup_trigger(
    task: Mapping[str, Any],
    category: FeatureFlagCategory,
) -> FeatureFlagSunsetTrigger:
    context = _task_context(task)
    if category == "experiment" or _EXPERIMENT_CONCLUDED_RE.search(context):
        return "experiment_concluded"
    if category == "rollout" or _ROLLOUT_RE.search(context) or _ROLLOUT_COMPLETE_RE.search(context):
        return "rollout_complete"
    return "validation_complete"


def _owner_role(task: Mapping[str, Any], category: FeatureFlagCategory) -> str:
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("sunset_owner_role", "cleanup_owner_role", "owner_role", "flag_owner_role"):
            if text := _optional_text(metadata.get(key)):
                return text

    owner_type = _optional_text(task.get("owner_type"))
    if owner_type and owner_type not in {"agent", "unknown"}:
        return owner_type
    return {
        "experiment": "product analytics owner",
        "configuration": "platform owner",
        "rollout": "release owner",
    }[category]


def _validation_command(task: Mapping[str, Any], flag_name: str) -> str:
    for value in (
        task.get("test_command"),
        task.get("validation_command"),
        task.get("validation_commands"),
    ):
        strings = _strings(value)
        if strings:
            return strings[0]
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("sunset_validation_command", "cleanup_validation_command", "validation_command"):
            strings = _strings(metadata.get(key))
            if strings:
                return strings[0]
    return f"Run regression validation after removing {flag_name}."


def _suggested_acceptance_criteria(
    flag_name: str,
    cleanup_trigger: FeatureFlagSunsetTrigger,
    validation_command: str,
) -> tuple[str, ...]:
    trigger_text = cleanup_trigger.replace("_", " ")
    return (
        f"Sunset work for {flag_name} is scheduled when {trigger_text}.",
        f"{flag_name} configuration and guarded code paths are removed after sunset.",
        f"Validation command passes after cleanup: {validation_command}",
    )


def _flag_name(task: Mapping[str, Any], index: int) -> str:
    metadata = task.get("metadata")
    for value in (
        task.get("flag_name"),
        task.get("feature_flag"),
        metadata.get("flag_name") if isinstance(metadata, Mapping) else None,
        metadata.get("feature_flag") if isinstance(metadata, Mapping) else None,
        metadata.get("feature_flags") if isinstance(metadata, Mapping) else None,
        metadata.get("toggle") if isinstance(metadata, Mapping) else None,
        metadata.get("experiment") if isinstance(metadata, Mapping) else None,
    ):
        strings = _strings(value)
        if strings:
            return _slug(strings[0], drop_generic=False)

    title = _optional_text(task.get("title")) or _optional_text(task.get("id")) or f"task-{index}"
    return _slug(title)


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, Mapping) or hasattr(source, "model_dump"):
        plan = _plan_payload(source)
        if "tasks" in plan:
            return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))
        task = _task_payload(source)
        return None, [task] if task else []

    tasks: list[dict[str, Any]] = []
    for item in source:
        task = _task_payload(item)
        if task:
            tasks.append(task)
    return None, tasks


def _plan_payload(source: Mapping[str, Any] | ExecutionPlan | ExecutionTask) -> dict[str, Any]:
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(source).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(source) if isinstance(source, Mapping) else {}


def _task_payload(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    return dict(value) if isinstance(value, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [task for item in value if (task := _task_payload(item))]


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "milestone", "owner_type", "suggested_engine"):
        text = _optional_text(task.get(field_name))
        if text:
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("files_or_modules") or task.get("files"))):
        texts.append((f"files_or_modules[{index}]", text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    for index, text in enumerate(_strings(task.get("test_command"))):
        texts.append((f"test_command[{index}]", text))
    return texts


def _metadata_texts(value: Any, *, prefix: str = "metadata") -> list[tuple[str, str]]:
    if not isinstance(value, Mapping):
        return []
    texts: list[tuple[str, str]] = []
    for key in sorted(value, key=lambda item: str(item)):
        field = f"{prefix}.{key}"
        item = value[key]
        if isinstance(item, Mapping):
            texts.extend(_metadata_texts(item, prefix=field))
            continue
        for text in _strings(item):
            texts.append((field, text))
    return texts


def _task_context(task: Mapping[str, Any]) -> str:
    values = [text for _, text in _task_texts(task)]
    values.extend(text for _, text in _metadata_texts(task.get("metadata")))
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


def _slug(value: str, *, drop_generic: bool = True) -> str:
    words = _TOKEN_RE.findall(value.casefold())
    if drop_generic:
        words = [word for word in words if word not in _STOP_WORDS]
    return "_".join(words[:5]) or "feature_flag"


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/").strip().strip("`'\",;:(){}[] ").strip("/")


def _markdown_cell(value: str) -> str:
    return _SPACE_RE.sub(" ", value.replace("|", "\\|").replace("\n", "<br>")).strip()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "FeatureFlagCategory",
    "FeatureFlagSunsetTrigger",
    "TaskFeatureFlagSunsetPlan",
    "TaskFeatureFlagSunsetRecommendation",
    "build_task_feature_flag_sunset_plan",
    "task_feature_flag_sunset_plan_to_dict",
    "task_feature_flag_sunset_plan_to_markdown",
]
