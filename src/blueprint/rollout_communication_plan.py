"""Build stakeholder communication checkpoints for execution rollouts."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


CheckpointSourceType = Literal["milestone", "task"]
_T = TypeVar("_T")

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_HIGH_RISK_LEVELS = {"high", "critical", "blocker"}
_AUDIENCE_KEYS = {
    "audience",
    "audiences",
    "owner",
    "owners",
    "stakeholder",
    "stakeholders",
}
_CHANNEL_KEYS = {"channel", "channels", "channel_labels", "labels", "tags"}
_DEPENDENCY_KEYS = {"depends_on", "dependencies", "dependency", "blockers", "blocked_by"}
_EVIDENCE_KEYS = {
    "evidence",
    "expected_evidence",
    "validation_evidence",
    "success_metric",
    "success_metrics",
}


@dataclass(frozen=True, slots=True)
class RolloutCommunicationCheckpoint:
    """One deterministic communication checkpoint for rollout stakeholders."""

    checkpoint_id: str
    source_type: CheckpointSourceType
    source_id: str
    audience: tuple[str, ...] = field(default_factory=tuple)
    trigger: str = ""
    summary: str = ""
    dependencies: tuple[str, ...] = field(default_factory=tuple)
    expected_evidence: tuple[str, ...] = field(default_factory=tuple)
    channel_labels: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "checkpoint_id": self.checkpoint_id,
            "source_type": self.source_type,
            "source_id": self.source_id,
            "audience": list(self.audience),
            "trigger": self.trigger,
            "summary": self.summary,
            "dependencies": list(self.dependencies),
            "expected_evidence": list(self.expected_evidence),
            "channel_labels": list(self.channel_labels),
        }


def build_rollout_communication_plan(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
    implementation_brief: Mapping[str, Any] | ImplementationBrief | None = None,
) -> tuple[RolloutCommunicationCheckpoint, ...]:
    """Build communication checkpoints for milestones and high-risk tasks."""
    plan = _plan_payload(execution_plan)
    brief = _brief_payload(implementation_brief)
    tasks = _task_payloads(plan.get("tasks"))
    milestone_names = _milestone_names(plan.get("milestones"))
    task_ids_by_milestone = _task_ids_by_milestone(tasks)

    checkpoints: list[RolloutCommunicationCheckpoint] = []
    for index, milestone in enumerate(_milestone_payloads(plan.get("milestones")), start=1):
        checkpoints.append(
            _milestone_checkpoint(
                milestone=milestone,
                index=index,
                plan=plan,
                brief=brief,
                task_ids=task_ids_by_milestone.get(_milestone_label(milestone, index), []),
            )
        )

    for index, task in enumerate(tasks, start=1):
        if _risk_level(task) not in _HIGH_RISK_LEVELS:
            continue
        checkpoints.append(
            _task_checkpoint(
                task=task,
                index=index,
                plan=plan,
                brief=brief,
                milestone_names=milestone_names,
            )
        )

    return tuple(checkpoints)


def rollout_communication_checkpoints_to_dicts(
    checkpoints: tuple[RolloutCommunicationCheckpoint, ...] | list[RolloutCommunicationCheckpoint],
) -> list[dict[str, Any]]:
    """Serialize rollout communication checkpoints to plain dictionaries."""
    return [checkpoint.to_dict() for checkpoint in checkpoints]


rollout_communication_checkpoints_to_dicts.__test__ = False


def _milestone_checkpoint(
    *,
    milestone: Mapping[str, Any],
    index: int,
    plan: Mapping[str, Any],
    brief: Mapping[str, Any],
    task_ids: list[str],
) -> RolloutCommunicationCheckpoint:
    label = _milestone_label(milestone, index)
    slug = _slug(label) or f"milestone-{index}"
    description = _optional_text(milestone.get("description"))
    title = _optional_text(brief.get("title"))
    summary = f"Announce milestone '{label}'"
    if title:
        summary += f" for {title}"
    if description:
        summary += f": {description}"
    summary = _sentence(summary)

    evidence = []
    if task_ids:
        evidence.append("Milestone task completion: " + ", ".join(task_ids))
    evidence.extend(_metadata_strings(milestone, _EVIDENCE_KEYS))
    evidence.extend(_strings(milestone.get("validation")))
    evidence.extend(_brief_evidence(brief))
    evidence.extend(_strings(plan.get("test_strategy")))

    return RolloutCommunicationCheckpoint(
        checkpoint_id=f"comm-milestone-{slug}",
        source_type="milestone",
        source_id=slug,
        audience=tuple(_audience(plan, brief, milestone)),
        trigger=f"Milestone '{label}' is ready for kickoff or completion review.",
        summary=summary,
        dependencies=tuple(_dependencies(plan, brief, milestone)),
        expected_evidence=tuple(_dedupe(evidence)),
        channel_labels=tuple(_channels(plan, brief, milestone, default=("release", "product"))),
    )


def _task_checkpoint(
    *,
    task: Mapping[str, Any],
    index: int,
    plan: Mapping[str, Any],
    brief: Mapping[str, Any],
    milestone_names: list[str],
) -> RolloutCommunicationCheckpoint:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    risk = _risk_level(task)
    summary = f"Escalate high-risk task {task_id}: {title} ({risk} risk)."
    risk_note = _risk_note(task)
    if risk_note:
        summary = f"{summary[:-1]}: {risk_note}."

    evidence = []
    evidence.extend(_strings(task.get("acceptance_criteria")))
    evidence.extend(_metadata_strings(task, _EVIDENCE_KEYS))
    evidence.extend(_strings(task.get("test_command")))
    evidence.extend(_brief_evidence(brief))

    return RolloutCommunicationCheckpoint(
        checkpoint_id=f"comm-task-{_slug(task_id) or index}",
        source_type="task",
        source_id=task_id,
        audience=tuple(_audience(plan, brief, task)),
        trigger=f"High-risk task '{task_id}' is ready for implementation or release review.",
        summary=summary,
        dependencies=tuple(_task_dependencies(task, brief, milestone_names)),
        expected_evidence=tuple(_dedupe(evidence)),
        channel_labels=tuple(_channels(plan, brief, task, default=("engineering", "release"))),
    )


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _brief_payload(
    brief: Mapping[str, Any] | ImplementationBrief | None,
) -> dict[str, Any]:
    if brief is None:
        return {}
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(brief)


def _milestone_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    milestones: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            milestones.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            milestones.append(dict(item))
    return milestones


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _milestone_names(value: Any) -> list[str]:
    return [
        _milestone_label(milestone, index)
        for index, milestone in enumerate(_milestone_payloads(value), start=1)
    ]


def _task_ids_by_milestone(tasks: list[dict[str, Any]]) -> dict[str, list[str]]:
    task_ids: dict[str, list[str]] = {}
    for index, task in enumerate(tasks, start=1):
        milestone = _optional_text(task.get("milestone"))
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        if milestone:
            task_ids.setdefault(milestone, []).append(task_id)
    return task_ids


def _milestone_label(milestone: Mapping[str, Any], index: int) -> str:
    return (
        _optional_text(milestone.get("id"))
        or _optional_text(milestone.get("name"))
        or _optional_text(milestone.get("title"))
        or f"milestone-{index}"
    )


def _audience(
    plan: Mapping[str, Any],
    brief: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[str]:
    values: list[str] = []
    values.extend(_metadata_strings(plan, _AUDIENCE_KEYS))
    values.extend(_metadata_strings(source, _AUDIENCE_KEYS))
    values.extend(_strings(source.get("owner_type")))
    values.extend(_strings(brief.get("target_user")))
    values.extend(_strings(brief.get("buyer")))
    values.extend(_strings(brief.get("integration_points")))

    tags = _all_tags(source)
    risk_text = " ".join(_strings(source.get("risk")) + _strings(source.get("risk_level")) + _strings(brief.get("risks")))
    values.extend(_audience_from_terms(tags + _tokens(risk_text)))

    if not values:
        values.append("engineering")
    return _dedupe(values)


def _channels(
    plan: Mapping[str, Any],
    brief: Mapping[str, Any],
    source: Mapping[str, Any],
    *,
    default: tuple[str, ...],
) -> list[str]:
    channels: list[str] = list(default)
    channels.extend(_metadata_strings(plan, _CHANNEL_KEYS))
    channels.extend(_metadata_strings(source, _CHANNEL_KEYS))

    terms = _all_tags(source) + _tokens(" ".join(_strings(brief.get("risks"))))
    if any(term in {"customer", "support", "operator", "ops", "incident"} for term in terms):
        channels.append("support")
    if any(term in {"ui", "ux", "product", "user", "workflow"} for term in terms):
        channels.append("product")
    if any(term in {"api", "backend", "frontend", "data", "security"} for term in terms):
        channels.append("engineering")
    if _strings(brief.get("integration_points")):
        channels.append("release")

    return _dedupe(_channel_label(channel) for channel in channels if _channel_label(channel))


def _dependencies(
    plan: Mapping[str, Any],
    brief: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[str]:
    values: list[str] = []
    values.extend(_metadata_strings(source, _DEPENDENCY_KEYS))
    values.extend(_strings(source.get("depends_on")))
    values.extend(_strings(source.get("dependencies")))
    values.extend(f"Integration: {item}" for item in _strings(brief.get("integration_points")))
    if plan.get("target_repo"):
        values.append(f"Repository: {_text(plan.get('target_repo'))}")
    return _dedupe(values)


def _task_dependencies(
    task: Mapping[str, Any],
    brief: Mapping[str, Any],
    milestone_names: list[str],
) -> list[str]:
    values: list[str] = []
    values.extend(f"Task dependency: {item}" for item in _strings(task.get("depends_on")))
    milestone = _optional_text(task.get("milestone"))
    if milestone and milestone in milestone_names:
        values.append(f"Milestone: {milestone}")
    values.extend(f"Integration: {item}" for item in _strings(brief.get("integration_points")))
    return _dedupe(values)


def _brief_evidence(brief: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    validation_plan = _optional_text(brief.get("validation_plan"))
    if validation_plan:
        values.append(f"Validation plan: {validation_plan}")
    values.extend(f"Definition of done: {item}" for item in _strings(brief.get("definition_of_done")))
    return values


def _metadata_strings(source: Mapping[str, Any], keys: set[str]) -> list[str]:
    values: list[str] = []
    for key, value in source.items():
        normalized = "_".join(_tokens(key))
        if normalized in keys:
            values.extend(_strings(value))
    metadata = source.get("metadata")
    if isinstance(metadata, Mapping):
        for key, value in metadata.items():
            normalized = "_".join(_tokens(key))
            if normalized in keys:
                values.extend(_strings(value))
    return values


def _all_tags(source: Mapping[str, Any]) -> list[str]:
    return [
        token
        for value in _metadata_strings(source, _CHANNEL_KEYS)
        for token in _tokens(value)
    ]


def _audience_from_terms(terms: list[str]) -> list[str]:
    values: list[str] = []
    if any(term in {"api", "backend", "frontend", "data", "security", "engineer"} for term in terms):
        values.append("engineering")
    if any(term in {"product", "ui", "ux", "user", "workflow"} for term in terms):
        values.append("product")
    if any(term in {"customer", "support", "operator", "ops", "incident", "availability"} for term in terms):
        values.append("support")
    return values


def _risk_level(task: Mapping[str, Any]) -> str:
    value = _optional_text(task.get("risk_level")) or _optional_text(task.get("risk"))
    if value:
        return value.lower()
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        return (_optional_text(metadata.get("risk_level")) or _optional_text(metadata.get("risk")) or "unspecified").lower()
    return "unspecified"


def _risk_note(task: Mapping[str, Any]) -> str | None:
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        return _optional_text(metadata.get("risk")) or _optional_text(metadata.get("risk_note"))
    return None


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        return [
            text
            for key in sorted(value, key=lambda item: str(item))
            if (text := _optional_text(value[key]))
        ]
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return [text for item in items for text in _strings(item)]
    text = _optional_text(value)
    return [text] if text else []


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _sentence(value: str) -> str:
    return value if value.endswith((".", "!", "?")) else value + "."


def _tokens(value: Any) -> list[str]:
    return _TOKEN_RE.findall(str(value).lower())


def _slug(value: Any) -> str:
    return "-".join(_tokens(value))


def _channel_label(value: str) -> str:
    tokens = _tokens(value)
    if not tokens:
        return ""
    if any(token in {"release", "rollout", "deploy", "deployment"} for token in tokens):
        return "release"
    if any(token in {"product", "pm", "ui", "ux", "user", "workflow"} for token in tokens):
        return "product"
    if any(token in {"support", "customer", "operator", "ops", "incident"} for token in tokens):
        return "support"
    if any(token in {"eng", "engineering", "api", "backend", "frontend", "data", "security"} for token in tokens):
        return "engineering"
    return "-".join(tokens)


def _dedupe(values: list[_T] | tuple[_T, ...]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "CheckpointSourceType",
    "RolloutCommunicationCheckpoint",
    "build_rollout_communication_plan",
    "rollout_communication_checkpoints_to_dicts",
]
