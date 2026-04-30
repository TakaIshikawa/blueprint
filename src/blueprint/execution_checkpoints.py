"""Recommend human and validation checkpoints for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch
from pathlib import PurePosixPath
import re
from typing import Any, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


_FAN_OUT_DEPENDENT_THRESHOLD = 3
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9-]*")
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class ExecutionCheckpoint:
    """One recommended review gate for execution dispatch."""

    checkpoint_id: str
    reason: str
    trigger_task_ids: tuple[str, ...]
    recommended_reviewer_role: str
    blocking: bool
    suggested_acceptance_checks: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "checkpoint_id": self.checkpoint_id,
            "reason": self.reason,
            "trigger_task_ids": list(self.trigger_task_ids),
            "recommended_reviewer_role": self.recommended_reviewer_role,
            "blocking": self.blocking,
            "suggested_acceptance_checks": list(self.suggested_acceptance_checks),
        }


def recommend_execution_checkpoints(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
    implementation_brief: Mapping[str, Any] | ImplementationBrief | None = None,
) -> tuple[ExecutionCheckpoint, ...]:
    """Recommend deterministic checkpoint reviews for an execution plan."""
    plan_payload = _plan_payload(execution_plan)
    brief_payload = _brief_payload(implementation_brief)
    tasks = _task_payloads(plan_payload.get("tasks"))
    contexts = [_task_context(task, index) for index, task in enumerate(tasks, start=1)]
    sensitive_patterns = _configured_sensitive_patterns(plan_payload, brief_payload)
    builder = _CheckpointBuilder()

    _add_milestone_checkpoints(builder, contexts)
    _add_task_checkpoints(builder, contexts, sensitive_patterns)
    _add_fan_out_checkpoints(builder, contexts)

    return builder.records()


def execution_checkpoints_to_dict(
    checkpoints: tuple[ExecutionCheckpoint, ...] | list[ExecutionCheckpoint],
) -> list[dict[str, Any]]:
    """Serialize execution checkpoints to dictionaries."""
    return [checkpoint.to_dict() for checkpoint in checkpoints]


execution_checkpoints_to_dict.__test__ = False


@dataclass
class _CheckpointDraft:
    sort_index: int
    timing_rank: int
    timing: str
    trigger_task_ids: tuple[str, ...]
    reasons: list[str] = field(default_factory=list)
    roles: list[str] = field(default_factory=list)
    checks: list[str] = field(default_factory=list)
    blocking: bool = True


class _CheckpointBuilder:
    def __init__(self) -> None:
        self._drafts: dict[tuple[str, tuple[str, ...]], _CheckpointDraft] = {}

    def add(
        self,
        *,
        timing: str,
        sort_index: int,
        trigger_task_ids: tuple[str, ...],
        reason: str,
        reviewer_role: str,
        checks: tuple[str, ...],
        blocking: bool = True,
    ) -> None:
        if not trigger_task_ids:
            return
        key = (timing, trigger_task_ids)
        draft = self._drafts.setdefault(
            key,
            _CheckpointDraft(
                sort_index=sort_index,
                timing_rank=_timing_rank(timing),
                timing=timing,
                trigger_task_ids=trigger_task_ids,
            ),
        )
        draft.reasons.append(reason)
        draft.roles.append(reviewer_role)
        draft.checks.extend(checks)
        draft.blocking = draft.blocking or blocking

    def records(self) -> tuple[ExecutionCheckpoint, ...]:
        drafts = sorted(
            self._drafts.values(),
            key=lambda draft: (draft.sort_index, draft.timing_rank, draft.trigger_task_ids),
        )
        return tuple(_record_from_draft(draft) for draft in drafts)


def _add_milestone_checkpoints(
    builder: _CheckpointBuilder,
    contexts: list[dict[str, Any]],
) -> None:
    last_task_by_milestone: dict[str, dict[str, Any]] = {}
    for context in contexts:
        milestone = _optional_text(context["task"].get("milestone"))
        if milestone:
            last_task_by_milestone[milestone] = context

    for milestone, context in last_task_by_milestone.items():
        builder.add(
            timing="after",
            sort_index=context["index"],
            trigger_task_ids=(context["task_id"],),
            reason=f"milestone_boundary:{milestone}",
            reviewer_role="delivery_lead",
            checks=(
                f"Confirm milestone '{milestone}' acceptance criteria are complete.",
                f"Review validation evidence before dispatching work after '{milestone}'.",
            ),
        )


def _add_task_checkpoints(
    builder: _CheckpointBuilder,
    contexts: list[dict[str, Any]],
    sensitive_patterns: tuple[str, ...],
) -> None:
    for context in contexts:
        task = context["task"]
        task_id = context["task_id"]
        task_title = context["title"]
        risk_level = _risk_level(task)
        if risk_level in {"high", "critical", "blocker"}:
            builder.add(
                timing="before",
                sort_index=context["index"],
                trigger_task_ids=(task_id,),
                reason=f"high_risk_task:{risk_level}",
                reviewer_role="technical_lead",
                checks=(
                    f"Review the implementation approach for '{task_title}'.",
                    f"Confirm rollback or mitigation notes exist for high-risk task {task_id}.",
                ),
            )

        sensitive_matches = _sensitive_matches(context, sensitive_patterns)
        if sensitive_matches:
            role = _sensitive_role(sensitive_matches)
            sensitive_checks: list[str] = []
            for match in sensitive_matches:
                sensitive_checks.append(
                    f"Review sensitive change '{match.path}' before dispatch."
                )
                sensitive_checks.append(
                    f"Confirm tests or manual checks cover {match.label} behavior."
                )
            builder.add(
                timing="before",
                sort_index=context["index"],
                trigger_task_ids=(task_id,),
                reason=f"sensitive_files:{', '.join(match.label for match in sensitive_matches)}",
                reviewer_role=role,
                checks=tuple(_dedupe(sensitive_checks)),
            )


def _add_fan_out_checkpoints(
    builder: _CheckpointBuilder,
    contexts: list[dict[str, Any]],
) -> None:
    dependents_by_task_id: dict[str, list[str]] = {
        context["task_id"]: [] for context in contexts
    }
    for context in contexts:
        for dependency_id in _strings(context["task"].get("depends_on")):
            if dependency_id in dependents_by_task_id:
                dependents_by_task_id[dependency_id].append(context["task_id"])

    contexts_by_task_id = {context["task_id"]: context for context in contexts}
    for task_id, dependent_ids in dependents_by_task_id.items():
        if len(dependent_ids) < _FAN_OUT_DEPENDENT_THRESHOLD:
            continue
        context = contexts_by_task_id[task_id]
        builder.add(
            timing="after",
            sort_index=context["index"],
            trigger_task_ids=(task_id,),
            reason=f"fan_out_dependency:{len(dependent_ids)} dependents",
            reviewer_role="technical_lead",
            checks=(
                f"Validate {task_id} output before unblocking {len(dependent_ids)} dependent tasks.",
                f"Confirm dependent tasks can consume the completed {task_id} contract.",
            ),
        )


def _record_from_draft(draft: _CheckpointDraft) -> ExecutionCheckpoint:
    reason = "; ".join(_dedupe(draft.reasons))
    return ExecutionCheckpoint(
        checkpoint_id=_checkpoint_id(draft.timing, draft.trigger_task_ids),
        reason=reason,
        trigger_task_ids=draft.trigger_task_ids,
        recommended_reviewer_role=_highest_priority_role(draft.roles),
        blocking=draft.blocking,
        suggested_acceptance_checks=tuple(_dedupe(draft.checks)),
    )


def _checkpoint_id(timing: str, task_ids: tuple[str, ...]) -> str:
    task_slug = _slug("-".join(task_ids)) or "tasks"
    return f"checkpoint-{timing}-{task_slug}"


def _task_context(task: Mapping[str, Any], index: int) -> dict[str, Any]:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    files = _strings(task.get("files_or_modules"))
    title = _optional_text(task.get("title")) or task_id
    text_values = [
        title,
        _optional_text(task.get("description")),
        _optional_text(task.get("risk_level")),
        *files,
        *_strings(task.get("acceptance_criteria")),
        *_metadata_texts(task.get("metadata")),
    ]
    return {
        "index": index,
        "task": task,
        "task_id": task_id,
        "title": title,
        "files": files,
        "context_text": " ".join(value for value in text_values if value).lower(),
    }


def _sensitive_matches(
    context: Mapping[str, Any],
    configured_patterns: tuple[str, ...],
) -> tuple["_SensitiveMatch", ...]:
    matches: list[_SensitiveMatch] = []
    for path in context["files"]:
        if label := _sensitive_path_label(path, configured_patterns):
            matches.append(_SensitiveMatch(path=path, label=label))

    matched_labels = {match.label for match in matches}
    if "configured" in matched_labels:
        return tuple(_dedupe_matches(matches))

    context_text = context["context_text"]
    for label, tokens in _SENSITIVE_TEXT_TOKENS.items():
        if label not in matched_labels and _has_token(context_text, tokens):
            matches.append(_SensitiveMatch(path=label, label=label))
            matched_labels.add(label)

    return tuple(_dedupe_matches(matches))


@dataclass(frozen=True, slots=True)
class _SensitiveMatch:
    path: str
    label: str


def _sensitive_path_label(path: str, configured_patterns: tuple[str, ...]) -> str | None:
    normalized = path.strip().replace("\\", "/").lower().strip("/")
    if not normalized:
        return None

    for pattern in configured_patterns:
        if fnmatch(normalized, pattern.lower().strip().replace("\\", "/")):
            return "configured"

    pure_path = PurePosixPath(normalized)
    name = pure_path.name
    parts = set(pure_path.parts)
    if parts & {"alembic", "backfills", "migrations"} or pure_path.suffix == ".sql":
        return "migration"
    if parts & {"auth", "authentication", "oauth", "permissions", "security"}:
        return "auth"
    if parts & {"billing", "checkout", "invoices", "payments", "stripe"}:
        return "billing"
    if parts & {"config", "configs", "deploy", "deployment", "infra", "k8s", "terraform"}:
        return "deployment"
    if parts & {"exports", "exporters", "reports"}:
        return "data_export"
    if name in _SENSITIVE_FILENAMES or pure_path.suffix in _CONFIG_SUFFIXES:
        return "config"
    return None


def _configured_sensitive_patterns(
    plan_payload: Mapping[str, Any],
    brief_payload: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    patterns: list[str] = []
    for payload in (plan_payload, brief_payload or {}):
        metadata = payload.get("metadata")
        if isinstance(metadata, Mapping):
            patterns.extend(_strings(metadata.get("sensitive_files")))
            patterns.extend(_strings(metadata.get("sensitive_file_patterns")))
        patterns.extend(_strings(payload.get("sensitive_files")))
        patterns.extend(_strings(payload.get("sensitive_file_patterns")))
    return tuple(_dedupe(patterns))


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _brief_payload(
    brief: Mapping[str, Any] | ImplementationBrief | None,
) -> dict[str, Any] | None:
    if brief is None:
        return None
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(brief)


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


def _metadata_texts(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        texts: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            texts.extend(_metadata_texts(value[key]))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[str] = []
        for item in items:
            texts.extend(_metadata_texts(item))
        return texts
    text = _optional_text(value)
    return [text] if text else []


def _risk_level(task: Mapping[str, Any]) -> str:
    return (_optional_text(task.get("risk_level")) or "unspecified").lower()


def _sensitive_role(matches: tuple[_SensitiveMatch, ...]) -> str:
    labels = {match.label for match in matches}
    if labels & {"auth", "billing"}:
        return "security_reviewer"
    if labels & {"migration", "data_export"}:
        return "data_reviewer"
    if labels & {"config", "deployment", "configured"}:
        return "release_manager"
    return "technical_lead"


def _highest_priority_role(roles: list[str]) -> str:
    for role in (
        "security_reviewer",
        "data_reviewer",
        "release_manager",
        "technical_lead",
        "delivery_lead",
    ):
        if role in roles:
            return role
    return roles[0] if roles else "technical_lead"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return [text for item in items if (text := _optional_text(item))]
    return []


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _has_token(text: str, tokens: set[str]) -> bool:
    return bool(set(_TOKEN_RE.findall(text.lower())) & tokens)


def _dedupe(values: list[_T] | tuple[_T, ...]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _dedupe_matches(matches: list[_SensitiveMatch]) -> list[_SensitiveMatch]:
    deduped: list[_SensitiveMatch] = []
    seen: set[tuple[str, str]] = set()
    for match in matches:
        key = (match.path, match.label)
        if key in seen:
            continue
        deduped.append(match)
        seen.add(key)
    return deduped


def _slug(value: str) -> str:
    return "-".join(_TOKEN_RE.findall(value.lower()))


def _timing_rank(timing: str) -> int:
    return {"before": 0, "after": 1}.get(timing, 2)


_SENSITIVE_TEXT_TOKENS = {
    "migration": {"backfill", "migration", "migrations"},
    "auth": {"auth", "authentication", "oauth", "permission", "permissions"},
    "billing": {"billing", "checkout", "invoice", "payment", "payments", "stripe"},
    "config": {"config", "configuration", "secret", "secrets"},
    "deployment": {"deploy", "deployment", "release", "rollout"},
    "data_export": {"export", "exports", "report", "reports"},
}
_SENSITIVE_FILENAMES = {
    ".env",
    "dockerfile",
    "package.json",
    "poetry.lock",
    "pyproject.toml",
    "requirements.txt",
}
_CONFIG_SUFFIXES = (".cfg", ".conf", ".env", ".ini", ".toml", ".yaml", ".yml")


__all__ = [
    "ExecutionCheckpoint",
    "execution_checkpoints_to_dict",
    "recommend_execution_checkpoints",
]
