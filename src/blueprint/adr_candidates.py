"""Detect planning decisions that should be captured as ADR candidates."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


@dataclass(frozen=True, slots=True)
class ADRCandidate:
    """One potential Architecture Decision Record to write before implementation."""

    title: str
    context: str
    decision_prompt: str
    source_refs: tuple[str, ...] = field(default_factory=tuple)
    priority: int = 50

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "title": self.title,
            "context": self.context,
            "decision_prompt": self.decision_prompt,
            "source_refs": list(self.source_refs),
            "priority": self.priority,
        }


def detect_adr_candidates(
    brief: Mapping[str, Any] | ImplementationBrief,
    plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> tuple[ADRCandidate, ...]:
    """Return ADR candidates from an implementation brief and optional execution plan."""
    brief_payload = _brief_payload(brief)
    plan_payload = _plan_payload(plan)

    drafts: list[ADRCandidate] = []
    architecture_notes = _optional_text(brief_payload.get("architecture_notes"))
    if architecture_notes:
        drafts.append(
            _candidate(
                title=f"Decide architecture approach for {_brief_title(brief_payload)}",
                context=architecture_notes,
                decision_prompt=(
                    "Which architecture approach should guide implementation, and what "
                    "tradeoffs or constraints should be documented?"
                ),
                source_refs=[_source_ref("brief", brief_payload, "architecture_notes")],
                base_priority=55,
                priority_text=architecture_notes,
            )
        )

    for integration in _strings(brief_payload.get("integration_points")):
        drafts.append(
            _candidate(
                title=f"Decide integration boundary for {integration}",
                context=f"Integration point: {integration}",
                decision_prompt=(
                    f"How should `{integration}` be integrated, owned, monitored, "
                    "and isolated from failure?"
                ),
                source_refs=[_source_ref("brief", brief_payload, "integration_points")],
                base_priority=60,
                priority_text=integration,
            )
        )

    for data_requirement in _strings(brief_payload.get("data_requirements")):
        drafts.append(
            _candidate(
                title=f"Decide data handling for {data_requirement}",
                context=f"Data requirement: {data_requirement}",
                decision_prompt=(
                    f"What data model, migration, retention, and recovery decisions are "
                    f"needed for `{data_requirement}`?"
                ),
                source_refs=[_source_ref("brief", brief_payload, "data_requirements")],
                base_priority=65,
                priority_text=data_requirement,
            )
        )

    task_payloads = _task_payloads(plan_payload.get("tasks") if plan_payload else None)
    for index, task in enumerate(task_payloads, 1):
        task_candidate = _task_candidate(task, index)
        if task_candidate is not None:
            drafts.append(task_candidate)

    return tuple(sorted(_dedupe_candidates(drafts), key=_candidate_sort_key))


def build_adr_candidates(
    brief: Mapping[str, Any] | ImplementationBrief,
    plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> tuple[ADRCandidate, ...]:
    """Alias for callers that prefer build_* helper naming."""
    return detect_adr_candidates(brief, plan)


def adr_candidates_to_dicts(
    candidates: tuple[ADRCandidate, ...] | list[ADRCandidate],
) -> list[dict[str, Any]]:
    """Serialize ADR candidates to dictionaries."""
    return [candidate.to_dict() for candidate in candidates]


adr_candidates_to_dicts.__test__ = False


def _task_candidate(task: Mapping[str, Any], index: int) -> ADRCandidate | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    description = _optional_text(task.get("description")) or ""
    files = _strings(task.get("files_or_modules"))
    risk_level = (_optional_text(task.get("risk_level")) or "").lower()
    context_text = " ".join(
        [
            title,
            description,
            risk_level,
            *files,
            *_strings(task.get("acceptance_criteria")),
            *_metadata_texts(task.get("metadata")),
        ]
    )
    is_high_risk = risk_level in {"high", "critical", "blocker"}
    touches_infra_or_config = _touches_infra_or_config(files, context_text)
    if not is_high_risk and not touches_infra_or_config:
        return None

    reasons: list[str] = []
    if is_high_risk:
        reasons.append(f"risk_level: {risk_level}")
    if touches_infra_or_config:
        reasons.append("touches infrastructure or configuration")
    if files:
        reasons.append("files_or_modules: " + ", ".join(files))

    return _candidate(
        title=f"Decide implementation approach for {title}",
        context="; ".join(reasons) or description,
        decision_prompt=(
            f"What implementation, rollout, rollback, and ownership decisions should be "
            f"recorded before `{task_id}` changes {title}?"
        ),
        source_refs=[
            f"task:{task_id}",
            *[f"task:{task_id}:files_or_modules:{file}" for file in files],
        ],
        base_priority=70 if is_high_risk else 62,
        priority_text=context_text,
    )


def _candidate(
    *,
    title: str,
    context: str,
    decision_prompt: str,
    source_refs: list[str],
    base_priority: int,
    priority_text: str,
) -> ADRCandidate:
    return ADRCandidate(
        title=title,
        context=context,
        decision_prompt=decision_prompt,
        source_refs=tuple(_dedupe(source_refs)),
        priority=_priority(base_priority, priority_text),
    )


def _dedupe_candidates(candidates: list[ADRCandidate]) -> list[ADRCandidate]:
    merged: dict[str, ADRCandidate] = {}
    for candidate in candidates:
        key = _candidate_key(candidate)
        existing = merged.get(key)
        if existing is None:
            merged[key] = candidate
            continue
        merged[key] = ADRCandidate(
            title=existing.title,
            context=_join_context(existing.context, candidate.context),
            decision_prompt=existing.decision_prompt,
            source_refs=tuple(_dedupe([*existing.source_refs, *candidate.source_refs])),
            priority=max(existing.priority, candidate.priority),
        )
    return list(merged.values())


def _candidate_key(candidate: ADRCandidate) -> str:
    tokens = [
        token
        for token in _tokens(f"{candidate.title} {candidate.context}")
        if token not in _STOP_WORDS
    ]
    return " ".join(tokens[:8])


def _priority(base_priority: int, text: str) -> int:
    tokens = set(_tokens(text))
    priority = base_priority
    if tokens & _IRREVERSIBLE_TOKENS:
        priority += 20
    if tokens & _DATA_TOKENS:
        priority += 15
    if tokens & _SECURITY_TOKENS:
        priority += 15
    if tokens & _EXTERNAL_INTEGRATION_TOKENS:
        priority += 12
    if tokens & {"critical", "high", "blocker"}:
        priority += 10
    return min(priority, 100)


def _touches_infra_or_config(files: list[str], context_text: str) -> bool:
    if _has_token(context_text, _INFRA_TOKENS | _CONFIG_TOKENS):
        return True
    for file_path in files:
        normalized = _normalized_path(file_path)
        if not normalized:
            continue
        path = PurePosixPath(normalized)
        if path.name in _INFRA_OR_CONFIG_FILENAMES:
            return True
        if path.suffix in _CONFIG_SUFFIXES:
            return True
        if any(part in _INFRA_OR_CONFIG_PATH_PARTS for part in path.parts):
            return True
    return False


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(brief)


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | None) -> dict[str, Any] | None:
    if plan is None:
        return None
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


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
    if not isinstance(value, Mapping):
        return []
    texts: list[str] = []
    for item in value.values():
        if isinstance(item, Mapping):
            texts.extend(_metadata_texts(item))
        else:
            texts.extend(_strings(item))
    return texts


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item for part in _SPLIT_RE.split(value) if (item := _optional_text(part))]
    if isinstance(value, (list, tuple, set)):
        return [text for item in value if (text := _optional_text(item))]
    return []


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _brief_title(brief: Mapping[str, Any]) -> str:
    return _optional_text(brief.get("title")) or _optional_text(brief.get("id")) or "brief"


def _source_ref(kind: str, payload: Mapping[str, Any], field_name: str) -> str:
    source_id = _optional_text(payload.get("id")) or kind
    return f"{kind}:{source_id}:{field_name}"


def _join_context(first: str, second: str) -> str:
    if first == second:
        return first
    return " | ".join(_dedupe([first, second]))


def _dedupe(values: list[str] | tuple[str, ...]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value and value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped


def _candidate_sort_key(candidate: ADRCandidate) -> tuple[Any, ...]:
    return (-candidate.priority, candidate.title, candidate.context)


def _normalized_path(path: str) -> str:
    return path.strip().replace("\\", "/").lower().strip("/")


def _has_token(text: str, tokens: set[str]) -> bool:
    return bool(set(_tokens(text)) & tokens)


def _tokens(value: str) -> list[str]:
    return _TOKEN_RE.findall(value.lower())


_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9-]*")
_SPLIT_RE = re.compile(r"(?:\r?\n|;)+")
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "approach",
    "boundary",
    "decide",
    "for",
    "handling",
    "implementation",
    "integration",
    "of",
    "the",
    "to",
}
_IRREVERSIBLE_TOKENS = {
    "cannot",
    "delete",
    "destructive",
    "drop",
    "irreversible",
    "migration",
    "one-way",
    "production",
    "remove",
}
_DATA_TOKENS = {
    "backfill",
    "database",
    "data",
    "dataset",
    "migration",
    "retention",
    "schema",
    "sql",
}
_SECURITY_TOKENS = {
    "auth",
    "authentication",
    "authorization",
    "credential",
    "permission",
    "secret",
    "security",
    "token",
}
_EXTERNAL_INTEGRATION_TOKENS = {
    "api",
    "external",
    "oauth",
    "provider",
    "remote",
    "third-party",
    "webhook",
}
_INFRA_TOKENS = {
    "deployment",
    "docker",
    "helm",
    "infrastructure",
    "kubernetes",
    "terraform",
}
_CONFIG_TOKENS = {
    "config",
    "configuration",
    "environment",
    "feature-flag",
    "setting",
    "settings",
}
_INFRA_OR_CONFIG_PATH_PARTS = {
    ".github",
    "charts",
    "config",
    "configuration",
    "deploy",
    "deployment",
    "helm",
    "infra",
    "infrastructure",
    "k8s",
    "kubernetes",
    "settings",
    "terraform",
}
_INFRA_OR_CONFIG_FILENAMES = {
    ".env",
    ".env.local",
    ".env.production",
    "docker-compose.yml",
    "docker-compose.yaml",
    "dockerfile",
    "package.json",
    "pyproject.toml",
    "settings.py",
}
_CONFIG_SUFFIXES = (".cfg", ".conf", ".env", ".ini", ".tf", ".tfvars", ".toml", ".yaml", ".yml")


__all__ = [
    "ADRCandidate",
    "adr_candidates_to_dicts",
    "build_adr_candidates",
    "detect_adr_candidates",
]
