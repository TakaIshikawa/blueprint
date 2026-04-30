"""Build per-task file scope guardrails for autonomous execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


@dataclass(frozen=True, slots=True)
class TaskScopeGuard:
    """File scope guardrails for one execution task."""

    task_id: str
    title: str
    allowed_paths: tuple[str, ...] = field(default_factory=tuple)
    review_only_paths: tuple[str, ...] = field(default_factory=tuple)
    blocked_path_patterns: tuple[str, ...] = field(default_factory=tuple)
    escalation_reasons: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "allowed_paths": list(self.allowed_paths),
            "review_only_paths": list(self.review_only_paths),
            "blocked_path_patterns": list(self.blocked_path_patterns),
            "escalation_reasons": list(self.escalation_reasons),
        }


def build_task_scope_guards(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
    implementation_brief: Mapping[str, Any] | ImplementationBrief | None = None,
) -> tuple[TaskScopeGuard, ...]:
    """Return deterministic file scope guardrails for every task in a plan."""
    plan = _plan_payload(execution_plan)
    blocked_patterns = _blocked_path_patterns(implementation_brief)
    guards: list[TaskScopeGuard] = []

    for index, task in enumerate(_task_payloads(plan.get("tasks")), start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        title = _text(task.get("title")) or task_id
        allowed_paths = tuple(
            _dedupe([_normalized_path(path) for path in _strings(task.get("files_or_modules"))])
        )
        review_only_paths = tuple(path for path in allowed_paths if _requires_review(path))

        escalation_reasons: list[str] = []
        if not allowed_paths:
            escalation_reasons.append("missing files_or_modules scope")
        for path in review_only_paths:
            escalation_reasons.append(f"review required for risk-sensitive path: {path}")
        for path in allowed_paths:
            if _is_shared_package_path(path):
                escalation_reasons.append(f"review required for shared package path: {path}")

        guards.append(
            TaskScopeGuard(
                task_id=task_id,
                title=title,
                allowed_paths=allowed_paths,
                review_only_paths=review_only_paths,
                blocked_path_patterns=tuple(blocked_patterns),
                escalation_reasons=tuple(_dedupe(escalation_reasons)),
            )
        )

    return tuple(guards)


def task_scope_guards_to_dicts(
    guards: tuple[TaskScopeGuard, ...] | list[TaskScopeGuard],
) -> list[dict[str, Any]]:
    """Serialize task scope guardrails to dictionaries."""
    return [guard.to_dict() for guard in guards]


task_scope_guards_to_dicts.__test__ = False


def _blocked_path_patterns(
    implementation_brief: Mapping[str, Any] | ImplementationBrief | None,
) -> list[str]:
    if implementation_brief is None:
        return []
    brief = _brief_payload(implementation_brief)
    patterns: list[str] = []
    for non_goal in _strings(brief.get("non_goals")):
        patterns.extend(_patterns_from_non_goal(non_goal))
    return sorted(_dedupe(patterns), key=_pattern_sort_key)


def _patterns_from_non_goal(non_goal: str) -> list[str]:
    patterns: list[str] = []
    for match in _PATH_RE.findall(non_goal):
        path = _normalized_path(match.rstrip(".,:;)"))
        if path:
            patterns.append(_path_pattern(path))

    lowered = non_goal.lower()
    for match in _MODULE_RE.finditer(lowered):
        name = _slug(match.group("name"))
        kind = match.group("kind")
        if not name:
            continue
        if kind in {"module", "package"}:
            patterns.extend([f"**/{name}/**", f"**/{name}.py"])
        else:
            patterns.extend([f"**/{name}/**", f"**/{name}.*"])
    return patterns


def _path_pattern(path: str) -> str:
    if any(char in path for char in "*?["):
        return path
    if "." in PurePosixPath(path).name:
        return path
    return f"{path}/**"


def _requires_review(path: str) -> bool:
    normalized = path.lower()
    path_obj = PurePosixPath(normalized)
    if path_obj.name in _CONFIG_FILENAMES:
        return True
    if path_obj.suffix in _CONFIG_SUFFIXES:
        return True
    parts = set(path_obj.parts)
    if parts & _RISK_PATH_PARTS:
        return True
    return bool(set(_TOKEN_RE.findall(normalized)) & _RISK_TOKENS)


def _is_shared_package_path(path: str) -> bool:
    path_obj = PurePosixPath(path.lower())
    parts = set(path_obj.parts)
    if parts & _SHARED_PACKAGE_PARTS:
        return True
    return len(path_obj.parts) >= 2 and path_obj.parts[0] in {"packages", "libs"}


def _plan_payload(execution_plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(execution_plan, "model_dump"):
        return execution_plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(execution_plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(execution_plan)


def _brief_payload(implementation_brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(implementation_brief, "model_dump"):
        return implementation_brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(implementation_brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(implementation_brief)


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


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _text(value)
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return [text for item in items if (text := _text(item))]
    return []


def _text(value: Any) -> str:
    if isinstance(value, str):
        return " ".join(value.split())
    return ""


def _normalized_path(value: str) -> str:
    return value.strip().replace("\\", "/").strip("/")


def _slug(value: str) -> str:
    return "-".join(_TOKEN_RE.findall(value.lower()))


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _pattern_sort_key(pattern: str) -> tuple[str, int, str]:
    base = pattern.removeprefix("**/")
    if base.endswith("/**"):
        return (base.removesuffix("/**"), 0, pattern)
    if base.endswith(".*"):
        return (base.removesuffix(".*"), 1, pattern)
    if base.endswith(".py"):
        return (base.removesuffix(".py"), 1, pattern)
    return (base, 2, pattern)


_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9-]*")
_PATH_RE = re.compile(r"(?<![\w.-])(?:\.?/)?(?:[A-Za-z0-9_.-]+/)+[A-Za-z0-9_.*?{}[\]-]+")
_MODULE_RE = re.compile(
    r"\b(?P<name>[a-z0-9][a-z0-9_.-]*)\s+(?P<kind>module|package|surface)\b"
)
_CONFIG_FILENAMES = {
    ".env",
    ".env.local",
    ".env.production",
    "dockerfile",
    "package.json",
    "pyproject.toml",
    "settings.py",
}
_CONFIG_SUFFIXES = (".cfg", ".conf", ".env", ".ini", ".json", ".toml", ".yaml", ".yml")
_RISK_PATH_PARTS = {
    ".github",
    "auth",
    "authentication",
    "authorization",
    "config",
    "configuration",
    "migrations",
    "secrets",
    "settings",
}
_RISK_TOKENS = {
    "auth",
    "authentication",
    "authorization",
    "config",
    "credential",
    "credentials",
    "migration",
    "migrations",
    "secret",
    "secrets",
    "setting",
    "settings",
    "token",
    "tokens",
}
_SHARED_PACKAGE_PARTS = {"common", "core", "shared"}


__all__ = [
    "TaskScopeGuard",
    "build_task_scope_guards",
    "task_scope_guards_to_dicts",
]
