"""Deterministic git branch name helpers for execution plan tasks."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


DEFAULT_BRANCH_PREFIX = "task"
DEFAULT_MAX_BRANCH_NAME_LENGTH = 96
DEFAULT_COLLISION_STRATEGY = "suffix"
COLLISION_STRATEGIES = frozenset({"suffix", "error"})
MIN_BRANCH_NAME_LENGTH = 16

_UNSAFE_REF_CHARS = re.compile(r"[\x00-\x20\x7f~^:?*[\]\\]+")
_UNSAFE_COMPONENT_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_SEPARATORS = re.compile(r"[-.]{2,}")


def generate_task_branch_names(
    execution_plan: dict[str, Any] | ExecutionPlan,
    *,
    prefix: str = DEFAULT_BRANCH_PREFIX,
    max_length: int = DEFAULT_MAX_BRANCH_NAME_LENGTH,
    collision_strategy: str = DEFAULT_COLLISION_STRATEGY,
) -> dict[str, str]:
    """Return deterministic task-id to git branch name mappings.

    Branch names include sanitized task IDs for stability and sanitized titles
    for readability. Prefixes may contain slash-separated path components.
    """
    if max_length < MIN_BRANCH_NAME_LENGTH:
        raise ValueError(
            f"max_length must be at least {MIN_BRANCH_NAME_LENGTH} characters"
        )
    if collision_strategy not in COLLISION_STRATEGIES:
        raise ValueError(
            "collision_strategy must be one of: "
            + ", ".join(sorted(COLLISION_STRATEGIES))
        )

    plan = _validated_plan_payload(execution_plan)
    tasks = _list_of_task_payloads(plan.get("tasks"))
    prefix_components = _safe_prefix_components(prefix)
    prefix_text = "/".join(prefix_components)
    available_leaf_length = max_length - len(prefix_text) - 1
    if available_leaf_length < MIN_BRANCH_NAME_LENGTH:
        raise ValueError("max_length is too short for the sanitized prefix")

    branch_names_by_task_id: dict[str, str] = {}
    seen_branch_names: set[str] = set()

    for index, task in enumerate(tasks, start=1):
        task_id = str(task.get("id") or f"task-{index}")
        title = str(task.get("title") or "")
        leaf = _branch_leaf(task_id, title, available_leaf_length)
        branch_name = f"{prefix_text}/{leaf}"

        if branch_name in seen_branch_names:
            if collision_strategy == "error":
                raise ValueError(f"duplicate branch name generated: {branch_name}")
            branch_name = _deduplicated_branch_name(
                prefix_text,
                leaf,
                seen_branch_names,
                max_length,
            )

        seen_branch_names.add(branch_name)
        branch_names_by_task_id[task_id] = branch_name

    return branch_names_by_task_id


def _validated_plan_payload(
    execution_plan: dict[str, Any] | ExecutionPlan,
) -> dict[str, Any]:
    if hasattr(execution_plan, "model_dump"):
        return execution_plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(execution_plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(execution_plan)


def _list_of_task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, dict):
            tasks.append(item)
    return tasks


def _safe_prefix_components(prefix: str) -> list[str]:
    components = [
        _slugify_ref_component(component, fallback="tasks")
        for component in str(prefix or "").split("/")
    ]
    return [component for component in components if component] or [DEFAULT_BRANCH_PREFIX]


def _branch_leaf(task_id: str, title: str, max_length: int) -> str:
    task_id_slug = _slugify_ref_component(task_id, fallback="task")
    title_slug = _slugify_ref_component(title, fallback="task")
    leaf = f"{task_id_slug}-{title_slug}"
    if len(leaf) <= max_length:
        return leaf

    separator_length = 1
    minimum_title_length = 4
    max_task_id_length = max_length - separator_length - minimum_title_length
    if len(task_id_slug) > max_task_id_length:
        task_id_slug = task_id_slug[:max_task_id_length].rstrip(".-_") or "task"

    max_title_length = max_length - len(task_id_slug) - separator_length
    title_slug = title_slug[:max_title_length].rstrip(".-_") or "task"
    return f"{task_id_slug}-{title_slug}"


def _deduplicated_branch_name(
    prefix_text: str,
    leaf: str,
    seen_branch_names: set[str],
    max_length: int,
) -> str:
    for suffix_number in range(2, len(seen_branch_names) + 3):
        suffix = f"-{suffix_number}"
        leaf_length = max_length - len(prefix_text) - 1
        candidate_leaf = f"{leaf[: leaf_length - len(suffix)].rstrip('.-_')}{suffix}"
        candidate = f"{prefix_text}/{candidate_leaf}"
        if candidate not in seen_branch_names:
            return candidate
    raise ValueError("could not generate a unique branch name")


def _slugify_ref_component(value: str, *, fallback: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    slug = _UNSAFE_REF_CHARS.sub("-", ascii_value)
    slug = _UNSAFE_COMPONENT_CHARS.sub("-", slug)
    slug = slug.replace("@{", "-").replace("@", "-")
    slug = _SEPARATORS.sub("-", slug)
    slug = slug.strip(".-_/")
    if slug.endswith(".lock"):
        slug = slug[: -len(".lock")].rstrip(".-_")
    if slug in {"", ".", ".."}:
        return fallback
    return slug.lower()


__all__ = [
    "DEFAULT_BRANCH_PREFIX",
    "DEFAULT_COLLISION_STRATEGY",
    "DEFAULT_MAX_BRANCH_NAME_LENGTH",
    "generate_task_branch_names",
]
