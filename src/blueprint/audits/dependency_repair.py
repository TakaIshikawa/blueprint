"""Repair suggestions for execution-plan dependency issues."""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Literal

from blueprint.audits.plan_audit import audit_execution_plan


DependencyRepairAction = Literal[
    "remove_dependency",
    "replace_dependency",
    "split_cycle",
]

TERMINAL_BLOCKER_STATUSES = {"completed", "skipped"}


@dataclass(frozen=True)
class DependencyRepairSuggestion:
    """A concrete dependency edit that can be applied by a human or agent."""

    action: DependencyRepairAction
    task_id: str
    dependency_id: str
    confidence: float
    rationale: str
    replacement_dependency_id: str | None = None
    affected_task_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable suggestion payload."""
        payload: dict[str, Any] = {
            "action": self.action,
            "task_id": self.task_id,
            "dependency_id": self.dependency_id,
            "confidence": self.confidence,
            "rationale": self.rationale,
        }
        if self.replacement_dependency_id is not None:
            payload["replacement_dependency_id"] = self.replacement_dependency_id
        if self.affected_task_ids:
            payload["affected_task_ids"] = self.affected_task_ids
        return payload


@dataclass(frozen=True)
class DependencyRepairResult:
    """Dependency repair suggestions for an execution plan."""

    plan_id: str
    suggestions: list[DependencyRepairSuggestion] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.suggestions

    @property
    def suggestion_count(self) -> int:
        return len(self.suggestions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "ok": self.ok,
            "summary": {"suggestions": self.suggestion_count},
            "suggestions": [suggestion.to_dict() for suggestion in self.suggestions],
        }


def suggest_dependency_repairs(plan: dict[str, Any]) -> DependencyRepairResult:
    """Suggest deterministic dependency edits without mutating the plan."""
    plan_id = str(plan.get("id") or "")
    tasks = _list_of_dicts(plan.get("tasks"))
    tasks_by_id = {
        task_id: task
        for task in tasks
        if (task_id := str(task.get("id") or ""))
    }
    suggestions: list[DependencyRepairSuggestion] = []

    for issue in audit_execution_plan(plan).issues:
        if issue.code == "unknown_dependency" and issue.task_id and issue.dependency_id:
            suggestions.append(
                _missing_dependency_suggestion(
                    issue.task_id,
                    issue.dependency_id,
                    tasks_by_id,
                )
            )
        elif issue.code == "self_dependency" and issue.task_id and issue.dependency_id:
            suggestions.append(
                DependencyRepairSuggestion(
                    action="remove_dependency",
                    task_id=issue.task_id,
                    dependency_id=issue.dependency_id,
                    confidence=1.0,
                    rationale=(
                        f"Task {issue.task_id} cannot depend on itself; remove "
                        f"{issue.dependency_id} from depends_on."
                    ),
                    affected_task_ids=[issue.task_id],
                )
            )
        elif issue.code == "dependency_cycle" and issue.cycle:
            suggestions.append(_cycle_suggestion(issue.cycle))

    suggestions.extend(_terminal_blocker_suggestions(tasks, tasks_by_id))
    suggestions = _dedupe_suggestions(suggestions)
    return DependencyRepairResult(plan_id=plan_id, suggestions=suggestions)


def _missing_dependency_suggestion(
    task_id: str,
    dependency_id: str,
    tasks_by_id: dict[str, dict[str, Any]],
) -> DependencyRepairSuggestion:
    replacement = _best_replacement(task_id, dependency_id, tasks_by_id)
    if replacement:
        return DependencyRepairSuggestion(
            action="replace_dependency",
            task_id=task_id,
            dependency_id=dependency_id,
            replacement_dependency_id=replacement,
            confidence=0.86,
            rationale=(
                f"Dependency {dependency_id} is not in the plan; replace it with "
                f"{replacement}, the closest existing task ID."
            ),
            affected_task_ids=[task_id, replacement],
        )

    return DependencyRepairSuggestion(
        action="remove_dependency",
        task_id=task_id,
        dependency_id=dependency_id,
        confidence=0.76,
        rationale=(
            f"Dependency {dependency_id} is not in the plan and no clear replacement "
            "task ID was found; remove it from depends_on."
        ),
        affected_task_ids=[task_id],
    )


def _cycle_suggestion(cycle: list[str]) -> DependencyRepairSuggestion:
    nodes = cycle[:-1] if len(cycle) > 1 and cycle[0] == cycle[-1] else cycle
    edges = [
        (cycle[index], cycle[index + 1])
        for index in range(len(cycle) - 1)
        if cycle[index] and cycle[index + 1]
    ]
    task_id, dependency_id = min(edges)
    return DependencyRepairSuggestion(
        action="split_cycle",
        task_id=task_id,
        dependency_id=dependency_id,
        confidence=0.72,
        rationale=(
            "Dependency cycle detected; remove or replace "
            f"{dependency_id} from {task_id}.depends_on to split "
            f"{' -> '.join(cycle)}."
        ),
        affected_task_ids=sorted(set(nodes)),
    )


def _terminal_blocker_suggestions(
    tasks: list[dict[str, Any]],
    tasks_by_id: dict[str, dict[str, Any]],
) -> list[DependencyRepairSuggestion]:
    suggestions: list[DependencyRepairSuggestion] = []
    for task in sorted(tasks, key=lambda item: str(item.get("id") or "")):
        task_id = str(task.get("id") or "")
        if not task_id or task.get("status") not in {"pending", "blocked", "in_progress"}:
            continue

        for dependency_id in _string_list(task.get("depends_on")):
            dependency = tasks_by_id.get(dependency_id)
            dependency_status = str(dependency.get("status") or "") if dependency else ""
            if dependency_status not in TERMINAL_BLOCKER_STATUSES:
                continue

            confidence = 0.68 if dependency_status == "skipped" else 0.58
            suggestions.append(
                DependencyRepairSuggestion(
                    action="remove_dependency",
                    task_id=task_id,
                    dependency_id=dependency_id,
                    confidence=confidence,
                    rationale=(
                        f"Dependency {dependency_id} is already {dependency_status}; "
                        f"remove it from {task_id}.depends_on if it no longer blocks "
                        "task execution."
                    ),
                    affected_task_ids=[task_id, dependency_id],
                )
            )
    return suggestions


def _best_replacement(
    task_id: str,
    dependency_id: str,
    tasks_by_id: dict[str, dict[str, Any]],
) -> str | None:
    candidates = sorted(candidate for candidate in tasks_by_id if candidate != task_id)
    scored = [
        (_similarity(dependency_id, candidate), candidate)
        for candidate in candidates
    ]
    scored.sort(key=lambda item: (-item[0], item[1]))
    if not scored or scored[0][0] < 0.72:
        return None
    if len(scored) > 1 and scored[0][0] - scored[1][0] < 0.05:
        return None
    return scored[0][1]


def _similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, _normalized_id(left), _normalized_id(right)).ratio()


def _normalized_id(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum())


def _dedupe_suggestions(
    suggestions: list[DependencyRepairSuggestion],
) -> list[DependencyRepairSuggestion]:
    by_key: dict[tuple[str, str, str, str | None], DependencyRepairSuggestion] = {}
    for suggestion in suggestions:
        key = (
            suggestion.action,
            suggestion.task_id,
            suggestion.dependency_id,
            suggestion.replacement_dependency_id,
        )
        if key not in by_key:
            by_key[key] = suggestion

    return sorted(
        by_key.values(),
        key=lambda suggestion: (
            suggestion.task_id,
            suggestion.dependency_id,
            suggestion.action,
            suggestion.replacement_dependency_id or "",
        ),
    )


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if _has_text(item)]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())
