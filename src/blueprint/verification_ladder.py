"""Build ordered verification ladders for execution plans."""

from __future__ import annotations

from dataclasses import dataclass
import re
import shlex
from typing import Any, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


VerificationScope = Literal[
    "targeted_test",
    "integration_test",
    "lint",
    "typecheck",
    "build",
    "package_test",
    "custom_validation",
]
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class VerificationStep:
    """One command in an ordered verification ladder."""

    command: str
    scope: VerificationScope
    reason: str
    tasks_covered: tuple[str, ...]
    blocking: bool

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "command": self.command,
            "scope": self.scope,
            "reason": self.reason,
            "tasks_covered": list(self.tasks_covered),
            "blocking": self.blocking,
        }


@dataclass(frozen=True, slots=True)
class VerificationCoverageGap:
    """Task-level validation missing from an execution plan."""

    task_id: str
    title: str
    reason: str
    blocking: bool

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "reason": self.reason,
            "blocking": self.blocking,
        }


@dataclass(frozen=True, slots=True)
class VerificationLadder:
    """Ordered verification commands and task coverage gaps for a plan."""

    plan_id: str | None
    steps: tuple[VerificationStep, ...]
    gaps: tuple[VerificationCoverageGap, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "steps": [step.to_dict() for step in self.steps],
            "gaps": [gap.to_dict() for gap in self.gaps],
        }


def build_verification_ladder(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> VerificationLadder:
    """Build an ordered verification ladder from task-level test commands."""
    plan = _plan_payload(execution_plan)
    tasks = _task_payloads(plan.get("tasks"))
    command_drafts: dict[str, _CommandDraft] = {}
    gaps: list[VerificationCoverageGap] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        title = _optional_text(task.get("title")) or task_id
        command = _optional_text(task.get("test_command"))

        if not command:
            gaps.append(
                VerificationCoverageGap(
                    task_id=task_id,
                    title=title,
                    reason="missing task test_command",
                    blocking=True,
                )
            )
            continue

        draft = command_drafts.setdefault(command, _CommandDraft(command=command))
        draft.tasks_covered.append(task_id)
        draft.first_task_index = min(draft.first_task_index, index)

    steps = tuple(
        _step_from_draft(draft)
        for draft in sorted(
            command_drafts.values(),
            key=lambda draft: (
                _scope_rank(_classify_command(draft.command)),
                draft.first_task_index,
                draft.command,
            ),
        )
    )

    return VerificationLadder(
        plan_id=_optional_text(plan.get("id")),
        steps=steps,
        gaps=tuple(gaps),
    )


def verification_ladder_to_dict(ladder: VerificationLadder) -> dict[str, Any]:
    """Serialize a verification ladder to a dictionary."""
    return ladder.to_dict()


verification_ladder_to_dict.__test__ = False


@dataclass
class _CommandDraft:
    command: str
    tasks_covered: list[str] | None = None
    first_task_index: int = 1_000_000

    def __post_init__(self) -> None:
        if self.tasks_covered is None:
            self.tasks_covered = []


def _step_from_draft(draft: _CommandDraft) -> VerificationStep:
    scope = _classify_command(draft.command)
    return VerificationStep(
        command=draft.command,
        scope=scope,
        reason=_reason_for_scope(scope),
        tasks_covered=tuple(_dedupe(draft.tasks_covered or [])),
        blocking=True,
    )


def _classify_command(command: str) -> VerificationScope:
    tokens = _command_tokens(command)
    normalized = _normalized_command(command)

    if _is_lint_command(tokens, normalized):
        return "lint"
    if _is_typecheck_command(tokens, normalized):
        return "typecheck"
    if _is_build_command(tokens, normalized):
        return "build"
    if _is_integration_command(tokens, normalized):
        return "integration_test"
    if _is_test_command(tokens, normalized):
        return "targeted_test" if _has_targeted_test_selector(tokens) else "package_test"
    return "custom_validation"


def _reason_for_scope(scope: VerificationScope) -> str:
    return {
        "targeted_test": "Focused task-level test command should run before broader checks.",
        "integration_test": "Integration or end-to-end validation covers cross-boundary behavior.",
        "lint": "Static lint command catches style and correctness issues without running the full suite.",
        "typecheck": "Type checking validates contracts across changed modules.",
        "build": "Build command validates packaging or compile-time integration.",
        "package_test": "Broad package-level test command should run after narrower checks.",
        "custom_validation": "Custom validation command supplied by the execution plan.",
    }[scope]


def _scope_rank(scope: VerificationScope) -> int:
    return {
        "targeted_test": 0,
        "integration_test": 1,
        "lint": 2,
        "typecheck": 3,
        "build": 4,
        "package_test": 5,
        "custom_validation": 6,
    }[scope]


def _is_lint_command(tokens: list[str], normalized: str) -> bool:
    return _has_any(tokens, {"lint", "eslint", "ruff", "flake8", "pylint", "golangci-lint"}) or (
        "ruff check" in normalized
    )


def _is_typecheck_command(tokens: list[str], normalized: str) -> bool:
    return _has_any(tokens, {"mypy", "pyright", "typecheck", "tsc"}) or "tsc --noemit" in normalized


def _is_build_command(tokens: list[str], normalized: str) -> bool:
    return _has_any(tokens, {"build"}) or normalized in {"poetry build", "python -m build"}


def _is_integration_command(tokens: list[str], normalized: str) -> bool:
    return _has_any(tokens, {"playwright", "cypress"}) or bool(
        {"integration", "e2e"} & set(_word_tokens(normalized))
    )


def _is_test_command(tokens: list[str], normalized: str) -> bool:
    return (
        _has_any(
            tokens,
            {
                "pytest",
                "test",
                "tests",
                "jest",
                "vitest",
                "mocha",
                "unittest",
                "tox",
            },
        )
        or "python -m pytest" in normalized
    )


def _has_targeted_test_selector(tokens: list[str]) -> bool:
    for token in tokens:
        normalized = token.strip()
        if "::" in normalized:
            return True
        if normalized.startswith("-k") and len(normalized) > 2:
            return True
        if normalized in {"-k", "-m"}:
            return True
        if _looks_like_test_path(normalized):
            return True
    return False


def _looks_like_test_path(value: str) -> bool:
    normalized = value.replace("\\", "/").strip("'\"")
    if not normalized or normalized.startswith("-"):
        return False
    path_parts = normalized.lower().split("/")
    name = path_parts[-1]
    return (
        "tests" in path_parts
        or name.startswith("test_")
        or name.endswith("_test.py")
        or name.endswith(".test.ts")
        or name.endswith(".test.tsx")
        or name.endswith(".spec.ts")
        or name.endswith(".spec.tsx")
    )


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
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


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _command_tokens(command: str) -> list[str]:
    try:
        return shlex.split(command)
    except ValueError:
        return command.split()


def _normalized_command(command: str) -> str:
    return " ".join(command.lower().split())


def _word_tokens(value: str) -> list[str]:
    return re.findall(r"[a-z0-9][a-z0-9-]*", value.lower())


def _has_any(tokens: list[str], values: set[str]) -> bool:
    normalized_tokens = {_token_name(token) for token in tokens}
    return bool(normalized_tokens & values)


def _token_name(token: str) -> str:
    value = token.strip().lower()
    if value.startswith("run:"):
        value = value.removeprefix("run:")
    if "/" in value:
        value = value.rsplit("/", 1)[-1]
    return value


def _dedupe(values: list[_T] | tuple[_T, ...]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "VerificationCoverageGap",
    "VerificationLadder",
    "VerificationScope",
    "VerificationStep",
    "build_verification_ladder",
    "verification_ladder_to_dict",
]
