"""Structured per-task handoff checklists for autonomous coding agents."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class AgentHandoffContext:
    """Concise execution context an agent needs before starting a task."""

    plan_id: str | None
    brief_id: str | None
    target_engine: str | None
    target_repo: str | None
    project_type: str | None
    milestone: str | None
    task_status: str
    owner_type: str | None
    suggested_engine: str | None
    estimated_complexity: str | None
    risk_level: str | None
    description: str
    acceptance_criteria: tuple[str, ...]
    mvp_goal: str | None = None
    validation_plan: str | None = None
    handoff_prompt: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible context dictionary in stable key order."""
        return {
            "plan_id": self.plan_id,
            "brief_id": self.brief_id,
            "target_engine": self.target_engine,
            "target_repo": self.target_repo,
            "project_type": self.project_type,
            "milestone": self.milestone,
            "task_status": self.task_status,
            "owner_type": self.owner_type,
            "suggested_engine": self.suggested_engine,
            "estimated_complexity": self.estimated_complexity,
            "risk_level": self.risk_level,
            "description": self.description,
            "acceptance_criteria": list(self.acceptance_criteria),
            "mvp_goal": self.mvp_goal,
            "validation_plan": self.validation_plan,
            "handoff_prompt": self.handoff_prompt,
        }


@dataclass(frozen=True, slots=True)
class AgentHandoffChecklist:
    """One task's structured execution checklist."""

    task_id: str
    title: str
    context: AgentHandoffContext
    files_to_inspect: tuple[str, ...] = field(default_factory=tuple)
    prechecks: tuple[str, ...] = field(default_factory=tuple)
    implementation: tuple[str, ...] = field(default_factory=tuple)
    validation: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    escalation: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible checklist dictionary in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "context": self.context.to_dict(),
            "files_to_inspect": list(self.files_to_inspect),
            "prechecks": list(self.prechecks),
            "implementation": list(self.implementation),
            "validation": list(self.validation),
            "evidence": list(self.evidence),
            "escalation": list(self.escalation),
        }


def build_agent_handoff_checklists(
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any] | None = None,
) -> list[AgentHandoffChecklist]:
    """Build one handoff checklist per task in execution-plan order."""
    tasks = list(execution_plan.get("tasks") or [])
    tasks_by_id = {str(task.get("id")): task for task in tasks if task.get("id")}
    return [
        build_agent_handoff_checklist(
            task=task,
            execution_plan=execution_plan,
            implementation_brief=implementation_brief,
            tasks_by_id=tasks_by_id,
        )
        for task in tasks
    ]


def build_agent_handoff_checklist(
    *,
    task: dict[str, Any],
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any] | None = None,
    tasks_by_id: dict[str, dict[str, Any]] | None = None,
) -> AgentHandoffChecklist:
    """Build a structured handoff checklist for a single execution task."""
    task_id = str(task["id"])
    tasks_by_id = tasks_by_id or {}
    dependency_ids = _string_tuple(task.get("depends_on"))
    risk_level = _normalized_optional(task.get("risk_level") or task.get("risk"))

    context = AgentHandoffContext(
        plan_id=_normalized_optional(execution_plan.get("id") or task.get("execution_plan_id")),
        brief_id=_normalized_optional(
            (implementation_brief or {}).get("id")
            or execution_plan.get("implementation_brief_id")
        ),
        target_engine=_normalized_optional(execution_plan.get("target_engine")),
        target_repo=_normalized_optional(execution_plan.get("target_repo")),
        project_type=_normalized_optional(execution_plan.get("project_type")),
        milestone=_normalized_optional(task.get("milestone")),
        task_status=_normalized_optional(task.get("status")) or "pending",
        owner_type=_normalized_optional(task.get("owner_type")),
        suggested_engine=_normalized_optional(task.get("suggested_engine")),
        estimated_complexity=_normalized_optional(task.get("estimated_complexity")),
        risk_level=risk_level,
        description=str(task.get("description") or ""),
        acceptance_criteria=_string_tuple(task.get("acceptance_criteria")),
        mvp_goal=_normalized_optional((implementation_brief or {}).get("mvp_goal")),
        validation_plan=_normalized_optional((implementation_brief or {}).get("validation_plan")),
        handoff_prompt=_normalized_optional(execution_plan.get("handoff_prompt")),
    )

    return AgentHandoffChecklist(
        task_id=task_id,
        title=str(task.get("title") or task_id),
        context=context,
        files_to_inspect=_files_to_inspect(task, execution_plan, implementation_brief),
        prechecks=_prechecks(task_id, dependency_ids, tasks_by_id),
        implementation=_implementation_items(task),
        validation=_validation_items(task, execution_plan, implementation_brief),
        evidence=_evidence_items(task, dependency_ids),
        escalation=_escalation_items(task_id, task, risk_level),
    )


def checklists_to_dicts(
    checklists: list[AgentHandoffChecklist] | tuple[AgentHandoffChecklist, ...],
) -> list[dict[str, Any]]:
    """Serialize checklists to JSON-compatible dictionaries."""
    return [checklist.to_dict() for checklist in checklists]


def _files_to_inspect(
    task: dict[str, Any],
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any] | None,
) -> tuple[str, ...]:
    files = list(_string_tuple(task.get("files_or_modules")))
    metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
    files.extend(_string_tuple(metadata.get("files_to_inspect")))
    if execution_plan.get("target_repo"):
        files.append(f"Repository: {execution_plan['target_repo']}")
    if implementation_brief and implementation_brief.get("architecture_notes"):
        files.append("Implementation brief architecture notes")
    return _dedupe_tuple(files)


def _prechecks(
    task_id: str,
    dependency_ids: tuple[str, ...],
    tasks_by_id: dict[str, dict[str, Any]],
) -> tuple[str, ...]:
    items = [
        f"Confirm task `{task_id}` is still pending or assigned for implementation.",
        "Review context, files, acceptance criteria, and validation expectations before editing.",
    ]
    for dependency_id in dependency_ids:
        dependency = tasks_by_id.get(dependency_id)
        status = dependency.get("status") if dependency else "unknown"
        items.append(
            f"Confirm dependency `{dependency_id}` is completed or explicitly unblocked "
            f"before starting; current status: {status or 'unknown'}."
        )
    return tuple(items)


def _implementation_items(task: dict[str, Any]) -> tuple[str, ...]:
    items = [
        "Inspect the listed files and nearby tests before changing behavior.",
        "Keep edits scoped to this task and preserve unrelated worktree changes.",
    ]
    files = _string_tuple(task.get("files_or_modules"))
    if files:
        items.append("Update affected files or modules: " + ", ".join(files) + ".")
    for criterion in _string_tuple(task.get("acceptance_criteria")):
        items.append(f"Implement acceptance criterion: {criterion}")
    if not task.get("acceptance_criteria"):
        items.append("Translate the task description into concrete code and reviewable behavior.")
    return tuple(items)


def _validation_items(
    task: dict[str, Any],
    execution_plan: dict[str, Any],
    implementation_brief: dict[str, Any] | None,
) -> tuple[str, ...]:
    items: list[str] = []
    test_command = _normalized_optional(task.get("test_command"))
    plan_strategy = _normalized_optional(execution_plan.get("test_strategy"))
    brief_validation = _normalized_optional((implementation_brief or {}).get("validation_plan"))

    if test_command:
        items.append(f"Run task validation command: {test_command}")
    elif plan_strategy:
        items.append(
            "No task-specific test_command provided; use plan test strategy: "
            f"{plan_strategy}"
        )
    elif brief_validation:
        items.append(
            "No task-specific test_command provided; use brief validation plan: "
            f"{brief_validation}"
        )
    else:
        items.append(
            "No task-specific test_command provided; identify and run the narrowest "
            "relevant validation before handoff."
        )

    items.append("Verify each acceptance criterion is satisfied.")
    return tuple(items)


def _evidence_items(task: dict[str, Any], dependency_ids: tuple[str, ...]) -> tuple[str, ...]:
    items = [
        "Record changed files and summarize behavior changes.",
        "Capture validation command output or explain why validation could not run.",
        "Map evidence back to each acceptance criterion.",
    ]
    if dependency_ids:
        items.append(
            "Note dependency IDs verified before implementation: "
            + ", ".join(dependency_ids)
            + "."
        )
    if task.get("blocked_reason"):
        items.append(f"Include current blocked reason in handoff notes: {task['blocked_reason']}")
    return tuple(items)


def _escalation_items(
    task_id: str,
    task: dict[str, Any],
    risk_level: str | None,
) -> tuple[str, ...]:
    items = [
        "Escalate if required context, files, or acceptance criteria are missing "
        "or contradictory.",
        "Escalate if validation fails and the failure is not clearly caused by "
        "this task's changes.",
    ]
    if _is_high_risk(task, risk_level):
        items.extend(
            [
                f"High-risk task `{task_id}`: escalate before broadening scope, changing "
                "public contracts, or touching production-sensitive paths.",
                f"High-risk task `{task_id}`: escalate if validation coverage cannot prove "
                "the acceptance criteria or rollback path.",
            ]
        )
    return tuple(items)


def _is_high_risk(task: dict[str, Any], risk_level: str | None) -> bool:
    if risk_level and risk_level.lower() == "high":
        return True
    metadata = task.get("metadata")
    if isinstance(metadata, dict):
        metadata_risk = _normalized_optional(metadata.get("risk_level") or metadata.get("risk"))
        return bool(metadata_risk and metadata_risk.lower() == "high")
    return False


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,) if value else ()
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value if item is not None and str(item))
    return (str(value),) if str(value) else ()


def _dedupe_tuple(values: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return tuple(deduped)


def _normalized_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
