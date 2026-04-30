"""Build execution handoff risk registers for autonomous task delegation."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class ExecutionHandoffRiskRecord:
    """One risk that can derail delegated execution."""

    risk_id: str
    category: str
    severity: str
    likelihood: str
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    mitigation: str = ""
    escalation_trigger: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "risk_id": self.risk_id,
            "category": self.category,
            "severity": self.severity,
            "likelihood": self.likelihood,
            "impacted_task_ids": list(self.impacted_task_ids),
            "evidence": list(self.evidence),
            "mitigation": self.mitigation,
            "escalation_trigger": self.escalation_trigger,
        }


@dataclass(frozen=True, slots=True)
class ExecutionHandoffRiskRegister:
    """Risk register and aggregate counts for an execution handoff."""

    plan_id: str
    risk_count: int
    counts_by_severity: dict[str, int] = field(default_factory=dict)
    counts_by_category: dict[str, int] = field(default_factory=dict)
    risks: tuple[ExecutionHandoffRiskRecord, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "risk_count": self.risk_count,
            "counts_by_severity": dict(self.counts_by_severity),
            "counts_by_category": dict(self.counts_by_category),
            "risks": [risk.to_dict() for risk in self.risks],
        }


def build_execution_handoff_risk_register(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
    implementation_brief: Mapping[str, Any] | ImplementationBrief | None = None,
) -> ExecutionHandoffRiskRegister:
    """Combine plan and brief signals into handoff-specific risk records."""
    plan = _plan_payload(execution_plan)
    brief = _brief_payload(implementation_brief)
    tasks = _task_contexts(plan.get("tasks"))
    task_by_id = {task["task_id"]: task for task in tasks}
    dependents_by_task_id = _dependents_by_task_id(tasks, task_by_id)

    risks: list[ExecutionHandoffRiskRecord] = []
    risks.extend(_blocked_risks(tasks))
    risks.extend(_missing_validation_risks(tasks))
    risks.extend(_unresolved_owner_risks(tasks))
    risks.extend(_task_level_risks(tasks))
    risks.extend(_dependency_risks(tasks, task_by_id, dependents_by_task_id))
    risks.extend(_file_contention_risks(tasks))
    risks.extend(_brief_risks(brief, tasks))

    ordered_risks = tuple(sorted(risks, key=lambda risk: risk.risk_id))
    return ExecutionHandoffRiskRegister(
        plan_id=_optional_text(plan.get("id")) or "",
        risk_count=len(ordered_risks),
        counts_by_severity=_counts(risk.severity for risk in ordered_risks),
        counts_by_category=_counts(risk.category for risk in ordered_risks),
        risks=ordered_risks,
    )


def execution_handoff_risk_register_to_dict(
    register: ExecutionHandoffRiskRegister,
) -> dict[str, Any]:
    """Serialize an execution handoff risk register to a plain dictionary."""
    return register.to_dict()


execution_handoff_risk_register_to_dict.__test__ = False


def _blocked_risks(tasks: list[dict[str, Any]]) -> list[ExecutionHandoffRiskRecord]:
    risks: list[ExecutionHandoffRiskRecord] = []
    for task in tasks:
        status = task["status"]
        blocked_reason = task["blocked_reason"]
        if "blocked" not in _tokens(status) and not blocked_reason:
            continue
        task_id = task["task_id"]
        evidence = [f"Task {task_id} status is {status or 'blocked'}."]
        if blocked_reason:
            evidence.append(f"Blocked reason: {blocked_reason}")
        risks.append(
            ExecutionHandoffRiskRecord(
                risk_id=f"blocked-{_slug(task_id)}",
                category="blocked",
                severity=_max_severity(["high", task["risk_level"]]),
                likelihood="high",
                impacted_task_ids=(task_id,),
                evidence=tuple(evidence),
                mitigation=(
                    f"Resolve the blocker for {task_id} or re-scope the branch before dispatch."
                ),
                escalation_trigger=(
                    f"Escalate when {task_id} is still blocked at branch assignment time."
                ),
            )
        )
    return risks


def _missing_validation_risks(
    tasks: list[dict[str, Any]],
) -> list[ExecutionHandoffRiskRecord]:
    risks: list[ExecutionHandoffRiskRecord] = []
    for task in tasks:
        if task["test_commands"]:
            continue
        task_id = task["task_id"]
        risks.append(
            ExecutionHandoffRiskRecord(
                risk_id=f"validation-missing-{_slug(task_id)}",
                category="validation",
                severity=_max_severity(["medium", task["risk_level"]]),
                likelihood="medium",
                impacted_task_ids=(task_id,),
                evidence=(
                    f"Task {task_id} has no test_command.",
                    f"Acceptance criteria count: {len(task['acceptance_criteria'])}",
                ),
                mitigation=(
                    f"Define a focused test command or reviewer validation evidence for {task_id}."
                ),
                escalation_trigger=(
                    f"Escalate if {task_id} cannot name executable validation before implementation starts."
                ),
            )
        )
    return risks


def _unresolved_owner_risks(
    tasks: list[dict[str, Any]],
) -> list[ExecutionHandoffRiskRecord]:
    risks: list[ExecutionHandoffRiskRecord] = []
    for task in tasks:
        if task["owner_type"]:
            continue
        task_id = task["task_id"]
        risks.append(
            ExecutionHandoffRiskRecord(
                risk_id=f"ownership-unresolved-{_slug(task_id)}",
                category="ownership",
                severity="low",
                likelihood="medium",
                impacted_task_ids=(task_id,),
                evidence=(f"Task {task_id} has no owner_type.",),
                mitigation=f"Assign an owner type for {task_id} before opening a branch.",
                escalation_trigger=(
                    f"Escalate if no coordinator can assign {task_id} to an accountable owner."
                ),
            )
        )
    return risks


def _task_level_risks(tasks: list[dict[str, Any]]) -> list[ExecutionHandoffRiskRecord]:
    risks: list[ExecutionHandoffRiskRecord] = []
    for task in tasks:
        if task["risk_level"] not in {"high", "critical", "blocker"}:
            continue
        task_id = task["task_id"]
        risks.append(
            ExecutionHandoffRiskRecord(
                risk_id=f"task-risk-{_slug(task_id)}",
                category="task_risk",
                severity=_severity(task["risk_level"]),
                likelihood="medium",
                impacted_task_ids=(task_id,),
                evidence=(f"Task {task_id} risk_level is {task['risk_level']}.",),
                mitigation=(
                    f"Require the assigned agent to document assumptions, validation, and rollback notes for {task_id}."
                ),
                escalation_trigger=(
                    f"Escalate if {task_id} changes shared behavior or cannot produce validation evidence."
                ),
            )
        )
    return risks


def _dependency_risks(
    tasks: list[dict[str, Any]],
    task_by_id: dict[str, dict[str, Any]],
    dependents_by_task_id: dict[str, list[str]],
) -> list[ExecutionHandoffRiskRecord]:
    risks: list[ExecutionHandoffRiskRecord] = []

    for task in tasks:
        missing = [item for item in task["depends_on"] if item not in task_by_id]
        if not missing:
            continue
        task_id = task["task_id"]
        risks.append(
            ExecutionHandoffRiskRecord(
                risk_id=f"dependency-missing-{_slug(task_id)}",
                category="dependency",
                severity="high",
                likelihood="high",
                impacted_task_ids=(task_id,),
                evidence=tuple(f"Unknown dependency id: {item}" for item in missing),
                mitigation=(
                    f"Resolve missing dependency ids for {task_id} before handing off execution."
                ),
                escalation_trigger=(
                    f"Escalate if {task_id} still references unknown dependencies during branch planning."
                ),
            )
        )

    for task in tasks:
        task_id = task["task_id"]
        downstream = _downstream_task_ids(task_id, dependents_by_task_id)
        if len(downstream) < 2 and len(dependents_by_task_id.get(task_id, [])) < 2:
            continue
        impacted = tuple([task_id, *downstream])
        risks.append(
            ExecutionHandoffRiskRecord(
                risk_id=f"dependency-chain-{_slug(task_id)}",
                category="dependency",
                severity=_dependency_chain_severity(task, downstream, task_by_id),
                likelihood="medium",
                impacted_task_ids=impacted,
                evidence=(
                    f"Task {task_id} has {len(dependents_by_task_id.get(task_id, []))} direct dependents.",
                    f"Dependency chain reaches {len(downstream)} downstream tasks.",
                ),
                mitigation=(
                    f"Sequence {task_id} before dependent branches and publish completion evidence to downstream agents."
                ),
                escalation_trigger=(
                    f"Escalate if {task_id} slips after dependent agents have started implementation."
                ),
            )
        )

    return risks


def _file_contention_risks(
    tasks: list[dict[str, Any]],
) -> list[ExecutionHandoffRiskRecord]:
    tasks_by_file: dict[str, list[dict[str, Any]]] = {}
    for task in tasks:
        for path in task["files_or_modules"]:
            tasks_by_file.setdefault(path, []).append(task)

    risks: list[ExecutionHandoffRiskRecord] = []
    for path in sorted(tasks_by_file):
        contenders = tasks_by_file[path]
        if len(contenders) < 2:
            continue
        task_ids = tuple(task["task_id"] for task in contenders)
        risks.append(
            ExecutionHandoffRiskRecord(
                risk_id=f"contention-{_slug(path)}",
                category="contention",
                severity=_max_severity(["medium", *[task["risk_level"] for task in contenders]]),
                likelihood="high",
                impacted_task_ids=task_ids,
                evidence=(f"Shared file or module '{path}' is assigned to {', '.join(task_ids)}.",),
                mitigation=(
                    f"Assign one owner for {path} or sequence the branches touching it."
                ),
                escalation_trigger=(
                    f"Escalate if multiple agents need to edit {path} in parallel."
                ),
            )
        )
    return risks


def _brief_risks(
    brief: Mapping[str, Any],
    tasks: list[dict[str, Any]],
) -> list[ExecutionHandoffRiskRecord]:
    risks: list[ExecutionHandoffRiskRecord] = []
    for index, risk in enumerate(_distinct_strings(brief.get("risks")), start=1):
        matched_tasks = _matching_tasks(risk, tasks)
        impacted = tuple(task["task_id"] for task in matched_tasks)
        risks.append(
            ExecutionHandoffRiskRecord(
                risk_id=f"brief-risk-{index}-{_slug(risk)[:48] or 'risk'}",
                category="brief",
                severity=_max_severity([_text_severity(risk), *[task["risk_level"] for task in matched_tasks]]),
                likelihood=_brief_likelihood(risk, matched_tasks),
                impacted_task_ids=impacted,
                evidence=tuple(
                    _dedupe(
                        [
                            f"Brief risk: {risk}",
                            *[
                                f"Matched task {task['task_id']}: {task['title']}"
                                for task in matched_tasks
                            ],
                        ]
                    )
                ),
                mitigation=_brief_mitigation(risk, impacted),
                escalation_trigger=_brief_escalation(risk, impacted),
            )
        )
    return risks


def _brief_mitigation(risk: str, impacted_task_ids: tuple[str, ...]) -> str:
    if impacted_task_ids:
        return (
            "Carry the brief-level risk into "
            f"{', '.join(impacted_task_ids)} handoff notes and require validation evidence."
        )
    return f"Assign an owner to track brief-level risk before dispatch: {risk}."


def _brief_escalation(risk: str, impacted_task_ids: tuple[str, ...]) -> str:
    if impacted_task_ids:
        return (
            "Escalate if impacted tasks cannot show mitigation evidence for the inherited brief risk."
        )
    return f"Escalate if no task or coordinator owns brief-level risk: {risk}."


def _matching_tasks(risk: str, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    risk_tokens = _signal_tokens(risk)
    if not risk_tokens:
        return []

    matched: list[tuple[int, int, dict[str, Any]]] = []
    for task in tasks:
        overlap = risk_tokens & task["tokens"]
        score = len(overlap)
        if overlap & _DOMAIN_TOKENS:
            score += 2
        if overlap & _RISK_TOKENS:
            score += 1
        if score >= 2:
            matched.append((task["index"], -score, task))
    return [task for _, _, task in sorted(matched, key=lambda item: (item[0], item[1]))]


def _dependents_by_task_id(
    tasks: list[dict[str, Any]],
    task_by_id: dict[str, dict[str, Any]],
) -> dict[str, list[str]]:
    dependents: dict[str, list[str]] = {}
    for task in tasks:
        for dependency_id in task["depends_on"]:
            if dependency_id not in task_by_id:
                continue
            dependents.setdefault(dependency_id, []).append(task["task_id"])
    return {task_id: sorted(dependents[task_id]) for task_id in sorted(dependents)}


def _downstream_task_ids(
    task_id: str,
    dependents_by_task_id: dict[str, list[str]],
) -> list[str]:
    downstream: set[str] = set()
    visiting: set[str] = set()

    def visit(current_task_id: str) -> None:
        if current_task_id in visiting:
            return
        visiting.add(current_task_id)
        for dependent_id in dependents_by_task_id.get(current_task_id, []):
            if dependent_id == task_id:
                continue
            downstream.add(dependent_id)
            visit(dependent_id)
        visiting.remove(current_task_id)

    visit(task_id)
    return sorted(downstream)


def _dependency_chain_severity(
    task: dict[str, Any],
    downstream_task_ids: list[str],
    task_by_id: dict[str, dict[str, Any]],
) -> str:
    return _max_severity(
        [
            "medium",
            task["risk_level"],
            *[task_by_id[task_id]["risk_level"] for task_id in downstream_task_ids],
        ]
    )


def _task_contexts(value: Any) -> list[dict[str, Any]]:
    contexts: list[dict[str, Any]] = []
    for index, task in enumerate(_task_payloads(value), start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        title = _optional_text(task.get("title")) or task_id
        description = _optional_text(task.get("description")) or ""
        files = _strings(task.get("files_or_modules"))
        acceptance = _strings(task.get("acceptance_criteria"))
        metadata_texts = _strings(task.get("metadata"))
        context_text = " ".join(
            value
            for value in [
                title,
                description,
                _optional_text(task.get("risk_level")),
                _optional_text(task.get("blocked_reason")),
                *files,
                *acceptance,
                *metadata_texts,
            ]
            if value
        )
        contexts.append(
            {
                "index": index,
                "task_id": task_id,
                "title": title,
                "description": description,
                "depends_on": _strings(task.get("depends_on")),
                "files_or_modules": files,
                "acceptance_criteria": acceptance,
                "test_commands": _strings(task.get("test_command")),
                "owner_type": _optional_text(task.get("owner_type")),
                "risk_level": _severity(_optional_text(task.get("risk_level"))),
                "status": (_optional_text(task.get("status")) or "").lower(),
                "blocked_reason": _optional_text(task.get("blocked_reason")),
                "tokens": _signal_tokens(context_text),
            }
        )
    return contexts


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


def _brief_payload(
    brief: Mapping[str, Any] | ImplementationBrief | None,
) -> dict[str, Any]:
    if brief is None:
        return {}
    if hasattr(brief, "model_dump"):
        value = brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ImplementationBrief.model_validate(brief).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(brief, Mapping):
            return dict(brief)
    return {}


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


def _counts(values: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return {key: counts[key] for key in sorted(counts)}


def _brief_likelihood(risk: str, matched_tasks: list[dict[str, Any]]) -> str:
    if matched_tasks:
        return "medium"
    if _tokens(risk) & _HIGH_LIKELIHOOD_TOKENS:
        return "medium"
    return "low"


def _text_severity(text: str) -> str:
    tokens = _tokens(text)
    if tokens & _HIGH_SEVERITY_TOKENS:
        return "high"
    if tokens & _MEDIUM_SEVERITY_TOKENS:
        return "medium"
    return "low"


def _max_severity(values: Iterable[Any]) -> str:
    highest = "low"
    for value in values:
        severity = _severity(value)
        if _SEVERITY_RANK[severity] > _SEVERITY_RANK[highest]:
            highest = severity
    return highest


def _severity(value: Any) -> str:
    text = (_optional_text(value) or "").lower()
    if text in {"critical", "blocker", "high"}:
        return "high"
    if text in {"medium", "moderate"}:
        return "medium"
    return "low"


def _distinct_strings(value: Any) -> list[str]:
    strings: list[str] = []
    seen: set[str] = set()
    for item in _strings(value):
        key = " ".join(_tokens(item))
        if len(key) < 3 or key in seen:
            continue
        strings.append(item)
        seen.add(key)
    return strings


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


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _signal_tokens(value: str) -> set[str]:
    return {
        token
        for token in _tokens(value)
        if len(token) > 2 and token not in _STOPWORDS
    }


def _tokens(value: Any) -> set[str]:
    return set(_TOKEN_RE.findall(str(value).lower()))


def _slug(value: Any) -> str:
    return "-".join(sorted(_tokens(value))) or "item"


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


_SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2}
_HIGH_SEVERITY_TOKENS = {
    "security",
    "privacy",
    "breach",
    "corrupt",
    "outage",
    "incident",
    "loss",
    "blocked",
    "breaking",
}
_MEDIUM_SEVERITY_TOKENS = {
    "dependency",
    "integration",
    "timeout",
    "migration",
    "rollback",
    "customer",
    "contract",
    "delay",
}
_HIGH_LIKELIHOOD_TOKENS = {"known", "existing", "current", "already", "blocked"}
_DOMAIN_TOKENS = {
    "api",
    "auth",
    "billing",
    "cache",
    "client",
    "config",
    "data",
    "database",
    "export",
    "import",
    "migration",
    "queue",
    "rollout",
    "schema",
    "security",
    "sync",
    "webhook",
}
_RISK_TOKENS = _HIGH_SEVERITY_TOKENS | _MEDIUM_SEVERITY_TOKENS
_STOPWORDS = {
    "and",
    "are",
    "can",
    "for",
    "from",
    "has",
    "have",
    "into",
    "may",
    "not",
    "the",
    "this",
    "that",
    "with",
}


__all__ = [
    "ExecutionHandoffRiskRecord",
    "ExecutionHandoffRiskRegister",
    "build_execution_handoff_risk_register",
    "execution_handoff_risk_register_to_dict",
]
