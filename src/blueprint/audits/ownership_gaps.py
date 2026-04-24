"""Task ownership clarity audit for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


DEFAULT_OWNERSHIP_THRESHOLD = 5
"""Maximum recommended task count for a single owner group."""

Severity = Literal["blocking", "warning"]

_HUMAN_OWNER_TYPES = {"human", "person", "team"}
_AGENT_OWNER_TYPES = {"agent", "ai", "autonomous_agent"}
_MANUAL_ENGINES = {"manual", "human"}


@dataclass(frozen=True)
class OwnershipGapFinding:
    """A single ambiguous ownership or overloaded ownership finding."""

    severity: Severity
    code: str
    message: str
    remediation: str
    task_ids: list[str] = field(default_factory=list)
    owner_type: str | None = None
    suggested_engine: str | None = None
    task_count: int | None = None
    threshold: int | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "remediation": self.remediation,
            "task_ids": self.task_ids,
        }
        if self.owner_type is not None:
            payload["owner_type"] = self.owner_type
        if self.suggested_engine is not None:
            payload["suggested_engine"] = self.suggested_engine
        if self.task_count is not None:
            payload["task_count"] = self.task_count
        if self.threshold is not None:
            payload["threshold"] = self.threshold
        return payload


@dataclass(frozen=True)
class OwnershipGapResult:
    """Ownership clarity audit result for an execution plan."""

    plan_id: str
    task_count: int
    threshold: int
    findings: list[OwnershipGapFinding] = field(default_factory=list)

    @property
    def blocking_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "blocking")

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "warning")

    @property
    def passed(self) -> bool:
        return not self.findings

    def findings_by_severity(self) -> dict[str, list[OwnershipGapFinding]]:
        return {
            "blocking": [
                finding for finding in self.findings if finding.severity == "blocking"
            ],
            "warning": [
                finding for finding in self.findings if finding.severity == "warning"
            ],
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "threshold": self.threshold,
            "passed": self.passed,
            "summary": {
                "blocking": self.blocking_count,
                "warning": self.warning_count,
                "findings": len(self.findings),
            },
            "findings": [finding.to_dict() for finding in self.findings],
        }


def audit_ownership_gaps(
    plan: dict[str, Any],
    *,
    threshold: int = DEFAULT_OWNERSHIP_THRESHOLD,
) -> OwnershipGapResult:
    """Find tasks with unclear ownership and owner groups above the threshold."""
    tasks = _list_of_dicts(plan.get("tasks"))
    findings: list[OwnershipGapFinding] = []

    findings.extend(_missing_owner_findings(tasks))
    findings.extend(_missing_engine_findings(tasks))
    findings.extend(_conflicting_owner_engine_findings(tasks))
    findings.extend(_overloaded_owner_findings(tasks, threshold))

    return OwnershipGapResult(
        plan_id=str(plan.get("id") or ""),
        task_count=len(tasks),
        threshold=threshold,
        findings=findings,
    )


def _missing_owner_findings(tasks: list[dict[str, Any]]) -> list[OwnershipGapFinding]:
    return [
        OwnershipGapFinding(
            severity="blocking",
            code="missing_owner_type",
            message=f"Task {_task_id(task)} has no owner_type assignment.",
            remediation="Set owner_type to human, agent, or the responsible ownership lane.",
            task_ids=[_task_id(task)],
            suggested_engine=_normalized_value(task.get("suggested_engine")),
        )
        for task in tasks
        if _normalized_key(task.get("owner_type")) == "unspecified"
    ]


def _missing_engine_findings(tasks: list[dict[str, Any]]) -> list[OwnershipGapFinding]:
    return [
        OwnershipGapFinding(
            severity="blocking",
            code="missing_suggested_engine",
            message=f"Task {_task_id(task)} has no suggested_engine assignment.",
            remediation="Set suggested_engine to the execution engine or manual lane expected to do the work.",
            task_ids=[_task_id(task)],
            owner_type=_normalized_value(task.get("owner_type")),
        )
        for task in tasks
        if _normalized_key(task.get("suggested_engine")) == "unspecified"
    ]


def _conflicting_owner_engine_findings(
    tasks: list[dict[str, Any]],
) -> list[OwnershipGapFinding]:
    findings: list[OwnershipGapFinding] = []
    for task in tasks:
        owner_key = _normalized_key(task.get("owner_type"))
        engine_key = _normalized_key(task.get("suggested_engine"))
        if "unspecified" in {owner_key, engine_key}:
            continue
        if owner_key in _HUMAN_OWNER_TYPES and engine_key not in _MANUAL_ENGINES:
            findings.append(
                _conflict_finding(
                    task,
                    "Human-owned tasks should use a manual or human execution lane.",
                )
            )
        elif owner_key in _AGENT_OWNER_TYPES and engine_key in _MANUAL_ENGINES:
            findings.append(
                _conflict_finding(
                    task,
                    "Agent-owned tasks should use an autonomous execution engine.",
                )
            )
    return findings


def _conflict_finding(task: dict[str, Any], message: str) -> OwnershipGapFinding:
    return OwnershipGapFinding(
        severity="warning",
        code="conflicting_owner_engine",
        message=f"Task {_task_id(task)} has a conflicting owner/engine assignment. {message}",
        remediation="Align owner_type and suggested_engine so handoff routing is unambiguous.",
        task_ids=[_task_id(task)],
        owner_type=_normalized_value(task.get("owner_type")),
        suggested_engine=_normalized_value(task.get("suggested_engine")),
    )


def _overloaded_owner_findings(
    tasks: list[dict[str, Any]],
    threshold: int,
) -> list[OwnershipGapFinding]:
    task_ids_by_owner: dict[str, list[str]] = {}
    for task in tasks:
        owner_key = _normalized_key(task.get("owner_type"))
        if owner_key == "unspecified":
            continue
        task_ids_by_owner.setdefault(owner_key, []).append(_task_id(task))

    findings: list[OwnershipGapFinding] = []
    for owner_type, task_ids in sorted(task_ids_by_owner.items()):
        if len(task_ids) <= threshold:
            continue
        findings.append(
            OwnershipGapFinding(
                severity="warning",
                code="overloaded_owner_group",
                message=(
                    f"Owner group {owner_type} has {len(task_ids)} tasks, "
                    f"above the threshold of {threshold}."
                ),
                remediation="Split this work across additional owner groups or raise the threshold if this lane is intentional.",
                task_ids=task_ids,
                owner_type=owner_type,
                task_count=len(task_ids),
                threshold=threshold,
            )
        )
    return findings


def _normalized_key(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    return "unspecified"


def _normalized_value(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _task_id(task: dict[str, Any]) -> str:
    return str(task.get("id") or "")


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
