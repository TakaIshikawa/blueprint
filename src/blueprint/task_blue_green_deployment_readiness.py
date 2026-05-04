"""Plan blue/green deployment readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


BlueGreenDeploymentSignal = Literal[
    "traffic_switching",
    "health_checks",
    "database_compatibility",
    "rollback_trigger",
    "monitoring",
    "smoke_validation",
    "operator_ownership",
]
BlueGreenDeploymentSafeguard = Literal[
    "traffic_switch_mechanism",
    "health_check_validation",
    "database_schema_compatibility",
    "rollback_procedure",
    "deployment_monitoring",
    "smoke_test_execution",
    "operator_handoff",
]
BlueGreenDeploymentReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[BlueGreenDeploymentReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[BlueGreenDeploymentSignal, ...] = (
    "traffic_switching",
    "health_checks",
    "database_compatibility",
    "rollback_trigger",
    "monitoring",
    "smoke_validation",
    "operator_ownership",
)
_SAFEGUARD_ORDER: tuple[BlueGreenDeploymentSafeguard, ...] = (
    "traffic_switch_mechanism",
    "health_check_validation",
    "database_schema_compatibility",
    "rollback_procedure",
    "deployment_monitoring",
    "smoke_test_execution",
    "operator_handoff",
)
_SIGNAL_PATTERNS: dict[BlueGreenDeploymentSignal, re.Pattern[str]] = {
    "traffic_switching": re.compile(
        r"\b(?:traffic switch|switch traffic|traffic routing|route traffic|traffic cutover|traffic migration|"
        r"traffic shift|shift traffic|load balancer|routing rule|traffic director|switch endpoint)\b",
        re.I,
    ),
    "health_checks": re.compile(
        r"\b(?:health check|healthcheck|health endpoint|health validation|readiness probe|liveness probe|"
        r"health monitor|health status|service health|health verification)\b",
        re.I,
    ),
    "database_compatibility": re.compile(
        r"\b(?:database compatibility|schema compatibility|backward compatible|forward compatible|"
        r"db migration|schema migration|database version|schema version|dual write|migration strategy)\b",
        re.I,
    ),
    "rollback_trigger": re.compile(
        r"\b(?:rollback|roll back|revert|switch back|failback|fallback|rollback trigger|rollback condition|"
        r"rollback plan|rollback procedure|rollback strategy|deployment revert)\b",
        re.I,
    ),
    "monitoring": re.compile(
        r"\b(?:monitor|monitoring|observability|metric|metrics|alert|alerting|dashboard|"
        r"error rate|latency|success rate|deployment metric|canary metric)\b",
        re.I,
    ),
    "smoke_validation": re.compile(
        r"\b(?:smoke test|smoke validation|smoke check|sanity test|sanity check|"
        r"post-deployment test|deployment validation|integration test|critical path)\b",
        re.I,
    ),
    "operator_ownership": re.compile(
        r"\b(?:operator|ops team|sre|site reliability|operations team|on-call|runbook|"
        r"deployment owner|deployment responsibility|manual approval|operator approval)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[BlueGreenDeploymentSignal, re.Pattern[str]] = {
    "traffic_switching": re.compile(r"traffic|routing|switch|loadbalancer|lb|router", re.I),
    "health_checks": re.compile(r"health|readiness|liveness|probe", re.I),
    "database_compatibility": re.compile(r"migration|schema|database|db", re.I),
    "rollback_trigger": re.compile(r"rollback|revert|fallback", re.I),
    "monitoring": re.compile(r"monitor|metric|alert|dashboard|observability", re.I),
    "smoke_validation": re.compile(r"smoke|sanity|validation|test", re.I),
    "operator_ownership": re.compile(r"runbook|ops|operator|deployment", re.I),
}
_SAFEGUARD_PATTERNS: dict[BlueGreenDeploymentSafeguard, re.Pattern[str]] = {
    "traffic_switch_mechanism": re.compile(
        r"\b(?:(?:traffic|routing).{0,80}(?:switch|cut|route|direct|config|mechanism|implementation|procedure)|"
        r"(?:switch|cut|route|direct|config|mechanism|implementation|procedure).{0,80}(?:traffic|routing)|"
        r"load balancer config|routing rule|endpoint switch|dns update|traffic percentage)\b",
        re.I,
    ),
    "health_check_validation": re.compile(
        r"\b(?:(?:health|readiness|liveness).{0,80}(?:check|validation|verify|probe|endpoint|test|assert)|"
        r"(?:check|validation|verify|probe|endpoint|test|assert).{0,80}(?:health|readiness|liveness)|"
        r"health validation|readiness validation|health probe|service ready)\b",
        re.I,
    ),
    "database_schema_compatibility": re.compile(
        r"\b(?:(?:schema|database).{0,80}(?:compatibility|compatible|version|migration|backward|forward|dual write)|"
        r"(?:compatibility|compatible|version|migration|backward|forward|dual write).{0,80}(?:schema|database)|"
        r"backward compatible|forward compatible|schema version|migration script)\b",
        re.I,
    ),
    "rollback_procedure": re.compile(
        r"\b(?:(?:rollback|revert).{0,80}(?:procedure|plan|strategy|trigger|condition|script|automation|step)|"
        r"(?:procedure|plan|strategy|trigger|condition|script|automation|step).{0,80}(?:rollback|revert)|"
        r"rollback plan|rollback trigger|revert procedure|failback strategy)\b",
        re.I,
    ),
    "deployment_monitoring": re.compile(
        r"(?:(?:deployment|release).{0,80}(?:monitor|metric|alert|dashboard|observability|tracking|error rate|latency)|"
        r"(?:monitor|metric|alert|dashboard|observability|tracking|error rate|latency).{0,80}(?:deployment|release)|"
        r"(?:set up|setup|configure).{0,40}(?:deployment.{0,20})?monitor|"
        r"deployment metric|deployment dashboard|deployment alert|error tracking)",
        re.I,
    ),
    "smoke_test_execution": re.compile(
        r"\b(?:(?:smoke|sanity).{0,80}(?:test|validation|check|execution|run|suite|automation|covering|critical)|"
        r"(?:test|validation|check|execution|run|suite|automation|execute).{0,80}(?:smoke|sanity|critical path|integration point)|"
        r"post-deployment test|smoke test suite|critical path test|validation script|execute.{0,40}smoke)\b",
        re.I,
    ),
    "operator_handoff": re.compile(
        r"\b(?:(?:operator|ops|sre).{0,80}(?:handoff|approval|ownership|responsibility|runbook|procedure|documentation)|"
        r"(?:handoff|approval|ownership|responsibility|runbook|procedure|documentation).{0,80}(?:operator|ops|sre)|"
        r"manual approval|operator approval|runbook|deployment guide|ops documentation)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:deployment|blue.?green|traffic switch|rollout)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[BlueGreenDeploymentSafeguard, str] = {
    "traffic_switch_mechanism": "Define traffic switching mechanism (load balancer configuration, DNS update, routing rules, or service mesh) with percentage-based rollout.",
    "health_check_validation": "Implement health check validation with readiness and liveness probes to verify green environment before traffic switch.",
    "database_schema_compatibility": "Ensure database schema compatibility with backward-compatible migrations, dual-write support, or version checks.",
    "rollback_procedure": "Document rollback procedure with clear trigger conditions, automation steps, and time constraints for reverting to blue environment.",
    "deployment_monitoring": "Implement deployment monitoring with key metrics (error rate, latency, throughput), alerts, and dashboards for real-time tracking.",
    "smoke_test_execution": "Execute smoke tests on green environment covering critical paths and integration points before traffic switch.",
    "operator_handoff": "Define operator ownership with runbook, approval gates, manual verification steps, and on-call responsibilities.",
}


@dataclass(frozen=True, slots=True)
class TaskBlueGreenDeploymentReadinessFinding:
    """Blue/green deployment readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[BlueGreenDeploymentSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[BlueGreenDeploymentSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[BlueGreenDeploymentSafeguard, ...] = field(default_factory=tuple)
    readiness: BlueGreenDeploymentReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actionable_remediations: tuple[str, ...] = field(default_factory=tuple)

    @property
    def actionable_gaps(self) -> tuple[str, ...]:
        """Compatibility view for readiness modules that expose gaps."""
        return self.actionable_remediations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "actionable_remediations": list(self.actionable_remediations),
        }


@dataclass(frozen=True, slots=True)
class TaskBlueGreenDeploymentReadinessPlan:
    """Plan-level blue/green deployment readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskBlueGreenDeploymentReadinessFinding, ...] = field(default_factory=tuple)
    blue_green_deployment_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskBlueGreenDeploymentReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "blue_green_deployment_task_ids": list(self.blue_green_deployment_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }


def plan_task_blue_green_deployment_readiness(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskBlueGreenDeploymentReadinessPlan:
    """Generate blue/green deployment readiness safeguards for all tasks in a plan."""
    plan_id, tasks = _plan_payload(plan)
    findings: list[TaskBlueGreenDeploymentReadinessFinding] = []
    blue_green_deployment_task_ids: list[str] = []
    not_applicable_task_ids: list[str] = []

    for task in tasks:
        task_id, task_payload = _task_payload(task)
        if not task_id:
            continue

        signals = _detect_signals(task_payload)
        if _has_no_impact(task_payload) or not signals:
            not_applicable_task_ids.append(task_id)
            continue

        blue_green_deployment_task_ids.append(task_id)
        present_safeguards = _detect_safeguards(task_payload, signals)
        missing_safeguards = tuple(
            safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in present_safeguards
        )
        evidence = _collect_evidence(task_payload, signals)
        remediations = tuple(_REMEDIATIONS[safeguard] for safeguard in missing_safeguards[:3])
        readiness = _assess_readiness(signals, present_safeguards, missing_safeguards)

        findings.append(
            TaskBlueGreenDeploymentReadinessFinding(
                task_id=task_id,
                title=_task_title(task_payload),
                detected_signals=signals,
                present_safeguards=present_safeguards,
                missing_safeguards=missing_safeguards,
                readiness=readiness,
                evidence=evidence,
                actionable_remediations=remediations,
            )
        )

    return TaskBlueGreenDeploymentReadinessPlan(
        plan_id=plan_id,
        findings=tuple(findings),
        blue_green_deployment_task_ids=tuple(blue_green_deployment_task_ids),
        not_applicable_task_ids=tuple(not_applicable_task_ids),
        summary=_plan_summary(findings, blue_green_deployment_task_ids, not_applicable_task_ids),
    )


def task_blue_green_deployment_readiness_to_dict(
    plan: TaskBlueGreenDeploymentReadinessPlan,
) -> dict[str, Any]:
    """Serialize a blue/green deployment readiness plan to a plain dictionary."""
    return plan.to_dict()


task_blue_green_deployment_readiness_to_dict.__test__ = False


def _plan_payload(plan: ExecutionPlan | Mapping[str, Any] | str | object) -> tuple[str | None, list[Any]]:
    if isinstance(plan, str):
        return None, []
    if isinstance(plan, ExecutionPlan):
        return plan.id, list(plan.tasks)
    if hasattr(plan, "model_dump"):
        payload = plan.model_dump(mode="python")
        if isinstance(payload, Mapping):
            return _optional_text(payload.get("id")), list(payload.get("tasks", []))
    if isinstance(plan, Mapping):
        try:
            execution_plan = ExecutionPlan.model_validate(plan)
            return execution_plan.id, list(execution_plan.tasks)
        except (TypeError, ValueError, ValidationError):
            pass
        return _optional_text(plan.get("id")), list(plan.get("tasks", []))
    if hasattr(plan, "tasks"):
        plan_id = getattr(plan, "id", None)
        return _optional_text(plan_id), list(plan.tasks) if plan.tasks else []
    return None, []


def _task_payload(task: ExecutionTask | Mapping[str, Any] | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(task, ExecutionTask):
        return task.task_id, dict(task.model_dump(mode="python"))
    if hasattr(task, "model_dump"):
        payload = task.model_dump(mode="python")
        if isinstance(payload, Mapping):
            return _optional_text(payload.get("task_id")), dict(payload)
    if isinstance(task, Mapping):
        try:
            execution_task = ExecutionTask.model_validate(task)
            return execution_task.task_id, dict(execution_task.model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            pass
        return _optional_text(task.get("task_id")), dict(task)
    task_id = getattr(task, "task_id", None)
    payload = {}
    for field in ("task_id", "title", "prompt", "scope", "outputs", "validation", "test_files"):
        if hasattr(task, field):
            payload[field] = getattr(task, field)
    return _optional_text(task_id), payload


def _detect_signals(task_payload: Mapping[str, Any]) -> tuple[BlueGreenDeploymentSignal, ...]:
    text = _searchable_text(task_payload)
    paths = _searchable_paths(task_payload)
    signals: list[BlueGreenDeploymentSignal] = []

    for signal in _SIGNAL_ORDER:
        if _SIGNAL_PATTERNS[signal].search(text):
            signals.append(signal)
        elif paths and _PATH_SIGNAL_PATTERNS[signal].search(paths):
            signals.append(signal)

    return tuple(signals)


def _detect_safeguards(
    task_payload: Mapping[str, Any],
    signals: tuple[BlueGreenDeploymentSignal, ...],
) -> tuple[BlueGreenDeploymentSafeguard, ...]:
    text = _searchable_text(task_payload)
    safeguards: list[BlueGreenDeploymentSafeguard] = []

    for safeguard in _SAFEGUARD_ORDER:
        if _SAFEGUARD_PATTERNS[safeguard].search(text):
            safeguards.append(safeguard)

    return tuple(safeguards)


def _has_no_impact(task_payload: Mapping[str, Any]) -> bool:
    text = _searchable_text(task_payload)
    return bool(_NO_IMPACT_RE.search(text))


def _collect_evidence(
    task_payload: Mapping[str, Any],
    signals: tuple[BlueGreenDeploymentSignal, ...],
) -> tuple[str, ...]:
    evidence: list[str] = []
    for field_name in ("title", "prompt", "scope"):
        if field_name in task_payload and task_payload[field_name]:
            text = _clean_text(str(task_payload[field_name]))
            if text and any(_SIGNAL_PATTERNS[signal].search(text) for signal in signals):
                snippet = text[:200] + ("..." if len(text) > 200 else "")
                evidence.append(f"{field_name}: {snippet}")
    return tuple(evidence[:3])


def _assess_readiness(
    signals: tuple[BlueGreenDeploymentSignal, ...],
    present_safeguards: tuple[BlueGreenDeploymentSafeguard, ...],
    missing_safeguards: tuple[BlueGreenDeploymentSafeguard, ...],
) -> BlueGreenDeploymentReadiness:
    if not signals:
        return "weak"
    safeguard_coverage = len(present_safeguards) / (len(present_safeguards) + len(missing_safeguards)) if (len(present_safeguards) + len(missing_safeguards)) > 0 else 0.0
    if safeguard_coverage >= 0.75:
        return "strong"
    if safeguard_coverage >= 0.5:
        return "partial"
    return "weak"


def _plan_summary(
    findings: list[TaskBlueGreenDeploymentReadinessFinding],
    blue_green_deployment_task_ids: list[str],
    not_applicable_task_ids: list[str],
) -> dict[str, Any]:
    return {
        "total_tasks_reviewed": len(blue_green_deployment_task_ids) + len(not_applicable_task_ids),
        "blue_green_deployment_task_count": len(blue_green_deployment_task_ids),
        "not_applicable_task_count": len(not_applicable_task_ids),
        "findings_count": len(findings),
        "readiness_distribution": {
            "weak": sum(1 for finding in findings if finding.readiness == "weak"),
            "partial": sum(1 for finding in findings if finding.readiness == "partial"),
            "strong": sum(1 for finding in findings if finding.readiness == "strong"),
        },
        "common_missing_safeguards": _top_missing_safeguards(findings),
        "overall_readiness": _overall_readiness(findings),
    }


def _overall_readiness(findings: list[TaskBlueGreenDeploymentReadinessFinding]) -> BlueGreenDeploymentReadiness:
    if not findings:
        return "weak"
    weak_count = sum(1 for finding in findings if finding.readiness == "weak")
    if weak_count > len(findings) / 2:
        return "weak"
    strong_count = sum(1 for finding in findings if finding.readiness == "strong")
    if strong_count >= len(findings) * 0.7:
        return "strong"
    return "partial"


def _top_missing_safeguards(findings: list[TaskBlueGreenDeploymentReadinessFinding]) -> list[str]:
    safeguard_counts: dict[BlueGreenDeploymentSafeguard, int] = {}
    for finding in findings:
        for safeguard in finding.missing_safeguards:
            safeguard_counts[safeguard] = safeguard_counts.get(safeguard, 0) + 1
    sorted_safeguards = sorted(
        safeguard_counts.items(),
        key=lambda item: (item[1], _SAFEGUARD_ORDER.index(item[0])),
        reverse=True,
    )
    return [safeguard for safeguard, _ in sorted_safeguards[:5]]


def _searchable_text(task_payload: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for field_name in ("title", "prompt", "scope", "outputs", "validation"):
        if field_name in task_payload and task_payload[field_name]:
            parts.append(_clean_text(str(task_payload[field_name])))
    return " ".join(parts)


def _searchable_paths(task_payload: Mapping[str, Any]) -> str:
    paths: list[str] = []
    for field_name in ("outputs", "test_files"):
        if field_name in task_payload:
            value = task_payload[field_name]
            if isinstance(value, (list, tuple)):
                for item in value:
                    if item:
                        try:
                            path = PurePosixPath(str(item))
                            paths.append(path.name)
                            if path.parent != PurePosixPath("."):
                                paths.append(str(path.parent))
                        except (ValueError, TypeError):
                            continue
            elif value:
                try:
                    path = PurePosixPath(str(value))
                    paths.append(path.name)
                    if path.parent != PurePosixPath("."):
                        paths.append(str(path.parent))
                except (ValueError, TypeError):
                    pass
    return " ".join(paths)


def _task_title(task_payload: Mapping[str, Any]) -> str:
    return _clean_text(str(task_payload.get("title", ""))) or "Untitled task"


def _clean_text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    text = str(value)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


__all__ = [
    "BlueGreenDeploymentReadiness",
    "BlueGreenDeploymentSafeguard",
    "BlueGreenDeploymentSignal",
    "TaskBlueGreenDeploymentReadinessFinding",
    "TaskBlueGreenDeploymentReadinessPlan",
    "plan_task_blue_green_deployment_readiness",
    "task_blue_green_deployment_readiness_to_dict",
]
