"""Assess canary release readiness for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CanaryReadiness = Literal["ready", "partial", "not_applicable"]
CanaryRiskLevel = Literal["low", "medium", "high"]
CanarySignal = Literal["canary_release", "traffic_slice", "audience_criteria", "success_metrics", "rollback_trigger", "monitoring", "ramp_schedule"]
_SIGNAL_ORDER: tuple[CanarySignal, ...] = ("canary_release", "traffic_slice", "audience_criteria", "success_metrics", "rollback_trigger", "monitoring", "ramp_schedule")
_CRITERIA: tuple[CanarySignal, ...] = ("traffic_slice", "audience_criteria", "success_metrics", "rollback_trigger", "monitoring", "ramp_schedule")
_RISK_ORDER = {"high": 0, "medium": 1, "low": 2}
_SIGNALS: dict[CanarySignal, re.Pattern[str]] = {
    "canary_release": re.compile(r"\b(?:canary release|canary rollout|canary deploy|canary launch|canary)\b", re.I),
    "traffic_slice": re.compile(r"\b(?:traffic slice|traffic percentage|\d+%|percent traffic|small slice|initial slice)\b", re.I),
    "audience_criteria": re.compile(r"\b(?:audience criteria|cohort|segment|internal users?|beta users?|region|tenant allowlist|allowlist)\b", re.I),
    "success_metrics": re.compile(r"\b(?:success metrics?|error rate|latency|conversion|slo|health metric|guardrail metric)\b", re.I),
    "rollback_trigger": re.compile(r"\b(?:rollback trigger|rollback threshold|kill switch|revert trigger|backout criteria|automatic rollback)\b", re.I),
    "monitoring": re.compile(r"\b(?:monitoring|dashboard|alert|observability|datadog|grafana|prometheus|logs)\b", re.I),
    "ramp_schedule": re.compile(r"\b(?:ramp schedule|ramp plan|increase to|ramp to|phased rollout|10%.*50%|hourly ramp|daily ramp)\b", re.I),
}
_GAPS = {
    "traffic_slice": "Define the initial traffic slice or percentage for the canary.",
    "audience_criteria": "Define the user, tenant, region, or cohort criteria for canary exposure.",
    "success_metrics": "Define success metrics and guardrail thresholds for promotion.",
    "rollback_trigger": "Define rollback triggers, thresholds, and ownership.",
    "monitoring": "Define dashboards, alerts, and monitoring coverage during canary.",
    "ramp_schedule": "Define the ramp schedule and promotion checkpoints.",
}
_NEGATED_RE = re.compile(r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}\b(?:canary|canary release|canary rollout)\b.{0,120}\b(?:required|needed|planned|in scope|work)\b|\b(?:canary|canary release|canary rollout)\b.{0,120}\b(?:not required|not needed|out of scope|no work)\b", re.I)
_SPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class TaskCanaryReleaseReadinessFinding:
    task_id: str
    title: str
    readiness: CanaryReadiness
    risk_level: CanaryRiskLevel
    detected_signals: tuple[CanarySignal, ...] = field(default_factory=tuple)
    present_criteria: tuple[CanarySignal, ...] = field(default_factory=tuple)
    missing_criteria: tuple[CanarySignal, ...] = field(default_factory=tuple)
    actionable_gaps: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def recommended_follow_up_actions(self) -> tuple[str, ...]:
        return self.actionable_gaps

    def to_dict(self) -> dict[str, Any]:
        return {"task_id": self.task_id, "title": self.title, "readiness": self.readiness, "risk_level": self.risk_level, "detected_signals": list(self.detected_signals), "present_criteria": list(self.present_criteria), "missing_criteria": list(self.missing_criteria), "actionable_gaps": list(self.actionable_gaps), "evidence": list(self.evidence)}


@dataclass(frozen=True, slots=True)
class TaskCanaryReleaseReadinessPlan:
    plan_id: str | None = None
    findings: tuple[TaskCanaryReleaseReadinessFinding, ...] = field(default_factory=tuple)
    canary_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskCanaryReleaseReadinessFinding, ...]:
        return self.findings

    @property
    def recommendations(self) -> tuple[TaskCanaryReleaseReadinessFinding, ...]:
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        return {"plan_id": self.plan_id, "findings": [item.to_dict() for item in self.findings], "records": [item.to_dict() for item in self.records], "recommendations": [item.to_dict() for item in self.recommendations], "canary_task_ids": list(self.canary_task_ids), "ignored_task_ids": list(self.ignored_task_ids), "summary": dict(self.summary)}

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.findings]

    def to_markdown(self) -> str:
        lines = [f"# Task Canary Release Readiness{': ' + self.plan_id if self.plan_id else ''}", "", f"Canary tasks: {self.summary.get('canary_task_count', 0)} of {self.summary.get('task_count', 0)}"]
        if not self.findings:
            return "\n".join([*lines, "", "No canary release readiness findings were inferred."])
        lines.extend(["", "| Task | Readiness | Risk | Missing Criteria | Evidence |", "| --- | --- | --- | --- | --- |"])
        for item in self.findings:
            lines.append(f"| `{_cell(item.task_id)}` {_cell(item.title)} | {item.readiness} | {item.risk_level} | {_cell(', '.join(item.missing_criteria) or 'none')} | {_cell('; '.join(item.evidence))} |")
        return "\n".join(lines)


def build_task_canary_release_readiness_plan(source: Any) -> TaskCanaryReleaseReadinessPlan:
    plan_id, tasks = _source_payload(source)
    findings = tuple(sorted((finding for index, task in enumerate(tasks, 1) if (finding := _finding(task, index)) is not None), key=lambda item: (_RISK_ORDER[item.risk_level], item.task_id)))
    canary_ids = tuple(item.task_id for item in findings)
    canary_set = set(canary_ids)
    ignored = tuple(_task_id(task, index) for index, task in enumerate(tasks, 1) if _task_id(task, index) not in canary_set)
    return TaskCanaryReleaseReadinessPlan(plan_id, findings, canary_ids, ignored, _summary(findings, len(tasks), len(ignored)))


analyze_task_canary_release_readiness = build_task_canary_release_readiness_plan
summarize_task_canary_release_readiness = build_task_canary_release_readiness_plan


def recommend_task_canary_release_readiness(source: Any) -> tuple[TaskCanaryReleaseReadinessFinding, ...]:
    return build_task_canary_release_readiness_plan(source).findings


generate_task_canary_release_readiness = recommend_task_canary_release_readiness


def task_canary_release_readiness_plan_to_dict(plan: TaskCanaryReleaseReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_canary_release_readiness_plan_to_dict.__test__ = False


def task_canary_release_readiness_to_dicts(items: TaskCanaryReleaseReadinessPlan | Iterable[TaskCanaryReleaseReadinessFinding]) -> list[dict[str, Any]]:
    if isinstance(items, TaskCanaryReleaseReadinessPlan):
        return items.to_dicts()
    return [item.to_dict() for item in items]


task_canary_release_readiness_to_dicts.__test__ = False


def task_canary_release_readiness_plan_to_markdown(plan: TaskCanaryReleaseReadinessPlan) -> str:
    return plan.to_markdown()


task_canary_release_readiness_plan_to_markdown.__test__ = False


def _finding(task: Mapping[str, Any], index: int) -> TaskCanaryReleaseReadinessFinding | None:
    text_items = _candidate_texts(task)
    combined = " ".join(text for _, text in text_items)
    if _NEGATED_RE.search(combined):
        return None
    evidence_by_signal: dict[CanarySignal, list[str]] = {}
    for field_name, text in text_items:
        for signal, pattern in _SIGNALS.items():
            if pattern.search(text):
                evidence_by_signal.setdefault(signal, []).append(_evidence(field_name, text))
    if "canary_release" not in evidence_by_signal:
        return None
    detected = tuple(signal for signal in _SIGNAL_ORDER if signal in evidence_by_signal)
    present = tuple(signal for signal in _CRITERIA if signal in evidence_by_signal)
    missing = tuple(signal for signal in _CRITERIA if signal not in evidence_by_signal)
    readiness: CanaryReadiness = "ready" if not missing else "partial"
    risk: CanaryRiskLevel = "low" if not missing else ("high" if "rollback_trigger" in missing or "monitoring" in missing else "medium")
    return TaskCanaryReleaseReadinessFinding(_task_id(task, index), _text(task.get("title")) or _task_id(task, index), readiness, risk, detected, present, missing, tuple(_GAPS[item] for item in missing), tuple(dedupe(ev for signal in detected for ev in evidence_by_signal.get(signal, [])))[:8])


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _text(source.id) or None, [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _text(payload.get("id")) or None, _tasks(payload.get("tasks"))
        return None, [dict(source)]
    try:
        iterator = iter(source)
    except TypeError:
        return None, []
    return None, _tasks(list(iterator))


def _plan_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return dict(ExecutionPlan.model_validate(plan).model_dump(mode="python"))
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _tasks(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    tasks = []
    for item in (sorted(value, key=lambda entry: str(entry)) if isinstance(value, set) else value):
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            dumped = item.model_dump(mode="python")
            if isinstance(dumped, Mapping):
                tasks.append(dict(dumped))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    fields = ("title", "description", "milestone", "owner_type", "risk_level", "test_command", "blocked_reason")
    out = [(field, text) for field in fields if (text := _text(task.get(field)))]
    for field in ("acceptance_criteria", "tags", "labels", "notes", "risks", "dependencies", "depends_on", "files_or_modules"):
        for index, text in enumerate(_strings(task.get(field))):
            out.append((f"{field}[{index}]", text))
    out.extend(_metadata_texts(task.get("metadata")))
    return out


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        out = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                out.extend(_metadata_texts(child, field))
            elif text := _text(child):
                out.append((field, f"{key_text}: {text}"))
            else:
                out.append((field, key_text))
        return out
    if isinstance(value, (list, tuple, set)):
        return [(f"{prefix}[{index}]", text) for index, item in enumerate(value) if (text := _text(item))]
    text = _text(value)
    return [(prefix, text)] if text else []


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [_text(value)] if _text(value) else []
    if isinstance(value, Mapping):
        return [text for key in sorted(value, key=lambda item: str(item)) for text in _strings(value[key])]
    if isinstance(value, (list, tuple, set)):
        return [text for item in (sorted(value, key=lambda entry: str(entry)) if isinstance(value, set) else value) for text in _strings(item)]
    text = _text(value)
    return [text] if text else []


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _text(task.get("id")) or f"task-{index}"


def _summary(findings: tuple[TaskCanaryReleaseReadinessFinding, ...], task_count: int, ignored_count: int) -> dict[str, Any]:
    return {"task_count": task_count, "canary_task_count": len(findings), "ignored_task_count": ignored_count, "readiness_counts": {value: sum(1 for item in findings if item.readiness == value) for value in ("ready", "partial", "not_applicable")}, "risk_counts": {value: sum(1 for item in findings if item.risk_level == value) for value in ("high", "medium", "low")}, "missing_criterion_counts": {item: sum(1 for finding in findings if item in finding.missing_criteria) for item in _CRITERIA}}


def _evidence(field_name: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{field_name}: {value}"


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def dedupe(values: Iterable[str | None]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if not value:
            continue
        key = value.casefold()
        if key not in seen:
            seen.add(key)
            out.append(value)
    return out


__all__ = [
    "CanaryReadiness",
    "CanaryRiskLevel",
    "CanarySignal",
    "TaskCanaryReleaseReadinessFinding",
    "TaskCanaryReleaseReadinessPlan",
    "analyze_task_canary_release_readiness",
    "build_task_canary_release_readiness_plan",
    "generate_task_canary_release_readiness",
    "recommend_task_canary_release_readiness",
    "summarize_task_canary_release_readiness",
    "task_canary_release_readiness_plan_to_dict",
    "task_canary_release_readiness_plan_to_markdown",
    "task_canary_release_readiness_to_dicts",
]
