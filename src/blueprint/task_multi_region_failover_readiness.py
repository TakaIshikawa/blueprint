"""Plan multi-region failover readiness controls for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FailoverSurface = Literal[
    "multi_region_deployment",
    "failover",
    "active_active_routing",
    "active_passive_routing",
    "regional_replica",
    "disaster_recovery",
    "dns_failover",
    "traffic_steering",
    "data_replication",
    "customer_facing",
]
FailoverControl = Literal[
    "failover_trigger",
    "health_check_signal",
    "data_replication_validation",
    "traffic_steering_plan",
    "regional_rollback_path",
    "runbook_owner",
    "recovery_metric",
]
FailoverRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[FailoverRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER: tuple[FailoverSurface, ...] = (
    "multi_region_deployment",
    "failover",
    "active_active_routing",
    "active_passive_routing",
    "regional_replica",
    "disaster_recovery",
    "dns_failover",
    "traffic_steering",
    "data_replication",
    "customer_facing",
)
_CONTROL_ORDER: tuple[FailoverControl, ...] = (
    "failover_trigger",
    "health_check_signal",
    "data_replication_validation",
    "traffic_steering_plan",
    "regional_rollback_path",
    "runbook_owner",
    "recovery_metric",
)
_HIGH_RISK_SURFACES = {"customer_facing", "data_replication", "regional_replica"}
_ROUTING_SURFACES = {
    "active_active_routing",
    "active_passive_routing",
    "dns_failover",
    "traffic_steering",
}

_PATH_SURFACE_PATTERNS: tuple[tuple[FailoverSurface, re.Pattern[str]], ...] = (
    ("multi_region_deployment", re.compile(r"(?:^|/)(?:multi[-_]?region|regions?|regional|geo)(?:/|\.|_|-|$)", re.I)),
    ("failover", re.compile(r"(?:^|/)(?:failover|fail[-_]?over|fallback[-_]?region)(?:/|\.|_|-|$)", re.I)),
    ("active_active_routing", re.compile(r"(?:^|/)(?:active[-_]?active|aa[-_]?routing)(?:/|\.|_|-|$)", re.I)),
    ("active_passive_routing", re.compile(r"(?:^|/)(?:active[-_]?passive|primary[-_]?secondary|ap[-_]?routing)(?:/|\.|_|-|$)", re.I)),
    ("regional_replica", re.compile(r"(?:^|/)(?:replicas?|regional[-_]?replicas?|read[-_]?replicas?)(?:/|\.|_|-|$)", re.I)),
    ("disaster_recovery", re.compile(r"(?:^|/)(?:disaster[-_]?recovery|dr|business[-_]?continuity)(?:/|\.|_|-|$)", re.I)),
    ("dns_failover", re.compile(r"(?:^|/)(?:dns|route53|cloudflare|global[-_]?accelerator)(?:/|\.|_|-|$)", re.I)),
    ("traffic_steering", re.compile(r"(?:^|/)(?:traffic[-_]?steering|weighted[-_]?routing|geo[-_]?routing|load[-_]?balanc(?:er|ing))(?:/|\.|_|-|$)", re.I)),
    ("data_replication", re.compile(r"(?:^|/)(?:replication|cdc|cross[-_]?region[-_]?data|regional[-_]?data)(?:/|\.|_|-|$)", re.I)),
    ("customer_facing", re.compile(r"(?:^|/)(?:edge|public|customer|checkout|api|frontend)(?:/|\.|_|-|$)", re.I)),
)
_TEXT_SURFACE_PATTERNS: dict[FailoverSurface, re.Pattern[str]] = {
    "multi_region_deployment": re.compile(r"\b(?:multi[- ]region|multiple regions|cross[- ]region|regional deployment|deploy(?:ed)? to regions|geo[- ]redundant)\b", re.I),
    "failover": re.compile(r"\b(?:failover|fail over|fail-over|fallback region|regional failover|automated failover|manual failover)\b", re.I),
    "active_active_routing": re.compile(r"\b(?:active[- ]active|active/active|dual active|both regions serve traffic)\b", re.I),
    "active_passive_routing": re.compile(r"\b(?:active[- ]passive|active/passive|primary[- ]secondary|warm standby|cold standby|passive region)\b", re.I),
    "regional_replica": re.compile(r"\b(?:regional replicas?|read replicas?|replica lag|secondary replicas?|cross[- ]region replicas?)\b", re.I),
    "disaster_recovery": re.compile(r"\b(?:disaster recovery|dr plan|business continuity|regional outage|region outage|region evacuation)\b", re.I),
    "dns_failover": re.compile(r"\b(?:dns failover|route53|route 53|cloudflare load balancing|health checked dns|global accelerator)\b", re.I),
    "traffic_steering": re.compile(r"\b(?:traffic steering|weighted routing|geo routing|latency routing|global load balanc(?:er|ing)|route traffic)\b", re.I),
    "data_replication": re.compile(r"\b(?:data replication|replication validation|cross[- ]region data|cdc|replication lag|replicated data)\b", re.I),
    "customer_facing": re.compile(r"\b(?:customer[- ]facing|public api|end users?|checkout|login traffic|user traffic|production traffic|external customers?)\b", re.I),
}
_CONTROL_PATTERNS: dict[FailoverControl, re.Pattern[str]] = {
    "failover_trigger": re.compile(r"\b(?:failover trigger|trigger threshold|promotion trigger|manual trigger|automatic trigger|go/no-go|decision point|when to fail over)\b", re.I),
    "health_check_signal": re.compile(r"\b(?:health checks?|health signal|synthetic checks?|readiness probe|liveness probe|canary health|regional health|monitoring signal)\b", re.I),
    "data_replication_validation": re.compile(r"\b(?:data replication validation|replication validation|replica lag|consistency check|data reconciliation|row counts?|checksum|cdc validation)\b", re.I),
    "traffic_steering_plan": re.compile(r"\b(?:traffic steering plan|weighted routing|geo routing|dns routing|route traffic|shift traffic|drain traffic|global load balancer|traffic weights?)\b", re.I),
    "regional_rollback_path": re.compile(r"\b(?:regional rollback|rollback path|roll back region|restore primary|restore traffic|revert routing|rollback to primary|regional revert)\b", re.I),
    "runbook_owner": re.compile(r"\b(?:runbook owner|owner on[- ]call|operational owner|dr owner|incident commander|responsible owner|escalation owner)\b", re.I),
    "recovery_metric": re.compile(r"\b(?:recovery metric|rto|rpo|time to recover|recovery objective|recovery time|success metric|availability target|error budget)\b", re.I),
}
_CONTROL_GUIDANCE: dict[FailoverControl, str] = {
    "failover_trigger": "Define the exact manual or automatic trigger for regional failover.",
    "health_check_signal": "Identify health checks, synthetic probes, or monitoring signals used to decide regional health.",
    "data_replication_validation": "Validate replicated data, replica lag, consistency, and reconciliation before and after failover.",
    "traffic_steering_plan": "Document how DNS, load balancers, or traffic weights move users between regions.",
    "regional_rollback_path": "Provide a tested path to restore the original region or revert regional routing.",
    "runbook_owner": "Name the runbook owner, on-call contact, or decision maker for failover execution.",
    "recovery_metric": "State recovery metrics such as RTO, RPO, availability, latency, or customer-impact targets.",
}


@dataclass(frozen=True, slots=True)
class TaskMultiRegionFailoverReadinessRecommendation:
    """Failover readiness guidance for one multi-region execution task."""

    task_id: str
    title: str
    failover_surfaces: tuple[FailoverSurface, ...] = field(default_factory=tuple)
    required_controls: tuple[FailoverControl, ...] = field(default_factory=tuple)
    present_controls: tuple[FailoverControl, ...] = field(default_factory=tuple)
    missing_controls: tuple[FailoverControl, ...] = field(default_factory=tuple)
    risk_level: FailoverRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_follow_up_actions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "failover_surfaces": list(self.failover_surfaces),
            "required_controls": list(self.required_controls),
            "present_controls": list(self.present_controls),
            "missing_controls": list(self.missing_controls),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_follow_up_actions": list(self.recommended_follow_up_actions),
        }


@dataclass(frozen=True, slots=True)
class TaskMultiRegionFailoverReadinessPlan:
    """Plan-level multi-region failover readiness recommendations."""

    plan_id: str | None = None
    recommendations: tuple[TaskMultiRegionFailoverReadinessRecommendation, ...] = field(default_factory=tuple)
    failover_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ignored_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskMultiRegionFailoverReadinessRecommendation, ...]:
        """Compatibility view matching analyzers that expose rows as records."""
        return self.recommendations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [recommendation.to_dict() for recommendation in self.recommendations],
            "records": [record.to_dict() for record in self.records],
            "failover_task_ids": list(self.failover_task_ids),
            "ignored_task_ids": list(self.ignored_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness recommendations as plain dictionaries."""
        return [recommendation.to_dict() for recommendation in self.recommendations]

    def to_markdown(self) -> str:
        """Render failover readiness recommendations as deterministic Markdown."""
        title = "# Task Multi-Region Failover Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Failover task count: {self.summary.get('failover_task_count', 0)}",
            f"- Missing control count: {self.summary.get('missing_control_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.recommendations:
            lines.extend(["", "No multi-region failover readiness recommendations were inferred."])
            if self.ignored_task_ids:
                lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Recommendations",
                "",
                "| Task | Title | Risk | Failover Surfaces | Present Controls | Missing Controls | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for recommendation in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(recommendation.task_id)}` | "
                f"{_markdown_cell(recommendation.title)} | "
                f"{recommendation.risk_level} | "
                f"{_markdown_cell(', '.join(recommendation.failover_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(recommendation.present_controls) or 'none')} | "
                f"{_markdown_cell(', '.join(recommendation.missing_controls) or 'none')} | "
                f"{_markdown_cell('; '.join(recommendation.evidence) or 'none')} |"
            )
        if self.ignored_task_ids:
            lines.extend(["", f"Ignored tasks: {_markdown_cell(', '.join(self.ignored_task_ids))}"])
        return "\n".join(lines)


def build_task_multi_region_failover_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskMultiRegionFailoverReadinessPlan:
    """Build failover readiness recommendations for multi-region execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_recommendation(task, index) for index, task in enumerate(tasks, start=1)]
    recommendations = tuple(
        sorted(
            (recommendation for recommendation in candidates if recommendation is not None),
            key=lambda recommendation: (
                _RISK_ORDER[recommendation.risk_level],
                recommendation.task_id,
                recommendation.title.casefold(),
            ),
        )
    )
    ignored_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskMultiRegionFailoverReadinessPlan(
        plan_id=plan_id,
        recommendations=recommendations,
        failover_task_ids=tuple(recommendation.task_id for recommendation in recommendations),
        ignored_task_ids=ignored_task_ids,
        summary=_summary(recommendations, task_count=len(tasks), ignored_task_ids=ignored_task_ids),
    )


def analyze_task_multi_region_failover_readiness(source: Any) -> TaskMultiRegionFailoverReadinessPlan:
    """Compatibility alias for building failover readiness recommendations."""
    return build_task_multi_region_failover_readiness_plan(source)


def summarize_task_multi_region_failover_readiness(source: Any) -> TaskMultiRegionFailoverReadinessPlan:
    """Compatibility alias for building failover readiness recommendations."""
    return build_task_multi_region_failover_readiness_plan(source)


def extract_task_multi_region_failover_readiness(source: Any) -> TaskMultiRegionFailoverReadinessPlan:
    """Compatibility alias for building failover readiness recommendations."""
    return build_task_multi_region_failover_readiness_plan(source)


def generate_task_multi_region_failover_readiness(source: Any) -> TaskMultiRegionFailoverReadinessPlan:
    """Compatibility alias for generating failover readiness recommendations."""
    return build_task_multi_region_failover_readiness_plan(source)


def recommend_task_multi_region_failover_readiness(source: Any) -> TaskMultiRegionFailoverReadinessPlan:
    """Compatibility alias for recommending failover readiness controls."""
    return build_task_multi_region_failover_readiness_plan(source)


def task_multi_region_failover_readiness_plan_to_dict(
    result: TaskMultiRegionFailoverReadinessPlan,
) -> dict[str, Any]:
    """Serialize a failover readiness plan to a plain dictionary."""
    return result.to_dict()


task_multi_region_failover_readiness_plan_to_dict.__test__ = False


def task_multi_region_failover_readiness_plan_to_dicts(
    result: TaskMultiRegionFailoverReadinessPlan
    | Iterable[TaskMultiRegionFailoverReadinessRecommendation],
) -> list[dict[str, Any]]:
    """Serialize failover readiness records to plain dictionaries."""
    if isinstance(result, TaskMultiRegionFailoverReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_multi_region_failover_readiness_plan_to_dicts.__test__ = False


def task_multi_region_failover_readiness_plan_to_markdown(
    result: TaskMultiRegionFailoverReadinessPlan,
) -> str:
    """Render a failover readiness plan as Markdown."""
    return result.to_markdown()


task_multi_region_failover_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[FailoverSurface, ...] = field(default_factory=tuple)
    surface_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_controls: tuple[FailoverControl, ...] = field(default_factory=tuple)
    control_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_recommendation(
    task: Mapping[str, Any],
    index: int,
) -> TaskMultiRegionFailoverReadinessRecommendation | None:
    signals = _signals(task)
    if not signals.surfaces:
        return None

    missing_controls = tuple(control for control in _CONTROL_ORDER if control not in signals.present_controls)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskMultiRegionFailoverReadinessRecommendation(
        task_id=task_id,
        title=title,
        failover_surfaces=signals.surfaces,
        required_controls=_CONTROL_ORDER,
        present_controls=signals.present_controls,
        missing_controls=missing_controls,
        risk_level=_risk_level(signals.surfaces, missing_controls),
        evidence=tuple(_dedupe([*signals.surface_evidence, *signals.control_evidence])),
        recommended_follow_up_actions=tuple(_CONTROL_GUIDANCE[control] for control in missing_controls),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surface_hits: set[FailoverSurface] = set()
    control_hits: set[FailoverControl] = set()
    surface_evidence: list[str] = []
    control_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_surfaces = _path_surfaces(normalized)
        if path_surfaces:
            surface_hits.update(path_surfaces)
            surface_evidence.append(f"files_or_modules: {path}")
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for control, pattern in _CONTROL_PATTERNS.items():
            if pattern.search(searchable) or pattern.search(normalized):
                control_hits.add(control)
                control_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_surface = False
        for surface, pattern in _TEXT_SURFACE_PATTERNS.items():
            if pattern.search(text):
                surface_hits.add(surface)
                matched_surface = True
        if matched_surface:
            surface_evidence.append(snippet)
        for control, pattern in _CONTROL_PATTERNS.items():
            if pattern.search(text):
                control_hits.add(control)
                control_evidence.append(snippet)

    return _Signals(
        surfaces=tuple(surface for surface in _SURFACE_ORDER if surface in surface_hits),
        surface_evidence=tuple(_dedupe(surface_evidence)),
        present_controls=tuple(control for control in _CONTROL_ORDER if control in control_hits),
        control_evidence=tuple(_dedupe(control_evidence)),
    )


def _path_surfaces(path: str) -> set[FailoverSurface]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    surfaces: set[FailoverSurface] = set()
    for surface, pattern in _PATH_SURFACE_PATTERNS:
        if pattern.search(normalized) or pattern.search(text):
            surfaces.add(surface)
    name = PurePosixPath(normalized).name
    if name in {"failover.py", "failover.ts", "dr.yml", "dr.yaml"}:
        surfaces.add("failover" if name.startswith("failover") else "disaster_recovery")
    return surfaces


def _risk_level(
    surfaces: tuple[FailoverSurface, ...],
    missing_controls: tuple[FailoverControl, ...],
) -> FailoverRiskLevel:
    if not missing_controls:
        return "medium" if any(surface in _HIGH_RISK_SURFACES for surface in surfaces) else "low"
    if any(surface in _HIGH_RISK_SURFACES for surface in surfaces):
        return "high"
    if any(surface in _ROUTING_SURFACES for surface in surfaces) or len(missing_controls) >= 3:
        return "medium"
    return "low"


def _summary(
    recommendations: tuple[TaskMultiRegionFailoverReadinessRecommendation, ...],
    *,
    task_count: int,
    ignored_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "failover_task_count": len(recommendations),
        "ignored_task_ids": list(ignored_task_ids),
        "missing_control_count": sum(len(recommendation.missing_controls) for recommendation in recommendations),
        "risk_counts": {
            risk: sum(1 for recommendation in recommendations if recommendation.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "surface_counts": {
            surface: sum(1 for recommendation in recommendations if surface in recommendation.failover_surfaces)
            for surface in sorted(
                {surface for recommendation in recommendations for surface in recommendation.failover_surfaces}
            )
        },
        "present_control_counts": {
            control: sum(1 for recommendation in recommendations if control in recommendation.present_controls)
            for control in _CONTROL_ORDER
        },
        "failover_task_ids": [recommendation.task_id for recommendation in recommendations],
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _metadata_key_is_signal(value: str) -> bool:
    return any(pattern.search(value) for pattern in [*_TEXT_SURFACE_PATTERNS.values(), *_CONTROL_PATTERNS.values()])


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


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


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "FailoverControl",
    "FailoverRiskLevel",
    "FailoverSurface",
    "TaskMultiRegionFailoverReadinessPlan",
    "TaskMultiRegionFailoverReadinessRecommendation",
    "analyze_task_multi_region_failover_readiness",
    "build_task_multi_region_failover_readiness_plan",
    "extract_task_multi_region_failover_readiness",
    "generate_task_multi_region_failover_readiness",
    "recommend_task_multi_region_failover_readiness",
    "summarize_task_multi_region_failover_readiness",
    "task_multi_region_failover_readiness_plan_to_dict",
    "task_multi_region_failover_readiness_plan_to_dicts",
    "task_multi_region_failover_readiness_plan_to_markdown",
]
