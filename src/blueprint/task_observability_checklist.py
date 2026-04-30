"""Build task-level observability checklists for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ObservabilityArea = Literal[
    "logs",
    "metrics",
    "traces",
    "audit_events",
    "dashboards",
    "alerts",
    "post_release_inspection",
    "validation",
]
ObservabilitySeverity = Literal["none", "low", "medium", "high"]
RiskLevel = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_BACKEND_RE = re.compile(
    r"\b(?:api|endpoint|route|controller|handler|service|backend|server|request|"
    r"response|webhook|graphql|grpc|rest)\b",
    re.IGNORECASE,
)
_JOB_RE = re.compile(
    r"\b(?:async|background|job|worker|queue|scheduled|cron|task runner|retry|"
    r"dead[- ]?letter|celery|sidekiq|rq)\b",
    re.IGNORECASE,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrations|schema|database|db|sql|alembic|backfill|seed|"
    r"index|reindex|existing rows|data migration)\b",
    re.IGNORECASE,
)
_FRONTEND_RE = re.compile(
    r"\b(?:frontend|ui|ux|component|form|flow|"
    r"browser|client|checkout|onboarding)\b",
    re.IGNORECASE,
)
_AUTH_RE = re.compile(
    r"\b(?:auth|authentication|authorization|permission|permissions|rbac|oauth|"
    r"login|logout|session|token|password|secret|security|audit)\b",
    re.IGNORECASE,
)
_INTEGRATION_RE = re.compile(
    r"\b(?:integration|third[- ]?party|external|vendor|webhook|callback|sync|"
    r"import|export|stripe|slack|github|salesforce|api client)\b",
    re.IGNORECASE,
)
_PAYMENT_RE = re.compile(r"\b(?:billing|payment|invoice|subscription)\b", re.I)

_FILE_SIGNAL_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("backend", re.compile(r"(?:^|/)(?:api|server|routes?|controllers?|handlers?|services?)(?:/|$)", re.I)),
    ("job", re.compile(r"(?:^|/)(?:jobs?|workers?|queues?|tasks?|cron|schedulers?)(?:/|$)", re.I)),
    ("migration", re.compile(r"(?:^|/)(?:migrations?|alembic|versions|db|database)(?:/|$)|\.(?:sql|ddl)$", re.I)),
    ("frontend", re.compile(r"(?:^|/)(?:components?|pages?|views?|screens?|frontend|client|ui)(?:/|$)|\.(?:jsx|tsx|vue|svelte)$", re.I)),
    ("auth", re.compile(r"(?:^|/)(?:auth|authentication|authorization|security|permissions?)(?:/|$)|(?:token|session|password|permission)\w*\.", re.I)),
    ("integration", re.compile(r"(?:^|/)(?:integrations?|webhooks?|clients?|adapters?|providers?)(?:/|$)", re.I)),
)

_AREA_ORDER: dict[ObservabilityArea, int] = {
    "logs": 0,
    "metrics": 1,
    "traces": 2,
    "audit_events": 3,
    "dashboards": 4,
    "alerts": 5,
    "post_release_inspection": 6,
    "validation": 7,
}
_SEVERITY_RANK: dict[ObservabilitySeverity, int] = {
    "none": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}


@dataclass(frozen=True, slots=True)
class TaskObservabilityRecommendation:
    """One observability recommendation for an execution task."""

    area: ObservabilityArea
    severity: ObservabilitySeverity
    reason: str
    checklist_items: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "area": self.area,
            "severity": self.severity,
            "reason": self.reason,
            "checklist_items": list(self.checklist_items),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskObservabilityChecklist:
    """Observability checklist for one execution task."""

    task_id: str
    title: str
    risk_level: RiskLevel
    severity: ObservabilitySeverity
    reason: str
    checklist_items: tuple[str, ...] = field(default_factory=tuple)
    recommendations: tuple[TaskObservabilityRecommendation, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_level": self.risk_level,
            "severity": self.severity,
            "reason": self.reason,
            "checklist_items": list(self.checklist_items),
            "recommendations": [
                recommendation.to_dict() for recommendation in self.recommendations
            ],
        }

    def to_markdown(self) -> str:
        """Render this task checklist as deterministic Markdown."""
        lines = [
            f"### {self.task_id}: {self.title}",
            f"- Severity: {self.severity}",
            f"- Reason: {self.reason}",
        ]
        for item in self.checklist_items:
            lines.append(f"- [ ] {item}")
        for recommendation in self.recommendations:
            lines.append(f"- {recommendation.area}: {recommendation.severity}")
            lines.append(f"  - Reason: {recommendation.reason}")
            for item in recommendation.checklist_items:
                lines.append(f"  - [ ] {item}")
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class TaskObservabilityChecklistResult:
    """Complete observability checklist result for a plan or task collection."""

    plan_id: str | None = None
    checklists: tuple[TaskObservabilityChecklist, ...] = field(default_factory=tuple)
    severity_counts: dict[ObservabilitySeverity, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "checklists": [checklist.to_dict() for checklist in self.checklists],
            "severity_counts": dict(self.severity_counts),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return checklist records as plain dictionaries."""
        return [checklist.to_dict() for checklist in self.checklists]

    def to_markdown(self) -> str:
        """Render all task checklists as deterministic Markdown."""
        title = f"# Task Observability Checklist"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        sections = [title]
        sections.extend(checklist.to_markdown() for checklist in self.checklists)
        return "\n\n".join(sections)


def build_task_observability_checklist(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskObservabilityChecklistResult:
    """Recommend task-level observability work from execution task signals."""
    plan_id, tasks = _source_payload(source)
    checklists = tuple(_task_checklist(task, index) for index, task in enumerate(tasks, start=1))
    return TaskObservabilityChecklistResult(
        plan_id=plan_id,
        checklists=checklists,
        severity_counts=_severity_counts(checklist.severity for checklist in checklists),
    )


def task_observability_checklist_to_dict(
    result: TaskObservabilityChecklistResult,
) -> dict[str, Any]:
    """Serialize an observability checklist result to a plain dictionary."""
    return result.to_dict()


task_observability_checklist_to_dict.__test__ = False


def _task_checklist(task: Mapping[str, Any], index: int) -> TaskObservabilityChecklist:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    risk_level = _risk_level(task.get("risk_level") or task.get("risk"))
    signals = _signals(task)
    recommendations = tuple(sorted(_recommendations(signals, risk_level).values(), key=_sort_key))
    severity = _overall_severity(recommendations, risk_level)
    if recommendations:
        reason = _overall_reason(signals, risk_level, severity)
        checklist_items = tuple(
            _dedupe(item for recommendation in recommendations for item in recommendation.checklist_items)
        )
    else:
        reason = "No runtime observability signal detected; focused validation is sufficient."
        checklist_items = (
            "Run the focused validation for this task; no extra observability is required unless runtime behavior changes.",
        )
        recommendations = (
            TaskObservabilityRecommendation(
                area="validation",
                severity="none",
                reason=reason,
                checklist_items=checklist_items,
            ),
        )
        severity = "none"

    return TaskObservabilityChecklist(
        task_id=task_id,
        title=title,
        risk_level=risk_level,
        severity=severity,
        reason=reason,
        checklist_items=checklist_items,
        recommendations=recommendations,
    )


def _recommendations(
    signals: Mapping[str, tuple[str, ...]],
    risk_level: RiskLevel,
) -> dict[ObservabilityArea, TaskObservabilityRecommendation]:
    categories = set(signals)
    recommendations: dict[ObservabilityArea, TaskObservabilityRecommendation] = {}

    if categories & {"backend", "integration", "auth", "job", "migration"}:
        _put(
            recommendations,
            "logs",
            _elevate("medium", risk_level, categories),
            _reason("logs", categories),
            _items("logs", categories),
            _evidence(signals, categories),
        )
    if categories & {"backend", "integration", "job", "migration", "frontend"}:
        _put(
            recommendations,
            "metrics",
            _elevate("medium" if categories & {"integration", "job", "migration"} else "low", risk_level, categories),
            _reason("metrics", categories),
            _items("metrics", categories),
            _evidence(signals, categories),
        )
    if categories & {"backend", "integration"}:
        _put(
            recommendations,
            "traces",
            _elevate("medium", risk_level, categories),
            _reason("traces", categories),
            _items("traces", categories),
            _evidence(signals, categories),
        )
    if categories & {"auth", "migration"}:
        _put(
            recommendations,
            "audit_events",
            _elevate("high" if "auth" in categories else "medium", risk_level, categories),
            _reason("audit_events", categories),
            _items("audit_events", categories),
            _evidence(signals, categories),
        )
    if categories & {"integration", "job", "migration", "auth"}:
        _put(
            recommendations,
            "dashboards",
            _elevate("medium", risk_level, categories),
            _reason("dashboards", categories),
            _items("dashboards", categories),
            _evidence(signals, categories),
        )
    if categories & {"integration", "job", "migration", "auth"} or risk_level == "high":
        _put(
            recommendations,
            "alerts",
            _elevate("high" if categories & {"auth"} else "medium", risk_level, categories),
            _reason("alerts", categories),
            _items("alerts", categories),
            _evidence(signals, categories),
        )
    if categories:
        _put(
            recommendations,
            "post_release_inspection",
            _elevate("medium" if categories & {"auth", "integration", "job", "migration"} else "low", risk_level, categories),
            _reason("post_release_inspection", categories),
            _items("post_release_inspection", categories),
            _evidence(signals, categories),
        )

    return recommendations


def _signals(task: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    signals: dict[str, list[str]] = {}
    for path in _strings(task.get("files_or_modules")):
        normalized = _normalized_path(path)
        for category, pattern in _FILE_SIGNAL_PATTERNS:
            if pattern.search(normalized):
                _append(signals, category, f"files_or_modules: {path}")

    for source_field, text in _task_texts(task):
        _add_text_signals(signals, source_field, text)

    for source_field, text in _metadata_texts(task.get("metadata")):
        field = source_field.casefold()
        if "observability" in field or "telemetry" in field:
            _append(signals, "metadata", f"{source_field}: {text}")
        _add_text_signals(signals, source_field, text)

    return {category: tuple(_dedupe(evidence)) for category, evidence in signals.items()}


def _add_text_signals(signals: dict[str, list[str]], source_field: str, text: str) -> None:
    evidence = f"{source_field}: {text}"
    if _AUTH_RE.search(text):
        _append(signals, "auth", evidence)
    if _INTEGRATION_RE.search(text) or _PAYMENT_RE.search(text):
        _append(signals, "integration", evidence)
    if _JOB_RE.search(text):
        _append(signals, "job", evidence)
    if _MIGRATION_RE.search(text):
        _append(signals, "migration", evidence)
    if _BACKEND_RE.search(text):
        _append(signals, "backend", evidence)
    if _FRONTEND_RE.search(text):
        _append(signals, "frontend", evidence)


def _put(
    recommendations: dict[ObservabilityArea, TaskObservabilityRecommendation],
    area: ObservabilityArea,
    severity: ObservabilitySeverity,
    reason: str,
    checklist_items: Iterable[str],
    evidence: Iterable[str],
) -> None:
    recommendations[area] = TaskObservabilityRecommendation(
        area=area,
        severity=severity,
        reason=reason,
        checklist_items=tuple(_dedupe(checklist_items)),
        evidence=tuple(_dedupe(evidence)),
    )


def _reason(area: ObservabilityArea, categories: set[str]) -> str:
    subject = _subject(categories)
    return {
        "logs": f"{subject} work needs structured logs for runtime diagnosis.",
        "metrics": f"{subject} work needs counters or latency metrics to detect regressions.",
        "traces": f"{subject} work benefits from request or dependency trace correlation.",
        "audit_events": f"{subject} work changes sensitive or durable state that should be auditable.",
        "dashboards": f"{subject} work should be visible in an operational dashboard after release.",
        "alerts": f"{subject} work can fail in production and should have actionable alert coverage.",
        "post_release_inspection": f"{subject} work should have an explicit post-release inspection path.",
        "validation": "Focused validation is sufficient for this low-signal task.",
    }[area]


def _items(area: ObservabilityArea, categories: set[str]) -> tuple[str, ...]:
    items = {
        "logs": ["Add structured success and failure logs with task-relevant identifiers."],
        "metrics": ["Track success, failure, and latency or duration for the changed path."],
        "traces": ["Propagate trace context across the changed API or external dependency boundary."],
        "audit_events": ["Record actor, target, action, outcome, and timestamp for sensitive state changes."],
        "dashboards": ["Confirm the changed path appears on an existing dashboard or add a focused panel."],
        "alerts": ["Define an alert for sustained failures, error-rate spikes, or stalled work."],
        "post_release_inspection": ["Document the query, dashboard, or log filter to inspect after release."],
        "validation": ["Run focused validation; no extra observability is required."],
    }[area]
    if "job" in categories and area in {"logs", "metrics", "alerts", "post_release_inspection"}:
        items.append("Include job id, retry count, queue name, and dead-letter or failure visibility.")
    if "integration" in categories and area in {"logs", "metrics", "traces", "alerts"}:
        items.append("Capture external provider, operation, status code, timeout, and retry outcome.")
    if "migration" in categories and area in {"logs", "metrics", "dashboards", "post_release_inspection"}:
        items.append("Track affected row counts, batch progress, duration, and rollback inspection points.")
    if "auth" in categories and area in {"logs", "audit_events", "alerts"}:
        items.append("Avoid secrets in telemetry while preserving actor and permission decision context.")
    if "frontend" in categories and area in {"metrics", "post_release_inspection"}:
        items.append("Inspect client-side errors or funnel completion for the changed user flow.")
    return tuple(items)


def _subject(categories: set[str]) -> str:
    labels = {
        "auth": "Auth/security",
        "integration": "Integration",
        "job": "Async job",
        "migration": "Migration/data",
        "backend": "Backend/API",
        "frontend": "Frontend flow",
        "metadata": "Metadata-indicated observability",
    }
    ordered = [labels[category] for category in labels if category in categories]
    return " and ".join(ordered) if ordered else "Runtime"


def _evidence(signals: Mapping[str, tuple[str, ...]], categories: set[str]) -> list[str]:
    evidence: list[str] = []
    for category in ("auth", "integration", "job", "migration", "backend", "frontend", "metadata"):
        if category in categories:
            evidence.extend(signals.get(category, ()))
    return _dedupe(evidence)


def _elevate(
    baseline: ObservabilitySeverity,
    risk_level: RiskLevel,
    categories: set[str],
) -> ObservabilitySeverity:
    severity = baseline
    if risk_level == "high" and severity != "high":
        severity = "medium" if severity == "low" else "high"
    if categories & {"auth", "integration", "job", "migration"} and _SEVERITY_RANK[severity] < 2:
        severity = "medium"
    return severity


def _overall_severity(
    recommendations: Iterable[TaskObservabilityRecommendation],
    risk_level: RiskLevel,
) -> ObservabilitySeverity:
    severity: ObservabilitySeverity = "none"
    for recommendation in recommendations:
        if _SEVERITY_RANK[recommendation.severity] > _SEVERITY_RANK[severity]:
            severity = recommendation.severity
    if risk_level == "high" and severity == "none":
        return "medium"
    return severity


def _overall_reason(
    signals: Mapping[str, tuple[str, ...]],
    risk_level: RiskLevel,
    severity: ObservabilitySeverity,
) -> str:
    categories = set(signals)
    if risk_level == "high":
        return f"High-risk {_subject(categories).lower()} signals require {severity} observability coverage."
    return f"{_subject(categories)} signals require {severity} observability coverage."


def _sort_key(recommendation: TaskObservabilityRecommendation) -> tuple[int, int, str]:
    return (
        _AREA_ORDER[recommendation.area],
        -_SEVERITY_RANK[recommendation.severity],
        recommendation.area,
    )


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
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

    tasks: list[dict[str, Any]] = []
    for item in source:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


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


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("title", "description", "milestone", "owner_type", "suggested_engine", "test_command"):
        text = _optional_text(task.get(field_name))
        if text:
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif (text := _optional_text(child)):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif (text := _optional_text(item)):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _risk_level(value: Any) -> RiskLevel:
    text = (_optional_text(value) or "").casefold()
    if text in {"critical", "blocker", "high"}:
        return "high"
    if text in {"medium", "moderate"}:
        return "medium"
    return "low"


def _severity_counts(values: Iterable[ObservabilitySeverity]) -> dict[ObservabilitySeverity, int]:
    counts: dict[ObservabilitySeverity, int] = {
        "none": 0,
        "low": 0,
        "medium": 0,
        "high": 0,
    }
    for value in values:
        counts[value] += 1
    return counts


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


def _append(signals: dict[str, list[str]], category: str, evidence: str) -> None:
    signals.setdefault(category, []).append(evidence)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "ObservabilityArea",
    "ObservabilitySeverity",
    "TaskObservabilityChecklist",
    "TaskObservabilityChecklistResult",
    "TaskObservabilityRecommendation",
    "build_task_observability_checklist",
    "task_observability_checklist_to_dict",
]
