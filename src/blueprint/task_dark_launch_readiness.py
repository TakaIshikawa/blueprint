"""Plan dark-launch readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DarkLaunchRiskLevel = Literal["low", "medium", "high"]
DarkLaunchSignal = Literal[
    "dark_launch",
    "shadow_mode",
    "silent_release",
    "hidden_rollout",
    "internal_only_launch",
    "beta_cohort",
    "traffic_mirroring",
]
DarkLaunchSafeguard = Literal[
    "audience_isolation",
    "telemetry_comparison",
    "rollback_trigger",
    "support_visibility",
    "data_write_safety",
    "success_metric_review",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[DarkLaunchRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: dict[DarkLaunchSignal, int] = {
    "dark_launch": 0,
    "shadow_mode": 1,
    "silent_release": 2,
    "hidden_rollout": 3,
    "internal_only_launch": 4,
    "beta_cohort": 5,
    "traffic_mirroring": 6,
}
_SAFEGUARD_ORDER: dict[DarkLaunchSafeguard, int] = {
    "audience_isolation": 0,
    "telemetry_comparison": 1,
    "rollback_trigger": 2,
    "support_visibility": 3,
    "data_write_safety": 4,
    "success_metric_review": 5,
}
_SIGNAL_PATTERNS: dict[DarkLaunchSignal, re.Pattern[str]] = {
    "dark_launch": re.compile(r"\b(?:dark[- ]?launch(?:es|ed|ing)?|dark release|dark ship)\b", re.I),
    "shadow_mode": re.compile(r"\b(?:shadow[- ]?mode|shadow run|shadow traffic|shadow execution|shadow compare)\b", re.I),
    "silent_release": re.compile(r"\b(?:silent release|silent rollout|silent launch|quiet release|quiet rollout)\b", re.I),
    "hidden_rollout": re.compile(r"\b(?:hidden rollout|hidden release|hidden launch|not visible to users|invisible rollout)\b", re.I),
    "internal_only_launch": re.compile(r"\b(?:internal[- ]?only|staff[- ]?only|employee[- ]?only|dogfood|internal users?)\b", re.I),
    "beta_cohort": re.compile(r"\b(?:beta cohorts?|beta users?|private beta|early access|preview cohort|allowlist|whitelist)\b", re.I),
    "traffic_mirroring": re.compile(r"\b(?:traffic mirroring|mirror(?:ed|ing)? traffic|request mirroring|tee traffic|dual run|parallel run)\b", re.I),
}
_PATH_PATTERNS: dict[DarkLaunchSignal, re.Pattern[str]] = {
    "dark_launch": re.compile(r"dark[_-]?launch|dark[_-]?release|darkship", re.I),
    "shadow_mode": re.compile(r"shadow[_-]?mode|shadow[_-]?run|shadow", re.I),
    "silent_release": re.compile(r"silent[_-]?(?:release|rollout|launch)|quiet[_-]?(?:release|rollout)", re.I),
    "hidden_rollout": re.compile(r"hidden[_-]?(?:rollout|release|launch)|invisible[_-]?rollout", re.I),
    "internal_only_launch": re.compile(r"internal[_-]?only|staff[_-]?only|dogfood", re.I),
    "beta_cohort": re.compile(r"beta[_-]?cohort|private[_-]?beta|early[_-]?access|allowlist|whitelist", re.I),
    "traffic_mirroring": re.compile(r"traffic[_-]?mirror|request[_-]?mirror|mirror[_-]?traffic|dual[_-]?run|parallel[_-]?run", re.I),
}
_SAFEGUARD_PATTERNS: dict[DarkLaunchSafeguard, re.Pattern[str]] = {
    "audience_isolation": re.compile(
        r"\b(?:audience isolation|cohort isolation|isolated audience|allowlist only|"
        r"internal only audience|exclude production users|segmentation rules?|targeting rules?)\b",
        re.I,
    ),
    "telemetry_comparison": re.compile(
        r"\b(?:telemetry comparison|compare telemetry|baseline comparison|shadow comparison|"
        r"diff metrics?|parity metrics?|control vs treatment|old vs new)\b",
        re.I,
    ),
    "rollback_trigger": re.compile(
        r"\b(?:rollback trigger|rollback threshold|rollback criteria|kill switch|disable trigger|"
        r"automatic rollback|revert trigger|backout trigger)\b",
        re.I,
    ),
    "support_visibility": re.compile(
        r"\b(?:support visibility|support runbook|support notes?|support dashboard|cs visibility|"
        r"customer support|on[- ]?call visibility|incident runbook)\b",
        re.I,
    ),
    "data_write_safety": re.compile(
        r"\b(?:data write safety|write safety|read[- ]?only|no writes?|dry run|discard writes?|"
        r"suppress writes?|idempotent writes?|write guard|side effect guard)\b",
        re.I,
    ),
    "success_metric_review": re.compile(
        r"\b(?:success metric review|success metrics?|review metrics?|launch review|go/no[- ]?go|"
        r"promotion gate|exit criteria|metric sign[- ]?off)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class TaskDarkLaunchReadinessRecommendation:
    """Dark-launch readiness guidance for one affected execution task."""

    task_id: str
    title: str
    dark_launch_signals: tuple[DarkLaunchSignal, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[DarkLaunchSafeguard, ...] = field(default_factory=tuple)
    risk_level: DarkLaunchRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "dark_launch_signals": list(self.dark_launch_signals),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskDarkLaunchReadinessPlan:
    """Plan-level dark-launch readiness summary."""

    plan_id: str | None = None
    recommendations: tuple[TaskDarkLaunchReadinessRecommendation, ...] = field(default_factory=tuple)
    dark_launch_task_ids: tuple[str, ...] = field(default_factory=tuple)
    suppressed_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [record.to_dict() for record in self.recommendations],
            "dark_launch_task_ids": list(self.dark_launch_task_ids),
            "suppressed_task_ids": list(self.suppressed_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return dark-launch recommendations as plain dictionaries."""
        return [record.to_dict() for record in self.recommendations]

    @property
    def records(self) -> tuple[TaskDarkLaunchReadinessRecommendation, ...]:
        """Compatibility view matching planners that name task rows records."""
        return self.recommendations

    def to_markdown(self) -> str:
        """Render the dark-launch readiness plan as deterministic Markdown."""
        title = "# Task Dark Launch Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('dark_launch_task_count', 0)} dark-launch tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(suppressed: {self.summary.get('suppressed_task_count', 0)})."
            ),
        ]
        if not self.recommendations:
            lines.extend(["", "No dark-launch readiness recommendations were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Risk | Signals | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.dark_launch_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_dark_launch_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskDarkLaunchReadinessPlan:
    """Build task-level dark-launch readiness recommendations."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _record_for_task(task, index)) is not None
            ),
            key=lambda record: (
                _RISK_ORDER[record.risk_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    dark_launch_task_ids = tuple(record.task_id for record in records)
    dark_launch_task_id_set = set(dark_launch_task_ids)
    suppressed_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in dark_launch_task_id_set
    )
    return TaskDarkLaunchReadinessPlan(
        plan_id=plan_id,
        recommendations=records,
        dark_launch_task_ids=dark_launch_task_ids,
        suppressed_task_ids=suppressed_task_ids,
        summary=_summary(records, total_task_count=len(tasks), suppressed_task_count=len(suppressed_task_ids)),
    )


def recommend_task_dark_launch_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[TaskDarkLaunchReadinessRecommendation, ...]:
    """Return dark-launch readiness recommendations for relevant execution tasks."""
    return build_task_dark_launch_readiness_plan(source).recommendations


def generate_task_dark_launch_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[TaskDarkLaunchReadinessRecommendation, ...]:
    """Compatibility alias for returning dark-launch recommendations."""
    return recommend_task_dark_launch_readiness(source)


def summarize_task_dark_launch_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskDarkLaunchReadinessPlan:
    """Compatibility alias for building dark-launch readiness plans."""
    return build_task_dark_launch_readiness_plan(source)


def task_dark_launch_readiness_plan_to_dict(
    result: TaskDarkLaunchReadinessPlan,
) -> dict[str, Any]:
    """Serialize a dark-launch readiness plan to a plain dictionary."""
    return result.to_dict()


task_dark_launch_readiness_plan_to_dict.__test__ = False


def task_dark_launch_readiness_to_dicts(
    records: (
        tuple[TaskDarkLaunchReadinessRecommendation, ...]
        | list[TaskDarkLaunchReadinessRecommendation]
        | TaskDarkLaunchReadinessPlan
    ),
) -> list[dict[str, Any]]:
    """Serialize dark-launch readiness recommendations to dictionaries."""
    if isinstance(records, TaskDarkLaunchReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


task_dark_launch_readiness_to_dicts.__test__ = False


def task_dark_launch_readiness_plan_to_markdown(
    result: TaskDarkLaunchReadinessPlan,
) -> str:
    """Render a dark-launch readiness plan as Markdown."""
    return result.to_markdown()


task_dark_launch_readiness_plan_to_markdown.__test__ = False


def _record_for_task(
    task: Mapping[str, Any],
    index: int,
) -> TaskDarkLaunchReadinessRecommendation | None:
    signals: dict[DarkLaunchSignal, list[str]] = {}
    safeguards: set[DarkLaunchSafeguard] = set()
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _inspect_path(path, signals)
    for source_field, text in _candidate_texts(task):
        _inspect_text(source_field, text, signals, safeguards)

    if not signals:
        return None

    dark_launch_signals = tuple(signal for signal in _SIGNAL_ORDER if signal in signals)
    missing_safeguards = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in safeguards)
    task_id = _task_id(task, index)
    return TaskDarkLaunchReadinessRecommendation(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        dark_launch_signals=dark_launch_signals,
        missing_safeguards=missing_safeguards,
        risk_level=_risk_level(dark_launch_signals, missing_safeguards),
        evidence=tuple(
            _dedupe(
                evidence
                for signal in dark_launch_signals
                for evidence in signals.get(signal, [])
            )
        ),
    )


def _inspect_path(
    path: str,
    signals: dict[DarkLaunchSignal, list[str]],
) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    evidence = f"files_or_modules: {path}"
    searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    for signal, pattern in _PATH_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(searchable):
            signals.setdefault(signal, []).append(evidence)


def _inspect_text(
    source_field: str,
    text: str,
    signals: dict[DarkLaunchSignal, list[str]],
    safeguards: set[DarkLaunchSafeguard],
) -> None:
    evidence = _evidence_snippet(source_field, text)
    for signal, pattern in _SIGNAL_PATTERNS.items():
        if pattern.search(text):
            signals.setdefault(signal, []).append(evidence)
    for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
        if pattern.search(text):
            safeguards.add(safeguard)


def _risk_level(
    dark_launch_signals: tuple[DarkLaunchSignal, ...],
    missing_safeguards: tuple[DarkLaunchSafeguard, ...],
) -> DarkLaunchRiskLevel:
    if not missing_safeguards:
        return "low"
    if (
        "traffic_mirroring" in dark_launch_signals
        or any(
            safeguard in missing_safeguards
            for safeguard in ("rollback_trigger", "data_write_safety")
        )
    ):
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskDarkLaunchReadinessRecommendation, ...],
    *,
    total_task_count: int,
    suppressed_task_count: int,
) -> dict[str, Any]:
    return {
        "task_count": total_task_count,
        "dark_launch_task_count": len(records),
        "suppressed_task_count": suppressed_task_count,
        "risk_counts": {
            level: sum(1 for record in records if record.risk_level == level)
            for level in ("high", "medium", "low")
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.dark_launch_signals)
            for signal in _SIGNAL_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
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

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
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
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "dependencies",
        "depends_on",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        patterns = (*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in patterns):
                texts.append((field, key_text))
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
    "DarkLaunchRiskLevel",
    "DarkLaunchSafeguard",
    "DarkLaunchSignal",
    "TaskDarkLaunchReadinessPlan",
    "TaskDarkLaunchReadinessRecommendation",
    "build_task_dark_launch_readiness_plan",
    "generate_task_dark_launch_readiness",
    "recommend_task_dark_launch_readiness",
    "summarize_task_dark_launch_readiness",
    "task_dark_launch_readiness_plan_to_dict",
    "task_dark_launch_readiness_plan_to_markdown",
    "task_dark_launch_readiness_to_dicts",
]
