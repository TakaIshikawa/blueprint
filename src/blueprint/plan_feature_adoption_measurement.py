"""Build plan-level feature adoption measurement matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FeatureAdoptionMeasurementObjective = Literal[
    "adoption",
    "activation",
    "engagement",
    "retention",
    "funnel",
    "analytics",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_OBJECTIVE_ORDER: dict[FeatureAdoptionMeasurementObjective, int] = {
    "adoption": 0,
    "activation": 1,
    "engagement": 2,
    "retention": 3,
    "funnel": 4,
    "analytics": 5,
}
_TEXT_OBJECTIVE_PATTERNS: tuple[tuple[FeatureAdoptionMeasurementObjective, re.Pattern[str]], ...] = (
    (
        "adoption",
        re.compile(r"\b(?:adoption|adopted|feature usage|usage rate|rollout uptake|new users? using)\b", re.I),
    ),
    (
        "activation",
        re.compile(r"\b(?:activation|activated|aha moment|first value|onboarding completion|setup complete)\b", re.I),
    ),
    (
        "engagement",
        re.compile(r"\b(?:engagement|engaged|active usage|usage frequency|dau|wau|mau|session|clicks?)\b", re.I),
    ),
    (
        "retention",
        re.compile(r"\b(?:retention|retained|cohort|churn|repeat usage|returning users?|day[- ]?[0-9]+)\b", re.I),
    ),
    (
        "funnel",
        re.compile(r"\b(?:funnel|conversion|drop[- ]?off|step conversion|checkout funnel|onboarding funnel)\b", re.I),
    ),
    (
        "analytics",
        re.compile(
            r"\b(?:analytics|telemetry|instrumentation|event tracking|track event|product event|"
            r"dashboard|reporting|metric|metrics|kpi|success metric|segment|amplitude|mixpanel|ga4|posthog)\b",
            re.I,
        ),
    ),
)
_PATH_OBJECTIVE_PATTERNS: tuple[tuple[FeatureAdoptionMeasurementObjective, re.Pattern[str]], ...] = (
    ("adoption", re.compile(r"(?:^|/)(?:adoption|usage|rollout)(?:/|$)|adoption|usage", re.I)),
    ("activation", re.compile(r"(?:^|/)(?:activation|onboarding)(?:/|$)|activation|onboarding", re.I)),
    ("engagement", re.compile(r"(?:^|/)(?:engagement|sessions?|activity)(?:/|$)|engagement", re.I)),
    ("retention", re.compile(r"(?:^|/)(?:retention|cohorts?|churn)(?:/|$)|retention|cohort", re.I)),
    ("funnel", re.compile(r"(?:^|/)(?:funnels?|conversion)(?:/|$)|funnel|conversion", re.I)),
    ("analytics", re.compile(r"(?:^|/)(?:analytics|telemetry|events?|dashboards?|metrics)(?:/|$)", re.I)),
)
_METRIC_KEYS = ("metric", "metrics", "success_metric", "success_metrics", "kpi", "kpis")
_EVENT_KEYS = (
    "event",
    "events",
    "event_name",
    "event_names",
    "analytics_event",
    "analytics_events",
    "required_event",
    "required_events",
)
_DIMENSION_KEYS = ("dimension", "dimensions", "cohort", "cohorts", "segment", "segments", "filters")
_DASHBOARD_KEYS = ("dashboard", "dashboards", "report", "reports", "reporting_surface", "reporting_surfaces")
_OWNER_KEYS = ("owner", "owners", "owner_hint", "owner_hints", "dri", "assignee", "assignees")
_BASELINE_KEYS = ("baseline", "baseline_requirement", "baseline_requirements", "current_baseline")
_RISK_KEYS = ("risk", "risks", "measurement_risk", "measurement_risks")
_OBJECTIVE_KEYS = ("measurement_objective", "measurement_objectives", "objective", "objectives")
_EVENT_RE = re.compile(r"\b(?:event name|event names|track(?:ed)? event|analytics event|product event)\b", re.I)
_BASELINE_RE = re.compile(r"\b(?:baseline|current rate|before launch|pre[- ]launch|existing usage)\b", re.I)
_DASHBOARD_RE = re.compile(r"\b(?:dashboard|report|reporting surface|looker|mode|metabase|amplitude|mixpanel)\b", re.I)


@dataclass(frozen=True, slots=True)
class PlanFeatureAdoptionMeasurementRow:
    """Measurement guidance for one feature adoption objective."""

    measurement_objective: FeatureAdoptionMeasurementObjective
    covered_task_ids: tuple[str, ...] = field(default_factory=tuple)
    success_metrics: tuple[str, ...] = field(default_factory=tuple)
    required_events: tuple[str, ...] = field(default_factory=tuple)
    dimensions: tuple[str, ...] = field(default_factory=tuple)
    baseline_requirement: str = ""
    reporting_surface: str = ""
    owner_hint: str = ""
    risks: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "measurement_objective": self.measurement_objective,
            "covered_task_ids": list(self.covered_task_ids),
            "success_metrics": list(self.success_metrics),
            "required_events": list(self.required_events),
            "dimensions": list(self.dimensions),
            "baseline_requirement": self.baseline_requirement,
            "reporting_surface": self.reporting_surface,
            "owner_hint": self.owner_hint,
            "risks": list(self.risks),
            "follow_up_questions": list(self.follow_up_questions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanFeatureAdoptionMeasurementMatrix:
    """Plan-level feature adoption measurement matrix and rollup counts."""

    plan_id: str | None = None
    rows: tuple[PlanFeatureAdoptionMeasurementRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return matrix rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the feature adoption measurement matrix as deterministic Markdown."""
        title = "# Plan Feature Adoption Measurement Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        objective_counts = self.summary.get("objective_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Measurement objective count: {self.summary.get('measurement_objective_count', 0)}",
            f"- Covered task count: {self.summary.get('covered_task_count', 0)}",
            f"- Follow-up question count: {self.summary.get('follow_up_question_count', 0)}",
            "- Objective counts: "
            + ", ".join(f"{objective} {objective_counts.get(objective, 0)}" for objective in _OBJECTIVE_ORDER),
        ]
        if not self.rows:
            lines.extend(["", "No feature adoption measurement signals were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Objective | Tasks | Metrics | Events | Dimensions | Baseline | Reporting Surface | "
                    "Owner | Risks | Follow-up Questions | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.measurement_objective} | "
                f"{_markdown_cell(', '.join(row.covered_task_ids) or 'plan metadata')} | "
                f"{_markdown_cell('; '.join(row.success_metrics) or 'none')} | "
                f"{_markdown_cell('; '.join(row.required_events) or 'none')} | "
                f"{_markdown_cell('; '.join(row.dimensions) or 'none')} | "
                f"{_markdown_cell(row.baseline_requirement or 'none')} | "
                f"{_markdown_cell(row.reporting_surface or 'none')} | "
                f"{_markdown_cell(row.owner_hint or 'none')} | "
                f"{_markdown_cell('; '.join(row.risks) or 'none')} | "
                f"{_markdown_cell('; '.join(row.follow_up_questions) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_feature_adoption_measurement_matrix(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanFeatureAdoptionMeasurementMatrix:
    """Derive feature adoption measurement guidance from an execution plan."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    grouped: dict[FeatureAdoptionMeasurementObjective, dict[str, Any]] = {
        objective: {
            "task_ids": [],
            "metrics": [],
            "events": [],
            "dimensions": [],
            "baselines": [],
            "dashboards": [],
            "owners": [],
            "risks": [],
            "evidence": [],
            "texts": [],
        }
        for objective in _OBJECTIVE_ORDER
    }

    for source_field, text in _plan_candidate_texts(plan):
        for objective, pattern in _TEXT_OBJECTIVE_PATTERNS:
            if pattern.search(text):
                grouped[objective]["evidence"].append(_evidence_snippet(source_field, text))
                grouped[objective]["texts"].append(text)
        metadata = plan.get("metadata")
        if isinstance(metadata, Mapping):
            for objective in _metadata_objectives(metadata):
                grouped[objective]["evidence"].append(_evidence_snippet(source_field, text))
                grouped[objective]["texts"].append(text)

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        signals = _task_signals(task)
        for objective in signals.objectives:
            values = grouped[objective]
            values["task_ids"].append(task_id)
            values["metrics"].extend(signals.metrics)
            values["events"].extend(signals.events)
            values["dimensions"].extend(signals.dimensions)
            values["baselines"].extend(signals.baselines)
            values["dashboards"].extend(signals.dashboards)
            values["owners"].extend(signals.owners)
            values["risks"].extend(signals.risks)
            values["evidence"].extend(signals.evidence)
            values["texts"].extend(signals.texts)

    rows = tuple(
        _row(objective, grouped[objective])
        for objective in _OBJECTIVE_ORDER
        if grouped[objective]["task_ids"] or grouped[objective]["evidence"]
    )
    covered_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.covered_task_ids))
    objective_counts = {
        objective: sum(1 for row in rows if row.measurement_objective == objective)
        for objective in _OBJECTIVE_ORDER
    }
    return PlanFeatureAdoptionMeasurementMatrix(
        plan_id=_optional_text(plan.get("id")),
        rows=rows,
        summary={
            "task_count": len(tasks),
            "measurement_objective_count": len(rows),
            "covered_task_count": len(covered_task_ids),
            "follow_up_question_count": sum(len(row.follow_up_questions) for row in rows),
            "risk_count": sum(len(row.risks) for row in rows),
            "objective_counts": objective_counts,
        },
    )


def summarize_plan_feature_adoption_measurement(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanFeatureAdoptionMeasurementMatrix:
    """Compatibility alias for building feature adoption measurement matrices."""
    return build_plan_feature_adoption_measurement_matrix(source)


def plan_feature_adoption_measurement_matrix_to_dict(
    matrix: PlanFeatureAdoptionMeasurementMatrix,
) -> dict[str, Any]:
    """Serialize a feature adoption measurement matrix to a plain dictionary."""
    return matrix.to_dict()


plan_feature_adoption_measurement_matrix_to_dict.__test__ = False


def plan_feature_adoption_measurement_matrix_to_markdown(
    matrix: PlanFeatureAdoptionMeasurementMatrix,
) -> str:
    """Render a feature adoption measurement matrix as Markdown."""
    return matrix.to_markdown()


plan_feature_adoption_measurement_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskSignals:
    objectives: tuple[FeatureAdoptionMeasurementObjective, ...] = field(default_factory=tuple)
    metrics: tuple[str, ...] = field(default_factory=tuple)
    events: tuple[str, ...] = field(default_factory=tuple)
    dimensions: tuple[str, ...] = field(default_factory=tuple)
    baselines: tuple[str, ...] = field(default_factory=tuple)
    dashboards: tuple[str, ...] = field(default_factory=tuple)
    owners: tuple[str, ...] = field(default_factory=tuple)
    risks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    texts: tuple[str, ...] = field(default_factory=tuple)


def _task_signals(task: Mapping[str, Any]) -> _TaskSignals:
    objectives: set[FeatureAdoptionMeasurementObjective] = set()
    evidence: list[str] = []
    texts: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        path_text = _path_text(normalized)
        matched = False
        for objective, pattern in _PATH_OBJECTIVE_PATTERNS:
            if pattern.search(normalized) or pattern.search(path_text):
                objectives.add(objective)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")
            texts.append(path)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        objectives.update(_metadata_objectives(metadata))

    for source_field, text in _candidate_texts(task):
        matched = False
        for objective, pattern in _TEXT_OBJECTIVE_PATTERNS:
            if pattern.search(text):
                objectives.add(objective)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))
            texts.append(text)

    if metadata and isinstance(metadata, Mapping):
        metadata_values = _metadata_values(metadata)
        if any(metadata_values[key] for key in ("metrics", "events", "dimensions", "dashboards")):
            objectives.add("analytics")
    else:
        metadata_values = {key: [] for key in ("metrics", "events", "dimensions", "baselines", "dashboards", "owners", "risks")}

    return _TaskSignals(
        objectives=tuple(objective for objective in _OBJECTIVE_ORDER if objective in objectives),
        metrics=tuple(_dedupe(metadata_values["metrics"])),
        events=tuple(_dedupe(metadata_values["events"])),
        dimensions=tuple(_dedupe(metadata_values["dimensions"])),
        baselines=tuple(_dedupe(metadata_values["baselines"])),
        dashboards=tuple(_dedupe(metadata_values["dashboards"])),
        owners=tuple(_dedupe([*metadata_values["owners"], *_owner_hints(task)])),
        risks=tuple(_dedupe(metadata_values["risks"])),
        evidence=tuple(_dedupe(evidence)),
        texts=tuple(_dedupe(texts)),
    )


def _row(
    objective: FeatureAdoptionMeasurementObjective,
    values: Mapping[str, list[str]],
) -> PlanFeatureAdoptionMeasurementRow:
    texts = tuple(values.get("texts", ()))
    explicit_events = tuple(_dedupe(values.get("events", ())))
    explicit_baselines = tuple(_dedupe(values.get("baselines", ())))
    explicit_dashboards = tuple(_dedupe(values.get("dashboards", ())))
    follow_ups = _follow_up_questions(objective, texts, explicit_events, explicit_baselines, explicit_dashboards)
    return PlanFeatureAdoptionMeasurementRow(
        measurement_objective=objective,
        covered_task_ids=tuple(sorted(_dedupe(values.get("task_ids", ())))),
        success_metrics=tuple(_dedupe(values.get("metrics", ())) or _default_metrics(objective)),
        required_events=explicit_events or _default_events(objective),
        dimensions=tuple(_dedupe(values.get("dimensions", ())) or _default_dimensions(objective)),
        baseline_requirement=("; ".join(explicit_baselines) if explicit_baselines else _default_baseline(objective)),
        reporting_surface=("; ".join(explicit_dashboards) if explicit_dashboards else _default_reporting_surface(objective)),
        owner_hint=", ".join(_dedupe(values.get("owners", ())) or _default_owner_hints(objective)),
        risks=tuple(_dedupe([*values.get("risks", ()), *_default_risks(objective, follow_ups)])),
        follow_up_questions=tuple(follow_ups),
        evidence=tuple(_dedupe(values.get("evidence", ()))),
    )


def _follow_up_questions(
    objective: FeatureAdoptionMeasurementObjective,
    texts: tuple[str, ...],
    events: tuple[str, ...],
    baselines: tuple[str, ...],
    dashboards: tuple[str, ...],
) -> list[str]:
    context = " ".join(texts)
    questions: list[str] = []
    if not events and not _EVENT_RE.search(context):
        questions.append(f"Which canonical event names prove {objective} measurement?")
    if not baselines and not _BASELINE_RE.search(context):
        questions.append(f"What pre-launch baseline or comparison window applies to {objective}?")
    if not dashboards and not _DASHBOARD_RE.search(context):
        questions.append(f"Which dashboard or reporting surface will track {objective}?")
    return questions


def _metadata_values(metadata: Mapping[str, Any]) -> dict[str, list[str]]:
    return {
        "metrics": _metadata_key_values(metadata, _METRIC_KEYS),
        "events": _metadata_key_values(metadata, _EVENT_KEYS),
        "dimensions": _metadata_key_values(metadata, _DIMENSION_KEYS),
        "baselines": _metadata_key_values(metadata, _BASELINE_KEYS),
        "dashboards": _metadata_key_values(metadata, _DASHBOARD_KEYS),
        "owners": _metadata_key_values(metadata, _OWNER_KEYS),
        "risks": _metadata_key_values(metadata, _RISK_KEYS),
    }


def _metadata_key_values(metadata: Mapping[str, Any], keys: tuple[str, ...]) -> list[str]:
    values: list[str] = []
    wanted = {key.casefold() for key in keys}
    for key, value in _walk_metadata(metadata):
        normalized = key.casefold().replace("-", "_").replace(" ", "_")
        if normalized in wanted:
            values.extend(_strings(value))
    return values


def _metadata_objectives(metadata: Mapping[str, Any]) -> tuple[FeatureAdoptionMeasurementObjective, ...]:
    objectives: list[FeatureAdoptionMeasurementObjective] = []
    for _, value in _walk_selected_metadata(metadata, _OBJECTIVE_KEYS):
        for item in _strings(value):
            normalized = item.casefold().replace("-", "_").replace(" ", "_")
            if normalized in _OBJECTIVE_ORDER:
                objectives.append(normalized)  # type: ignore[arg-type]
            elif normalized in {"conversion", "conversion_funnel", "funnel_conversion"}:
                objectives.append("funnel")
            elif normalized in {"telemetry", "instrumentation", "events", "analytics_events"}:
                objectives.append("analytics")
    return tuple(_dedupe(objectives))


def _walk_selected_metadata(metadata: Mapping[str, Any], keys: tuple[str, ...]) -> list[tuple[str, Any]]:
    wanted = {key.casefold() for key in keys}
    return [
        (key, value)
        for key, value in _walk_metadata(metadata)
        if key.casefold().replace("-", "_").replace(" ", "_") in wanted
    ]


def _walk_metadata(value: Mapping[str, Any], prefix: str = "") -> list[tuple[str, Any]]:
    pairs: list[tuple[str, Any]] = []
    for key in sorted(value, key=lambda item: str(item)):
        key_text = str(key)
        child = value[key]
        pairs.append((key_text, child))
        if isinstance(child, Mapping):
            pairs.extend(_walk_metadata(child, f"{prefix}.{key_text}" if prefix else key_text))
    return pairs


def _default_metrics(objective: FeatureAdoptionMeasurementObjective) -> tuple[str, ...]:
    defaults = {
        "adoption": ("Feature adoption rate",),
        "activation": ("Activation completion rate",),
        "engagement": ("Engaged usage rate",),
        "retention": ("Cohort retention rate",),
        "funnel": ("Funnel conversion rate",),
        "analytics": ("Instrumentation coverage rate",),
    }
    return defaults[objective]


def _default_events(objective: FeatureAdoptionMeasurementObjective) -> tuple[str, ...]:
    defaults = {
        "adoption": ("feature_viewed", "feature_used"),
        "activation": ("activation_started", "activation_completed"),
        "engagement": ("feature_engaged",),
        "retention": ("feature_returned",),
        "funnel": ("funnel_step_completed", "funnel_completed"),
        "analytics": ("analytics_event_emitted",),
    }
    return defaults[objective]


def _default_dimensions(objective: FeatureAdoptionMeasurementObjective) -> tuple[str, ...]:
    defaults = {
        "adoption": ("account_id", "plan_tier", "feature_variant"),
        "activation": ("account_id", "user_role", "activation_step"),
        "engagement": ("account_id", "user_role", "session_source"),
        "retention": ("cohort_week", "account_id", "plan_tier"),
        "funnel": ("funnel_step", "account_id", "traffic_source"),
        "analytics": ("event_name", "account_id", "environment"),
    }
    return defaults[objective]


def _default_baseline(objective: FeatureAdoptionMeasurementObjective) -> str:
    return f"Capture a pre-launch baseline or comparison window for {objective}."


def _default_reporting_surface(objective: FeatureAdoptionMeasurementObjective) -> str:
    return f"Product analytics dashboard for {objective}."


def _default_owner_hints(objective: FeatureAdoptionMeasurementObjective) -> list[str]:
    defaults = {
        "adoption": ["product owner", "analytics owner"],
        "activation": ["growth product owner", "analytics owner"],
        "engagement": ["product owner", "data analyst"],
        "retention": ["lifecycle owner", "data analyst"],
        "funnel": ["growth owner", "analytics owner"],
        "analytics": ["analytics engineer", "data owner"],
    }
    return defaults[objective]


def _default_risks(
    objective: FeatureAdoptionMeasurementObjective,
    follow_ups: list[str],
) -> list[str]:
    risks: list[str] = []
    if follow_ups:
        risks.append(f"{objective} measurement may be ambiguous until open questions are answered.")
    return risks


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "depends_on", "dependencies", "tags", "labels", "notes"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        texts.append(("files_or_modules", path))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key, value in _walk_metadata(metadata):
            for text in _strings(value):
                texts.append((f"metadata.{key}", text))
    return texts


def _plan_candidate_texts(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("target_engine", "target_repo", "project_type", "test_strategy", "handoff_prompt", "status"):
        if text := _optional_text(plan.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("milestones",):
        for index, text in enumerate(_strings(plan.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    metadata = plan.get("metadata")
    if isinstance(metadata, Mapping):
        for key, value in _walk_metadata(metadata):
            for text in _strings(value):
                texts.append((f"metadata.{key}", text))
    return texts


def _owner_hints(task: Mapping[str, Any]) -> list[str]:
    hints: list[str] = []
    for key in _OWNER_KEYS:
        hints.extend(_strings(task.get(key)))
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        hints.extend(_metadata_key_values(metadata, _OWNER_KEYS))
    return hints


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if isinstance(plan, ExecutionPlan):
        return dict(plan.model_dump(mode="python"))
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
        if isinstance(item, ExecutionTask):
            tasks.append(dict(item.model_dump(mode="python")))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _path_text(value: str) -> str:
    path = PurePosixPath(value.casefold())
    return " ".join(path.parts).replace("_", " ").replace("-", " ")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


__all__ = [
    "FeatureAdoptionMeasurementObjective",
    "PlanFeatureAdoptionMeasurementMatrix",
    "PlanFeatureAdoptionMeasurementRow",
    "build_plan_feature_adoption_measurement_matrix",
    "plan_feature_adoption_measurement_matrix_to_dict",
    "plan_feature_adoption_measurement_matrix_to_markdown",
    "summarize_plan_feature_adoption_measurement",
]
