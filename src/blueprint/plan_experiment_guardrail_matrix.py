"""Build experiment guardrail readiness matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ExperimentGuardrailField = Literal[
    "primary_metric",
    "guardrail_metrics",
    "sample_or_cohort_signal",
    "stop_condition",
    "owner",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_REQUIRED_FIELDS: tuple[ExperimentGuardrailField, ...] = (
    "primary_metric",
    "guardrail_metrics",
    "sample_or_cohort_signal",
    "stop_condition",
    "owner",
)
_SURFACE_ORDER = {
    "a/b test": 0,
    "feature test": 1,
    "rollout": 2,
    "experiment": 3,
    "cohort rollout": 4,
    "conversion test": 5,
}
_SURFACE_PATTERNS: dict[str, re.Pattern[str]] = {
    "a/b test": re.compile(r"\b(?:a/b|ab)\s+tests?\b|\bsplit tests?\b", re.I),
    "experiment": re.compile(r"\bexperiments?\b", re.I),
    "feature test": re.compile(
        r"\bfeature tests?\b|\btest(?:ing)?\s+(?:the\s+)?(?:new\s+)?feature\b",
        re.I,
    ),
    "rollout": re.compile(r"\b(?:rollout|roll out|ramp)\b", re.I),
    "cohort rollout": re.compile(r"\b(?:cohorts?|beta users?|holdout|treatment|control)\b", re.I),
    "conversion test": re.compile(r"\b(?:conversion|activation|signup|checkout|upgrade)\b", re.I),
}
_EXPERIMENT_RE = re.compile(
    r"\b(?:experiments?|a/b|ab test|split test|feature test|rollout|roll out|ramp|"
    r"cohorts?|treatment|control|holdout|variant|conversion|activation|signup|checkout|upgrade)\b",
    re.I,
)
_METRIC_HINT_RE = re.compile(
    r"\b(?:metric|metrics|kpi|conversion|activation|retention|revenue|ctr|click[- ]through|"
    r"adoption|latency|error rate|support tickets?|complaints?|crash rate)\b",
    re.I,
)
_FIELD_KEY_ALIASES: dict[ExperimentGuardrailField, tuple[str, ...]] = {
    "primary_metric": (
        "primary_metric",
        "success_metric",
        "north_star_metric",
        "primary_kpi",
        "metric",
    ),
    "guardrail_metrics": (
        "guardrail_metric",
        "guardrail_metrics",
        "guardrails",
        "safety_metrics",
        "counter_metrics",
    ),
    "sample_or_cohort_signal": (
        "sample_size",
        "sample",
        "sample_window",
        "cohort",
        "cohorts",
        "audience",
        "population",
        "traffic_split",
        "exposure",
    ),
    "stop_condition": (
        "stop_condition",
        "stop_conditions",
        "stopping_rule",
        "kill_switch",
        "rollback_threshold",
        "rollback_trigger",
        "abort_condition",
    ),
    "owner": (
        "owner",
        "owners",
        "experiment_owner",
        "analysis_owner",
        "metric_owner",
        "dri",
        "assignee",
        "team",
    ),
}
_FIELD_PATTERNS: dict[ExperimentGuardrailField, tuple[re.Pattern[str], ...]] = {
    "primary_metric": (
        re.compile(
            r"\b(?:primary metric|success metric|north star metric|primary kpi)"
            r"\s*[:=-]?\s*((?:\d+\.\d+|[^.;\n])*)",
            re.I,
        ),
    ),
    "guardrail_metrics": (
        re.compile(
            r"\b(?:guardrail metrics?|guardrails?|safety metrics?|counter metrics?)"
            r"\s*[:=-]?\s*((?:\d+\.\d+|[^.;\n])*)",
            re.I,
        ),
    ),
    "sample_or_cohort_signal": (
        re.compile(
            r"\b(?:sample size|sample window|sample|cohorts?|audience|population|"
            r"traffic split|exposure)"
            r"\s*[:=-]\s*((?:\d+\.\d+|[^.;\n])*)",
            re.I,
        ),
        re.compile(
            r"\b((?:beta|holdout|treatment|control)[^.;\n]*"
            r"(?:users?|customers?|accounts?|cohorts?|groups?))\b",
            re.I,
        ),
        re.compile(
            r"\b(?:for|over)\s+(\d+\s+(?:hours?|days?|weeks?))\b|"
            r"\b(\d{1,3}\s*%\s+(?:of\s+)?(?:users|traffic|accounts|customers))\b",
            re.I,
        ),
    ),
    "stop_condition": (
        re.compile(
            r"\b(?:stop condition|stopping rule|stop if|halt if|pause if|abort if|kill switch|"
            r"rollback if|roll back if|revert if|rollback threshold|rollback trigger)"
            r"\s*[:=-]?\s*((?:\d+\.\d+|[^.;\n])*)",
            re.I,
        ),
    ),
    "owner": (
        re.compile(
            r"\b(?:experiment owner|analysis owner|metric owner|owner|dri)"
            r"\s*[:=-]?\s*((?:\d+\.\d+|[^.;\n])*)",
            re.I,
        ),
    ),
}


@dataclass(frozen=True, slots=True)
class PlanExperimentGuardrailMatrixRow:
    """Guardrail readiness details for one experiment-like task."""

    task_id: str
    title: str
    experiment_surface: str
    primary_metric: str = ""
    guardrail_metrics: tuple[str, ...] = field(default_factory=tuple)
    sample_or_cohort_signal: str = ""
    stop_condition: str = ""
    owner: str = ""
    missing_fields: tuple[ExperimentGuardrailField, ...] = field(default_factory=tuple)
    recommendation: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "experiment_surface": self.experiment_surface,
            "primary_metric": self.primary_metric,
            "guardrail_metrics": list(self.guardrail_metrics),
            "sample_or_cohort_signal": self.sample_or_cohort_signal,
            "stop_condition": self.stop_condition,
            "owner": self.owner,
            "missing_fields": list(self.missing_fields),
            "recommendation": self.recommendation,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanExperimentGuardrailMatrix:
    """Plan-level experiment guardrail matrix."""

    plan_id: str | None = None
    rows: tuple[PlanExperimentGuardrailMatrixRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanExperimentGuardrailMatrixRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "summary": dict(self.summary),
            "rows": [row.to_dict() for row in self.rows],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return experiment guardrail rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the experiment guardrail matrix as deterministic Markdown."""
        title = "# Plan Experiment Guardrail Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Total tasks: {self.summary.get('total_task_count', 0)}",
            f"- Experiment tasks: {self.summary.get('experiment_task_count', 0)}",
            f"- Tasks missing guardrails: {self.summary.get('tasks_missing_guardrails', 0)}",
            "- Tasks missing stop conditions: "
            f"{self.summary.get('tasks_missing_stop_conditions', 0)}",
            f"- Tasks missing owners: {self.summary.get('tasks_missing_owners', 0)}",
        ]
        if not self.rows:
            lines.extend(["", "No experiment guardrail rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                (
                    "| Task | Title | Surface | Primary Metric | Guardrails | Sample/Cohort | "
                    "Stop Condition | Owner | Missing Fields | Recommendation | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.task_id)} | "
                f"{_markdown_cell(row.title)} | "
                f"{_markdown_cell(row.experiment_surface)} | "
                f"{_markdown_cell(row.primary_metric or 'unspecified')} | "
                f"{_markdown_cell(', '.join(row.guardrail_metrics) or 'unspecified')} | "
                f"{_markdown_cell(row.sample_or_cohort_signal or 'unspecified')} | "
                f"{_markdown_cell(row.stop_condition or 'unspecified')} | "
                f"{_markdown_cell(row.owner or 'unspecified')} | "
                f"{_markdown_cell(', '.join(row.missing_fields) or 'none')} | "
                f"{_markdown_cell(row.recommendation)} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_experiment_guardrail_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanExperimentGuardrailMatrix:
    """Build guardrail readiness rows for experiment-like execution tasks."""
    plan_id, tasks = _source_payload(source)
    rows = tuple(
        sorted(
            (
                row
                for index, task in enumerate(tasks, start=1)
                if (row := _row_for_task(task, index)) is not None
            ),
            key=lambda row: (
                len(row.missing_fields),
                row.task_id,
                row.title.casefold(),
            ),
            reverse=False,
        )
    )
    return PlanExperimentGuardrailMatrix(
        plan_id=plan_id,
        rows=rows,
        summary=_summary(rows, total_task_count=len(tasks)),
    )


def generate_plan_experiment_guardrail_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[PlanExperimentGuardrailMatrixRow, ...]:
    """Return experiment guardrail rows for relevant execution tasks."""
    return build_plan_experiment_guardrail_matrix(source).rows


def derive_plan_experiment_guardrail_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanExperimentGuardrailMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanExperimentGuardrailMatrix:
    """Return an existing matrix or generate one from a plan-shaped source."""
    if isinstance(source, PlanExperimentGuardrailMatrix):
        return source
    return build_plan_experiment_guardrail_matrix(source)


def summarize_plan_experiment_guardrail_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanExperimentGuardrailMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanExperimentGuardrailMatrix:
    """Compatibility alias for experiment guardrail summaries."""
    return derive_plan_experiment_guardrail_matrix(source)


def plan_experiment_guardrail_matrix_to_dict(
    matrix: PlanExperimentGuardrailMatrix,
) -> dict[str, Any]:
    """Serialize an experiment guardrail matrix to a plain dictionary."""
    return matrix.to_dict()


plan_experiment_guardrail_matrix_to_dict.__test__ = False


def plan_experiment_guardrail_matrix_to_dicts(
    rows: (
        PlanExperimentGuardrailMatrix
        | tuple[PlanExperimentGuardrailMatrixRow, ...]
        | list[PlanExperimentGuardrailMatrixRow]
    ),
) -> list[dict[str, Any]]:
    """Serialize experiment guardrail rows to dictionaries."""
    if isinstance(rows, PlanExperimentGuardrailMatrix):
        return rows.to_dicts()
    return [row.to_dict() for row in rows]


plan_experiment_guardrail_matrix_to_dicts.__test__ = False


def plan_experiment_guardrail_matrix_to_markdown(
    matrix: PlanExperimentGuardrailMatrix,
) -> str:
    """Render an experiment guardrail matrix as Markdown."""
    return matrix.to_markdown()


plan_experiment_guardrail_matrix_to_markdown.__test__ = False


def _row_for_task(
    task: Mapping[str, Any],
    index: int,
) -> PlanExperimentGuardrailMatrixRow | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    surfaces: dict[str, list[str]] = {}
    fields: dict[ExperimentGuardrailField, list[str]] = {key: [] for key in _REQUIRED_FIELDS}
    evidence: list[str] = []

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        if _EXPERIMENT_RE.search(text):
            for surface, pattern in _SURFACE_PATTERNS.items():
                if pattern.search(text):
                    surfaces.setdefault(surface, []).append(snippet)
            if _METRIC_HINT_RE.search(text):
                evidence.append(snippet)
        for field_name, patterns in _FIELD_PATTERNS.items():
            for pattern in patterns:
                for match in pattern.finditer(text):
                    value = next((group for group in match.groups() if group), match.group(0))
                    fields[field_name].append(_clean(value))
                    evidence.append(snippet)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for field_name, aliases in _FIELD_KEY_ALIASES.items():
            for source_field, value in _metadata_values(metadata, aliases):
                for item in _strings(value):
                    fields[field_name].append(_clean(item))
                    evidence.append(_evidence_snippet(source_field, item))
        for source_field, text in _metadata_texts(metadata):
            key_text = source_field.replace("_", " ")
            if _EXPERIMENT_RE.search(text) or _EXPERIMENT_RE.search(key_text):
                for surface, pattern in _SURFACE_PATTERNS.items():
                    if pattern.search(text) or pattern.search(key_text):
                        surfaces.setdefault(surface, []).append(
                            _evidence_snippet(source_field, text or key_text)
                        )

    for key in ("owner", "assignee", "dri", "team"):
        if owner := _optional_text(task.get(key)):
            fields["owner"].append(owner)
            evidence.append(_evidence_snippet(key, owner))

    if not surfaces:
        return None

    experiment_surface = sorted(surfaces, key=lambda item: _SURFACE_ORDER[item])[0]
    primary_metric = _first(fields["primary_metric"])
    guardrails = tuple(_dedupe(_clean(value) for value in fields["guardrail_metrics"]))
    sample_or_cohort = _first(fields["sample_or_cohort_signal"])
    stop_condition = _first(fields["stop_condition"])
    owner = _first(fields["owner"])
    missing_fields = tuple(
        field_name
        for field_name in _REQUIRED_FIELDS
        if (
            (field_name == "primary_metric" and not primary_metric)
            or (field_name == "guardrail_metrics" and not guardrails)
            or (field_name == "sample_or_cohort_signal" and not sample_or_cohort)
            or (field_name == "stop_condition" and not stop_condition)
            or (field_name == "owner" and not owner)
        )
    )
    return PlanExperimentGuardrailMatrixRow(
        task_id=task_id,
        title=title,
        experiment_surface=experiment_surface,
        primary_metric=primary_metric,
        guardrail_metrics=guardrails,
        sample_or_cohort_signal=sample_or_cohort,
        stop_condition=stop_condition,
        owner=owner,
        missing_fields=missing_fields,
        recommendation=_recommendation(missing_fields),
        evidence=tuple(_dedupe([*surfaces[experiment_surface], *evidence])),
    )


def _recommendation(missing_fields: tuple[ExperimentGuardrailField, ...]) -> str:
    if not missing_fields:
        return (
            "Ready: launch plan includes metric, guardrail, cohort, stop-condition, "
            "and owner signals."
        )
    actions = {
        "primary_metric": "define the primary success metric",
        "guardrail_metrics": "add guardrail metrics for quality, reliability, or support impact",
        "sample_or_cohort_signal": "state the sample size, cohort, or traffic split",
        "stop_condition": "set an explicit stop, rollback, or pause condition",
        "owner": "assign a directly responsible owner",
    }
    return "Before launch, " + "; ".join(actions[field_name] for field_name in missing_fields) + "."


def _summary(
    rows: tuple[PlanExperimentGuardrailMatrixRow, ...],
    *,
    total_task_count: int,
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "experiment_task_count": len(rows),
        "unrelated_task_count": max(total_task_count - len(rows), 0),
        "tasks_missing_guardrails": sum(
            1 for row in rows if "guardrail_metrics" in row.missing_fields
        ),
        "tasks_missing_stop_conditions": sum(
            1 for row in rows if "stop_condition" in row.missing_fields
        ),
        "tasks_missing_owners": sum(1 for row in rows if "owner" in row.missing_fields),
        "missing_field_count": sum(len(row.missing_fields) for row in rows),
        "missing_field_counts": {
            field_name: sum(1 for row in rows if field_name in row.missing_fields)
            for field_name in _REQUIRED_FIELDS
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
        if hasattr(item, "model_dump"):
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
        "owner",
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
        "depends_on",
        "dependencies",
        "tags",
        "labels",
        "notes",
        "risks",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    return texts


def _metadata_values(
    value: Any,
    aliases: tuple[str, ...],
    prefix: str = "metadata",
) -> list[tuple[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    wanted = {alias.casefold() for alias in aliases}
    matches: list[tuple[str, Any]] = []
    for key in sorted(value, key=lambda item: str(item)):
        child = value[key]
        field = f"{prefix}.{key}"
        normalized = str(key).casefold().replace("-", "_").replace(" ", "_")
        if normalized in wanted:
            matches.append((field, child))
        if isinstance(child, Mapping):
            matches.extend(_metadata_values(child, aliases, field))
    return matches


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            key_text = str(key).replace("_", " ")
            if _EXPERIMENT_RE.search(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
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


def _first(values: Iterable[str]) -> str:
    values = _dedupe(_clean(value) for value in values)
    return values[0] if values else ""


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _clean(value: str) -> str:
    return _SPACE_RE.sub(" ", str(value)).strip().strip("`'\",;:()[]{}").rstrip(".")


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


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


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


__all__ = [
    "ExperimentGuardrailField",
    "PlanExperimentGuardrailMatrix",
    "PlanExperimentGuardrailMatrixRow",
    "build_plan_experiment_guardrail_matrix",
    "derive_plan_experiment_guardrail_matrix",
    "generate_plan_experiment_guardrail_matrix",
    "plan_experiment_guardrail_matrix_to_dict",
    "plan_experiment_guardrail_matrix_to_dicts",
    "plan_experiment_guardrail_matrix_to_markdown",
    "summarize_plan_experiment_guardrail_matrix",
]
