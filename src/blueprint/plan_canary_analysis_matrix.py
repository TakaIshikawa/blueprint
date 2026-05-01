"""Build canary and progressive-exposure analysis matrices for plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CanaryAnalysisInput = Literal[
    "baseline_metric",
    "success_metric",
    "guardrail_metric",
    "sample_window",
    "owner",
    "rollback_threshold",
    "customer_impact_check",
]
CanaryAnalysisRiskLevel = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_REQUIRED_INPUTS: tuple[CanaryAnalysisInput, ...] = (
    "baseline_metric",
    "success_metric",
    "guardrail_metric",
    "sample_window",
    "owner",
    "rollback_threshold",
    "customer_impact_check",
)
_RISK_ORDER: dict[CanaryAnalysisRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER = {
    "canary": 0,
    "progressive exposure": 1,
    "percentage rollout": 2,
    "beta cohort": 3,
    "dark launch": 4,
    "shadow traffic": 5,
    "experiment ramp": 6,
}
_SURFACE_PATTERNS: dict[str, re.Pattern[str]] = {
    "canary": re.compile(r"\bcanar(?:y|ies)\b", re.I),
    "progressive exposure": re.compile(
        r"\b(?:progressive exposure|progressive rollout|gradual exposure|gradual rollout)\b",
        re.I,
    ),
    "percentage rollout": re.compile(
        r"\b(?:percentage rollout|\d{1,3}\s*%\s+(?:of\s+)?(?:users|traffic|accounts|cohort|customers)|"
        r"(?:rollout|expose|ramp)\s+(?:to\s+)?\d{1,3}\s*%)\b",
        re.I,
    ),
    "beta cohort": re.compile(
        r"\b(?:beta cohorts?|beta users?|beta customers?|beta audience)\b", re.I
    ),
    "dark launch": re.compile(r"\bdark launch\b", re.I),
    "shadow traffic": re.compile(r"\bshadow traffic\b", re.I),
    "experiment ramp": re.compile(
        r"\b(?:experiment ramp|ramp experiment|ramp schedule|traffic ramp)\b", re.I
    ),
}
_INPUT_KEY_ALIASES: dict[CanaryAnalysisInput, tuple[str, ...]] = {
    "baseline_metric": ("baseline_metric", "baseline", "baseline_kpi"),
    "success_metric": ("success_metric", "success_metrics", "primary_metric", "success_kpi"),
    "guardrail_metric": ("guardrail_metric", "guardrail_metrics", "guardrail", "guardrails"),
    "sample_window": ("sample_window", "analysis_window", "measurement_window", "sample_period"),
    "owner": ("owner", "analysis_owner", "metric_owner", "rollout_owner"),
    "rollback_threshold": (
        "rollback_threshold",
        "rollback_thresholds",
        "rollback_gate",
        "rollback_criteria",
        "rollback_trigger",
    ),
    "customer_impact_check": (
        "customer_impact_check",
        "customer_impact",
        "impact_check",
        "customer_impact_review",
    ),
}
_INPUT_PATTERNS: dict[CanaryAnalysisInput, re.Pattern[str]] = {
    "baseline_metric": re.compile(
        r"\b(?:baseline metric|baseline kpi)\s*[:=-]?\s*([^.;\n]+)|\bbaseline\s*[:=-]\s*([^.;\n]+)",
        re.I,
    ),
    "success_metric": re.compile(
        r"\b(?:success metric|primary metric|success kpi)\s*[:=-]?\s*([^.;\n]+)",
        re.I,
    ),
    "guardrail_metric": re.compile(
        r"\b(?:guardrail metric|guardrail|guardrails)\s*[:=-]?\s*([^.;\n]+)",
        re.I,
    ),
    "sample_window": re.compile(
        r"\b(?:sample window|analysis window|measurement window|sample period)\s*[:=-]?\s*([^.;\n]+)|"
        r"\b(?:for|over)\s+(\d+\s+(?:hours?|days?|weeks?))\b",
        re.I,
    ),
    "owner": re.compile(
        r"\b(?:analysis owner|metric owner|rollout owner|owner)\s*[:=-]?\s*([^.;\n]+)", re.I
    ),
    "rollback_threshold": re.compile(
        r"\b(?:rollback threshold|rollback if|roll back if|revert if|rollback criteria|rollback trigger)"
        r"\s*[:=-]?\s*([^.;\n]+)",
        re.I,
    ),
    "customer_impact_check": re.compile(
        r"\b(?:customer impact check|customer impact review|customer impact|impact check)"
        r"\s*[:=-]?\s*([^.;\n]+)",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class PlanCanaryAnalysisMatrixRow:
    """Required analysis evidence for one canary or progressive-exposure task."""

    task_id: str
    title: str
    rollout_surface: str
    required_metrics: tuple[str, ...] = field(default_factory=tuple)
    missing_analysis_inputs: tuple[CanaryAnalysisInput, ...] = field(default_factory=tuple)
    rollback_thresholds: tuple[str, ...] = field(default_factory=tuple)
    risk_level: CanaryAnalysisRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "rollout_surface": self.rollout_surface,
            "required_metrics": list(self.required_metrics),
            "missing_analysis_inputs": list(self.missing_analysis_inputs),
            "rollback_thresholds": list(self.rollback_thresholds),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanCanaryAnalysisMatrix:
    """Plan-level canary analysis readiness matrix."""

    plan_id: str | None = None
    rows: tuple[PlanCanaryAnalysisMatrixRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "summary": dict(self.summary),
            "rows": [row.to_dict() for row in self.rows],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return canary analysis rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Canary Analysis Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title, "", "## Summary", ""]
        lines.extend(
            [
                f"- Total tasks: {self.summary.get('total_task_count', 0)}",
                f"- Canary tasks: {self.summary.get('canary_task_count', 0)}",
                f"- At-risk tasks: {self.summary.get('at_risk_task_count', 0)}",
                f"- Missing analysis inputs: {self.summary.get('missing_input_count', 0)}",
                f"- Rollback threshold gaps: {self.summary.get('rollback_threshold_gap_count', 0)}",
            ]
        )
        if not self.rows:
            lines.extend(["", "No canary or progressive-exposure tasks were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                (
                    "| Task | Title | Rollout Surface | Required Metrics | Missing Inputs | "
                    "Rollback Thresholds | Risk | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.task_id)} | "
                f"{_markdown_cell(row.title)} | "
                f"{_markdown_cell(row.rollout_surface)} | "
                f"{_markdown_cell(', '.join(row.required_metrics) or 'unspecified')} | "
                f"{_markdown_cell(', '.join(row.missing_analysis_inputs) or 'none')} | "
                f"{_markdown_cell('; '.join(row.rollback_thresholds) or 'unspecified')} | "
                f"{row.risk_level} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_canary_analysis_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanCanaryAnalysisMatrix:
    """Build analysis readiness rows for canary and progressive-exposure tasks."""
    plan_id, tasks = _source_payload(source)
    rows = tuple(
        sorted(
            (
                row
                for index, task in enumerate(tasks, start=1)
                if (row := _row_for_task(task, index)) is not None
            ),
            key=lambda row: (_RISK_ORDER[row.risk_level], row.task_id, row.title.casefold()),
        )
    )
    return PlanCanaryAnalysisMatrix(
        plan_id=plan_id,
        rows=rows,
        summary=_summary(rows, total_task_count=len(tasks)),
    )


def generate_plan_canary_analysis_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[PlanCanaryAnalysisMatrixRow, ...]:
    """Return canary analysis rows for relevant execution tasks."""
    return build_plan_canary_analysis_matrix(source).rows


def summarize_plan_canary_analysis(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanCanaryAnalysisMatrix:
    """Compatibility alias for building the plan canary analysis matrix."""
    return build_plan_canary_analysis_matrix(source)


def plan_canary_analysis_matrix_to_dict(matrix: PlanCanaryAnalysisMatrix) -> dict[str, Any]:
    """Serialize a canary analysis matrix to a plain dictionary."""
    return matrix.to_dict()


plan_canary_analysis_matrix_to_dict.__test__ = False


def plan_canary_analysis_matrix_to_dicts(
    rows: (
        PlanCanaryAnalysisMatrix
        | tuple[PlanCanaryAnalysisMatrixRow, ...]
        | list[PlanCanaryAnalysisMatrixRow]
    ),
) -> list[dict[str, Any]]:
    """Serialize canary analysis rows to dictionaries."""
    if isinstance(rows, PlanCanaryAnalysisMatrix):
        return rows.to_dicts()
    return [row.to_dict() for row in rows]


plan_canary_analysis_matrix_to_dicts.__test__ = False


def plan_canary_analysis_matrix_to_markdown(matrix: PlanCanaryAnalysisMatrix) -> str:
    """Render a canary analysis matrix as Markdown."""
    return matrix.to_markdown()


plan_canary_analysis_matrix_to_markdown.__test__ = False


def _row_for_task(task: Mapping[str, Any], index: int) -> PlanCanaryAnalysisMatrixRow | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    surfaces: dict[str, list[str]] = {}
    inputs: dict[CanaryAnalysisInput, list[str]] = {key: [] for key in _REQUIRED_INPUTS}
    evidence: list[str] = []

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        for surface, pattern in _SURFACE_PATTERNS.items():
            if pattern.search(text):
                surfaces.setdefault(surface, []).append(snippet)
        for input_name, pattern in _INPUT_PATTERNS.items():
            for match in pattern.finditer(text):
                value = next((group for group in match.groups() if group), match.group(0))
                inputs[input_name].append(_clean(value))
                evidence.append(snippet)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for input_name, aliases in _INPUT_KEY_ALIASES.items():
            for source_field, value in _metadata_values(metadata, aliases):
                for item in _strings(value):
                    inputs[input_name].append(_clean(item))
                    evidence.append(_evidence_snippet(source_field, item))
        for source_field, text in _metadata_texts(metadata):
            for surface, pattern in _SURFACE_PATTERNS.items():
                key_text = source_field.replace("_", " ")
                if pattern.search(text) or pattern.search(key_text):
                    surfaces.setdefault(surface, []).append(
                        _evidence_snippet(source_field, text or key_text)
                    )

    owner = _optional_text(task.get("owner")) or _optional_text(task.get("assignee"))
    if owner:
        inputs["owner"].append(owner)
        evidence.append(_evidence_snippet("owner", owner))

    if not surfaces:
        return None

    rollout_surface = sorted(surfaces, key=lambda item: _SURFACE_ORDER[item])[0]
    present_inputs = {
        name for name, values in inputs.items() if _dedupe(_clean(value) for value in values)
    }
    missing_inputs = tuple(name for name in _REQUIRED_INPUTS if name not in present_inputs)
    metric_inputs = ("baseline_metric", "success_metric", "guardrail_metric")
    required_metrics = tuple(
        _dedupe(
            f"{name}: {_clean(value)}"
            for name in metric_inputs
            for value in inputs[name]
            if _clean(value)
        )
    )
    rollback_thresholds = tuple(_dedupe(_clean(value) for value in inputs["rollback_threshold"]))
    return PlanCanaryAnalysisMatrixRow(
        task_id=task_id,
        title=title,
        rollout_surface=rollout_surface,
        required_metrics=required_metrics,
        missing_analysis_inputs=missing_inputs,
        rollback_thresholds=rollback_thresholds,
        risk_level=_risk_level(missing_inputs),
        evidence=tuple(_dedupe([*surfaces[rollout_surface], *evidence])),
    )


def _risk_level(missing_inputs: tuple[CanaryAnalysisInput, ...]) -> CanaryAnalysisRiskLevel:
    if not missing_inputs:
        return "low"
    if "rollback_threshold" in missing_inputs or len(missing_inputs) >= 4:
        return "high"
    return "medium"


def _summary(
    rows: tuple[PlanCanaryAnalysisMatrixRow, ...],
    *,
    total_task_count: int,
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "canary_task_count": len(rows),
        "unrelated_task_count": max(total_task_count - len(rows), 0),
        "at_risk_task_count": sum(1 for row in rows if row.risk_level != "low"),
        "missing_input_count": sum(len(row.missing_analysis_inputs) for row in rows),
        "rollback_threshold_gap_count": sum(
            1 for row in rows if "rollback_threshold" in row.missing_analysis_inputs
        ),
        "risk_counts": {
            level: sum(1 for row in rows if row.risk_level == level)
            for level in ("low", "medium", "high")
        },
        "missing_analysis_input_counts": {
            input_name: sum(1 for row in rows if input_name in row.missing_analysis_inputs)
            for input_name in _REQUIRED_INPUTS
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
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    return texts


def _metadata_values(
    value: Any, aliases: tuple[str, ...], prefix: str = "metadata"
) -> list[tuple[str, Any]]:
    if not isinstance(value, Mapping):
        return []
    wanted = {alias.casefold() for alias in aliases}
    matches: list[tuple[str, Any]] = []
    for key in sorted(value, key=lambda item: str(item)):
        child = value[key]
        field = f"{prefix}.{key}"
        normalized = str(key).casefold()
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
            if any(pattern.search(key_text) for pattern in _SURFACE_PATTERNS.values()):
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
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "CanaryAnalysisInput",
    "CanaryAnalysisRiskLevel",
    "PlanCanaryAnalysisMatrix",
    "PlanCanaryAnalysisMatrixRow",
    "build_plan_canary_analysis_matrix",
    "generate_plan_canary_analysis_matrix",
    "plan_canary_analysis_matrix_to_dict",
    "plan_canary_analysis_matrix_to_dicts",
    "plan_canary_analysis_matrix_to_markdown",
    "summarize_plan_canary_analysis",
]
