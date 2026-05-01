"""Derive experiment design matrices from execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


_T = TypeVar("_T")

_EXPERIMENT_SIGNAL_RE = re.compile(
    r"\b(?:a/b|ab test|split test|experiment|feature flag|feature toggle|"
    r"cohort|metric|conversion|adoption|retention|activation|gradual(?:ly)?\s+"
    r"(?:exposure|rollout)|exposure|rollout|canary|percentage rollout|holdout)\b",
    re.IGNORECASE,
)
_GUARDRAIL_RE = re.compile(
    r"\b(?:error rate|latency|p95|p99|slo|sla|incident|rollback|"
    r"churn|refund|support ticket|complaint|crash|timeout|availability)\b",
    re.IGNORECASE,
)
_SUCCESS_METRIC_RE = re.compile(
    r"\b(?:conversion rate|conversion|adoption rate|adoption|retention rate|retention|"
    r"activation rate|activation|completion rate|completion|click[- ]through rate|ctr|"
    r"signup rate|trial start|upgrade rate|revenue|engagement|usage|primary metric)\b",
    re.IGNORECASE,
)
_EXPOSURE_RE = re.compile(
    r"\b(?:\d{1,3}\s*%\s+(?:of\s+)?(?:users|traffic|accounts|cohort)|"
    r"feature[- ]flag|feature toggle|gradual(?:ly)?\s+(?:exposure|rollout)|"
    r"rollout|canary|percentage rollout|holdout|internal users)\b",
    re.IGNORECASE,
)
_COHORT_RE = re.compile(
    r"(?:\b(?:cohort|audience|segment|target users?)\s*[:=-]\s+|\bfor\s+)"
    r"([^.;\n]+?)(?=\s+(?:with|using|to|and track|and monitor)\b|[.;\n]|$)",
    re.IGNORECASE,
)
_HYPOTHESIS_RE = re.compile(
    r"\b(?:hypothesis|we believe|expect(?:ed)?|assume)\s*[:=-]?\s*([^.;\n]+)",
    re.IGNORECASE,
)
_OBJECTIVE_RE = re.compile(
    r"\b(?:experiment|a/b test|ab test|feature flag|feature toggle|rollout)\s+"
    r"(?:for|on|of)?\s*([^.;\n]+)",
    re.IGNORECASE,
)
_SPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class PlanExperimentDesignMatrixRow:
    """One experiment design row covering related execution tasks."""

    objective: str
    covered_task_ids: tuple[str, ...] = field(default_factory=tuple)
    hypothesis: str = "unspecified"
    cohort: str = "unspecified"
    success_metrics: tuple[str, ...] = field(default_factory=tuple)
    guardrail_metrics: tuple[str, ...] = field(default_factory=tuple)
    exposure_strategy: str = "unspecified"
    risks: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "objective": self.objective,
            "covered_task_ids": list(self.covered_task_ids),
            "hypothesis": self.hypothesis,
            "cohort": self.cohort,
            "success_metrics": list(self.success_metrics),
            "guardrail_metrics": list(self.guardrail_metrics),
            "exposure_strategy": self.exposure_strategy,
            "risks": list(self.risks),
            "follow_up_questions": list(self.follow_up_questions),
        }


@dataclass(frozen=True, slots=True)
class PlanExperimentDesignMatrix:
    """Experiment planning matrix for an execution plan."""

    plan_id: str | None = None
    rows: tuple[PlanExperimentDesignMatrixRow, ...] = field(default_factory=tuple)

    @property
    def summary(self) -> dict[str, Any]:
        """Return compact rollup counts in stable key order."""
        return {
            "experiment_count": len(self.rows),
            "covered_task_count": len(
                {task_id for row in self.rows for task_id in row.covered_task_ids}
            ),
            "missing_hypothesis_count": sum(
                1 for row in self.rows if row.hypothesis == "unspecified"
            ),
            "missing_metric_count": sum(
                1
                for row in self.rows
                if not row.success_metrics or not row.guardrail_metrics
            ),
        }

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "summary": self.summary,
            "rows": [row.to_dict() for row in self.rows],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return experiment rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Experiment Design Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.rows:
            lines.extend(["", "No experiment design rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Objective | Covered Tasks | Hypothesis | Cohort | Success Metrics | "
                    "Guardrails | Exposure | Risks | Follow-up |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.objective)} | "
                f"{_markdown_cell(', '.join(row.covered_task_ids) or 'plan')} | "
                f"{_markdown_cell(row.hypothesis)} | "
                f"{_markdown_cell(row.cohort)} | "
                f"{_markdown_cell(', '.join(row.success_metrics) or 'unspecified')} | "
                f"{_markdown_cell(', '.join(row.guardrail_metrics) or 'unspecified')} | "
                f"{_markdown_cell(row.exposure_strategy)} | "
                f"{_markdown_cell('; '.join(row.risks) or 'unspecified')} | "
                f"{_markdown_cell('; '.join(row.follow_up_questions) or '-')} |"
            )
        return "\n".join(lines)


def build_plan_experiment_design_matrix(
    source: Mapping[str, Any] | ExecutionPlan,
) -> PlanExperimentDesignMatrix:
    """Derive experiment design rows from explicit metadata and plan text."""
    plan = _plan_payload(source)
    builders: dict[str, _RowBuilder] = {}
    tasks = _task_payloads(plan.get("tasks"))
    all_task_ids = [
        _optional_text(task.get("id")) or f"task-{index}"
        for index, task in enumerate(tasks, start=1)
    ]

    for task_id, task in zip(all_task_ids, tasks, strict=True):
        metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}
        context = _task_context(task)
        text_context = _task_text_context(task)
        if not _has_experiment_signal(context, metadata):
            continue
        objective = _objective(task, metadata, text_context)
        key = _key(objective)
        builder = builders.setdefault(key, _RowBuilder(objective=objective))
        builder.task_ids.append(task_id)
        builder.hypotheses.extend(_metadata_values(metadata, "hypothesis", "experiment_hypothesis"))
        builder.hypotheses.extend(_regex_values(_HYPOTHESIS_RE, text_context))
        builder.cohorts.extend(
            _metadata_values(metadata, "cohort", "cohorts", "target_cohort", "audience", "segment")
        )
        builder.cohorts.extend(_regex_values(_COHORT_RE, text_context))
        builder.success_metrics.extend(_success_metrics(metadata, text_context))
        builder.guardrail_metrics.extend(_guardrail_metrics(metadata, text_context))
        builder.exposure_strategies.extend(_exposure_strategies(metadata, text_context))
        builder.risks.extend(_risks(task, metadata, text_context))

    plan_metadata = plan.get("metadata") if isinstance(plan.get("metadata"), Mapping) else {}
    plan_text_context = _plan_text_context(plan)
    plan_context = " ".join([plan_text_context, *_strings(plan_metadata)])
    if _has_experiment_signal(plan_context, plan_metadata):
        objective = _objective({"title": "Plan experiment"}, plan_metadata, plan_text_context)
        builder = builders.setdefault(_key(objective), _RowBuilder(objective=objective))
        builder.task_ids.extend(all_task_ids)
        builder.hypotheses.extend(
            _metadata_values(plan_metadata, "hypothesis", "experiment_hypothesis")
        )
        builder.hypotheses.extend(_regex_values(_HYPOTHESIS_RE, plan_text_context))
        builder.cohorts.extend(
            _metadata_values(
                plan_metadata, "cohort", "cohorts", "target_cohort", "audience", "segment"
            )
        )
        builder.cohorts.extend(_regex_values(_COHORT_RE, plan_text_context))
        builder.success_metrics.extend(_success_metrics(plan_metadata, plan_text_context))
        builder.guardrail_metrics.extend(_guardrail_metrics(plan_metadata, plan_text_context))
        builder.exposure_strategies.extend(_exposure_strategies(plan_metadata, plan_text_context))
        builder.risks.extend(_risks({}, plan_metadata, plan_text_context))

    rows = tuple(_row(builder) for builder in sorted(builders.values(), key=lambda item: item.key))
    return PlanExperimentDesignMatrix(plan_id=_optional_text(plan.get("id")), rows=rows)


def plan_experiment_design_matrix_to_dict(
    matrix: PlanExperimentDesignMatrix,
) -> dict[str, Any]:
    """Serialize an experiment design matrix to a plain dictionary."""
    return matrix.to_dict()


plan_experiment_design_matrix_to_dict.__test__ = False


def plan_experiment_design_matrix_to_markdown(
    matrix: PlanExperimentDesignMatrix,
) -> str:
    """Render an experiment design matrix as Markdown."""
    return matrix.to_markdown()


plan_experiment_design_matrix_to_markdown.__test__ = False


@dataclass(slots=True)
class _RowBuilder:
    objective: str
    task_ids: list[str] = field(default_factory=list)
    hypotheses: list[str] = field(default_factory=list)
    cohorts: list[str] = field(default_factory=list)
    success_metrics: list[str] = field(default_factory=list)
    guardrail_metrics: list[str] = field(default_factory=list)
    exposure_strategies: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)

    @property
    def key(self) -> str:
        return _key(self.objective)


def _row(builder: _RowBuilder) -> PlanExperimentDesignMatrixRow:
    hypothesis = next(iter(_dedupe(_clean_sentence(value) for value in builder.hypotheses)), None)
    cohort = next(iter(_dedupe(_clean_sentence(value) for value in builder.cohorts)), None)
    success_metrics = tuple(_dedupe(_metric_name(value) for value in builder.success_metrics))
    guardrail_metrics = tuple(_dedupe(_metric_name(value) for value in builder.guardrail_metrics))
    exposure = next(
        iter(_dedupe(_clean_sentence(value) for value in builder.exposure_strategies)),
        None,
    )
    if hypothesis is None and cohort and success_metrics:
        hypothesis = (
            f"Changing {builder.objective} for {cohort} will improve "
            f"{success_metrics[0]}."
        )

    follow_ups: list[str] = []
    if hypothesis is None:
        follow_ups.append("What is the falsifiable hypothesis for this experiment?")
    if cohort is None:
        follow_ups.append("Which cohort or segment should be exposed?")
    if not success_metrics:
        follow_ups.append("Which primary success metric decides the outcome?")
    if not guardrail_metrics:
        follow_ups.append("Which guardrail metrics must stay healthy?")
    if exposure is None:
        follow_ups.append("What exposure strategy, holdout, or ramp schedule should be used?")

    return PlanExperimentDesignMatrixRow(
        objective=builder.objective,
        covered_task_ids=tuple(_dedupe(builder.task_ids)),
        hypothesis=hypothesis or "unspecified",
        cohort=cohort or "unspecified",
        success_metrics=success_metrics,
        guardrail_metrics=guardrail_metrics,
        exposure_strategy=exposure or "unspecified",
        risks=tuple(_dedupe(_clean_sentence(value) for value in builder.risks)),
        follow_up_questions=tuple(follow_ups),
    )


def _has_experiment_signal(context: str, metadata: Mapping[str, Any]) -> bool:
    if _EXPERIMENT_SIGNAL_RE.search(context):
        return True
    keys = " ".join(str(key) for key in metadata)
    if _EXPERIMENT_SIGNAL_RE.search(keys):
        return True
    return bool(
        _metadata_values(
            metadata,
            "experiment",
            "experiment_id",
            "experiment_name",
            "experiment_objective",
            "feature_flag",
            "flag_name",
            "success_metrics",
            "guardrail_metrics",
            "cohort",
            "exposure_strategy",
            "rollout",
        )
    )


def _objective(task: Mapping[str, Any], metadata: Mapping[str, Any], context: str) -> str:
    explicit = next(
        iter(
            _metadata_values(
                metadata,
                "experiment_objective",
                "objective",
                "experiment_name",
                "feature_area",
                "feature_flag",
                "flag_name",
            )
        ),
        None,
    )
    if explicit:
        return _title(_slug_to_words(explicit))
    title = _optional_text(task.get("title"))
    if title:
        match = _OBJECTIVE_RE.search(title)
        if match:
            return _title(_trim_objective(match.group(1)))
        return _title(_trim_objective(title))
    match = _OBJECTIVE_RE.search(context)
    if match:
        return _title(_trim_objective(match.group(1)))
    return "Unspecified Experiment"


def _success_metrics(metadata: Mapping[str, Any], context: str) -> list[str]:
    values = _metadata_values(
        metadata,
        "success_metric",
        "success_metrics",
        "primary_metric",
        "metrics",
        "kpi",
        "kpis",
    )
    values.extend(match.group(0) for match in _SUCCESS_METRIC_RE.finditer(context))
    return values


def _guardrail_metrics(metadata: Mapping[str, Any], context: str) -> list[str]:
    values = _metadata_values(metadata, "guardrail_metric", "guardrail_metrics", "guardrails")
    values.extend(match.group(0) for match in _GUARDRAIL_RE.finditer(context))
    return values


def _exposure_strategies(metadata: Mapping[str, Any], context: str) -> list[str]:
    values = _metadata_values(metadata, "exposure_strategy", "rollout_strategy", "ramp", "holdout")
    rollout = metadata.get("rollout")
    if isinstance(rollout, Mapping):
        values.extend(_strings(rollout))
    values.extend(match.group(0) for match in _EXPOSURE_RE.finditer(context))
    return values


def _risks(task: Mapping[str, Any], metadata: Mapping[str, Any], context: str) -> list[str]:
    values = _metadata_values(metadata, "risk", "risks", "analysis_risks", "experiment_risks")
    risk_level = _optional_text(task.get("risk_level"))
    if risk_level and risk_level.lower() in {"high", "critical", "blocker"}:
        values.append("high-risk implementation task may bias or delay experiment analysis")
    if "sample size" in context.lower() or "underpowered" in context.lower():
        values.append("sample size or power may be insufficient")
    if "seasonal" in context.lower() or "novelty" in context.lower():
        values.append("seasonality or novelty effects may bias analysis")
    return values


def _metadata_values(metadata: Mapping[str, Any], *keys: str) -> list[str]:
    values: list[str] = []
    wanted = {key.lower() for key in keys}
    for key, value in metadata.items():
        normalized = str(key).lower()
        if normalized in wanted:
            values.extend(_strings(value))
        elif isinstance(value, Mapping):
            values.extend(_metadata_values(value, *keys))
    return values


def _regex_values(pattern: re.Pattern[str], text: str) -> list[str]:
    return [_clean_sentence(match.group(1)) for match in pattern.finditer(text)]


def _task_context(task: Mapping[str, Any]) -> str:
    values = [_task_text_context(task)]
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        values.extend(_strings(metadata))
    return " ".join(value for value in values if value)


def _task_text_context(task: Mapping[str, Any]) -> str:
    values = [
        _text(task.get("title")),
        _text(task.get("description")),
        *_strings(task.get("files_or_modules")),
        *_strings(task.get("acceptance_criteria")),
        *_strings(task.get("tags")),
    ]
    return " ".join(value for value in values if value)


def _plan_text_context(plan: Mapping[str, Any]) -> str:
    values = [
        _text(plan.get("test_strategy")),
        _text(plan.get("handoff_prompt")),
        _text(plan.get("target_repo")),
        _text(plan.get("project_type")),
    ]
    return " ".join(value for value in values if value)


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            dumped = item.model_dump(mode="python")
            if isinstance(dumped, Mapping):
                tasks.append(dict(dumped))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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


def _trim_objective(value: str) -> str:
    text = _clean_sentence(value)
    text = re.sub(
        r"^(?:add|build|implement|create|wire|prepare|launch|run|monitor|enable|"
        r"introduce|test|experiment|a/b test|ab test|feature flag|feature toggle)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    return re.split(r"\b(?:behind|using|with|for cohort|and track|and monitor)\b", text)[0].strip()


def _metric_name(value: str) -> str:
    return _clean_sentence(value).lower()


def _clean_sentence(value: str) -> str:
    return _SPACE_RE.sub(" ", str(value)).strip().strip("`'\",;:()[]{}").rstrip(".")


def _slug_to_words(value: str) -> str:
    return re.sub(r"[-_]+", " ", _clean_sentence(value))


def _title(value: str) -> str:
    text = _clean_sentence(value)
    return text[:1].upper() + text[1:] if text else "Unspecified Experiment"


def _key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "unspecified"


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


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "PlanExperimentDesignMatrix",
    "PlanExperimentDesignMatrixRow",
    "build_plan_experiment_design_matrix",
    "plan_experiment_design_matrix_to_dict",
    "plan_experiment_design_matrix_to_markdown",
]
