"""Build beta feedback-loop matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


BetaFeedbackChannel = Literal[
    "user_feedback",
    "survey",
    "feedback_form",
    "product_analytics",
    "support_tagging",
    "usability_notes",
]
BetaFeedbackLoopStep = Literal[
    "feedback_channel",
    "triage_owner",
    "review_cadence",
    "decision_criteria",
    "iteration_criteria",
    "broad_launch_gate",
]
BetaFeedbackRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[BetaFeedbackRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_CHANNEL_ORDER: tuple[BetaFeedbackChannel, ...] = (
    "user_feedback",
    "survey",
    "feedback_form",
    "product_analytics",
    "support_tagging",
    "usability_notes",
)
_REQUIRED_LOOP_STEPS: tuple[BetaFeedbackLoopStep, ...] = (
    "feedback_channel",
    "triage_owner",
    "review_cadence",
    "decision_criteria",
    "iteration_criteria",
    "broad_launch_gate",
)

_BETA_SIGNAL_RE = re.compile(
    r"\b(?:beta|beta users?|beta customers?|beta cohort|beta audience|beta program|"
    r"pilot|pilot users?|pilot cohort|pilot customers?|pilot audience|pilot program|"
    r"preview|preview release|private preview|public preview|early access|limited preview|"
    r"limited availability|limited rollout|trial cohort)\b",
    re.I,
)
_BROAD_LAUNCH_RE = re.compile(
    r"\b(?:broad launch|general availability|ga\b|full launch|public launch|wider launch|"
    r"production launch|rollout to all|all users|all customers|launch gate|go[- ]live gate)\b",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:feedback owner|triage owner|insight owner|research owner|analytics owner|"
    r"support owner|dri|responsible|owner|team|pm|product manager)\b",
    re.I,
)
_CADENCE_RE = re.compile(
    r"\b(?:daily|weekly|biweekly|bi-weekly|fortnightly|review cadence|cadence|"
    r"review every|check(?: |-)?in|office hours|readout|feedback review)\b",
    re.I,
)
_DECISION_RE = re.compile(
    r"\b(?:decision criteria|decision gate|exit criteria|launch criteria|go/no-go|go no go|"
    r"success criteria|success threshold|ship if|launch if|promote if|graduate if|"
    r"block launch|stop criteria)\b",
    re.I,
)
_ITERATION_RE = re.compile(
    r"\b(?:iteration criteria|iterate if|iteration plan|follow[- ]?up iteration|"
    r"next iteration|improvement backlog|feedback backlog|action items?|"
    r"fix forward|revise|adjust|prioriti[sz]e feedback)\b",
    re.I,
)

_CHANNEL_PATTERNS: dict[BetaFeedbackChannel, re.Pattern[str]] = {
    "user_feedback": re.compile(
        r"\b(?:user feedback|customer feedback|beta feedback|pilot feedback|"
        r"preview feedback|in[- ]app feedback|feedback session)\b",
        re.I,
    ),
    "survey": re.compile(r"\b(?:survey|surveys|nps|csat|questionnaire|poll)\b", re.I),
    "feedback_form": re.compile(
        r"\b(?:feedback form|intake form|google form|typeform|form submission|"
        r"feedback link|feedback widget)\b",
        re.I,
    ),
    "product_analytics": re.compile(
        r"\b(?:product analytics|analytics event|analytics events|telemetry|"
        r"usage metric|usage metrics|funnel|activation metric|adoption metric|"
        r"conversion metric|dashboard)\b",
        re.I,
    ),
    "support_tagging": re.compile(
        r"\b(?:support tag|support tagging|ticket tag|ticket tagging|helpdesk tag|"
        r"zendesk tag|intercom tag|support queue|support tickets?)\b",
        re.I,
    ),
    "usability_notes": re.compile(
        r"\b(?:usability notes?|usability findings?|research notes?|user interviews?|"
        r"moderated sessions?|session notes?|ux notes?)\b",
        re.I,
    ),
}
_STEP_KEY_ALIASES: dict[BetaFeedbackLoopStep, tuple[str, ...]] = {
    "feedback_channel": (
        "feedback_channel",
        "feedback_channels",
        "channels",
        "capture_channels",
        "feedback_capture",
    ),
    "triage_owner": (
        "triage_owner",
        "feedback_owner",
        "owner",
        "owners",
        "dri",
        "responsible",
        "team",
    ),
    "review_cadence": (
        "review_cadence",
        "cadence",
        "feedback_cadence",
        "readout_cadence",
        "check_in",
    ),
    "decision_criteria": (
        "decision_criteria",
        "launch_criteria",
        "exit_criteria",
        "go_no_go",
        "success_threshold",
    ),
    "iteration_criteria": (
        "iteration_criteria",
        "iteration_plan",
        "feedback_backlog",
        "follow_up",
        "action_items",
    ),
    "broad_launch_gate": (
        "broad_launch_gate",
        "launch_gate",
        "ga_gate",
        "general_availability_gate",
        "broad_launch",
    ),
}


@dataclass(frozen=True, slots=True)
class PlanBetaFeedbackLoopMatrixRow:
    """Feedback-loop readiness for one beta, pilot, or preview task."""

    task_id: str
    title: str
    feedback_channels: tuple[BetaFeedbackChannel, ...] = field(default_factory=tuple)
    missing_loop_steps: tuple[BetaFeedbackLoopStep, ...] = field(default_factory=tuple)
    risk_level: BetaFeedbackRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "feedback_channels": list(self.feedback_channels),
            "missing_loop_steps": list(self.missing_loop_steps),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanBetaFeedbackLoopMatrix:
    """Plan-level beta feedback-loop matrix."""

    plan_id: str | None = None
    rows: tuple[PlanBetaFeedbackLoopMatrixRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "summary": dict(self.summary),
            "rows": [row.to_dict() for row in self.rows],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return beta feedback-loop rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Beta Feedback Loop Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Feedback-loop task count: {self.summary.get('feedback_loop_task_count', 0)}",
            f"- Missing loop step count: {self.summary.get('missing_loop_step_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.rows:
            lines.extend(["", "No beta or feedback-loop signals were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                "| Task | Title | Risk | Feedback Channels | Missing Loop Steps | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{_markdown_cell(row.title)} | "
                f"{row.risk_level} | "
                f"{_markdown_cell('; '.join(row.feedback_channels) or 'none')} | "
                f"{_markdown_cell('; '.join(row.missing_loop_steps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_beta_feedback_loop_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
        | object
    ),
) -> PlanBetaFeedbackLoopMatrix:
    """Build rows for tasks that need beta feedback-loop decisions before launch."""
    plan_id, plan_context, tasks = _source_payload(source)
    rows = tuple(
        sorted(
            (
                row
                for index, task in enumerate(tasks, start=1)
                if (row := _row_for_task(task, index, plan_context)) is not None
            ),
            key=lambda row: (_RISK_ORDER[row.risk_level], row.task_id, row.title.casefold()),
        )
    )
    return PlanBetaFeedbackLoopMatrix(
        plan_id=plan_id,
        rows=rows,
        summary=_summary(rows, task_count=len(tasks)),
    )


def generate_plan_beta_feedback_loop_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
        | object
    ),
) -> tuple[PlanBetaFeedbackLoopMatrixRow, ...]:
    """Return beta feedback-loop rows for relevant execution tasks."""
    return build_plan_beta_feedback_loop_matrix(source).rows


def summarize_plan_beta_feedback_loop_matrix(
    source_or_matrix: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanBetaFeedbackLoopMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask]
        | object
    ),
) -> PlanBetaFeedbackLoopMatrix:
    """Build the beta feedback-loop matrix, accepting an existing matrix unchanged."""
    if isinstance(source_or_matrix, PlanBetaFeedbackLoopMatrix):
        return source_or_matrix
    return build_plan_beta_feedback_loop_matrix(source_or_matrix)


def summarize_plan_beta_feedback_loop(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
        | object
    ),
) -> PlanBetaFeedbackLoopMatrix:
    """Compatibility alias for building the plan beta feedback-loop matrix."""
    return build_plan_beta_feedback_loop_matrix(source)


def plan_beta_feedback_loop_matrix_to_dict(
    matrix: PlanBetaFeedbackLoopMatrix,
) -> dict[str, Any]:
    """Serialize a beta feedback-loop matrix to a plain dictionary."""
    return matrix.to_dict()


plan_beta_feedback_loop_matrix_to_dict.__test__ = False


def plan_beta_feedback_loop_matrix_to_dicts(
    rows: (
        PlanBetaFeedbackLoopMatrix
        | tuple[PlanBetaFeedbackLoopMatrixRow, ...]
        | list[PlanBetaFeedbackLoopMatrixRow]
    ),
) -> list[dict[str, Any]]:
    """Serialize beta feedback-loop rows to dictionaries."""
    if isinstance(rows, PlanBetaFeedbackLoopMatrix):
        return rows.to_dicts()
    return [row.to_dict() for row in rows]


plan_beta_feedback_loop_matrix_to_dicts.__test__ = False


def plan_beta_feedback_loop_matrix_to_markdown(matrix: PlanBetaFeedbackLoopMatrix) -> str:
    """Render a beta feedback-loop matrix as Markdown."""
    return matrix.to_markdown()


plan_beta_feedback_loop_matrix_to_markdown.__test__ = False


def _row_for_task(
    task: Mapping[str, Any],
    index: int,
    plan_context: tuple[tuple[str, str], ...],
) -> PlanBetaFeedbackLoopMatrixRow | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    context = [*plan_context, *_candidate_texts(task), *_metadata_texts(task.get("metadata"))]

    beta_evidence: list[str] = []
    channel_evidence: list[str] = []
    loop_evidence: dict[BetaFeedbackLoopStep, list[str]] = {
        step: [] for step in _REQUIRED_LOOP_STEPS
    }
    channel_hits: dict[BetaFeedbackChannel, list[str]] = {channel: [] for channel in _CHANNEL_ORDER}

    for source_field, text in context:
        snippet = _evidence_snippet(source_field, text)
        if _BETA_SIGNAL_RE.search(text):
            beta_evidence.append(snippet)
        if _BROAD_LAUNCH_RE.search(text):
            loop_evidence["broad_launch_gate"].append(snippet)
        if _OWNER_RE.search(text):
            loop_evidence["triage_owner"].append(snippet)
        if _CADENCE_RE.search(text):
            loop_evidence["review_cadence"].append(snippet)
        if _DECISION_RE.search(text):
            loop_evidence["decision_criteria"].append(snippet)
        if _ITERATION_RE.search(text):
            loop_evidence["iteration_criteria"].append(snippet)
        for channel, pattern in _CHANNEL_PATTERNS.items():
            if pattern.search(text):
                channel_hits[channel].append(snippet)
                channel_evidence.append(snippet)
                loop_evidence["feedback_channel"].append(snippet)

    metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}
    plan_metadata_context = tuple(item for item in plan_context if item[0].startswith("plan.metadata"))
    for source_field, value in [*plan_metadata_context, *_metadata_texts(metadata)]:
        key_text = source_field.replace("_", " ")
        combined = f"{key_text} {value}"
        for channel, pattern in _CHANNEL_PATTERNS.items():
            if pattern.search(combined):
                channel_hits[channel].append(_evidence_snippet(source_field, value or key_text))
                loop_evidence["feedback_channel"].append(
                    _evidence_snippet(source_field, value or key_text)
                )
        for step, aliases in _STEP_KEY_ALIASES.items():
            if any(alias.replace("_", " ") in key_text.casefold() for alias in aliases):
                loop_evidence[step].append(_evidence_snippet(source_field, value or key_text))

    feedback_channels = tuple(
        channel for channel in _CHANNEL_ORDER if _dedupe(channel_hits[channel])
    )
    has_signal = bool(beta_evidence or channel_evidence or feedback_channels)
    if not has_signal:
        return None

    present_steps = {step for step, evidence in loop_evidence.items() if _dedupe(evidence)}
    missing_loop_steps = tuple(step for step in _REQUIRED_LOOP_STEPS if step not in present_steps)
    return PlanBetaFeedbackLoopMatrixRow(
        task_id=task_id,
        title=title,
        feedback_channels=feedback_channels,
        missing_loop_steps=missing_loop_steps,
        risk_level=_risk_level(missing_loop_steps, beta_signal=bool(beta_evidence)),
        evidence=tuple(_dedupe([*beta_evidence, *channel_evidence, *sum(loop_evidence.values(), [])])),
    )


def _risk_level(
    missing_loop_steps: tuple[BetaFeedbackLoopStep, ...],
    *,
    beta_signal: bool,
) -> BetaFeedbackRiskLevel:
    if not missing_loop_steps:
        return "low"
    critical = {"feedback_channel", "decision_criteria", "iteration_criteria", "broad_launch_gate"}
    if beta_signal and critical.intersection(missing_loop_steps):
        return "high"
    if len(missing_loop_steps) >= 3:
        return "high"
    return "medium"


def _summary(
    rows: tuple[PlanBetaFeedbackLoopMatrixRow, ...],
    *,
    task_count: int,
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "feedback_loop_task_count": len(rows),
        "missing_loop_step_count": sum(len(row.missing_loop_steps) for row in rows),
        "at_risk_task_count": sum(1 for row in rows if row.risk_level != "low"),
        "risk_counts": {
            level: sum(1 for row in rows if row.risk_level == level) for level in _RISK_ORDER
        },
        "feedback_channel_counts": {
            channel: sum(1 for row in rows if channel in row.feedback_channels)
            for channel in _CHANNEL_ORDER
        },
        "missing_loop_step_counts": {
            step: sum(1 for row in rows if step in row.missing_loop_steps)
            for step in _REQUIRED_LOOP_STEPS
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
        | object
    ),
) -> tuple[str | None, tuple[tuple[str, str], ...], list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        plan = source.model_dump(mode="python")
        return (
            _optional_text(source.id),
            tuple(_plan_context(plan)),
            [task.model_dump(mode="python") for task in source.tasks],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return (
                _optional_text(plan.get("id")),
                tuple(_plan_context(plan)),
                _task_payloads(plan.get("tasks")),
            )
        return None, (), [dict(source)]

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, (), []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, (), tasks


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


def _plan_context(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "test_strategy",
        "handoff_prompt",
        "status",
        "target_engine",
        "project_type",
    ):
        if text := _optional_text(plan.get(field_name)):
            texts.append((f"plan.{field_name}", text))
    texts.extend(_metadata_texts(plan.get("metadata"), "plan.metadata"))
    for index, milestone in enumerate(_items(plan.get("milestones"))):
        if isinstance(milestone, Mapping):
            for field_name, value in sorted(milestone.items(), key=lambda item: str(item[0])):
                for text in _strings(value):
                    texts.append((f"plan.milestones[{index}].{field_name}", text))
        elif text := _optional_text(milestone):
            texts.append((f"plan.milestones[{index}]", text))
    return texts


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
    for index, path in enumerate(_strings(task.get("files_or_modules"))):
        texts.append((f"files_or_modules[{index}]", path))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            key_text = str(key).replace("_", " ")
            if (
                _BETA_SIGNAL_RE.search(key_text)
                or _BROAD_LAUNCH_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _CHANNEL_PATTERNS.values())
                or any(
                    alias.replace("_", " ") in key_text.casefold()
                    for aliases in _STEP_KEY_ALIASES.values()
                    for alias in aliases
                )
            ):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        texts = []
        for index, item in enumerate(_items(value)):
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
        strings: list[str] = []
        for item in _items(value):
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _items(value: Any) -> list[Any]:
    if isinstance(value, set):
        return sorted(value, key=lambda item: str(item))
    if isinstance(value, (list, tuple)):
        return list(value)
    return []


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
    "BetaFeedbackChannel",
    "BetaFeedbackLoopStep",
    "BetaFeedbackRiskLevel",
    "PlanBetaFeedbackLoopMatrix",
    "PlanBetaFeedbackLoopMatrixRow",
    "build_plan_beta_feedback_loop_matrix",
    "generate_plan_beta_feedback_loop_matrix",
    "plan_beta_feedback_loop_matrix_to_dict",
    "plan_beta_feedback_loop_matrix_to_dicts",
    "plan_beta_feedback_loop_matrix_to_markdown",
    "summarize_plan_beta_feedback_loop",
    "summarize_plan_beta_feedback_loop_matrix",
]
