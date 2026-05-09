"""Generate capacity planning matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CapacityPlanningScore = Literal["adequate", "partial", "insufficient"]

_SCORE_ORDER: dict[CapacityPlanningScore, int] = {"insufficient": 0, "partial": 1, "adequate": 2}

# Pattern matching for capacity planning signals
_RESOURCE_FORECAST_RE = re.compile(
    r"\b(?:resource\s+forecast|capacity\s+forecast|forecast\s+(?:demand|resources?)|"
    r"(?:cpu|memory|storage|network)\s+forecast|future\s+(?:capacity|resources?)|"
    r"projected\s+(?:usage|demand|load))\b",
    re.I,
)
_GROWTH_PROJECTION_RE = re.compile(
    r"\b(?:growth\s+projections?|projected\s+growth|growth\s+(?:rate|forecast)|"
    r"traffic\s+growth|user\s+growth|scale\s+projections?|"
    r"(?:expected|anticipated)\s+growth|growth\s+(?:plan|trajectory))\b",
    re.I,
)
_SCALING_TRIGGERS_RE = re.compile(
    r"\b(?:scaling\s+triggers?|trigger\s+(?:scaling|threshold)|"
    r"(?:auto[- ]?)?scal(?:e|ing)\s+(?:threshold|limit)|"
    r"(?:cpu|memory|disk)\s+threshold|alert\s+threshold|"
    r"capacity\s+threshold|scale\s+(?:up|down|out)\s+(?:at|when|trigger))\b",
    re.I,
)
_PERFORMANCE_TARGETS_RE = re.compile(
    r"\b(?:performance\s+targets?|latency\s+targets?|throughput\s+targets?|"
    r"sla\s+targets?|slo\s+targets?|response\s+time\s+targets?|"
    r"target\s+(?:latency|throughput|availability|uptime))\b",
    re.I,
)
_BUDGET_CONSTRAINTS_RE = re.compile(
    r"\b(?:budget\s+constraints?|cost\s+constraints?|budget\s+limits?|"
    r"cost\s+(?:budget|ceiling|cap)|spending\s+limits?|"
    r"budget\s+(?:allocation|planning)|cost\s+optimization)\b",
    re.I,
)
_BOTTLENECK_RESOURCES_RE = re.compile(
    r"\b(?:bottleneck(?:s)?|resource\s+bottleneck|capacity\s+bottleneck|"
    r"performance\s+bottleneck|constraint(?:s)?|limiting\s+factor|"
    r"saturation\s+point|resource\s+constraint)\b",
    re.I,
)
_SCALING_FLEXIBILITY_RE = re.compile(
    r"\b(?:scaling\s+flexibility|flexible\s+scaling|elastic(?:ity)?|"
    r"auto[- ]?scal(?:e|ing)|dynamic\s+capacity|on[- ]demand\s+scaling|"
    r"scale\s+(?:horizontally|vertically|elastically))\b",
    re.I,
)
_COST_EFFICIENCY_RE = re.compile(
    r"\b(?:cost\s+efficiency|cost[- ]effective|cost\s+optimization|"
    r"optimize\s+cost|reduce\s+cost|cost\s+savings?|"
    r"(?:right[- ]?size|right[- ]?sizing)|resource\s+utilization)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class CapacityPlanningMatrixRow:
    """Capacity planning signals for one execution task."""

    task_id: str
    title: str
    resource_forecasts: str = "missing"
    growth_projections: str = "missing"
    scaling_triggers: str = "missing"
    performance_targets: str = "missing"
    budget_constraints: str = "missing"
    bottleneck_analysis: str = "missing"
    scaling_flexibility: str = "missing"
    cost_efficiency: str = "missing"
    planning_score: CapacityPlanningScore = "insufficient"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "resource_forecasts": self.resource_forecasts,
            "growth_projections": self.growth_projections,
            "scaling_triggers": self.scaling_triggers,
            "performance_targets": self.performance_targets,
            "budget_constraints": self.budget_constraints,
            "bottleneck_analysis": self.bottleneck_analysis,
            "scaling_flexibility": self.scaling_flexibility,
            "cost_efficiency": self.cost_efficiency,
            "planning_score": self.planning_score,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class CapacityPlanningMatrix:
    """Capacity planning readiness matrix for an execution plan."""

    plan_id: str | None = None
    rows: tuple[CapacityPlanningMatrixRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible representation."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_markdown(self) -> str:
        """Render the matrix as Markdown."""
        title = "# Capacity Planning Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"

        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Tasks analyzed: {self.summary.get('task_count', 0)}",
            f"- Adequate planning: {self.summary.get('adequate_count', 0)}",
            f"- Partial planning: {self.summary.get('partial_count', 0)}",
            f"- Insufficient planning: {self.summary.get('insufficient_count', 0)}",
            f"- Overall coverage: {self.summary.get('overall_coverage', 0)}%",
        ]

        if not self.rows:
            lines.extend(["", "No capacity planning signals detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                "| Task | Forecasts | Growth | Triggers | Targets | Budget | Bottlenecks | Flexibility | Cost Efficiency | Score |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )

        for row in self.rows:
            lines.append(
                f"| {_markdown_cell(row.title)} | "
                f"{_markdown_cell(row.resource_forecasts)} | "
                f"{_markdown_cell(row.growth_projections)} | "
                f"{_markdown_cell(row.scaling_triggers)} | "
                f"{_markdown_cell(row.performance_targets)} | "
                f"{_markdown_cell(row.budget_constraints)} | "
                f"{_markdown_cell(row.bottleneck_analysis)} | "
                f"{_markdown_cell(row.scaling_flexibility)} | "
                f"{_markdown_cell(row.cost_efficiency)} | "
                f"{row.planning_score} |"
            )

        return "\n".join(lines)


def generate_capacity_planning_matrix(
    plan: ExecutionPlan | Mapping[str, Any] | str,
) -> CapacityPlanningMatrix:
    """Generate capacity planning matrix from execution plan."""
    plan_id, tasks = _extract_plan_data(plan)
    rows = tuple(_analyze_task(task) for task in tasks)
    summary = _calculate_summary(rows)

    return CapacityPlanningMatrix(
        plan_id=plan_id,
        rows=rows,
        summary=summary,
    )


def _extract_plan_data(plan: ExecutionPlan | Mapping[str, Any] | str) -> tuple[str | None, list[Mapping[str, Any]]]:
    """Extract plan ID and tasks from various input formats."""
    if isinstance(plan, ExecutionPlan):
        return plan.id, [task.model_dump() for task in plan.tasks]
    if isinstance(plan, Mapping):
        plan_id = plan.get("id") or plan.get("plan_id")
        tasks = plan.get("tasks", [])
        return str(plan_id) if plan_id else None, tasks
    return None, []


def _analyze_task(task: Mapping[str, Any]) -> CapacityPlanningMatrixRow:
    """Analyze capacity planning signals in a task."""
    task_id = str(task.get("id", "unknown"))
    title = str(task.get("title", "Untitled"))

    text = _extract_searchable_text(task)
    evidence_list: list[str] = []

    resource_forecasts = _check_signal(_RESOURCE_FORECAST_RE, text, evidence_list)
    growth_projections = _check_signal(_GROWTH_PROJECTION_RE, text, evidence_list)
    scaling_triggers = _check_signal(_SCALING_TRIGGERS_RE, text, evidence_list)
    performance_targets = _check_signal(_PERFORMANCE_TARGETS_RE, text, evidence_list)
    budget_constraints = _check_signal(_BUDGET_CONSTRAINTS_RE, text, evidence_list)
    bottleneck_analysis = _check_signal(_BOTTLENECK_RESOURCES_RE, text, evidence_list)
    scaling_flexibility = _check_signal(_SCALING_FLEXIBILITY_RE, text, evidence_list)
    cost_efficiency = _check_signal(_COST_EFFICIENCY_RE, text, evidence_list)

    present_count = sum(
        1
        for signal in [
            resource_forecasts,
            growth_projections,
            scaling_triggers,
            performance_targets,
            budget_constraints,
            bottleneck_analysis,
            scaling_flexibility,
            cost_efficiency,
        ]
        if signal == "present"
    )

    if present_count >= 6:
        planning_score: CapacityPlanningScore = "adequate"
    elif present_count >= 3:
        planning_score = "partial"
    else:
        planning_score = "insufficient"

    return CapacityPlanningMatrixRow(
        task_id=task_id,
        title=title,
        resource_forecasts=resource_forecasts,
        growth_projections=growth_projections,
        scaling_triggers=scaling_triggers,
        performance_targets=performance_targets,
        budget_constraints=budget_constraints,
        bottleneck_analysis=bottleneck_analysis,
        scaling_flexibility=scaling_flexibility,
        cost_efficiency=cost_efficiency,
        planning_score=planning_score,
        evidence=tuple(evidence_list[:5]),
    )


def _extract_searchable_text(task: Mapping[str, Any]) -> str:
    """Extract all searchable text from a task."""
    parts: list[str] = []
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = task.get(field)
        if isinstance(value, str):
            parts.append(value)
    for field in ("acceptance_criteria", "requirements", "notes"):
        value = task.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)
    return " ".join(parts)


def _check_signal(pattern: re.Pattern[str], text: str, evidence_list: list[str]) -> str:
    """Check if pattern is present in text and collect evidence."""
    match = pattern.search(text)
    if match:
        evidence_list.append(match.group()[:50])
        return "present"
    return "missing"


def _calculate_summary(rows: tuple[CapacityPlanningMatrixRow, ...]) -> dict[str, Any]:
    """Calculate summary statistics for the matrix."""
    if not rows:
        return {
            "task_count": 0,
            "adequate_count": 0,
            "partial_count": 0,
            "insufficient_count": 0,
            "overall_coverage": 0,
        }

    score_counts = {"adequate": 0, "partial": 0, "insufficient": 0}
    for row in rows:
        score_counts[row.planning_score] += 1

    overall_coverage = int((score_counts["adequate"] + score_counts["partial"] * 0.5) / len(rows) * 100)

    return {
        "task_count": len(rows),
        "adequate_count": score_counts["adequate"],
        "partial_count": score_counts["partial"],
        "insufficient_count": score_counts["insufficient"],
        "overall_coverage": overall_coverage,
    }


def _markdown_cell(text: str) -> str:
    """Escape text for Markdown table cells."""
    return text.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "CapacityPlanningMatrixRow",
    "CapacityPlanningMatrix",
    "generate_capacity_planning_matrix",
]
