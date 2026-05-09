"""Generate infrastructure capacity matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from blueprint.domain.models import ExecutionPlan, ExecutionTask


InfrastructureCapacityScore = Literal["optimal", "adequate", "at_risk"]

_SCORE_ORDER: dict[InfrastructureCapacityScore, int] = {"at_risk": 0, "adequate": 1, "optimal": 2}

# Pattern matching for infrastructure capacity signals
_COMPUTE_REQUIREMENTS_RE = re.compile(
    r"\b(?:compute\s+requirements?|cpu\s+requirements?|processing\s+power|"
    r"instance\s+types?|vm\s+sizes?|cores?\s+(?:count|needed)|"
    r"vcpu|compute\s+capacity|processor\s+requirements?)\b",
    re.I,
)
_STORAGE_NEEDS_RE = re.compile(
    r"\b(?:storage\s+(?:needs?|requirements?|capacity)|disk\s+(?:space|capacity|size)|"
    r"volume\s+size|(?:ebs|ssd|hdd)\s+capacity|data\s+storage|"
    r"persistent\s+storage|storage\s+tier|s3\s+(?:bucket|storage))\b",
    re.I,
)
_NETWORK_BANDWIDTH_RE = re.compile(
    r"\b(?:network\s+(?:bandwidth|capacity|throughput)|bandwidth\s+requirements?|"
    r"network\s+traffic|egress|ingress|data\s+transfer|"
    r"(?:gbps|mbps)\s+(?:bandwidth|throughput)|network\s+io)\b",
    re.I,
)
_SCALING_TRIGGERS_RE = re.compile(
    r"\b(?:scaling\s+triggers?|trigger\s+(?:scaling|threshold)|"
    r"(?:auto[- ]?)?scal(?:e|ing)\s+(?:threshold|limit|policy|rules?)|"
    r"(?:cpu|memory|disk|network)\s+threshold|scale\s+(?:up|down|out|in)\s+(?:at|when|trigger)|"
    r"horizontal\s+scaling|vertical\s+scaling)\b",
    re.I,
)
_RESOURCE_CONSTRAINTS_RE = re.compile(
    r"\b(?:resource\s+constraints?|capacity\s+(?:limits?|constraints?)|"
    r"(?:cpu|memory|storage|network)\s+(?:limits?|constraints?)|"
    r"quota|resource\s+quota|api\s+limits?|rate\s+limits?)\b",
    re.I,
)
_BOTTLENECKS_RE = re.compile(
    r"\b(?:bottleneck(?:s)?|performance\s+bottleneck|capacity\s+bottleneck|"
    r"resource\s+bottleneck|limiting\s+factor|saturation\s+point|"
    r"choke\s+point|constraint(?:s)?)\b",
    re.I,
)
_COST_SPIKES_RE = re.compile(
    r"\b(?:cost\s+spikes?|spending\s+spikes?|(?:unexpected|unplanned)\s+costs?|"
    r"cost\s+overrun|budget\s+overage|cost\s+anomal(?:y|ies)|"
    r"cost\s+alert|cost\s+threshold)\b",
    re.I,
)
_AVAILABILITY_ZONES_RE = re.compile(
    r"\b(?:availability\s+zones?|az|multi[- ]?az|zone\s+redundan(?:cy|t)|"
    r"regional?\s+(?:deployment|redundan(?:cy|t))|cross[- ]?region|"
    r"failover\s+region|disaster\s+recovery|high\s+availability|ha\s+setup)\b",
    re.I,
)
_SIZING_ACCURACY_RE = re.compile(
    r"\b(?:sizing\s+(?:accuracy|precision|estimate)|(?:right[- ]?size|right[- ]?sizing)|"
    r"capacity\s+sizing|resource\s+sizing|instance\s+sizing|"
    r"accurate\s+sizing|size\s+(?:calculation|estimate))\b",
    re.I,
)
_HEADROOM_BUFFERS_RE = re.compile(
    r"\b(?:headroom|buffer\s+capacity|capacity\s+buffer|overhead|"
    r"spare\s+capacity|reserve\s+capacity|safety\s+margin|"
    r"capacity\s+margin|(?:buffer|headroom)\s+percentage?)\b",
    re.I,
)
_AUTOSCALING_CONFIG_RE = re.compile(
    r"\b(?:(?:auto[- ]?)?scal(?:e|ing)\s+(?:config|configuration|setup|policy)|"
    r"autoscaling\s+group|asg|scaling\s+policy|"
    r"target\s+tracking|step\s+scaling|scheduled\s+scaling|"
    r"min(?:imum)?\s+instances?|max(?:imum)?\s+instances?)\b",
    re.I,
)
_COST_PROJECTIONS_RE = re.compile(
    r"\b(?:cost\s+(?:projections?|forecast|estimate|budget)|"
    r"projected\s+cost|estimated\s+cost|budget\s+projections?|"
    r"cost\s+planning|monthly\s+cost|annual\s+cost|tco|total\s+cost\s+of\s+ownership)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class InfrastructureCapacityMatrixRow:
    """Infrastructure capacity signals for one execution task."""

    task_id: str
    title: str
    compute_requirements: str = "missing"
    storage_needs: str = "missing"
    network_bandwidth: str = "missing"
    scaling_triggers: str = "missing"
    resource_constraints: str = "missing"
    bottlenecks: str = "missing"
    cost_spikes: str = "missing"
    availability_zones: str = "missing"
    sizing_accuracy: str = "missing"
    headroom_buffers: str = "missing"
    autoscaling_config: str = "missing"
    cost_projections: str = "missing"
    capacity_score: InfrastructureCapacityScore = "at_risk"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommendations: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "compute_requirements": self.compute_requirements,
            "storage_needs": self.storage_needs,
            "network_bandwidth": self.network_bandwidth,
            "scaling_triggers": self.scaling_triggers,
            "resource_constraints": self.resource_constraints,
            "bottlenecks": self.bottlenecks,
            "cost_spikes": self.cost_spikes,
            "availability_zones": self.availability_zones,
            "sizing_accuracy": self.sizing_accuracy,
            "headroom_buffers": self.headroom_buffers,
            "autoscaling_config": self.autoscaling_config,
            "cost_projections": self.cost_projections,
            "capacity_score": self.capacity_score,
            "evidence": list(self.evidence),
            "recommendations": list(self.recommendations),
        }


@dataclass(frozen=True, slots=True)
class InfrastructureCapacityMatrix:
    """Infrastructure capacity planning matrix for an execution plan."""

    plan_id: str | None = None
    rows: tuple[InfrastructureCapacityMatrixRow, ...] = field(default_factory=tuple)
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
        title = "# Infrastructure Capacity Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"

        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Tasks analyzed: {self.summary.get('task_count', 0)}",
            f"- Optimal capacity: {self.summary.get('optimal_count', 0)}",
            f"- Adequate capacity: {self.summary.get('adequate_count', 0)}",
            f"- At risk capacity: {self.summary.get('at_risk_count', 0)}",
            f"- Overall capacity score: {self.summary.get('overall_capacity_score', 0)}%",
        ]

        if not self.rows:
            lines.extend(["", "No infrastructure capacity signals detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                "| Task | Compute | Storage | Network | Scaling | Constraints | Bottlenecks | Cost Spikes | AZ | Score |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )

        for row in self.rows:
            lines.append(
                f"| {_markdown_cell(row.title)} | "
                f"{_markdown_cell(row.compute_requirements)} | "
                f"{_markdown_cell(row.storage_needs)} | "
                f"{_markdown_cell(row.network_bandwidth)} | "
                f"{_markdown_cell(row.scaling_triggers)} | "
                f"{_markdown_cell(row.resource_constraints)} | "
                f"{_markdown_cell(row.bottlenecks)} | "
                f"{_markdown_cell(row.cost_spikes)} | "
                f"{_markdown_cell(row.availability_zones)} | "
                f"{row.capacity_score} |"
            )

        if any(row.recommendations for row in self.rows):
            lines.extend(["", "## Recommendations", ""])
            for row in self.rows:
                if row.recommendations:
                    lines.append(f"### {row.title}")
                    for rec in row.recommendations:
                        lines.append(f"- {rec}")
                    lines.append("")

        return "\n".join(lines)


def generate_infrastructure_capacity_matrix(
    plan: ExecutionPlan | Mapping[str, Any] | str,
) -> InfrastructureCapacityMatrix:
    """Generate infrastructure capacity matrix from execution plan."""
    plan_id, tasks = _extract_plan_data(plan)
    rows = tuple(_analyze_task(task) for task in tasks)
    summary = _calculate_summary(rows)

    return InfrastructureCapacityMatrix(
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


def _analyze_task(task: Mapping[str, Any]) -> InfrastructureCapacityMatrixRow:
    """Analyze infrastructure capacity signals in a task."""
    task_id = str(task.get("id", "unknown"))
    title = str(task.get("title", "Untitled"))

    text = _extract_searchable_text(task)
    evidence_list: list[str] = []

    compute_requirements = _check_signal(_COMPUTE_REQUIREMENTS_RE, text, evidence_list)
    storage_needs = _check_signal(_STORAGE_NEEDS_RE, text, evidence_list)
    network_bandwidth = _check_signal(_NETWORK_BANDWIDTH_RE, text, evidence_list)
    scaling_triggers = _check_signal(_SCALING_TRIGGERS_RE, text, evidence_list)
    resource_constraints = _check_signal(_RESOURCE_CONSTRAINTS_RE, text, evidence_list)
    bottlenecks = _check_signal(_BOTTLENECKS_RE, text, evidence_list)
    cost_spikes = _check_signal(_COST_SPIKES_RE, text, evidence_list)
    availability_zones = _check_signal(_AVAILABILITY_ZONES_RE, text, evidence_list)
    sizing_accuracy = _check_signal(_SIZING_ACCURACY_RE, text, evidence_list)
    headroom_buffers = _check_signal(_HEADROOM_BUFFERS_RE, text, evidence_list)
    autoscaling_config = _check_signal(_AUTOSCALING_CONFIG_RE, text, evidence_list)
    cost_projections = _check_signal(_COST_PROJECTIONS_RE, text, evidence_list)

    # Core capacity signals (required for good capacity planning)
    core_signals = [compute_requirements, storage_needs, network_bandwidth]
    core_present_count = sum(1 for signal in core_signals if signal == "present")

    # Scaling and reliability signals
    scaling_signals = [scaling_triggers, autoscaling_config, availability_zones]
    scaling_present_count = sum(1 for signal in scaling_signals if signal == "present")

    # Risk mitigation signals
    risk_signals = [resource_constraints, bottlenecks, cost_spikes]
    risk_present_count = sum(1 for signal in risk_signals if signal == "present")

    # Optimization signals
    optimization_signals = [sizing_accuracy, headroom_buffers, cost_projections]
    optimization_present_count = sum(1 for signal in optimization_signals if signal == "present")

    total_present = sum(
        1
        for signal in [
            compute_requirements,
            storage_needs,
            network_bandwidth,
            scaling_triggers,
            resource_constraints,
            bottlenecks,
            cost_spikes,
            availability_zones,
            sizing_accuracy,
            headroom_buffers,
            autoscaling_config,
            cost_projections,
        ]
        if signal == "present"
    )

    # Scoring logic: optimal requires core + scaling + some optimization
    # adequate requires core + some scaling or risk awareness
    # at_risk missing core signals
    if core_present_count >= 2 and scaling_present_count >= 2 and optimization_present_count >= 2:
        capacity_score: InfrastructureCapacityScore = "optimal"
    elif core_present_count >= 2 and (scaling_present_count >= 1 or risk_present_count >= 1):
        capacity_score = "adequate"
    else:
        capacity_score = "at_risk"

    recommendations = _generate_recommendations(
        compute_requirements,
        storage_needs,
        network_bandwidth,
        scaling_triggers,
        autoscaling_config,
        availability_zones,
        sizing_accuracy,
        headroom_buffers,
        cost_projections,
        bottlenecks,
        resource_constraints,
        cost_spikes,
    )

    return InfrastructureCapacityMatrixRow(
        task_id=task_id,
        title=title,
        compute_requirements=compute_requirements,
        storage_needs=storage_needs,
        network_bandwidth=network_bandwidth,
        scaling_triggers=scaling_triggers,
        resource_constraints=resource_constraints,
        bottlenecks=bottlenecks,
        cost_spikes=cost_spikes,
        availability_zones=availability_zones,
        sizing_accuracy=sizing_accuracy,
        headroom_buffers=headroom_buffers,
        autoscaling_config=autoscaling_config,
        cost_projections=cost_projections,
        capacity_score=capacity_score,
        evidence=tuple(evidence_list[:8]),
        recommendations=tuple(recommendations),
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


def _generate_recommendations(
    compute: str,
    storage: str,
    network: str,
    scaling_triggers: str,
    autoscaling: str,
    availability: str,
    sizing: str,
    headroom: str,
    cost_proj: str,
    bottlenecks: str,
    constraints: str,
    cost_spikes: str,
) -> list[str]:
    """Generate capacity planning recommendations based on missing signals."""
    recommendations: list[str] = []

    if compute == "missing":
        recommendations.append("Define compute requirements (CPU, instance types, processing power)")
    if storage == "missing":
        recommendations.append("Specify storage needs (capacity, type, IOPS requirements)")
    if network == "missing":
        recommendations.append("Document network bandwidth requirements (ingress/egress, data transfer)")
    if scaling_triggers == "missing" and autoscaling == "missing":
        recommendations.append("Configure scaling triggers and autoscaling policies")
    if availability == "missing":
        recommendations.append("Plan for high availability (multi-AZ, regional redundancy)")
    if sizing == "missing":
        recommendations.append("Perform right-sizing analysis for cost optimization")
    if headroom == "missing":
        recommendations.append("Define capacity headroom/buffer for unexpected load")
    if cost_proj == "missing":
        recommendations.append("Create cost projections and budget estimates")
    if bottlenecks == "present" and constraints == "missing":
        recommendations.append("Document resource constraints and quotas to address bottlenecks")
    if cost_spikes == "present" and cost_proj == "missing":
        recommendations.append("Add cost monitoring and alerts to prevent budget overruns")

    return recommendations


def _calculate_summary(rows: tuple[InfrastructureCapacityMatrixRow, ...]) -> dict[str, Any]:
    """Calculate summary statistics for the matrix."""
    if not rows:
        return {
            "task_count": 0,
            "optimal_count": 0,
            "adequate_count": 0,
            "at_risk_count": 0,
            "overall_capacity_score": 0,
        }

    score_counts = {"optimal": 0, "adequate": 0, "at_risk": 0}
    for row in rows:
        score_counts[row.capacity_score] += 1

    # Calculate overall capacity score (optimal=100%, adequate=60%, at_risk=0%)
    overall_capacity_score = int(
        (score_counts["optimal"] * 100 + score_counts["adequate"] * 60) / len(rows)
    )

    return {
        "task_count": len(rows),
        "optimal_count": score_counts["optimal"],
        "adequate_count": score_counts["adequate"],
        "at_risk_count": score_counts["at_risk"],
        "overall_capacity_score": overall_capacity_score,
    }


def _markdown_cell(text: str) -> str:
    """Escape text for Markdown table cells."""
    return text.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "InfrastructureCapacityMatrixRow",
    "InfrastructureCapacityMatrix",
    "generate_infrastructure_capacity_matrix",
]
