"""Generate phased rollout impact matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


RolloutPhaseType = Literal[
    "canary",
    "staged",
    "full_deployment",
    "blue_green",
    "feature_flag",
]
PhaseRiskLevel = Literal["high", "medium", "low"]
PhaseDependency = Literal[
    "infrastructure_readiness",
    "monitoring_requirements",
    "rollback_triggers",
    "health_checks",
    "traffic_routing",
]

_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")

# Phase type patterns
_CANARY_PHASE_RE = re.compile(
    r"\b(?:canary[_\s]+(?:deployment|release|rollout|phase)|"
    r"small[_\s]+percentage|initial[_\s]+rollout|"
    r"\d+%[_\s]+traffic|subset[_\s]+of[_\s]+users)\b",
    re.I,
)
_STAGED_PHASE_RE = re.compile(
    r"\b(?:staged?[_\s]+(?:deployment|release|rollout|phase)|"
    r"incremental[_\s]+rollout|gradual[_\s]+rollout|"
    r"phased?[_\s]+(?:deployment|rollout)|progressive[_\s]+rollout|"
    r"multi[_\s-]*phase|wave[_\s]+\d+)\b",
    re.I,
)
_FULL_DEPLOYMENT_RE = re.compile(
    r"\b(?:full[_\s]+(?:deployment|release|rollout)|"
    r"100%[_\s]+traffic|complete[_\s]+rollout|"
    r"final[_\s]+phase|general[_\s]+availability|GA)\b",
    re.I,
)
_BLUE_GREEN_RE = re.compile(
    r"\b(?:blue[_\s-]*green|blue/green|"
    r"green[_\s]+environment|swap[_\s]+environments?|"
    r"parallel[_\s]+environments?|zero[_\s-]*downtime)\b",
    re.I,
)
_FEATURE_FLAG_RE = re.compile(
    r"\b(?:feature[_\s]+flag|feature[_\s]+toggle|"
    r"flag[_\s]+rollout|toggle[_\s]+based|"
    r"gradual[_\s]+enablement|dark[_\s]+launch)\b",
    re.I,
)

# Dependency patterns
_INFRASTRUCTURE_RE = re.compile(
    r"\b(?:infrastructure[_\s]+(?:readiness|ready|prepared)|"
    r"infra[_\s]+check|capacity[_\s]+planning|"
    r"resource[_\s]+provisioning|scaling[_\s]+ready)\b",
    re.I,
)
_MONITORING_RE = re.compile(
    r"\b(?:monitoring[_\s]+(?:requirements|setup|configured)|"
    r"observability|metrics[_\s]+collection|"
    r"alerting[_\s]+(?:configured|setup)|dashboard|"
    r"slo|sli|health[_\s]+metrics)\b",
    re.I,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback[_\s]+(?:triggers?|plan|strategy|procedure)|"
    r"automatic[_\s]+rollback|rollback[_\s]+criteria|"
    r"revert[_\s]+plan|failure[_\s]+handling|abort[_\s]+criteria)\b",
    re.I,
)
_HEALTH_CHECK_RE = re.compile(
    r"\b(?:health[_\s]+check|healthcheck|liveness[_\s]+probe|"
    r"readiness[_\s]+probe|smoke[_\s]+test(?:s)?|"
    r"validation[_\s]+test(?:s)?|sanity[_\s]+check(?:s)?)\b",
    re.I,
)
_TRAFFIC_ROUTING_RE = re.compile(
    r"\b(?:traffic[_\s]+(?:routing|split|shifting)|"
    r"load[_\s]+balancer|routing[_\s]+rules?|"
    r"canary[_\s]+analysis|weighted[_\s]+routing)\b",
    re.I,
)

# Risk factor patterns
_HIGH_BLAST_RADIUS_RE = re.compile(
    r"\b(?:all[_\s]+users|entire[_\s]+(?:system|platform)|"
    r"critical[_\s]+(?:system|service)|production[_\s]+database|"
    r"core[_\s]+infrastructure|mission[_\s-]*critical)\b",
    re.I,
)
_COMPLEX_ROLLBACK_RE = re.compile(
    r"\b(?:complex[_\s]+rollback|difficult[_\s]+to[_\s]+revert|"
    r"database[_\s]+migration|schema[_\s]+change|"
    r"data[_\s]+migration|stateful[_\s]+change)\b",
    re.I,
)
_LIMITED_MONITORING_RE = re.compile(
    r"\b(?:limited[_\s]+monitoring|no[_\s]+monitoring|"
    r"insufficient[_\s]+(?:metrics|observability)|"
    r"manual[_\s]+verification|basic[_\s]+alerting)\b",
    re.I,
)

# Success criteria patterns
_SUCCESS_CRITERIA_RE = re.compile(
    r"\b(?:success[_\s]+(?:criteria|metrics?)|"
    r"acceptance[_\s]+criteria|validation[_\s]+criteria|"
    r"go/no[_\s-]*go[_\s]+criteria|phase[_\s]+gate)\b",
    re.I,
)

# Affected systems patterns
_AFFECTED_SYSTEM_RE = re.compile(
    r"\b(?:affects?|impacts?|involves?|depends[_\s]+on|"
    r"integrates?[_\s]+with)\s+([A-Za-z0-9_\- ]+(?:service|system|database|api|component))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PhasedRolloutPhase:
    """Individual phase in a phased rollout."""

    phase_name: str
    phase_type: RolloutPhaseType
    sequence_order: int
    dependencies: tuple[PhaseDependency, ...] = field(default_factory=tuple)
    success_criteria: tuple[str, ...] = field(default_factory=tuple)
    rollback_points: tuple[str, ...] = field(default_factory=tuple)
    affected_systems: tuple[str, ...] = field(default_factory=tuple)
    risk_level: PhaseRiskLevel = "medium"
    blast_radius_score: float = 0.5
    rollback_complexity_score: float = 0.5
    monitoring_coverage_score: float = 0.5

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "phase_name": self.phase_name,
            "phase_type": self.phase_type,
            "sequence_order": self.sequence_order,
            "dependencies": list(self.dependencies),
            "success_criteria": list(self.success_criteria),
            "rollback_points": list(self.rollback_points),
            "affected_systems": list(self.affected_systems),
            "risk_level": self.risk_level,
            "blast_radius_score": self.blast_radius_score,
            "rollback_complexity_score": self.rollback_complexity_score,
            "monitoring_coverage_score": self.monitoring_coverage_score,
        }


@dataclass(frozen=True, slots=True)
class PlanPhasedRolloutImpactMatrix:
    """Phased rollout impact matrix for an execution plan."""

    plan_id: str | None = None
    phases: tuple[PhasedRolloutPhase, ...] = field(default_factory=tuple)
    overall_risk_level: PhaseRiskLevel = "medium"
    recommendations: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "plan_id": self.plan_id,
            "phases": [phase.to_dict() for phase in self.phases],
            "overall_risk_level": self.overall_risk_level,
            "recommendations": list(self.recommendations),
            "summary": dict(self.summary),
        }

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Phased Rollout Impact Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"

        lines = [title, ""]

        if not self.phases:
            lines.append("No rollout phases detected in the plan.")
            return "\n".join(lines)

        # Summary section
        lines.extend([
            f"**Overall Risk Level**: {self.overall_risk_level}",
            f"**Total Phases**: {len(self.phases)}",
            "",
        ])

        # Phase details table
        lines.extend([
            "## Rollout Phases",
            "",
            "| Phase | Type | Risk | Blast Radius | Rollback Complexity | Monitoring | Dependencies | Success Criteria |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ])

        for phase in self.phases:
            lines.append(
                f"| {phase.sequence_order}. {_markdown_cell(phase.phase_name)} | "
                f"{phase.phase_type} | "
                f"{phase.risk_level} | "
                f"{phase.blast_radius_score:.2f} | "
                f"{phase.rollback_complexity_score:.2f} | "
                f"{phase.monitoring_coverage_score:.2f} | "
                f"{_markdown_cell(', '.join(phase.dependencies) or 'none')} | "
                f"{_markdown_cell('; '.join(phase.success_criteria) or 'none')} |"
            )

        # Recommendations section
        if self.recommendations:
            lines.extend([
                "",
                "## Recommendations",
                "",
            ])
            for rec in self.recommendations:
                lines.append(f"- {rec}")

        return "\n".join(lines)


def generate_plan_phased_rollout_impact_matrix(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask | object] | object,
) -> PlanPhasedRolloutImpactMatrix:
    """Generate a phased rollout impact matrix from an execution plan."""
    plan_id, tasks = _source_payload(source)

    # Extract phases from all tasks
    all_phases: list[PhasedRolloutPhase] = []

    for index, task in enumerate(tasks, start=1):
        phases = _extract_phases_from_task(task, index)
        all_phases.extend(phases)

    # If no explicit phases found, infer a basic structure if deployment-related
    if not all_phases:
        inferred_phase = _infer_basic_phase_from_tasks(tasks)
        if inferred_phase:
            all_phases.append(inferred_phase)

    # Sort phases by sequence order
    sorted_phases = tuple(sorted(all_phases, key=lambda p: p.sequence_order))

    # Calculate overall risk
    overall_risk = _calculate_overall_risk(sorted_phases)

    # Generate recommendations
    recommendations = _generate_recommendations(sorted_phases)

    # Build summary
    summary = _build_summary(sorted_phases)

    return PlanPhasedRolloutImpactMatrix(
        plan_id=plan_id,
        phases=sorted_phases,
        overall_risk_level=overall_risk,
        recommendations=recommendations,
        summary=summary,
    )


def _extract_phases_from_task(task: Mapping[str, Any], task_index: int) -> list[PhasedRolloutPhase]:
    """Extract rollout phases from a single task."""
    phases: list[PhasedRolloutPhase] = []
    texts = _candidate_texts(task)
    combined_text = " ".join(text for _, text in texts)

    # Detect phase types
    phase_types: list[tuple[RolloutPhaseType, str]] = []

    if _CANARY_PHASE_RE.search(combined_text):
        phase_types.append(("canary", "Canary Deployment"))
    if _STAGED_PHASE_RE.search(combined_text):
        phase_types.append(("staged", "Staged Rollout"))
    if _BLUE_GREEN_RE.search(combined_text):
        phase_types.append(("blue_green", "Blue-Green Deployment"))
    if _FEATURE_FLAG_RE.search(combined_text):
        phase_types.append(("feature_flag", "Feature Flag Rollout"))
    if _FULL_DEPLOYMENT_RE.search(combined_text):
        phase_types.append(("full_deployment", "Full Deployment"))

    # Extract dependencies
    dependencies = _extract_dependencies(combined_text)

    # Extract success criteria
    success_criteria = _extract_success_criteria(task)

    # Extract rollback points
    rollback_points = _extract_rollback_points(combined_text)

    # Extract affected systems
    affected_systems = _extract_affected_systems(combined_text)

    # Calculate risk scores
    blast_radius = _calculate_blast_radius(combined_text, affected_systems)
    rollback_complexity = _calculate_rollback_complexity(combined_text)
    monitoring_coverage = _calculate_monitoring_coverage(combined_text)

    # Create phases
    for seq, (phase_type, phase_name) in enumerate(phase_types, start=1):
        risk_level = _calculate_phase_risk(blast_radius, rollback_complexity, monitoring_coverage)

        phase = PhasedRolloutPhase(
            phase_name=phase_name,
            phase_type=phase_type,
            sequence_order=seq,
            dependencies=dependencies,
            success_criteria=success_criteria,
            rollback_points=rollback_points,
            affected_systems=affected_systems,
            risk_level=risk_level,
            blast_radius_score=blast_radius,
            rollback_complexity_score=rollback_complexity,
            monitoring_coverage_score=monitoring_coverage,
        )
        phases.append(phase)

    return phases


def _extract_dependencies(text: str) -> tuple[PhaseDependency, ...]:
    """Extract phase dependencies from text."""
    dependencies: set[PhaseDependency] = set()

    if _INFRASTRUCTURE_RE.search(text):
        dependencies.add("infrastructure_readiness")
    if _MONITORING_RE.search(text):
        dependencies.add("monitoring_requirements")
    if _ROLLBACK_RE.search(text):
        dependencies.add("rollback_triggers")
    if _HEALTH_CHECK_RE.search(text):
        dependencies.add("health_checks")
    if _TRAFFIC_ROUTING_RE.search(text):
        dependencies.add("traffic_routing")

    # Sort for deterministic output
    return tuple(sorted(dependencies))


def _extract_success_criteria(task: Mapping[str, Any]) -> tuple[str, ...]:
    """Extract success criteria from task data."""
    criteria: list[str] = []

    # Check acceptance_criteria field
    ac = task.get("acceptance_criteria")
    if isinstance(ac, (list, tuple)):
        for item in ac:
            if isinstance(item, str) and item.strip():
                criteria.append(item.strip())
    elif isinstance(ac, str) and ac.strip():
        criteria.append(ac.strip())

    # Check definition_of_done field
    dod = task.get("definition_of_done")
    if isinstance(dod, (list, tuple)):
        for item in dod:
            if isinstance(item, str) and item.strip():
                criteria.append(item.strip())
    elif isinstance(dod, str) and dod.strip():
        criteria.append(dod.strip())

    return tuple(_dedupe(criteria))


def _extract_rollback_points(text: str) -> tuple[str, ...]:
    """Extract rollback points from text."""
    rollback_points: list[str] = []

    # Look for specific rollback mentions
    if re.search(r"\bautomatic[_\s]+rollback\b", text, re.I):
        rollback_points.append("Automatic rollback on failure")
    if re.search(r"\bmanual[_\s]+rollback\b", text, re.I):
        rollback_points.append("Manual rollback procedure")
    if re.search(r"\brollback[_\s]+(?:criteria|triggers?)\b", text, re.I):
        rollback_points.append("Defined rollback criteria")

    # Check for general rollback mentions if no specific points found
    if not rollback_points and _ROLLBACK_RE.search(text):
        rollback_points.append("Rollback capability available")

    return tuple(rollback_points)


def _extract_affected_systems(text: str) -> tuple[str, ...]:
    """Extract affected systems from text."""
    systems: list[str] = []

    # Find system mentions with explicit patterns
    for match in _AFFECTED_SYSTEM_RE.finditer(text):
        system = match.group(1).strip()
        if system:
            systems.append(system)

    # Also look for common system types mentioned directly
    system_keywords = [
        ("database", "Database"),
        ("api", "API"),
        ("service", "Service"),
        ("frontend", "Frontend"),
        ("backend", "Backend"),
        ("load balancer", "Load Balancer"),
        ("cache", "Cache"),
        ("queue", "Queue"),
        ("worker", "Worker"),
        ("payment", "Payment System"),
        ("notification", "Notification System"),
        ("user", "User System"),
    ]

    for keyword, display_name in system_keywords:
        # Look for the keyword with word boundaries
        pattern = re.compile(rf"\b{re.escape(keyword)}\b", re.I)
        if pattern.search(text):
            # Check if we haven't already added a similar system
            if not any(keyword in s.lower() for s in systems):
                systems.append(display_name)

    return tuple(_dedupe(systems))


def _calculate_blast_radius(text: str, affected_systems: tuple[str, ...]) -> float:
    """Calculate blast radius score (0.0 = low impact, 1.0 = high impact)."""
    score = 0.3  # Base score

    # High blast radius indicators
    if _HIGH_BLAST_RADIUS_RE.search(text):
        score += 0.4

    # More affected systems = higher blast radius
    if len(affected_systems) >= 5:
        score += 0.3
    elif len(affected_systems) >= 3:
        score += 0.2
    elif len(affected_systems) >= 1:
        score += 0.1

    return min(1.0, score)


def _calculate_rollback_complexity(text: str) -> float:
    """Calculate rollback complexity score (0.0 = easy, 1.0 = complex)."""
    score = 0.3  # Base score

    # Complex rollback indicators
    if _COMPLEX_ROLLBACK_RE.search(text):
        score += 0.4

    # Automatic rollback reduces complexity
    if re.search(r"\bautomatic[_\s]+rollback\b", text, re.I):
        score -= 0.2

    # Rollback plan reduces complexity
    if _ROLLBACK_RE.search(text):
        score -= 0.1

    return max(0.0, min(1.0, score))


def _calculate_monitoring_coverage(text: str) -> float:
    """Calculate monitoring coverage score (0.0 = poor, 1.0 = excellent)."""
    score = 0.3  # Base score

    # Good monitoring indicators
    if _MONITORING_RE.search(text):
        score += 0.3
    if _HEALTH_CHECK_RE.search(text):
        score += 0.2
    if re.search(r"\b(?:dashboard|metrics|slo|sli)\b", text, re.I):
        score += 0.2

    # Poor monitoring indicators
    if _LIMITED_MONITORING_RE.search(text):
        score -= 0.3

    return max(0.0, min(1.0, score))


def _calculate_phase_risk(
    blast_radius: float,
    rollback_complexity: float,
    monitoring_coverage: float,
) -> PhaseRiskLevel:
    """Calculate overall phase risk level."""
    # Risk is high if blast radius is high AND (rollback is complex OR monitoring is poor)
    if blast_radius > 0.7 and (rollback_complexity > 0.6 or monitoring_coverage < 0.4):
        return "high"

    # Risk is low if blast radius is low AND rollback is simple AND monitoring is good
    if blast_radius < 0.4 and rollback_complexity < 0.4 and monitoring_coverage > 0.6:
        return "low"

    return "medium"


def _calculate_overall_risk(phases: tuple[PhasedRolloutPhase, ...]) -> PhaseRiskLevel:
    """Calculate overall risk level for all phases."""
    if not phases:
        return "low"

    # If any phase is high risk, overall is high
    if any(p.risk_level == "high" for p in phases):
        return "high"

    # If all phases are low risk, overall is low
    if all(p.risk_level == "low" for p in phases):
        return "low"

    return "medium"


def _generate_recommendations(phases: tuple[PhasedRolloutPhase, ...]) -> tuple[str, ...]:
    """Generate recommendations based on phase analysis."""
    recommendations: list[str] = []

    if not phases:
        return tuple(recommendations)

    # Check for missing dependencies
    all_dependencies = set()
    for phase in phases:
        all_dependencies.update(phase.dependencies)

    if "monitoring_requirements" not in all_dependencies:
        recommendations.append("Add comprehensive monitoring and alerting for all rollout phases")

    if "rollback_triggers" not in all_dependencies:
        recommendations.append("Define clear rollback triggers and automated rollback procedures")

    if "health_checks" not in all_dependencies:
        recommendations.append("Implement health checks and validation tests for each phase")

    # Check for high-risk phases
    high_risk_phases = [p for p in phases if p.risk_level == "high"]
    if high_risk_phases:
        recommendations.append(
            f"Review and mitigate risks for {len(high_risk_phases)} high-risk phase(s)"
        )

    # Check for phases without success criteria
    phases_without_criteria = [p for p in phases if not p.success_criteria]
    if phases_without_criteria:
        recommendations.append(
            f"Define success criteria for {len(phases_without_criteria)} phase(s)"
        )

    # Check monitoring coverage
    low_monitoring_phases = [p for p in phases if p.monitoring_coverage_score < 0.4]
    if low_monitoring_phases:
        recommendations.append(
            f"Improve monitoring coverage for {len(low_monitoring_phases)} phase(s)"
        )

    # Phase ordering recommendation
    if len(phases) > 1:
        # Check if canary comes before full deployment
        canary_phases = [p for p in phases if p.phase_type == "canary"]
        full_phases = [p for p in phases if p.phase_type == "full_deployment"]

        if canary_phases and full_phases:
            if canary_phases[0].sequence_order > full_phases[0].sequence_order:
                recommendations.append(
                    "Consider deploying canary phase before full deployment for risk mitigation"
                )

    return tuple(recommendations)


def _build_summary(phases: tuple[PhasedRolloutPhase, ...]) -> dict[str, Any]:
    """Build summary statistics for phases."""
    if not phases:
        return {
            "total_phases": 0,
            "phase_types": {},
            "risk_distribution": {"high": 0, "medium": 0, "low": 0},
            "avg_blast_radius": 0.0,
            "avg_rollback_complexity": 0.0,
            "avg_monitoring_coverage": 0.0,
        }

    # Count phase types
    phase_types: dict[str, int] = {}
    for phase in phases:
        phase_types[phase.phase_type] = phase_types.get(phase.phase_type, 0) + 1

    # Count risk distribution
    risk_distribution = {
        "high": sum(1 for p in phases if p.risk_level == "high"),
        "medium": sum(1 for p in phases if p.risk_level == "medium"),
        "low": sum(1 for p in phases if p.risk_level == "low"),
    }

    # Calculate averages
    avg_blast_radius = sum(p.blast_radius_score for p in phases) / len(phases)
    avg_rollback_complexity = sum(p.rollback_complexity_score for p in phases) / len(phases)
    avg_monitoring_coverage = sum(p.monitoring_coverage_score for p in phases) / len(phases)

    return {
        "total_phases": len(phases),
        "phase_types": phase_types,
        "risk_distribution": risk_distribution,
        "avg_blast_radius": round(avg_blast_radius, 2),
        "avg_rollback_complexity": round(avg_rollback_complexity, 2),
        "avg_monitoring_coverage": round(avg_monitoring_coverage, 2),
    }


def _infer_basic_phase_from_tasks(tasks: list[dict[str, Any]]) -> PhasedRolloutPhase | None:
    """Infer a basic phase structure if deployment-related tasks are found."""
    # Combine all task texts
    all_texts: list[str] = []
    for task in tasks:
        texts = _candidate_texts(task)
        all_texts.extend(text for _, text in texts)

    combined_text = " ".join(all_texts)

    # Extract all attributes from combined tasks
    dependencies = _extract_dependencies(combined_text)

    # Check if deployment-related OR if we have deployment-related dependencies
    deployment_pattern = re.compile(
        r"\b(?:deploy|deployment|release|rollout|launch|ship)\b", re.I
    )

    # If we have traffic routing or other rollout-specific dependencies, infer a canary phase
    has_rollout_dependencies = any(
        dep in dependencies
        for dep in ("traffic_routing", "rollback_triggers", "monitoring_requirements")
    )

    is_deployment = deployment_pattern.search(combined_text)

    if not is_deployment and not has_rollout_dependencies:
        return None

    # Extract success criteria from all tasks
    all_success_criteria: list[str] = []
    for task in tasks:
        criteria = _extract_success_criteria(task)
        all_success_criteria.extend(criteria)

    rollback_points = _extract_rollback_points(combined_text)
    affected_systems = _extract_affected_systems(combined_text)

    # Calculate risk scores from combined text
    blast_radius = _calculate_blast_radius(combined_text, affected_systems)
    rollback_complexity = _calculate_rollback_complexity(combined_text)
    monitoring_coverage = _calculate_monitoring_coverage(combined_text)

    risk_level = _calculate_phase_risk(blast_radius, rollback_complexity, monitoring_coverage)

    # Choose phase type based on context
    if "traffic_routing" in dependencies:
        phase_name = "Canary Deployment"
        phase_type: RolloutPhaseType = "canary"
    else:
        phase_name = "Standard Deployment"
        phase_type = "staged"

    return PhasedRolloutPhase(
        phase_name=phase_name,
        phase_type=phase_type,
        sequence_order=1,
        dependencies=dependencies,
        success_criteria=tuple(_dedupe(all_success_criteria)),
        rollback_points=rollback_points,
        affected_systems=affected_systems,
        risk_level=risk_level,
        blast_radius_score=blast_radius,
        rollback_complexity_score=rollback_complexity,
        monitoring_coverage_score=monitoring_coverage,
    )


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask | object] | object,
) -> tuple[str | None, list[dict[str, Any]]]:
    """Extract plan ID and tasks from various source types."""
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]

    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    if _looks_like_task(source):
        return None, [_object_payload(source)]

    # Try to iterate
    try:
        iterator = iter(source)  # type: ignore[arg-type]
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
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))

    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    """Extract plan payload as dictionary."""
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
    """Extract task payloads as dictionaries."""
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
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _looks_like_plan(value: object) -> bool:
    """Check if object looks like a plan."""
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    """Check if object looks like a task."""
    return not isinstance(value, (str, bytes)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    """Extract object attributes as dictionary."""
    fields = (
        "id", "title", "description", "milestone", "owner_type",
        "suggested_engine", "depends_on", "files_or_modules", "files",
        "acceptance_criteria", "definition_of_done", "estimated_complexity",
        "estimated_hours", "risk_level", "test_command", "status",
        "metadata", "blocked_reason", "tags", "labels", "notes", "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    """Extract candidate texts from task for analysis."""
    texts: list[tuple[str, str]] = []

    for field_name in (
        "title", "description", "milestone", "owner_type",
        "suggested_engine", "risk_level", "test_command", "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))

    for field_name in (
        "depends_on", "files_or_modules", "files",
        "acceptance_criteria", "definition_of_done",
        "tags", "labels", "notes",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))

    texts.extend(_metadata_texts(task.get("metadata")))

    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    """Extract texts from metadata structure."""
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.append((field, key_text))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            else:
                texts.append((field, key_text))
        return texts

    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
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
    """Extract strings from various value types."""
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
    """Extract optional text from value."""
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    """Extract text from value."""
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    """Format value for markdown table cell."""
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    """Remove duplicates while preserving order."""
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "PhasedRolloutPhase",
    "PlanPhasedRolloutImpactMatrix",
    "RolloutPhaseType",
    "PhaseRiskLevel",
    "PhaseDependency",
    "generate_plan_phased_rollout_impact_matrix",
]
