"""Generate feature flag management matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FeatureFlagManagementScore = Literal["comprehensive", "partial", "minimal"]

_SCORE_ORDER: dict[FeatureFlagManagementScore, int] = {"minimal": 0, "partial": 1, "comprehensive": 2}

# Pattern matching for feature flag management signals
_FLAG_INVENTORY_RE = re.compile(
    r"\b(?:flag\s+inventory|feature\s+flag\s+(?:catalog|registry|list)|"
    r"flag\s+(?:catalog|registry|tracking)|inventory\s+(?:of\s+)?flags?|"
    r"flag\s+documentation|document\s+flags?)\b",
    re.I,
)
_OWNERSHIP_ASSIGNMENTS_RE = re.compile(
    r"\b(?:ownership\s+assignments?|flag\s+owner(?:ship)?|owner\s+(?:of\s+)?(?:the\s+)?flag|"
    r"assign\s+owner|flag\s+(?:responsibility|assignee)|"
    r"(?:team|dri)\s+(?:owns?|responsible\s+for)\s+flag|"
    r"flag\s+(?:team|lead|maintainer))\b",
    re.I,
)
_CLEANUP_SCHEDULES_RE = re.compile(
    r"\b(?:cleanup\s+schedule|flag\s+cleanup|remove\s+flag|"
    r"flag\s+removal|flag\s+(?:retirement|sunset|decommission)|"
    r"schedule\s+(?:flag\s+)?cleanup|cleanup\s+(?:plan|timeline)|"
    r"flag\s+lifecycle|flag\s+expir(?:y|ation))\b",
    re.I,
)
_ROLLOUT_STRATEGIES_RE = re.compile(
    r"\b(?:rollout\s+strateg(?:y|ies)|gradual\s+rollout|"
    r"phased\s+rollout|rollout\s+plan|canary\s+rollout|"
    r"percentage\s+rollout|feature\s+rollout|staged\s+rollout|"
    r"incremental\s+rollout|rollout\s+approach)\b",
    re.I,
)
_TARGETING_RULES_RE = re.compile(
    r"\b(?:targeting\s+rules?|target\s+(?:users?|cohorts?|segments?)|"
    r"user\s+targeting|cohort\s+targeting|segment\s+targeting|"
    r"targeting\s+criteria|flag\s+targeting|allowlist|denylist|"
    r"user\s+(?:segment|cohort)s?\s+for\s+flag)\b",
    re.I,
)
_FLAG_TRACKING_RE = re.compile(
    r"\b(?:flag\s+tracking|track\s+flags?|flag\s+monitor(?:ing)?|"
    r"flag\s+usage|flag\s+metrics|flag\s+analytics|"
    r"flag\s+telemetry|flag\s+observability)\b",
    re.I,
)
_OWNER_ASSIGNMENT_RE = re.compile(
    r"\b(?:assign\s+owner|owner\s+assignment|ownership\s+model|"
    r"assign\s+(?:responsibility|team)|dri\s+assignment|"
    r"flag\s+ownership\s+(?:structure|model))\b",
    re.I,
)
_CLEANUP_SCHEDULING_RE = re.compile(
    r"\b(?:cleanup\s+scheduling|schedule\s+cleanup|cleanup\s+timeline|"
    r"removal\s+schedule|retirement\s+schedule|"
    r"flag\s+(?:lifecycle|ttl|expiration)\s+policy)\b",
    re.I,
)
_GRADUAL_ROLLOUT_RE = re.compile(
    r"\b(?:gradual\s+rollout|progressive\s+rollout|incremental\s+(?:rollout|deployment)|"
    r"percentage\s+based|traffic\s+ramp(?:ing)?|ramp\s+up|"
    r"slow\s+rollout|controlled\s+rollout)\b",
    re.I,
)
_TARGETING_CONFIGURATION_RE = re.compile(
    r"\b(?:targeting\s+config(?:uration)?|configure\s+targeting|"
    r"targeting\s+setup|set(?:up)?\s+(?:targeting|rules)|"
    r"define\s+(?:targeting|cohorts?|segments?))\b",
    re.I,
)
_DEPENDENCY_MAPPING_RE = re.compile(
    r"\b(?:flag\s+dependenc(?:y|ies)|dependent\s+flags?|"
    r"flag\s+(?:dependency|relationship)\s+(?:map|graph)|"
    r"flag\s+interaction|cascading\s+flags?|"
    r"flag\s+(?:coupling|interdependenc(?:y|ies)))\b",
    re.I,
)
_KILL_SWITCH_SETUP_RE = re.compile(
    r"\b(?:kill[- ]switch|emergency\s+(?:disable|off)|"
    r"instant\s+disable|circuit\s+breaker|emergency\s+rollback|"
    r"quick\s+disable|emergency\s+shutoff|panic\s+button)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class FeatureFlagManagementMatrixRow:
    """Feature flag management signals for one execution task."""

    task_id: str
    title: str
    flag_inventory: str = "missing"
    ownership_assignments: str = "missing"
    cleanup_schedules: str = "missing"
    rollout_strategies: str = "missing"
    targeting_rules: str = "missing"
    flag_tracking: str = "missing"
    owner_assignment: str = "missing"
    cleanup_scheduling: str = "missing"
    gradual_rollout: str = "missing"
    targeting_configuration: str = "missing"
    dependency_mapping: str = "missing"
    kill_switch_setup: str = "missing"
    management_score: FeatureFlagManagementScore = "minimal"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "flag_inventory": self.flag_inventory,
            "ownership_assignments": self.ownership_assignments,
            "cleanup_schedules": self.cleanup_schedules,
            "rollout_strategies": self.rollout_strategies,
            "targeting_rules": self.targeting_rules,
            "flag_tracking": self.flag_tracking,
            "owner_assignment": self.owner_assignment,
            "cleanup_scheduling": self.cleanup_scheduling,
            "gradual_rollout": self.gradual_rollout,
            "targeting_configuration": self.targeting_configuration,
            "dependency_mapping": self.dependency_mapping,
            "kill_switch_setup": self.kill_switch_setup,
            "management_score": self.management_score,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class FeatureFlagManagementMatrix:
    """Feature flag management readiness matrix for an execution plan."""

    plan_id: str | None = None
    rows: tuple[FeatureFlagManagementMatrixRow, ...] = field(default_factory=tuple)
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
        title = "# Feature Flag Management Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"

        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Tasks analyzed: {self.summary.get('task_count', 0)}",
            f"- Comprehensive management: {self.summary.get('comprehensive_count', 0)}",
            f"- Partial management: {self.summary.get('partial_count', 0)}",
            f"- Minimal management: {self.summary.get('minimal_count', 0)}",
            f"- Overall coverage: {self.summary.get('overall_coverage', 0)}%",
        ]

        if not self.rows:
            lines.extend(["", "No feature flag management signals detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Matrix",
                "",
                "| Task | Inventory | Ownership | Cleanup | Rollout | Targeting | Tracking | "
                "Owner Assignment | Cleanup Scheduling | Gradual Rollout | Targeting Config | "
                "Dependencies | Kill Switch | Score |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )

        for row in self.rows:
            lines.append(
                f"| {_markdown_cell(row.title)} | "
                f"{_markdown_cell(row.flag_inventory)} | "
                f"{_markdown_cell(row.ownership_assignments)} | "
                f"{_markdown_cell(row.cleanup_schedules)} | "
                f"{_markdown_cell(row.rollout_strategies)} | "
                f"{_markdown_cell(row.targeting_rules)} | "
                f"{_markdown_cell(row.flag_tracking)} | "
                f"{_markdown_cell(row.owner_assignment)} | "
                f"{_markdown_cell(row.cleanup_scheduling)} | "
                f"{_markdown_cell(row.gradual_rollout)} | "
                f"{_markdown_cell(row.targeting_configuration)} | "
                f"{_markdown_cell(row.dependency_mapping)} | "
                f"{_markdown_cell(row.kill_switch_setup)} | "
                f"{row.management_score} |"
            )

        return "\n".join(lines)


def generate_feature_flag_management_matrix(
    plan: ExecutionPlan | Mapping[str, Any] | str,
) -> FeatureFlagManagementMatrix:
    """Generate feature flag management matrix from execution plan."""
    plan_id, tasks = _extract_plan_data(plan)
    rows = tuple(_analyze_task(task) for task in tasks)
    summary = _calculate_summary(rows)

    return FeatureFlagManagementMatrix(
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


def _analyze_task(task: Mapping[str, Any]) -> FeatureFlagManagementMatrixRow:
    """Analyze feature flag management signals in a task."""
    task_id = str(task.get("id", "unknown"))
    title = str(task.get("title", "Untitled"))

    text = _extract_searchable_text(task)
    evidence_list: list[str] = []

    flag_inventory = _check_signal(_FLAG_INVENTORY_RE, text, evidence_list)
    ownership_assignments = _check_signal(_OWNERSHIP_ASSIGNMENTS_RE, text, evidence_list)
    cleanup_schedules = _check_signal(_CLEANUP_SCHEDULES_RE, text, evidence_list)
    rollout_strategies = _check_signal(_ROLLOUT_STRATEGIES_RE, text, evidence_list)
    targeting_rules = _check_signal(_TARGETING_RULES_RE, text, evidence_list)
    flag_tracking = _check_signal(_FLAG_TRACKING_RE, text, evidence_list)
    owner_assignment = _check_signal(_OWNER_ASSIGNMENT_RE, text, evidence_list)
    cleanup_scheduling = _check_signal(_CLEANUP_SCHEDULING_RE, text, evidence_list)
    gradual_rollout = _check_signal(_GRADUAL_ROLLOUT_RE, text, evidence_list)
    targeting_configuration = _check_signal(_TARGETING_CONFIGURATION_RE, text, evidence_list)
    dependency_mapping = _check_signal(_DEPENDENCY_MAPPING_RE, text, evidence_list)
    kill_switch_setup = _check_signal(_KILL_SWITCH_SETUP_RE, text, evidence_list)

    present_count = sum(
        1
        for signal in [
            flag_inventory,
            ownership_assignments,
            cleanup_schedules,
            rollout_strategies,
            targeting_rules,
            flag_tracking,
            owner_assignment,
            cleanup_scheduling,
            gradual_rollout,
            targeting_configuration,
            dependency_mapping,
            kill_switch_setup,
        ]
        if signal == "present"
    )

    if present_count >= 8:
        management_score: FeatureFlagManagementScore = "comprehensive"
    elif present_count >= 4:
        management_score = "partial"
    else:
        management_score = "minimal"

    return FeatureFlagManagementMatrixRow(
        task_id=task_id,
        title=title,
        flag_inventory=flag_inventory,
        ownership_assignments=ownership_assignments,
        cleanup_schedules=cleanup_schedules,
        rollout_strategies=rollout_strategies,
        targeting_rules=targeting_rules,
        flag_tracking=flag_tracking,
        owner_assignment=owner_assignment,
        cleanup_scheduling=cleanup_scheduling,
        gradual_rollout=gradual_rollout,
        targeting_configuration=targeting_configuration,
        dependency_mapping=dependency_mapping,
        kill_switch_setup=kill_switch_setup,
        management_score=management_score,
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


def _calculate_summary(rows: tuple[FeatureFlagManagementMatrixRow, ...]) -> dict[str, Any]:
    """Calculate summary statistics for the matrix."""
    if not rows:
        return {
            "task_count": 0,
            "comprehensive_count": 0,
            "partial_count": 0,
            "minimal_count": 0,
            "overall_coverage": 0,
        }

    score_counts = {"comprehensive": 0, "partial": 0, "minimal": 0}
    for row in rows:
        score_counts[row.management_score] += 1

    overall_coverage = int((score_counts["comprehensive"] + score_counts["partial"] * 0.5) / len(rows) * 100)

    return {
        "task_count": len(rows),
        "comprehensive_count": score_counts["comprehensive"],
        "partial_count": score_counts["partial"],
        "minimal_count": score_counts["minimal"],
        "overall_coverage": overall_coverage,
    }


def _markdown_cell(text: str) -> str:
    """Escape text for Markdown table cells."""
    return text.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "FeatureFlagManagementMatrixRow",
    "FeatureFlagManagementMatrix",
    "generate_feature_flag_management_matrix",
]
