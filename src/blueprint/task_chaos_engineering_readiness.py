"""Analyze chaos engineering readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for chaos engineering concepts
_FAILURE_INJECTION_RE = re.compile(
    r"\b(?:failure[_\s]+injection|inject[_\s]+(?:failure|fault|errors?|latency)|"
    r"fault[_\s]+injection|error[_\s]+injection|"
    r"latency[_\s]+injection|network[_\s]+(?:partition|delay|failure|degradation)|"
    r"service[_\s]+(?:failure|outage|shutdown)|"
    r"resource[_\s]+(?:exhaustion|starvation|contention)|"
    r"disk[_\s]+(?:failure|full)|memory[_\s]+(?:leak|pressure)|"
    r"cpu[_\s]+(?:spike|throttle|exhaustion)|"
    r"simulate[_\s]+(?:failure|fault|outage|degradation|system[_\s]+degradation)|"
    r"chaos[_\s]+(?:experiment|test|scenario|injection)|"
    r"test[_\s]+failure[_\s]+injection|test_failure_injection)\b",
    re.I,
)
_STEADY_STATE_RE = re.compile(
    r"\b(?:steady[_\s-]*state|baseline[_\s]+(?:metric|measurement|behavior)|"
    r"normal[_\s]+(?:operation|behavior|state)|"
    r"expected[_\s]+(?:behavior|state|metric)|"
    r"health(?:y)?[_\s]+state|system[_\s]+health|"
    r"define[_\s]+(?:steady[_\s-]*state|baseline|normal[_\s]+behavior)|"
    r"steady[_\s-]*state[_\s]+(?:definition|hypothesis|metric)|"
    r"hypothesis[_\s]+(?:definition|validation)|"
    r"success[_\s]+(?:criteria|metric|condition)|"
    r"test[_\s]+steady[_\s]+state|test_steady_state)\b",
    re.I,
)
_BLAST_RADIUS_RE = re.compile(
    r"\b(?:blast[_\s]+radius|impact[_\s]+(?:scope|radius|boundary)|"
    r"failure[_\s]+(?:scope|boundary|containment)|"
    r"(?:limit|contain|control)[_\s]+(?:impact|blast|scope|failure)|"
    r"scope[_\s]+(?:of[_\s]+)?(?:failure|impact|experiment)|"
    r"isolation[_\s]+(?:boundary|zone|scope)|"
    r"rollback[_\s]+(?:plan|strategy|mechanism|procedure)|"
    r"circuit[_\s]+breaker|bulkhead[_\s]+pattern|"
    r"canary[_\s]+(?:deployment|release)|"
    r"incremental[_\s]+(?:rollout|deployment)|"
    r"test[_\s]+blast[_\s]+radius|test_blast_radius)\b",
    re.I,
)
_EXPERIMENT_DESIGN_RE = re.compile(
    r"\b(?:experiment[_\s]+(?:design|plan|procedure|protocol|methodology)|"
    r"chaos[_\s]+(?:experiment|engineering|testing)[_\s]+(?:design|plan|strategy|methodology)|"
    r"test[_\s]+(?:plan|procedure|protocol|methodology)[_\s]+(?:for[_\s]+)?chaos|"
    r"hypothesis[_\s-]*driven[_\s]+(?:experiment|testing|chaos)|"
    r"controlled[_\s]+(?:experiment|failure|chaos)|"
    r"experiment[_\s]+(?:control|variable|condition)|"
    r"a/?b[_\s]+test(?:ing)?[_\s]+(?:for[_\s]+)?chaos|"
    r"scientific[_\s]+method|experimental[_\s]+approach|"
    r"test[_\s]+experiment[_\s]+design|test_experiment_design)\b",
    re.I,
)
_SAFETY_CONTROLS_RE = re.compile(
    r"\b(?:safety[_\s]+(?:control|mechanism|measure|constraint|limit)|"
    r"production[_\s]+(?:safety|protection|guard|safeguard)|"
    r"emergency[_\s]+(?:stop|shutdown|abort|brake)|"
    r"kill[_\s]+switch|abort[_\s]+(?:mechanism|procedure|switch)|"
    r"automatic[_\s]+(?:rollback|recovery|shutdown)|"
    r"fail[_\s-]*safe|safety[_\s]+net|guardrails?|"
    r"pre[_\s-]*production[_\s]+(?:testing|validation)|"
    r"staging[_\s]+(?:environment|test)|"
    r"(?:no|without)[_\s]+safety[_\s]+controls?|"
    r"test[_\s]+safety[_\s]+controls|test_safety_controls)\b",
    re.I,
)
_OBSERVABILITY_RE = re.compile(
    r"\b(?:observability(?:[_\s]+(?:needed|required|coverage))?|monitor(?:ing)?|metric(?:s)?|telemetry|"
    r"logging|trace(?:s|ing)?|instrumentation|"
    r"alert(?:ing)?|alarm(?:s)?|notification(?:s)?|"
    r"dashboard(?:s)?|visualization|"
    r"service[_\s]+level[_\s]+(?:indicator|objective|agreement)|"
    r"sli|slo|sla|"
    r"health[_\s]+check|liveness[_\s]+probe|readiness[_\s]+probe|"
    r"detect[_\s]+(?:failure|anomaly|deviation)|"
    r"measure[_\s]+(?:impact|effect|outcome)|"
    r"test[_\s]+observability|test_observability)\b",
    re.I,
)
_ROLLBACK_MECHANISM_RE = re.compile(
    r"\b(?:rollback[_\s]+(?:mechanism|procedure|strategy|plan|capability)|"
    r"(?:revert|undo)[_\s]+(?:change|deployment|experiment)|"
    r"automatic[_\s]+(?:rollback|recovery|revert)|"
    r"manual[_\s]+(?:rollback|recovery|intervention)|"
    r"restore[_\s]+(?:service|system|state|baseline)|"
    r"recovery[_\s]+(?:procedure|plan|time|objective)|"
    r"rto|rpo|"
    r"failover|failback|"
    r"test[_\s]+rollback|test_rollback)\b",
    re.I,
)
_TEAM_PREPAREDNESS_RE = re.compile(
    r"\b(?:team[_\s]+(?:preparedness|readiness|training|capability)|"
    r"(?:train|training)[_\s]+(?:team|engineer|operator|sre)|"
    r"runbook(?:s)?|playbook(?:s)?|"
    r"incident[_\s]+(?:response|management|procedure)|"
    r"on[_\s-]*call[_\s]+(?:rotation|team|engineer)|"
    r"escalation[_\s]+(?:path|procedure|policy)|"
    r"game[_\s]+day|chaos[_\s]+game[_\s]+day|"
    r"fire[_\s]+drill|disaster[_\s]+recovery[_\s]+(?:drill|exercise)|"
    r"post[_\s-]*mortem|retrospective|"
    r"test[_\s]+team[_\s]+preparedness|test_team_preparedness)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class ChaosEngineeringReadiness:
    """Chaos engineering readiness analysis for a task."""

    failure_injection_planned: bool = False
    steady_state_defined: bool = False
    blast_radius_controlled: bool = False
    experiment_designed: bool = False
    safety_controls_implemented: bool = False
    observability_coverage: bool = False
    rollback_mechanism_ready: bool = False
    team_prepared: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        total_checks = 8
        passed_checks = sum([
            self.failure_injection_planned,
            self.steady_state_defined,
            self.blast_radius_controlled,
            self.experiment_designed,
            self.safety_controls_implemented,
            self.observability_coverage,
            self.rollback_mechanism_ready,
            self.team_prepared,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "failure_injection_planned": self.failure_injection_planned,
            "steady_state_defined": self.steady_state_defined,
            "blast_radius_controlled": self.blast_radius_controlled,
            "experiment_designed": self.experiment_designed,
            "safety_controls_implemented": self.safety_controls_implemented,
            "observability_coverage": self.observability_coverage,
            "rollback_mechanism_ready": self.rollback_mechanism_ready,
            "team_prepared": self.team_prepared,
            "readiness_score": self.readiness_score,
        }


def analyze_chaos_engineering_readiness(task_data: Mapping[str, Any]) -> ChaosEngineeringReadiness:
    """
    Analyze chaos engineering readiness from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        ChaosEngineeringReadiness with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return ChaosEngineeringReadiness()

    searchable_text = _extract_searchable_text(task_data)

    return ChaosEngineeringReadiness(
        failure_injection_planned=bool(_FAILURE_INJECTION_RE.search(searchable_text)),
        steady_state_defined=bool(_STEADY_STATE_RE.search(searchable_text)),
        blast_radius_controlled=bool(_BLAST_RADIUS_RE.search(searchable_text)),
        experiment_designed=bool(_EXPERIMENT_DESIGN_RE.search(searchable_text)),
        safety_controls_implemented=bool(_SAFETY_CONTROLS_RE.search(searchable_text)),
        observability_coverage=bool(_OBSERVABILITY_RE.search(searchable_text)),
        rollback_mechanism_ready=bool(_ROLLBACK_MECHANISM_RE.search(searchable_text)),
        team_prepared=bool(_TEAM_PREPAREDNESS_RE.search(searchable_text)),
    )


def _extract_searchable_text(task_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the task data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = task_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = task_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = task_data.get("validation_command") or task_data.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "ChaosEngineeringReadiness",
    "analyze_chaos_engineering_readiness",
]
