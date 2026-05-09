"""Extract workflow and state machine requirements from source brief data."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for workflow concepts
_STATE_DEFINITIONS_RE = re.compile(
    r"\b(?:state[_\s]+(?:definitions?|machine|model|diagram|relationships?)|"
    r"define[_\s]+states?|workflow[_\s]+states?|"
    r"status[_\s]+(?:values?|enum|states?)|"
    r"state[_\s]+(?:enum|values?|list)|"
    r"(?:draft|pending|approved|rejected|completed|active|inactive|suspended)[_\s]+state|"
    r"(?:nested|sub)[_\s-]*workflows?|parent[_\s-]*child[_\s]+(?:workflow|state)|"
    r"fsm|finite[_\s]+state[_\s]+machine)\b",
    re.I,
)
_TRANSITION_RULES_RE = re.compile(
    r"\b(?:transition[_\s]+(?:rules?|logic|validation|guards?)|"
    r"state[_\s]+transition|valid[_\s]+transitions?|"
    r"allowed[_\s]+transitions?|transition[_\s]+(?:from|to)|"
    r"state[_\s]+change|move[_\s]+(?:from|to)[_\s]+state|"
    r"transition[_\s]+(?:conditions?|requirements?)|"
    r"can[_\s]+transition|state[_\s]+flow)\b",
    re.I,
)
_WORKFLOW_TRIGGERS_RE = re.compile(
    r"\b(?:trigger|event[_\s]+(?:triggers?|driven|handlers?|listeners?)|"
    r"workflow[_\s]+(?:triggers?|events?)|"
    r"state[_\s]+(?:triggers?|events?)|"
    r"transition[_\s]+(?:triggers?|events?)|"
    r"(?:on|upon|when)[_\s]+(?:event|trigger|action)|"
    r"event[_\s-]*driven)\b",
    re.I,
)
_WORKFLOW_CONDITIONS_RE = re.compile(
    r"\b(?:transition[_\s]+(?:conditions?|guards?|rules?)|"
    r"workflow[_\s]+conditions?|state[_\s]+(?:guards?|conditions?)|"
    r"(?:if|when|unless)[_\s]+(?:condition|criteria)|"
    r"conditional[_\s]+(?:transition|workflow)|"
    r"guard[_\s]+(?:clause|condition|expression|expressions?)|"
    r"(?:evaluate|evaluating)[_\s]+guard|"
    r"validation[_\s]+(?:rules?|logic)[_\s]+(?:for|before)[_\s]+transition)\b",
    re.I,
)
_WORKFLOW_ACTORS_RE = re.compile(
    r"\b(?:workflow[_\s]+(?:actors?|roles?|participants?)|"
    r"state[_\s]+(?:owner|assignee)|"
    r"role[_\s-]*based[_\s]+(?:workflow|transitions?)|"
    r"actor[_\s]+(?:permissions?|authorization)|"
    r"(?:who|which[_\s]+role)[_\s]+can[_\s]+(?:approve|reject|transition)|"
    r"approval[_\s]+(?:chain|workflow|hierarchy))\b",
    re.I,
)
_STATE_PERSISTENCE_RE = re.compile(
    r"\b(?:state[_\s]+(?:persistence|storage|saving|tracking)|"
    r"persist[_\s]+(?:state|workflow)|"
    r"save[_\s]+(?:state|workflow[_\s]+state)|"
    r"workflow[_\s]+(?:history|log|audit[_\s]+trail)|"
    r"state[_\s]+(?:history|changelog|audit)|"
    r"track(?:ing)?[_\s]+(?:workflow[_\s]+)?state[_\s]+changes?)\b",
    re.I,
)
_CONCURRENT_TRANSITIONS_RE = re.compile(
    r"\b(?:concurrent[_\s]+transitions?|parallel[_\s]+transitions?|"
    r"simultaneous[_\s]+(?:transitions?|state[_\s]+changes?)|"
    r"race[_\s]+conditions?|optimistic[_\s]+locking|"
    r"pessimistic[_\s]+locking|lock[_\s]+(?:state|workflow)|"
    r"conflict[_\s]+(?:resolution|detection|handling))\b",
    re.I,
)
_ROLLBACK_SCENARIOS_RE = re.compile(
    r"\b(?:rollback|revert[_\s]+(?:state|transition)|"
    r"undo[_\s]+(?:transition|state[_\s]+change)|"
    r"compensation|compensat(?:e|ed|ing)[_\s]+(?:transactions?|actions?|logic)|"
    r"fallback[_\s]+(?:state|logic)|"
    r"error[_\s]+(?:recovery|rollback|handling)[_\s]+(?:for|in)[_\s]+workflow)\b",
    re.I,
)
_WORKFLOW_VERSIONING_RE = re.compile(
    r"\b(?:workflow[_\s]+(?:versioning|version|evolution)|"
    r"state[_\s]+machine[_\s]+version|"
    r"migrate[_\s]+(?:workflow|state[_\s]+machine)|"
    r"backward[_\s]+compatib(?:le|ility)[_\s]+(?:workflow|state)|"
    r"workflow[_\s]+schema[_\s]+(?:migration|evolution))\b",
    re.I,
)
_WORKFLOW_AUDIT_RE = re.compile(
    r"\b(?:workflow[_\s]+(?:audit|monitoring|observability)|"
    r"audit[_\s]+trail|state[_\s]+(?:audit|tracking|monitoring)|"
    r"transition[_\s]+(?:log|history|audit)|"
    r"workflow[_\s]+(?:events?|telemetry)|"
    r"track[_\s]+(?:workflow|state[_\s]+changes?|transitions?))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class WorkflowRequirements:
    """Workflow and state machine requirements extracted from source brief."""

    state_definitions_specified: bool = False
    transition_rules_defined: bool = False
    workflow_triggers_identified: bool = False
    workflow_conditions_specified: bool = False
    workflow_actors_defined: bool = False
    state_persistence_addressed: bool = False
    concurrent_transitions_handled: bool = False
    rollback_scenarios_planned: bool = False
    workflow_versioning_considered: bool = False
    workflow_audit_included: bool = False

    @property
    def completeness_score(self) -> float:
        """Calculate completeness score (0.0 to 1.0)."""
        total_checks = 10
        passed_checks = sum([
            self.state_definitions_specified,
            self.transition_rules_defined,
            self.workflow_triggers_identified,
            self.workflow_conditions_specified,
            self.workflow_actors_defined,
            self.state_persistence_addressed,
            self.concurrent_transitions_handled,
            self.rollback_scenarios_planned,
            self.workflow_versioning_considered,
            self.workflow_audit_included,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "state_definitions_specified": self.state_definitions_specified,
            "transition_rules_defined": self.transition_rules_defined,
            "workflow_triggers_identified": self.workflow_triggers_identified,
            "workflow_conditions_specified": self.workflow_conditions_specified,
            "workflow_actors_defined": self.workflow_actors_defined,
            "state_persistence_addressed": self.state_persistence_addressed,
            "concurrent_transitions_handled": self.concurrent_transitions_handled,
            "rollback_scenarios_planned": self.rollback_scenarios_planned,
            "workflow_versioning_considered": self.workflow_versioning_considered,
            "workflow_audit_included": self.workflow_audit_included,
            "completeness_score": self.completeness_score,
        }


def extract_workflow_requirements(source_data: Mapping[str, Any]) -> WorkflowRequirements:
    """
    Extract workflow requirements from source brief data.

    Args:
        source_data: A mapping containing source brief information with fields like
                    'title', 'description', 'requirements', etc.

    Returns:
        WorkflowRequirements with boolean flags for each aspect and overall score.
    """
    if not isinstance(source_data, Mapping):
        return WorkflowRequirements()

    searchable_text = _extract_searchable_text(source_data)

    return WorkflowRequirements(
        state_definitions_specified=bool(_STATE_DEFINITIONS_RE.search(searchable_text)),
        transition_rules_defined=bool(_TRANSITION_RULES_RE.search(searchable_text)),
        workflow_triggers_identified=bool(_WORKFLOW_TRIGGERS_RE.search(searchable_text)),
        workflow_conditions_specified=bool(_WORKFLOW_CONDITIONS_RE.search(searchable_text)),
        workflow_actors_defined=bool(_WORKFLOW_ACTORS_RE.search(searchable_text)),
        state_persistence_addressed=bool(_STATE_PERSISTENCE_RE.search(searchable_text)),
        concurrent_transitions_handled=bool(_CONCURRENT_TRANSITIONS_RE.search(searchable_text)),
        rollback_scenarios_planned=bool(_ROLLBACK_SCENARIOS_RE.search(searchable_text)),
        workflow_versioning_considered=bool(_WORKFLOW_VERSIONING_RE.search(searchable_text)),
        workflow_audit_included=bool(_WORKFLOW_AUDIT_RE.search(searchable_text)),
    )


def _extract_searchable_text(source_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the source data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "summary", "rationale"):
        value = source_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("requirements", "acceptance_criteria", "constraints", "notes", "definition_of_done"):
        value = source_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "WorkflowRequirements",
    "extract_workflow_requirements",
]
