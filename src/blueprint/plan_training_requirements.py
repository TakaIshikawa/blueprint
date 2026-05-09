"""Generate training requirements matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for training requirements concepts
_TARGET_AUDIENCES_RE = re.compile(
    r"\b(?:target[_\s]+audience(?:s)?|training[_\s]+audience|"
    r"(?:train|training[_\s]+for)[_\s]+(?:developers?|users?|admins?|operators?|stakeholders?)|"
    r"audience[_\s]+(?:segments?|groups?)|"
    r"(?:end[_\s-]*users?|developers?|admins?|operators?)[_\s]+training)\b",
    re.I,
)
_TRAINING_CONTENT_RE = re.compile(
    r"\b(?:training[_\s]+(?:content|material(?:s)?|modules?|curriculum|program)|"
    r"(?:training|course)[_\s]+(?:material(?:s)?|content|modules?)|"
    r"training[_\s]+(?:topics?|subjects?|areas?)|"
    r"learning[_\s]+(?:material(?:s)?|content|objectives?))\b",
    re.I,
)
_DELIVERY_METHODS_RE = re.compile(
    r"\b(?:delivery[_\s]+method(?:s)?|training[_\s]+(?:delivery|format)|"
    r"(?:online|in[_\s-]*person|virtual|remote|hands[_\s-]*on)[_\s]+training|"
    r"(?:workshop|webinar|video|self[_\s-]*paced|instructor[_\s-]*led)[_\s]+training|"
    r"training[_\s]+(?:sessions?|workshops?))\b",
    re.I,
)
_CERTIFICATION_RE = re.compile(
    r"\b(?:certification|certif(?:y|ied)|"
    r"training[_\s]+certification|"
    r"(?:complete|pass)[_\s]+certification|"
    r"certification[_\s]+(?:requirement(?:s)?|program|test))\b",
    re.I,
)
_KNOWLEDGE_VALIDATION_RE = re.compile(
    r"\b(?:knowledge[_\s]+validation|validate[_\s]+(?:knowledge|learning)|"
    r"(?:quiz|test|exam|assessment)[_\s]+(?:knowledge|learning)|"
    r"training[_\s]+(?:assessment|evaluation|test)|"
    r"(?:verify|check)[_\s]+(?:knowledge|understanding|competency))\b",
    re.I,
)
_TRAINING_GAPS_RE = re.compile(
    r"\b(?:training[_\s]+gap(?:s)?|(?:skill|knowledge)[_\s]+gap(?:s)?|"
    r"(?:missing|lacking)[_\s]+(?:training|knowledge|skills?)|"
    r"identify[_\s]+(?:training[_\s]+)?gap(?:s)?|"
    r"gap[_\s]+analysis)\b",
    re.I,
)
_HANDS_ON_PRACTICE_RE = re.compile(
    r"\b(?:hands[_\s-]*on[_\s]+(?:training|practice|exercise(?:s)?|lab(?:s)?)|"
    r"practical[_\s]+(?:training|exercise(?:s)?|experience)|"
    r"lab[_\s]+(?:exercise(?:s)?|session(?:s)?)|"
    r"practice[_\s]+(?:exercise(?:s)?|session(?:s)?))\b",
    re.I,
)
_FOLLOW_UP_RE = re.compile(
    r"\b(?:follow[_\s-]*up[_\s]+(?:training|session(?:s)?)|"
    r"(?:refresher|ongoing|continuous)[_\s]+training|"
    r"post[_\s-]*training[_\s]+(?:support|follow[_\s-]*up)|"
    r"training[_\s]+(?:reinforcement|refresher))\b",
    re.I,
)
_TRAINING_SCHEDULE_RE = re.compile(
    r"\b(?:training[_\s]+(?:schedule|timeline|calendar|plan)|"
    r"(?:schedule|plan)[_\s]+training|"
    r"training[_\s]+(?:delivery|session)[_\s]+schedule|"
    r"when[_\s]+to[_\s]+(?:train|deliver[_\s]+training))\b",
    re.I,
)
_SUCCESS_METRICS_RE = re.compile(
    r"\b(?:training[_\s]+(?:success|effectiveness)[_\s]+metric(?:s)?|"
    r"measure[_\s]+training[_\s]+(?:success|effectiveness|impact)|"
    r"training[_\s]+(?:kpi(?:s)?|outcome(?:s)?|result(?:s)?)|"
    r"(?:track|monitor)[_\s]+training[_\s]+(?:progress|completion|success))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TrainingRequirements:
    """Training requirements extracted from plan data."""

    target_audiences_identified: bool = False
    training_content_defined: bool = False
    delivery_methods_specified: bool = False
    certification_required: bool = False
    knowledge_validation_planned: bool = False
    training_gaps_identified: bool = False
    hands_on_practice_included: bool = False
    follow_up_planned: bool = False
    training_schedule_defined: bool = False
    success_metrics_established: bool = False

    @property
    def completeness_score(self) -> float:
        """Calculate completeness score (0.0 to 1.0)."""
        total_checks = 10
        passed_checks = sum([
            self.target_audiences_identified,
            self.training_content_defined,
            self.delivery_methods_specified,
            self.certification_required,
            self.knowledge_validation_planned,
            self.training_gaps_identified,
            self.hands_on_practice_included,
            self.follow_up_planned,
            self.training_schedule_defined,
            self.success_metrics_established,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "target_audiences_identified": self.target_audiences_identified,
            "training_content_defined": self.training_content_defined,
            "delivery_methods_specified": self.delivery_methods_specified,
            "certification_required": self.certification_required,
            "knowledge_validation_planned": self.knowledge_validation_planned,
            "training_gaps_identified": self.training_gaps_identified,
            "hands_on_practice_included": self.hands_on_practice_included,
            "follow_up_planned": self.follow_up_planned,
            "training_schedule_defined": self.training_schedule_defined,
            "success_metrics_established": self.success_metrics_established,
            "completeness_score": self.completeness_score,
        }


def generate_training_requirements(plan_data: Mapping[str, Any]) -> TrainingRequirements:
    """
    Generate training requirements from plan data.

    Args:
        plan_data: A mapping containing plan information with fields like
                  'title', 'description', 'requirements', etc.

    Returns:
        TrainingRequirements with boolean flags for each aspect and overall score.
    """
    if not isinstance(plan_data, Mapping):
        return TrainingRequirements()

    searchable_text = _extract_searchable_text(plan_data)

    return TrainingRequirements(
        target_audiences_identified=bool(_TARGET_AUDIENCES_RE.search(searchable_text)),
        training_content_defined=bool(_TRAINING_CONTENT_RE.search(searchable_text)),
        delivery_methods_specified=bool(_DELIVERY_METHODS_RE.search(searchable_text)),
        certification_required=bool(_CERTIFICATION_RE.search(searchable_text)),
        knowledge_validation_planned=bool(_KNOWLEDGE_VALIDATION_RE.search(searchable_text)),
        training_gaps_identified=bool(_TRAINING_GAPS_RE.search(searchable_text)),
        hands_on_practice_included=bool(_HANDS_ON_PRACTICE_RE.search(searchable_text)),
        follow_up_planned=bool(_FOLLOW_UP_RE.search(searchable_text)),
        training_schedule_defined=bool(_TRAINING_SCHEDULE_RE.search(searchable_text)),
        success_metrics_established=bool(_SUCCESS_METRICS_RE.search(searchable_text)),
    )


def _extract_searchable_text(plan_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the plan data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale", "context", "handoff_prompt"):
        value = plan_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("requirements", "acceptance_criteria", "notes", "milestones", "objectives"):
        value = plan_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "TrainingRequirements",
    "generate_training_requirements",
]
