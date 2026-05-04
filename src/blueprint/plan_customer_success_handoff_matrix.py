"""Build plan-level customer success handoff matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_secrets_rotation_readiness_matrix import (
    _candidate_texts,
    _dedupe,
    _evidence_snippet,
    _markdown_cell,
    _optional_text,
    _source_payload,
    _task_id,
)


CustomerSegment = Literal["enterprise", "smb", "individual", "free_tier", "trial", "all_customers"]
HandoffTrigger = Literal["launch", "migration", "onboarding", "rollout", "sunset", "support_transition"]
HandoffOwner = Literal["csm", "support", "sales", "product", "engineering", "documentation", "unassigned"]

_SEGMENT_ORDER: tuple[CustomerSegment, ...] = (
    "trial",
    "free_tier",
    "enterprise",
    "smb",
    "individual",
    "all_customers",
)
_TRIGGER_ORDER: tuple[HandoffTrigger, ...] = (
    "sunset",
    "rollout",
    "migration",
    "onboarding",
    "support_transition",
    "launch",
)

_SEGMENT_PATTERNS: dict[CustomerSegment, re.Pattern[str]] = {
    "enterprise": re.compile(
        r"\b(?:enterprise|large account|strategic account|fortune 500|white glove|dedicated support)\b",
        re.I,
    ),
    "smb": re.compile(
        r"\b(?:smb|small business|medium business|small[- ]medium|mid[- ]market|startup)\b",
        re.I,
    ),
    "individual": re.compile(
        r"\b(?:individual|personal|pro user|professional|freelancer|self[- ]serve)\b",
        re.I,
    ),
    "free_tier": re.compile(
        r"\b(?:free tier|free plan|freemium|community|basic plan)\b",
        re.I,
    ),
    "trial": re.compile(
        r"\b(?:trial user|trial|pilot|beta|early access|preview)\b",
        re.I,
    ),
    "all_customers": re.compile(
        r"\b(?:all customers|all users|everyone|every plan|all tiers|all segments)\b",
        re.I,
    ),
}

_TRIGGER_PATTERNS: dict[HandoffTrigger, re.Pattern[str]] = {
    "sunset": re.compile(
        r"\b(?:sunset|deprecat(?:e|ion)|end[- ]of[- ]life|eol|retire|phase out|decommission)\b",
        re.I,
    ),
    "rollout": re.compile(
        r"\b(?:phased|gradual|canary|enterprise rollout|staged deployment)\b",
        re.I,
    ),
    "migration": re.compile(
        r"\b(?:migrat(?:e|ion|ing))\b",
        re.I,
    ),
    "onboarding": re.compile(
        r"\b(?:onboard(?:ing)?|getting started|initial configuration|welcome|activation)\b",
        re.I,
    ),
    "support_transition": re.compile(
        r"\b(?:support transition|tier change|support model|escalation|handoff to support)\b",
        re.I,
    ),
    "launch": re.compile(
        r"\b(?:launch|release|ga|general availability|go[- ]live|new feature|feature flag|rollout)\b",
        re.I,
    ),
}

_OWNER_PATTERNS: dict[HandoffOwner, re.Pattern[str]] = {
    "csm": re.compile(
        r"\b(?:csm|customer success|success manager|account manager|tam|technical account manager)\b",
        re.I,
    ),
    "support": re.compile(
        r"\b(?:support|help desk|support team|tier.{0,10}support|support engineer)\b",
        re.I,
    ),
    "sales": re.compile(
        r"\b(?:sales|account executive|ae|sales engineer|pre[- ]sales)\b",
        re.I,
    ),
    "product": re.compile(
        r"\b(?:product|product manager|pm|product team)\b",
        re.I,
    ),
    "engineering": re.compile(
        r"\b(?:engineering|eng|developer|development team)\b",
        re.I,
    ),
    "documentation": re.compile(
        r"\b(?:documentation|docs|technical writer|devrel|developer relations)\b",
        re.I,
    ),
}

_CUSTOMER_ARTIFACT_PATTERNS = re.compile(
    r"\b(?:guide|documentation|runbook|playbook|faq|help article|knowledge base|tutorial|"
    r"video|webinar|training|communication|email|announcement|blog post|release note|changelog)\b",
    re.I,
)

_READINESS_GAP_PATTERNS = re.compile(
    r"\b(?:missing|gap|incomplete|draft|todo|tbd|needs|require|pending|"
    r"not ready|no (?:guide|docs|plan|training)|lack)\b",
    re.I,
)

_CUSTOMER_SUCCESS_RE = re.compile(
    r"\b(?:customer|user|client|account|onboard|migration|launch|rollout|training|"
    r"adoption|success|support|help|guide|documentation)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PlanCustomerSuccessHandoffRow:
    """Customer success handoff signals for one task."""

    task_id: str
    title: str
    segment: CustomerSegment
    trigger: HandoffTrigger
    owner: HandoffOwner
    customer_artifact: str
    gap: str
    recommended_action: str
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "segment": self.segment,
            "trigger": self.trigger,
            "owner": self.owner,
            "customer_artifact": self.customer_artifact,
            "gap": self.gap,
            "recommended_action": self.recommended_action,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanCustomerSuccessHandoffMatrix:
    """Plan-level customer success handoff matrix."""

    plan_id: str | None = None
    rows: tuple[PlanCustomerSuccessHandoffRow, ...] = field(default_factory=tuple)
    handoff_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_handoff_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanCustomerSuccessHandoffRow, ...]:
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "handoff_task_ids": list(self.handoff_task_ids),
            "no_handoff_task_ids": list(self.no_handoff_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        title = "# Plan Customer Success Handoff Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        if not self.rows:
            return "\n".join([title, "", "No customer success handoff rows were inferred."])
        lines = [
            title,
            "",
            "| Task | Segment | Trigger | Owner | Customer Artifact | Gap | Recommended Action |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | {row.segment} | {row.trigger} | "
                f"{row.owner} | {_markdown_cell(row.customer_artifact)} | "
                f"{_markdown_cell(row.gap)} | {_markdown_cell(row.recommended_action)} |"
            )
        return "\n".join(lines)


def build_plan_customer_success_handoff_matrix(source: Any) -> PlanCustomerSuccessHandoffMatrix:
    plan_id, tasks = _source_payload(source)
    rows: list[PlanCustomerSuccessHandoffRow] = []
    no_handoff_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index)
        if row:
            rows.append(row)
        else:
            no_handoff_task_ids.append(_task_id(task, index))
    result = tuple(rows)
    return PlanCustomerSuccessHandoffMatrix(
        plan_id=plan_id,
        rows=result,
        handoff_task_ids=tuple(row.task_id for row in result),
        no_handoff_task_ids=tuple(no_handoff_task_ids),
        summary=_summary(len(tasks), result),
    )


def generate_plan_customer_success_handoff_matrix(source: Any) -> PlanCustomerSuccessHandoffMatrix:
    return build_plan_customer_success_handoff_matrix(source)


def analyze_plan_customer_success_handoff_matrix(source: Any) -> PlanCustomerSuccessHandoffMatrix:
    if isinstance(source, PlanCustomerSuccessHandoffMatrix):
        return source
    return build_plan_customer_success_handoff_matrix(source)


def derive_plan_customer_success_handoff_matrix(source: Any) -> PlanCustomerSuccessHandoffMatrix:
    return analyze_plan_customer_success_handoff_matrix(source)


def extract_plan_customer_success_handoff_matrix(source: Any) -> PlanCustomerSuccessHandoffMatrix:
    return derive_plan_customer_success_handoff_matrix(source)


def summarize_plan_customer_success_handoff_matrix(
    source: PlanCustomerSuccessHandoffMatrix | Iterable[PlanCustomerSuccessHandoffRow] | Any,
) -> dict[str, Any] | PlanCustomerSuccessHandoffMatrix:
    if isinstance(source, PlanCustomerSuccessHandoffMatrix):
        return dict(source.summary)
    if isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)) or hasattr(source, "tasks") or hasattr(source, "title"):
        return build_plan_customer_success_handoff_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows)


def plan_customer_success_handoff_matrix_to_dict(matrix: PlanCustomerSuccessHandoffMatrix) -> dict[str, Any]:
    return matrix.to_dict()


plan_customer_success_handoff_matrix_to_dict.__test__ = False


def plan_customer_success_handoff_matrix_to_dicts(
    matrix: PlanCustomerSuccessHandoffMatrix | Iterable[PlanCustomerSuccessHandoffRow],
) -> list[dict[str, Any]]:
    if isinstance(matrix, PlanCustomerSuccessHandoffMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_customer_success_handoff_matrix_to_dicts.__test__ = False


def plan_customer_success_handoff_matrix_to_markdown(matrix: PlanCustomerSuccessHandoffMatrix) -> str:
    return matrix.to_markdown()


plan_customer_success_handoff_matrix_to_markdown.__test__ = False


def _task_row(task: Mapping[str, Any], index: int) -> PlanCustomerSuccessHandoffRow | None:
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)

    # Must have customer success handoff signals
    if not _CUSTOMER_SUCCESS_RE.search(context):
        return None

    # Detect segment
    segment = _detect_segment(texts)
    if not segment:
        return None

    # Detect trigger
    trigger = _detect_trigger(texts)
    if not trigger:
        return None

    # Detect owner
    owner = _detect_owner(texts)

    # Detect customer artifact and gap
    customer_artifact = _detect_customer_artifact(texts)
    gap = _detect_gap(texts, customer_artifact)

    # Generate recommended action
    recommended_action = _generate_recommended_action(segment, trigger, owner, gap)

    return PlanCustomerSuccessHandoffRow(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        segment=segment,
        trigger=trigger,
        owner=owner,
        customer_artifact=customer_artifact,
        gap=gap,
        recommended_action=recommended_action,
        evidence=tuple(
            _dedupe(
                _evidence_snippet(field, text)
                for field, text in texts
                if _CUSTOMER_SUCCESS_RE.search(text)
            )
        ),
    )


def _detect_segment(texts: Iterable[tuple[str, str]]) -> CustomerSegment | None:
    context = " ".join(text for _, text in texts)
    for segment in _SEGMENT_ORDER:
        if _SEGMENT_PATTERNS[segment].search(context):
            return segment
    return None


def _detect_trigger(texts: Iterable[tuple[str, str]]) -> HandoffTrigger | None:
    context = " ".join(text for _, text in texts)
    for trigger in _TRIGGER_ORDER:
        if _TRIGGER_PATTERNS[trigger].search(context):
            return trigger
    return None


def _detect_owner(texts: Iterable[tuple[str, str]]) -> HandoffOwner:
    context = " ".join(text for _, text in texts)
    for owner, pattern in _OWNER_PATTERNS.items():
        if pattern.search(context):
            return owner
    return "unassigned"


def _detect_customer_artifact(texts: Iterable[tuple[str, str]]) -> str:
    context = " ".join(text for _, text in texts)

    # Try to find specific artifacts, prioritizing more specific ones
    specific_patterns = [
        (r"\b(?:migration guide|onboarding guide|getting started guide|runbook|playbook)\b", re.I),
        (r"\b(?:guide|documentation|runbook|playbook|faq|help article)\b", re.I),
        (r"\b(?:tutorial|video|webinar|training)\b", re.I),
        (r"\b(?:email|announcement|communication)\b", re.I),
        (r"\b(?:blog post|release note|changelog)\b", re.I),
    ]

    for pattern_str, flags in specific_patterns:
        pattern = re.compile(pattern_str, flags)
        match = pattern.search(context)
        if match:
            return match.group(0).strip().title()

    # Fallback to general pattern
    match = _CUSTOMER_ARTIFACT_PATTERNS.search(context)
    if match:
        return match.group(0).strip().title()
    return "none"


def _detect_gap(texts: Iterable[tuple[str, str]], customer_artifact: str) -> str:
    context = " ".join(text for _, text in texts)

    if customer_artifact == "none":
        return "No customer-facing artifact identified"

    if _READINESS_GAP_PATTERNS.search(context):
        # Try to extract what's missing
        for pattern in [
            re.compile(r"(?:missing|no|lack|needs?)\s+(\w+(?:\s+\w+){0,3})", re.I),
            re.compile(r"(\w+(?:\s+\w+){0,3})\s+(?:not ready|incomplete|draft|tbd)", re.I),
        ]:
            match = pattern.search(context)
            if match:
                gap_text = match.group(1).strip()
                if len(gap_text) > 50:
                    gap_text = gap_text[:47] + "..."
                return f"Missing or incomplete: {gap_text}"
        return "Readiness gap detected"

    return "none"


def _generate_recommended_action(
    segment: CustomerSegment,
    trigger: HandoffTrigger,
    owner: HandoffOwner,
    gap: str,
) -> str:
    if gap != "none":
        if owner == "unassigned":
            return f"Assign owner and complete customer-facing materials for {segment} {trigger}."
        return f"Complete customer-facing materials and prepare {owner} for {segment} {trigger}."

    if owner == "unassigned":
        return f"Assign {segment} {trigger} owner and verify readiness."

    return f"Coordinate with {owner} to ensure {segment} {trigger} readiness."


def _summary(task_count: int, rows: Iterable[PlanCustomerSuccessHandoffRow]) -> dict[str, Any]:
    row_list = list(rows)

    segment_counts: dict[CustomerSegment, int] = {segment: 0 for segment in _SEGMENT_ORDER}
    trigger_counts: dict[HandoffTrigger, int] = {trigger: 0 for trigger in _TRIGGER_ORDER}
    owner_counts: dict[str, int] = {}
    gap_count = 0

    for row in row_list:
        segment_counts[row.segment] = segment_counts.get(row.segment, 0) + 1
        trigger_counts[row.trigger] = trigger_counts.get(row.trigger, 0) + 1
        owner_counts[row.owner] = owner_counts.get(row.owner, 0) + 1
        if row.gap != "none":
            gap_count += 1

    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "handoff_task_count": len(row_list),
        "no_handoff_task_count": task_count - len(row_list),
        "segment_counts": segment_counts,
        "trigger_counts": trigger_counts,
        "owner_counts": owner_counts,
        "tasks_with_gaps": gap_count,
        "tasks_without_gaps": len(row_list) - gap_count,
    }
