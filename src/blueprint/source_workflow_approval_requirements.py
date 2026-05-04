"""Extract source-level workflow approval and review requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


WorkflowApprovalSurface = Literal[
    "manager_approval",
    "admin_review",
    "legal_signoff",
    "security_review",
    "finance_approval",
    "multi_step_approval",
    "escalation",
    "rejection_reason",
    "approval_sla",
]
WorkflowApprovalMissingDetail = Literal["missing_approver_role", "missing_approval_sla"]
WorkflowApprovalConfidence = Literal["high", "medium", "low"]

_SURFACE_ORDER: tuple[WorkflowApprovalSurface, ...] = (
    "manager_approval",
    "admin_review",
    "legal_signoff",
    "security_review",
    "finance_approval",
    "multi_step_approval",
    "escalation",
    "rejection_reason",
    "approval_sla",
)
_CONFIDENCE_ORDER: dict[WorkflowApprovalConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_APPROVAL_CONTEXT_RE = re.compile(
    r"\b(?:approval|approve|review|sign-?off|sign off|"
    r"authorization|authorize|permission|consent|"
    r"escalat|reject|veto|accept|"
    r"workflow|process|gate|checkpoint|"
    r"manager|admin|legal|security|finance|owner|reviewer)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:approval|review|workflow|process|authorization|"
    r"escalation|rejection|sign-?off|"
    r"requirements?|acceptance|done)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"provide|include|submit|obtain|get|request|"
    r"before|prior to|after|once|"
    r"acceptance|done when)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:approval|review|sign-?off|authorization|workflow|escalation)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:approval|review|sign-?off|authorization|workflow|escalation)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_APPROVAL_RE = re.compile(
    r"\b(?:no approval|no review|no sign-?off|no authorization|"
    r"no workflow|no escalation|"
    r"approval is out of scope|self-service(?:d)?|"
    r"(?:fully )?automat(?:ic|ed)(?:\s+approval)?(?!\s+escalation))\b",
    re.I,
)
_APPROVER_ROLE_RE = re.compile(
    r"\b(?:manager|supervisor|lead|director|"
    r"admin|administrator|owner|"
    r"legal|compliance|counsel|"
    r"security|infosec|"
    r"finance|accounting|controller|"
    r"reviewer|approver)\b",
    re.I,
)
_SLA_RE = re.compile(
    r"\b(?:sla|service level|turnaround time|"
    r"within \d+|"
    r"\d+\s*(?:hour|day|business day|week)|"
    r"response time|approval time)\b",
    re.I,
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
    "id",
    "source_id",
    "source_brief_id",
    "status",
    "created_by",
    "updated_by",
    "owner",
    "last_editor",
}
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "scope",
    "non_goals",
    "assumptions",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "workflow",
    "approval",
    "review",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_SURFACE_PATTERNS: dict[WorkflowApprovalSurface, re.Pattern[str]] = {
    "manager_approval": re.compile(
        r"\b(?:manager(?:'s)? (?:approval|review|approve|sign-?off)|"
        r"supervisor (?:approval|review|approve)|"
        r"director (?:approval|approve|sign-?off)|"
        r"lead (?:approval|approve)|team lead (?:review|approve)|"
        r"managerial approval|management approval)\b",
        re.I,
    ),
    "admin_review": re.compile(
        r"\b(?:admin(?:istrator)? (?:approval|review|sign-?off)|"
        r"admin check|administrative approval|"
        r"owner approval|owner review)\b",
        re.I,
    ),
    "legal_signoff": re.compile(
        r"\b(?:legal (?:approval|review|sign-?off|signoff)|"
        r"legal check|legal team review|"
        r"compliance (?:approval|review)|"
        r"(?:legal )?counsel (?:approval|signoff|sign-?off)|general counsel)\b",
        re.I,
    ),
    "security_review": re.compile(
        r"\b(?:security (?:approval|review|reviews|sign-?off)|"
        r"security (?:check|team review|team reviews)|"
        r"infosec (?:approval|review)|"
        r"security clearance|security audit)\b",
        re.I,
    ),
    "finance_approval": re.compile(
        r"\b(?:finance (?:approval|review|sign-?off)|"
        r"financial approval|accounting approval|"
        r"controller approval|budget approval|"
        r"cost approval|expense approval)\b",
        re.I,
    ),
    "multi_step_approval": re.compile(
        r"\b(?:multi[- ]step approval|multiple approval|"
        r"approval (?:chain|workflow|process|pipeline)|"
        r"two[- ]stage approval|sequential approval|"
        r"tiered approval|staged approval|"
        r"approval hierarchy)\b",
        re.I,
    ),
    "escalation": re.compile(
        r"\b(?:escalat(?:e|ion|ed|ing)|escalation (?:path|process|workflow)|"
        r"escalate to|escalated to|"
        r"raise to|elevate to|"
        r"next level approval|higher level|"
        r"automatic escalation|auto escalation)\b",
        re.I,
    ),
    "rejection_reason": re.compile(
        r"\b(?:reject(?:ion)? reason|rejection (?:message|comment|note)|"
        r"deny reason|denial reason|"
        r"veto reason|disapproval reason|"
        r"reason for (?:rejection|denial)|"
        r"rejection explanation)\b",
        re.I,
    ),
    "approval_sla": re.compile(
        r"\b(?:approval (?:sla|time|timeline|turnaround)|"
        r"review (?:sla|time|timeline|turnaround)|"
        r"approval within|review within|approve within|"
        r"respond within|response time|"
        r"(?:completed|done) within \d+|"
        r"\d+\s*(?:hour|day|business day|week)|"
        r"approval deadline)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[WorkflowApprovalSurface, tuple[str, ...]] = {
    "manager_approval": ("product_manager", "engineering_manager", "backend"),
    "admin_review": ("backend", "platform", "product_manager"),
    "legal_signoff": ("legal", "compliance", "product_manager"),
    "security_review": ("security", "infosec", "platform"),
    "finance_approval": ("finance", "accounting", "product_manager"),
    "multi_step_approval": ("backend", "platform", "product_manager"),
    "escalation": ("backend", "platform", "product_manager"),
    "rejection_reason": ("backend", "frontend", "ux"),
    "approval_sla": ("backend", "platform", "product_manager"),
}
_PLANNING_NOTES: dict[WorkflowApprovalSurface, tuple[str, ...]] = {
    "manager_approval": ("Define manager approval trigger, notification flow, approval UI, and manager identification logic.",),
    "admin_review": ("Specify admin role definition, review criteria, approval permissions, and admin notification process.",),
    "legal_signoff": ("Document legal review triggers, required documentation, legal team routing, and signoff tracking.",),
    "security_review": ("Plan security review checklist, security team assignment, review SLA, and approval criteria.",),
    "finance_approval": ("Define finance approval thresholds, budget validation, finance team routing, and cost tracking.",),
    "multi_step_approval": ("Design approval workflow stages, sequential vs parallel approval, state tracking, and notification cascade.",),
    "escalation": ("Specify escalation triggers, escalation path, timeout handling, and escalation notification.",),
    "rejection_reason": ("Plan rejection UI, required reason fields, rejection tracking, and rejection notification flow.",),
    "approval_sla": ("Set approval SLA target, SLA monitoring, timeout actions, and SLA breach notification.",),
}
_GAP_MESSAGES: dict[WorkflowApprovalMissingDetail, str] = {
    "missing_approver_role": "Specify approver role or actor responsible for approval decision.",
    "missing_approval_sla": "Define approval turnaround time, SLA, or response time expectation.",
}


@dataclass(frozen=True, slots=True)
class SourceWorkflowApprovalRequirement:
    """One source-backed workflow approval requirement."""

    surface: WorkflowApprovalSurface
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: WorkflowApprovalConfidence = "medium"
    approver: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> WorkflowApprovalSurface:
        """Compatibility view for extractors that expose requirement_category."""
        return self.surface

    @property
    def concern(self) -> WorkflowApprovalSurface:
        """Compatibility view for extractors that expose concern naming."""
        return self.surface

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        """Compatibility view matching adjacent source extractors."""
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "surface": self.surface,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "approver": self.approver,
            "suggested_owners": list(self.suggested_owners),
            "planning_notes": list(self.planning_notes),
            "gap_messages": list(self.gap_messages),
        }


@dataclass(frozen=True, slots=True)
class SourceWorkflowApprovalRequirementsReport:
    """Source-level workflow approval requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceWorkflowApprovalRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceWorkflowApprovalRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceWorkflowApprovalRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return workflow approval requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Workflow Approval Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        surface_counts = self.summary.get("surface_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Surface counts: "
            + ", ".join(f"{surface} {surface_counts.get(surface, 0)}" for surface in _SURFACE_ORDER),
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source workflow approval requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
            ]
        )
        for req in self.requirements:
            lines.extend(
                [
                    f"### {req.surface}",
                    "",
                    f"- Source field: `{req.source_field}`",
                    f"- Confidence: {req.confidence}",
                ]
            )
            if req.approver:
                lines.append(f"- Approver: {req.approver}")
            if req.evidence:
                lines.extend(["- Evidence:", *[f"  - {ev}" for ev in req.evidence]])
            if req.suggested_owners:
                lines.append(f"- Suggested owners: {', '.join(req.suggested_owners)}")
            if req.planning_notes:
                lines.extend(["- Planning notes:", *[f"  - {note}" for note in req.planning_notes]])
            if req.gap_messages:
                lines.extend(["- Gaps:", *[f"  - {gap}" for gap in req.gap_messages]])
            lines.append("")
        return "\n".join(lines)


def extract_source_workflow_approval_requirements(
    brief: SourceBrief | ImplementationBrief | Mapping[str, Any] | str | object,
) -> SourceWorkflowApprovalRequirementsReport:
    """Extract source workflow approval requirements from a source or implementation brief."""
    brief_id, title, payload = _brief_payload(brief)
    if _has_negated_scope(payload):
        return SourceWorkflowApprovalRequirementsReport(
            brief_id=brief_id,
            title=title,
            requirements=tuple(),
            summary=_empty_summary(),
        )

    requirements: list[SourceWorkflowApprovalRequirement] = []
    seen_surfaces: set[WorkflowApprovalSurface] = set()

    for surface in _SURFACE_ORDER:
        if surface in seen_surfaces:
            continue
        matches = _find_surface_matches(payload, surface)
        if not matches:
            continue
        seen_surfaces.add(surface)
        evidence, source_field, confidence, approver = _best_match(matches, surface)
        gaps = _detect_gaps(payload, surface, approver)
        requirements.append(
            SourceWorkflowApprovalRequirement(
                surface=surface,
                source_field=source_field,
                evidence=evidence,
                confidence=confidence,
                approver=approver,
                suggested_owners=_OWNER_SUGGESTIONS.get(surface, tuple()),
                planning_notes=_PLANNING_NOTES.get(surface, tuple()),
                gap_messages=tuple(_GAP_MESSAGES[g] for g in gaps),
            )
        )

    return SourceWorkflowApprovalRequirementsReport(
        brief_id=brief_id,
        title=title,
        requirements=tuple(requirements),
        summary=_compute_summary(requirements),
    )


def _brief_payload(
    brief: SourceBrief | ImplementationBrief | Mapping[str, Any] | str | object,
) -> tuple[str | None, str | None, dict[str, Any]]:
    if isinstance(brief, (SourceBrief, ImplementationBrief)):
        return brief.id, getattr(brief, "title", None), dict(brief.model_dump(mode="python"))
    if isinstance(brief, str):
        return None, None, {"body": brief}
    if isinstance(brief, Mapping):
        try:
            validated = SourceBrief.model_validate(brief)
            return validated.id, getattr(validated, "title", None), dict(validated.model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            pass
        try:
            validated = ImplementationBrief.model_validate(brief)
            return validated.id, getattr(validated, "title", None), dict(validated.model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            pass
        return brief.get("id"), brief.get("title"), dict(brief)
    if hasattr(brief, "id"):
        payload = {}
        for field in _SCANNED_FIELDS:
            if hasattr(brief, field):
                payload[field] = getattr(brief, field)
        return getattr(brief, "id", None), getattr(brief, "title", None), payload
    return None, None, {}


def _has_negated_scope(payload: Mapping[str, Any]) -> bool:
    searchable = " ".join(str(v) for v in payload.values() if v)
    return bool(_NO_APPROVAL_RE.search(searchable)) or bool(_NEGATED_SCOPE_RE.search(searchable))


def _find_surface_matches(payload: Mapping[str, Any], surface: WorkflowApprovalSurface) -> list[tuple[str, str, str]]:
    pattern = _SURFACE_PATTERNS[surface]
    matches: list[tuple[str, str, str]] = []

    # Check if the brief itself has approval workflow context in key fields
    brief_has_context = False
    for key in ("domain", "title", "summary", "workflow", "workflow_context", "approval", "review"):
        if key in payload and payload[key]:
            if _APPROVAL_CONTEXT_RE.search(str(payload[key])):
                brief_has_context = True
                break

    def _scan_value(field_name: str, value: Any, parent_has_context: bool = False) -> None:
        if isinstance(value, dict):
            # Recursively scan nested dictionaries
            # Check if this dict level has approval workflow context
            dict_text = " ".join(str(v) for v in value.values() if v)
            has_context = parent_has_context or bool(_APPROVAL_CONTEXT_RE.search(dict_text))
            for nested_key, nested_value in value.items():
                nested_field = f"{field_name}.{nested_key}" if field_name else nested_key
                _scan_value(nested_field, nested_value, has_context)
        elif isinstance(value, (list, tuple)):
            # Scan list/tuple items
            for item in value:
                _scan_value(field_name, item, parent_has_context)
        elif value:
            text = str(value)
            # Only require context if parent doesn't have it and text is long enough
            if not parent_has_context and len(text) > 50 and not _APPROVAL_CONTEXT_RE.search(text):
                return

            for match in pattern.finditer(text):
                snippet = text[max(0, match.start() - 40) : min(len(text), match.end() + 40)]
                snippet = _SPACE_RE.sub(" ", snippet).strip()
                matches.append((field_name, snippet, match.group(0)))

    for field_name in _SCANNED_FIELDS:
        if field_name in _IGNORED_FIELDS:
            continue
        value = payload.get(field_name)
        if value:
            _scan_value(field_name, value, brief_has_context)

    return matches


def _best_match(
    matches: list[tuple[str, str, str]], surface: WorkflowApprovalSurface
) -> tuple[tuple[str, ...], str, WorkflowApprovalConfidence, str | None]:
    if not matches:
        return tuple(), "", "low", None

    field_name, snippet, _keyword = matches[0]
    evidence = tuple(f"{field_name}: ...{snippet}..." for field_name, snippet, _ in matches[:3])

    confidence: WorkflowApprovalConfidence = "medium"
    if _REQUIREMENT_RE.search(snippet):
        confidence = "high"
    elif not _STRUCTURED_FIELD_RE.search(field_name):
        confidence = "low"

    # Extract approver role from snippet
    approver = None
    role_match = _APPROVER_ROLE_RE.search(snippet)
    if role_match:
        approver = role_match.group(0).lower()

    return evidence, field_name, confidence, approver


def _detect_gaps(payload: Mapping[str, Any], surface: WorkflowApprovalSurface, approver: str | None) -> list[WorkflowApprovalMissingDetail]:
    gaps: list[WorkflowApprovalMissingDetail] = []
    searchable = " ".join(str(v) for v in payload.values() if v)

    if not approver and surface not in ("multi_step_approval", "escalation", "rejection_reason"):
        if not _APPROVER_ROLE_RE.search(searchable):
            gaps.append("missing_approver_role")

    if surface == "approval_sla":
        if not _SLA_RE.search(searchable):
            gaps.append("missing_approval_sla")

    return gaps


def _compute_summary(requirements: list[SourceWorkflowApprovalRequirement]) -> dict[str, Any]:
    surface_counts = {surface: 0 for surface in _SURFACE_ORDER}
    confidence_counts = {"high": 0, "medium": 0, "low": 0}
    missing_detail_flags: set[str] = set()

    for req in requirements:
        surface_counts[req.surface] += 1
        confidence_counts[req.confidence] += 1
        for gap_msg in req.gap_messages:
            for detail, msg in _GAP_MESSAGES.items():
                if msg == gap_msg:
                    missing_detail_flags.add(detail)

    return {
        "requirement_count": len(requirements),
        "surface_counts": surface_counts,
        "confidence_counts": confidence_counts,
        "missing_detail_flags": sorted(missing_detail_flags),
    }


def _empty_summary() -> dict[str, Any]:
    return {
        "requirement_count": 0,
        "surface_counts": {surface: 0 for surface in _SURFACE_ORDER},
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
    }


__all__ = [
    "WorkflowApprovalSurface",
    "WorkflowApprovalMissingDetail",
    "WorkflowApprovalConfidence",
    "SourceWorkflowApprovalRequirement",
    "SourceWorkflowApprovalRequirementsReport",
    "extract_source_workflow_approval_requirements",
]
