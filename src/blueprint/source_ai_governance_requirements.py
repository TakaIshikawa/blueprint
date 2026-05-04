"""Extract source-level AI governance expectations from design briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


AIGovernanceRequirementType = Literal[
    "human_review",
    "model_evaluation",
    "prompt_traceability",
    "version_traceability",
    "safety_filters",
    "generated_content_disclosure",
    "prompt_response_retention",
    "fallback_behavior",
    "prohibited_use",
]
AIGovernanceMissingDetail = Literal["missing_review_criteria", "missing_retention_period"]
AIGovernanceConfidence = Literal["high", "medium", "low"]

_TYPE_ORDER: tuple[AIGovernanceRequirementType, ...] = (
    "human_review",
    "model_evaluation",
    "prompt_traceability",
    "version_traceability",
    "safety_filters",
    "generated_content_disclosure",
    "prompt_response_retention",
    "fallback_behavior",
    "prohibited_use",
)
_CONFIDENCE_ORDER: dict[AIGovernanceConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_AI_GOVERNANCE_CONTEXT_RE = re.compile(
    r"(?:^|[^a-z])(?:ai|llm|large language model|language model|generative|"
    r"genai|gpt|openai|anthropic|claude|gemini|copilot|"
    r"model|prompt|completion|inference|generated|"
    r"governance|compliance|review|oversight|audit|"
    r"safety|evaluation|testing|validation|"
    r"traceability|transparency|disclosure)(?:[^a-z]|$)",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:ai|llm|model|prompt|governance|compliance|review|"
    r"evaluation|safety|audit|traceability|transparency|"
    r"requirements?|acceptance|done|validation)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"provide|include|submit|implement|track|log|record|"
    r"before|prior to|after|whenever|"
    r"acceptance|done when)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:ai|llm|model|prompt|governance|compliance|oversight|evaluation|review)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:ai|llm|model|prompt|governance|compliance|oversight|evaluation|review)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_AI_GOVERNANCE_RE = re.compile(
    r"\b(?:no (?:ai|llm|model|governance|compliance|oversight)|"
    r"ai is out of scope|no ai features?|"
    r"(?:fully )?manual\s+(?:process|workflow))\b",
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
    "ai_context",
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
    "compliance",
    "governance",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_TYPE_PATTERNS: dict[AIGovernanceRequirementType, re.Pattern[str]] = {
    "human_review": re.compile(
        r"\b(?:human (?:review|oversight|approval|verification|validation)|"
        r"human[- ]in[- ]the[- ]loop|hitl|manual review|"
        r"review (?:queue|before|prior to|(?:for|of) (?:ai|llm|model))|operator review|"
        r"supervisor (?:review|approval)|editor approval|"
        r"review (?:ai|llm|model|generated) (?:output|content|response))\b",
        re.I,
    ),
    "model_evaluation": re.compile(
        r"\b(?:(?:model|llm|ai) (?:evaluation|testing|validation|benchmarking)|"
        r"eval(?:uation)? (?:dataset|set|harness|framework)|"
        r"quality (?:evaluation|assessment|metrics)|"
        r"performance (?:testing|metrics|evaluation)|"
        r"red[- ]team(?:ing)?|adversarial (?:testing|evaluation)|"
        r"regression (?:testing|suite)|prompt regression|"
        r"golden (?:dataset|set)|test (?:prompts?|cases?))\b",
        re.I,
    ),
    "prompt_traceability": re.compile(
        r"\b(?:prompt (?:traceability|tracking|logging|versioning|history)|"
        r"track (?:(?:all )?(?:user )?)?(?:prompts?|inputs?)|log (?:(?:all )?(?:user )?)?(?:prompts?|inputs?)|"
        r"prompt (?:audit|record)|store (?:prompts?|user inputs?)|"
        r"input (?:logging|tracking|history)|"
        r"traceability (?:of prompts?|for (?:ai|llm|model) inputs?))\b",
        re.I,
    ),
    "version_traceability": re.compile(
        r"\b(?:(?:model|prompt) (?:version|versioning)|"
        r"version (?:control|tracking|management) (?:for|of) (?:models?|prompts?)|"
        r"track (?:model|prompt) version|log model version|"
        r"model (?:id|identifier|registry)|prompt (?:template )?version|"
        r"deployment version|inference version|"
        r"traceability (?:of|for) model version)\b",
        re.I,
    ),
    "safety_filters": re.compile(
        r"\b(?:safety (?:filters?|filtering|check|validation|guardrails?)|"
        r"content (?:filters?|filtering|moderation)|"
        r"harmful (?:content|output) (?:detection|filters?|prevention)|"
        r"toxicity (?:filters?|detection|check)|"
        r"profanity (?:filters?|check)|(?:pii|personal data) (?:detection|filters?|redaction)|"
        r"input (?:validation|sanitization)|output (?:validation|filtering)|"
        r"guardrails?|safety layers?)\b",
        re.I,
    ),
    "generated_content_disclosure": re.compile(
        r"\b(?:(?:ai|llm|model)[- ]generated (?:content )?(?:disclosure|indicator|label|badge)|"
        r"disclose (?:(?:ai|llm|model)[- ]generated (?:content|output)|ai (?:usage|use))|"
        r"indicate (?:ai|llm|model)[- ]generated|label (?:ai|llm|model) (?:content|output)|"
        r"transparency (?:about|regarding) (?:ai|llm|model) (?:usage|use)|"
        r"notify (?:users?|customers?) (?:of|about) (?:ai|llm) (?:usage|use)|"
        r"(?:ai|llm) (?:disclosure|transparency))\b",
        re.I,
    ),
    "prompt_response_retention": re.compile(
        r"\b(?:(?:prompt|input|response|output) (?:retention|storage)|"
        r"retain (?:prompts?|inputs?|responses?|outputs?)|"
        r"store (?:prompts?|inputs?|responses?|outputs?)|"
        r"(?:prompt|response|interaction) (?:history|log|archive)|"
        r"data retention (?:for|of) (?:prompts?|responses?|(?:ai|llm) (?:interactions?|data))|"
        r"retention period (?:for prompts?|for responses?)|"
        r"delete (?:prompts?|responses?) after|"
        r"(?:prompt|response) (?:ttl|expiration|cleanup))\b",
        re.I,
    ),
    "fallback_behavior": re.compile(
        r"\b(?:fallback (?:behavior|response|strategy|logic|handling)|"
        r"(?:model|ai|llm) (?:failure|unavailable|down|error) (?:handling|fallback)|"
        r"graceful degradation|degrade gracefully|"
        r"default (?:behavior|response|action) (?:when|if) (?:model|ai|llm) (?:fails?|unavailable)|"
        r"(?:when|if) (?:model|ai|llm) (?:fails?|errors?|unavailable)|"
        r"circuit breaker|timeout handling|error (?:recovery|handling))\b",
        re.I,
    ),
    "prohibited_use": re.compile(
        r"\b(?:prohibited (?:use|usage|uses|content)|disallowed (?:use|usage|content)|"
        r"(?:not|cannot|must not) (?:use|be used) (?:for|to)|"
        r"use case (?:restrictions?|limitations?|constraints?)|"
        r"(?:illegal|harmful|malicious|unethical) (?:use|usage|content)|"
        r"acceptable use (?:policy|guidelines?)|terms of (?:use|service)|"
        r"usage (?:restrictions?|limitations?|constraints?|policy)|"
        r"no (?:medical|legal|financial) advice)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[AIGovernanceRequirementType, tuple[str, ...]] = {
    "human_review": ("product_manager", "backend", "ml_engineer"),
    "model_evaluation": ("ml_engineer", "ml_ops", "qa"),
    "prompt_traceability": ("backend", "ml_engineer", "platform"),
    "version_traceability": ("ml_engineer", "ml_ops", "platform"),
    "safety_filters": ("ml_engineer", "backend", "security"),
    "generated_content_disclosure": ("frontend", "product_manager", "legal"),
    "prompt_response_retention": ("backend", "platform", "legal"),
    "fallback_behavior": ("backend", "ml_engineer", "platform"),
    "prohibited_use": ("product_manager", "legal", "compliance"),
}
_PLANNING_NOTES: dict[AIGovernanceRequirementType, tuple[str, ...]] = {
    "human_review": ("Define review criteria, review UI/workflow, reviewer assignment, and approval tracking.",),
    "model_evaluation": ("Establish evaluation dataset, metrics, testing frequency, regression suite, and quality thresholds.",),
    "prompt_traceability": ("Implement prompt logging infrastructure, define log format, set retention policy, and enable audit queries.",),
    "version_traceability": ("Track model version, prompt template version, deployment metadata, and enable version rollback.",),
    "safety_filters": ("Implement input/output filters, define filter rules, handle filter violations, and log filter events.",),
    "generated_content_disclosure": ("Design disclosure UI, determine disclosure placement, handle user awareness, and track disclosure events.",),
    "prompt_response_retention": ("Define retention period, implement data cleanup, handle GDPR/privacy requirements, and enable data export.",),
    "fallback_behavior": ("Design fallback logic, implement circuit breaker, define timeout thresholds, and track fallback events.",),
    "prohibited_use": ("Document acceptable use policy, implement use case validation, handle policy violations, and enable compliance reporting.",),
}
_GAP_MESSAGES: dict[AIGovernanceMissingDetail, str] = {
    "missing_review_criteria": "Specify criteria or conditions for triggering human review.",
    "missing_retention_period": "Define retention period or data lifecycle for prompts and responses.",
}


@dataclass(frozen=True, slots=True)
class SourceAIGovernanceRequirement:
    """One source-backed AI governance requirement."""

    requirement_type: AIGovernanceRequirementType
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: AIGovernanceConfidence = "medium"
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> AIGovernanceRequirementType:
        """Compatibility view for extractors that expose requirement_category."""
        return self.requirement_type

    @property
    def concern(self) -> AIGovernanceRequirementType:
        """Compatibility view for extractors that expose concern naming."""
        return self.requirement_type

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        """Compatibility view matching adjacent source extractors."""
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "suggested_owners": list(self.suggested_owners),
            "planning_notes": list(self.planning_notes),
            "gap_messages": list(self.gap_messages),
        }


@dataclass(frozen=True, slots=True)
class SourceAIGovernanceRequirementsReport:
    """Source-level AI governance requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceAIGovernanceRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAIGovernanceRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAIGovernanceRequirement, ...]:
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
        """Return AI governance requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source AI Governance Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        type_counts = self.summary.get("type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Type counts: "
            + ", ".join(f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER),
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source AI governance requirements were inferred."])
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
                    f"### {req.requirement_type}",
                    "",
                    f"- Source field: `{req.source_field}`",
                    f"- Confidence: {req.confidence}",
                ]
            )
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


def extract_source_ai_governance_requirements(
    brief: SourceBrief | ImplementationBrief | Mapping[str, Any] | str | object,
) -> SourceAIGovernanceRequirementsReport:
    """Extract source AI governance requirements from a source or implementation brief."""
    brief_id, title, payload = _brief_payload(brief)
    if _has_negated_scope(payload):
        return SourceAIGovernanceRequirementsReport(
            brief_id=brief_id,
            title=title,
            requirements=tuple(),
            summary=_empty_summary(),
        )

    requirements: list[SourceAIGovernanceRequirement] = []
    seen_types: set[AIGovernanceRequirementType] = set()

    for req_type in _TYPE_ORDER:
        if req_type in seen_types:
            continue
        matches = _find_type_matches(payload, req_type)
        if not matches:
            continue
        seen_types.add(req_type)
        evidence, source_field, confidence = _best_match(matches, req_type)
        gaps = _detect_gaps(payload, req_type)
        requirements.append(
            SourceAIGovernanceRequirement(
                requirement_type=req_type,
                source_field=source_field,
                evidence=evidence,
                confidence=confidence,
                suggested_owners=_OWNER_SUGGESTIONS.get(req_type, tuple()),
                planning_notes=_PLANNING_NOTES.get(req_type, tuple()),
                gap_messages=tuple(_GAP_MESSAGES[g] for g in gaps),
            )
        )

    return SourceAIGovernanceRequirementsReport(
        brief_id=brief_id,
        title=title,
        requirements=tuple(requirements),
        summary=_compute_summary(requirements),
    )


build_source_ai_governance_requirements = extract_source_ai_governance_requirements


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
        for field_name in _SCANNED_FIELDS:
            if hasattr(brief, field_name):
                payload[field_name] = getattr(brief, field_name)
        return getattr(brief, "id", None), getattr(brief, "title", None), payload
    return None, None, {}


def _has_negated_scope(payload: Mapping[str, Any]) -> bool:
    searchable = " ".join(str(v) for v in payload.values() if v)
    return bool(_NO_AI_GOVERNANCE_RE.search(searchable)) or bool(_NEGATED_SCOPE_RE.search(searchable))


def _find_type_matches(payload: Mapping[str, Any], req_type: AIGovernanceRequirementType) -> list[tuple[str, str, str]]:
    pattern = _TYPE_PATTERNS[req_type]
    matches: list[tuple[str, str, str]] = []

    # Check if the brief itself has AI governance context in key fields
    brief_has_context = False
    for key in ("domain", "title", "summary", "ai_context", "compliance", "governance"):
        if key in payload and payload[key]:
            if _AI_GOVERNANCE_CONTEXT_RE.search(str(payload[key])):
                brief_has_context = True
                break

    def _scan_value(field_name: str, value: Any, parent_has_context: bool = False) -> None:
        if isinstance(value, dict):
            # Recursively scan nested dictionaries
            # Check if this dict level has AI governance context
            dict_text = " ".join(str(v) for v in value.values() if v)
            has_context = parent_has_context or bool(_AI_GOVERNANCE_CONTEXT_RE.search(dict_text))
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
            if not parent_has_context and len(text) > 50 and not _AI_GOVERNANCE_CONTEXT_RE.search(text):
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
    matches: list[tuple[str, str, str]], req_type: AIGovernanceRequirementType
) -> tuple[tuple[str, ...], str, AIGovernanceConfidence]:
    if not matches:
        return tuple(), "", "low"

    field_name, snippet, _keyword = matches[0]
    evidence = tuple(f"{field_name}: ...{snippet}..." for field_name, snippet, _ in matches[:3])

    confidence: AIGovernanceConfidence = "medium"
    if _REQUIREMENT_RE.search(snippet):
        confidence = "high"
    elif not _STRUCTURED_FIELD_RE.search(field_name):
        confidence = "low"

    return evidence, field_name, confidence


def _detect_gaps(payload: Mapping[str, Any], req_type: AIGovernanceRequirementType) -> list[AIGovernanceMissingDetail]:
    gaps: list[AIGovernanceMissingDetail] = []
    searchable = " ".join(str(v) for v in payload.values() if v)

    if req_type == "human_review":
        # Check for review criteria
        criteria_re = re.compile(
            r"\b(?:criteria|conditions?|threshold|trigger|when|if)\b",
            re.I,
        )
        if not criteria_re.search(searchable):
            gaps.append("missing_review_criteria")

    if req_type == "prompt_response_retention":
        # Check for retention period
        retention_re = re.compile(
            r"\b(?:\d+\s*(?:days?|months?|years?)|retention period|ttl|expiration|delete after)\b",
            re.I,
        )
        if not retention_re.search(searchable):
            gaps.append("missing_retention_period")

    return gaps


def _compute_summary(requirements: list[SourceAIGovernanceRequirement]) -> dict[str, Any]:
    type_counts = {req_type: 0 for req_type in _TYPE_ORDER}
    confidence_counts = {"high": 0, "medium": 0, "low": 0}
    missing_detail_flags: set[str] = set()

    for req in requirements:
        type_counts[req.requirement_type] += 1
        confidence_counts[req.confidence] += 1
        for gap_msg in req.gap_messages:
            for detail, msg in _GAP_MESSAGES.items():
                if msg == gap_msg:
                    missing_detail_flags.add(detail)

    return {
        "requirement_count": len(requirements),
        "type_counts": type_counts,
        "confidence_counts": confidence_counts,
        "missing_detail_flags": sorted(missing_detail_flags),
    }


def _empty_summary() -> dict[str, Any]:
    return {
        "requirement_count": 0,
        "type_counts": {req_type: 0 for req_type in _TYPE_ORDER},
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
    }


__all__ = [
    "AIGovernanceRequirementType",
    "AIGovernanceMissingDetail",
    "AIGovernanceConfidence",
    "SourceAIGovernanceRequirement",
    "SourceAIGovernanceRequirementsReport",
    "extract_source_ai_governance_requirements",
    "build_source_ai_governance_requirements",
]
