"""Extract source-level AI governance expectations from design briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


AIGovernanceRequirementType = Literal[
    "human_review",
    "model_disclosure",
    "prompt_data_retention",
    "training_data_exclusion",
    "evaluation_criteria",
    "bias_safety_review",
    "fallback_behavior",
    "auditability",
    "prohibited_automated_decisions",
]
AIGovernanceMissingDetail = Literal[
    "missing_review_policy",
    "missing_retention_training_policy",
]
AIGovernanceConfidence = Literal["high", "medium", "low"]
AIGovernanceStatus = Literal["complete", "needs_detail", "no_requirements"]

_TYPE_ORDER: tuple[AIGovernanceRequirementType, ...] = (
    "human_review",
    "model_disclosure",
    "prompt_data_retention",
    "training_data_exclusion",
    "evaluation_criteria",
    "bias_safety_review",
    "fallback_behavior",
    "auditability",
    "prohibited_automated_decisions",
)
_CONFIDENCE_ORDER: dict[AIGovernanceConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|cannot ship|policy|compliance)\b",
    re.I,
)
_AI_CONTEXT_RE = re.compile(
    r"\b(?:ai|llm|large language model|language model|generative|genai|model|models|"
    r"machine learning|ml|prompt|prompts|completion|inference|generated|"
    r"automated decision|automated decisions|algorithmic decision|copilot)\b",
    re.I,
)
_GOVERNANCE_CONTEXT_RE = re.compile(
    r"\b(?:governance|compliance|policy|review|oversight|audit|traceability|"
    r"disclosure|transparency|retention|training data|evaluation|eval|bias|safety|"
    r"fallback|human review|human[- ]in[- ]the[- ]loop|prohibited|not use|must not)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:ai|llm|model|prompt|governance|compliance|review|evaluation|safety|"
    r"audit|retention|training|disclosure|requirements?|constraints?|acceptance|done)",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:ai|llm|model|prompt|automated decision|"
    r"governance|review|evaluation)\b.{0,100}\b(?:scope|required|needed|planned|support|changes?)\b|"
    r"\b(?:ai|llm|model|prompt|automated decision|governance|review|evaluation)\b"
    r".{0,100}\b(?:out of scope|not required|not needed|no support|unsupported|"
    r"non[- ]?goal)\b",
    re.I,
)
_NO_AI_RE = re.compile(
    r"\b(?:no ai features?|ai is out of scope|no llm|no model use|fully manual workflow|"
    r"manual workflow with no ai)\b",
    re.I,
)
_REVIEW_DETAIL_RE = re.compile(
    r"\b(?:criteria|condition|threshold|trigger|queue|sla|approval|reviewer|"
    r"human[- ]in[- ]the[- ]loop|escalat|when|if|before)\b",
    re.I,
)
_RETENTION_TRAINING_DETAIL_RE = re.compile(
    r"\b(?:\d+\s*(?:days?|weeks?|months?|years?|hours?)|retention period|ttl|"
    r"delete after|purge|expiration|do not train|not train|training exclusion|"
    r"exclud(?:e|ed|ing) from (?:model )?training|opt[- ]out|zero retention|"
    r"data lifecycle|vendor retention)\b",
    re.I,
)
_IGNORED_FIELDS = {
    "id",
    "source_id",
    "source_brief_id",
    "source_project",
    "source_entity_type",
    "source_links",
    "created_at",
    "updated_at",
    "created_by",
    "updated_by",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
    "status",
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
    "workflow_context",
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
        r"human[- ]in[- ]the[- ]loop|hitl|manual review|operator review|"
        r"editor approval|review queue|review (?:ai|llm|model|generated) "
        r"(?:output|content|response|decision)|approval before)\b",
        re.I,
    ),
    "model_disclosure": re.compile(
        r"\b(?:(?:ai|llm|model)[- ]generated (?:content )?(?:disclosure|indicator|label|badge)|"
        r"disclose (?:(?:ai|llm|model)[- ]generated (?:content|output)|ai (?:usage|use)|model use)|"
        r"label (?:ai|llm|model)[- ]generated|transparency (?:about|regarding) (?:ai|llm|model)|"
        r"notify (?:users?|customers?) (?:of|about) (?:ai|llm|model) (?:usage|use)|"
        r"(?:ai|llm|model) (?:disclosure|transparency|labeling))\b",
        re.I,
    ),
    "prompt_data_retention": re.compile(
        r"\b(?:(?:prompt|input|response|output|conversation|interaction|model data) "
        r"(?:retention|storage|history|log|archive)|retain(?:ed|ing)? "
        r"(?:prompts?|inputs?|responses?|outputs?|model data)|"
        r"(?:prompts?|inputs?|responses?|outputs?|model data).{0,40}\bretained|"
        r"store (?:prompts?|inputs?|responses?|outputs?)|data retention (?:for|of) "
        r"(?:prompts?|responses?|(?:ai|llm|model) (?:interactions?|data))|"
        r"retention period (?:for prompts?|for responses?|for ai data)|"
        r"delete (?:prompts?|responses?|model data) after|"
        r"(?:prompt|response|conversation) (?:ttl|expiration|cleanup)|zero retention)\b",
        re.I,
    ),
    "training_data_exclusion": re.compile(
        r"\b(?:do not train|not train|never train|exclude (?:prompts?|inputs?|responses?|"
        r"outputs?|customer data|personal data) from (?:model )?training|training data exclusion|"
        r"(?:prompts?|inputs?|responses?|outputs?|customer data|personal data).{0,40}"
        r"\bexclud(?:e|ed|ing) from (?:model )?training|"
        r"training opt[- ]out|opt out of (?:model )?training|no training on (?:customer|user|prompt)|"
        r"disable vendor training|not used for training|prevent training data use)\b",
        re.I,
    ),
    "evaluation_criteria": re.compile(
        r"\b(?:(?:model|llm|ai|prompt) (?:evaluation|testing|validation|benchmarking)|"
        r"eval(?:uation)? (?:criteria|dataset|set|harness|framework|metrics)|"
        r"quality (?:criteria|evaluation|assessment|metrics|thresholds?)|"
        r"acceptance metrics? for (?:ai|llm|model)|golden (?:dataset|set)|"
        r"prompt regression|regression (?:suite|testing)|red[- ]team(?:ing)?|"
        r"adversarial (?:testing|evaluation)|test (?:prompts?|cases?))\b",
        re.I,
    ),
    "bias_safety_review": re.compile(
        r"\b(?:bias (?:review|assessment|testing|mitigation|audit)|fairness (?:review|testing|metrics)|"
        r"safety (?:review|assessment|filters?|filtering|check|validation|guardrails?)|"
        r"content (?:filters?|filtering|moderation)|harmful (?:content|output) "
        r"(?:detection|filters?|prevention)|toxicity (?:filters?|detection|check)|"
        r"(?:pii|personal data) (?:detection|filters?|redaction)|"
        r"output (?:validation|filtering)|guardrails?)\b",
        re.I,
    ),
    "fallback_behavior": re.compile(
        r"\b(?:fallback (?:behavior|response|strategy|logic|handling)|"
        r"(?:model|ai|llm) (?:failure|unavailable|down|error|timeout) (?:handling|fallback)|"
        r"graceful degradation|degrade gracefully|default (?:behavior|response|action) "
        r"(?:when|if) (?:model|ai|llm) (?:fails?|unavailable)|"
        r"(?:when|if) (?:model|ai|llm) (?:fails?|errors?|times out|unavailable)|"
        r"circuit breaker|timeout handling)\b",
        re.I,
    ),
    "auditability": re.compile(
        r"\b(?:auditability|audit trail|audit log(?:ging|s)?|prompt audit|model audit|"
        r"trace(?:ability)? (?:of|for) (?:prompts?|responses?|model|ai)|"
        r"track (?:(?:all )?(?:user )?)?(?:prompts?|inputs?|outputs?|responses?)|"
        r"log (?:(?:all )?(?:user )?)?(?:prompts?|inputs?|outputs?|responses?)|"
        r"track (?:model|prompt) version|log model version|model (?:id|identifier|registry)|"
        r"prompt (?:template )?version|inference trace|decision trace)\b",
        re.I,
    ),
    "prohibited_automated_decisions": re.compile(
        r"\b(?:prohibit(?:ed)? automated decisions?|no automated decisions?|"
        r"must not (?:make|perform|use) automated decisions?|"
        r"(?:ai|llm|model) must not (?:approve|deny|reject|decide|make).{0,80}\bdecisions?|"
        r"no (?:fully )?automated (?:approval|denial|rejection|decisioning)|"
        r"human required for final decision|not use (?:ai|llm|model) for (?:final )?"
        r"(?:hiring|credit|loan|medical|legal|employment|eligibility) decisions?|"
        r"prohibited (?:use|usage).{0,80}(?:automated decision|decisioning|approval|denial))\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[AIGovernanceRequirementType, tuple[str, ...]] = {
    "human_review": ("product_manager", "ml_engineer", "operations"),
    "model_disclosure": ("frontend", "product_manager", "legal"),
    "prompt_data_retention": ("backend", "platform", "legal"),
    "training_data_exclusion": ("ml_engineer", "legal", "vendor_management"),
    "evaluation_criteria": ("ml_engineer", "qa", "product_manager"),
    "bias_safety_review": ("ml_engineer", "security", "compliance"),
    "fallback_behavior": ("backend", "ml_engineer", "platform"),
    "auditability": ("backend", "platform", "compliance"),
    "prohibited_automated_decisions": ("product_manager", "legal", "compliance"),
}
_PLANNING_NOTES: dict[AIGovernanceRequirementType, tuple[str, ...]] = {
    "human_review": ("Define review triggers, reviewer ownership, queue workflow, and approval evidence.",),
    "model_disclosure": ("Design disclosure placement, wording, event tracking, and product/legal approval.",),
    "prompt_data_retention": ("Define prompt, response, and interaction retention periods plus deletion workflows.",),
    "training_data_exclusion": ("Document provider settings and controls that prevent source data from training use.",),
    "evaluation_criteria": ("Define evaluation datasets, metrics, quality thresholds, and regression cadence.",),
    "bias_safety_review": ("Add bias, fairness, safety, and harmful-output review before release.",),
    "fallback_behavior": ("Design timeout, error, degraded-mode, and escalation behavior for model failures.",),
    "auditability": ("Record prompts, outputs, model versions, decision traces, and audit query paths.",),
    "prohibited_automated_decisions": ("Document decisions the model cannot make and enforce human final approval.",),
}
_GAP_MESSAGES: dict[AIGovernanceMissingDetail, str] = {
    "missing_review_policy": "Specify human review policy, triggers, owners, or approval criteria.",
    "missing_retention_training_policy": (
        "Define prompt/data retention period and whether source data is excluded from model training."
    ),
}


@dataclass(frozen=True, slots=True)
class SourceAIGovernanceRequirement:
    """One source-backed AI governance requirement."""

    source_brief_id: str | None
    requirement_type: AIGovernanceRequirementType
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: AIGovernanceConfidence = "medium"
    missing_details: tuple[AIGovernanceMissingDetail, ...] = field(default_factory=tuple)
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
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "source_field": self.source_field,
            "missing_details": list(self.missing_details),
            "confidence": self.confidence,
            "evidence": list(self.evidence),
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
    def source_id(self) -> str | None:
        """Compatibility alias for source requirement reports."""
        return self.brief_id

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
            "source_id": self.source_id,
            "title": self.title,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
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
            f"- Status: {self.summary.get('status', 'no_requirements')}",
            "- Type counts: "
            + ", ".join(f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER),
            "- Missing detail flags: "
            + (", ".join(self.summary.get("missing_detail_flags", [])) or "none"),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        gap_messages = self.summary.get("gap_messages", [])
        if gap_messages:
            lines.extend(["- Gap messages:", *[f"  - {message}" for message in gap_messages]])
        if not self.requirements:
            lines.extend(["", "No source AI governance requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Source Brief | Type | Confidence | Missing Details | Owners | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for req in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(req.source_brief_id or '')} | "
                f"{req.requirement_type} | "
                f"{req.confidence} | "
                f"{_markdown_cell(', '.join(req.missing_details) or 'none')} | "
                f"{_markdown_cell(', '.join(req.suggested_owners) or 'none')} | "
                f"{_markdown_cell('; '.join(req.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_source_ai_governance_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAIGovernanceRequirementsReport:
    """Extract AI governance requirement records from SourceBrief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda req: (
                req.source_brief_id or "",
                _TYPE_ORDER.index(req.requirement_type),
                _CONFIDENCE_ORDER[req.confidence],
                req.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _, _ in brief_payloads if source_id)
    titles = _dedupe(title for _, title, _ in brief_payloads if title)
    return SourceAIGovernanceRequirementsReport(
        brief_id=source_ids[0] if len(source_ids) == 1 else None,
        title=titles[0] if len(titles) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, source_count=len(brief_payloads)),
    )


def extract_source_ai_governance_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAIGovernanceRequirementsReport:
    """Compatibility alias for building an AI governance requirements report."""
    return build_source_ai_governance_requirements(source)


def generate_source_ai_governance_requirements(source: Any) -> SourceAIGovernanceRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_ai_governance_requirements(source)


def derive_source_ai_governance_requirements(source: Any) -> SourceAIGovernanceRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_ai_governance_requirements(source)


def summarize_source_ai_governance_requirements(
    source_or_result: Any,
) -> dict[str, Any]:
    """Return deterministic counts for extracted AI governance requirements."""
    if isinstance(source_or_result, SourceAIGovernanceRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_ai_governance_requirements(source_or_result).summary


def source_ai_governance_requirements_to_dict(
    report: SourceAIGovernanceRequirementsReport,
) -> dict[str, Any]:
    """Serialize an AI governance requirements report to a plain dictionary."""
    return report.to_dict()


source_ai_governance_requirements_to_dict.__test__ = False


def source_ai_governance_requirements_to_dicts(
    requirements: (
        tuple[SourceAIGovernanceRequirement, ...]
        | list[SourceAIGovernanceRequirement]
        | SourceAIGovernanceRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize AI governance requirement records to dictionaries."""
    if isinstance(requirements, SourceAIGovernanceRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_ai_governance_requirements_to_dicts.__test__ = False


def source_ai_governance_requirements_to_markdown(
    report: SourceAIGovernanceRequirementsReport,
) -> str:
    """Render an AI governance requirements report as Markdown."""
    return report.to_markdown()


source_ai_governance_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_type: AIGovernanceRequirementType
    source_field: str
    evidence: str
    confidence: AIGovernanceConfidence
    missing_details: tuple[AIGovernanceMissingDetail, ...]


def _source_payloads(source: Any) -> list[tuple[str | None, str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(
        source, "model_dump"
    ):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Any) -> tuple[str | None, str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), _optional_text(payload.get("title")), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), _optional_text(payload.get("title")), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                value = model.model_validate(source).model_dump(mode="python")
                payload = dict(value)
                return _source_id(payload), _optional_text(payload.get("title")), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), _optional_text(payload.get("title")), payload
    payload = _object_payload(source)
    return _source_id(payload), _optional_text(payload.get("title")), payload


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (*_SCANNED_FIELDS, "id", "source_brief_id", "source_id", "title")
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, _title, payload in brief_payloads:
        if _has_negated_scope(payload):
            continue
        for source_field, segment in _candidate_segments(payload):
            requirement_types = _requirement_types(segment, source_field)
            if not requirement_types:
                continue
            evidence = _evidence_snippet(source_field, segment)
            for requirement_type in requirement_types:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        requirement_type=requirement_type,
                        source_field=source_field,
                        evidence=evidence,
                        confidence=_confidence(segment, source_field),
                        missing_details=_missing_details(requirement_type, segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAIGovernanceRequirement]:
    grouped: dict[tuple[str | None, AIGovernanceRequirementType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.requirement_type), []).append(candidate)

    requirements: list[SourceAIGovernanceRequirement] = []
    for (source_brief_id, requirement_type), items in grouped.items():
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        missing = set(items[0].missing_details)
        for item in items[1:]:
            missing.intersection_update(item.missing_details)
        missing_details = tuple(detail for detail in _GAP_MESSAGES if detail in missing)
        requirements.append(
            SourceAIGovernanceRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                source_field=items[0].source_field,
                missing_details=missing_details,
                confidence=confidence,
                evidence=tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:5],
                suggested_owners=_OWNER_SUGGESTIONS[requirement_type],
                planning_notes=_PLANNING_NOTES[requirement_type],
                gap_messages=tuple(_GAP_MESSAGES[detail] for detail in missing_details),
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _key_has_governance_context(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, child)
            if _key_has_governance_context(key_text) and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    values.extend((child_field, segment) for segment in _segments(f"{key_text}: {text}"))
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        values.extend((source_field, segment) for segment in _segments(text))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                clause = _clean_text(clause)
                if clause:
                    segments.append(clause)
    return segments


def _requirement_types(segment: str, source_field: str) -> tuple[AIGovernanceRequirementType, ...]:
    if _is_negated_segment(segment):
        return tuple()
    field_has_context = bool(_STRUCTURED_FIELD_RE.search(source_field))
    has_ai_context = bool(_AI_CONTEXT_RE.search(segment))
    has_governance_context = bool(_GOVERNANCE_CONTEXT_RE.search(segment)) or field_has_context
    if not ((has_ai_context and has_governance_context) or field_has_context):
        return tuple()
    hits = [req_type for req_type in _TYPE_ORDER if _TYPE_PATTERNS[req_type].search(segment)]
    if not has_ai_context:
        hits = [req_type for req_type in hits if req_type in {"human_review", "fallback_behavior"} or field_has_context]
    return tuple(hits)


def _missing_details(
    requirement_type: AIGovernanceRequirementType,
    segment: str,
) -> tuple[AIGovernanceMissingDetail, ...]:
    missing: list[AIGovernanceMissingDetail] = []
    if requirement_type == "human_review" and not _REVIEW_DETAIL_RE.search(segment):
        missing.append("missing_review_policy")
    if requirement_type in {"prompt_data_retention", "training_data_exclusion"} and not (
        _RETENTION_TRAINING_DETAIL_RE.search(segment)
    ):
        missing.append("missing_retention_training_policy")
    return tuple(missing)


def _confidence(segment: str, source_field: str) -> AIGovernanceConfidence:
    if _REQUIRED_RE.search(segment):
        return "high"
    if _STRUCTURED_FIELD_RE.search(source_field):
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceAIGovernanceRequirement, ...],
    *,
    source_count: int,
) -> dict[str, Any]:
    type_counts = {
        req_type: sum(1 for requirement in requirements if requirement.requirement_type == req_type)
        for req_type in _TYPE_ORDER
    }
    confidence_counts = {
        confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
        for confidence in _CONFIDENCE_ORDER
    }
    missing_detail_flags = tuple(
        detail
        for detail in _GAP_MESSAGES
        if any(detail in requirement.missing_details for requirement in requirements)
    )
    gap_messages = tuple(_GAP_MESSAGES[detail] for detail in missing_detail_flags)
    if not requirements:
        status: AIGovernanceStatus = "no_requirements"
    elif missing_detail_flags:
        status = "needs_detail"
    else:
        status = "complete"
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "type_counts": type_counts,
        "confidence_counts": confidence_counts,
        "requirement_types": [req.requirement_type for req in requirements],
        "missing_detail_flags": list(missing_detail_flags),
        "gap_messages": list(gap_messages),
        "status": status,
    }


def _has_negated_scope(payload: Mapping[str, Any]) -> bool:
    searchable = " ".join(_strings(payload))
    return bool(_NO_AI_RE.search(searchable)) or bool(_NEGATED_SCOPE_RE.search(searchable))


def _is_negated_segment(segment: str) -> bool:
    return bool(_NO_AI_RE.search(segment)) or bool(_NEGATED_SCOPE_RE.search(segment))


def _key_has_governance_context(value: str) -> bool:
    return bool(_AI_CONTEXT_RE.search(value)) or bool(_GOVERNANCE_CONTEXT_RE.search(value))


def _strings(value: Any) -> list[str]:
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
    if value is None:
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip()


def _evidence_snippet(source_field: str, segment: str) -> str:
    cleaned = _clean_text(segment)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _dedupe(values: Iterable[Any]) -> list[Any]:
    seen: set[str] = set()
    deduped: list[Any] = []
    for value in values:
        key = str(value).casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        key = re.sub(r"\[\d+\]", "[]", value).casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "AIGovernanceRequirementType",
    "AIGovernanceMissingDetail",
    "AIGovernanceConfidence",
    "AIGovernanceStatus",
    "SourceAIGovernanceRequirement",
    "SourceAIGovernanceRequirementsReport",
    "build_source_ai_governance_requirements",
    "extract_source_ai_governance_requirements",
    "generate_source_ai_governance_requirements",
    "derive_source_ai_governance_requirements",
    "summarize_source_ai_governance_requirements",
    "source_ai_governance_requirements_to_dict",
    "source_ai_governance_requirements_to_dicts",
    "source_ai_governance_requirements_to_markdown",
]
