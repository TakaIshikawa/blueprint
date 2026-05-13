"""Extract source-level email bounce handling requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint._source_requirement_utils import (
    dedupe,
    evidence_snippet,
    markdown_cell,
    optional_text,
    segments,
    source_id,
    source_payloads,
)


EmailBounceCategory = Literal[
    "hard_bounce",
    "soft_bounce",
    "suppression_list",
    "complaint_feedback",
    "retry_policy",
    "provider_webhook",
    "deliverability_metrics",
    "user_notification",
    "retention",
]
EmailBounceConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[EmailBounceCategory, ...] = (
    "hard_bounce",
    "soft_bounce",
    "suppression_list",
    "complaint_feedback",
    "retry_policy",
    "provider_webhook",
    "deliverability_metrics",
    "user_notification",
    "retention",
)
_CONFIDENCE_ORDER: dict[EmailBounceConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SCANNED_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "scope",
    "non_goals",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "email",
    "deliverability",
    "bounce",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CONTEXT_RE = re.compile(
    r"\b(?:email bounce|bounces?|bounce handling|hard bounce|soft bounce|suppression|complaint|"
    r"feedback loop|fbl|deliverability|provider webhook|ses webhook|sendgrid webhook|"
    r"mailgun webhook|smtp bounce|unsubscribe|retry email)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"classify|suppress|retry|webhook|notify|alert|monitor|metric|retain|retention|"
    r"owner|ownership|runbook|support|provide|acceptance|done when|unsubscribe)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}"
    r"\b(?:email bounce|bounce handling|hard bounce|soft bounce|suppression|complaint|deliverability)\b|"
    r"\b(?:email bounce|bounce handling|hard bounce|soft bounce|suppression|complaint|deliverability)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|excluded|no changes?)\b",
    re.I,
)
_UNRELATED_RE = re.compile(r"\b(?:bounce animation|bounce rate for web analytics|css bounce|bouncy)\b", re.I)
_CATEGORY_PATTERNS: dict[EmailBounceCategory, re.Pattern[str]] = {
    "hard_bounce": re.compile(r"\b(?:hard bounce|permanent bounce|permanent delivery failure|invalid address|mailbox does not exist)\b", re.I),
    "soft_bounce": re.compile(r"\b(?:soft bounce|temporary bounce|temporary delivery failure|mailbox full|deferral|deferred)\b", re.I),
    "suppression_list": re.compile(r"\b(?:suppression list|suppress|suppressed|do not email|blocklist|unsubscribe)\b", re.I),
    "complaint_feedback": re.compile(r"\b(?:complaint|spam complaint|feedback loop|fbl|abuse report)\b", re.I),
    "retry_policy": re.compile(r"\b(?:retry policy|retry|backoff|reattempt|resend|delivery retry)\b", re.I),
    "provider_webhook": re.compile(r"\b(?:provider webhook|bounce webhook|ses webhook|sendgrid webhook|mailgun webhook|postmark webhook|webhook event)\b", re.I),
    "deliverability_metrics": re.compile(r"\b(?:deliverability|metric|metrics|monitoring|dashboard|alert|bounce rate|complaint rate)\b", re.I),
    "user_notification": re.compile(r"\b(?:notify user|user notification|account notification|email warning|prompt user|update email address)\b", re.I),
    "retention": re.compile(r"\b(?:retention|retain|ttl|expire|expiration|purge|archive|compliance retention)\b", re.I),
}
_OWNER_SUGGESTIONS = {
    "hard_bounce": ("email_platform", "backend"),
    "soft_bounce": ("email_platform", "backend"),
    "suppression_list": ("email_platform", "compliance"),
    "complaint_feedback": ("email_platform", "trust_safety"),
    "retry_policy": ("email_platform", "backend"),
    "provider_webhook": ("email_platform", "integrations"),
    "deliverability_metrics": ("email_platform", "observability"),
    "user_notification": ("email_platform", "product"),
    "retention": ("email_platform", "compliance"),
}
_PLANNING_NOTES = {
    "hard_bounce": ("Define hard bounce classification and when addresses are permanently suppressed.",),
    "soft_bounce": ("Define soft bounce retry limits, deferral handling, and escalation to suppression.",),
    "suppression_list": ("Specify suppression list writes, reads, exceptions, and unsubscribe interactions.",),
    "complaint_feedback": ("Plan complaint feedback loop ingestion and suppression behavior.",),
    "retry_policy": ("Document email retry backoff, max attempts, and terminal failure behavior.",),
    "provider_webhook": ("Define provider webhook validation, event mapping, idempotency, and failure handling.",),
    "deliverability_metrics": ("Add deliverability metrics, dashboards, and alerts for bounce and complaint rates.",),
    "user_notification": ("Define user-facing notification or remediation when addresses cannot receive email.",),
    "retention": ("Set retention and purge policy for bounce, complaint, and suppression evidence.",),
}
_MISSING_DETAIL_MESSAGES = {
    "missing_suppression": "Specify suppression list behavior for hard bounces and complaints.",
    "missing_webhook": "Define provider webhook ingestion and validation for bounce events.",
    "missing_metrics": "Define deliverability metrics and alerting for bounce handling.",
}


@dataclass(frozen=True, slots=True)
class SourceEmailBounceRequirement:
    category: EmailBounceCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: EmailBounceConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> EmailBounceCategory:
        return self.category

    @property
    def concern(self) -> EmailBounceCategory:
        return self.category

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "planning_notes": list(self.planning_notes),
            "gap_messages": list(self.gap_messages),
        }


@dataclass(frozen=True, slots=True)
class SourceEmailBounceRequirementsReport:
    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceEmailBounceRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceEmailBounceRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceEmailBounceRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source Email Bounce Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        category_counts = self.summary.get("category_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Category counts: " + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
        ]
        if not self.requirements:
            lines.extend(["", "No source email bounce requirements were inferred."])
            return "\n".join(lines)
        lines.extend(["", "## Requirements", "", "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |", "| --- | --- | --- | --- | --- | --- | --- | --- |"])
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | {markdown_cell(requirement.value or '')} | {requirement.confidence} | "
                f"{markdown_cell(requirement.source_field)} | {markdown_cell(', '.join(requirement.suggested_owners))} | "
                f"{markdown_cell('; '.join(requirement.evidence))} | {markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{markdown_cell('; '.join(requirement.gap_messages))} |"
            )
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: EmailBounceCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: EmailBounceConfidence


def build_source_email_bounce_requirements(source: Mapping[str, Any] | SourceBrief | ImplementationBrief | Iterable[Any] | str | object) -> SourceEmailBounceRequirementsReport:
    payloads = source_payloads(source)
    candidates: list[_Candidate] = []
    for _, payload in payloads:
        if not _has_no_scope(payload):
            candidates.extend(_candidates(payload))
    requirements = tuple(_merge(candidates, _gap_messages(candidates)))
    ids = dedupe(source_id(payload) for _, payload in payloads)
    return SourceEmailBounceRequirementsReport(
        brief_id=ids[0] if len(ids) == 1 else None,
        title=optional_text(payloads[0][1].get("title")) if payloads else None,
        requirements=requirements,
        summary=_summary(requirements, len(payloads)),
    )


def extract_source_email_bounce_requirements(source: Any) -> tuple[SourceEmailBounceRequirement, ...]:
    return build_source_email_bounce_requirements(source).requirements


def derive_source_email_bounce_requirements(source: Any) -> SourceEmailBounceRequirementsReport:
    return build_source_email_bounce_requirements(source)


def generate_source_email_bounce_requirements(source: Any) -> SourceEmailBounceRequirementsReport:
    return build_source_email_bounce_requirements(source)


def summarize_source_email_bounce_requirements(source: Any) -> dict[str, Any]:
    if isinstance(source, SourceEmailBounceRequirementsReport):
        return dict(source.summary)
    return build_source_email_bounce_requirements(source).summary


def source_email_bounce_requirements_to_dict(report: SourceEmailBounceRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_email_bounce_requirements_to_dict.__test__ = False


def source_email_bounce_requirements_to_dicts(requirements: tuple[SourceEmailBounceRequirement, ...] | list[SourceEmailBounceRequirement] | SourceEmailBounceRequirementsReport) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceEmailBounceRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_email_bounce_requirements_to_dicts.__test__ = False


def source_email_bounce_requirements_to_markdown(report: SourceEmailBounceRequirementsReport) -> str:
    return report.to_markdown()


source_email_bounce_requirements_to_markdown.__test__ = False


def _candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    found: list[_Candidate] = []
    for source_field, text in segments(payload, _SCANNED_FIELDS):
        if source_field == "title" and re.search(r"\brequirements?\b\.?$", text, re.I):
            continue
        searchable = f"{source_field.replace('_', ' ')} {text}"
        if not (_CONTEXT_RE.search(searchable) and _REQUIREMENT_RE.search(searchable)) or _NEGATED_RE.search(searchable) or _UNRELATED_RE.search(searchable):
            continue
        for category in _CATEGORY_ORDER:
            if _CATEGORY_PATTERNS[category].search(searchable):
                found.append(_Candidate(category, _value(category, text), source_field, evidence_snippet(source_field, text), _confidence(searchable)))
    return found


def _has_no_scope(payload: Mapping[str, Any]) -> bool:
    return any(_NEGATED_RE.search(text) for _, text in segments(payload, _SCANNED_FIELDS))


def _merge(candidates: Iterable[_Candidate], gap_messages: tuple[str, ...]) -> list[SourceEmailBounceRequirement]:
    grouped: dict[EmailBounceCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)
    result: list[SourceEmailBounceRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        result.append(
            SourceEmailBounceRequirement(
                category=category,
                source_field=sorted({item.source_field for item in items}, key=str.casefold)[0],
                evidence=tuple(sorted(dedupe(item.evidence for item in items), key=str.casefold))[:5],
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                value=dedupe(item.value for item in items)[0],
                suggested_owners=_OWNER_SUGGESTIONS[category],
                planning_notes=_PLANNING_NOTES[category],
                gap_messages=gap_messages,
            )
        )
    return result


def _gap_messages(candidates: list[_Candidate]) -> tuple[str, ...]:
    if not candidates:
        return ()
    categories = {candidate.category for candidate in candidates}
    flags = []
    if "suppression_list" not in categories:
        flags.append("missing_suppression")
    if "provider_webhook" not in categories:
        flags.append("missing_webhook")
    if "deliverability_metrics" not in categories:
        flags.append("missing_metrics")
    return tuple(_MISSING_DETAIL_MESSAGES[flag] for flag in flags)


def _summary(requirements: tuple[SourceEmailBounceRequirement, ...], source_count: int) -> dict[str, Any]:
    category_counts = {category: 0 for category in _CATEGORY_ORDER}
    confidence_counts = {confidence: 0 for confidence in _CONFIDENCE_ORDER}
    for requirement in requirements:
        category_counts[requirement.category] += 1
        confidence_counts[requirement.confidence] += 1
    gap_messages = dedupe(message for requirement in requirements for message in requirement.gap_messages)
    missing_flags = [flag for flag, message in _MISSING_DETAIL_MESSAGES.items() if message in gap_messages]
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "categories": [category for category in _CATEGORY_ORDER if category_counts[category]],
        "category_counts": category_counts,
        "confidence_counts": confidence_counts,
        "missing_detail_flags": missing_flags,
        "gap_messages": gap_messages,
        "owner_suggestions": dedupe(owner for requirement in requirements for owner in requirement.suggested_owners),
        "status": "no_email_bounce_language" if not requirements else ("needs_bounce_details" if missing_flags else "ready_for_planning"),
    }


def _confidence(text: str) -> EmailBounceConfidence:
    return "high" if re.search(r"\b(?:hard bounce|soft bounce|suppression|complaint|provider webhook|deliverability)\b", text, re.I) else "medium"


def _value(category: EmailBounceCategory, text: str) -> str | None:
    match = _CATEGORY_PATTERNS[category].search(text)
    return optional_text(match.group(0).casefold()) if match else None
