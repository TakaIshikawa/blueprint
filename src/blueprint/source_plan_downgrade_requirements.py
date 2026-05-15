"""Extract source-level plan downgrade requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourcePlanDowngradeRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourcePlanDowngradeRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("downgrade_eligibility", re.compile(r"\b(?:downgrade eligibility|eligible to downgrade|downgrade rules?|downgrade criteria|can downgrade)\b", re.I), ("eligibility rule",), {"eligibility rule": re.compile(r"\b(?:eligible when|only if|criteria|minimum term|renewal|balance|contract)\b", re.I)}),
    KeywordRequirementSpec("feature_removal", re.compile(r"\b(?:feature removal|remove features?|lost features?|feature access|disabled features?)\b", re.I), ("feature list",), {"feature list": re.compile(r"\b(?:remove|disable|features?:|exports?|advanced|premium|limits?)\b", re.I)}),
    KeywordRequirementSpec("entitlement_transition", re.compile(r"\b(?:entitlement transition|entitlement change|access transition|plan entitlement|capability transition)\b", re.I), ("entitlement transition",), {"entitlement transition": re.compile(r"\b(?:transition to|map|migrate|preserve|revoke|entitlement state|access level|lower tier)\b", re.I)}),
    KeywordRequirementSpec("billing_proration", re.compile(r"\b(?:billing proration|proration|prorated credit|billing credit|downgrade credit)\b", re.I), ("proration rule",), {"proration rule": re.compile(r"\b(?:prorate by|unused time|credit|refund|invoice|billing cycle|amount)\b", re.I)}),
    KeywordRequirementSpec("scheduled_effective_date", re.compile(r"\b(?:scheduled effective date|effective date|takes effect|downgrade date|next renewal)\b", re.I), ("effective date",), {"effective date": re.compile(r"\b(?:immediately|next renewal|billing cycle end|on \d{4}-\d{2}-\d{2}|scheduled for)\b", re.I)}),
    KeywordRequirementSpec("customer_notice", re.compile(r"\b(?:customer notice|notify customer|downgrade notification|customer email|notice period)\b", re.I), ("notice detail",), {"notice detail": re.compile(r"\b(?:email|in-app|sms|days? before|template|message|notice period)\b", re.I)}),
    KeywordRequirementSpec("data_retention", re.compile(r"\b(?:data retention|retain data|data access|archive data|delete data)\b", re.I), ("retention rule",), {"retention rule": re.compile(r"\b(?:retain|archive|delete|days?|export|grace|read-only)\b", re.I)}),
    KeywordRequirementSpec("support_exception", re.compile(r"\b(?:support exception|manual exception|support override|downgrade exception|agent override)\b", re.I), ("exception path",), {"exception path": re.compile(r"\b(?:support|agent|approval|override|ticket|manual)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:plan downgrade|downgrade plan|subscription downgrade|downgrade flow|lower tier)\b", re.I)
_STRUCTURED = re.compile(r"(?:plan|downgrade|subscription|billing|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:plan downgrade|subscription downgrade|downgrade flow)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:plan downgrade|subscription downgrade|downgrade flow)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_eligibility": ("eligibility rule",), "missing_effective_date": ("effective date",), "missing_proration": ("proration rule",), "missing_notice": ("notice detail",), "missing_entitlement_transition": ("entitlement transition",)}


def build_source_plan_downgrade_requirements(source: Any) -> SourcePlanDowngradeRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Plan Downgrade Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_plan_downgrade_requirements(source: Any) -> SourcePlanDowngradeRequirementsReport:
    return build_source_plan_downgrade_requirements(source)


def generate_source_plan_downgrade_requirements(source: Any) -> SourcePlanDowngradeRequirementsReport:
    return build_source_plan_downgrade_requirements(source)


def derive_source_plan_downgrade_requirements(source: Any) -> SourcePlanDowngradeRequirementsReport:
    return build_source_plan_downgrade_requirements(source)


def summarize_source_plan_downgrade_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourcePlanDowngradeRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_plan_downgrade_requirements(source_or_result).summary


def source_plan_downgrade_requirements_to_dict(report: SourcePlanDowngradeRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_plan_downgrade_requirements_to_dict.__test__ = False


def source_plan_downgrade_requirements_to_dicts(requirements: SourcePlanDowngradeRequirementsReport | list[SourcePlanDowngradeRequirement] | tuple[SourcePlanDowngradeRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourcePlanDowngradeRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_plan_downgrade_requirements_to_dicts.__test__ = False


def source_plan_downgrade_requirements_to_markdown(report: SourcePlanDowngradeRequirementsReport) -> str:
    return report.to_markdown()


source_plan_downgrade_requirements_to_markdown.__test__ = False
