"""Extract source-level feature adoption requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import (
    KeywordRequirement as SourceFeatureAdoptionRequirement,
    KeywordRequirementSpec,
    KeywordRequirementsReport as SourceFeatureAdoptionRequirementsReport,
    build_keyword_requirements_report,
)


_SPECS = (
    KeywordRequirementSpec("activation_metric", re.compile(r"\b(?:activation metric|activated users?|activation event|first value|aha moment|key action)\b", re.I), ("activation metric",), {"activation metric": re.compile(r"\b(?:activation event|first value|aha|activated users?|key action)\b", re.I)}),
    KeywordRequirementSpec("cohort_segmentation", re.compile(r"\b(?:cohort segmentation|target cohort|user cohort|segment adoption|persona segment|customer segment)\b", re.I), ("target cohort",), {"target cohort": re.compile(r"\b(?:persona|plan tier|new users?|existing users?|enterprise|customer segment|admin users?)\b", re.I)}),
    KeywordRequirementSpec("funnel_milestones", re.compile(r"\b(?:funnel milestones?|adoption funnel|setup milestone|onboarding milestone|journey step)\b", re.I), ("funnel milestones",), {"funnel milestones": re.compile(r"\b(?:funnel|milestone|step|setup|onboarding|complete)\b", re.I)}),
    KeywordRequirementSpec("adoption_targets", re.compile(r"\b(?:adoption target|target adoption|goal adoption|usage target|activation target|success target)\b", re.I), ("adoption target",), {"adoption target": re.compile(r"\b(?:target|goal|%|percent|\d+|by launch|within)\b", re.I)}),
    KeywordRequirementSpec("experiment_linkage", re.compile(r"\b(?:experiment linkage|ab test|a/b test|experiment|feature flag experiment|holdout|variant)\b", re.I), ("experiment linkage",), {"experiment linkage": re.compile(r"\b(?:experiment|variant|holdout|flag|ab test|a/b)\b", re.I)}),
    KeywordRequirementSpec("lifecycle_messaging", re.compile(r"\b(?:lifecycle messaging|email nudge|push notification|drip campaign|re-engagement|lifecycle campaign)\b", re.I), ("messaging trigger",), {"messaging trigger": re.compile(r"\b(?:email|push|nudge|campaign|trigger|drip|message)\b", re.I)}),
    KeywordRequirementSpec("in_product_education", re.compile(r"\b(?:in-product education|tooltip|coach mark|guided tour|checklist|empty state education|onboarding guide)\b", re.I), ("education surface",), {"education surface": re.compile(r"\b(?:tooltip|coach mark|tour|checklist|guide|modal|empty state)\b", re.I)}),
    KeywordRequirementSpec("success_reporting", re.compile(r"\b(?:success reporting|adoption reporting|usage report|adoption dashboard|success dashboard|weekly report)\b", re.I), ("reporting cadence",), {"reporting cadence": re.compile(r"\b(?:dashboard|weekly|monthly|cadence|owner|metric)\b", re.I)}),
    KeywordRequirementSpec("feedback_loops", re.compile(r"\b(?:feedback loop|user feedback|survey|interview|feedback collection|qualitative feedback)\b", re.I), ("feedback loop",), {"feedback loop": re.compile(r"\b(?:survey|interview|csat|nps|collection channel|qualitative)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:feature adoption|adoption planning|activation|adoption funnel|product adoption|usage adoption)\b", re.I)
_STRUCTURED = re.compile(r"(?:feature|adoption|activation|funnel|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:feature adoption|adoption planning|activation|adoption funnel)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:feature adoption|adoption planning|activation|adoption funnel)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {
    "missing_activation_metric": ("activation metric",),
    "missing_target_cohort": ("target cohort",),
    "missing_reporting_feedback_loop": ("reporting cadence", "feedback loop"),
}


def build_source_feature_adoption_requirements(source: Any) -> SourceFeatureAdoptionRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Feature Adoption Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_feature_adoption_requirements(source: Any) -> SourceFeatureAdoptionRequirementsReport:
    return build_source_feature_adoption_requirements(source)


def generate_source_feature_adoption_requirements(source: Any) -> SourceFeatureAdoptionRequirementsReport:
    return build_source_feature_adoption_requirements(source)


def derive_source_feature_adoption_requirements(source: Any) -> SourceFeatureAdoptionRequirementsReport:
    return build_source_feature_adoption_requirements(source)


def summarize_source_feature_adoption_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceFeatureAdoptionRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_feature_adoption_requirements(source_or_result).summary


def source_feature_adoption_requirements_to_dict(report: SourceFeatureAdoptionRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_feature_adoption_requirements_to_dict.__test__ = False


def source_feature_adoption_requirements_to_dicts(requirements: SourceFeatureAdoptionRequirementsReport | list[SourceFeatureAdoptionRequirement] | tuple[SourceFeatureAdoptionRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceFeatureAdoptionRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_feature_adoption_requirements_to_dicts.__test__ = False


def source_feature_adoption_requirements_to_markdown(report: SourceFeatureAdoptionRequirementsReport) -> str:
    return report.to_markdown()


source_feature_adoption_requirements_to_markdown.__test__ = False
