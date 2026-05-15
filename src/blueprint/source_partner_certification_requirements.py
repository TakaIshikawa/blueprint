"""Extract source-level partner certification requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourcePartnerCertificationRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourcePartnerCertificationRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("certification_criteria", re.compile(r"\b(?:certification criteria|certification requirements?|criteria checklist|pass criteria|partner criteria)\b", re.I), ("certification criteria",), {"certification criteria": re.compile(r"\b(?:checklist|pass|quality|security|performance)\b", re.I)}),
    KeywordRequirementSpec("test_environment", re.compile(r"\b(?:test environment|sandbox environment|certification sandbox|partner test tenant|staging environment)\b", re.I), ("test environment",), {"test environment": re.compile(r"\b(?:sandbox|staging|tenant|test|environment|credentials)\b", re.I)}),
    KeywordRequirementSpec("submission_artifacts", re.compile(r"\b(?:submission artifacts?|certification artifacts?|artifact submission|evidence package|submitted artifacts?)\b", re.I), ("submission artifacts",), {"submission artifacts": re.compile(r"\b(?:screenshots?|logs?|test results?|documents?|package|evidence)\b", re.I)}),
    KeywordRequirementSpec("review_owner", re.compile(r"\b(?:review owner|certification owner|reviewer owner|approval owner|partner reviewer)\b", re.I), ("review owner",), {"review owner": re.compile(r"\b(?:owner|reviewer|partner team|security|solutions|approver)\b", re.I)}),
    KeywordRequirementSpec("version_compatibility", re.compile(r"\b(?:version compatibility|api version|sdk version|compatible versions?|version support)\b", re.I), ("version compatibility",), {"version compatibility": re.compile(r"\b(?:version|api|sdk|minimum|supported|compatibility|v\d+)\b", re.I)}),
    KeywordRequirementSpec("security_questionnaire", re.compile(r"\b(?:security questionnaire|security review questionnaire|vendor questionnaire|questionnaire response)\b", re.I), ("security questionnaire",), {"security questionnaire": re.compile(r"\b(?:questionnaire|soc 2|iso|security|privacy|vendor|responses?)\b", re.I)}),
    KeywordRequirementSpec("remediation_loop", re.compile(r"\b(?:remediation loop|remediation workflow|fix loop|retest workflow|defect remediation)\b", re.I), ("remediation loop",), {"remediation loop": re.compile(r"\b(?:fix|retest|defect|issue|remediation|sla|loop)\b", re.I)}),
    KeywordRequirementSpec("renewal_cadence", re.compile(r"\b(?:renewal cadence|recertification cadence|annual renewal|renewal schedule|certification renewal)\b", re.I), ("renewal cadence",), {"renewal cadence": re.compile(r"\b(?:annual|quarterly|yearly|schedule|renewal|recertification|cadence)\b", re.I)}),
    KeywordRequirementSpec("launch_approval", re.compile(r"\b(?:launch approval|go[- ]live approval|marketplace approval|approval to launch|launch gate)\b", re.I), ("launch approval",), {"launch approval": re.compile(r"\b(?:go-live|gate|marketplace|sign-off|approver)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:partner certification|certification planning|partner certification program|partner cert)\b", re.I)
_STRUCTURED = re.compile(r"(?:partner|certification|cert|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:partner certification|certification planning|partner cert)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:partner certification|certification planning|partner cert)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_certification_criteria": ("certification criteria",), "missing_submission_artifacts": ("submission artifacts",), "missing_launch_approval": ("launch approval",)}


def build_source_partner_certification_requirements(source: Any) -> SourcePartnerCertificationRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Partner Certification Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_partner_certification_requirements(source: Any) -> SourcePartnerCertificationRequirementsReport:
    return build_source_partner_certification_requirements(source)


def generate_source_partner_certification_requirements(source: Any) -> SourcePartnerCertificationRequirementsReport:
    return build_source_partner_certification_requirements(source)


def derive_source_partner_certification_requirements(source: Any) -> SourcePartnerCertificationRequirementsReport:
    return build_source_partner_certification_requirements(source)


def summarize_source_partner_certification_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourcePartnerCertificationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_partner_certification_requirements(source_or_result).summary


def source_partner_certification_requirements_to_dict(report: SourcePartnerCertificationRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_partner_certification_requirements_to_dict.__test__ = False


def source_partner_certification_requirements_to_dicts(requirements: SourcePartnerCertificationRequirementsReport | list[SourcePartnerCertificationRequirement] | tuple[SourcePartnerCertificationRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourcePartnerCertificationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_partner_certification_requirements_to_dicts.__test__ = False


def source_partner_certification_requirements_to_markdown(report: SourcePartnerCertificationRequirementsReport) -> str:
    return report.to_markdown()


source_partner_certification_requirements_to_markdown.__test__ = False
