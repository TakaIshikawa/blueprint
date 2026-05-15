"""Extract source-level license seat management requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceLicenseSeatManagementRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceLicenseSeatManagementRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("seat_assignment", re.compile(r"\b(?:seat assignment|assign seats?|seat allocation|license assignment|user seats?)\b", re.I), ("assignment rule",), {"assignment rule": re.compile(r"\b(?:assign|allocation|invite|claim|user|group|team|automatic|manual)\b", re.I)}),
    KeywordRequirementSpec("seat_limit", re.compile(r"\b(?:seat limit|license limit|seat cap|licensed seats?|maximum seats?|seat quota)\b", re.I), ("limit value",), {"limit value": re.compile(r"\b(?:\d+|maximum of|up to|no more than|per plan|quota of|cap of)\b", re.I)}),
    KeywordRequirementSpec("overage_policy", re.compile(r"\b(?:overage policy|seat overage|over limit|exceed(?:s|ing)? seats?|extra seats?|true[- ]?up)\b", re.I), ("overage handling",), {"overage handling": re.compile(r"\b(?:block|allow|charge|bill|true[- ]?up|approval|notify|grace)\b", re.I)}),
    KeywordRequirementSpec("role_mapping", re.compile(r"\b(?:role mapping|seat role|license role|admin role|member role|permission mapping)\b", re.I), ("role mapping",), {"role mapping": re.compile(r"\b(?:owner|viewer|rbac|maps? .{0,40} roles?|roles? .{0,40} permissions?)\b", re.I)}),
    KeywordRequirementSpec("deprovisioning", re.compile(r"\b(?:deprovisioning|deprovision seats?|remove seats?|revoke seats?|unassign seats?|offboard users?)\b", re.I), ("deprovisioning rule",), {"deprovisioning rule": re.compile(r"\b(?:remove|revoke|unassign|offboard|inactive|disabled|release|transfer)\b", re.I)}),
    KeywordRequirementSpec("audit_trail", re.compile(r"\b(?:audit trail|seat audit|license audit|assignment history|seat history|change log)\b", re.I), ("audit events",), {"audit events": re.compile(r"\b(?:audit|history|actor|timestamp|change|event|log)\b", re.I)}),
    KeywordRequirementSpec("admin_controls", re.compile(r"\b(?:admin controls?|seat admin|license admin|admin console|bulk seat|manage seats?)\b", re.I), ("admin action",), {"admin action": re.compile(r"\b(?:admin|console|bulk|manage|approve|delegate|control)\b", re.I)}),
    KeywordRequirementSpec("usage_reporting", re.compile(r"\b(?:usage reporting|seat usage|license usage|utilization report|unused seats?|seat metrics?)\b", re.I), ("usage metric",), {"usage metric": re.compile(r"\b(?:utilization|unused|active seats?|dashboard|metrics?|export)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:license seat management|seat management|license management|licensed seats?|user seats?|seat allocation)\b", re.I)
_STRUCTURED = re.compile(r"(?:license|seat|admin|billing|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:license seat management|seat management|license management|licensed seats?|user seats?)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:license seat management|seat management|license management|licensed seats?|user seats?)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {
    "missing_seat_limit": ("limit value",),
    "missing_overage_policy": ("overage handling",),
    "missing_role_mapping": ("role mapping",),
    "missing_deprovisioning": ("deprovisioning rule",),
    "missing_usage_reporting": ("usage metric",),
}


def build_source_license_seat_management_requirements(source: Any) -> SourceLicenseSeatManagementRequirementsReport:
    return build_keyword_requirements_report(source, title="Source License Seat Management Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_license_seat_management_requirements(source: Any) -> SourceLicenseSeatManagementRequirementsReport:
    return build_source_license_seat_management_requirements(source)


def generate_source_license_seat_management_requirements(source: Any) -> SourceLicenseSeatManagementRequirementsReport:
    return build_source_license_seat_management_requirements(source)


def derive_source_license_seat_management_requirements(source: Any) -> SourceLicenseSeatManagementRequirementsReport:
    return build_source_license_seat_management_requirements(source)


def summarize_source_license_seat_management_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceLicenseSeatManagementRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_license_seat_management_requirements(source_or_result).summary


def source_license_seat_management_requirements_to_dict(report: SourceLicenseSeatManagementRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_license_seat_management_requirements_to_dict.__test__ = False


def source_license_seat_management_requirements_to_dicts(requirements: SourceLicenseSeatManagementRequirementsReport | list[SourceLicenseSeatManagementRequirement] | tuple[SourceLicenseSeatManagementRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceLicenseSeatManagementRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_license_seat_management_requirements_to_dicts.__test__ = False


def source_license_seat_management_requirements_to_markdown(report: SourceLicenseSeatManagementRequirementsReport) -> str:
    return report.to_markdown()


source_license_seat_management_requirements_to_markdown.__test__ = False
