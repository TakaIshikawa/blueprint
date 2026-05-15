"""Extract source-level chargeback management requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceChargebackManagementRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceChargebackManagementRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("dispute_intake", re.compile(r"\b(?:dispute intake|chargeback intake|incoming disputes?|dispute webhook|chargeback case)\b", re.I), ("intake source",), {"intake source": re.compile(r"\b(?:webhook|portal|processor|queue|case|ticket|intake from)\b", re.I)}),
    KeywordRequirementSpec("evidence_collection", re.compile(r"\b(?:evidence collection|collect evidence|chargeback evidence|dispute evidence|evidence packet)\b", re.I), ("evidence detail",), {"evidence detail": re.compile(r"\b(?:receipt|invoice|usage log|shipping|ip address|customer message|evidence packet)\b", re.I)}),
    KeywordRequirementSpec("representment_deadline", re.compile(r"\b(?:representment deadline|dispute deadline|response deadline|chargeback deadline|submit by)\b", re.I), ("deadline rule",), {"deadline rule": re.compile(r"\b(?:\d+\s*days?|submit by|before|sla|calendar|deadline at)\b", re.I)}),
    KeywordRequirementSpec("processor_integration", re.compile(r"\b(?:processor integration|processor sync|stripe dispute|adyen dispute|gateway dispute)\b", re.I), ("processor integration",), {"processor integration": re.compile(r"\b(?:stripe|adyen|braintree|gateway|webhook|api|processor status)\b", re.I)}),
    KeywordRequirementSpec("customer_account_hold", re.compile(r"\b(?:customer account hold|account hold|account impact|suspend account|entitlement hold)\b", re.I), ("account impact",), {"account impact": re.compile(r"\b(?:suspend|freeze|entitlement|access|release hold|account status)\b", re.I)}),
    KeywordRequirementSpec("fee_tracking", re.compile(r"\b(?:fee tracking|chargeback fee|dispute fee|processor fee|fee ledger)\b", re.I), ("fee detail",), {"fee detail": re.compile(r"\b(?:fee amount|ledger|invoice|cost center|processor fee|accounting)\b", re.I)}),
    KeywordRequirementSpec("win_loss_reporting", re.compile(r"\b(?:win loss reporting|win/loss reporting|dispute reporting|chargeback metrics|representment win rate)\b", re.I), ("reporting detail",), {"reporting detail": re.compile(r"\b(?:win rate|loss rate|dashboard|metric|reason code)\b", re.I)}),
    KeywordRequirementSpec("audit_trail", re.compile(r"\b(?:audit trail|dispute audit|chargeback audit|case history|decision log)\b", re.I), ("audit events",), {"audit events": re.compile(r"\b(?:actor|timestamp|decision|event|history|audit log)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:chargeback management|chargeback|payment dispute|card dispute|representment)\b", re.I)
_STRUCTURED = re.compile(r"(?:chargeback|dispute|billing|support|processor|compliance|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:chargeback management|chargeback|payment dispute|representment)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:chargeback management|chargeback|payment dispute|representment)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_evidence": ("evidence detail",), "missing_deadline": ("deadline rule",), "missing_processor_integration": ("processor integration",), "missing_account_impact": ("account impact",), "missing_fee": ("fee detail",), "missing_reporting": ("reporting detail",)}


def build_source_chargeback_management_requirements(source: Any) -> SourceChargebackManagementRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Chargeback Management Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_chargeback_management_requirements(source: Any) -> SourceChargebackManagementRequirementsReport:
    return build_source_chargeback_management_requirements(source)


def generate_source_chargeback_management_requirements(source: Any) -> SourceChargebackManagementRequirementsReport:
    return build_source_chargeback_management_requirements(source)


def derive_source_chargeback_management_requirements(source: Any) -> SourceChargebackManagementRequirementsReport:
    return build_source_chargeback_management_requirements(source)


def summarize_source_chargeback_management_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceChargebackManagementRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_chargeback_management_requirements(source_or_result).summary


def source_chargeback_management_requirements_to_dict(report: SourceChargebackManagementRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_chargeback_management_requirements_to_dict.__test__ = False


def source_chargeback_management_requirements_to_dicts(requirements: SourceChargebackManagementRequirementsReport | list[SourceChargebackManagementRequirement] | tuple[SourceChargebackManagementRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceChargebackManagementRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_chargeback_management_requirements_to_dicts.__test__ = False


def source_chargeback_management_requirements_to_markdown(report: SourceChargebackManagementRequirementsReport) -> str:
    return report.to_markdown()


source_chargeback_management_requirements_to_markdown.__test__ = False
