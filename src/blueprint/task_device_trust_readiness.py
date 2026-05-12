"""Assess readiness for device trust and managed-device enforcement tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskDeviceTrustReadinessFinding = SimpleReadinessRecord
TaskDeviceTrustReadinessPlan = SimpleReadinessPlan

_SIGNALS = {
    "trusted_device": re.compile(r"\b(?:trusted device|device trust|trusted endpoint|known device|device-bound access)\b", re.I),
    "device_posture": re.compile(r"\b(?:device posture|posture check|endpoint posture|security posture|posture signal)\b", re.I),
    "managed_device": re.compile(r"\b(?:managed device|managed endpoint|corporate device|company managed|enrolled device)\b", re.I),
    "mdm": re.compile(r"\b(?:mdm|mobile device management|jamf|intune|workspace one|device management)\b", re.I),
    "device_attestation": re.compile(r"\b(?:device attestation|attested device|hardware attestation|device certificate|certificate-bound)\b", re.I),
    "device_compliance": re.compile(r"\b(?:device compliance|compliant device|compliance status|healthy device|jailbreak|rooted device)\b", re.I),
}
_PATH_SIGNALS = {
    "trusted_device": re.compile(r"device[-_]?trust|trusted[-_]?devices?|devices?", re.I),
    "device_posture": re.compile(r"posture|endpoint[-_]?security", re.I),
    "managed_device": re.compile(r"managed[-_]?devices?|enrollment|corporate[-_]?device", re.I),
    "mdm": re.compile(r"mdm|jamf|intune|workspace[-_]?one", re.I),
    "device_attestation": re.compile(r"attestation|certificates?|certificate[-_]?bound", re.I),
    "device_compliance": re.compile(r"compliance|compliant|jailbreak|rooted", re.I),
}
_CRITERIA = {
    "enrollment_source": re.compile(r"\b(?:enrollment source|device enrollment|mdm source|source of truth|jamf|intune|device inventory|managed inventory)\b", re.I),
    "posture_checks": re.compile(r"\b(?:posture checks?|posture signal|os version|disk encryption|screen lock|jailbreak|rooted|edr|compliance check)\b", re.I),
    "enforcement_scope": re.compile(r"\b(?:enforcement scope|scope|apps? in scope|users? in scope|groups? in scope|conditional access|block access|allowlist)\b", re.I),
    "exception_path": re.compile(r"\b(?:exception path|exception workflow|breakglass|break glass|temporary exception|waiver|fallback access|support override)\b", re.I),
    "audit_monitoring": re.compile(r"\b(?:audit|monitoring|monitor|evidence|logs?|alerts?|dashboard|compliance report|access report)\b", re.I),
}
_GUIDANCE = {
    "enrollment_source": "Identify the MDM, inventory, or enrollment source used for device trust.",
    "posture_checks": "Define required posture checks such as OS, encryption, screen lock, EDR, or jailbreak status.",
    "enforcement_scope": "State the users, apps, groups, and access paths where device trust is enforced.",
    "exception_path": "Define exception, breakglass, or temporary waiver handling.",
    "audit_monitoring": "Add audit evidence, monitoring, alerts, and reports for device enforcement.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:device trust|managed device|trusted device|device posture|mdm|device compliance)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_device_trust_readiness_plan(source: Any) -> TaskDeviceTrustReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task Device Trust Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_device_trust_readiness(source: Any) -> TaskDeviceTrustReadinessPlan:
    return build_task_device_trust_readiness_plan(source)


def extract_task_device_trust_readiness(source: Any) -> TaskDeviceTrustReadinessPlan:
    return build_task_device_trust_readiness_plan(source)


def generate_task_device_trust_readiness(source: Any) -> TaskDeviceTrustReadinessPlan:
    return build_task_device_trust_readiness_plan(source)


def derive_task_device_trust_readiness(source: Any) -> TaskDeviceTrustReadinessPlan:
    return build_task_device_trust_readiness_plan(source)


def summarize_task_device_trust_readiness(source: Any) -> TaskDeviceTrustReadinessPlan:
    return build_task_device_trust_readiness_plan(source)


def recommend_task_device_trust_readiness(source: Any) -> TaskDeviceTrustReadinessPlan:
    return build_task_device_trust_readiness_plan(source)


def task_device_trust_readiness_plan_to_dict(report: TaskDeviceTrustReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_device_trust_readiness_plan_to_dict.__test__ = False


def task_device_trust_readiness_plan_to_dicts(report: TaskDeviceTrustReadinessPlan) -> list[dict[str, Any]]:
    return report.to_dicts()


task_device_trust_readiness_plan_to_dicts.__test__ = False


def task_device_trust_readiness_plan_to_markdown(report: TaskDeviceTrustReadinessPlan) -> str:
    return report.to_markdown()


task_device_trust_readiness_plan_to_markdown.__test__ = False
