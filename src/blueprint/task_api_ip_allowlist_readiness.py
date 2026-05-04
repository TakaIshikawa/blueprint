"""Plan API IP allowlist readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


IPAllowlistSignal = Literal[
    "allowlist_middleware",
    "cidr_validation",
    "tenant_management",
    "ip_auth_integration",
    "bypass_configuration",
    "update_automation",
    "geo_blocking_setup",
    "violation_logging",
]
IPAllowlistSafeguard = Literal[
    "middleware_integration",
    "cidr_range_validation",
    "tenant_isolation_enforcement",
    "ip_based_auth_logic",
    "endpoint_bypass_config",
    "dynamic_update_handling",
    "geo_blocking_implementation",
    "violation_audit_logging",
]
IPAllowlistReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[IPAllowlistReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[IPAllowlistSignal, ...] = (
    "allowlist_middleware",
    "cidr_validation",
    "tenant_management",
    "ip_auth_integration",
    "bypass_configuration",
    "update_automation",
    "geo_blocking_setup",
    "violation_logging",
)
_SAFEGUARD_ORDER: tuple[IPAllowlistSafeguard, ...] = (
    "middleware_integration",
    "cidr_range_validation",
    "tenant_isolation_enforcement",
    "ip_based_auth_logic",
    "endpoint_bypass_config",
    "dynamic_update_handling",
    "geo_blocking_implementation",
    "violation_audit_logging",
)
_SIGNAL_PATTERNS: dict[IPAllowlistSignal, re.Pattern[str]] = {
    "allowlist_middleware": re.compile(
        r"\b(?:allowlist middleware|middleware|ip filter|network filter|"
        r"ip check|allowlist check|ip allowlist)\b",
        re.I,
    ),
    "cidr_validation": re.compile(
        r"\b(?:cidr(?: validation| validate| check| range)?|"
        r"validate (?:cidr|ip range)|cidr|ip range validation|subnet validation)\b",
        re.I,
    ),
    "tenant_management": re.compile(
        r"\b(?:tenant|multi[- ]?tenant|per[- ]?tenant|tenant[- ]?specific|tenant isolation)\b",
        re.I,
    ),
    "ip_auth_integration": re.compile(
        r"\b(?:ip[- ]?based auth|ip authentication|authenticate by ip|source ip|ip-based)\b",
        re.I,
    ),
    "bypass_configuration": re.compile(
        r"\b(?:bypass|exempt|exemption|public endpoint|skip allowlist)\b",
        re.I,
    ),
    "update_automation": re.compile(
        r"\b(?:dynamic update|runtime update|update allowlist|modify allowlist|automation)\b",
        re.I,
    ),
    "geo_blocking_setup": re.compile(
        r"\b(?:geo[- ]?block|geo[- ]?fenc|geo[- ]?restrict|geographic|country|region)\b",
        re.I,
    ),
    "violation_logging": re.compile(
        r"\b(?:violation|denied access|blocked request|unauthorized|audit log|access log)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[IPAllowlistSignal, re.Pattern[str]] = {
    "allowlist_middleware": re.compile(r"middleware|filter|allowlist", re.I),
    "cidr_validation": re.compile(r"cidr|validation|range", re.I),
    "tenant_management": re.compile(r"tenant|isolation", re.I),
    "ip_auth_integration": re.compile(r"auth|ip|authentication", re.I),
    "bypass_configuration": re.compile(r"bypass|exempt|public", re.I),
    "update_automation": re.compile(r"update|dynamic|automation", re.I),
    "geo_blocking_setup": re.compile(r"geo|country|region|location", re.I),
    "violation_logging": re.compile(r"log|audit|violation", re.I),
}
_SAFEGUARD_PATTERNS: dict[IPAllowlistSafeguard, re.Pattern[str]] = {
    "middleware_integration": re.compile(
        r"\b(?:(?:middleware|filter).{0,80}(?:integration|install|setup|config|implement)|"
        r"(?:integration|install|setup|config|implement).{0,80}(?:middleware|filter)|"
        r"express|koa|fastify|middleware)\b",
        re.I,
    ),
    "cidr_range_validation": re.compile(
        r"\b(?:(?:cidr|ip range).{0,80}(?:validation|validate|check|verify|parse)|"
        r"(?:validation|validate|check|verify|parse).{0,80}(?:cidr|ip range)|"
        r"ipaddr|ip-cidr|netmask)\b",
        re.I,
    ),
    "tenant_isolation_enforcement": re.compile(
        r"\b(?:(?:tenant|isolation).{0,80}(?:enforcement|enforce|check|validate|separate)|"
        r"(?:enforcement|enforce|check|validate|separate).{0,80}(?:tenant|isolation)|"
        r"tenant[- ]?specific|per[- ]?tenant)\b",
        re.I,
    ),
    "ip_based_auth_logic": re.compile(
        r"\b(?:(?:ip[- ]?based|source ip).{0,80}(?:auth|authentication|verification|check)|"
        r"(?:auth|authentication|verification|check).{0,80}(?:ip[- ]?based|source ip)|"
        r"x-forwarded-for|x-real-ip)\b",
        re.I,
    ),
    "endpoint_bypass_config": re.compile(
        r"\b(?:(?:bypass|exempt|public).{0,80}(?:endpoint|route|path|config)|"
        r"(?:endpoint|route|path|config).{0,80}(?:bypass|exempt|public)|"
        r"allow[- ]?list[- ]?bypass)\b",
        re.I,
    ),
    "dynamic_update_handling": re.compile(
        r"\b(?:(?:dynamic|runtime).{0,80}(?:update|reload|refresh|modify)|"
        r"(?:update|reload|refresh|modify).{0,80}(?:dynamic|runtime|allowlist)|"
        r"cache[- ]?invalidation)\b",
        re.I,
    ),
    "geo_blocking_implementation": re.compile(
        r"\b(?:(?:geo|geoip|maxmind).{0,80}(?:block|restrict|database|lookup|integration)|"
        r"(?:block|restrict|database|lookup|integration).{0,80}(?:geo|geoip|country)|"
        r"country[- ]?code|geo[- ]?location)\b",
        re.I,
    ),
    "violation_audit_logging": re.compile(
        r"\b(?:(?:violation|denied|blocked).{0,80}(?:log|audit|record|track)|"
        r"(?:log|audit|record|track).{0,80}(?:violation|denied|blocked|unauthorized)|"
        r"audit[- ]?trail|access[- ]?log)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:ip allowlist|allowlist|cidr|network restriction)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[IPAllowlistSafeguard, str] = {
    "middleware_integration": "Integrate IP allowlist middleware (e.g., Express, Koa) to filter requests based on source IP.",
    "cidr_range_validation": "Implement CIDR range validation library (e.g., ipaddr.js, ip-cidr) to parse and validate IP ranges.",
    "tenant_isolation_enforcement": "Enforce tenant-specific allowlist isolation to prevent cross-tenant access.",
    "ip_based_auth_logic": "Implement IP-based authentication logic with proper source IP extraction (X-Forwarded-For, X-Real-IP).",
    "endpoint_bypass_config": "Configure endpoint-level bypass for public routes that should skip IP allowlist checks.",
    "dynamic_update_handling": "Implement dynamic allowlist update mechanism with cache invalidation and propagation.",
    "geo_blocking_implementation": "Integrate geo-blocking with GeoIP database (e.g., MaxMind) for country/region restrictions.",
    "violation_audit_logging": "Add comprehensive violation logging with audit trail for denied requests and unauthorized access.",
}


@dataclass(frozen=True, slots=True)
class TaskApiIPAllowlistReadinessFinding:
    """API IP allowlist readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[IPAllowlistSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[IPAllowlistSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[IPAllowlistSafeguard, ...] = field(default_factory=tuple)
    readiness: IPAllowlistReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actionable_remediations: tuple[str, ...] = field(default_factory=tuple)

    @property
    def actionable_gaps(self) -> tuple[str, ...]:
        """Compatibility view for readiness modules that expose gaps."""
        return self.actionable_remediations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "actionable_remediations": list(self.actionable_remediations),
        }


@dataclass(frozen=True, slots=True)
class TaskApiIPAllowlistReadinessPlan:
    """Plan-level API IP allowlist readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiIPAllowlistReadinessFinding, ...] = field(default_factory=tuple)
    ip_allowlist_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiIPAllowlistReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "ip_allowlist_task_ids": list(self.ip_allowlist_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }


def plan_task_api_ip_allowlist_readiness(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiIPAllowlistReadinessPlan:
    """Generate API IP allowlist readiness safeguards for all tasks in a plan."""
    plan_id, tasks = _plan_payload(plan)
    findings: list[TaskApiIPAllowlistReadinessFinding] = []
    ip_allowlist_task_ids: list[str] = []
    not_applicable_task_ids: list[str] = []

    for task in tasks:
        task_id, task_payload = _task_payload(task)
        if not task_id:
            continue

        signals = _detect_signals(task_payload)
        if _has_no_impact(task_payload) or not signals:
            not_applicable_task_ids.append(task_id)
            continue

        ip_allowlist_task_ids.append(task_id)
        safeguards = _detect_safeguards(task_payload, signals)
        missing = tuple(s for s in _expected_safeguards(signals) if s not in safeguards)
        readiness = _assess_readiness(signals, safeguards, missing)
        evidence = _collect_evidence(task_payload, signals, safeguards)
        remediations = tuple(_REMEDIATIONS[s] for s in missing)

        findings.append(
            TaskApiIPAllowlistReadinessFinding(
                task_id=task_id,
                title=_task_title(task_payload),
                detected_signals=signals,
                present_safeguards=safeguards,
                missing_safeguards=missing,
                readiness=readiness,
                evidence=evidence,
                actionable_remediations=remediations,
            )
        )

    return TaskApiIPAllowlistReadinessPlan(
        plan_id=plan_id,
        findings=tuple(findings),
        ip_allowlist_task_ids=tuple(ip_allowlist_task_ids),
        not_applicable_task_ids=tuple(not_applicable_task_ids),
        summary=_summary(findings, ip_allowlist_task_ids, not_applicable_task_ids),
    )


def _plan_payload(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[str | None, list[Any]]:
    if isinstance(plan, ExecutionPlan):
        return plan.id, list(plan.tasks)
    if isinstance(plan, Mapping):
        try:
            validated = ExecutionPlan.model_validate(plan)
            return validated.id, list(validated.tasks)
        except (TypeError, ValueError, ValidationError):
            pass
        return plan.get("id"), plan.get("tasks", [])
    if hasattr(plan, "id") and hasattr(plan, "tasks"):
        return getattr(plan, "id", None), list(getattr(plan, "tasks", []))
    return None, []


def _task_payload(task: ExecutionTask | Mapping[str, Any] | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(task, ExecutionTask):
        return task.id, dict(task.model_dump(mode="python"))
    if isinstance(task, Mapping):
        try:
            validated = ExecutionTask.model_validate(task)
            return validated.id, dict(validated.model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            pass
        return task.get("id"), dict(task)
    if hasattr(task, "id"):
        return getattr(task, "id", None), _object_to_dict(task)
    return None, {}


def _object_to_dict(obj: object) -> dict[str, Any]:
    result = {}
    for attr in ("id", "title", "description", "acceptance_criteria", "files", "dependencies"):
        if hasattr(obj, attr):
            result[attr] = getattr(obj, attr)
    return result


def _task_title(payload: Mapping[str, Any]) -> str:
    return str(payload.get("title", "")).strip() or "Untitled task"


def _has_no_impact(payload: Mapping[str, Any]) -> bool:
    searchable = " ".join(str(v) for v in payload.values() if v)
    return bool(_NO_IMPACT_RE.search(searchable))


def _detect_signals(payload: Mapping[str, Any]) -> tuple[IPAllowlistSignal, ...]:
    text_signals = _text_signals(payload)
    path_signals = _path_signals(payload)
    combined = set(text_signals) | set(path_signals)
    return tuple(s for s in _SIGNAL_ORDER if s in combined)


def _text_signals(payload: Mapping[str, Any]) -> list[IPAllowlistSignal]:
    searchable = " ".join(
        str(payload.get(f, ""))
        for f in ("title", "description", "acceptance_criteria", "files")
        if payload.get(f)
    )
    return [signal for signal in _SIGNAL_ORDER if _SIGNAL_PATTERNS[signal].search(searchable)]


def _path_signals(payload: Mapping[str, Any]) -> list[IPAllowlistSignal]:
    files = payload.get("files", [])
    if isinstance(files, str):
        files = [files]
    paths = " ".join(str(PurePosixPath(f).as_posix()) for f in files if f)
    return [signal for signal in _SIGNAL_ORDER if _PATH_SIGNAL_PATTERNS[signal].search(paths)]


def _detect_safeguards(payload: Mapping[str, Any], signals: tuple[IPAllowlistSignal, ...]) -> tuple[IPAllowlistSafeguard, ...]:
    _ = signals  # signals parameter reserved for future use
    searchable = " ".join(str(v) for v in payload.values() if v)
    detected: list[IPAllowlistSafeguard] = [
        safeguard
        for safeguard in _SAFEGUARD_ORDER
        if _SAFEGUARD_PATTERNS[safeguard].search(searchable)
    ]
    return tuple(detected)


def _expected_safeguards(signals: tuple[IPAllowlistSignal, ...]) -> list[IPAllowlistSafeguard]:
    mapping: dict[IPAllowlistSignal, list[IPAllowlistSafeguard]] = {
        "allowlist_middleware": ["middleware_integration"],
        "cidr_validation": ["cidr_range_validation"],
        "tenant_management": ["tenant_isolation_enforcement"],
        "ip_auth_integration": ["ip_based_auth_logic"],
        "bypass_configuration": ["endpoint_bypass_config"],
        "update_automation": ["dynamic_update_handling"],
        "geo_blocking_setup": ["geo_blocking_implementation"],
        "violation_logging": ["violation_audit_logging"],
    }
    expected: set[IPAllowlistSafeguard] = set()
    for signal in signals:
        expected.update(mapping.get(signal, []))
    return [s for s in _SAFEGUARD_ORDER if s in expected]


def _assess_readiness(
    signals: tuple[IPAllowlistSignal, ...],
    safeguards: tuple[IPAllowlistSafeguard, ...],
    missing: tuple[IPAllowlistSafeguard, ...],
) -> IPAllowlistReadiness:
    if not signals:
        return "partial"
    expected = _expected_safeguards(signals)
    if not expected:
        return "strong"
    coverage = len(safeguards) / len(expected) if expected else 0
    if coverage >= 0.8:
        return "strong"
    if coverage >= 0.4:
        return "partial"
    return "weak"


def _collect_evidence(
    payload: Mapping[str, Any],
    signals: tuple[IPAllowlistSignal, ...],
    safeguards: tuple[IPAllowlistSafeguard, ...],
) -> tuple[str, ...]:
    evidence: list[str] = []
    searchable_fields = {
        "title": payload.get("title", ""),
        "description": payload.get("description", ""),
        "acceptance_criteria": payload.get("acceptance_criteria", ""),
        "files": payload.get("files", []),
    }

    for signal in signals[:3]:  # Limit evidence collection
        pattern = _SIGNAL_PATTERNS[signal]
        for field_name, value in searchable_fields.items():
            text = str(value) if value else ""
            if match := pattern.search(text):
                snippet = match.group(0)
                evidence.append(f"{field_name}: ...{snippet}...")
                break

    for safeguard in safeguards[:2]:  # Limit evidence collection
        pattern = _SAFEGUARD_PATTERNS[safeguard]
        for field_name, value in searchable_fields.items():
            text = str(value) if value else ""
            if match := pattern.search(text):
                snippet = match.group(0)
                evidence.append(f"{field_name}: ...{snippet}...")
                break

    return tuple(evidence[:5])


def _summary(
    findings: list[TaskApiIPAllowlistReadinessFinding],
    ip_allowlist_task_ids: list[str],
    not_applicable_task_ids: list[str],
) -> dict[str, Any]:
    return {
        "total_tasks": len(findings) + len(not_applicable_task_ids),
        "ip_allowlist_tasks": len(ip_allowlist_task_ids),
        "not_applicable_tasks": len(not_applicable_task_ids),
        "readiness_distribution": {
            "strong": sum(1 for f in findings if f.readiness == "strong"),
            "partial": sum(1 for f in findings if f.readiness == "partial"),
            "weak": sum(1 for f in findings if f.readiness == "weak"),
        },
        "most_common_gaps": _top_gaps([f.missing_safeguards for f in findings]),
    }


def _top_gaps(all_gaps: list[tuple[IPAllowlistSafeguard, ...]]) -> list[str]:
    from collections import Counter

    flat = [gap for gaps in all_gaps for gap in gaps]
    return [gap for gap, _ in Counter(flat).most_common(3)]


__all__ = [
    "IPAllowlistSignal",
    "IPAllowlistSafeguard",
    "IPAllowlistReadiness",
    "TaskApiIPAllowlistReadinessFinding",
    "TaskApiIPAllowlistReadinessPlan",
    "plan_task_api_ip_allowlist_readiness",
]
