"""Plan API password policy readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


PasswordPolicySignal = Literal[
    "password_complexity_rules",
    "password_length_requirements",
    "password_history_tracking",
    "password_expiration_policies",
    "password_reset_workflows",
    "breach_password_detection",
    "password_strength_meter",
    "password_hashing_algorithms",
]
PasswordPolicySafeguard = Literal[
    "complexity_rule_validation",
    "length_requirement_enforcement",
    "history_tracking_implementation",
    "expiration_automation_logic",
    "reset_workflow_security",
    "breach_database_integration",
    "strength_meter_accuracy",
    "hashing_algorithm_strength",
]
PasswordPolicyReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[PasswordPolicyReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[PasswordPolicySignal, ...] = (
    "password_complexity_rules",
    "password_length_requirements",
    "password_history_tracking",
    "password_expiration_policies",
    "password_reset_workflows",
    "breach_password_detection",
    "password_strength_meter",
    "password_hashing_algorithms",
)
_SAFEGUARD_ORDER: tuple[PasswordPolicySafeguard, ...] = (
    "complexity_rule_validation",
    "length_requirement_enforcement",
    "history_tracking_implementation",
    "expiration_automation_logic",
    "reset_workflow_security",
    "breach_database_integration",
    "strength_meter_accuracy",
    "hashing_algorithm_strength",
)
_SIGNAL_PATTERNS: dict[PasswordPolicySignal, re.Pattern[str]] = {
    "password_complexity_rules": re.compile(
        r"\b(?:password complexity|complexity rule|character requirement|mixed case|alphanumeric|special character)\b",
        re.I,
    ),
    "password_length_requirements": re.compile(
        r"\b(?:password length|minimum length|maximum length|length requirement|character count)\b",
        re.I,
    ),
    "password_history_tracking": re.compile(
        r"\b(?:password history|history tracking|password reuse|reuse prevention|historical password)\b",
        re.I,
    ),
    "password_expiration_policies": re.compile(
        r"\b(?:password expiration|password expiry|expiration policy|password rotation|password age)\b",
        re.I,
    ),
    "password_reset_workflows": re.compile(
        r"\b(?:password reset|reset workflow|password recovery|forgot password|reset token)\b",
        re.I,
    ),
    "breach_password_detection": re.compile(
        r"\b(?:breach password|pwned password|compromised password|breach detection|haveibeenpwned|hibp)\b",
        re.I,
    ),
    "password_strength_meter": re.compile(
        r"\b(?:password strength|strength meter|strength indicator|password quality|strength score)\b",
        re.I,
    ),
    "password_hashing_algorithms": re.compile(
        r"\b(?:password hash|hashing algorithm|bcrypt|argon2|scrypt|pbkdf2|cryptographic hash|salted hash)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[PasswordPolicySignal, re.Pattern[str]] = {
    "password_complexity_rules": re.compile(r"complexity|validation|character|rule", re.I),
    "password_length_requirements": re.compile(r"length|minimum|maximum|size", re.I),
    "password_history_tracking": re.compile(r"history|reuse|previous|archive", re.I),
    "password_expiration_policies": re.compile(r"expiration|expiry|rotation|age", re.I),
    "password_reset_workflows": re.compile(r"reset|recovery|forgot|token", re.I),
    "breach_password_detection": re.compile(r"breach|pwned|compromised|leak", re.I),
    "password_strength_meter": re.compile(r"strength|meter|indicator|quality", re.I),
    "password_hashing_algorithms": re.compile(r"hash|bcrypt|argon2|scrypt|crypt", re.I),
}
_SAFEGUARD_PATTERNS: dict[PasswordPolicySafeguard, re.Pattern[str]] = {
    "complexity_rule_validation": re.compile(
        r"\b(?:(?:complexity|character).{0,80}(?:validat|check|verify|enforce|test|assert)|"
        r"(?:validat|check|verify|enforce|test|assert).{0,80}(?:complexity|character)|"
        r"uppercase check|lowercase check|digit check|special char check)\b",
        re.I,
    ),
    "length_requirement_enforcement": re.compile(
        r"\b(?:(?:length|minimum|maximum).{0,80}(?:validat|check|verify|enforce|requirement|test)|"
        r"(?:validat|check|verify|enforce|requirement|test).{0,80}(?:length|minimum|maximum)|"
        r"min length|max length|length validation)\b",
        re.I,
    ),
    "history_tracking_implementation": re.compile(
        r"\b(?:(?:history|previous password).{0,80}(?:stor|track|record|database|implementation|code)|"
        r"(?:stor|track|record|database|implementation|code).{0,80}(?:history|previous password)|"
        r"password archive|history table|reuse check)\b",
        re.I,
    ),
    "expiration_automation_logic": re.compile(
        r"\b(?:(?:expiration|expiry|rotation).{0,80}(?:automation|scheduler|cron|job|task|check|notification)|"
        r"(?:automation|scheduler|cron|job|task|check|notification).{0,80}(?:expiration|expiry|rotation)|"
        r"expiry check|rotation scheduler|password age check)\b",
        re.I,
    ),
    "reset_workflow_security": re.compile(
        r"\b(?:(?:reset|recovery).{0,80}(?:token|security|validation|verification|email|link|url)|"
        r"(?:token|security|validation|verification|email|link|url).{0,80}(?:reset|recovery)|"
        r"reset token|recovery email|secure link|token expiration)\b",
        re.I,
    ),
    "breach_database_integration": re.compile(
        r"\b(?:(?:breach|pwned).{0,80}(?:databas|api|service|integration|check|lookup|hibp)|"
        r"(?:databas|api|service|integration|check|lookup|hibp).{0,80}(?:breach|pwned)|"
        r"haveibeenpwned|hibp api|breach check|pwned lookup)\b",
        re.I,
    ),
    "strength_meter_accuracy": re.compile(
        r"\b(?:(?:strength|quality).{0,80}(?:meter|indicator|calculation|algorithm|score|feedback|visualization)|"
        r"(?:meter|indicator|calculation|algorithm|score|feedback|visualization).{0,80}(?:strength|quality)|"
        r"strength calculation|entropy calculation|password score)\b",
        re.I,
    ),
    "hashing_algorithm_strength": re.compile(
        r"\b(?:(?:bcrypt|argon2|scrypt|pbkdf2).{0,80}(?:implementation|config|work factor|cost|rounds|salt)|"
        r"(?:implementation|config|work factor|cost|rounds|salt).{0,80}(?:bcrypt|argon2|scrypt|pbkdf2)|"
        r"hash algorithm|salting|work factor|cost factor)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:password policy|password requirement|password rule)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[PasswordPolicySafeguard, str] = {
    "complexity_rule_validation": "Implement password complexity rule validation including uppercase, lowercase, digits, and special character checks.",
    "length_requirement_enforcement": "Enforce minimum and maximum password length requirements with clear validation and error messages.",
    "history_tracking_implementation": "Implement password history tracking with secure storage and reuse prevention checks.",
    "expiration_automation_logic": "Automate password expiration checks with scheduled tasks, notifications, and forced rotation enforcement.",
    "reset_workflow_security": "Implement secure password reset workflow with time-limited tokens, email verification, and link expiration.",
    "breach_database_integration": "Integrate breach password database (e.g., HaveIBeenPwned API) for compromised password detection.",
    "strength_meter_accuracy": "Implement accurate password strength meter with entropy calculation and real-time visual feedback.",
    "hashing_algorithm_strength": "Use strong hashing algorithm (bcrypt, argon2, scrypt) with proper work factor and salting configuration.",
}


@dataclass(frozen=True, slots=True)
class TaskApiPasswordPolicyReadinessFinding:
    """API password policy readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[PasswordPolicySignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[PasswordPolicySafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[PasswordPolicySafeguard, ...] = field(default_factory=tuple)
    readiness: PasswordPolicyReadiness = "partial"
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
class TaskApiPasswordPolicyReadinessPlan:
    """Plan-level API password policy readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiPasswordPolicyReadinessFinding, ...] = field(default_factory=tuple)
    password_policy_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiPasswordPolicyReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "password_policy_task_ids": list(self.password_policy_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }


def plan_task_api_password_policy_readiness(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiPasswordPolicyReadinessPlan:
    """Generate API password policy readiness safeguards for all tasks in a plan."""
    plan_id, tasks = _plan_payload(plan)
    findings: list[TaskApiPasswordPolicyReadinessFinding] = []
    password_policy_task_ids: list[str] = []
    not_applicable_task_ids: list[str] = []

    for task in tasks:
        task_id, task_payload = _task_payload(task)
        if not task_id:
            continue

        signals = _detect_signals(task_payload)
        if _has_no_impact(task_payload) or not signals:
            not_applicable_task_ids.append(task_id)
            continue

        password_policy_task_ids.append(task_id)
        present_safeguards = _detect_safeguards(task_payload, signals)
        missing_safeguards = tuple(
            safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in present_safeguards
        )
        evidence = _collect_evidence(task_payload, signals)
        remediations = tuple(_REMEDIATIONS[safeguard] for safeguard in missing_safeguards[:3])
        readiness = _assess_readiness(signals, present_safeguards, missing_safeguards)

        findings.append(
            TaskApiPasswordPolicyReadinessFinding(
                task_id=task_id,
                title=_task_title(task_payload),
                detected_signals=signals,
                present_safeguards=present_safeguards,
                missing_safeguards=missing_safeguards,
                readiness=readiness,
                evidence=evidence,
                actionable_remediations=remediations,
            )
        )

    return TaskApiPasswordPolicyReadinessPlan(
        plan_id=plan_id,
        findings=tuple(findings),
        password_policy_task_ids=tuple(password_policy_task_ids),
        not_applicable_task_ids=tuple(not_applicable_task_ids),
        summary=_plan_summary(findings, password_policy_task_ids, not_applicable_task_ids),
    )


def task_api_password_policy_readiness_to_dict(
    plan: TaskApiPasswordPolicyReadinessPlan,
) -> dict[str, Any]:
    """Serialize a password policy readiness plan to a plain dictionary."""
    return plan.to_dict()


task_api_password_policy_readiness_to_dict.__test__ = False


def _plan_payload(plan: ExecutionPlan | Mapping[str, Any] | str | object) -> tuple[str | None, list[Any]]:
    if isinstance(plan, str):
        return None, []
    if isinstance(plan, ExecutionPlan):
        return plan.id, list(plan.tasks)
    if hasattr(plan, "model_dump"):
        payload = plan.model_dump(mode="python")
        if isinstance(payload, Mapping):
            return _optional_text(payload.get("id")), list(payload.get("tasks", []))
    if isinstance(plan, Mapping):
        try:
            execution_plan = ExecutionPlan.model_validate(plan)
            return execution_plan.id, list(execution_plan.tasks)
        except (TypeError, ValueError, ValidationError):
            pass
        return _optional_text(plan.get("id")), list(plan.get("tasks", []))
    if hasattr(plan, "tasks"):
        plan_id = getattr(plan, "id", None)
        return _optional_text(plan_id), list(plan.tasks) if plan.tasks else []
    return None, []


def _task_payload(task: ExecutionTask | Mapping[str, Any] | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(task, ExecutionTask):
        return task.task_id, dict(task.model_dump(mode="python"))
    if hasattr(task, "model_dump"):
        payload = task.model_dump(mode="python")
        if isinstance(payload, Mapping):
            return _optional_text(payload.get("task_id")), dict(payload)
    if isinstance(task, Mapping):
        try:
            execution_task = ExecutionTask.model_validate(task)
            return execution_task.task_id, dict(execution_task.model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            pass
        return _optional_text(task.get("task_id")), dict(task)
    task_id = getattr(task, "task_id", None)
    payload = {}
    for field in ("task_id", "title", "prompt", "scope", "outputs", "validation", "test_files"):
        if hasattr(task, field):
            payload[field] = getattr(task, field)
    return _optional_text(task_id), payload


def _detect_signals(task_payload: Mapping[str, Any]) -> tuple[PasswordPolicySignal, ...]:
    text = _searchable_text(task_payload)
    paths = _searchable_paths(task_payload)
    signals: list[PasswordPolicySignal] = []

    for signal in _SIGNAL_ORDER:
        if _SIGNAL_PATTERNS[signal].search(text):
            signals.append(signal)
        elif paths and _PATH_SIGNAL_PATTERNS[signal].search(paths):
            signals.append(signal)

    return tuple(signals)


def _detect_safeguards(
    task_payload: Mapping[str, Any],
    signals: tuple[PasswordPolicySignal, ...],
) -> tuple[PasswordPolicySafeguard, ...]:
    text = _searchable_text(task_payload)
    safeguards: list[PasswordPolicySafeguard] = []

    for safeguard in _SAFEGUARD_ORDER:
        if _SAFEGUARD_PATTERNS[safeguard].search(text):
            safeguards.append(safeguard)

    return tuple(safeguards)


def _has_no_impact(task_payload: Mapping[str, Any]) -> bool:
    text = _searchable_text(task_payload)
    return bool(_NO_IMPACT_RE.search(text))


def _collect_evidence(
    task_payload: Mapping[str, Any],
    signals: tuple[PasswordPolicySignal, ...],
) -> tuple[str, ...]:
    evidence: list[str] = []
    for field_name in ("title", "prompt", "scope"):
        if field_name in task_payload and task_payload[field_name]:
            text = _clean_text(str(task_payload[field_name]))
            if text and any(_SIGNAL_PATTERNS[signal].search(text) for signal in signals):
                snippet = text[:200] + ("..." if len(text) > 200 else "")
                evidence.append(f"{field_name}: {snippet}")
    return tuple(evidence[:3])


def _assess_readiness(
    signals: tuple[PasswordPolicySignal, ...],
    present_safeguards: tuple[PasswordPolicySafeguard, ...],
    missing_safeguards: tuple[PasswordPolicySafeguard, ...],
) -> PasswordPolicyReadiness:
    if not signals:
        return "weak"
    safeguard_coverage = len(present_safeguards) / (len(present_safeguards) + len(missing_safeguards)) if (len(present_safeguards) + len(missing_safeguards)) > 0 else 0.0
    if safeguard_coverage >= 0.75:
        return "strong"
    if safeguard_coverage >= 0.4:
        return "partial"
    return "weak"


def _plan_summary(
    findings: list[TaskApiPasswordPolicyReadinessFinding],
    password_policy_task_ids: list[str],
    not_applicable_task_ids: list[str],
) -> dict[str, Any]:
    return {
        "total_tasks_reviewed": len(password_policy_task_ids) + len(not_applicable_task_ids),
        "password_policy_task_count": len(password_policy_task_ids),
        "not_applicable_task_count": len(not_applicable_task_ids),
        "findings_count": len(findings),
        "readiness_distribution": {
            "weak": sum(1 for finding in findings if finding.readiness == "weak"),
            "partial": sum(1 for finding in findings if finding.readiness == "partial"),
            "strong": sum(1 for finding in findings if finding.readiness == "strong"),
        },
        "common_missing_safeguards": _top_missing_safeguards(findings),
        "overall_readiness": _overall_readiness(findings),
    }


def _overall_readiness(findings: list[TaskApiPasswordPolicyReadinessFinding]) -> PasswordPolicyReadiness:
    if not findings:
        return "weak"
    weak_count = sum(1 for finding in findings if finding.readiness == "weak")
    if weak_count > len(findings) / 2:
        return "weak"
    strong_count = sum(1 for finding in findings if finding.readiness == "strong")
    if strong_count >= len(findings) * 0.7:
        return "strong"
    return "partial"


def _top_missing_safeguards(findings: list[TaskApiPasswordPolicyReadinessFinding]) -> list[str]:
    safeguard_counts: dict[PasswordPolicySafeguard, int] = {}
    for finding in findings:
        for safeguard in finding.missing_safeguards:
            safeguard_counts[safeguard] = safeguard_counts.get(safeguard, 0) + 1
    sorted_safeguards = sorted(
        safeguard_counts.items(),
        key=lambda item: (item[1], _SAFEGUARD_ORDER.index(item[0])),
        reverse=True,
    )
    return [safeguard for safeguard, _ in sorted_safeguards[:5]]


def _searchable_text(task_payload: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for field_name in ("title", "prompt", "scope", "outputs", "validation"):
        if field_name in task_payload and task_payload[field_name]:
            parts.append(_clean_text(str(task_payload[field_name])))
    return " ".join(parts)


def _searchable_paths(task_payload: Mapping[str, Any]) -> str:
    paths: list[str] = []
    for field_name in ("outputs", "test_files"):
        if field_name in task_payload:
            value = task_payload[field_name]
            if isinstance(value, (list, tuple)):
                for item in value:
                    if item:
                        try:
                            path = PurePosixPath(str(item))
                            paths.append(path.name)
                            if path.parent != PurePosixPath("."):
                                paths.append(str(path.parent))
                        except (ValueError, TypeError):
                            continue
            elif value:
                try:
                    path = PurePosixPath(str(value))
                    paths.append(path.name)
                    if path.parent != PurePosixPath("."):
                        paths.append(str(path.parent))
                except (ValueError, TypeError):
                    pass
    return " ".join(paths)


def _task_title(task_payload: Mapping[str, Any]) -> str:
    return _clean_text(str(task_payload.get("title", ""))) or "Untitled task"


def _clean_text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    text = str(value)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


__all__ = [
    "PasswordPolicyReadiness",
    "PasswordPolicySafeguard",
    "PasswordPolicySignal",
    "TaskApiPasswordPolicyReadinessFinding",
    "TaskApiPasswordPolicyReadinessPlan",
    "plan_task_api_password_policy_readiness",
    "task_api_password_policy_readiness_to_dict",
]
