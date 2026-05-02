"""Plan account recovery readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


AccountRecoveryScenario = Literal[
    "self_service_reset",
    "mfa_recovery",
    "account_lockout",
    "support_assisted_recovery",
    "compromised_account",
    "recovery_audit_logging",
]
AccountRecoverySafeguard = Literal[
    "token_expiry",
    "one_time_use",
    "enumeration_resistance",
    "rate_limiting",
    "abuse_prevention",
    "support_verification",
    "telemetry",
    "test_coverage",
]
AccountRecoveryRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SCENARIO_ORDER: tuple[AccountRecoveryScenario, ...] = (
    "self_service_reset",
    "mfa_recovery",
    "account_lockout",
    "support_assisted_recovery",
    "compromised_account",
    "recovery_audit_logging",
)
_SAFEGUARD_ORDER: tuple[AccountRecoverySafeguard, ...] = (
    "token_expiry",
    "one_time_use",
    "enumeration_resistance",
    "rate_limiting",
    "abuse_prevention",
    "support_verification",
    "telemetry",
    "test_coverage",
)
_RISK_ORDER: dict[AccountRecoveryRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}

_PATH_SCENARIO_PATTERNS: dict[AccountRecoveryScenario, tuple[re.Pattern[str], ...]] = {
    "self_service_reset": (
        re.compile(
            r"(?:^|/)(?:password[-_]?reset|reset[-_]?password|forgot[-_]?password|reset[-_]?tokens?|recovery[-_]?email)(?:/|$)|"
            r"(?:password[-_]?reset|reset[-_]?token|forgot[-_]?password|recovery[-_]?email)",
            re.I,
        ),
    ),
    "mfa_recovery": (
        re.compile(r"(?:^|/)(?:mfa|2fa|totp|authenticator|backup[-_]?codes?)(?:/|$).*recover|mfa[-_]?recover|backup[-_]?codes?", re.I),
    ),
    "account_lockout": (
        re.compile(r"(?:^|/)(?:lockout|unlock|locked[-_]?account|account[-_]?lock)(?:/|$)|account[-_]?unlock|lockout", re.I),
    ),
    "support_assisted_recovery": (
        re.compile(r"(?:^|/)(?:support|helpdesk|csr|agent)(?:/|$).*(?:recovery|unlock)|support[-_]?assisted|helpdesk[-_]?unlock", re.I),
    ),
    "compromised_account": (
        re.compile(r"(?:^|/)(?:compromised|account[-_]?takeover|ato|incident)(?:/|$)|compromised[-_]?account|account[-_]?takeover", re.I),
    ),
    "recovery_audit_logging": (
        re.compile(r"(?:^|/)(?:audit|events?|logs?|telemetry)(?:/|$).*(?:recovery|reset|unlock)|recovery[-_]?audit|reset[-_]?audit", re.I),
    ),
}
_TEXT_SCENARIO_PATTERNS: dict[AccountRecoveryScenario, tuple[re.Pattern[str], ...]] = {
    "self_service_reset": (
        re.compile(
            r"\b(?:password reset|reset password|forgot password|reset token|password recovery|recovery email|"
            r"recovery link|email recovery|self[- ]service reset|magic reset link)\b",
            re.I,
        ),
    ),
    "mfa_recovery": (
        re.compile(
            r"\b(?:mfa recovery|2fa recovery|multi[- ]factor recovery|recover mfa|recover 2fa|"
            r"lost authenticator|backup codes?|recovery codes?|totp reset|authenticator reset)\b",
            re.I,
        ),
    ),
    "account_lockout": (
        re.compile(
            r"\b(?:account[- ]lockout|locked account|unlock account|account unlock|failed login lockout|"
            r"too many login attempts|unlock flow|lockout recovery)\b",
            re.I,
        ),
    ),
    "support_assisted_recovery": (
        re.compile(
            r"\b(?:support[- ]assisted recovery|support assisted unlock|support unlock|helpdesk recovery|"
            r"customer support recovery|support agent|support verification|manual account recovery|agent assisted)\b",
            re.I,
        ),
    ),
    "compromised_account": (
        re.compile(
            r"\b(?:compromised account|account[- ]takeover|ato|suspected compromise|hijacked account|"
            r"account recovery after compromise|secure compromised account)\b",
            re.I,
        ),
    ),
    "recovery_audit_logging": (
        re.compile(
            r"\b(?:recovery audit|password reset audit|unlock audit|recovery log|audit log|audit trail|"
            r"security event|recovery telemetry|reset telemetry|account recovery events?)\b",
            re.I,
        ),
    ),
}
_SAFEGUARD_PATTERNS: dict[AccountRecoverySafeguard, tuple[re.Pattern[str], ...]] = {
    "token_expiry": (
        re.compile(
            r"\b(?:token expir(?:y|ation)|expires? after|expiring token|ttl|time[- ]to[- ]live|"
            r"valid for \d+|short[- ]lived|link expir(?:y|ation))\b",
            re.I,
        ),
    ),
    "one_time_use": (
        re.compile(
            r"\b(?:one[- ]time use|single[- ]use|consume token|invalidate token|token reuse|replay prevention|"
            r"nonce|used once|burn after use)\b",
            re.I,
        ),
    ),
    "enumeration_resistance": (
        re.compile(
            r"\b(?:enumeration resistance|account enumeration|user enumeration|generic response|same response|"
            r"do not reveal whether|avoid leaking account existence|constant response|neutral confirmation)\b",
            re.I,
        ),
    ),
    "rate_limiting": (
        re.compile(
            r"\b(?:rate limit|rate limiting|throttle|throttling|cooldown|attempt limit|per[- ]ip limit|"
            r"per[- ]account limit|request limit|lockout threshold)\b",
            re.I,
        ),
    ),
    "abuse_prevention": (
        re.compile(
            r"\b(?:abuse prevention|anti[- ]abuse|bot protection|captcha|recaptcha|challenge|risk scoring|"
            r"fraud check|suspicious activity|brute force|spray attack)\b",
            re.I,
        ),
    ),
    "support_verification": (
        re.compile(
            r"\b(?:support verification|identity verification|verify identity|agent verification|manual verification|"
            r"proof of identity|step[- ]up verification|verified support|support checklist|manager approval)\b",
            re.I,
        ),
    ),
    "telemetry": (
        re.compile(
            r"\b(?:telemetry|audit log|audit event|security event|metrics|monitoring|alerting|alerts?|"
            r"event log|siem|investigation trail|record actor|ip address|device fingerprint)\b",
            re.I,
        ),
    ),
    "test_coverage": (
        re.compile(
            r"\b(?:test coverage|unit tests?|integration tests?|e2e tests?|end[- ]to[- ]end tests?|"
            r"reset tests?|recovery tests?|lockout tests?|unlock tests?|mfa recovery tests?|abuse tests?|"
            r"test account recovery|test password reset|test lockout|test unlock)\b",
            re.I,
        ),
    ),
}
_RECOMMENDED_CHECKS: dict[AccountRecoverySafeguard, str] = {
    "token_expiry": "Define short reset, recovery, or unlock token expiry and document how stale links are rejected.",
    "one_time_use": "Ensure recovery tokens are single-use and invalidated after success, replacement, or credential change.",
    "enumeration_resistance": "Return neutral responses so reset, recovery, and unlock flows do not reveal whether an account exists.",
    "rate_limiting": "Rate-limit recovery requests and unlock attempts by account, IP, device, and channel.",
    "abuse_prevention": "Add abuse prevention for reset and unlock flows, such as bot challenges, risk scoring, or suspicious-activity gates.",
    "support_verification": "Define support-assisted verification steps, escalation rules, and evidence required before account access changes.",
    "telemetry": "Log recovery attempts, token issuance and use, support actions, lockout changes, IP/device context, and alert-worthy failures.",
    "test_coverage": "Cover reset, MFA recovery, lockout, support recovery, enumeration resistance, rate limits, telemetry, and token replay tests.",
}


@dataclass(frozen=True, slots=True)
class TaskAccountRecoveryReadinessRecord:
    """Account recovery readiness guidance for one execution task."""

    task_id: str
    title: str
    recovery_scenarios: tuple[AccountRecoveryScenario, ...]
    present_safeguards: tuple[AccountRecoverySafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[AccountRecoverySafeguard, ...] = field(default_factory=tuple)
    risk_level: AccountRecoveryRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "recovery_scenarios": list(self.recovery_scenarios),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskAccountRecoveryReadinessPlan:
    """Plan-level account recovery readiness review."""

    plan_id: str | None = None
    records: tuple[TaskAccountRecoveryReadinessRecord, ...] = field(default_factory=tuple)
    account_recovery_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskAccountRecoveryReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "account_recovery_task_ids": list(self.account_recovery_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render account recovery readiness as deterministic Markdown."""
        title = "# Task Account Recovery Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Account recovery task count: {self.summary.get('account_recovery_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No account recovery readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Scenarios | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.recovery_scenarios) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_account_recovery_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAccountRecoveryReadinessPlan:
    """Build readiness records for tasks that implement account recovery flows."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    account_recovery_task_ids = tuple(record.task_id for record in records)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskAccountRecoveryReadinessPlan(
        plan_id=plan_id,
        records=records,
        account_recovery_task_ids=account_recovery_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_account_recovery_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAccountRecoveryReadinessPlan:
    """Compatibility alias for building account recovery readiness plans."""
    return build_task_account_recovery_readiness_plan(source)


def summarize_task_account_recovery_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAccountRecoveryReadinessPlan:
    """Compatibility alias for building account recovery readiness plans."""
    return build_task_account_recovery_readiness_plan(source)


def extract_task_account_recovery_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAccountRecoveryReadinessPlan:
    """Compatibility alias for building account recovery readiness plans."""
    return build_task_account_recovery_readiness_plan(source)


def generate_task_account_recovery_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAccountRecoveryReadinessPlan:
    """Compatibility alias for generating account recovery readiness plans."""
    return build_task_account_recovery_readiness_plan(source)


def recommend_task_account_recovery_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskAccountRecoveryReadinessPlan:
    """Compatibility alias for recommending account recovery readiness plans."""
    return build_task_account_recovery_readiness_plan(source)


def task_account_recovery_readiness_plan_to_dict(
    result: TaskAccountRecoveryReadinessPlan,
) -> dict[str, Any]:
    """Serialize an account recovery readiness plan to a plain dictionary."""
    return result.to_dict()


task_account_recovery_readiness_plan_to_dict.__test__ = False


def task_account_recovery_readiness_plan_to_dicts(
    result: TaskAccountRecoveryReadinessPlan | Iterable[TaskAccountRecoveryReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize account recovery readiness records to plain dictionaries."""
    if isinstance(result, TaskAccountRecoveryReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_account_recovery_readiness_plan_to_dicts.__test__ = False


def task_account_recovery_readiness_plan_to_markdown(
    result: TaskAccountRecoveryReadinessPlan,
) -> str:
    """Render an account recovery readiness plan as Markdown."""
    return result.to_markdown()


task_account_recovery_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    scenarios: tuple[AccountRecoveryScenario, ...] = field(default_factory=tuple)
    scenario_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[AccountRecoverySafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskAccountRecoveryReadinessRecord | None:
    signals = _signals(task)
    if not signals.scenarios:
        return None

    missing = _missing_safeguards(signals.scenarios, signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskAccountRecoveryReadinessRecord(
        task_id=task_id,
        title=title,
        recovery_scenarios=signals.scenarios,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals.scenarios, signals.present_safeguards, missing),
        evidence=tuple(_dedupe([*signals.scenario_evidence, *signals.safeguard_evidence])),
        recommended_checks=tuple(_RECOMMENDED_CHECKS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    scenario_hits: set[AccountRecoveryScenario] = set()
    safeguard_hits: set[AccountRecoverySafeguard] = set()
    scenario_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_scenarios = _path_scenarios(normalized)
        if path_scenarios:
            scenario_hits.update(path_scenarios)
            scenario_evidence.append(f"files_or_modules: {path}")
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(searchable) or pattern.search(normalized) for pattern in patterns):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_scenario = False
        for scenario, patterns in _TEXT_SCENARIO_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                scenario_hits.add(scenario)
                matched_scenario = True
        if matched_scenario:
            scenario_evidence.append(snippet)
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    for command in _validation_commands(task):
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(command) or pattern.search(command_text) for pattern in patterns):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    return _Signals(
        scenarios=tuple(scenario for scenario in _SCENARIO_ORDER if scenario in scenario_hits),
        scenario_evidence=tuple(_dedupe(scenario_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_scenarios(path: str) -> set[AccountRecoveryScenario]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    scenarios: set[AccountRecoveryScenario] = set()
    for scenario, patterns in _PATH_SCENARIO_PATTERNS.items():
        if any(pattern.search(normalized) or pattern.search(text) for pattern in patterns):
            scenarios.add(scenario)
    name = PurePosixPath(normalized).name
    if re.search(r"\b(?:password|reset|forgot|recovery email)\b", text) or name.startswith("reset"):
        scenarios.add("self_service_reset")
    if re.search(r"\b(?:mfa|2fa|totp|authenticator|backup codes?)\b", text):
        scenarios.add("mfa_recovery")
    if re.search(r"\b(?:lockout|unlock|locked account)\b", text):
        scenarios.add("account_lockout")
    if re.search(r"\b(?:support|helpdesk|agent)\b", text) and re.search(r"\b(?:recover|recovery|unlock)\b", text):
        scenarios.add("support_assisted_recovery")
    return scenarios


def _missing_safeguards(
    scenarios: tuple[AccountRecoveryScenario, ...],
    present: tuple[AccountRecoverySafeguard, ...],
) -> tuple[AccountRecoverySafeguard, ...]:
    scenario_set = set(scenarios)
    required: set[AccountRecoverySafeguard] = {"telemetry", "test_coverage"}
    if scenario_set & {"self_service_reset", "mfa_recovery"}:
        required.update({"token_expiry", "one_time_use", "enumeration_resistance", "rate_limiting", "abuse_prevention"})
    if "account_lockout" in scenario_set:
        required.update({"enumeration_resistance", "rate_limiting", "abuse_prevention", "telemetry"})
    if scenario_set & {"support_assisted_recovery", "compromised_account"}:
        required.update({"support_verification", "telemetry", "test_coverage"})
    if "recovery_audit_logging" in scenario_set:
        required.add("telemetry")
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required and safeguard not in present)


def _risk_level(
    scenarios: tuple[AccountRecoveryScenario, ...],
    present: tuple[AccountRecoverySafeguard, ...],
    missing: tuple[AccountRecoverySafeguard, ...],
) -> AccountRecoveryRiskLevel:
    if not missing:
        return "low"

    scenario_set = set(scenarios)
    missing_set = set(missing)
    present_set = set(present)
    if scenario_set & {"self_service_reset", "account_lockout"} and {
        "enumeration_resistance",
        "rate_limiting",
    } & missing_set:
        return "high"
    if "self_service_reset" in scenario_set and {"token_expiry", "one_time_use"} <= missing_set:
        return "high"
    if scenario_set & {"support_assisted_recovery", "compromised_account"} and "support_verification" in missing_set:
        return "high"
    if "mfa_recovery" in scenario_set and len(missing_set - present_set) >= 4:
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskAccountRecoveryReadinessRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "account_recovery_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "scenario_counts": {
            scenario: sum(1 for record in records if scenario in record.recovery_scenarios)
            for scenario in sorted({scenario for record in records for scenario in record.recovery_scenarios})
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
        "validation_commands",
        "validation_command",
        "test_commands",
        "validation_plan",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    for source_field, text in _metadata_texts(task.get("validation_plan"), prefix="validation_plan"):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _metadata_key_is_signal(value: str) -> bool:
    return any(
        pattern.search(value)
        for patterns in (*_TEXT_SCENARIO_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
        for pattern in patterns
    )


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


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


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "AccountRecoveryRiskLevel",
    "AccountRecoverySafeguard",
    "AccountRecoveryScenario",
    "TaskAccountRecoveryReadinessPlan",
    "TaskAccountRecoveryReadinessRecord",
    "analyze_task_account_recovery_readiness",
    "build_task_account_recovery_readiness_plan",
    "extract_task_account_recovery_readiness",
    "generate_task_account_recovery_readiness",
    "recommend_task_account_recovery_readiness",
    "summarize_task_account_recovery_readiness",
    "task_account_recovery_readiness_plan_to_dict",
    "task_account_recovery_readiness_plan_to_dicts",
    "task_account_recovery_readiness_plan_to_markdown",
]
