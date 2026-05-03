"""Plan OAuth token refresh lifecycle readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


TokenRefreshSignal = Literal[
    "oauth_token_refresh",
    "refresh_token",
    "access_token_expiry",
    "offline_access",
    "provider_oauth",
]
TokenRefreshRequirement = Literal[
    "refresh_token_storage",
    "rotation_revocation_handling",
    "expired_token_retry",
    "provider_error_handling",
    "observability",
    "integration_tests",
]
TokenRefreshRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[TokenRefreshSignal, ...] = (
    "oauth_token_refresh",
    "refresh_token",
    "access_token_expiry",
    "offline_access",
    "provider_oauth",
)
_REQUIREMENT_ORDER: tuple[TokenRefreshRequirement, ...] = (
    "refresh_token_storage",
    "rotation_revocation_handling",
    "expired_token_retry",
    "provider_error_handling",
    "observability",
    "integration_tests",
)
_RISK_ORDER: dict[TokenRefreshRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_PATTERNS: dict[TokenRefreshSignal, re.Pattern[str]] = {
    "oauth_token_refresh": re.compile(
        r"\b(?:oauth|oauth2|oidc|openid connect).{0,80}(?:token refresh|refresh flow|refresh lifecycle|"
        r"refresh expired access tokens?)\b|\b(?:token refresh|refresh flow|refresh lifecycle)\b",
        re.I,
    ),
    "refresh_token": re.compile(r"\b(?:refresh tokens?|refresh_token)\b", re.I),
    "access_token_expiry": re.compile(
        r"\b(?:access tokens?|bearer tokens?).{0,80}(?:expir(?:e|es|ed|y|ation)|401|unauthorized)|"
        r"\b(?:expired access tokens?|token expir(?:e|es|ed|y|ation))\b",
        re.I,
    ),
    "offline_access": re.compile(r"\b(?:offline_access|offline access|long[- ]lived access|background access)\b", re.I),
    "provider_oauth": re.compile(
        r"\b(?:google|microsoft|azure|slack|github|salesforce|hubspot|stripe|shopify|okta|auth0|"
        r"identity provider|oauth provider|provider oauth)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[TokenRefreshSignal, re.Pattern[str]] = {
    "oauth_token_refresh": re.compile(r"(?:^|/)(?:oauth|oidc|auth)(?:/|$)|token[_-]?refresh|refresh[_-]?token", re.I),
    "refresh_token": re.compile(r"refresh[_-]?tokens?", re.I),
    "access_token_expiry": re.compile(r"access[_-]?tokens?|token[_-]?expiry|token[_-]?expiration", re.I),
    "offline_access": re.compile(r"offline[_-]?access", re.I),
    "provider_oauth": re.compile(r"(?:^|/)(?:providers?|integrations?|connectors?)(?:/|$)", re.I),
}
_REQUIREMENT_PATTERNS: dict[TokenRefreshRequirement, re.Pattern[str]] = {
    "refresh_token_storage": re.compile(
        r"\b(?:(?:refresh tokens?|token credentials?|oauth tokens?).{0,100}(?:store|storage|persist|"
        r"encrypt|encrypted|kms|vault|secret|database|at rest)|(?:encrypted|secure|vault|kms|secret)"
        r".{0,80}(?:refresh tokens?|token storage))\b",
        re.I,
    ),
    "rotation_revocation_handling": re.compile(
        r"\b(?:token rotation|rotate refresh tokens?|rotating refresh tokens?|refresh token reuse|"
        r"refresh token family|revocation|revoked tokens?|revoke access|invalid_grant|disconnect app|"
        r"provider revokes?|rotated by provider)\b",
        re.I,
    ),
    "expired_token_retry": re.compile(
        r"\b(?:(?:expired|expiring|401|unauthorized).{0,100}(?:retry|refresh and retry|replay|backoff)|"
        r"(?:retry|refresh and retry|single retry|one retry).{0,100}(?:expired|401|unauthorized|access token))\b",
        re.I,
    ),
    "provider_error_handling": re.compile(
        r"\b(?:provider errors?|oauth errors?|error codes?|invalid_grant|invalid_client|invalid_scope|"
        r"temporarily_unavailable|rate limit|429|5xx|provider outage|provider timeout|token endpoint failure)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|metrics?|alerts?|alerting|logs?|logging|audit events?|traces?|dashboard|"
        r"token refresh success|refresh failure rate|refresh latency|provider error rate)\b",
        re.I,
    ),
    "integration_tests": re.compile(
        r"\b(?:integration tests?|e2e tests?|end[- ]to[- ]end tests?|contract tests?|provider sandbox|"
        r"mock oauth provider|token endpoint test|test expired token|refresh token test)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:oauth|refresh tokens?|token refresh|access token)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_ACTIONABLE_GAPS: dict[TokenRefreshRequirement, str] = {
    "refresh_token_storage": "Specify encrypted refresh-token storage, secret handling, and persistence ownership.",
    "rotation_revocation_handling": "Handle refresh-token rotation, provider revocation, disconnects, and invalid_grant outcomes.",
    "expired_token_retry": "Define expired access-token detection, bounded refresh-and-retry behavior, and retry limits.",
    "provider_error_handling": "Map provider token endpoint errors, rate limits, and outages to user-visible and retry behavior.",
    "observability": "Add metrics, logs, alerts, and dashboards for refresh success, failure, latency, and provider errors.",
    "integration_tests": "Cover token refresh, expired-token retries, provider errors, revocation, and rotation in integration tests.",
}


@dataclass(frozen=True, slots=True)
class TaskOAuthTokenRefreshReadinessFinding:
    """OAuth token refresh lifecycle readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[TokenRefreshSignal, ...] = field(default_factory=tuple)
    present_requirements: tuple[TokenRefreshRequirement, ...] = field(default_factory=tuple)
    missing_requirements: tuple[TokenRefreshRequirement, ...] = field(default_factory=tuple)
    risk_level: TokenRefreshRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    actionable_gaps: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_requirements": list(self.present_requirements),
            "missing_requirements": list(self.missing_requirements),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "actionable_gaps": list(self.actionable_gaps),
        }


@dataclass(frozen=True, slots=True)
class TaskOAuthTokenRefreshReadinessPlan:
    """Plan-level OAuth token refresh lifecycle readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskOAuthTokenRefreshReadinessFinding, ...] = field(default_factory=tuple)
    refresh_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskOAuthTokenRefreshReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [record.to_dict() for record in self.findings],
            "refresh_task_ids": list(self.refresh_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness findings as plain dictionaries."""
        return [record.to_dict() for record in self.findings]


def build_task_oauth_token_refresh_readiness_plan(source: Any) -> TaskOAuthTokenRefreshReadinessPlan:
    """Build OAuth token refresh readiness findings for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_finding_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    findings = tuple(
        sorted(
            (finding for finding in candidates if finding is not None),
            key=lambda finding: (_RISK_ORDER[finding.risk_level], finding.task_id, finding.title.casefold()),
        )
    )
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskOAuthTokenRefreshReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        refresh_task_ids=tuple(finding.task_id for finding in findings),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_oauth_token_refresh_readiness(source: Any) -> TaskOAuthTokenRefreshReadinessPlan:
    """Compatibility alias for building OAuth token refresh readiness plans."""
    return build_task_oauth_token_refresh_readiness_plan(source)


def summarize_task_oauth_token_refresh_readiness(source: Any) -> TaskOAuthTokenRefreshReadinessPlan:
    """Compatibility alias for building OAuth token refresh readiness plans."""
    return build_task_oauth_token_refresh_readiness_plan(source)


def extract_task_oauth_token_refresh_readiness(source: Any) -> TaskOAuthTokenRefreshReadinessPlan:
    """Compatibility alias for extracting OAuth token refresh readiness plans."""
    return build_task_oauth_token_refresh_readiness_plan(source)


def generate_task_oauth_token_refresh_readiness(source: Any) -> TaskOAuthTokenRefreshReadinessPlan:
    """Compatibility alias for generating OAuth token refresh readiness plans."""
    return build_task_oauth_token_refresh_readiness_plan(source)


def task_oauth_token_refresh_readiness_plan_to_dict(result: TaskOAuthTokenRefreshReadinessPlan) -> dict[str, Any]:
    """Serialize an OAuth token refresh readiness plan to a plain dictionary."""
    return result.to_dict()


task_oauth_token_refresh_readiness_plan_to_dict.__test__ = False


def task_oauth_token_refresh_readiness_plan_to_dicts(
    result: TaskOAuthTokenRefreshReadinessPlan | Iterable[TaskOAuthTokenRefreshReadinessFinding],
) -> list[dict[str, Any]]:
    """Serialize OAuth token refresh readiness findings to plain dictionaries."""
    if isinstance(result, TaskOAuthTokenRefreshReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_oauth_token_refresh_readiness_plan_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[TokenRefreshSignal, ...] = field(default_factory=tuple)
    requirements: tuple[TokenRefreshRequirement, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskOAuthTokenRefreshReadinessFinding | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not _is_refresh_task(signals.signals):
        return None

    missing = tuple(requirement for requirement in _REQUIREMENT_ORDER if requirement not in signals.requirements)
    return TaskOAuthTokenRefreshReadinessFinding(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        detected_signals=signals.signals,
        present_requirements=signals.requirements,
        missing_requirements=missing,
        risk_level=_risk_level(signals.signals, missing),
        evidence=signals.evidence,
        actionable_gaps=tuple(_ACTIONABLE_GAPS[requirement] for requirement in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[TokenRefreshSignal] = set()
    requirement_hits: set[TokenRefreshRequirement] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        matched = False
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        matched = False
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for requirement, pattern in _REQUIREMENT_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                requirement_hits.add(requirement)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        requirements=tuple(requirement for requirement in _REQUIREMENT_ORDER if requirement in requirement_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _is_refresh_task(signals: tuple[TokenRefreshSignal, ...]) -> bool:
    signal_set = set(signals)
    return "oauth_token_refresh" in signal_set or "refresh_token" in signal_set or (
        "access_token_expiry" in signal_set and bool(signal_set & {"provider_oauth", "offline_access"})
    )


def _risk_level(
    signals: tuple[TokenRefreshSignal, ...],
    missing: tuple[TokenRefreshRequirement, ...],
) -> TokenRefreshRiskLevel:
    if not missing:
        return "low"
    missing_set = set(missing)
    if {"refresh_token_storage", "expired_token_retry", "provider_error_handling"} & missing_set and len(missing) >= 4:
        return "high"
    if "refresh_token" in signals and {"refresh_token_storage", "rotation_revocation_handling"} <= missing_set:
        return "high"
    return "medium"


def _summary(
    findings: tuple[TaskOAuthTokenRefreshReadinessFinding, ...],
    *,
    total_task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "refresh_task_count": len(findings),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_requirement_count": sum(len(finding.missing_requirements) for finding in findings),
        "risk_counts": {risk: sum(1 for finding in findings if finding.risk_level == risk) for risk in _RISK_ORDER},
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_requirement_counts": {
            requirement: sum(1 for finding in findings if requirement in finding.present_requirements)
            for requirement in _REQUIREMENT_ORDER
        },
        "missing_requirement_counts": {
            requirement: sum(1 for finding in findings if requirement in finding.missing_requirements)
            for requirement in _REQUIREMENT_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
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
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
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
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


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
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
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
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
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
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_REQUIREMENT_PATTERNS.values()])


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
    return str(PurePosixPath(value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")))


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
    "TaskOAuthTokenRefreshReadinessFinding",
    "TaskOAuthTokenRefreshReadinessPlan",
    "TokenRefreshRequirement",
    "TokenRefreshRiskLevel",
    "TokenRefreshSignal",
    "analyze_task_oauth_token_refresh_readiness",
    "build_task_oauth_token_refresh_readiness_plan",
    "extract_task_oauth_token_refresh_readiness",
    "generate_task_oauth_token_refresh_readiness",
    "summarize_task_oauth_token_refresh_readiness",
    "task_oauth_token_refresh_readiness_plan_to_dict",
    "task_oauth_token_refresh_readiness_plan_to_dicts",
]
