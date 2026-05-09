"""Plan API authentication strategy for execution tasks involving authentication mechanisms."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ApiAuthenticationSignal = Literal[
    "api_key",
    "bearer_token",
    "oauth",
    "mutual_tls",
    "token_lifecycle",
    "credential_rotation",
]
ApiAuthenticationStrategy = Literal[
    "api_key_mechanism",
    "bearer_token_auth",
    "oauth_flow",
    "mtls_handshake",
    "token_expiry_validation",
    "credential_rotation_policy",
    "rate_limiting_per_credential",
    "audit_logging",
    "revocation_mechanism",
]
ApiAuthenticationReadiness = Literal["ready", "partial", "weak"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[ApiAuthenticationSignal, ...] = (
    "api_key",
    "bearer_token",
    "oauth",
    "mutual_tls",
    "token_lifecycle",
    "credential_rotation",
)
_STRATEGY_ORDER: tuple[ApiAuthenticationStrategy, ...] = (
    "api_key_mechanism",
    "bearer_token_auth",
    "oauth_flow",
    "mtls_handshake",
    "token_expiry_validation",
    "credential_rotation_policy",
    "rate_limiting_per_credential",
    "audit_logging",
    "revocation_mechanism",
)
_READINESS_ORDER: dict[ApiAuthenticationReadiness, int] = {"weak": 0, "partial": 1, "ready": 2}
_SIGNAL_PATTERNS: dict[ApiAuthenticationSignal, re.Pattern[str]] = {
    "api_key": re.compile(
        r"\b(?:api[- ]?key(?:s)?|x[- ]?api[- ]?key|api[- ]?token|access[- ]?key|"
        r"client[- ]?secret|app[- ]?key|authentication[- ]?key|key[- ]?based auth(?:entication)?)\b",
        re.I,
    ),
    "bearer_token": re.compile(
        r"\b(?:bearer token(?:s)?|authorization bearer|jwt token(?:s)?|access token(?:s)?|"
        r"bearer auth(?:entication)?|token[- ]?based auth(?:entication)?|json web token(?:s)?|jwt)\b",
        re.I,
    ),
    "oauth": re.compile(
        r"\b(?:oauth|oauth2(?:\.0)?|openid connect|oidc|authorization code flow|"
        r"client credentials|refresh token(?:s)?|pkce|authorization server|token endpoint)\b",
        re.I,
    ),
    "mutual_tls": re.compile(
        r"\b(?:mutual tls|mtls|client certificate(?:s)?|certificate[- ]?based auth(?:entication)?|"
        r"two[- ]?way tls|x\.?509|ssl certificate(?:s)?|client cert)\b",
        re.I,
    ),
    "token_lifecycle": re.compile(
        r"\b(?:token lifecycle|token expiry|token expiration|ttl|time[- ]?to[- ]?live|"
        r"token validity|expir(?:e|ed|ing|ation) token(?:s)?|refresh mechanism|token renewal)\b",
        re.I,
    ),
    "credential_rotation": re.compile(
        r"\b(?:credential rotation|key rotation|rotate (?:keys?|credentials?)|secret rotation|"
        r"automated rotation|rotation policy|periodic rotation|credential lifecycle)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[ApiAuthenticationSignal, re.Pattern[str]] = {
    "api_key": re.compile(r"api[_-]?key|auth[_-]?key|access[_-]?key", re.I),
    "bearer_token": re.compile(r"bearer|jwt|token[_-]?auth|access[_-]?token", re.I),
    "oauth": re.compile(r"oauth|oidc|openid|authorization|token[_-]?endpoint", re.I),
    "mutual_tls": re.compile(r"mtls|client[_-]?cert(?:ificate)?|x509|ssl|tls", re.I),
    "token_lifecycle": re.compile(r"token[_-]?lifecycle|expir(?:y|ation)|ttl|refresh", re.I),
    "credential_rotation": re.compile(r"rotat(?:e|ion)|credential[_-]?lifecycle", re.I),
}
_STRATEGY_PATTERNS: dict[ApiAuthenticationStrategy, re.Pattern[str]] = {
    "api_key_mechanism": re.compile(
        r"\b(?:api[- ]?key mechanism|implement api[- ]?key|x[- ]?api[- ]?key header|"
        r"api[- ]?key validation|verify api[- ]?key|api[- ]?key authentication|"
        r"secure key generation|key[- ]?based auth(?:entication)?)\b",
        re.I,
    ),
    "bearer_token_auth": re.compile(
        r"\b(?:bearer token auth(?:entication)?|authorization bearer|jwt validation|"
        r"verify (?:jwt|bearer token)|validate access token|token[- ]?based authentication|"
        r"use bearer token(?:s)?|bearer auth)\b",
        re.I,
    ),
    "oauth_flow": re.compile(
        r"\b(?:oauth flow|oauth2 flow|authorization code(?: flow)?|client credentials(?: flow)?|"
        r"implicit flow|pkce flow|token exchange|oauth integration|implement oauth(?:2?\.?0)?|"
        r"configure token endpoint(?:s)?|handle refresh token(?:s)?|oauth2? (?:for|with)|"
        r"oauth2?\.?0? (?:flow )?with|openid connect (?:with )?oauth)\b",
        re.I,
    ),
    "mtls_handshake": re.compile(
        r"\b(?:mtls handshake|mutual tls handshake|client certificate validation|"
        r"verify client cert(?:ificate)?|certificate[- ]?based authentication|x\.?509 validation|"
        r"validate client cert(?:ificate)?(?:s)?|certificate (?:authority|revocation) (?:configuration|checking))\b",
        re.I,
    ),
    "token_expiry_validation": re.compile(
        r"\b(?:token expiry validation|validate token expiration|check token validity|"
        r"verify token ttl|expiration check|token freshness|validate exp claim|"
        r"token expir(?:y|ation|ing)|handle token (?:expiration|renewal))\b",
        re.I,
    ),
    "credential_rotation_policy": re.compile(
        r"\b(?:credential rotation policy|key rotation policy|rotation schedule|"
        r"automated credential rotation|rotation strategy|rotation interval(?:s)?|"
        r"automated rotation|periodic(?:ally)? rotat(?:e|ion)|zero[- ]?downtime rotation)\b",
        re.I,
    ),
    "rate_limiting_per_credential": re.compile(
        r"\b(?:rate limit(?:ing)? per credential|credential[- ]?based rate limit(?:ing)?|"
        r"api[- ]?key rate limit(?:ing)?|throttl(?:e|ing) per key|per[- ]?credential quota|"
        r"rate limit(?:ing)? (?:with|per) (?:api[- ]?key|credential)|token bucket algorithm|quota tracking)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit log(?:ging)?|authentication log(?:s|ging)?|access log(?:s|ging)?|"
        r"log auth(?:entication)? (?:attempts?|events?)|security audit|credential usage log(?:s|ging)?|"
        r"failed login attempts?|set up audit log(?:ging)?|log (?:credential usage|suspicious activity)|"
        r"(?:and|with) audit log(?:ging)?)\b",
        re.I,
    ),
    "revocation_mechanism": re.compile(
        r"\b(?:revocation mechanism|token revocation|revoke token(?:s)?|"
        r"credential revocation|invalidate token(?:s)?|blacklist token(?:s)?|revocation list|"
        r"(?:implement|build|set up) revocation|immediate invalidation|certificate revocation checking)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:auth(?:entication)?|api[- ]?key|token|oauth|security|"
    r"credential(?:s)?|authentication)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_ACTIONABLE_GAPS: dict[ApiAuthenticationStrategy, str] = {
    "api_key_mechanism": "Implement API key mechanism with secure key generation, storage in environment variables or secrets manager, and validation on each request.",
    "bearer_token_auth": "Configure bearer token authentication with JWT validation, signature verification, and claims-based authorization.",
    "oauth_flow": "Integrate OAuth 2.0 flow (authorization code, client credentials, or PKCE), configure token endpoints, and handle refresh tokens.",
    "mtls_handshake": "Set up mutual TLS with client certificate validation, certificate authority configuration, and certificate revocation checking.",
    "token_expiry_validation": "Validate token expiration on each request, check exp claim in JWT, and reject expired tokens with appropriate error messages.",
    "credential_rotation_policy": "Define credential rotation policy with automated rotation schedule, key versioning, and zero-downtime rotation process.",
    "rate_limiting_per_credential": "Implement rate limiting per credential with sliding window or token bucket algorithm, configurable limits, and quota tracking.",
    "audit_logging": "Set up audit logging for authentication events, failed login attempts, credential usage, and suspicious activity with timestamp and source IP.",
    "revocation_mechanism": "Build token revocation mechanism with blacklist or database-backed revocation list, immediate invalidation, and revocation API endpoint.",
}


@dataclass(frozen=True, slots=True)
class TaskApiAuthenticationStrategyFinding:
    """API authentication strategy guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[ApiAuthenticationSignal, ...] = field(default_factory=tuple)
    present_strategies: tuple[ApiAuthenticationStrategy, ...] = field(default_factory=tuple)
    missing_strategies: tuple[ApiAuthenticationStrategy, ...] = field(default_factory=tuple)
    readiness: ApiAuthenticationReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_strategies": list(self.present_strategies),
            "missing_strategies": list(self.missing_strategies),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskApiAuthenticationStrategyPlan:
    """Plan-level API authentication strategy review."""

    plan_id: str | None = None
    findings: tuple[TaskApiAuthenticationStrategyFinding, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiAuthenticationStrategyFinding, ...]:
        """Compatibility view for modules that expose authentication strategy records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [record.to_dict() for record in self.findings],
            "impacted_task_ids": list(self.impacted_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return authentication strategy findings as plain dictionaries."""
        return [record.to_dict() for record in self.findings]


def build_task_api_authentication_strategy_plan(source: Any) -> TaskApiAuthenticationStrategyPlan:
    """Build API authentication strategy findings for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_finding_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    findings = tuple(
        sorted(
            (finding for finding in candidates if finding is not None),
            key=lambda finding: (_READINESS_ORDER[finding.readiness], finding.task_id, finding.title.casefold()),
        )
    )
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskApiAuthenticationStrategyPlan(
        plan_id=plan_id,
        findings=findings,
        impacted_task_ids=tuple(finding.task_id for finding in findings),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_api_authentication_strategy(source: Any) -> TaskApiAuthenticationStrategyPlan:
    """Compatibility alias for building API authentication strategy plans."""
    return build_task_api_authentication_strategy_plan(source)


def summarize_task_api_authentication_strategy(source: Any) -> TaskApiAuthenticationStrategyPlan:
    """Compatibility alias for building API authentication strategy plans."""
    return build_task_api_authentication_strategy_plan(source)


def extract_task_api_authentication_strategy(source: Any) -> TaskApiAuthenticationStrategyPlan:
    """Compatibility alias for extracting API authentication strategy plans."""
    return build_task_api_authentication_strategy_plan(source)


def generate_task_api_authentication_strategy(source: Any) -> TaskApiAuthenticationStrategyPlan:
    """Compatibility alias for generating API authentication strategy plans."""
    return build_task_api_authentication_strategy_plan(source)


def recommend_task_api_authentication_strategy(source: Any) -> TaskApiAuthenticationStrategyPlan:
    """Compatibility alias for recommending API authentication strategy gaps."""
    return build_task_api_authentication_strategy_plan(source)


def task_api_authentication_strategy_plan_to_dict(result: TaskApiAuthenticationStrategyPlan) -> dict[str, Any]:
    """Serialize an API authentication strategy plan to a plain dictionary."""
    return result.to_dict()


task_api_authentication_strategy_plan_to_dict.__test__ = False


def task_api_authentication_strategy_plan_to_dicts(
    result: TaskApiAuthenticationStrategyPlan | Iterable[TaskApiAuthenticationStrategyFinding],
) -> list[dict[str, Any]]:
    """Serialize API authentication strategy findings to plain dictionaries."""
    if isinstance(result, TaskApiAuthenticationStrategyPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_api_authentication_strategy_plan_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[ApiAuthenticationSignal, ...] = field(default_factory=tuple)
    strategies: tuple[ApiAuthenticationStrategy, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskApiAuthenticationStrategyFinding | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None

    missing = tuple(strategy for strategy in _STRATEGY_ORDER if strategy not in signals.strategies)
    return TaskApiAuthenticationStrategyFinding(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        detected_signals=signals.signals,
        present_strategies=signals.strategies,
        missing_strategies=missing,  # type: ignore[arg-type]
        readiness=_readiness(signals.signals, missing),  # type: ignore[arg-type]
        evidence=signals.evidence,
        recommended_checks=tuple(_ACTIONABLE_GAPS[strategy] for strategy in missing),  # type: ignore[index]
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[ApiAuthenticationSignal] = set()
    strategy_hits: set[ApiAuthenticationStrategy] = set()
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
        for strategy, pattern in _STRATEGY_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                strategy_hits.add(strategy)
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
        for strategy, pattern in _STRATEGY_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                strategy_hits.add(strategy)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        strategies=tuple(strategy for strategy in _STRATEGY_ORDER if strategy in strategy_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _readiness(
    signals: tuple[ApiAuthenticationSignal, ...],
    missing: tuple[ApiAuthenticationStrategy, ...],
) -> ApiAuthenticationReadiness:
    if not missing:
        return "ready"
    missing_set = set(missing)

    # Critical strategies for production API authentication
    critical = {
        "token_expiry_validation",
        "audit_logging",
        "revocation_mechanism",
    }

    # If missing 2 or fewer strategies and none are critical, considered ready
    if len(missing) <= 2 and not (critical & missing_set):
        return "ready"

    # If missing 6 or more strategies, definitely weak
    if len(missing) >= 6:
        return "weak"

    # If missing critical strategies and 4 or more total, weak
    if critical & missing_set and len(missing) >= 4:
        return "weak"

    # If authentication signals present but missing both expiry validation and audit logging, weak
    if {"bearer_token", "oauth"} & set(signals) and {
        "token_expiry_validation",
        "audit_logging",
    } <= missing_set:
        return "weak"

    # If credential rotation signal present but missing rotation policy and revocation
    if "credential_rotation" in signals and {
        "credential_rotation_policy",
        "revocation_mechanism",
    } <= missing_set and len(missing) >= 4:
        return "weak"

    return "partial"


def _summary(
    findings: tuple[TaskApiAuthenticationStrategyFinding, ...],
    *,
    total_task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "impacted_task_count": len(findings),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_strategy_count": sum(len(finding.missing_strategies) for finding in findings),
        "readiness_counts": {readiness: sum(1 for finding in findings if finding.readiness == readiness) for readiness in _READINESS_ORDER},
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_strategy_counts": {
            strategy: sum(1 for finding in findings if strategy in finding.present_strategies)
            for strategy in _STRATEGY_ORDER
        },
        "missing_strategy_counts": {
            strategy: sum(1 for finding in findings if strategy in finding.missing_strategies)
            for strategy in _STRATEGY_ORDER
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
        "validation_command",
        "validation_commands",
        "test_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))
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
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_STRATEGY_PATTERNS.values()])


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
