"""Plan API authentication readiness safeguards for execution tasks."""

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
    "token_expiry",
    "credential_hashing",
    "rotation_revocation",
    "auth_failure",
]
ApiAuthenticationSafeguard = Literal[
    "api_key_or_bearer_token_mechanism",
    "oauth_client_credentials_flow",
    "token_expiry_validation",
    "credential_hashing_or_encryption",
    "rotation_or_revocation_workflow",
    "401_unauthorized_response",
    "test_coverage",
]
ApiAuthenticationReadiness = Literal["ready", "partial", "weak"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[ApiAuthenticationSignal, ...] = (
    "api_key",
    "bearer_token",
    "oauth",
    "token_expiry",
    "credential_hashing",
    "rotation_revocation",
    "auth_failure",
)
_SAFEGUARD_ORDER: tuple[ApiAuthenticationSafeguard, ...] = (
    "api_key_or_bearer_token_mechanism",
    "oauth_client_credentials_flow",
    "token_expiry_validation",
    "credential_hashing_or_encryption",
    "rotation_or_revocation_workflow",
    "401_unauthorized_response",
    "test_coverage",
)
_READINESS_ORDER: dict[ApiAuthenticationReadiness, int] = {"weak": 0, "partial": 1, "ready": 2}
_SIGNAL_PATTERNS: dict[ApiAuthenticationSignal, re.Pattern[str]] = {
    "api_key": re.compile(
        r"\b(?:api keys?|api[- ]?key auth(?:entication)?|developer keys?|access keys?|"
        r"client keys?|secret keys?|x-api-key)\b",
        re.I,
    ),
    "bearer_token": re.compile(
        r"\b(?:bearer tokens?|bearer auth(?:entication)?|access tokens?|jwt|json web tokens?|"
        r"authorization: bearer|authorization header|token auth)\b",
        re.I,
    ),
    "oauth": re.compile(
        r"\b(?:oauth|oauth2|oauth 2\.0|client credentials|client[- ]?id|client[- ]?secret|"
        r"grant[- ]?type|token endpoint|authorization flow)\b",
        re.I,
    ),
    "token_expiry": re.compile(
        r"\b(?:token expiry|token expiration|ttl|time to live|expires?(?:d| at| in)?|"
        r"lifetime|validity|refresh tokens?|short[- ]?lived|long[- ]?lived)\b",
        re.I,
    ),
    "credential_hashing": re.compile(
        r"\b(?:hash(?:ed|ing)?|bcrypt|argon2|scrypt|pbkdf2|encrypt(?:ed|ion)?|"
        r"aes[- ]?256|secret manager|vault|secure storage|one[- ]?way)\b",
        re.I,
    ),
    "rotation_revocation": re.compile(
        r"\b(?:rotate|rotation|rotated|regenerate|reissue|rollover|"
        r"revoke|revocation|revoked|invalidate|deactivate|compromised)\b",
        re.I,
    ),
    "auth_failure": re.compile(
        r"\b(?:401|unauthorized|unauthenticated|authentication failed?|invalid token|"
        r"invalid api[- ]?key|missing token|expired token|www[- ]?authenticate)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[ApiAuthenticationSignal, re.Pattern[str]] = {
    "api_key": re.compile(r"api[_-]?keys?|developer[_-]?keys?|access[_-]?keys?", re.I),
    "bearer_token": re.compile(r"bearer|tokens?|jwt|access[_-]?tokens?", re.I),
    "oauth": re.compile(r"oauth|client[_-]?credentials|authorization", re.I),
    "token_expiry": re.compile(r"expiry|expiration|ttl|refresh|lifetime", re.I),
    "credential_hashing": re.compile(r"hash(?:ed|ing)?|bcrypt|argon2|encrypt(?:ed|ion)?|vault", re.I),
    "rotation_revocation": re.compile(r"rotat(?:e|ed|ion)|revok(?:e|ed|ation)|regenerate", re.I),
    "auth_failure": re.compile(r"401|unauthorized|auth[_-]?fail(?:ure)?", re.I),
}
_SAFEGUARD_PATTERNS: dict[ApiAuthenticationSafeguard, re.Pattern[str]] = {
    "api_key_or_bearer_token_mechanism": re.compile(
        r"\b(?:api[- ]?key|bearer token|x-api-key|authorization: bearer|authorization header|"
        r"token validation|key validation|verify api[- ]?key|verify bearer|parse token|"
        r"extract token|extract api[- ]?key)\b",
        re.I,
    ),
    "oauth_client_credentials_flow": re.compile(
        r"\b(?:oauth|client credentials|client[- ]?id|client[- ]?secret|grant[- ]?type|"
        r"token endpoint|/token|/oauth/token|authorization server|scope validation)\b",
        re.I,
    ),
    "token_expiry_validation": re.compile(
        r"\b(?:expiry|expiration|ttl|expires?(?:d| at)?|check expiry|validate expiry|"
        r"exp claim|not after|valid until|refresh token|reissue|renew)\b",
        re.I,
    ),
    "credential_hashing_or_encryption": re.compile(
        r"\b(?:hash(?:ed|ing)?|bcrypt|argon2|scrypt|pbkdf2|encrypt(?:ed|ion)?|"
        r"aes[- ]?256|secret manager|vault|kms|hsm|one[- ]?way hash|"
        r"compare hash|verify hash|salt|pepper)\b",
        re.I,
    ),
    "rotation_or_revocation_workflow": re.compile(
        r"\b(?:rotate|rotation|regenerate|reissue|rollover|revoke|revocation|"
        r"invalidate|deactivate|expire manually|force expiry|delete api[- ]?key|"
        r"delete token|compromised|mark revoked|blacklist)\b",
        re.I,
    ),
    "401_unauthorized_response": re.compile(
        r"\b(?:401|unauthorized|unauthenticated|return 401|respond 401|"
        r"www[- ]?authenticate header|authentication failed|invalid credentials?|"
        r"missing auth(?:entication)?|expired auth)\b",
        re.I,
    ),
    "test_coverage": re.compile(
        r"\b(?:tests?|test coverage|unit tests?|integration tests?|auth tests?|"
        r"authentication tests?|token tests?|api[- ]?key tests?|401 tests?|"
        r"unauthorized tests?|expiry tests?|rotation tests?|revocation tests?)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:api auth(?:entication)?|api keys?|bearer tokens?|oauth|"
    r"client credentials|token expiry|credential storage|rotation|revocation)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_ACTIONABLE_GAPS: dict[ApiAuthenticationSafeguard, str] = {
    "api_key_or_bearer_token_mechanism": "Define API key or bearer token creation, validation, and extraction from request headers.",
    "oauth_client_credentials_flow": "Specify OAuth client credentials flow, client ID/secret validation, token endpoint, and scope enforcement.",
    "token_expiry_validation": "Implement token expiry checks, exp claim validation, TTL enforcement, and refresh token handling.",
    "credential_hashing_or_encryption": "Hash or encrypt credentials using bcrypt, argon2, or secret manager; avoid plaintext storage.",
    "rotation_or_revocation_workflow": "Provide API key or token rotation, revocation, regeneration, and compromised credential handling.",
    "401_unauthorized_response": "Return 401 Unauthorized with WWW-Authenticate header for missing, invalid, or expired credentials.",
    "test_coverage": "Add tests for auth success, 401 failures, token expiry, rotation, revocation, and invalid credentials.",
}


@dataclass(frozen=True, slots=True)
class TaskApiAuthenticationReadinessFinding:
    """API authentication readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[ApiAuthenticationSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[ApiAuthenticationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[ApiAuthenticationSafeguard, ...] = field(default_factory=tuple)
    readiness: ApiAuthenticationReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)

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
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskApiAuthenticationReadinessPlan:
    """Plan-level API authentication readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiAuthenticationReadinessFinding, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiAuthenticationReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
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
        """Return readiness findings as plain dictionaries."""
        return [record.to_dict() for record in self.findings]


def build_task_api_authentication_readiness_plan(source: Any) -> TaskApiAuthenticationReadinessPlan:
    """Build API authentication readiness findings for relevant execution tasks."""
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
    return TaskApiAuthenticationReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        impacted_task_ids=tuple(finding.task_id for finding in findings),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_api_authentication_readiness(source: Any) -> TaskApiAuthenticationReadinessPlan:
    """Compatibility alias for building API authentication readiness plans."""
    return build_task_api_authentication_readiness_plan(source)


def summarize_task_api_authentication_readiness(source: Any) -> TaskApiAuthenticationReadinessPlan:
    """Compatibility alias for building API authentication readiness plans."""
    return build_task_api_authentication_readiness_plan(source)


def extract_task_api_authentication_readiness(source: Any) -> TaskApiAuthenticationReadinessPlan:
    """Compatibility alias for extracting API authentication readiness plans."""
    return build_task_api_authentication_readiness_plan(source)


def generate_task_api_authentication_readiness(source: Any) -> TaskApiAuthenticationReadinessPlan:
    """Compatibility alias for generating API authentication readiness plans."""
    return build_task_api_authentication_readiness_plan(source)


def recommend_task_api_authentication_readiness(source: Any) -> TaskApiAuthenticationReadinessPlan:
    """Compatibility alias for recommending API authentication readiness gaps."""
    return build_task_api_authentication_readiness_plan(source)


def task_api_authentication_readiness_plan_to_dict(result: TaskApiAuthenticationReadinessPlan) -> dict[str, Any]:
    """Serialize an API authentication readiness plan to a plain dictionary."""
    return result.to_dict()


task_api_authentication_readiness_plan_to_dict.__test__ = False


def task_api_authentication_readiness_plan_to_dicts(
    result: TaskApiAuthenticationReadinessPlan | Iterable[TaskApiAuthenticationReadinessFinding],
) -> list[dict[str, Any]]:
    """Serialize API authentication readiness findings to plain dictionaries."""
    if isinstance(result, TaskApiAuthenticationReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_api_authentication_readiness_plan_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[ApiAuthenticationSignal, ...] = field(default_factory=tuple)
    safeguards: tuple[ApiAuthenticationSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskApiAuthenticationReadinessFinding | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.safeguards)
    return TaskApiAuthenticationReadinessFinding(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        detected_signals=signals.signals,
        present_safeguards=signals.safeguards,
        missing_safeguards=missing,
        readiness=_readiness(signals.signals, missing),
        evidence=signals.evidence,
        recommended_checks=tuple(_ACTIONABLE_GAPS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[ApiAuthenticationSignal] = set()
    safeguard_hits: set[ApiAuthenticationSafeguard] = set()
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
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
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
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _readiness(
    signals: tuple[ApiAuthenticationSignal, ...],
    missing: tuple[ApiAuthenticationSafeguard, ...],
) -> ApiAuthenticationReadiness:
    if not missing:
        return "ready"
    missing_set = set(missing)
    critical = {
        "api_key_or_bearer_token_mechanism",
        "credential_hashing_or_encryption",
        "401_unauthorized_response",
    }
    if len(missing) >= 5:
        return "weak"
    if critical & missing_set and len(missing) >= 3:
        return "weak"
    if {"api_key", "bearer_token", "oauth"} & set(signals) and {
        "api_key_or_bearer_token_mechanism",
        "credential_hashing_or_encryption",
    } <= missing_set:
        return "weak"
    return "partial"


def _summary(
    findings: tuple[TaskApiAuthenticationReadinessFinding, ...],
    *,
    total_task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "impacted_task_count": len(findings),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(finding.missing_safeguards) for finding in findings),
        "readiness_counts": {readiness: sum(1 for finding in findings if finding.readiness == readiness) for readiness in _READINESS_ORDER},
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
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
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


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
