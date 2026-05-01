"""Plan OAuth scope readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


OAuthScopeRiskLevel = Literal["low", "medium", "high"]
AuthSurface = Literal[
    "oauth",
    "api token",
    "service account",
    "delegated permission",
    "third-party app",
    "consent screen",
    "scope configuration",
]
ScopeSignal = Literal[
    "read_scope",
    "write_scope",
    "admin_scope",
    "wildcard_scope",
    "offline_access",
    "impersonation",
    "sensitive_token",
]
MissingControl = Literal[
    "least_privilege_scope_review",
    "consent_screen_review",
    "token_rotation_plan",
    "revocation_path",
    "audit_logging",
    "negative_permission_tests",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: dict[AuthSurface, int] = {
    "oauth": 0,
    "api token": 1,
    "service account": 2,
    "delegated permission": 3,
    "third-party app": 4,
    "consent screen": 5,
    "scope configuration": 6,
}
_SIGNAL_ORDER: dict[ScopeSignal, int] = {
    "read_scope": 0,
    "write_scope": 1,
    "admin_scope": 2,
    "wildcard_scope": 3,
    "offline_access": 4,
    "impersonation": 5,
    "sensitive_token": 6,
}
_CONTROL_ORDER: dict[MissingControl, int] = {
    "least_privilege_scope_review": 0,
    "consent_screen_review": 1,
    "token_rotation_plan": 2,
    "revocation_path": 3,
    "audit_logging": 4,
    "negative_permission_tests": 5,
}
_SURFACE_PATTERNS: dict[AuthSurface, re.Pattern[str]] = {
    "oauth": re.compile(r"\b(?:oauth|oauth2|oidc|openid connect|authorization code|refresh token)\b", re.I),
    "api token": re.compile(r"\b(?:api token|access token|bearer token|personal access token|pat|client secret)\b", re.I),
    "service account": re.compile(r"\b(?:service account|workload identity|machine account|client credentials)\b", re.I),
    "delegated permission": re.compile(r"\b(?:delegated permission|delegated access|delegation|on behalf of|user delegated|obo)\b", re.I),
    "third-party app": re.compile(r"\b(?:third[- ]party app|connected app|marketplace app|oauth app|app registration)\b", re.I),
    "consent screen": re.compile(r"\b(?:consent screen|consent prompt|admin consent|user consent)\b", re.I),
    "scope configuration": re.compile(r"\b(?:scopes?|permissions?|grants?|entitlements?)\b", re.I),
}
_PATH_PATTERNS: dict[AuthSurface, re.Pattern[str]] = {
    "oauth": re.compile(r"(?:^|/)(?:oauth|oidc|auth)(?:/|$)|oauth", re.I),
    "api token": re.compile(r"(?:^|/)(?:tokens?|credentials?|secrets?)(?:/|$)|api[_-]?token|access[_-]?token", re.I),
    "service account": re.compile(r"service[_-]?account|workload[_-]?identity", re.I),
    "delegated permission": re.compile(r"delegat|permissions?", re.I),
    "third-party app": re.compile(r"third[_-]?party|connected[_-]?app|app[_-]?registration", re.I),
    "consent screen": re.compile(r"consent", re.I),
    "scope configuration": re.compile(r"(?:^|/)(?:scopes?|permissions?|grants?)(?:/|$)|scope", re.I),
}
_SIGNAL_PATTERNS: dict[ScopeSignal, re.Pattern[str]] = {
    "read_scope": re.compile(r"\b(?:read[-_:][a-z0-9_.:-]+|[a-z0-9_.:-]+\.read|readonly|read-only|read only)\b", re.I),
    "write_scope": re.compile(
        r"\b(?:write[-_:][a-z0-9_.:-]+|[a-z0-9_.:-]+\.write|read/write|read and write|"
        r"(?:modify|delete|create|update)\s+(?:scopes?|permissions?|grants?|access))\b",
        re.I,
    ),
    "admin_scope": re.compile(r"\b(?:admin(?:istrator)?|superuser|directory\.readwrite\.all|manage[_:. -]?all|full access)\b", re.I),
    "wildcard_scope": re.compile(r"(?:\*|all scopes|\ball\b permissions|\bfull_access\b|\b[A-Za-z0-9_.:-]+:\*)", re.I),
    "offline_access": re.compile(r"\b(?:offline_access|offline access|refresh token|long[- ]lived token|background access)\b", re.I),
    "impersonation": re.compile(r"\b(?:impersonat\w+|act as user|domain[- ]wide delegation|on behalf of|delegated admin)\b", re.I),
    "sensitive_token": re.compile(r"\b(?:client secret|api token|bearer token|personal access token|service account key)\b", re.I),
}
_CONTROL_PATTERNS: dict[MissingControl, re.Pattern[str]] = {
    "least_privilege_scope_review": re.compile(r"\b(?:least privilege|minimal scopes?|scope review|permission review)\b", re.I),
    "consent_screen_review": re.compile(r"\b(?:consent screen|consent prompt|admin consent|user consent).*\b(?:review|verify|approved?|copy)\b|\b(?:review|verify|approved?).*\b(?:consent screen|consent prompt|admin consent|user consent)\b", re.I),
    "token_rotation_plan": re.compile(r"\b(?:token rotation|rotate tokens?|credential rotation|secret rotation|key rotation)\b", re.I),
    "revocation_path": re.compile(r"\b(?:revocation|revoke access|disconnect app|remove grant|revoke tokens?)\b", re.I),
    "audit_logging": re.compile(r"\b(?:audit logs?|audit events?|logging|log privileged|security log)\b", re.I),
    "negative_permission_tests": re.compile(r"\b(?:negative permission tests?|deny tests?|unauthorized tests?|forbidden tests?|403 tests?)\b", re.I),
}


@dataclass(frozen=True, slots=True)
class TaskOAuthScopeReadinessRecommendation:
    """OAuth scope readiness guidance for one affected execution task."""

    task_id: str
    title: str
    auth_surfaces: tuple[AuthSurface, ...] = field(default_factory=tuple)
    requested_scope_signals: tuple[ScopeSignal, ...] = field(default_factory=tuple)
    missing_controls: tuple[MissingControl, ...] = field(default_factory=tuple)
    risk_level: OAuthScopeRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "auth_surfaces": list(self.auth_surfaces),
            "requested_scope_signals": list(self.requested_scope_signals),
            "missing_controls": list(self.missing_controls),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskOAuthScopeReadinessPlan:
    """Plan-level OAuth scope readiness summary."""

    plan_id: str | None = None
    recommendations: tuple[TaskOAuthScopeReadinessRecommendation, ...] = field(default_factory=tuple)
    flagged_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [record.to_dict() for record in self.recommendations],
            "flagged_task_ids": list(self.flagged_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return recommendations as plain dictionaries."""
        return [record.to_dict() for record in self.recommendations]


def build_task_oauth_scope_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskOAuthScopeReadinessPlan:
    """Build OAuth scope readiness recommendations for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _record_for_task(task, index)) is not None
            ),
            key=lambda record: (record.task_id, record.title.casefold()),
        )
    )
    return TaskOAuthScopeReadinessPlan(
        plan_id=plan_id,
        recommendations=records,
        flagged_task_ids=tuple(record.task_id for record in records),
        summary=_summary(records, total_task_count=len(tasks)),
    )


def generate_task_oauth_scope_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[TaskOAuthScopeReadinessRecommendation, ...]:
    """Return OAuth scope readiness recommendations for relevant execution tasks."""
    return build_task_oauth_scope_readiness_plan(source).recommendations


def task_oauth_scope_readiness_to_dicts(
    records: (
        tuple[TaskOAuthScopeReadinessRecommendation, ...]
        | list[TaskOAuthScopeReadinessRecommendation]
        | TaskOAuthScopeReadinessPlan
    ),
) -> list[dict[str, Any]]:
    """Serialize OAuth scope readiness recommendations to dictionaries."""
    if isinstance(records, TaskOAuthScopeReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


def task_oauth_scope_readiness_plan_to_dict(result: TaskOAuthScopeReadinessPlan) -> dict[str, Any]:
    """Serialize an OAuth scope readiness plan to a plain dictionary."""
    return result.to_dict()


task_oauth_scope_readiness_plan_to_dict.__test__ = False


def summarize_task_oauth_scope_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskOAuthScopeReadinessPlan:
    """Compatibility alias for building OAuth scope readiness plans."""
    return build_task_oauth_scope_readiness_plan(source)


def _record_for_task(
    task: Mapping[str, Any],
    index: int,
) -> TaskOAuthScopeReadinessRecommendation | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    surfaces: dict[AuthSurface, list[str]] = {}
    signals: dict[ScopeSignal, list[str]] = {}
    controls: set[MissingControl] = set()

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _inspect_path(path, surfaces, signals)

    for source_field, text in _candidate_texts(task):
        _inspect_text(source_field, text, surfaces, signals, controls)

    if not surfaces and not signals:
        return None

    auth_surfaces = tuple(sorted(surfaces, key=lambda item: _SURFACE_ORDER[item]))
    requested_scope_signals = tuple(sorted(signals, key=lambda item: _SIGNAL_ORDER[item]))
    missing_controls = tuple(
        control for control in _CONTROL_ORDER if control not in controls
    )
    return TaskOAuthScopeReadinessRecommendation(
        task_id=task_id,
        title=title,
        auth_surfaces=auth_surfaces,
        requested_scope_signals=requested_scope_signals,
        missing_controls=missing_controls,
        risk_level=_risk_level(requested_scope_signals, missing_controls),
        evidence=tuple(
            _dedupe(
                evidence
                for key in (*auth_surfaces, *requested_scope_signals)
                for evidence in (surfaces.get(key, []) if key in surfaces else signals.get(key, []))
            )
        ),
    )


def _inspect_path(
    path: str,
    surfaces: dict[AuthSurface, list[str]],
    signals: dict[ScopeSignal, list[str]],
) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    evidence = f"files_or_modules: {path}"
    lowered = normalized.casefold()
    parts = set(PurePosixPath(lowered).parts)
    for surface, pattern in _PATH_PATTERNS.items():
        if pattern.search(lowered):
            surfaces.setdefault(surface, []).append(evidence)
    if bool({"scopes", "permissions", "grants"} & parts):
        surfaces.setdefault("scope configuration", []).append(evidence)
    if "readonly" in lowered or "read_only" in lowered or "read-only" in lowered:
        signals.setdefault("read_scope", []).append(evidence)
    if any(token in lowered for token in ("write", "mutation", "admin", "impersonat")):
        _inspect_text("files_or_modules", normalized, surfaces, signals, set())


def _inspect_text(
    source_field: str,
    text: str,
    surfaces: dict[AuthSurface, list[str]],
    signals: dict[ScopeSignal, list[str]],
    controls: set[MissingControl],
) -> None:
    matched = False
    for surface, pattern in _SURFACE_PATTERNS.items():
        if pattern.search(text):
            surfaces.setdefault(surface, []).append(_evidence_snippet(source_field, text))
            matched = True
    for signal, pattern in _SIGNAL_PATTERNS.items():
        if pattern.search(text):
            signals.setdefault(signal, []).append(_evidence_snippet(source_field, text))
            matched = True
    for control, pattern in _CONTROL_PATTERNS.items():
        if pattern.search(text):
            controls.add(control)
    if matched and any(word in text.casefold() for word in ("scope", "permission", "grant")):
        surfaces.setdefault("scope configuration", []).append(_evidence_snippet(source_field, text))


def _risk_level(
    requested_scope_signals: tuple[ScopeSignal, ...],
    missing_controls: tuple[MissingControl, ...],
) -> OAuthScopeRiskLevel:
    if any(
        signal in requested_scope_signals
        for signal in ("write_scope", "admin_scope", "wildcard_scope", "offline_access", "impersonation")
    ):
        return "high"
    if not missing_controls and requested_scope_signals == ("read_scope",):
        return "low"
    if not missing_controls and requested_scope_signals:
        return "low"
    return "medium"


def _summary(
    records: tuple[TaskOAuthScopeReadinessRecommendation, ...],
    *,
    total_task_count: int,
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "flagged_task_count": len(records),
        "unrelated_task_count": max(total_task_count - len(records), 0),
        "risk_counts": {
            level: sum(1 for record in records if record.risk_level == level)
            for level in ("low", "medium", "high")
        },
        "missing_control_counts": {
            control: sum(1 for record in records if control in record.missing_controls)
            for control in _CONTROL_ORDER
        },
        "auth_surface_counts": {
            surface: sum(1 for record in records if surface in record.auth_surfaces)
            for surface in _SURFACE_ORDER
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
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

    try:
        iterator = iter(source)
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
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks"):
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in (*_SURFACE_PATTERNS.values(), *_SIGNAL_PATTERNS.values(), *_CONTROL_PATTERNS.values())):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in (*_SURFACE_PATTERNS.values(), *_SIGNAL_PATTERNS.values(), *_CONTROL_PATTERNS.values())):
                texts.append((field, key_text))
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
    "AuthSurface",
    "MissingControl",
    "OAuthScopeRiskLevel",
    "ScopeSignal",
    "TaskOAuthScopeReadinessPlan",
    "TaskOAuthScopeReadinessRecommendation",
    "build_task_oauth_scope_readiness_plan",
    "generate_task_oauth_scope_readiness",
    "summarize_task_oauth_scope_readiness",
    "task_oauth_scope_readiness_plan_to_dict",
    "task_oauth_scope_readiness_to_dicts",
]
