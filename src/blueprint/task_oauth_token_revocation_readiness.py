"""Plan OAuth token revocation readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


OAuthRevocationSignal = Literal[
    "token_revocation",
    "refresh_token",
    "session_cleanup",
    "audit_logging",
    "connected_apps",
    "logout_flow",
]
OAuthRevocationSafeguard = Literal[
    "token_invalidation_mechanism",
    "refresh_token_cleanup",
    "active_session_termination",
    "audit_trail_capture",
    "cascade_revocation",
    "revocation_verification",
]
OAuthRevocationReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[OAuthRevocationReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[OAuthRevocationSignal, ...] = (
    "token_revocation",
    "refresh_token",
    "session_cleanup",
    "audit_logging",
    "connected_apps",
    "logout_flow",
)
_SAFEGUARD_ORDER: tuple[OAuthRevocationSafeguard, ...] = (
    "token_invalidation_mechanism",
    "refresh_token_cleanup",
    "active_session_termination",
    "audit_trail_capture",
    "cascade_revocation",
    "revocation_verification",
)
_SIGNAL_PATTERNS: dict[OAuthRevocationSignal, re.Pattern[str]] = {
    "token_revocation": re.compile(
        r"\b(?:token revocation|revoke token|token invalidation|invalidate token|"
        r"token expiry|expire token|token deletion|delete token|token removal|remove token|"
        r"oauth revoke|oauth revocation|access token revocation)\b",
        re.I,
    ),
    "refresh_token": re.compile(
        r"\b(?:refresh token|token refresh|refresh token rotation|rotate refresh token|"
        r"refresh token invalidation|invalidate refresh token|refresh token revocation|"
        r"refresh token cleanup|refresh token expiry)\b",
        re.I,
    ),
    "session_cleanup": re.compile(
        r"\b(?:session cleanup|clean up session|session termination|terminate session|"
        r"session invalidation|invalidate session|session revocation|revoke session|"
        r"active session|session removal|clear session|session logout)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit log|audit trail|audit logging|log revocation|revocation log|"
        r"revocation event|revocation audit|security log|security event|"
        r"audit event|event logging|revocation tracking)\b",
        re.I,
    ),
    "connected_apps": re.compile(
        r"\b(?:connected app|app disconnect|disconnect app|authorized app|"
        r"third-party app|oauth app|oauth client|client disconnect|"
        r"app revocation|revoke app|app authorization|authorized client)\b",
        re.I,
    ),
    "logout_flow": re.compile(
        r"\b(?:logout|log out|sign out|signout|logout flow|logout endpoint|"
        r"logout handler|logout process|user logout|session logout|"
        r"logout redirect|logout callback)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[OAuthRevocationSignal, re.Pattern[str]] = {
    "token_revocation": re.compile(r"token|revoke|revocation|oauth|invalidate", re.I),
    "refresh_token": re.compile(r"refresh|token|rotation", re.I),
    "session_cleanup": re.compile(r"session|cleanup|terminate", re.I),
    "audit_logging": re.compile(r"audit|log|event|tracking", re.I),
    "connected_apps": re.compile(r"app|client|connected|authorized", re.I),
    "logout_flow": re.compile(r"logout|signout", re.I),
}
_SAFEGUARD_PATTERNS: dict[OAuthRevocationSafeguard, re.Pattern[str]] = {
    "token_invalidation_mechanism": re.compile(
        r"\b(?:(?:token|access).{0,80}(?:invalidation|invalidate|revocation|revoke|deletion|delete|removal|remove|mechanism|implementation|procedure)|"
        r"(?:invalidation|invalidate|revocation|revoke|deletion|delete|removal|remove|mechanism|implementation|procedure).{0,80}(?:token|access)|"
        r"token blacklist|blacklist token|token store|token database|token cache|invalidate access)\b",
        re.I,
    ),
    "refresh_token_cleanup": re.compile(
        r"\b(?:(?:refresh.{0,20}token).{0,80}(?:cleanup|clean up|invalidation|invalidate|revocation|revoke|deletion|delete|removal|remove|rotation|rotate)|"
        r"(?:cleanup|clean up|invalidation|invalidate|revocation|revoke|deletion|delete|removal|remove|rotation|rotate).{0,80}(?:refresh.{0,20}token)|"
        r"revoke refresh token|invalidate refresh|rotate refresh)\b",
        re.I,
    ),
    "active_session_termination": re.compile(
        r"\b(?:(?:active.{0,20}session|session).{0,80}(?:termination|terminate|cleanup|clean up|invalidation|invalidate|revocation|revoke|removal|remove|clear)|"
        r"(?:termination|terminate|cleanup|clean up|invalidation|invalidate|revocation|revoke|removal|remove|clear).{0,80}(?:active.{0,20}session|session)|"
        r"terminate session|clear session|invalidate session|session termination)\b",
        re.I,
    ),
    "audit_trail_capture": re.compile(
        r"\b(?:(?:audit|revocation).{0,80}(?:trail|log|logging|event|tracking|capture|record|store)|"
        r"(?:trail|log|logging|event|tracking|capture|record|store).{0,80}(?:audit|revocation)|"
        r"log revocation|audit event|security log|track revocation|record revocation)\b",
        re.I,
    ),
    "cascade_revocation": re.compile(
        r"\b(?:(?:cascade|cascading|related|associated).{0,80}(?:revocation|revoke|invalidation|invalidate|cleanup|clean up)|"
        r"(?:revocation|revoke|invalidation|invalidate|cleanup|clean up).{0,80}(?:cascade|cascading|related|associated)|"
        r"revoke related|invalidate related|cascade invalidation|cascading revocation)\b",
        re.I,
    ),
    "revocation_verification": re.compile(
        r"\b(?:(?:revocation|logout|invalidation).{0,80}(?:verification|verify|test|testing|validation|validate|check|checking|regression|coverage)|"
        r"(?:verification|verify|test|testing|validation|validate|check|checking|regression|coverage).{0,80}(?:revocation|logout|invalidation)|"
        r"verify revocation|test revocation|revocation test|validate invalidation|revocation regression)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:revocation|token|logout|oauth|session)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[OAuthRevocationSafeguard, str] = {
    "token_invalidation_mechanism": "Implement token invalidation mechanism with token blacklist, database update, or cache invalidation to prevent revoked tokens from being accepted.",
    "refresh_token_cleanup": "Ensure refresh token cleanup by revoking or rotating refresh tokens when access tokens are invalidated to prevent token refresh after revocation.",
    "active_session_termination": "Implement active session termination to clear server-side session state and force re-authentication after token revocation or logout.",
    "audit_trail_capture": "Add audit trail capture to log revocation events with user ID, token ID, timestamp, reason, and IP address for security tracking and compliance.",
    "cascade_revocation": "Implement cascade revocation to invalidate related tokens (refresh tokens, session tokens, connected app tokens) when primary token is revoked.",
    "revocation_verification": "Add revocation verification with regression tests covering token invalidation, session cleanup, and re-authentication requirements after revocation.",
}


@dataclass(frozen=True, slots=True)
class TaskOAuthTokenRevocationReadinessFinding:
    """OAuth token revocation readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[OAuthRevocationSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[OAuthRevocationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[OAuthRevocationSafeguard, ...] = field(default_factory=tuple)
    readiness: OAuthRevocationReadiness = "partial"
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
class TaskOAuthTokenRevocationReadinessPlan:
    """Plan-level OAuth token revocation readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskOAuthTokenRevocationReadinessFinding, ...] = field(default_factory=tuple)
    oauth_revocation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskOAuthTokenRevocationReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "oauth_revocation_task_ids": list(self.oauth_revocation_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_markdown(self) -> str:
        """Render OAuth token revocation readiness as deterministic Markdown."""
        title = "# Task OAuth Token Revocation Readiness Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.findings:
            lines.extend(["", "No OAuth token revocation readiness findings were derived."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Readiness | Detected Signals | Present Safeguards | Missing Safeguards | Actionable Remediations |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for finding in self.findings:
            lines.append(
                "| "
                f"`{_markdown_cell(finding.task_id)}` | "
                f"{finding.readiness} | "
                f"{_markdown_cell(', '.join(finding.detected_signals))} | "
                f"{_markdown_cell(', '.join(finding.present_safeguards))} | "
                f"{_markdown_cell(', '.join(finding.missing_safeguards))} | "
                f"{_markdown_cell('; '.join(finding.actionable_remediations))} |"
            )
        return "\n".join(lines)


def plan_task_oauth_token_revocation_readiness(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskOAuthTokenRevocationReadinessPlan:
    """Generate OAuth token revocation readiness safeguards for all tasks in a plan."""
    plan_id, tasks = _plan_payload(plan)
    findings: list[TaskOAuthTokenRevocationReadinessFinding] = []
    oauth_revocation_task_ids: list[str] = []
    not_applicable_task_ids: list[str] = []

    for task in tasks:
        task_id, task_payload = _task_payload(task)
        if not task_id:
            continue

        signals = _detect_signals(task_payload)
        if _has_no_impact(task_payload) or not signals:
            not_applicable_task_ids.append(task_id)
            continue

        oauth_revocation_task_ids.append(task_id)
        present_safeguards = _detect_safeguards(task_payload, signals)
        missing_safeguards = tuple(
            safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in present_safeguards
        )
        evidence = _collect_evidence(task_payload, signals)
        remediations = tuple(_REMEDIATIONS[safeguard] for safeguard in missing_safeguards[:3])
        readiness = _assess_readiness(signals, present_safeguards, missing_safeguards)

        findings.append(
            TaskOAuthTokenRevocationReadinessFinding(
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

    return TaskOAuthTokenRevocationReadinessPlan(
        plan_id=plan_id,
        findings=tuple(findings),
        oauth_revocation_task_ids=tuple(oauth_revocation_task_ids),
        not_applicable_task_ids=tuple(not_applicable_task_ids),
        summary=_plan_summary(findings, oauth_revocation_task_ids, not_applicable_task_ids),
    )


def task_oauth_token_revocation_readiness_to_dict(
    plan: TaskOAuthTokenRevocationReadinessPlan,
) -> dict[str, Any]:
    """Serialize an OAuth token revocation readiness plan to a plain dictionary."""
    return plan.to_dict()


task_oauth_token_revocation_readiness_to_dict.__test__ = False


def task_oauth_token_revocation_readiness_to_markdown(
    plan: TaskOAuthTokenRevocationReadinessPlan,
) -> str:
    """Render an OAuth token revocation readiness plan as Markdown."""
    return plan.to_markdown()


task_oauth_token_revocation_readiness_to_markdown.__test__ = False


def analyze_task_oauth_token_revocation_readiness(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskOAuthTokenRevocationReadinessPlan:
    """Analyze OAuth token revocation readiness for tasks in a plan (alias)."""
    return plan_task_oauth_token_revocation_readiness(plan)


def extract_task_oauth_token_revocation_readiness(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskOAuthTokenRevocationReadinessPlan:
    """Extract OAuth token revocation readiness findings from a plan (alias)."""
    return plan_task_oauth_token_revocation_readiness(plan)


def generate_task_oauth_token_revocation_readiness(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskOAuthTokenRevocationReadinessPlan:
    """Generate OAuth token revocation readiness plan (alias)."""
    return plan_task_oauth_token_revocation_readiness(plan)


def recommend_task_oauth_token_revocation_readiness(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskOAuthTokenRevocationReadinessPlan:
    """Recommend OAuth token revocation readiness safeguards (alias)."""
    return plan_task_oauth_token_revocation_readiness(plan)


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


def _detect_signals(task_payload: Mapping[str, Any]) -> tuple[OAuthRevocationSignal, ...]:
    text = _searchable_text(task_payload)
    paths = _searchable_paths(task_payload)
    signals: list[OAuthRevocationSignal] = []

    for signal in _SIGNAL_ORDER:
        if _SIGNAL_PATTERNS[signal].search(text):
            signals.append(signal)
        elif paths and _PATH_SIGNAL_PATTERNS[signal].search(paths):
            signals.append(signal)

    return tuple(signals)


def _detect_safeguards(
    task_payload: Mapping[str, Any],
    signals: tuple[OAuthRevocationSignal, ...],
) -> tuple[OAuthRevocationSafeguard, ...]:
    text = _searchable_text(task_payload)
    safeguards: list[OAuthRevocationSafeguard] = []

    for safeguard in _SAFEGUARD_ORDER:
        if _SAFEGUARD_PATTERNS[safeguard].search(text):
            safeguards.append(safeguard)

    return tuple(safeguards)


def _has_no_impact(task_payload: Mapping[str, Any]) -> bool:
    text = _searchable_text(task_payload)
    return bool(_NO_IMPACT_RE.search(text))


def _collect_evidence(
    task_payload: Mapping[str, Any],
    signals: tuple[OAuthRevocationSignal, ...],
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
    signals: tuple[OAuthRevocationSignal, ...],
    present_safeguards: tuple[OAuthRevocationSafeguard, ...],
    missing_safeguards: tuple[OAuthRevocationSafeguard, ...],
) -> OAuthRevocationReadiness:
    if not signals:
        return "weak"
    safeguard_coverage = len(present_safeguards) / (len(present_safeguards) + len(missing_safeguards)) if (len(present_safeguards) + len(missing_safeguards)) > 0 else 0.0
    if safeguard_coverage >= 0.75:
        return "strong"
    if safeguard_coverage >= 0.5:
        return "partial"
    return "weak"


def _plan_summary(
    findings: list[TaskOAuthTokenRevocationReadinessFinding],
    oauth_revocation_task_ids: list[str],
    not_applicable_task_ids: list[str],
) -> dict[str, Any]:
    return {
        "total_tasks_reviewed": len(oauth_revocation_task_ids) + len(not_applicable_task_ids),
        "oauth_revocation_task_count": len(oauth_revocation_task_ids),
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


def _overall_readiness(findings: list[TaskOAuthTokenRevocationReadinessFinding]) -> OAuthRevocationReadiness:
    if not findings:
        return "weak"
    weak_count = sum(1 for finding in findings if finding.readiness == "weak")
    if weak_count > len(findings) / 2:
        return "weak"
    strong_count = sum(1 for finding in findings if finding.readiness == "strong")
    if strong_count >= len(findings) * 0.7:
        return "strong"
    return "partial"


def _top_missing_safeguards(findings: list[TaskOAuthTokenRevocationReadinessFinding]) -> list[str]:
    safeguard_counts: dict[OAuthRevocationSafeguard, int] = {}
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


def _markdown_cell(text: str) -> str:
    """Escape pipe characters for markdown table cells."""
    return text.replace("|", "\\|")


__all__ = [
    "OAuthRevocationReadiness",
    "OAuthRevocationSafeguard",
    "OAuthRevocationSignal",
    "TaskOAuthTokenRevocationReadinessFinding",
    "TaskOAuthTokenRevocationReadinessPlan",
    "plan_task_oauth_token_revocation_readiness",
    "task_oauth_token_revocation_readiness_to_dict",
    "task_oauth_token_revocation_readiness_to_markdown",
    "analyze_task_oauth_token_revocation_readiness",
    "extract_task_oauth_token_revocation_readiness",
    "generate_task_oauth_token_revocation_readiness",
    "recommend_task_oauth_token_revocation_readiness",
]
