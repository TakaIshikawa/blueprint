"""Build deterministic mitigation guidance from implementation brief risks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ImplementationBrief


_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9-]*")
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class RiskMitigationRecord:
    """One mitigation action for a distinct implementation risk."""

    risk: str
    mitigation: str
    validation_signal: str
    related_task_ids: tuple[str, ...] = field(default_factory=tuple)
    severity: str = "low"
    owner_hint: str = "technical_lead"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "risk": self.risk,
            "mitigation": self.mitigation,
            "validation_signal": self.validation_signal,
            "related_task_ids": list(self.related_task_ids),
            "severity": self.severity,
            "owner_hint": self.owner_hint,
        }


def build_risk_mitigation_plan(
    brief: Mapping[str, Any] | ImplementationBrief,
    plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> tuple[RiskMitigationRecord, ...]:
    """Convert brief risks and optional task metadata into mitigation records."""
    brief_payload = _brief_payload(brief)
    plan_payload = _plan_payload(plan)
    task_contexts = _task_contexts(plan_payload.get("tasks") if plan_payload else None)
    validation_plan = _optional_text(brief_payload.get("validation_plan"))

    records: list[RiskMitigationRecord] = []
    for risk in _distinct_risks(brief_payload.get("risks")):
        related_tasks = _related_tasks(risk, task_contexts)
        related_task_ids = tuple(task["task_id"] for task in related_tasks)
        severity = _severity(risk, related_tasks)
        owner_hint = _owner_hint(risk, related_tasks)
        records.append(
            RiskMitigationRecord(
                risk=risk,
                mitigation=_mitigation(risk, severity, related_tasks),
                validation_signal=_validation_signal(risk, related_tasks, validation_plan),
                related_task_ids=related_task_ids,
                severity=severity,
                owner_hint=owner_hint,
            )
        )

    return tuple(records)


def risk_mitigation_plan_to_dict(
    records: tuple[RiskMitigationRecord, ...] | list[RiskMitigationRecord],
) -> list[dict[str, Any]]:
    """Serialize mitigation records to dictionaries."""
    return [record.to_dict() for record in records]


risk_mitigation_plan_to_dict.__test__ = False


def _distinct_risks(value: Any) -> list[str]:
    risks: list[str] = []
    seen: set[str] = set()
    for item in _strings(value):
        risk = _clean_risk(item)
        if risk is None:
            continue
        key = _risk_key(risk)
        if key in seen:
            continue
        risks.append(risk)
        seen.add(key)
    return risks


def _related_tasks(
    risk: str,
    task_contexts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    risk_tokens = _signal_tokens(risk)
    if not risk_tokens:
        return []

    scored: list[tuple[int, int, dict[str, Any]]] = []
    for task in task_contexts:
        overlap = risk_tokens & task["tokens"]
        score = len(overlap)
        if not score:
            continue
        if overlap & _DOMAIN_TOKENS:
            score += 2
        if overlap & _RISK_AREA_TOKENS:
            score += 1
        if score >= 2:
            scored.append((task["index"], -score, task))

    return [task for _, _, task in sorted(scored, key=lambda item: (item[0], item[1]))]


def _severity(risk: str, related_tasks: list[dict[str, Any]]) -> str:
    task_severity = _highest_severity(task["risk_level"] for task in related_tasks)
    text_severity = _text_severity(risk)
    return _highest_severity([task_severity, text_severity])


def _text_severity(text: str) -> str:
    tokens = _tokens(text)
    if tokens & _HIGH_SEVERITY_TOKENS:
        return "high"
    if tokens & _MEDIUM_SEVERITY_TOKENS:
        return "medium"
    return "low"


def _highest_severity(values: Any) -> str:
    rank = {"low": 0, "medium": 1, "high": 2, "critical": 2, "blocker": 2}
    highest = "low"
    for value in values:
        severity = _severity_value(value)
        if rank[severity] > rank[highest]:
            highest = severity
    return highest


def _severity_value(value: Any) -> str:
    text = (_optional_text(value) or "").lower()
    if text in {"critical", "blocker", "high"}:
        return "high"
    if text in {"medium", "moderate"}:
        return "medium"
    return "low"


def _owner_hint(risk: str, related_tasks: list[dict[str, Any]]) -> str:
    task_owners = [task["owner_type"] for task in related_tasks if task["owner_type"]]
    if task_owners:
        return _dedupe(task_owners)[0]

    text = " ".join([risk, *[task["context_text"] for task in related_tasks]]).lower()
    tokens = _tokens(text)
    if tokens & _SECURITY_TOKENS:
        return "security_reviewer"
    if tokens & _DATA_TOKENS:
        return "data_reviewer"
    if tokens & _RELEASE_TOKENS:
        return "release_manager"
    if tokens & _INTEGRATION_TOKENS:
        return "integration_owner"
    if tokens & _PRODUCT_TOKENS:
        return "product_owner"
    return "technical_lead"


def _mitigation(risk: str, severity: str, related_tasks: list[dict[str, Any]]) -> str:
    task_phrase = _task_phrase(related_tasks)
    tokens = _tokens(risk)
    prefix = "Create a mitigation checklist before implementation"
    if severity == "high":
        prefix = "Require pre-dispatch review and rollback notes"
    elif severity == "medium":
        prefix = "Confirm the implementation approach and fallback path"

    if tokens & _SECURITY_TOKENS:
        action = "cover permission, credential, and audit behavior"
    elif tokens & _DATA_TOKENS:
        action = "cover migration, data integrity, and restore behavior"
    elif tokens & _INTEGRATION_TOKENS:
        action = "cover contract assumptions, retries, and degraded-service behavior"
    elif tokens & _RELEASE_TOKENS:
        action = "cover rollout, configuration, and rollback behavior"
    else:
        action = "cover the expected behavior, failure mode, and rollback path"

    return f"{prefix}{task_phrase}; {action} for risk: {risk}."


def _validation_signal(
    risk: str,
    related_tasks: list[dict[str, Any]],
    validation_plan: str | None,
) -> str:
    signals: list[str] = []
    for task in related_tasks:
        signals.extend(task["test_commands"])
    for task in related_tasks:
        signals.extend(task["acceptance_criteria"][:2])
    if validation_plan:
        signals.append(validation_plan)

    if not signals:
        tokens = _tokens(risk)
        if tokens & _SECURITY_TOKENS:
            signals.append("Run focused security, permission, and credential validation.")
        elif tokens & _DATA_TOKENS:
            signals.append("Run data integrity, migration, or restore validation.")
        elif tokens & _INTEGRATION_TOKENS:
            signals.append("Run integration contract or smoke validation.")
        else:
            signals.append("Capture reviewer-approved evidence that the risk is mitigated.")

    return " | ".join(_dedupe(signals))


def _task_phrase(related_tasks: list[dict[str, Any]]) -> str:
    if not related_tasks:
        return ""
    task_ids = ", ".join(task["task_id"] for task in related_tasks)
    return f" for {task_ids}"


def _task_contexts(value: Any) -> list[dict[str, Any]]:
    contexts: list[dict[str, Any]] = []
    for index, task in enumerate(_task_payloads(value), start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        title = _optional_text(task.get("title")) or task_id
        description = _optional_text(task.get("description")) or ""
        files = _strings(task.get("files_or_modules"))
        acceptance = _strings(task.get("acceptance_criteria"))
        metadata_texts = _metadata_texts(task.get("metadata"))
        test_commands = _task_validation_commands(task)
        context_values = [
            title,
            description,
            _optional_text(task.get("risk_level")),
            *files,
            *acceptance,
            *metadata_texts,
        ]
        context_text = " ".join(value for value in context_values if value)
        contexts.append(
            {
                "index": index,
                "task_id": task_id,
                "title": title,
                "context_text": context_text,
                "tokens": _signal_tokens(context_text),
                "risk_level": _optional_text(task.get("risk_level")),
                "owner_type": _optional_text(task.get("owner_type")),
                "acceptance_criteria": acceptance,
                "test_commands": test_commands,
            }
        )
    return contexts


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        if text := _optional_text(task.get(key)):
            commands.append(text)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        commands.extend(_strings(metadata.get("validation_commands")))
        commands.extend(_strings(metadata.get("validation_command")))
        commands.extend(_strings(metadata.get("test_commands")))
    return _dedupe(commands)


def _metadata_texts(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        texts: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            texts.extend(_metadata_texts(value[key]))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[str] = []
        for item in items:
            texts.extend(_metadata_texts(item))
        return texts
    text = _optional_text(value)
    return [text] if text else []


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(brief)


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | None) -> dict[str, Any] | None:
    if plan is None:
        return None
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _clean_risk(value: str) -> str | None:
    text = " ".join(value.strip(" \t\r\n-*.,;:").split())
    if len(_tokens(text)) < 2:
        return None
    return text


def _risk_key(value: str) -> str:
    tokens = [token for token in _tokens(value) if token not in _DUPLICATE_STOP_WORDS]
    return " ".join(tokens)


def _signal_tokens(value: str) -> set[str]:
    return {
        token
        for token in _tokens(value)
        if token not in _MATCH_STOP_WORDS and len(token) > 2
    }


def _tokens(value: str) -> set[str]:
    tokens = set(_TOKEN_RE.findall(value.lower()))
    expanded = set(tokens)
    for token in tokens:
        expanded.update(part for part in token.split("-") if part)
    return expanded


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return [text for item in items if (text := _optional_text(item))]
    return []


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _dedupe(values: list[_T] | tuple[_T, ...]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


_MATCH_STOP_WORDS = {
    "and",
    "are",
    "can",
    "for",
    "from",
    "into",
    "may",
    "not",
    "our",
    "the",
    "this",
    "that",
    "through",
    "with",
    "work",
}
_DUPLICATE_STOP_WORDS = _MATCH_STOP_WORDS | {"risk", "risks", "possible", "potential"}
_HIGH_SEVERITY_TOKENS = {
    "auth",
    "authentication",
    "billing",
    "breach",
    "compliance",
    "corruption",
    "credential",
    "credentials",
    "data-loss",
    "destructive",
    "leak",
    "migration",
    "outage",
    "payment",
    "permission",
    "privacy",
    "security",
}
_MEDIUM_SEVERITY_TOKENS = {
    "api",
    "dependency",
    "delay",
    "external",
    "integration",
    "latency",
    "performance",
    "queue",
    "rollout",
    "scope",
    "service",
    "timeout",
}
_SECURITY_TOKENS = {
    "auth",
    "authentication",
    "credential",
    "credentials",
    "oauth",
    "permission",
    "permissions",
    "privacy",
    "security",
    "token",
}
_DATA_TOKENS = {
    "backfill",
    "data",
    "database",
    "integrity",
    "migration",
    "schema",
    "sql",
}
_RELEASE_TOKENS = {
    "config",
    "configuration",
    "deploy",
    "deployment",
    "flag",
    "release",
    "rollout",
}
_INTEGRATION_TOKENS = {
    "api",
    "external",
    "integration",
    "service",
    "webhook",
}
_PRODUCT_TOKENS = {
    "scope",
    "user",
}
_DOMAIN_TOKENS = _SECURITY_TOKENS | _DATA_TOKENS | _RELEASE_TOKENS | _INTEGRATION_TOKENS
_RISK_AREA_TOKENS = _HIGH_SEVERITY_TOKENS | _MEDIUM_SEVERITY_TOKENS


__all__ = [
    "RiskMitigationRecord",
    "build_risk_mitigation_plan",
    "risk_mitigation_plan_to_dict",
]
