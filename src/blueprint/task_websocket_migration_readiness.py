"""Plan WebSocket migration readiness checks for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.validation_commands import flatten_validation_commands


WebSocketMigrationSignal = Literal[
    "protocol_upgrade",
    "fallback_strategy",
    "state_synchronization",
    "connection_lifecycle",
    "message_routing",
    "backward_compatibility",
]
WebSocketMigrationSafeguard = Literal[
    "upgrade_detection_tests",
    "fallback_mechanism_tests",
    "state_sync_tests",
    "connection_failure_tests",
    "migration_rollback_plan",
    "client_compatibility_matrix",
]
WebSocketMigrationReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[WebSocketMigrationReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[WebSocketMigrationSignal, ...] = (
    "protocol_upgrade",
    "fallback_strategy",
    "state_synchronization",
    "connection_lifecycle",
    "message_routing",
    "backward_compatibility",
)
_SAFEGUARD_ORDER: tuple[WebSocketMigrationSafeguard, ...] = (
    "upgrade_detection_tests",
    "fallback_mechanism_tests",
    "state_sync_tests",
    "connection_failure_tests",
    "migration_rollback_plan",
    "client_compatibility_matrix",
)
_SIGNAL_PATTERNS: dict[WebSocketMigrationSignal, re.Pattern[str]] = {
    "protocol_upgrade": re.compile(
        r"\b(?:protocol.{0,20}upgrade|upgrade.{0,20}(?:to.{0,20})?websocket|"
        r"http.{0,20}(?:to.{0,20})?(?:ws|websocket)|ws.{0,20}upgrade|"
        r"upgrade.{0,20}(?:connection|handshake)|101.{0,20}switching.{0,20}protocols?|"
        r"sec[- ]websocket[- ](?:key|accept|version))\b",
        re.I,
    ),
    "fallback_strategy": re.compile(
        r"\b(?:fallback.{0,40}(?:to.{0,20})?(?:http|polling|sse|long[- ]?poll)|"
        r"(?:http|polling|sse|long[- ]?poll).{0,40}fallback|"
        r"graceful.{0,20}degradation|progressive.{0,20}enhancement|"
        r"transport.{0,20}fallback|fallback.{0,20}(?:mechanism|strategy|transport))\b",
        re.I,
    ),
    "state_synchronization": re.compile(
        r"\b(?:state.{0,20}sync(?:hronization)?|sync.{0,20}state|"
        r"state.{0,20}reconciliation|reconcile.{0,20}state|"
        r"state.{0,20}consistency|consistent.{0,20}state|"
        r"session.{0,20}state|client.{0,20}state|shared.{0,20}state)\b",
        re.I,
    ),
    "connection_lifecycle": re.compile(
        r"\b(?:(?:websocket|ws|wss).{0,60}(?:connection|lifecycle|connect|disconnect|close|open)|"
        r"(?:connection|lifecycle).{0,60}(?:websocket|ws|wss)|"
        r"(?:on)?(?:open|close|error|message).{0,20}(?:event|handler|callback)|"
        r"connection.{0,20}(?:lifecycle|management|handling|close|cleanup))\b",
        re.I,
    ),
    "message_routing": re.compile(
        r"\b(?:message.{0,20}(?:routing|dispatch|broker|queue|channel)|"
        r"(?:route|dispatch|broker).{0,20}message|"
        r"event.{0,20}routing|pub[/-]?sub|topic.{0,20}subscription|"
        r"channel.{0,20}subscription|message.{0,20}handler)\b",
        re.I,
    ),
    "backward_compatibility": re.compile(
        r"\b(?:backward.{0,20}compatibility|backwards.{0,20}compatible|"
        r"legacy.{0,20}(?:client|support|api)|"
        r"(?:maintain|preserve|support).{0,40}(?:existing|legacy|old).{0,40}(?:client|api)|"
        r"version.{0,20}compatibility|compatibility.{0,20}layer|"
        r"gradual.{0,20}migration|phased.{0,20}migration)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[WebSocketMigrationSignal, re.Pattern[str]] = {
    "protocol_upgrade": re.compile(r"upgrade|websocket|ws", re.I),
    "fallback_strategy": re.compile(r"fallback|polling|sse|long[_-]?poll", re.I),
    "state_synchronization": re.compile(r"sync|state|reconcile", re.I),
    "connection_lifecycle": re.compile(r"connection|lifecycle|websocket", re.I),
    "message_routing": re.compile(r"routing|message|dispatch|channel", re.I),
    "backward_compatibility": re.compile(r"compatibility|legacy|migration", re.I),
}
_SAFEGUARD_PATTERNS: dict[WebSocketMigrationSafeguard, re.Pattern[str]] = {
    "upgrade_detection_tests": re.compile(
        r"\b(?:(?:upgrade|protocol).{0,80}(?:detect|test|coverage|scenario|validation)|"
        r"(?:test|coverage|scenario|validation).{0,80}(?:upgrade|protocol)|"
        r"test.{0,40}(?:websocket.{0,40})?upgrade|test.{0,40}protocol.{0,40}switching)\b",
        re.I,
    ),
    "fallback_mechanism_tests": re.compile(
        r"\b(?:(?:fallback|degradation).{0,80}(?:test|coverage|scenario|validation)|"
        r"(?:test|coverage|scenario|validation).{0,80}(?:fallback|degradation)|"
        r"test.{0,40}(?:graceful.{0,40})?(?:fallback|degradation)|"
        r"test.{0,40}transport.{0,40}fallback)\b",
        re.I,
    ),
    "state_sync_tests": re.compile(
        r"\b(?:(?:state|sync|reconciliation).{0,80}(?:test|coverage|scenario|validation)|"
        r"(?:test|coverage|scenario|validation).{0,80}(?:state.{0,40}sync|reconciliation)|"
        r"test.{0,40}state.{0,40}(?:sync|consistency|reconciliation))\b",
        re.I,
    ),
    "connection_failure_tests": re.compile(
        r"\b(?:(?:connection|network).{0,80}(?:failure|disconnect|drop|loss|outage|interruption).{0,80}(?:test|coverage|scenario)|"
        r"(?:test|coverage|scenario).{0,80}(?:connection|network).{0,80}(?:failure|disconnect|drop|loss)|"
        r"test.{0,40}(?:connection.{0,40})?(?:failure|disconnect|drop|loss|resilience)|"
        r"test.{0,40}reconnect(?:ion)?)\b",
        re.I,
    ),
    "migration_rollback_plan": re.compile(
        r"\b(?:migration.{0,40}(?:rollback|revert|back[- ]?out)|"
        r"rollback.{0,40}(?:plan|strategy|mechanism)|"
        r"revert.{0,40}migration|roll[- ]?back.{0,40}strategy|"
        r"contingency.{0,40}plan|abort.{0,40}migration)\b",
        re.I,
    ),
    "client_compatibility_matrix": re.compile(
        r"\b(?:(?:client|browser|device).{0,60}compatibility.{0,40}(?:matrix|table|list|support)|"
        r"compatibility.{0,40}(?:matrix|table|chart)|"
        r"supported.{0,40}(?:client|browser|device|version)|"
        r"browser.{0,40}support|client.{0,40}support.{0,40}matrix)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:websocket|migration|upgrade)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_REMEDIATIONS: dict[WebSocketMigrationSafeguard, str] = {
    "upgrade_detection_tests": "Add tests verifying protocol upgrade detection, handshake validation, and proper WebSocket connection establishment.",
    "fallback_mechanism_tests": "Add tests for graceful fallback to polling/SSE when WebSocket upgrade fails, ensuring no data loss during transport switching.",
    "state_sync_tests": "Add tests for state synchronization between HTTP and WebSocket sessions, including state reconciliation on connection changes.",
    "connection_failure_tests": "Add tests for connection failure scenarios including network drops, server restarts, and reconnection with state recovery.",
    "migration_rollback_plan": "Document migration rollback strategy with feature flags, version compatibility, and steps to revert to legacy transport.",
    "client_compatibility_matrix": "Create compatibility matrix documenting WebSocket support across target browsers, devices, and client library versions.",
}


@dataclass(frozen=True, slots=True)
class TaskWebSocketMigrationReadinessFinding:
    """WebSocket migration readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[WebSocketMigrationSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[WebSocketMigrationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[WebSocketMigrationSafeguard, ...] = field(default_factory=tuple)
    readiness: WebSocketMigrationReadiness = "partial"
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
class TaskWebSocketMigrationReadinessPlan:
    """Plan-level WebSocket migration readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskWebSocketMigrationReadinessFinding, ...] = field(default_factory=tuple)
    migration_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskWebSocketMigrationReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "migration_task_ids": list(self.migration_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return WebSocket migration readiness findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    def to_markdown(self) -> str:
        """Render WebSocket migration readiness guidance as deterministic Markdown."""
        title = "# Task WebSocket Migration Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Migration task count: {self.summary.get('migration_task_count', 0)}",
            f"- Not applicable task count: {self.summary.get('not_applicable_task_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{readiness} {readiness_counts.get(readiness, 0)}" for readiness in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER[:5]),
        ]
        if not self.findings:
            lines.extend(["", "No WebSocket migration readiness findings were identified."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Detected Signals | Present Safeguards | Missing Safeguards |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for finding in self.findings:
            lines.append(
                "| "
                f"`{_markdown_cell(finding.task_id)}` | "
                f"{_markdown_cell(finding.title)} | "
                f"{finding.readiness} | "
                f"{_markdown_cell(', '.join(finding.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(finding.missing_safeguards) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_websocket_migration_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskWebSocketMigrationReadinessPlan:
    """Build a WebSocket migration readiness review from an execution plan."""
    plan_id, tasks = _load_plan(plan)
    findings_list = [_analyze_task(task) for task in tasks]
    findings = tuple(finding for finding in findings_list if finding is not None)
    migration_ids = tuple(finding.task_id for finding in findings)
    not_applicable_ids = tuple(
        task["id"]
        for task in tasks
        if task.get("id") and task["id"] not in migration_ids
    )
    return TaskWebSocketMigrationReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        migration_task_ids=migration_ids,
        not_applicable_task_ids=not_applicable_ids,
        summary=_summary(findings, migration_ids, not_applicable_ids),
    )


def derive_task_websocket_migration_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskWebSocketMigrationReadinessPlan:
    """Compatibility helper for callers that use derive_* naming."""
    return build_task_websocket_migration_readiness_plan(plan)


def generate_task_websocket_migration_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskWebSocketMigrationReadinessPlan:
    """Compatibility helper for callers that use generate_* naming."""
    return build_task_websocket_migration_readiness_plan(plan)


def extract_task_websocket_migration_readiness_findings(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[TaskWebSocketMigrationReadinessFinding, ...]:
    """Return WebSocket migration readiness findings for applicable tasks."""
    return build_task_websocket_migration_readiness_plan(plan).findings


def summarize_task_websocket_migration_readiness(
    result: TaskWebSocketMigrationReadinessPlan | ExecutionPlan | Mapping[str, Any] | str | object,
) -> dict[str, Any]:
    """Return deterministic counts for WebSocket migration readiness review."""
    if isinstance(result, TaskWebSocketMigrationReadinessPlan):
        return dict(result.summary)
    return build_task_websocket_migration_readiness_plan(result).summary


def _load_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(plan, str):
        return None, []
    if isinstance(plan, ExecutionPlan):
        payload = dict(plan.model_dump(mode="python"))
        return payload.get("id"), payload.get("tasks", [])
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return payload.get("id"), payload.get("tasks", [])
    if isinstance(plan, Mapping):
        try:
            payload = dict(ExecutionPlan.model_validate(plan).model_dump(mode="python"))
            return payload.get("id"), payload.get("tasks", [])
        except (TypeError, ValueError, ValidationError):
            return plan.get("id"), plan.get("tasks", [])
    # Handle SimpleNamespace or other objects with tasks attribute
    if hasattr(plan, "tasks"):
        plan_id = getattr(plan, "id", None)
        tasks = getattr(plan, "tasks", [])
        # Convert tasks to dicts
        task_dicts = []
        for task in tasks:
            if isinstance(task, Mapping):
                task_dicts.append(dict(task))
            elif hasattr(task, "__dict__"):
                task_dicts.append(vars(task))
            else:
                task_dicts.append({})
        return plan_id, task_dicts
    return None, []


def _analyze_task(task: Mapping[str, Any]) -> TaskWebSocketMigrationReadinessFinding | None:
    task_id = task.get("id", "")
    title = task.get("title", "")

    if _NO_IMPACT_RE.search(_searchable_task_text(task)):
        return None

    detected_signals = _detect_signals(task)
    if not detected_signals:
        return None

    present_safeguards = _detect_safeguards(task)
    missing_safeguards: tuple[WebSocketMigrationSafeguard, ...] = tuple(
        safeguard
        for safeguard in _SAFEGUARD_ORDER
        if safeguard not in present_safeguards
    )

    readiness = _compute_readiness(detected_signals, present_safeguards, missing_safeguards)
    evidence = _collect_evidence(task, detected_signals)
    actionable_remediations = tuple(
        _REMEDIATIONS[safeguard] for safeguard in missing_safeguards
    )

    return TaskWebSocketMigrationReadinessFinding(
        task_id=task_id,
        title=title,
        detected_signals=detected_signals,
        present_safeguards=present_safeguards,
        missing_safeguards=missing_safeguards,
        readiness=readiness,
        evidence=evidence,
        actionable_remediations=actionable_remediations,
    )


def _detect_signals(task: Mapping[str, Any]) -> tuple[WebSocketMigrationSignal, ...]:
    searchable = _searchable_task_text(task)
    paths = _task_paths(task)
    validation_commands = _validation_commands(task)
    detected: list[WebSocketMigrationSignal] = []

    for signal in _SIGNAL_ORDER:
        if _SIGNAL_PATTERNS[signal].search(searchable):
            detected.append(signal)
        elif any(_PATH_SIGNAL_PATTERNS[signal].search(path) for path in paths):
            detected.append(signal)
        elif any(_SIGNAL_PATTERNS[signal].search(cmd) for cmd in validation_commands):
            detected.append(signal)

    return tuple(_dedupe(detected))


def _detect_safeguards(task: Mapping[str, Any]) -> tuple[WebSocketMigrationSafeguard, ...]:
    searchable = _searchable_task_text(task)
    validation_commands = _validation_commands(task)
    detected: list[WebSocketMigrationSafeguard] = []

    for safeguard in _SAFEGUARD_ORDER:
        if _SAFEGUARD_PATTERNS[safeguard].search(searchable):
            detected.append(safeguard)
        elif any(_SAFEGUARD_PATTERNS[safeguard].search(cmd) for cmd in validation_commands):
            detected.append(safeguard)

    return tuple(_dedupe(detected))


def _compute_readiness(
    signals: tuple[WebSocketMigrationSignal, ...],
    present: tuple[WebSocketMigrationSafeguard, ...],
    missing: tuple[WebSocketMigrationSafeguard, ...],
) -> WebSocketMigrationReadiness:
    if not signals:
        return "weak"

    # Strong readiness requires protocol_upgrade + fallback_strategy + tests
    signal_set = set(signals)
    safeguard_set = set(present)

    has_core_signals = "protocol_upgrade" in signal_set and "fallback_strategy" in signal_set
    has_critical_tests = (
        "upgrade_detection_tests" in safeguard_set
        and "fallback_mechanism_tests" in safeguard_set
    )

    if has_core_signals and has_critical_tests and len(missing) <= 2:
        return "strong"
    if len(present) >= len(missing):
        return "partial"
    return "weak"


def _collect_evidence(
    task: Mapping[str, Any],
    signals: tuple[WebSocketMigrationSignal, ...],
) -> tuple[str, ...]:
    evidence: list[str] = []
    searchable = _searchable_task_text(task)

    for signal in signals[:3]:
        pattern = _SIGNAL_PATTERNS[signal]
        match = pattern.search(searchable)
        if match:
            start = max(0, match.start() - 40)
            end = min(len(searchable), match.end() + 40)
            snippet = _clean_text(searchable[start:end])
            if len(snippet) > 100:
                snippet = f"{snippet[:97]}..."
            evidence.append(f"{signal}: ...{snippet}...")

    return tuple(evidence)


def _summary(
    findings: tuple[TaskWebSocketMigrationReadinessFinding, ...],
    migration_ids: tuple[str, ...],
    not_applicable_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "migration_task_count": len(migration_ids),
        "not_applicable_task_count": len(not_applicable_ids),
        "readiness_counts": {
            readiness: sum(1 for finding in findings if finding.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "safeguard_counts": {
            safeguard: sum(1 for finding in findings if safeguard in finding.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "overall_readiness": _overall_readiness(findings),
    }


def _overall_readiness(findings: tuple[TaskWebSocketMigrationReadinessFinding, ...]) -> WebSocketMigrationReadiness:
    if not findings:
        return "weak"
    readiness_values = [_READINESS_ORDER[finding.readiness] for finding in findings]
    avg = sum(readiness_values) / len(readiness_values)
    if avg >= 1.5:
        return "strong"
    if avg >= 0.5:
        return "partial"
    return "weak"


def _searchable_task_text(task: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for field in ("title", "body", "description", "prompt", "acceptance_criteria", "definition_of_done"):
        value = task.get(field)
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
    return " ".join(parts)


def _task_paths(task: Mapping[str, Any]) -> list[str]:
    paths: list[str] = []
    for field in ("expected_files", "output_files", "related_files", "files_or_modules", "files"):
        value = task.get(field)
        if isinstance(value, str):
            paths.append(value)
        elif isinstance(value, (list, tuple)):
            paths.extend(str(item) for item in value if item)
    return paths


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for field in ("validation_command", "validation_commands", "test_command", "test_commands"):
        value = task.get(field)
        if isinstance(value, str):
            commands.append(value)
        elif isinstance(value, (list, tuple)):
            commands.extend(str(item) for item in value if item)
        elif isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value))
    return commands


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    return _SPACE_RE.sub(" ", text).strip()


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


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
    "WebSocketMigrationReadiness",
    "WebSocketMigrationSafeguard",
    "WebSocketMigrationSignal",
    "TaskWebSocketMigrationReadinessFinding",
    "TaskWebSocketMigrationReadinessPlan",
    "build_task_websocket_migration_readiness_plan",
    "derive_task_websocket_migration_readiness_plan",
    "extract_task_websocket_migration_readiness_findings",
    "generate_task_websocket_migration_readiness_plan",
    "summarize_task_websocket_migration_readiness",
]
