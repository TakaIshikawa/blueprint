"""Score validation flakiness risk for execution plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


FlakyValidationRiskLevel = Literal["low", "medium", "high"]
FlakyValidationRiskCategory = Literal[
    "network",
    "browser",
    "timing",
    "broad_command",
    "snapshot",
    "concurrency",
    "randomness",
    "current_time",
    "external_api",
    "shared_state",
]

_T = TypeVar("_T")

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_PATH_STRIP_RE = re.compile(r"^[`'\",;:(){}\[\]\s]+|[`'\",;:(){}\[\]\s.]+$")


@dataclass(frozen=True, slots=True)
class TaskFlakyValidationEvidence:
    """One detected flakiness signal for a task."""

    category: FlakyValidationRiskCategory
    source: str
    detail: str
    weight: int

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "category": self.category,
            "source": self.source,
            "detail": self.detail,
            "weight": self.weight,
        }


@dataclass(frozen=True, slots=True)
class TaskFlakyValidationRisk:
    """Flaky validation risk assessment for one execution task."""

    task_id: str
    title: str
    score: int
    risk_level: FlakyValidationRiskLevel
    evidence: tuple[TaskFlakyValidationEvidence, ...] = field(default_factory=tuple)
    mitigations: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "score": self.score,
            "risk_level": self.risk_level,
            "evidence": [item.to_dict() for item in self.evidence],
            "mitigations": list(self.mitigations),
        }


@dataclass(frozen=True, slots=True)
class TaskFlakyValidationRiskReport:
    """Plan-level flaky validation risk report."""

    plan_id: str
    tasks: tuple[TaskFlakyValidationRisk, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "plan_id": self.plan_id,
            "tasks": [task.to_dict() for task in self.tasks],
            "summary": dict(self.summary),
        }


def score_task_flaky_validation_risk(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> TaskFlakyValidationRiskReport:
    """Score validation flakiness risk for every task in an execution plan."""
    payload = _plan_payload(plan)
    tasks = tuple(_score_task(record) for record in _task_records(_task_payloads(payload.get("tasks"))))
    return TaskFlakyValidationRiskReport(
        plan_id=_optional_text(payload.get("id")) or "plan",
        tasks=tasks,
        summary=_summary(tasks),
    )


def task_flaky_validation_risk_to_dict(
    report: TaskFlakyValidationRiskReport,
) -> dict[str, Any]:
    """Serialize a flaky validation risk report to a plain dictionary."""
    return report.to_dict()


task_flaky_validation_risk_to_dict.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task_id: str
    title: str
    description: str
    acceptance_criteria: tuple[str, ...]
    validation_commands: tuple[str, ...]
    files_or_modules: tuple[str, ...]
    metadata: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _SignalRule:
    category: FlakyValidationRiskCategory
    source: str
    patterns: tuple[str, ...]
    detail: str
    weight: int


_TEXT_RULES: tuple[_SignalRule, ...] = (
    _SignalRule(
        "network",
        "acceptance_criteria",
        ("network", "http request", "websocket", "dns", "socket", "online"),
        "acceptance criteria mention network-dependent behavior",
        22,
    ),
    _SignalRule(
        "browser",
        "acceptance_criteria",
        ("browser", "playwright", "selenium", "cypress", "viewport", "screenshot", "visual regression"),
        "acceptance criteria mention browser or visual validation",
        25,
    ),
    _SignalRule(
        "timing",
        "acceptance_criteria",
        ("sleep", "timeout", "timer", "debounce", "poll", "eventually", "wait for"),
        "acceptance criteria mention waits, timers, or timeouts",
        20,
    ),
    _SignalRule(
        "snapshot",
        "acceptance_criteria",
        ("snapshot", "golden file", "approval test", "visual diff"),
        "acceptance criteria mention snapshot or golden-file comparisons",
        16,
    ),
    _SignalRule(
        "concurrency",
        "acceptance_criteria",
        ("race", "concurrent", "parallel", "thread", "async", "lock", "queue", "retry"),
        "acceptance criteria mention concurrency or race behavior",
        18,
    ),
    _SignalRule(
        "randomness",
        "acceptance_criteria",
        ("random", "uuid", "faker", "generated data", "property-based"),
        "acceptance criteria mention random or generated data",
        15,
    ),
    _SignalRule(
        "current_time",
        "acceptance_criteria",
        ("current time", "today", "now", "timezone", "clock", "date boundary"),
        "acceptance criteria mention current time or timezone behavior",
        18,
    ),
    _SignalRule(
        "external_api",
        "acceptance_criteria",
        ("stripe", "slack", "github api", "sendgrid", "twilio", "oauth", "webhook", "third-party"),
        "acceptance criteria mention external API or webhook behavior",
        24,
    ),
    _SignalRule(
        "shared_state",
        "acceptance_criteria",
        ("database", "redis", "cache", "filesystem", "shared state", "global state"),
        "acceptance criteria mention shared state",
        12,
    ),
)

_COMMAND_RULES: tuple[_SignalRule, ...] = (
    _SignalRule(
        "network",
        "test_command",
        ("--allow-net", "http://", "https://", "curl ", "wget ", "nc ", "netcat"),
        "test command can reach the network",
        24,
    ),
    _SignalRule(
        "browser",
        "test_command",
        ("playwright", "selenium", "cypress", "webdriver", "browser", "e2e"),
        "test command runs browser or end-to-end tests",
        28,
    ),
    _SignalRule(
        "timing",
        "test_command",
        ("sleep", "timeout", "--timeout", "wait", "flaky", "rerun"),
        "test command includes waits, timeouts, or flaky-test controls",
        22,
    ),
    _SignalRule(
        "snapshot",
        "test_command",
        ("snapshot", "snapshots", "--update-snapshots", "visual"),
        "test command runs snapshot or visual comparisons",
        18,
    ),
    _SignalRule(
        "concurrency",
        "test_command",
        ("pytest-xdist", "-n ", "--numprocesses", "parallel", "thread", "race"),
        "test command uses parallelism or targets concurrency behavior",
        18,
    ),
    _SignalRule(
        "randomness",
        "test_command",
        ("hypothesis", "faker", "random", "--seed"),
        "test command involves random or generated data",
        16,
    ),
    _SignalRule(
        "current_time",
        "test_command",
        ("freezegun", "time-machine", "timezone", "date"),
        "test command targets clock or timezone behavior",
        14,
    ),
)

_FILE_RULES: tuple[_SignalRule, ...] = (
    _SignalRule(
        "browser",
        "files_or_modules",
        ("/e2e/", "/playwright", "/cypress", ".spec.ts", ".e2e.", "browser"),
        "expected files include browser or end-to-end test paths",
        22,
    ),
    _SignalRule(
        "network",
        "files_or_modules",
        ("clients/", "integrations/", "webhooks", "http", "api_client"),
        "expected files include network or integration paths",
        16,
    ),
    _SignalRule(
        "snapshot",
        "files_or_modules",
        ("__snapshots__", ".snap", "snapshots/", "golden"),
        "expected files include snapshot fixtures",
        14,
    ),
    _SignalRule(
        "shared_state",
        "files_or_modules",
        ("db/", "database", "cache", "redis", "state", "tmp"),
        "expected files include shared state paths",
        10,
    ),
)

_MITIGATIONS: Mapping[FlakyValidationRiskCategory, tuple[str, ...]] = {
    "network": (
        "Mock network calls and block live outbound traffic during validation.",
        "Record deterministic fixtures for request and response payloads.",
    ),
    "browser": (
        "Pin browser, viewport, locale, timezone, and animation settings.",
        "Prefer targeted component checks before full browser validation.",
    ),
    "timing": (
        "Replace sleeps and polling with deterministic readiness hooks.",
        "Use fake timers or explicit timeout budgets in tests.",
    ),
    "broad_command": (
        "Narrow validation to the task's touched tests or modules.",
        "Run broad suites separately after targeted validation passes.",
    ),
    "snapshot": (
        "Stabilize snapshots by removing time, random, and environment-specific output.",
        "Review snapshot updates separately from behavior changes.",
    ),
    "concurrency": (
        "Isolate shared state and make concurrent assertions order-independent.",
        "Add deterministic synchronization around race-prone behavior.",
    ),
    "randomness": (
        "Seed random data generators and assert invariants instead of exact incidental values.",
    ),
    "current_time": (
        "Freeze time and set an explicit timezone for validation.",
    ),
    "external_api": (
        "Mock external APIs with contract fixtures and avoid sandbox dependencies in task validation.",
    ),
    "shared_state": (
        "Reset database, cache, filesystem, and global state before each validation run.",
    ),
}


def _score_task(record: _TaskRecord) -> TaskFlakyValidationRisk:
    evidence = _evidence(record)
    score = _score(evidence)
    return TaskFlakyValidationRisk(
        task_id=record.task_id,
        title=record.title,
        score=score,
        risk_level=_risk_level(score),
        evidence=evidence,
        mitigations=_mitigations_for(evidence),
    )


def _evidence(record: _TaskRecord) -> tuple[TaskFlakyValidationEvidence, ...]:
    candidates: list[TaskFlakyValidationEvidence] = []
    candidates.extend(_rule_evidence(_COMMAND_RULES, record.validation_commands))
    candidates.extend(_rule_evidence(_TEXT_RULES, record.acceptance_criteria))
    candidates.extend(_description_evidence(record.description))
    candidates.extend(_rule_evidence(_FILE_RULES, record.files_or_modules))
    candidates.extend(_metadata_evidence(record.metadata))
    candidates.extend(_broad_command_evidence(record.validation_commands))
    return tuple(_dedupe_evidence(candidates))


def _rule_evidence(
    rules: tuple[_SignalRule, ...],
    values: Iterable[str],
) -> list[TaskFlakyValidationEvidence]:
    haystack = " ".join(value.lower() for value in values)
    if not haystack:
        return []
    evidence: list[TaskFlakyValidationEvidence] = []
    for rule in rules:
        if any(pattern in haystack for pattern in rule.patterns):
            evidence.append(
                TaskFlakyValidationEvidence(
                    category=rule.category,
                    source=rule.source,
                    detail=rule.detail,
                    weight=rule.weight,
                )
            )
    return evidence


def _description_evidence(description: str) -> list[TaskFlakyValidationEvidence]:
    items = _rule_evidence(_TEXT_RULES, (description,))
    return [
        TaskFlakyValidationEvidence(
            category=item.category,
            source="description",
            detail=item.detail.replace("acceptance criteria", "description"),
            weight=max(item.weight - 3, 8),
        )
        for item in items
    ]


def _metadata_evidence(metadata: Mapping[str, Any]) -> list[TaskFlakyValidationEvidence]:
    values = {key: _strings(value) for key, value in metadata.items()}
    rendered = " ".join([str(key).lower() for key in metadata] + [item.lower() for items in values.values() for item in items])
    evidence: list[TaskFlakyValidationEvidence] = []
    metadata_rules = (
        ("network", ("network", "live_http", "allow_net"), "metadata references network-sensitive validation", 20),
        ("browser", ("browser", "playwright", "cypress", "e2e"), "metadata references browser validation", 22),
        ("timing", ("timeout", "sleep", "timer", "retry"), "metadata references timing-sensitive validation", 17),
        ("randomness", ("random", "seed", "faker"), "metadata references random data", 14),
        ("current_time", ("clock", "timezone", "freeze_time", "current_time"), "metadata references clock control", 15),
        ("external_api", ("external_api", "sandbox", "stripe", "slack", "webhook"), "metadata references external APIs", 22),
        ("shared_state", ("database", "cache", "state", "isolation"), "metadata references shared state", 12),
    )
    for category, needles, detail, weight in metadata_rules:
        if any(needle in rendered for needle in needles):
            evidence.append(
                TaskFlakyValidationEvidence(
                    category=category,
                    source="metadata",
                    detail=detail,
                    weight=weight,
                )
            )
    return evidence


def _broad_command_evidence(commands: Iterable[str]) -> list[TaskFlakyValidationEvidence]:
    evidence: list[TaskFlakyValidationEvidence] = []
    for command in commands:
        normalized = " ".join(command.lower().split())
        if _is_broad_command(normalized):
            evidence.append(
                TaskFlakyValidationEvidence(
                    category="broad_command",
                    source="test_command",
                    detail=f"test command is broad: {command}",
                    weight=30,
                )
            )
    return evidence


def _is_broad_command(command: str) -> bool:
    broad_exact = {
        "pytest",
        "poetry run pytest",
        "python -m pytest",
        "npm test",
        "npm run test",
        "pnpm test",
        "yarn test",
        "make test",
        "go test ./...",
        "cargo test",
    }
    if command in broad_exact:
        return True
    return bool(
        re.search(r"\bpytest\s+\.?(?:\s|$)", command)
        or re.search(r"\bpytest\s+tests/?(?:\s|$)", command)
        or re.search(r"\bvitest(?:\s+run)?\s*(?:\.|$)", command)
        or re.search(r"\bjest\s*(?:\.|$)", command)
    )


def _score(evidence: tuple[TaskFlakyValidationEvidence, ...]) -> int:
    if not evidence:
        return 5
    category_count = len({item.category for item in evidence})
    source_count = len({item.source for item in evidence})
    total = sum(item.weight for item in evidence)
    total += max(category_count - 1, 0) * 5
    total += max(source_count - 1, 0) * 3
    return min(100, max(1, total))


def _risk_level(score: int) -> FlakyValidationRiskLevel:
    if score >= 65:
        return "high"
    if score >= 30:
        return "medium"
    return "low"


def _mitigations_for(evidence: tuple[TaskFlakyValidationEvidence, ...]) -> tuple[str, ...]:
    categories = _dedupe(item.category for item in evidence)
    mitigations: list[str] = []
    for category in categories:
        mitigations.extend(_MITIGATIONS[category])
    if not mitigations:
        mitigations.append("Keep validation targeted and deterministic.")
    return tuple(_dedupe(mitigations))


def _summary(tasks: tuple[TaskFlakyValidationRisk, ...]) -> dict[str, Any]:
    risk_counts = {"low": 0, "medium": 0, "high": 0}
    category_counts: dict[str, int] = {}
    for task in tasks:
        risk_counts[task.risk_level] += 1
        for category in {item.category for item in task.evidence}:
            category_counts[category] = category_counts.get(category, 0) + 1
    return {
        "task_count": len(tasks),
        "risk_counts": risk_counts,
        "category_counts": dict(sorted(category_counts.items())),
    }


def _task_records(tasks: list[dict[str, Any]]) -> tuple[_TaskRecord, ...]:
    records: list[_TaskRecord] = []
    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        if task_id in seen_ids:
            continue
        seen_ids.add(task_id)
        metadata = task.get("metadata")
        records.append(
            _TaskRecord(
                task_id=task_id,
                title=_optional_text(task.get("title")) or task_id,
                description=_optional_text(task.get("description")) or "",
                acceptance_criteria=tuple(_strings(task.get("acceptance_criteria"))),
                validation_commands=tuple(
                    _dedupe(
                        [
                            *_strings(task.get("test_command")),
                            *_strings(task.get("validation_command")),
                            *_strings(task.get("validation_commands")),
                        ]
                    )
                ),
                files_or_modules=tuple(
                    _dedupe(
                        _normalized_path(path) for path in _strings(task.get("files_or_modules"))
                    )
                ),
                metadata=metadata if isinstance(metadata, Mapping) else {},
            )
        )
    return tuple(records)


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


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
    return _PATH_STRIP_RE.sub("", value).replace("\\", "/").strip("/")


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _dedupe_evidence(
    evidence: Iterable[TaskFlakyValidationEvidence],
) -> list[TaskFlakyValidationEvidence]:
    deduped: list[TaskFlakyValidationEvidence] = []
    seen: set[tuple[str, str, str]] = set()
    for item in evidence:
        key = (item.category, item.source, item.detail)
        if key in seen:
            continue
        deduped.append(item)
        seen.add(key)
    return deduped


__all__ = [
    "FlakyValidationRiskCategory",
    "FlakyValidationRiskLevel",
    "TaskFlakyValidationEvidence",
    "TaskFlakyValidationRisk",
    "TaskFlakyValidationRiskReport",
    "score_task_flaky_validation_risk",
    "task_flaky_validation_risk_to_dict",
]
