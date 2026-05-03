"""Assess task-level readiness for gRPC service implementation work."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


GRPCServiceSignal = Literal[
    "protobuf_definition",
    "service_method",
    "streaming_operation",
    "error_status",
    "interceptor_middleware",
    "service_discovery",
    "tls_mtls",
]
GRPCServiceSafeguard = Literal[
    "protobuf_compilation",
    "service_registration",
    "error_handling_strategy",
    "deadline_enforcement",
    "retry_backoff_policy",
    "streaming_management",
    "health_checking",
]
GRPCServiceReadiness = Literal["weak", "partial", "strong"]
GRPCServiceImpact = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[GRPCServiceReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_IMPACT_ORDER: dict[GRPCServiceImpact, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: tuple[GRPCServiceSignal, ...] = (
    "protobuf_definition",
    "service_method",
    "streaming_operation",
    "error_status",
    "interceptor_middleware",
    "service_discovery",
    "tls_mtls",
)
_SAFEGUARD_ORDER: tuple[GRPCServiceSafeguard, ...] = (
    "protobuf_compilation",
    "service_registration",
    "error_handling_strategy",
    "deadline_enforcement",
    "retry_backoff_policy",
    "streaming_management",
    "health_checking",
)
_HIGH_IMPACT_SIGNALS: frozenset[GRPCServiceSignal] = frozenset(
    {"protobuf_definition", "streaming_operation", "service_discovery", "tls_mtls"}
)

_SIGNAL_PATTERNS: dict[GRPCServiceSignal, re.Pattern[str]] = {
    "protobuf_definition": re.compile(
        r"\b(?:protobuf|proto|\.proto|protocol buffer|protobuf schema|"
        r"proto file|proto definition|protobuf message|message definition|"
        r"proto3|proto2|protoc|proto compilation)\b",
        re.I,
    ),
    "service_method": re.compile(
        r"\b(?:grpc service|service method|rpc method|rpc definition|"
        r"service definition|grpc method|grpc endpoint|rpc endpoint|"
        r"service implementation|method implementation|rpc call)\b",
        re.I,
    ),
    "streaming_operation": re.compile(
        r"\b(?:stream(?:ing)?|server stream(?:ing)?|client stream(?:ing)?|"
        r"bidirectional stream(?:ing)?|bi[- ]?directional|duplex stream|"
        r"stream rpc|streaming rpc|unary stream)\b",
        re.I,
    ),
    "error_status": re.compile(
        r"\b(?:grpc status|status code|grpc error|error code|error status|"
        r"status\.(?:ok|cancelled|unknown|invalid_argument|deadline_exceeded)|"
        r"grpc\.status|status handling|error handling|grpc exception)\b",
        re.I,
    ),
    "interceptor_middleware": re.compile(
        r"\b(?:interceptor|grpc interceptor|middleware|server interceptor|"
        r"client interceptor|unary interceptor|stream interceptor|"
        r"interceptor chain|middleware chain|request interceptor|response interceptor)\b",
        re.I,
    ),
    "service_discovery": re.compile(
        r"\b(?:service discovery|service registration|service registry|"
        r"consul|etcd|zookeeper|eureka|name resolution|dns resolution|"
        r"load balanc(?:er|ing)|service mesh|envoy|istio)\b",
        re.I,
    ),
    "tls_mtls": re.compile(
        r"\b(?:tls|ssl|mtls|mutual tls|certificate|cert|x\.?509|"
        r"server certificate|client certificate|certificate authority|ca cert|"
        r"secure channel|encrypted connection|grpc security)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[GRPCServiceSignal, re.Pattern[str]] = {
    "protobuf_definition": re.compile(r"proto|protobuf|pb|\.proto$|message", re.I),
    "service_method": re.compile(r"service|grpc|rpc|method|endpoint", re.I),
    "streaming_operation": re.compile(r"stream|streaming|duplex|bidirectional", re.I),
    "error_status": re.compile(r"error|status|exception|handler", re.I),
    "interceptor_middleware": re.compile(r"interceptor|middleware|filter|handler", re.I),
    "service_discovery": re.compile(r"discovery|registry|consul|etcd|mesh|envoy", re.I),
    "tls_mtls": re.compile(r"tls|ssl|mtls|cert|certificate|security|auth", re.I),
}
_SAFEGUARD_PATTERNS: dict[GRPCServiceSafeguard, re.Pattern[str]] = {
    "protobuf_compilation": re.compile(
        r"\b(?:protobuf compilation|proto compilation|protoc|compile proto|"
        r"proto generate|code generation|protobuf versioning|proto version|"
        r"backward compatibility|forward compatibility|breaking change)\b",
        re.I,
    ),
    "service_registration": re.compile(
        r"\b(?:service registration|register service|service registry setup|"
        r"discovery setup|registration config|registry config|service catalog|"
        r"register endpoint|deregister|health registration)\b",
        re.I,
    ),
    "error_handling_strategy": re.compile(
        r"\b(?:error handling strategy|error strategy|status code handling|"
        r"error propagation|error mapping|error translation|error response|"
        r"graceful degradation|fallback|circuit breaker|error recovery)\b",
        re.I,
    ),
    "deadline_enforcement": re.compile(
        r"\b(?:deadline enforcement|deadline|timeout|context deadline|"
        r"request timeout|rpc timeout|deadline propagation|deadline exceeded|"
        r"timeout config(?:uration)?|timeout policy|timeout handling)\b",
        re.I,
    ),
    "retry_backoff_policy": re.compile(
        r"\b(?:retry policy|backoff policy|retry strategy|exponential backoff|"
        r"retry config(?:uration)?|retry attempt|max retry|retry limit|"
        r"backoff strategy|jitter|retry logic|idempotent retry)\b",
        re.I,
    ),
    "streaming_management": re.compile(
        r"\b(?:streaming management|stream management|connection management|"
        r"stream lifecycle|stream control|flow control|backpressure|"
        r"stream error handling|stream cancellation|stream timeout)\b",
        re.I,
    ),
    "health_checking": re.compile(
        r"\b(?:health check(?:ing)?|health service|health endpoint|"
        r"grpc health|health status|liveness|readiness|health probe|"
        r"health monitoring|service health|health protocol)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[GRPCServiceSafeguard, str] = {
    "protobuf_compilation": "Implement protobuf compilation with version control and backward/forward compatibility checks to prevent breaking changes in schema evolution.",
    "service_registration": "Configure service registration and discovery setup to enable dynamic service location and load balancing across service instances.",
    "error_handling_strategy": "Define comprehensive error handling strategy with proper gRPC status codes, error propagation, and graceful degradation patterns.",
    "deadline_enforcement": "Enforce request deadlines and timeouts with proper context propagation to prevent resource exhaustion from long-running requests.",
    "retry_backoff_policy": "Implement retry policies with exponential backoff and jitter to handle transient failures without overwhelming downstream services.",
    "streaming_management": "Add streaming connection management with flow control, backpressure handling, and proper stream lifecycle management.",
    "health_checking": "Implement gRPC health checking protocol with liveness and readiness probes for service monitoring and orchestration integration.",
}


@dataclass(frozen=True, slots=True)
class TaskGRPCServiceReadinessRecord:
    """gRPC service readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[GRPCServiceSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[GRPCServiceSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[GRPCServiceSafeguard, ...] = field(default_factory=tuple)
    readiness: GRPCServiceReadiness = "weak"
    impact: GRPCServiceImpact = "medium"
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def signals(self) -> tuple[GRPCServiceSignal, ...]:
        return self.detected_signals

    @property
    def safeguards(self) -> tuple[GRPCServiceSafeguard, ...]:
        return self.present_safeguards

    @property
    def recommendations(self) -> tuple[str, ...]:
        return self.recommended_checks

    @property
    def recommended_actions(self) -> tuple[str, ...]:
        return self.recommended_checks

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "impact": self.impact,
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskGRPCServiceReadinessPlan:
    """Plan-level gRPC service readiness review."""

    plan_id: str | None = None
    records: tuple[TaskGRPCServiceReadinessRecord, ...] = field(default_factory=tuple)
    grpc_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskGRPCServiceReadinessRecord, ...]:
        return self.records

    @property
    def recommendations(self) -> tuple[TaskGRPCServiceReadinessRecord, ...]:
        return self.records

    @property
    def impacted_task_ids(self) -> tuple[str, ...]:
        return self.grpc_task_ids

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "grpc_task_ids": list(self.grpc_task_ids),
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        title = "# Task gRPC Service Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        impact_counts = self.summary.get("impact_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- gRPC task count: {self.summary.get('grpc_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            "- Impact counts: " + ", ".join(f"{level} {impact_counts.get(level, 0)}" for level in _IMPACT_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task gRPC service readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Impact | Detected Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{record.impact} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_grpc_service_readiness_plan(source: Any) -> TaskGRPCServiceReadinessPlan:
    """Build gRPC service readiness records for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _task_record(task, index)) is not None
            ),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                _IMPACT_ORDER[record.impact],
                -len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    grpc_task_ids = tuple(record.task_id for record in records)
    grpc_task_id_set = set(grpc_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in grpc_task_id_set
    )
    return TaskGRPCServiceReadinessPlan(
        plan_id=plan_id,
        records=records,
        grpc_task_ids=grpc_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_grpc_service_readiness(source: Any) -> TaskGRPCServiceReadinessPlan:
    return build_task_grpc_service_readiness_plan(source)


def recommend_task_grpc_service_readiness(source: Any) -> TaskGRPCServiceReadinessPlan:
    return build_task_grpc_service_readiness_plan(source)


def summarize_task_grpc_service_readiness(source: Any) -> TaskGRPCServiceReadinessPlan:
    return build_task_grpc_service_readiness_plan(source)


def generate_task_grpc_service_readiness(source: Any) -> TaskGRPCServiceReadinessPlan:
    return build_task_grpc_service_readiness_plan(source)


def extract_task_grpc_service_readiness(source: Any) -> TaskGRPCServiceReadinessPlan:
    return build_task_grpc_service_readiness_plan(source)


def derive_task_grpc_service_readiness(source: Any) -> TaskGRPCServiceReadinessPlan:
    return build_task_grpc_service_readiness_plan(source)


def task_grpc_service_readiness_plan_to_dict(result: TaskGRPCServiceReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_grpc_service_readiness_plan_to_dict.__test__ = False


def task_grpc_service_readiness_plan_to_dicts(
    result: TaskGRPCServiceReadinessPlan | Iterable[TaskGRPCServiceReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, TaskGRPCServiceReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_grpc_service_readiness_plan_to_dicts.__test__ = False
task_grpc_service_readiness_to_dicts = task_grpc_service_readiness_plan_to_dicts
task_grpc_service_readiness_to_dicts.__test__ = False


def task_grpc_service_readiness_plan_to_markdown(result: TaskGRPCServiceReadinessPlan) -> str:
    return result.to_markdown()


task_grpc_service_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[GRPCServiceSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[GRPCServiceSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskGRPCServiceReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    missing: tuple[GRPCServiceSafeguard, ...] = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    return TaskGRPCServiceReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        readiness=_readiness(signals.present_safeguards, missing),
        impact=_impact(signals.signals, missing),
        recommended_checks=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[GRPCServiceSignal] = set()
    safeguard_hits: set[GRPCServiceSafeguard] = set()
    evidence: list[str] = []

    for path in _strings(
        task.get("files_or_modules")
        or task.get("files")
        or task.get("expected_file_paths")
        or task.get("expected_files")
        or task.get("paths")
    ):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
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

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
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
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        evidence=tuple(_dedupe(evidence)),
    )


def _readiness(
    present: tuple[GRPCServiceSafeguard, ...],
    missing: tuple[GRPCServiceSafeguard, ...],
) -> GRPCServiceReadiness:
    if not missing:
        return "strong"
    if len(present) >= 3:
        return "partial"
    return "weak"


def _impact(
    signals: tuple[GRPCServiceSignal, ...],
    missing: tuple[GRPCServiceSafeguard, ...],
) -> GRPCServiceImpact:
    high_signal = any(signal in _HIGH_IMPACT_SIGNALS for signal in signals)
    if high_signal and (
        len(missing) >= 3
        or "protobuf_compilation" in missing
        or "streaming_management" in missing and "streaming_operation" in signals
    ):
        return "high"
    if high_signal or len(missing) >= 3:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskGRPCServiceReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "grpc_task_count": len(records),
        "grpc_task_ids": [record.task_id for record in records],
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_count": len(no_impact_task_ids),
        "no_impact_task_ids": list(no_impact_task_ids),
        "signal_count": sum(len(record.detected_signals) for record in records),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "impact_counts": {
            impact: sum(1 for record in records if record.impact == impact)
            for impact in _IMPACT_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
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
        "expected_file_paths",
        "expected_files",
        "paths",
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
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
    ):
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


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value) or _strings(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value) or _strings(metadata_value))
            else:
                commands.extend(_strings(metadata_value))
    return tuple(("validation_commands", command) for command in _dedupe(commands))


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
    path = value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
    return str(PurePosixPath(path)) if path else ""


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
    "GRPCServiceImpact",
    "GRPCServiceReadiness",
    "GRPCServiceSafeguard",
    "GRPCServiceSignal",
    "TaskGRPCServiceReadinessPlan",
    "TaskGRPCServiceReadinessRecord",
    "analyze_task_grpc_service_readiness",
    "build_task_grpc_service_readiness_plan",
    "derive_task_grpc_service_readiness",
    "extract_task_grpc_service_readiness",
    "generate_task_grpc_service_readiness",
    "recommend_task_grpc_service_readiness",
    "summarize_task_grpc_service_readiness",
    "task_grpc_service_readiness_plan_to_dict",
    "task_grpc_service_readiness_plan_to_dicts",
    "task_grpc_service_readiness_plan_to_markdown",
    "task_grpc_service_readiness_to_dicts",
]
