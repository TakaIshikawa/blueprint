"""Plan DNS, domain, TLS, and traffic cutover readiness safeguards."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


DnsCutoverSignal = Literal[
    "dns_record_change",
    "ttl_management",
    "tls_certificate",
    "domain_verification",
    "traffic_shift",
    "rollback_record",
    "monitoring_probe",
    "owner_approval",
]
DnsCutoverSafeguard = Literal[
    "ttl_management",
    "tls_certificate",
    "domain_verification",
    "rollback_record",
    "monitoring_probe",
    "owner_approval",
]
DnsCutoverReadinessRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[DnsCutoverSignal, ...] = (
    "dns_record_change",
    "ttl_management",
    "tls_certificate",
    "domain_verification",
    "traffic_shift",
    "rollback_record",
    "monitoring_probe",
    "owner_approval",
)
_SAFEGUARD_ORDER: tuple[DnsCutoverSafeguard, ...] = (
    "ttl_management",
    "tls_certificate",
    "domain_verification",
    "rollback_record",
    "monitoring_probe",
    "owner_approval",
)
_RISK_ORDER: dict[DnsCutoverReadinessRisk, int] = {"high": 0, "medium": 1, "low": 2}
_CORE_CUTOVER_SIGNALS = {"dns_record_change", "tls_certificate", "domain_verification", "traffic_shift"}
_PATH_SIGNAL_PATTERNS: dict[DnsCutoverSignal, re.Pattern[str]] = {
    "dns_record_change": re.compile(
        r"(?:^|/)(?:dns|zone|zones|records?|route53|cloudflare)(?:/|$)|"
        r"(?:cname|a[_-]?record|aaaa[_-]?record|mx[_-]?record|txt[_-]?record|hosted[_-]?zone|dns)",
        re.I,
    ),
    "ttl_management": re.compile(r"(?:ttl|time[_-]?to[_-]?live)", re.I),
    "tls_certificate": re.compile(r"(?:tls|ssl|https|cert|certificate|acm|letsencrypt)", re.I),
    "domain_verification": re.compile(r"(?:domain[_-]?verification|txt[_-]?verification|spf|dkim|dmarc|ownership)", re.I),
    "traffic_shift": re.compile(r"(?:cutover|traffic[_-]?shift|traffic[_-]?switch|weighted[_-]?routing|blue[_-]?green|canary)", re.I),
    "rollback_record": re.compile(r"(?:rollback|backout|revert|previous[_-]?record|prior[_-]?record)", re.I),
    "monitoring_probe": re.compile(r"(?:probe|synthetic|health[_-]?check|smoke|dig|nslookup|curl|openssl)", re.I),
    "owner_approval": re.compile(r"(?:approval|signoff|sign[_-]?off|owner)", re.I),
}
_TEXT_SIGNAL_PATTERNS: dict[DnsCutoverSignal, re.Pattern[str]] = {
    "dns_record_change": re.compile(
        r"\b(?:dns record|dns change|dns update|cname|a record|aaaa record|mx record|txt record|"
        r"hosted zone|zone file|route ?53|cloudflare|nameserver|name server)\b",
        re.I,
    ),
    "ttl_management": re.compile(
        r"\b(?:ttl|time to live|lower ttl|lowered ttl|reduce ttl|reduced ttl|short ttl|"
        r"cache expiry|resolver cache)\b",
        re.I,
    ),
    "tls_certificate": re.compile(
        r"\b(?:tls|ssl|https|certificate|cert renewal|cert expiry|certificate expiry|x509|"
        r"acm|let'?s encrypt|san coverage|tls handshake)\b",
        re.I,
    ),
    "domain_verification": re.compile(
        r"\b(?:domain verification|domain ownership|verify domain|verified domain|validates? domain verification|"
        r"txt verification|site verification|spf|dkim|dmarc|acm validation|certificate validation)\b",
        re.I,
    ),
    "traffic_shift": re.compile(
        r"\b(?:traffic cutover|cut over|cutover|shift traffic|switch traffic|flip traffic|"
        r"route traffic|weighted routing|blue[- ]green|canary|production traffic)\b",
        re.I,
    ),
    "rollback_record": re.compile(
        r"\b(?:rollback record|rollback records|rollback plan|roll back|rollback|backout|back out|"
        r"revert|restore previous|records? rollback values?|previous cname|previous a record|previous dns|"
        r"prior dns|old record)\b",
        re.I,
    ),
    "monitoring_probe": re.compile(
        r"\b(?:monitoring probes?|synthetic probes?|synthetic monitoring probes?|synthetic checks?|"
        r"health checks?|smoke tests?|"
        r"dig|nslookup|curl|openssl s_client|resolver check|http probe|tls check)\b",
        re.I,
    ),
    "owner_approval": re.compile(
        r"\b(?:owner approval|domain owner|service owner|release owner|approved by|sign[- ]?off|"
        r"signoff|change approval|cab approval)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[DnsCutoverSafeguard, re.Pattern[str]] = {
    safeguard: _TEXT_SIGNAL_PATTERNS[safeguard] for safeguard in _SAFEGUARD_ORDER
}
_RECOMMENDED_STEPS: dict[DnsCutoverSafeguard, str] = {
    "ttl_management": "Lower TTLs before cutover and confirm resolver caches can expire inside the change window.",
    "tls_certificate": "Verify certificate validation, expiry, SAN coverage, renewal automation, and TLS handshake behavior.",
    "domain_verification": "Confirm required domain ownership or TXT verification records are present before production routing changes.",
    "rollback_record": "Record previous DNS, TLS, CDN, and routing values so rollback can be executed without rediscovery.",
    "monitoring_probe": "Define DNS, TLS, HTTP, and synthetic monitoring probes for the cutover and rollback window.",
    "owner_approval": "Attach domain, service, release, or change owner approval before executing the cutover.",
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:dns|domain|tls|ssl|certificate|traffic|cutover)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskDnsCutoverReadinessRecord:
    """Readiness guidance for one DNS, domain, TLS, or traffic cutover task."""

    task_id: str
    title: str
    detected_signals: tuple[DnsCutoverSignal, ...]
    present_safeguards: tuple[DnsCutoverSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[DnsCutoverSafeguard, ...] = field(default_factory=tuple)
    risk_level: DnsCutoverReadinessRisk = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_readiness_steps: tuple[str, ...] = field(default_factory=tuple)

    @property
    def readiness_level(self) -> DnsCutoverReadinessRisk:
        """Compatibility view for older callers that used readiness_level."""
        return self.risk_level

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_readiness_steps": list(self.recommended_readiness_steps),
        }


@dataclass(frozen=True, slots=True)
class TaskDnsCutoverReadinessPlan:
    """Plan-level DNS cutover readiness review."""

    plan_id: str | None = None
    records: tuple[TaskDnsCutoverReadinessRecord, ...] = field(default_factory=tuple)
    cutover_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def affected_task_ids(self) -> tuple[str, ...]:
        """Compatibility view for older callers."""
        return self.cutover_task_ids

    @property
    def no_signal_task_ids(self) -> tuple[str, ...]:
        """Compatibility view for older callers."""
        return self.not_applicable_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "cutover_task_ids": list(self.cutover_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render DNS cutover readiness as deterministic Markdown."""
        title = "# Task DNS Cutover Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Cutover task count: {self.summary.get('cutover_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No DNS cutover readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Signals | Present Safeguards | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_dns_cutover_readiness_plan(source: Any) -> TaskDnsCutoverReadinessPlan:
    """Build readiness records for DNS, domain, TLS, and traffic cutover tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    cutover_task_ids = tuple(record.task_id for record in records)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskDnsCutoverReadinessPlan(
        plan_id=plan_id,
        records=records,
        cutover_task_ids=cutover_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_dns_cutover_readiness(source: Any) -> TaskDnsCutoverReadinessPlan:
    """Compatibility alias for building DNS cutover readiness plans."""
    return build_task_dns_cutover_readiness_plan(source)


def summarize_task_dns_cutover_readiness(source: Any) -> TaskDnsCutoverReadinessPlan:
    """Compatibility alias for building DNS cutover readiness plans."""
    return build_task_dns_cutover_readiness_plan(source)


def extract_task_dns_cutover_readiness(source: Any) -> TaskDnsCutoverReadinessPlan:
    """Compatibility alias for extracting DNS cutover readiness plans."""
    return build_task_dns_cutover_readiness_plan(source)


def generate_task_dns_cutover_readiness(source: Any) -> TaskDnsCutoverReadinessPlan:
    """Compatibility alias for generating DNS cutover readiness plans."""
    return build_task_dns_cutover_readiness_plan(source)


def task_dns_cutover_readiness_plan_to_dict(result: TaskDnsCutoverReadinessPlan) -> dict[str, Any]:
    """Serialize a DNS cutover readiness plan to a plain dictionary."""
    return result.to_dict()


task_dns_cutover_readiness_plan_to_dict.__test__ = False


def task_dns_cutover_readiness_plan_to_dicts(
    result: TaskDnsCutoverReadinessPlan | Iterable[TaskDnsCutoverReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize DNS cutover readiness records to plain dictionaries."""
    if isinstance(result, TaskDnsCutoverReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_dns_cutover_readiness_plan_to_dicts.__test__ = False


def task_dns_cutover_readiness_plan_to_markdown(result: TaskDnsCutoverReadinessPlan) -> str:
    """Render a DNS cutover readiness plan as Markdown."""
    return result.to_markdown()


task_dns_cutover_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[DnsCutoverSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[DnsCutoverSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _task_record(task: Mapping[str, Any], index: int) -> TaskDnsCutoverReadinessRecord | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not (set(signals.signals) & _CORE_CUTOVER_SIGNALS):
        return None

    missing = _missing_safeguards(signals.signals, signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskDnsCutoverReadinessRecord(
        task_id=task_id,
        title=title,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(set(signals.signals), set(signals.present_safeguards), missing),
        evidence=signals.evidence,
        recommended_readiness_steps=tuple(_RECOMMENDED_STEPS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[DnsCutoverSignal] = set()
    safeguard_hits: set[DnsCutoverSafeguard] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        snippet = _evidence_snippet(source_field, text)
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(snippet)

    if "traffic_shift" in signal_hits and "dns_record_change" not in signal_hits:
        signal_hits.add("dns_record_change")
    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _missing_safeguards(
    signals: tuple[DnsCutoverSignal, ...],
    present_safeguards: tuple[DnsCutoverSafeguard, ...],
) -> tuple[DnsCutoverSafeguard, ...]:
    signal_set = set(signals)
    required: set[DnsCutoverSafeguard] = {"rollback_record", "monitoring_probe", "owner_approval"}
    if signal_set & {"dns_record_change", "traffic_shift"}:
        required.add("ttl_management")
    if "tls_certificate" in signal_set:
        required.add("tls_certificate")
    if "domain_verification" in signal_set:
        required.add("domain_verification")
    present = set(present_safeguards)
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required and safeguard not in present)


def _risk_level(
    signals: set[DnsCutoverSignal],
    present: set[DnsCutoverSafeguard],
    missing: tuple[DnsCutoverSafeguard, ...],
) -> DnsCutoverReadinessRisk:
    if not missing:
        return "low"
    missing_set = set(missing)
    if "traffic_shift" in signals and {"rollback_record", "monitoring_probe"} & missing_set:
        return "high"
    if "dns_record_change" in signals and {"ttl_management", "rollback_record"} <= missing_set:
        return "high"
    if "tls_certificate" in signals and {"tls_certificate", "monitoring_probe"} <= missing_set:
        return "high"
    if len(missing) >= 4:
        return "high"
    if present:
        return "medium"
    return "medium"


def _summary(
    records: tuple[TaskDnsCutoverReadinessRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "cutover_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {risk: sum(1 for record in records if record.risk_level == risk) for risk in _RISK_ORDER},
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
    return any(pattern.search(value) for pattern in [*_TEXT_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value))
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


DnsCutoverReadinessLevel = DnsCutoverReadinessRisk

__all__ = [
    "DnsCutoverReadinessLevel",
    "DnsCutoverReadinessRisk",
    "DnsCutoverSafeguard",
    "DnsCutoverSignal",
    "TaskDnsCutoverReadinessPlan",
    "TaskDnsCutoverReadinessRecord",
    "analyze_task_dns_cutover_readiness",
    "build_task_dns_cutover_readiness_plan",
    "extract_task_dns_cutover_readiness",
    "generate_task_dns_cutover_readiness",
    "summarize_task_dns_cutover_readiness",
    "task_dns_cutover_readiness_plan_to_dict",
    "task_dns_cutover_readiness_plan_to_dicts",
    "task_dns_cutover_readiness_plan_to_markdown",
]
