"""Plan DNS, certificate, CDN, and traffic cutover readiness safeguards."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


DnsCutoverSignal = Literal[
    "dns",
    "cname",
    "a_record",
    "mx",
    "txt",
    "domain",
    "subdomain",
    "certificate",
    "tls",
    "ssl",
    "cdn",
    "cloudflare",
    "route53",
    "propagation",
    "ttl",
    "traffic_cutover",
    "rollback",
]
DnsCutoverSafeguard = Literal[
    "ttl_lowering",
    "staged_validation",
    "certificate_renewal_checks",
    "propagation_window",
    "rollback_records",
    "owner_approval",
]
DnsCutoverReadinessLevel = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: dict[DnsCutoverSignal, int] = {
    "dns": 0,
    "cname": 1,
    "a_record": 2,
    "mx": 3,
    "txt": 4,
    "domain": 5,
    "subdomain": 6,
    "certificate": 7,
    "tls": 8,
    "ssl": 9,
    "cdn": 10,
    "cloudflare": 11,
    "route53": 12,
    "propagation": 13,
    "ttl": 14,
    "traffic_cutover": 15,
    "rollback": 16,
}
_SAFEGUARD_ORDER: tuple[DnsCutoverSafeguard, ...] = (
    "ttl_lowering",
    "staged_validation",
    "certificate_renewal_checks",
    "propagation_window",
    "rollback_records",
    "owner_approval",
)
_READINESS_ORDER: dict[DnsCutoverReadinessLevel, int] = {"weak": 0, "partial": 1, "strong": 2}
_DNS_LIKE_SIGNALS = {
    "dns",
    "cname",
    "a_record",
    "mx",
    "txt",
    "domain",
    "subdomain",
    "cdn",
    "cloudflare",
    "route53",
    "ttl",
    "traffic_cutover",
}
_CERTIFICATE_SIGNALS = {"certificate", "tls", "ssl"}

_TEXT_SIGNAL_PATTERNS: dict[DnsCutoverSignal, re.Pattern[str]] = {
    "dns": re.compile(r"\b(?:dns|nameservers?|name servers?|hosted zone|zone file|dns record)\b", re.I),
    "cname": re.compile(r"\b(?:cname|canonical name)\b", re.I),
    "a_record": re.compile(r"\b(?:a record|aaaa record|address record|apex record|root record)\b", re.I),
    "mx": re.compile(r"\b(?:mx|mail exchanger|mail routing|email routing)\b", re.I),
    "txt": re.compile(r"\b(?:txt record|spf|dkim|dmarc|domain verification|site verification)\b", re.I),
    "domain": re.compile(r"\b(?:domain|custom domain|hostname|host name|registrar|zone apex)\b", re.I),
    "subdomain": re.compile(r"\b(?:subdomain|sub-domain|www\.|api\.|app\.)\b", re.I),
    "certificate": re.compile(r"\b(?:certificates?|cert renewal|cert expiry|x509|acm|let'?s encrypt)\b", re.I),
    "tls": re.compile(r"\b(?:tls|tls handshake|tls termination|https)\b", re.I),
    "ssl": re.compile(r"\b(?:ssl|ssl certificate)\b", re.I),
    "cdn": re.compile(r"\b(?:cdn|edge cache|edge distribution|fastly|cloudfront|akamai)\b", re.I),
    "cloudflare": re.compile(r"\bcloudflare\b", re.I),
    "route53": re.compile(r"\b(?:route ?53|route53|aws hosted zone)\b", re.I),
    "propagation": re.compile(r"\b(?:propagat(?:e|es|ion)|dns cache|resolver cache|global resolvers?)\b", re.I),
    "ttl": re.compile(r"\b(?:ttl|time to live)\b", re.I),
    "traffic_cutover": re.compile(
        r"\b(?:traffic cutover|cut over|cutover|flip traffic|switch traffic|route traffic|"
        r"migrate traffic|weighted routing|blue[- ]green|canary)\b",
        re.I,
    ),
    "rollback": re.compile(r"\b(?:rollback|roll back|backout|back out|revert|restore previous)\b", re.I),
}
_SAFEGUARD_PATTERNS: dict[DnsCutoverSafeguard, re.Pattern[str]] = {
    "ttl_lowering": re.compile(
        r"\b(?:lower(?:ed|ing)? ttl|reduce(?:d|ing)? ttl|short ttl|ttl (?:to )?(?:60|120|300|600)|"
        r"time to live (?:to )?(?:60|120|300|600))\b",
        re.I,
    ),
    "staged_validation": re.compile(
        r"\b(?:staged validation|validate(?:d|s)?|preflight|smoke tests?|canary|dry run|"
        r"dig\b|nslookup\b|curl\b|openssl s_client|health checks?|synthetic checks?)\b",
        re.I,
    ),
    "certificate_renewal_checks": re.compile(
        r"\b(?:cert(?:ificate)? renewal|certificate expiry|cert expiry|expiration check|expires?|"
        r"validity window|renewal check|acm validation|let'?s encrypt renewal|tls renewal)\b",
        re.I,
    ),
    "propagation_window": re.compile(
        r"\b(?:propagation window|propagation wait|wait for propagation|dns propagation|"
        r"resolver propagation|cache expiry|ttl window|monitor propagation)\b",
        re.I,
    ),
    "rollback_records": re.compile(
        r"\b(?:rollback records?|roll back records?|previous records?|restore records?|"
        r"backout records?|revert dns|old cname|old a record|prior dns)\b",
        re.I,
    ),
    "owner_approval": re.compile(
        r"\b(?:owner approval|domain owner|service owner|release owner|approval required|"
        r"approved by|sign[- ]off|signoff|change approval|cab approval)\b",
        re.I,
    ),
}
_SAFEGUARD_ACCEPTANCE_CRITERIA: dict[DnsCutoverSafeguard, str] = {
    "ttl_lowering": "Lower DNS TTLs ahead of the cutover and confirm caches can expire inside the change window.",
    "staged_validation": "Run staged DNS, certificate, CDN, and HTTP validation before moving production traffic.",
    "certificate_renewal_checks": "Check certificate validation, expiry, renewal automation, SAN coverage, and TLS handshake behavior.",
    "propagation_window": "Reserve a propagation window and monitor authoritative and public resolvers until records converge.",
    "rollback_records": "Record previous DNS, CDN, and traffic-routing values so rollback can be executed without rediscovery.",
    "owner_approval": "Capture domain, service, or release owner approval before executing the cutover.",
}


@dataclass(frozen=True, slots=True)
class TaskDnsCutoverReadinessRecord:
    """Readiness guidance for one DNS, certificate, CDN, or traffic cutover task."""

    task_id: str
    title: str
    readiness_level: DnsCutoverReadinessLevel
    detected_signals: tuple[DnsCutoverSignal, ...]
    present_safeguards: tuple[DnsCutoverSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[DnsCutoverSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "readiness_level": self.readiness_level,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "evidence": list(self.evidence),
            "recommended_acceptance_criteria": list(self.recommended_acceptance_criteria),
        }


@dataclass(frozen=True, slots=True)
class TaskDnsCutoverReadinessPlan:
    """Plan-level DNS cutover readiness review."""

    plan_id: str | None = None
    records: tuple[TaskDnsCutoverReadinessRecord, ...] = field(default_factory=tuple)
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "affected_task_ids": list(self.affected_task_ids),
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the readiness plan as deterministic Markdown."""
        title = "# Task DNS Cutover Readiness Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Affected task count: {self.summary.get('affected_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No DNS, certificate, CDN, or traffic cutover tasks were detected."])
            if self.no_signal_task_ids:
                lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Records",
                "",
                "| Task | Title | Readiness | Signals | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell('; '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell('; '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_signal_task_ids:
            lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
        return "\n".join(lines)


def build_task_dns_cutover_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDnsCutoverReadinessPlan:
    """Build readiness records for DNS, certificate, CDN, and traffic cutover tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    no_signal_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskDnsCutoverReadinessPlan(
        plan_id=plan_id,
        records=records,
        affected_task_ids=tuple(record.task_id for record in records),
        no_signal_task_ids=no_signal_task_ids,
        summary=_summary(records, task_count=len(tasks), no_signal_task_count=len(no_signal_task_ids)),
    )


def analyze_task_dns_cutover_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDnsCutoverReadinessPlan:
    """Compatibility alias for building DNS cutover readiness plans."""
    return build_task_dns_cutover_readiness_plan(source)


def summarize_task_dns_cutover_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskDnsCutoverReadinessPlan:
    """Compatibility alias for building DNS cutover readiness plans."""
    return build_task_dns_cutover_readiness_plan(source)


def task_dns_cutover_readiness_plan_to_dict(
    result: TaskDnsCutoverReadinessPlan,
) -> dict[str, Any]:
    """Serialize a DNS cutover readiness plan to a plain dictionary."""
    return result.to_dict()


task_dns_cutover_readiness_plan_to_dict.__test__ = False


def task_dns_cutover_readiness_plan_to_markdown(
    result: TaskDnsCutoverReadinessPlan,
) -> str:
    """Render a DNS cutover readiness plan as Markdown."""
    return result.to_markdown()


task_dns_cutover_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[DnsCutoverSignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[DnsCutoverSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)
    validation_commands: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskDnsCutoverReadinessRecord | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals.signals:
        return None

    missing_safeguards = _missing_safeguards(signals.signals, signals.present_safeguards)
    readiness = _readiness_level(
        signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing_safeguards,
    )
    return TaskDnsCutoverReadinessRecord(
        task_id=task_id,
        title=title,
        readiness_level=readiness,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing_safeguards,
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
        recommended_acceptance_criteria=tuple(
            _SAFEGUARD_ACCEPTANCE_CRITERIA[safeguard] for safeguard in missing_safeguards
        ),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[DnsCutoverSignal] = set()
    safeguard_hits: set[DnsCutoverSafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_signals = _path_signals(normalized)
        if path_signals:
            signal_hits.update(path_signals)
            signal_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signal_hits.add(signal)
                signal_evidence.append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    validation_commands = tuple(_validation_commands(task))
    for command in validation_commands:
        snippet = _evidence_snippet("validation_commands", command)
        searchable = command.replace("/", " ").replace("_", " ").replace("-", " ")
        signal_added = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(command) or pattern.search(searchable):
                signal_hits.add(signal)
                signal_added = True
        if signal_added:
            signal_evidence.append(snippet)
        if signal_added or re.search(r"\b(?:dig|nslookup|curl|openssl|pytest|smoke)\b", command, re.I):
            safeguard_hits.add("staged_validation")
            safeguard_evidence.append(snippet)

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
        validation_commands=validation_commands,
    )


def _path_signals(path: str) -> set[DnsCutoverSignal]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals: set[DnsCutoverSignal] = set()
    if {"dns", "zones", "zone", "hosted-zones", "hosted_zones"} & parts:
        signals.add("dns")
    if "cname" in text:
        signals.add("cname")
    if any(token in text for token in ("a record", "aaaa record", "arecord", "apex")):
        signals.add("a_record")
    if "mx" in parts or "mx record" in text:
        signals.add("mx")
    if any(token in text for token in ("txt record", "spf", "dkim", "dmarc")):
        signals.add("txt")
    if any(token in text for token in ("domain", "hostname", "hosted zone")):
        signals.add("domain")
    if "subdomain" in text:
        signals.add("subdomain")
    if any(token in text for token in ("certificate", "cert", "acm", "letsencrypt")):
        signals.add("certificate")
    if any(token in text for token in ("tls", "https")):
        signals.add("tls")
    if "ssl" in text:
        signals.add("ssl")
    if any(token in text for token in ("cdn", "cloudfront", "fastly", "akamai")):
        signals.add("cdn")
    if "cloudflare" in text:
        signals.add("cloudflare")
    if "route53" in text or "route 53" in text:
        signals.add("route53")
    if "propagation" in text:
        signals.add("propagation")
    if "ttl" in text:
        signals.add("ttl")
    if any(token in text for token in ("cutover", "traffic switch", "weighted routing")):
        signals.add("traffic_cutover")
    if any(token in text for token in ("rollback", "backout", "revert")):
        signals.add("rollback")
    if name in {"zone.tf", "dns.tf", "records.tf", "cloudflare.tf", "route53.tf"}:
        signals.add("dns")
    return signals


def _missing_safeguards(
    signals: tuple[DnsCutoverSignal, ...],
    present_safeguards: tuple[DnsCutoverSafeguard, ...],
) -> tuple[DnsCutoverSafeguard, ...]:
    required: set[DnsCutoverSafeguard] = {
        "staged_validation",
        "propagation_window",
        "rollback_records",
        "owner_approval",
    }
    signal_set = set(signals)
    if signal_set & _DNS_LIKE_SIGNALS:
        required.add("ttl_lowering")
    if signal_set & _CERTIFICATE_SIGNALS:
        required.add("certificate_renewal_checks")
    present = set(present_safeguards)
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required and safeguard not in present)


def _readiness_level(
    signals: tuple[DnsCutoverSignal, ...],
    *,
    present_safeguards: tuple[DnsCutoverSafeguard, ...],
    missing_safeguards: tuple[DnsCutoverSafeguard, ...],
) -> DnsCutoverReadinessLevel:
    present = set(present_safeguards)
    has_validation = "staged_validation" in present
    has_propagation = "propagation_window" in present or "propagation" in signals
    has_rollback = "rollback_records" in present or "rollback" in signals
    if not missing_safeguards and has_validation and has_propagation and has_rollback:
        return "strong"
    if len(present) >= 2 or {"staged_validation", "rollback_records"} <= present:
        return "partial"
    return "weak"


def _summary(
    records: tuple[TaskDnsCutoverReadinessRecord, ...],
    *,
    task_count: int,
    no_signal_task_count: int,
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "affected_task_count": len(records),
        "no_signal_task_count": no_signal_task_count,
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            level: sum(1 for record in records if record.readiness_level == level)
            for level in _READINESS_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
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
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
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
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
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
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


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
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "test_commands",
        "validation_command",
        "validation_commands",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
            if (
                any(pattern.search(key_text) for pattern in _TEXT_SIGNAL_PATTERNS.values())
                or any(pattern.search(key_text) for pattern in _SAFEGUARD_PATTERNS.values())
            ):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        texts = []
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


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
    "DnsCutoverReadinessLevel",
    "DnsCutoverSafeguard",
    "DnsCutoverSignal",
    "TaskDnsCutoverReadinessPlan",
    "TaskDnsCutoverReadinessRecord",
    "analyze_task_dns_cutover_readiness",
    "build_task_dns_cutover_readiness_plan",
    "summarize_task_dns_cutover_readiness",
    "task_dns_cutover_readiness_plan_to_dict",
    "task_dns_cutover_readiness_plan_to_markdown",
]
