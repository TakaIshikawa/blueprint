"""Assess task readiness for API response cache header implementation work."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


CacheHeaderSignal = Literal[
    "cache_control_header",
    "max_age_directive",
    "no_store_directive",
    "private_public_directive",
    "etag_header",
    "last_modified_header",
    "conditional_get_request",
    "not_modified_304_response",
    "vary_header",
    "cdn_caching_behavior",
    "cache_invalidation_hooks",
]
CacheHeaderSafeguard = Literal[
    "cache_control_tests",
    "private_no_store_for_sensitive_data",
    "conditional_request_tests",
    "vary_header_configuration",
    "cache_invalidation_tests",
    "cache_documentation",
]
CacheHeaderReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[CacheHeaderReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[CacheHeaderSignal, ...] = (
    "cache_control_header",
    "max_age_directive",
    "no_store_directive",
    "private_public_directive",
    "etag_header",
    "last_modified_header",
    "conditional_get_request",
    "not_modified_304_response",
    "vary_header",
    "cdn_caching_behavior",
    "cache_invalidation_hooks",
)
_SAFEGUARD_ORDER: tuple[CacheHeaderSafeguard, ...] = (
    "cache_control_tests",
    "private_no_store_for_sensitive_data",
    "conditional_request_tests",
    "vary_header_configuration",
    "cache_invalidation_tests",
    "cache_documentation",
)
_SIGNAL_PATTERNS: dict[CacheHeaderSignal, re.Pattern[str]] = {
    "cache_control_header": re.compile(
        r"\b(?:cache-control|cache control|cachecontrol|set cache header|cache header)\b",
        re.I,
    ),
    "max_age_directive": re.compile(
        r"\b(?:max-age|max age|maxage|s-maxage|s maxage|smaxage|cache duration|cache ttl)\b",
        re.I,
    ),
    "no_store_directive": re.compile(
        r"\b(?:no-store|no store|nostore|must-revalidate|must revalidate)\b",
        re.I,
    ),
    "private_public_directive": re.compile(
        r"\b(?:private cache|public cache|cache private|cache public|private directive|public directive)\b",
        re.I,
    ),
    "etag_header": re.compile(
        r"\b(?:etag|e-tag|entity tag|weak etag|strong etag)\b",
        re.I,
    ),
    "last_modified_header": re.compile(
        r"\b(?:last-modified|last modified|modification time|lastmodified)\b",
        re.I,
    ),
    "conditional_get_request": re.compile(
        r"\b(?:conditional get|conditional request|if-none-match|if none match|if-modified-since|if modified since)\b",
        re.I,
    ),
    "not_modified_304_response": re.compile(
        r"\b(?:304 not modified|304 response|not modified response|304 status)\b",
        re.I,
    ),
    "vary_header": re.compile(
        r"\b(?:vary header|vary:|vary by|vary accept|vary encoding|vary authorization)\b",
        re.I,
    ),
    "cdn_caching_behavior": re.compile(
        r"\b(?:cdn cache|cdn caching|edge cache|edge caching|cloudfront|fastly|akamai|cloudflare)\b",
        re.I,
    ),
    "cache_invalidation_hooks": re.compile(
        r"\b(?:cache invalidation|invalidate cache|purge cache|cache purge|cache clearing|clear cache)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[CacheHeaderSignal, re.Pattern[str]] = {
    "cache_control_header": re.compile(r"cache[_-]?control|cache[_-]?header", re.I),
    "max_age_directive": re.compile(r"max[_-]?age|cache[_-]?ttl|cache[_-]?duration", re.I),
    "no_store_directive": re.compile(r"no[_-]?store|must[_-]?revalidate", re.I),
    "private_public_directive": re.compile(r"private|public|cache[_-]?directive", re.I),
    "etag_header": re.compile(r"etag|e[_-]?tag", re.I),
    "last_modified_header": re.compile(r"last[_-]?modified", re.I),
    "conditional_get_request": re.compile(r"conditional|if[_-]?none[_-]?match|if[_-]?modified", re.I),
    "not_modified_304_response": re.compile(r"304|not[_-]?modified", re.I),
    "vary_header": re.compile(r"vary", re.I),
    "cdn_caching_behavior": re.compile(r"cdn|edge|cloudfront|fastly", re.I),
    "cache_invalidation_hooks": re.compile(r"invalidat|purge|cache[_-]?clear", re.I),
}
_SAFEGUARD_PATTERNS: dict[CacheHeaderSafeguard, re.Pattern[str]] = {
    "cache_control_tests": re.compile(
        r"\b(?:(?:cache.{0,80}(?:control|header)).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:cache.{0,80}(?:control|header))|"
        r"cache tests?|max-age tests?)\b",
        re.I,
    ),
    "private_no_store_for_sensitive_data": re.compile(
        r"\b(?:private.{0,80}(?:sensitive|authenticated|personal|pii|user.{0,40}data)|"
        r"no-store.{0,80}(?:sensitive|authenticated|personal|pii|user.{0,40}data)|"
        r"(?:sensitive|authenticated|personal|pii|user.{0,40}data).{0,80}(?:private|no-store)|"
        r"prevent.{0,80}cach(?:e|ing).{0,80}(?:sensitive|authenticated|personal|pii))\b",
        re.I,
    ),
    "conditional_request_tests": re.compile(
        r"\b(?:(?:conditional.{0,80}(?:request|get)).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:conditional.{0,80}(?:request|get))|"
        r"304 tests?|if-none-match tests?|if-modified-since tests?)\b",
        re.I,
    ),
    "vary_header_configuration": re.compile(
        r"\b(?:vary header.{0,80}(?:config|configur|set|defin|ensur|proper|cache key)|"
        r"(?:config|configur|set|defin|ensur).{0,80}vary header|"
        r"vary.{0,80}(?:accept|encoding|authorization|cookie))\b",
        re.I,
    ),
    "cache_invalidation_tests": re.compile(
        r"\b(?:(?:cache.{0,80}(?:invalidat|purg|clear)).{0,80}(?:tests?|coverage|scenario|case)|"
        r"(?:tests?|coverage|scenario|case).{0,80}(?:cache.{0,80}(?:invalidat|purg|clear))|"
        r"invalidation tests?|purge tests?)\b",
        re.I,
    ),
    "cache_documentation": re.compile(
        r"\b(?:cache.{0,80}(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?)|"
        r"(?:docs|documentation|guide|readme|openapi|swagger|document|usage|examples?).{0,80}cache|"
        r"cache.{0,80}policy|caching.{0,80}strategy)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:cache|caching)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved|header)\b",
    re.I,
)
_SENSITIVE_ENDPOINT_RE = re.compile(
    r"\b(?:authenticated|authorization|auth|login|user.{0,40}data|personal|pii|sensitive|private)\b",
    re.I,
)
_REMEDIATIONS: dict[CacheHeaderSafeguard, str] = {
    "cache_control_tests": "Add tests verifying Cache-Control headers, max-age directives, and appropriate cache behavior for different response types.",
    "private_no_store_for_sensitive_data": "Ensure sensitive or authenticated endpoints use private or no-store directives to prevent caching of user-specific data.",
    "conditional_request_tests": "Add tests for conditional GET requests (If-None-Match, If-Modified-Since) and 304 Not Modified responses.",
    "vary_header_configuration": "Configure Vary headers appropriately to prevent serving cached responses when request headers differ (e.g., Accept, Authorization).",
    "cache_invalidation_tests": "Add tests for cache invalidation hooks, purge mechanisms, and cache clearing on data updates.",
    "cache_documentation": "Document cache policy, TTL values, sensitive data handling, invalidation strategy, and client usage examples.",
}


@dataclass(frozen=True, slots=True)
class TaskApiCacheHeaderReadinessFinding:
    """API cache header readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[CacheHeaderSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[CacheHeaderSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[CacheHeaderSafeguard, ...] = field(default_factory=tuple)
    readiness: CacheHeaderReadiness = "partial"
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
class TaskApiCacheHeaderReadinessPlan:
    """Plan-level API cache header readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiCacheHeaderReadinessFinding, ...] = field(default_factory=tuple)
    cache_header_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiCacheHeaderReadinessFinding, ...]:
        """Compatibility view for modules that expose readiness records."""
        return self.findings

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "findings": [finding.to_dict() for finding in self.findings],
            "cache_header_task_ids": list(self.cache_header_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return cache header readiness findings as plain dictionaries."""
        return [finding.to_dict() for finding in self.findings]

    def to_markdown(self) -> str:
        """Render cache header readiness guidance as deterministic Markdown."""
        title = "# Task API Cache Header Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Cache header task count: {self.summary.get('cache_header_task_count', 0)}",
            f"- Not applicable task count: {self.summary.get('not_applicable_task_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{readiness} {readiness_counts.get(readiness, 0)}" for readiness in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER[:5]),
        ]
        if not self.findings:
            lines.extend(["", "No API cache header readiness findings were identified."])
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


def build_task_api_cache_header_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiCacheHeaderReadinessPlan:
    """Build a cache header readiness review from an execution plan."""
    plan_id, tasks = _load_plan(plan)
    findings_list = [_analyze_task(task) for task in tasks]
    findings = tuple(finding for finding in findings_list if finding is not None)
    cache_header_ids = tuple(finding.task_id for finding in findings)
    not_applicable_ids = tuple(
        task["id"]
        for task in tasks
        if task.get("id") and task["id"] not in cache_header_ids
    )
    return TaskApiCacheHeaderReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        cache_header_task_ids=cache_header_ids,
        not_applicable_task_ids=not_applicable_ids,
        summary=_summary(findings, cache_header_ids, not_applicable_ids),
    )


def derive_task_api_cache_header_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiCacheHeaderReadinessPlan:
    """Compatibility helper for callers that use derive_* naming."""
    return build_task_api_cache_header_readiness_plan(plan)


def generate_task_api_cache_header_readiness_plan(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> TaskApiCacheHeaderReadinessPlan:
    """Compatibility helper for callers that use generate_* naming."""
    return build_task_api_cache_header_readiness_plan(plan)


def extract_task_api_cache_header_readiness_findings(
    plan: ExecutionPlan | Mapping[str, Any] | str | object,
) -> tuple[TaskApiCacheHeaderReadinessFinding, ...]:
    """Return cache header readiness findings for applicable tasks."""
    return build_task_api_cache_header_readiness_plan(plan).findings


def summarize_task_api_cache_header_readiness(
    result: TaskApiCacheHeaderReadinessPlan | ExecutionPlan | Mapping[str, Any] | str | object,
) -> dict[str, Any]:
    """Return deterministic counts for cache header readiness review."""
    if isinstance(result, TaskApiCacheHeaderReadinessPlan):
        return dict(result.summary)
    return build_task_api_cache_header_readiness_plan(result).summary


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


def _analyze_task(task: Mapping[str, Any]) -> TaskApiCacheHeaderReadinessFinding | None:
    task_id = task.get("id", "")
    title = task.get("title", "")

    if _NO_IMPACT_RE.search(_searchable_task_text(task)):
        return None

    detected_signals = _detect_signals(task)
    if not detected_signals:
        return None

    present_safeguards = _detect_safeguards(task)
    missing_safeguards = tuple(
        safeguard
        for safeguard in _SAFEGUARD_ORDER
        if safeguard not in present_safeguards
    )

    # Check if this is a sensitive/authenticated endpoint
    task_text = _searchable_task_text(task)
    has_sensitive_context = bool(_SENSITIVE_ENDPOINT_RE.search(task_text))

    readiness = _compute_readiness(detected_signals, present_safeguards, missing_safeguards, has_sensitive_context)
    evidence = _collect_evidence(task, detected_signals)
    actionable_remediations = tuple(
        _REMEDIATIONS[safeguard] for safeguard in missing_safeguards
    )

    return TaskApiCacheHeaderReadinessFinding(
        task_id=task_id,
        title=title,
        detected_signals=detected_signals,
        present_safeguards=present_safeguards,
        missing_safeguards=missing_safeguards,
        readiness=readiness,
        evidence=evidence,
        actionable_remediations=actionable_remediations,
    )


def _detect_signals(task: Mapping[str, Any]) -> tuple[CacheHeaderSignal, ...]:
    searchable = _searchable_task_text(task)
    paths = _task_paths(task)
    validation_commands = _validation_commands(task)
    detected: list[CacheHeaderSignal] = []

    for signal in _SIGNAL_ORDER:
        if _SIGNAL_PATTERNS[signal].search(searchable):
            detected.append(signal)
        elif any(_PATH_SIGNAL_PATTERNS[signal].search(path) for path in paths):
            detected.append(signal)
        elif any(_SIGNAL_PATTERNS[signal].search(cmd) for cmd in validation_commands):
            detected.append(signal)

    return tuple(_dedupe(detected))


def _detect_safeguards(task: Mapping[str, Any]) -> tuple[CacheHeaderSafeguard, ...]:
    searchable = _searchable_task_text(task)
    validation_commands = _validation_commands(task)
    detected: list[CacheHeaderSafeguard] = []

    for safeguard in _SAFEGUARD_ORDER:
        if _SAFEGUARD_PATTERNS[safeguard].search(searchable):
            detected.append(safeguard)
        elif any(_SAFEGUARD_PATTERNS[safeguard].search(cmd) for cmd in validation_commands):
            detected.append(safeguard)

    return tuple(_dedupe(detected))


def _compute_readiness(
    signals: tuple[CacheHeaderSignal, ...],
    present: tuple[CacheHeaderSafeguard, ...],
    missing: tuple[CacheHeaderSafeguard, ...],
    has_sensitive_context: bool,
) -> CacheHeaderReadiness:
    if not signals:
        return "weak"

    # For sensitive/authenticated endpoints, require private/no-store safeguards
    if has_sensitive_context and "private_no_store_for_sensitive_data" in missing:
        return "weak"

    if not missing:
        return "strong"
    if len(present) >= len(missing):
        return "partial"
    return "weak"


def _collect_evidence(
    task: Mapping[str, Any],
    signals: tuple[CacheHeaderSignal, ...],
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
    findings: tuple[TaskApiCacheHeaderReadinessFinding, ...],
    cache_header_ids: tuple[str, ...],
    not_applicable_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "cache_header_task_count": len(cache_header_ids),
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


def _overall_readiness(findings: tuple[TaskApiCacheHeaderReadinessFinding, ...]) -> CacheHeaderReadiness:
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
    "CacheHeaderReadiness",
    "CacheHeaderSafeguard",
    "CacheHeaderSignal",
    "TaskApiCacheHeaderReadinessFinding",
    "TaskApiCacheHeaderReadinessPlan",
    "build_task_api_cache_header_readiness_plan",
    "derive_task_api_cache_header_readiness_plan",
    "extract_task_api_cache_header_readiness_findings",
    "generate_task_api_cache_header_readiness_plan",
    "summarize_task_api_cache_header_readiness",
]
