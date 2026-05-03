"""Plan API SDK generation readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ApiSdkGenerationSignal = Literal[
    "sdk_generation",
    "language_targets",
    "generator_tooling",
    "sdk_versioning",
    "package_distribution",
    "sdk_documentation",
    "auth_helpers",
    "retry_logic",
]
ApiSdkGenerationSafeguard = Literal[
    "generator_configuration",
    "openapi_spec_completeness",
    "version_alignment",
    "publishing_workflow",
    "documentation_quality",
    "authentication_helpers",
    "retry_implementation",
    "example_code_coverage",
]
ApiSdkGenerationReadiness = Literal["ready", "partial", "weak"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[ApiSdkGenerationSignal, ...] = (
    "sdk_generation",
    "language_targets",
    "generator_tooling",
    "sdk_versioning",
    "package_distribution",
    "sdk_documentation",
    "auth_helpers",
    "retry_logic",
)
_SAFEGUARD_ORDER: tuple[ApiSdkGenerationSafeguard, ...] = (
    "generator_configuration",
    "openapi_spec_completeness",
    "version_alignment",
    "publishing_workflow",
    "documentation_quality",
    "authentication_helpers",
    "retry_implementation",
    "example_code_coverage",
)
_READINESS_ORDER: dict[ApiSdkGenerationReadiness, int] = {"weak": 0, "partial": 1, "ready": 2}
_SIGNAL_PATTERNS: dict[ApiSdkGenerationSignal, re.Pattern[str]] = {
    "sdk_generation": re.compile(
        r"\b(?:sdk generation|generate sdk(?:s)?|client generation|code generation|"
        r"client library(?:ies)?|api client(?:s)?|language binding(?:s)?)\b",
        re.I,
    ),
    "language_targets": re.compile(
        r"\b(?:python sdk|javascript sdk|go sdk|java sdk|ruby sdk|php sdk|typescript sdk|"
        r"language target(?:s)?|target language(?:s)?|multi[- ]?language)\b",
        re.I,
    ),
    "generator_tooling": re.compile(
        r"\b(?:openapi generator|swagger codegen|autorest|smithy|generator tool(?:s)?|"
        r"code generator|mustache template(?:s)?)\b",
        re.I,
    ),
    "sdk_versioning": re.compile(
        r"\b(?:sdk version(?:ing)?|semantic versioning|semver|version alignment|"
        r"version strategy|version compatibility)\b",
        re.I,
    ),
    "package_distribution": re.compile(
        r"\b(?:npm package|pypi package|rubygems|maven central|package distribution|"
        r"package publishing|publish to|package registry(?:ies)?)\b",
        re.I,
    ),
    "sdk_documentation": re.compile(
        r"\b(?:sdk documentation|generated documentation|jsdoc|pydoc|javadoc|rdoc|godoc|"
        r"api reference|usage guide(?:s)?)\b",
        re.I,
    ),
    "auth_helpers": re.compile(
        r"\b(?:authentication helper(?:s)?|auth helper(?:s)?|auth method(?:s)?|"
        r"credential(?:s)? helper(?:s)?|oauth helper(?:s)?)\b",
        re.I,
    ),
    "retry_logic": re.compile(
        r"\b(?:retry logic|retry polic(?:y|ies)|exponential backoff|retry mechanism|"
        r"retry strategy(?:ies)?|automatic retry(?:ies)?)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[ApiSdkGenerationSignal, re.Pattern[str]] = {
    "sdk_generation": re.compile(r"sdk[s]?|client[s]?|generator|codegen", re.I),
    "language_targets": re.compile(r"python|javascript|go|java|ruby|php|typescript", re.I),
    "generator_tooling": re.compile(r"openapi|swagger|generator|template[s]?", re.I),
    "sdk_versioning": re.compile(r"version(?:ing)?|semver", re.I),
    "package_distribution": re.compile(r"npm|pypi|rubygems|maven|publish|package", re.I),
    "sdk_documentation": re.compile(r"docs?|documentation|jsdoc|pydoc|javadoc", re.I),
    "auth_helpers": re.compile(r"auth(?:entication)?|credential[s]?|oauth", re.I),
    "retry_logic": re.compile(r"retry|backoff", re.I),
}
_SAFEGUARD_PATTERNS: dict[ApiSdkGenerationSafeguard, re.Pattern[str]] = {
    "generator_configuration": re.compile(
        r"\b(?:generator config(?:uration)?|configure generator|generator settings|"
        r"openapi[- ]?generator\.(?:yaml|yml|json)|swagger[- ]?codegen\.(?:yaml|yml|json)|"
        r"generator template(?:s)?|custom template(?:s)?|template config(?:uration)?)\b",
        re.I,
    ),
    "openapi_spec_completeness": re.compile(
        r"\b(?:openapi spec(?:ification)?|swagger spec|api spec|spec completeness|"
        r"complete spec|validate spec|spec validation|openapi\.(?:yaml|yml|json)|"
        r"swagger\.(?:yaml|yml|json)|spec quality|spec coverage)\b",
        re.I,
    ),
    "version_alignment": re.compile(
        r"\b(?:version alignment|align version(?:s)?|version sync|sdk version|api version|"
        r"version compatibility|version matching|semver|semantic versioning|"
        r"version strategy)\b",
        re.I,
    ),
    "publishing_workflow": re.compile(
        r"\b(?:publish(?:ing)? workflow|package workflow|release workflow|ci[/-]?cd|"
        r"publish to (?:npm|pypi|rubygems|maven)|npm publish|pypi upload|"
        r"package release|distribution workflow|deployment workflow)\b",
        re.I,
    ),
    "documentation_quality": re.compile(
        r"\b(?:documentation quality|doc(?:s)? quality|generated doc(?:s)?|"
        r"api reference|usage guide|integration guide|jsdoc|pydoc|javadoc|rdoc|godoc|"
        r"documentation site|docs site|readme|quickstart)\b",
        re.I,
    ),
    "authentication_helpers": re.compile(
        r"\b(?:auth(?:entication)? helper(?:s)?|auth method(?:s)?|credential provider(?:s)?|"
        r"api key helper(?:s)?|bearer token helper(?:s)?|oauth helper(?:s)?|"
        r"authenticate method|auth configuration|auth interceptor(?:s)?)\b",
        re.I,
    ),
    "retry_implementation": re.compile(
        r"\b(?:retry implementation|implement retry|retry logic|retry polic(?:y|ies)|"
        r"exponential backoff|backoff strategy(?:ies)?|jitter|max(?:imum)? retr(?:y|ies)|"
        r"retry configuration|automatic retry(?:ies)?|retry on failure)\b",
        re.I,
    ),
    "example_code_coverage": re.compile(
        r"\b(?:example code|code example(?:s)?|usage example(?:s)?|sample code|"
        r"quickstart|getting started|tutorial(?:s)?|integration example(?:s)?|"
        r"demo code|sample application(?:s)?|code snippet(?:s)?)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:sdk(?:s)?|client library(?:ies)?|code generation|"
    r"package distribution|sdk versioning|sdk documentation)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_ACTIONABLE_GAPS: dict[ApiSdkGenerationSafeguard, str] = {
    "generator_configuration": "Configure SDK generator with OpenAPI spec, templates, output directories, and language-specific settings.",
    "openapi_spec_completeness": "Ensure OpenAPI spec is complete with all endpoints, schemas, authentication, and examples for SDK generation.",
    "version_alignment": "Align SDK versions with API versions using semantic versioning and maintain version compatibility matrix.",
    "publishing_workflow": "Set up automated publishing workflows for npm, PyPI, RubyGems, Maven with credentials and release automation.",
    "documentation_quality": "Generate SDK documentation from OpenAPI spec, include usage examples, quickstart guides, and API reference.",
    "authentication_helpers": "Implement authentication helper methods for API keys, bearer tokens, OAuth flows with credential management.",
    "retry_implementation": "Add retry logic with exponential backoff, jitter, configurable max retries, and idempotent request handling.",
    "example_code_coverage": "Include example code for common operations, integration patterns, quickstart guides, and sample applications.",
}


@dataclass(frozen=True, slots=True)
class TaskApiSdkGenerationReadinessFinding:
    """API SDK generation readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[ApiSdkGenerationSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[ApiSdkGenerationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[ApiSdkGenerationSafeguard, ...] = field(default_factory=tuple)
    readiness: ApiSdkGenerationReadiness = "partial"
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
class TaskApiSdkGenerationReadinessPlan:
    """Plan-level API SDK generation readiness review."""

    plan_id: str | None = None
    findings: tuple[TaskApiSdkGenerationReadinessFinding, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskApiSdkGenerationReadinessFinding, ...]:
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


def build_task_api_sdk_generation_readiness_plan(source: Any) -> TaskApiSdkGenerationReadinessPlan:
    """Build API SDK generation readiness findings for relevant execution tasks."""
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
    return TaskApiSdkGenerationReadinessPlan(
        plan_id=plan_id,
        findings=findings,
        impacted_task_ids=tuple(finding.task_id for finding in findings),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_api_sdk_generation_readiness(source: Any) -> TaskApiSdkGenerationReadinessPlan:
    """Compatibility alias for building API SDK generation readiness plans."""
    return build_task_api_sdk_generation_readiness_plan(source)


def summarize_task_api_sdk_generation_readiness(source: Any) -> TaskApiSdkGenerationReadinessPlan:
    """Compatibility alias for building API SDK generation readiness plans."""
    return build_task_api_sdk_generation_readiness_plan(source)


def extract_task_api_sdk_generation_readiness(source: Any) -> TaskApiSdkGenerationReadinessPlan:
    """Compatibility alias for extracting API SDK generation readiness plans."""
    return build_task_api_sdk_generation_readiness_plan(source)


def generate_task_api_sdk_generation_readiness(source: Any) -> TaskApiSdkGenerationReadinessPlan:
    """Compatibility alias for generating API SDK generation readiness plans."""
    return build_task_api_sdk_generation_readiness_plan(source)


def recommend_task_api_sdk_generation_readiness(source: Any) -> TaskApiSdkGenerationReadinessPlan:
    """Compatibility alias for recommending API SDK generation readiness gaps."""
    return build_task_api_sdk_generation_readiness_plan(source)


def task_api_sdk_generation_readiness_plan_to_dict(result: TaskApiSdkGenerationReadinessPlan) -> dict[str, Any]:
    """Serialize an API SDK generation readiness plan to a plain dictionary."""
    return result.to_dict()


task_api_sdk_generation_readiness_plan_to_dict.__test__ = False


def task_api_sdk_generation_readiness_plan_to_dicts(
    result: TaskApiSdkGenerationReadinessPlan | Iterable[TaskApiSdkGenerationReadinessFinding],
) -> list[dict[str, Any]]:
    """Serialize API SDK generation readiness findings to plain dictionaries."""
    if isinstance(result, TaskApiSdkGenerationReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_api_sdk_generation_readiness_plan_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[ApiSdkGenerationSignal, ...] = field(default_factory=tuple)
    safeguards: tuple[ApiSdkGenerationSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskApiSdkGenerationReadinessFinding | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None

    missing: tuple[ApiSdkGenerationSafeguard, ...] = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.safeguards)
    return TaskApiSdkGenerationReadinessFinding(
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
    signal_hits: set[ApiSdkGenerationSignal] = set()
    safeguard_hits: set[ApiSdkGenerationSafeguard] = set()
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
            evidence.append(_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        evidence=tuple(sorted(set(evidence), key=str.casefold)[:8]),
        explicitly_no_impact=explicitly_no_impact,
    )


def _readiness(signals: tuple[ApiSdkGenerationSignal, ...], missing: tuple[ApiSdkGenerationSafeguard, ...]) -> ApiSdkGenerationReadiness:
    if not signals:
        return "weak"
    if not missing:
        return "ready"
    coverage = len(_SAFEGUARD_ORDER) - len(missing)
    ratio = coverage / len(_SAFEGUARD_ORDER) if _SAFEGUARD_ORDER else 0.0
    return "ready" if ratio >= 0.75 else "partial" if ratio >= 0.40 else "weak"


def _summary(
    findings: tuple[TaskApiSdkGenerationReadinessFinding, ...],
    total_task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    readiness_counts = {readiness: 0 for readiness in _READINESS_ORDER}
    for finding in findings:
        readiness_counts[finding.readiness] += 1

    all_missing = [safeguard for finding in findings for safeguard in finding.missing_safeguards]
    missing_counts = {safeguard: all_missing.count(safeguard) for safeguard in _SAFEGUARD_ORDER}

    return {
        "total_task_count": total_task_count,
        "impacted_task_count": len(findings),
        "not_applicable_task_count": len(not_applicable_task_ids),
        "readiness_counts": readiness_counts,
        "missing_safeguard_counts": missing_counts,
        "most_common_gaps": [
            safeguard
            for safeguard, _ in sorted(missing_counts.items(), key=lambda item: (-item[1], item[0]))
            if missing_counts[safeguard] > 0
        ][:5],
    }


def _source_payload(source: Any) -> tuple[str | None, list[Mapping[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        return source.id, [task.model_dump(mode="python") for task in source.tasks]
    if hasattr(source, "model_dump") and hasattr(source, "tasks"):
        value = source.model_dump(mode="python")
        return value.get("id"), value.get("tasks", [])
    if isinstance(source, Mapping):
        try:
            plan = ExecutionPlan.model_validate(source)
            return plan.id, [task.model_dump(mode="python") for task in plan.tasks]
        except (TypeError, ValueError, ValidationError):
            pass
        return source.get("id"), source.get("tasks", [])
    return None, []


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or _optional_text(task.get("task_id")) or f"task_{index}"


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    candidate_texts: list[tuple[str, str]] = []
    scanned_fields = (
        "title",
        "description",
        "what",
        "why",
        "how",
        "summary",
        "goal",
        "goals",
        "scope",
        "requirements",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "approach",
        "implementation_notes",
        "technical_details",
        "risks",
        "testing",
    )
    for field_name in scanned_fields:
        if field_name in task:
            _collect_texts(candidate_texts, field_name, task[field_name])
    for key in sorted(task, key=lambda item: str(item)):
        if key not in scanned_fields and key not in {"id", "task_id", "status", "created_at", "updated_at"}:
            _collect_texts(candidate_texts, str(key), task[key])
    return candidate_texts


def _collect_texts(collector: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            _collect_texts(collector, f"{source_field}.{key}", value[key])
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _collect_texts(collector, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        collector.append((source_field, text))


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return [item for item in value if isinstance(item, str)]
    return []


def _normalized_path(path: str) -> str:
    try:
        normalized = PurePosixPath(path).as_posix()
        return normalized if "/" in normalized else ""
    except (TypeError, ValueError):
        return ""


def _snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 120:
        cleaned = f"{cleaned[:117].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(value)
    return text or None


__all__ = [
    "ApiSdkGenerationReadiness",
    "ApiSdkGenerationSafeguard",
    "ApiSdkGenerationSignal",
    "TaskApiSdkGenerationReadinessFinding",
    "TaskApiSdkGenerationReadinessPlan",
    "analyze_task_api_sdk_generation_readiness",
    "build_task_api_sdk_generation_readiness_plan",
    "extract_task_api_sdk_generation_readiness",
    "generate_task_api_sdk_generation_readiness",
    "recommend_task_api_sdk_generation_readiness",
    "summarize_task_api_sdk_generation_readiness",
    "task_api_sdk_generation_readiness_plan_to_dict",
    "task_api_sdk_generation_readiness_plan_to_dicts",
]
