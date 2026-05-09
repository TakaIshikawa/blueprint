"""Plan frontend rendering strategy for execution tasks involving UI rendering."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FrontendRenderingSignal = Literal[
    "csr",
    "ssr",
    "ssg",
    "isr",
    "hydration",
    "data_fetching",
    "bundle_optimization",
    "performance_budget",
    "seo",
]
FrontendRenderingStrategy = Literal[
    "client_side_rendering",
    "server_side_rendering",
    "static_site_generation",
    "incremental_static_regeneration",
    "hydration_strategy",
    "data_fetching_pattern",
    "bundle_splitting",
    "code_splitting",
    "lazy_loading",
    "performance_monitoring",
    "seo_optimization",
    "time_to_interactive_target",
]
FrontendRenderingReadiness = Literal["ready", "partial", "weak"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[FrontendRenderingSignal, ...] = (
    "csr",
    "ssr",
    "ssg",
    "isr",
    "hydration",
    "data_fetching",
    "bundle_optimization",
    "performance_budget",
    "seo",
)
_STRATEGY_ORDER: tuple[FrontendRenderingStrategy, ...] = (
    "client_side_rendering",
    "server_side_rendering",
    "static_site_generation",
    "incremental_static_regeneration",
    "hydration_strategy",
    "data_fetching_pattern",
    "bundle_splitting",
    "code_splitting",
    "lazy_loading",
    "performance_monitoring",
    "seo_optimization",
    "time_to_interactive_target",
)
_READINESS_ORDER: dict[FrontendRenderingReadiness, int] = {"weak": 0, "partial": 1, "ready": 2}
_SIGNAL_PATTERNS: dict[FrontendRenderingSignal, re.Pattern[str]] = {
    "csr": re.compile(
        r"\b(?:csr|client[- ]?side rendering|client[- ]?rendered|spa|single[- ]?page app(?:lication)?|"
        r"react app|vue app|svelte app|client only)\b",
        re.I,
    ),
    "ssr": re.compile(
        r"\b(?:ssr|server[- ]?side rendering|server[- ]?rendered|next\.?js|nuxt|sveltekit|remix|"
        r"render on server|getServerSideProps|loader function)\b",
        re.I,
    ),
    "ssg": re.compile(
        r"\b(?:ssg|static[- ]?site generation|static[- ]?generated|pre[- ]?rendered?|"
        r"getStaticProps|getStaticPaths|build[- ]?time rendering|jamstack)\b",
        re.I,
    ),
    "isr": re.compile(
        r"\b(?:isr|incremental static regeneration|revalidate|on[- ]?demand revalidation|"
        r"stale[- ]?while[- ]?revalidate|background regeneration)\b",
        re.I,
    ),
    "hydration": re.compile(
        r"\b(?:hydrat(?:e|ed|ion)|progressive hydration|partial hydration|selective hydration|"
        r"islands architecture|resumability|streaming ssr|streaming html)\b",
        re.I,
    ),
    "data_fetching": re.compile(
        r"\b(?:data fetching|fetch strategy|getServerSideProps|getStaticProps|loader|"
        r"use(?:Query|SWR|Fetch)|tanstack query|react query|swr|apollo|graphql client)\b",
        re.I,
    ),
    "bundle_optimization": re.compile(
        r"\b(?:bundle(?:d| size| optimization)?|tree[- ]?shak(?:e|ing)|code[- ]?split(?:ting)?|"
        r"lazy load(?:ing)?|dynamic import|chunk(?:s| splitting)?|minif(?:y|ication)|"
        r"webpack|vite|rollup|esbuild|turbopack)\b",
        re.I,
    ),
    "performance_budget": re.compile(
        r"\b(?:performance budget|ttfb|fcp|lcp|tti|time to interactive|first contentful paint|"
        r"largest contentful paint|core web vitals|lighthouse score|web vitals|cls|cumulative layout shift|"
        r"fid|first input delay|inp|interaction to next paint)\b",
        re.I,
    ),
    "seo": re.compile(
        r"\b(?:seo|search engine optimization|meta tags?|og tags?|open graph|twitter cards?|"
        r"structured data|schema\.org|canonical|robots\.txt|sitemap\.xml|crawl(?:able|ing)?|"
        r"index(?:able|ing)?|search ranking)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[FrontendRenderingSignal, re.Pattern[str]] = {
    "csr": re.compile(r"client|spa|react|vue|svelte", re.I),
    "ssr": re.compile(r"ssr|server[_-]?side|pages?|next|nuxt|remix|sveltekit|getServerSideProps", re.I),
    "ssg": re.compile(r"ssg|static|pre[_-]?render|getStaticProps|build[_-]?time", re.I),
    "isr": re.compile(r"isr|revalidat(?:e|ion)|incremental", re.I),
    "hydration": re.compile(r"hydrat(?:e|ion)|islands|streaming", re.I),
    "data_fetching": re.compile(r"data[_-]?fetch(?:ing)?|query|swr|loader", re.I),
    "bundle_optimization": re.compile(r"bundle|chunk|split|lazy|minif(?:y|ied)|webpack|vite|rollup", re.I),
    "performance_budget": re.compile(r"performance|perf|ttfb|fcp|lcp|tti|web[_-]?vitals|lighthouse", re.I),
    "seo": re.compile(r"seo|meta|og[_-]?tags?|sitemap|robots|schema", re.I),
}
_STRATEGY_PATTERNS: dict[FrontendRenderingStrategy, re.Pattern[str]] = {
    "client_side_rendering": re.compile(
        r"\b(?:client[- ]?side rendering|render on client|spa|single[- ]?page app(?:lication)?|"
        r"react app|vue app|svelte app|client only)\b",
        re.I,
    ),
    "server_side_rendering": re.compile(
        r"\b(?:server[- ]?side rendering|render on server|getServerSideProps|dynamic rendering|"
        r"ssr pages?|server rendering|server rendered|streaming ssr|remix)\b",
        re.I,
    ),
    "static_site_generation": re.compile(
        r"\b(?:static[- ]?site generation|getStaticProps|getStaticPaths|pre[- ]?render(?:ed|ing)?|"
        r"build[- ]?time rendering|static generation)\b",
        re.I,
    ),
    "incremental_static_regeneration": re.compile(
        r"\b(?:incremental static regeneration|revalidate intervals?|on[- ]?demand revalidation|"
        r"stale[- ]?while[- ]?revalidate|background regeneration)\b",
        re.I,
    ),
    "hydration_strategy": re.compile(
        r"\b(?:hydration strategy|progressive hydration|partial hydration|selective hydration|"
        r"islands architecture|resumability|hydration approach)\b",
        re.I,
    ),
    "data_fetching_pattern": re.compile(
        r"\b(?:data fetching (?:pattern|strategy)|fetch strategy|parallel (?:fetching|queries|loading|data)|"
        r"waterfall|useQuery|useSWR|react query|tanstack query|swr|apollo client|"
        r"getServerSideProps|getStaticProps|loader functions?)\b",
        re.I,
    ),
    "bundle_splitting": re.compile(
        r"\b(?:bundle splitting|route[- ]?based splitting|vendor chunks?|shared dependency|"
        r"bundle strategy|chunk strategy)\b",
        re.I,
    ),
    "code_splitting": re.compile(
        r"\b(?:code splitting|dynamic import(?:s)?|lazy component|"
        r"React\.lazy|loadable component|async component|route[- ]?based chunking)\b",
        re.I,
    ),
    "lazy_loading": re.compile(
        r"\b(?:lazy load(?:ing)?|on[- ]?demand loading|intersection observer|"
        r"below[- ]?the[- ]?fold|viewport visibility|lazy[- ]?loaded)\b",
        re.I,
    ),
    "performance_monitoring": re.compile(
        r"\b(?:performance monitor(?:ing)?|web vitals|lighthouse|pagespeed|"
        r"real user monitoring|rum|synthetic monitoring|performance api|monitor performance|"
        r"optimize for web vitals)\b",
        re.I,
    ),
    "seo_optimization": re.compile(
        r"\b(?:seo optimization|meta tags?|open graph tags?|twitter cards?|structured data|"
        r"schema\.org|canonical urls?|sitemap\.xml|robots\.txt|og tags?)\b",
        re.I,
    ),
    "time_to_interactive_target": re.compile(
        r"\b(?:tti target|time[- ]?to[- ]?interactive target|interactive in \d+|tti < \d+|tti budget|"
        r"interactive within|target tti|tti < ?\d+\.?\d*s?)\b",
        re.I,
    ),
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:frontend|ui|rendering|ssr|csr|ssg|hydration|"
    r"client[- ]?side|server[- ]?side|performance budget)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)
_ACTIONABLE_GAPS: dict[FrontendRenderingStrategy, str] = {
    "client_side_rendering": "Specify client-side rendering approach, initial bundle size limits, and JavaScript execution strategy.",
    "server_side_rendering": "Define server-side rendering requirements, data fetching on server, and caching strategy for SSR responses.",
    "static_site_generation": "Identify static generation paths, build-time data fetching, and fallback for dynamic routes.",
    "incremental_static_regeneration": "Configure ISR revalidation intervals, on-demand revalidation triggers, and stale-while-revalidate behavior.",
    "hydration_strategy": "Choose hydration approach (full, progressive, partial, or islands) and specify interactive component boundaries.",
    "data_fetching_pattern": "Define data fetching strategy (parallel, waterfall, server-only, or client-side), and error handling.",
    "bundle_splitting": "Implement bundle splitting strategy with vendor chunks, route-based splits, and shared dependency extraction.",
    "code_splitting": "Apply code splitting with dynamic imports, lazy-loaded components, and route-based chunking.",
    "lazy_loading": "Configure lazy loading for below-the-fold content, images, and non-critical components using intersection observer.",
    "performance_monitoring": "Set up performance monitoring with Web Vitals, Lighthouse CI, and real user monitoring for TTI, LCP, FCP.",
    "seo_optimization": "Add SEO meta tags, Open Graph tags, structured data, canonical URLs, and XML sitemap for crawlability.",
    "time_to_interactive_target": "Define time-to-interactive target (e.g., <3s on 3G), measure with Lighthouse, and enforce performance budgets.",
}


@dataclass(frozen=True, slots=True)
class TaskFrontendRenderingStrategyFinding:
    """Frontend rendering strategy guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[FrontendRenderingSignal, ...] = field(default_factory=tuple)
    present_strategies: tuple[FrontendRenderingStrategy, ...] = field(default_factory=tuple)
    missing_strategies: tuple[FrontendRenderingStrategy, ...] = field(default_factory=tuple)
    readiness: FrontendRenderingReadiness = "partial"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_strategies": list(self.present_strategies),
            "missing_strategies": list(self.missing_strategies),
            "readiness": self.readiness,
            "evidence": list(self.evidence),
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskFrontendRenderingStrategyPlan:
    """Plan-level frontend rendering strategy review."""

    plan_id: str | None = None
    findings: tuple[TaskFrontendRenderingStrategyFinding, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskFrontendRenderingStrategyFinding, ...]:
        """Compatibility view for modules that expose rendering strategy records."""
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
        """Return rendering strategy findings as plain dictionaries."""
        return [record.to_dict() for record in self.findings]


def build_task_frontend_rendering_strategy_plan(source: Any) -> TaskFrontendRenderingStrategyPlan:
    """Build frontend rendering strategy findings for relevant execution tasks."""
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
    return TaskFrontendRenderingStrategyPlan(
        plan_id=plan_id,
        findings=findings,
        impacted_task_ids=tuple(finding.task_id for finding in findings),
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(findings, total_task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_frontend_rendering_strategy(source: Any) -> TaskFrontendRenderingStrategyPlan:
    """Compatibility alias for building frontend rendering strategy plans."""
    return build_task_frontend_rendering_strategy_plan(source)


def summarize_task_frontend_rendering_strategy(source: Any) -> TaskFrontendRenderingStrategyPlan:
    """Compatibility alias for building frontend rendering strategy plans."""
    return build_task_frontend_rendering_strategy_plan(source)


def extract_task_frontend_rendering_strategy(source: Any) -> TaskFrontendRenderingStrategyPlan:
    """Compatibility alias for extracting frontend rendering strategy plans."""
    return build_task_frontend_rendering_strategy_plan(source)


def generate_task_frontend_rendering_strategy(source: Any) -> TaskFrontendRenderingStrategyPlan:
    """Compatibility alias for generating frontend rendering strategy plans."""
    return build_task_frontend_rendering_strategy_plan(source)


def recommend_task_frontend_rendering_strategy(source: Any) -> TaskFrontendRenderingStrategyPlan:
    """Compatibility alias for recommending frontend rendering strategy gaps."""
    return build_task_frontend_rendering_strategy_plan(source)


def task_frontend_rendering_strategy_plan_to_dict(result: TaskFrontendRenderingStrategyPlan) -> dict[str, Any]:
    """Serialize a frontend rendering strategy plan to a plain dictionary."""
    return result.to_dict()


task_frontend_rendering_strategy_plan_to_dict.__test__ = False


def task_frontend_rendering_strategy_plan_to_dicts(
    result: TaskFrontendRenderingStrategyPlan | Iterable[TaskFrontendRenderingStrategyFinding],
) -> list[dict[str, Any]]:
    """Serialize frontend rendering strategy findings to plain dictionaries."""
    if isinstance(result, TaskFrontendRenderingStrategyPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_frontend_rendering_strategy_plan_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[FrontendRenderingSignal, ...] = field(default_factory=tuple)
    strategies: tuple[FrontendRenderingStrategy, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _finding_for_task(task: Mapping[str, Any], index: int) -> TaskFrontendRenderingStrategyFinding | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not signals.signals:
        return None

    missing = tuple(strategy for strategy in _STRATEGY_ORDER if strategy not in signals.strategies)
    return TaskFrontendRenderingStrategyFinding(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        detected_signals=signals.signals,
        present_strategies=signals.strategies,
        missing_strategies=missing,  # type: ignore[arg-type]
        readiness=_readiness(signals.signals, missing),  # type: ignore[arg-type]
        evidence=signals.evidence,
        recommended_checks=tuple(_ACTIONABLE_GAPS[strategy] for strategy in missing),  # type: ignore[index]
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[FrontendRenderingSignal] = set()
    strategy_hits: set[FrontendRenderingStrategy] = set()
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
        for strategy, pattern in _STRATEGY_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                strategy_hits.add(strategy)
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
        for strategy, pattern in _STRATEGY_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                strategy_hits.add(strategy)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        strategies=tuple(strategy for strategy in _STRATEGY_ORDER if strategy in strategy_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _readiness(
    signals: tuple[FrontendRenderingSignal, ...],
    missing: tuple[FrontendRenderingStrategy, ...],
) -> FrontendRenderingReadiness:
    if not missing:
        return "ready"
    missing_set = set(missing)

    # Critical strategies for production frontend rendering
    critical = {
        "hydration_strategy",
        "data_fetching_pattern",
        "performance_monitoring",
    }

    # If missing 2 or fewer strategies and none are critical, considered ready
    if len(missing) <= 2 and not (critical & missing_set):
        return "ready"

    # If missing 8 or more strategies, definitely weak
    if len(missing) >= 8:
        return "weak"

    # If missing critical strategies and 5 or more total, weak
    if critical & missing_set and len(missing) >= 5:
        return "weak"

    # If rendering signals present but missing both hydration and data fetching, weak
    if {"ssr", "ssg", "isr"} & set(signals) and {
        "hydration_strategy",
        "data_fetching_pattern",
    } <= missing_set:
        return "weak"

    # If performance signals present but missing monitoring and budgets
    if "performance_budget" in signals and {
        "performance_monitoring",
        "time_to_interactive_target",
    } <= missing_set and len(missing) >= 4:
        return "weak"

    return "partial"


def _summary(
    findings: tuple[TaskFrontendRenderingStrategyFinding, ...],
    *,
    total_task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "impacted_task_count": len(findings),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_strategy_count": sum(len(finding.missing_strategies) for finding in findings),
        "readiness_counts": {readiness: sum(1 for finding in findings if finding.readiness == readiness) for readiness in _READINESS_ORDER},
        "signal_counts": {
            signal: sum(1 for finding in findings if signal in finding.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_strategy_counts": {
            strategy: sum(1 for finding in findings if strategy in finding.present_strategies)
            for strategy in _STRATEGY_ORDER
        },
        "missing_strategy_counts": {
            strategy: sum(1 for finding in findings if strategy in finding.missing_strategies)
            for strategy in _STRATEGY_ORDER
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
        "validation_command",
        "validation_commands",
        "test_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))
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
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_STRATEGY_PATTERNS.values()])


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
