"""Build plan-level mobile app release readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


MobileReleaseReadiness = Literal["ready", "partial", "blocked"]
MobileReleaseSeverity = Literal["high", "medium", "low"]
MobileReleasePlatform = Literal["android", "ios", "mobile", "multi_platform"]
MobileReleaseSignal = Literal[
    "app_store_review",
    "phased_rollout",
    "version_compatibility",
    "push_notification_impact",
    "crash_analytics_monitoring",
    "rollback_hotfix_path",
    "feature_flag_gating",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[MobileReleaseReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}
_SEVERITY_ORDER: dict[MobileReleaseSeverity, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: tuple[MobileReleaseSignal, ...] = (
    "app_store_review",
    "phased_rollout",
    "version_compatibility",
    "push_notification_impact",
    "crash_analytics_monitoring",
    "rollback_hotfix_path",
    "feature_flag_gating",
)
_MOBILE_RELEASE_RE = re.compile(
    r"\b(?:mobile app|native app|app release|mobile release|ios|iphone|ipad|android|"
    r"app store|appstore|app store connect|google play|play store|play console|"
    r"testflight|firebase app distribution|apk|aab|ipa|version code|build number|"
    r"phased rollout|staged rollout|crashlytics|app tracking transparency)\b",
    re.I,
)
_PLATFORM_PATTERNS: tuple[tuple[MobileReleasePlatform, re.Pattern[str]], ...] = (
    ("ios", re.compile(r"\b(?:ios|iphone|ipad|ipados|app store connect|app store|testflight|ipa)\b", re.I)),
    ("android", re.compile(r"\b(?:android|google play|play store|play console|apk|aab|version code)\b", re.I)),
)
_SIGNAL_PATTERNS: dict[MobileReleaseSignal, re.Pattern[str]] = {
    "app_store_review": re.compile(
        r"\b(?:app store review|store review|app review|review submission|store approval|"
        r"app store connect|google play review|play store review|play console review|"
        r"reviewer access|review notes|store metadata|store listing|submit for review)\b",
        re.I,
    ),
    "phased_rollout": re.compile(
        r"\b(?:phased release|phased rollout|staged rollout|gradual rollout|canary rollout|"
        r"rollout percentage|rollout percent|release to \d+\s?%|\d+\s?% rollout|"
        r"pause rollout|accelerate rollout|widen rollout|hold point)\b",
        re.I,
    ),
    "version_compatibility": re.compile(
        r"\b(?:version(?:ing)?|build number|build numbers|version code|version name|"
        r"cfbundleversion|cfbundleshortversionstring|semantic version|semver|"
        r"release version|minimum os|min os|supported os|device support|"
        r"ios\s*\d+(?:\.\d+)?\+?|android\s*\d+(?:\.\d+)?\+?|"
        r"minimum sdk|min sdk|target sdk|backward compatibility|compatibility)\b",
        re.I,
    ),
    "push_notification_impact": re.compile(
        r"\b(?:push notification|push notifications|apns|fcm|firebase cloud messaging|"
        r"notification permission|notification opt[- ]?in|deep link|deeplink|badge count|"
        r"silent push|foreground notification)\b",
        re.I,
    ),
    "crash_analytics_monitoring": re.compile(
        r"\b(?:crash[- ]?free|crash free|crash rate|crash threshold|stability threshold|"
        r"stability gate|anr rate|anrs?|crashlytics|sentry|datadog rum|analytics|"
        r"telemetry|monitoring|alerts?|dashboard|no new crashes|fatal crashes)\b",
        re.I,
    ),
    "rollback_hotfix_path": re.compile(
        r"\b(?:hotfix|hot fix|rollback|roll back|expedited review|emergency patch|"
        r"previous build|revert release|kill switch|disable remotely|pause release|"
        r"stop rollout|recover previous version)\b",
        re.I,
    ),
    "feature_flag_gating": re.compile(
        r"\b(?:feature flag|feature flags|flag gate|gated rollout|remote config|"
        r"remote kill switch|server[- ]side flag|launchdarkly|configcat|"
        r"gradual exposure|disable remotely)\b",
        re.I,
    ),
}
_EXPLICIT_GAP_RE = re.compile(
    r"\b(?:gap|missing|unknown|unresolved|tbd|todo|not documented|not defined|"
    r"needs rollout|needs rollback|needs monitoring|without rollout|without rollback|"
    r"without monitoring|no rollback|no hotfix|no phased rollout|no monitoring)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PlanMobileAppReleaseReadinessRow:
    """One grouped mobile app release readiness row."""

    platform: MobileReleasePlatform
    release_area: str
    task_ids: tuple[str, ...]
    titles: tuple[str, ...]
    signals: tuple[MobileReleaseSignal, ...] = field(default_factory=tuple)
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: MobileReleaseReadiness = "partial"
    severity: MobileReleaseSeverity = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "platform": self.platform,
            "release_area": self.release_area,
            "task_ids": list(self.task_ids),
            "titles": list(self.titles),
            "signals": list(self.signals),
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "severity": self.severity,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanMobileAppReleaseReadinessMatrix:
    """Plan-level mobile app release readiness matrix and rollup counts."""

    plan_id: str | None = None
    rows: tuple[PlanMobileAppReleaseReadinessRow, ...] = field(default_factory=tuple)
    mobile_release_task_ids: tuple[str, ...] = field(default_factory=tuple)
    non_mobile_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanMobileAppReleaseReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "mobile_release_task_ids": list(self.mobile_release_task_ids),
            "non_mobile_task_ids": list(self.non_mobile_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return mobile release rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the mobile app release readiness matrix as deterministic Markdown."""
        title = "# Plan Mobile App Release Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('mobile_release_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks need mobile release readiness "
                f"(high: {severity_counts.get('high', 0)}, "
                f"medium: {severity_counts.get('medium', 0)}, "
                f"low: {severity_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No mobile app release readiness rows were inferred."])
            if self.non_mobile_task_ids:
                lines.extend(["", f"Non-mobile tasks: {_markdown_cell(', '.join(self.non_mobile_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Platform | Release Area | Tasks | Titles | Readiness | Severity | Signals | Gaps | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.platform} | "
                f"{_markdown_cell(row.release_area)} | "
                f"{_markdown_cell(', '.join(row.task_ids))} | "
                f"{_markdown_cell('; '.join(row.titles))} | "
                f"{row.readiness} | "
                f"{row.severity} | "
                f"{_markdown_cell(', '.join(row.signals) or 'none')} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.non_mobile_task_ids:
            lines.extend(["", f"Non-mobile tasks: {_markdown_cell(', '.join(self.non_mobile_task_ids))}"])
        return "\n".join(lines)


def build_plan_mobile_app_release_readiness_matrix(source: Any) -> PlanMobileAppReleaseReadinessMatrix:
    """Build grouped mobile app release readiness for an execution plan."""
    plan_id, tasks = _source_payload(source)
    grouped: dict[tuple[MobileReleasePlatform, str], list[_TaskMobileReleaseSignals]] = {}
    non_mobile_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        signals = _task_signals(task, index)
        if not signals.has_mobile_release:
            non_mobile_task_ids.append(signals.task_id)
            continue
        grouped.setdefault((signals.platform, signals.release_area), []).append(signals)
    if ("multi_platform", "mobile_app_release") in grouped and ("mobile", "mobile_app_release") in grouped:
        grouped[("multi_platform", "mobile_app_release")].extend(grouped.pop(("mobile", "mobile_app_release")))

    rows = tuple(sorted((_row_from_group(key, values) for key, values in grouped.items()), key=_row_sort_key))
    mobile_release_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.task_ids))
    return PlanMobileAppReleaseReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        mobile_release_task_ids=mobile_release_task_ids,
        non_mobile_task_ids=tuple(non_mobile_task_ids),
        summary=_summary(len(tasks), rows, non_mobile_task_ids),
    )


def generate_plan_mobile_app_release_readiness_matrix(source: Any) -> PlanMobileAppReleaseReadinessMatrix:
    """Generate a mobile app release readiness matrix from a plan-like source."""
    return build_plan_mobile_app_release_readiness_matrix(source)


def analyze_plan_mobile_app_release_readiness_matrix(source: Any) -> PlanMobileAppReleaseReadinessMatrix:
    """Analyze an execution plan for mobile app release readiness."""
    if isinstance(source, PlanMobileAppReleaseReadinessMatrix):
        return source
    return build_plan_mobile_app_release_readiness_matrix(source)


def derive_plan_mobile_app_release_readiness_matrix(source: Any) -> PlanMobileAppReleaseReadinessMatrix:
    """Derive a mobile app release readiness matrix from a plan-like source."""
    return analyze_plan_mobile_app_release_readiness_matrix(source)


def extract_plan_mobile_app_release_readiness_matrix(source: Any) -> PlanMobileAppReleaseReadinessMatrix:
    """Extract a mobile app release readiness matrix from a plan-like source."""
    return derive_plan_mobile_app_release_readiness_matrix(source)


def summarize_plan_mobile_app_release_readiness_matrix(
    source: PlanMobileAppReleaseReadinessMatrix | Iterable[PlanMobileAppReleaseReadinessRow] | Any,
) -> dict[str, Any] | PlanMobileAppReleaseReadinessMatrix:
    """Return deterministic summary counts for a matrix, row iterable, or source."""
    if isinstance(source, PlanMobileAppReleaseReadinessMatrix):
        return dict(source.summary)
    if _looks_like_plan(source) or _looks_like_task(source) or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_mobile_app_release_readiness_matrix(source)
    rows = tuple(source)
    return _summary(len(rows), rows, ())


def plan_mobile_app_release_readiness_matrix_to_dict(
    matrix: PlanMobileAppReleaseReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a mobile app release readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_mobile_app_release_readiness_matrix_to_dict.__test__ = False


def plan_mobile_app_release_readiness_matrix_to_dicts(
    matrix: PlanMobileAppReleaseReadinessMatrix | Iterable[PlanMobileAppReleaseReadinessRow],
) -> list[dict[str, Any]]:
    """Serialize mobile app release readiness rows to plain dictionaries."""
    if isinstance(matrix, PlanMobileAppReleaseReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_mobile_app_release_readiness_matrix_to_dicts.__test__ = False


def plan_mobile_app_release_readiness_matrix_to_markdown(
    matrix: PlanMobileAppReleaseReadinessMatrix,
) -> str:
    """Render a mobile app release readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_mobile_app_release_readiness_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskMobileReleaseSignals:
    task_id: str
    title: str
    platform: MobileReleasePlatform
    release_area: str
    signals: tuple[MobileReleaseSignal, ...]
    gaps: tuple[str, ...]
    evidence: tuple[str, ...]
    has_mobile_release: bool


def _task_signals(task: Mapping[str, Any], index: int) -> _TaskMobileReleaseSignals:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    platform = _platform(context)
    signal_evidence = _signal_evidence(texts)
    signals = tuple(signal for signal in _SIGNAL_ORDER if signal_evidence.get(signal))
    has_mobile_release = bool(_MOBILE_RELEASE_RE.search(context) or signals)
    explicit_gaps = tuple(
        _dedupe(_evidence_snippet(field, text) for field, text in texts if _EXPLICIT_GAP_RE.search(text))
    )
    gaps = list(explicit_gaps)
    if has_mobile_release:
        for signal, label in (
            ("phased_rollout", "Missing phased rollout or staged rollout plan."),
            ("crash_analytics_monitoring", "Missing crash, analytics, or release monitoring evidence."),
            ("rollback_hotfix_path", "Missing rollback or hotfix path."),
        ):
            if signal not in signals:
                gaps.append(label)
    evidence = tuple(
        _dedupe(
            item
            for signal in _SIGNAL_ORDER
            for item in signal_evidence.get(signal, ())
        )
    )
    return _TaskMobileReleaseSignals(
        task_id=task_id,
        title=title,
        platform=platform,
        release_area="mobile_app_release",
        signals=signals,
        gaps=tuple(_dedupe(gaps)),
        evidence=evidence,
        has_mobile_release=has_mobile_release,
    )


def _row_from_group(
    key: tuple[MobileReleasePlatform, str],
    signals: list[_TaskMobileReleaseSignals],
) -> PlanMobileAppReleaseReadinessRow:
    platform, release_area = key
    merged_signals = tuple(
        signal for signal in _SIGNAL_ORDER if any(signal in task.signals for task in signals)
    )
    gaps = _group_gaps(merged_signals, tuple(_dedupe(gap for task in signals for gap in task.gaps)))
    readiness = _readiness(merged_signals, gaps)
    sorted_signals = sorted(signals, key=lambda item: item.task_id)
    return PlanMobileAppReleaseReadinessRow(
        platform=platform,
        release_area=release_area,
        task_ids=tuple(_dedupe(signal.task_id for signal in sorted_signals)),
        titles=tuple(_dedupe(signal.title for signal in sorted_signals)),
        signals=merged_signals,
        gaps=gaps,
        readiness=readiness,
        severity=_severity(readiness, merged_signals, gaps),
        evidence=tuple(_dedupe(item for signal in signals for item in signal.evidence)),
    )


def _group_gaps(
    signals: tuple[MobileReleaseSignal, ...],
    task_gaps: tuple[str, ...],
) -> tuple[str, ...]:
    gaps = [
        gap
        for gap in task_gaps
        if not (
            (gap == "Missing phased rollout or staged rollout plan." and "phased_rollout" in signals)
            or (gap == "Missing crash, analytics, or release monitoring evidence." and "crash_analytics_monitoring" in signals)
            or (gap == "Missing rollback or hotfix path." and "rollback_hotfix_path" in signals)
        )
    ]
    if "app_store_review" not in signals:
        gaps.append("Missing App Store or Google Play review evidence.")
    if "phased_rollout" not in signals:
        gaps.append("Missing phased rollout or staged rollout plan.")
    if "crash_analytics_monitoring" not in signals:
        gaps.append("Missing crash, analytics, or release monitoring evidence.")
    if "rollback_hotfix_path" not in signals:
        gaps.append("Missing rollback or hotfix path.")
    return tuple(_dedupe(gaps))


def _readiness(
    signals: tuple[MobileReleaseSignal, ...],
    gaps: tuple[str, ...],
) -> MobileReleaseReadiness:
    required = {"app_store_review", "phased_rollout", "crash_analytics_monitoring", "rollback_hotfix_path"}
    if required.issubset(signals) and not gaps:
        return "ready"
    if "phased_rollout" not in signals or "rollback_hotfix_path" not in signals:
        return "blocked"
    return "partial"


def _severity(
    readiness: MobileReleaseReadiness,
    signals: tuple[MobileReleaseSignal, ...],
    gaps: tuple[str, ...],
) -> MobileReleaseSeverity:
    if readiness == "blocked":
        return "high"
    if readiness == "partial" or gaps:
        return "medium"
    return "low"


def _summary(
    task_count: int,
    rows: Iterable[PlanMobileAppReleaseReadinessRow],
    non_mobile_task_ids: Iterable[str],
) -> dict[str, Any]:
    row_list = list(rows)
    mobile_release_task_ids = tuple(_dedupe(task_id for row in row_list for task_id in row.task_ids))
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "mobile_release_task_count": len(mobile_release_task_ids),
        "non_mobile_task_count": len(tuple(non_mobile_task_ids)),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "severity_counts": {
            severity: sum(1 for row in row_list if row.severity == severity)
            for severity in _SEVERITY_ORDER
        },
        "platform_counts": {
            platform: sum(1 for row in row_list if row.platform == platform)
            for platform in sorted({row.platform for row in row_list})
        },
        "signal_counts": {
            signal: sum(1 for row in row_list if signal in row.signals)
            for signal in _SIGNAL_ORDER
        },
    }


def _row_sort_key(row: PlanMobileAppReleaseReadinessRow) -> tuple[int, int, str, str, str]:
    return (
        _SEVERITY_ORDER[row.severity],
        _READINESS_ORDER[row.readiness],
        row.platform,
        row.release_area,
        ",".join(row.task_ids),
    )


def _platform(context: str) -> MobileReleasePlatform:
    matched = tuple(platform for platform, pattern in _PLATFORM_PATTERNS if pattern.search(context))
    if len(matched) > 1:
        return "multi_platform"
    if matched:
        return matched[0]
    return "mobile"


def _signal_evidence(texts: Iterable[tuple[str, str]]) -> dict[MobileReleaseSignal, tuple[str, ...]]:
    evidence: dict[MobileReleaseSignal, tuple[str, ...]] = {}
    for signal, pattern in _SIGNAL_PATTERNS.items():
        evidence[signal] = tuple(
            _dedupe(
                _evidence_snippet(source_field, text)
                for source_field, text in texts
                if pattern.search(text) and not _negates_signal(signal, text)
            )
        )
    return evidence


def _negates_signal(signal: MobileReleaseSignal, text: str) -> bool:
    if not _EXPLICIT_GAP_RE.search(text):
        return False
    negative_patterns: dict[MobileReleaseSignal, re.Pattern[str]] = {
        "phased_rollout": re.compile(
            r"\b(?:rollout|phased|staged)\b.{0,80}\b(?:tbd|missing|not documented|not defined|unknown|unresolved)\b|"
            r"\b(?:tbd|missing|not documented|not defined|unknown|unresolved|without|no)\b.{0,80}\b(?:rollout|phased|staged)\b",
            re.I,
        ),
        "crash_analytics_monitoring": re.compile(
            r"\b(?:monitoring|analytics|crash|alert|dashboard)\b.{0,80}\b(?:tbd|missing|not documented|not defined|unknown|unresolved)\b|"
            r"\b(?:tbd|missing|not documented|not defined|unknown|unresolved|without|no)\b.{0,80}\b(?:monitoring|analytics|crash|alert|dashboard)\b",
            re.I,
        ),
        "rollback_hotfix_path": re.compile(
            r"\b(?:rollback|hotfix|previous build|expedited review)\b.{0,80}\b(?:tbd|missing|not documented|not defined|unknown|unresolved)\b|"
            r"\b(?:tbd|missing|not documented|not defined|unknown|unresolved|without|no)\b.{0,80}\b(?:rollback|hotfix|previous build|expedited review)\b",
            re.I,
        ),
    }
    pattern = negative_patterns.get(signal)
    return bool(pattern and pattern.search(text))


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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
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
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "risk_level",
        "test_command",
        "test_commands",
        "validation_command",
        "validation_commands",
        "status",
        "tags",
        "labels",
        "notes",
        "metadata",
        "blocked_reason",
        "tasks",
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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("depends_on", "dependencies", "files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes"):
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
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            elif key_text:
                texts.append((field, key_text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    if text := _optional_text(value):
        return [(prefix, text)]
    return []


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or _optional_text(task.get("task_id")) or f"task-{index}"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [_clean_text(value)] if value.strip() else []
    if isinstance(value, Mapping):
        return [
            text
            for key in sorted(value, key=lambda item: str(item))
            for text in _strings(value[key])
        ]
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return [text for item in items for text in _strings(item)]
    text = _optional_text(value)
    return [text] if text else []


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    return _SPACE_RE.sub(" ", value.strip())


def _evidence_snippet(source_field: str, text: str, *, limit: int = 180) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > limit:
        cleaned = f"{cleaned[: limit - 1].rstrip()}..."
    return f"{source_field}: {cleaned}" if source_field else cleaned


def _dedupe(values: Iterable[_T]) -> tuple[_T, ...]:
    seen: set[_T] = set()
    result: list[_T] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return tuple(result)


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")
