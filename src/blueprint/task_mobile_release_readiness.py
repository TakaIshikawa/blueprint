"""Plan mobile release readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


MobilePlatform = Literal["ios", "android", "react_native", "expo"]
MobileReleaseSignal = Literal[
    "ios",
    "android",
    "react_native",
    "expo",
    "app_store",
    "play_store",
    "testflight",
    "signing",
    "permissions",
    "push_notifications",
    "deep_links",
    "versioning",
    "crash_reporting",
]
MobileReadinessLevel = Literal[
    "ready",
    "needs_device_validation",
    "needs_release_safeguards",
    "needs_store_or_signing_plan",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_PLATFORM_ORDER: dict[MobilePlatform, int] = {
    "ios": 0,
    "android": 1,
    "react_native": 2,
    "expo": 3,
}
_SIGNAL_ORDER: dict[MobileReleaseSignal, int] = {
    "ios": 0,
    "android": 1,
    "react_native": 2,
    "expo": 3,
    "app_store": 4,
    "play_store": 5,
    "testflight": 6,
    "signing": 7,
    "permissions": 8,
    "push_notifications": 9,
    "deep_links": 10,
    "versioning": 11,
    "crash_reporting": 12,
}
_READINESS_ORDER: dict[MobileReadinessLevel, int] = {
    "needs_store_or_signing_plan": 0,
    "needs_release_safeguards": 1,
    "needs_device_validation": 2,
    "ready": 3,
}
_TEXT_SIGNAL_PATTERNS: dict[MobileReleaseSignal, re.Pattern[str]] = {
    "ios": re.compile(r"\b(?:ios|iphone|ipad|xcode|swift|testflight|app store)\b", re.I),
    "android": re.compile(
        r"\b(?:android|google play|play store|gradle|kotlin|apk|aab|androidmanifest)\b",
        re.I,
    ),
    "react_native": re.compile(r"\b(?:react native|react-native|\brn\b|metro bundler)\b", re.I),
    "expo": re.compile(r"\b(?:expo|eas build|eas submit|expo go|app\.config|app\.json)\b", re.I),
    "app_store": re.compile(r"\b(?:app store|app store connect|ios store submission|asc)\b", re.I),
    "play_store": re.compile(r"\b(?:play store|google play|play console|android store submission)\b", re.I),
    "testflight": re.compile(r"\b(?:testflight|test flight|internal testing|external testing)\b", re.I),
    "signing": re.compile(
        r"\b(?:signing|codesign|code signing|provisioning profile|certificate|keystore|"
        r"upload key|app signing|entitlements|eas credentials)\b",
        re.I,
    ),
    "permissions": re.compile(
        r"\b(?:permissions?|privacy manifest|info\.plist|androidmanifest|camera|"
        r"microphone|location|bluetooth|contacts|photo library|notification permission)\b",
        re.I,
    ),
    "push_notifications": re.compile(
        r"\b(?:push notifications?|apns|fcm|firebase cloud messaging|notification token|"
        r"device token)\b",
        re.I,
    ),
    "deep_links": re.compile(
        r"\b(?:deep links?|deeplinks?|universal links?|app links?|url scheme|intent filter)\b",
        re.I,
    ),
    "versioning": re.compile(
        r"\b(?:app version|build number|version code|versioncode|version name|versionname|cfbundleversion|"
        r"cfbundleshortversionstring|release version)\b",
        re.I,
    ),
    "crash_reporting": re.compile(
        r"\b(?:crash|crash reporting|crashlytics|sentry|bugsnag|symbolication|dsym|proguard|"
        r"mapping file|native crash)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class TaskMobileReleaseReadinessRecord:
    """Mobile release readiness guidance for one execution task."""

    task_id: str
    title: str
    platform: tuple[MobilePlatform, ...]
    readiness_level: MobileReadinessLevel
    detected_signals: tuple[MobileReleaseSignal, ...]
    release_safeguards: tuple[str, ...] = field(default_factory=tuple)
    store_or_signing_checks: tuple[str, ...] = field(default_factory=tuple)
    device_validation_recommendations: tuple[str, ...] = field(default_factory=tuple)
    rollback_constraints: tuple[str, ...] = field(default_factory=tuple)
    open_questions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "platform": list(self.platform),
            "readiness_level": self.readiness_level,
            "detected_signals": list(self.detected_signals),
            "release_safeguards": list(self.release_safeguards),
            "store_or_signing_checks": list(self.store_or_signing_checks),
            "device_validation_recommendations": list(self.device_validation_recommendations),
            "rollback_constraints": list(self.rollback_constraints),
            "open_questions": list(self.open_questions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskMobileReleaseReadinessPlan:
    """Plan-level mobile release readiness review."""

    plan_id: str | None = None
    records: tuple[TaskMobileReleaseReadinessRecord, ...] = field(default_factory=tuple)
    mobile_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_signal_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "mobile_task_ids": list(self.mobile_task_ids),
            "no_signal_task_ids": list(self.no_signal_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return mobile release readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the mobile release readiness plan as deterministic Markdown."""
        title = "# Task Mobile Release Readiness Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No mobile release readiness records were inferred."])
            if self.no_signal_task_ids:
                lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Platform | Readiness | Store / Signing Checks | Device Validation | Rollback |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{_markdown_cell(', '.join(record.platform))} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell('; '.join(record.store_or_signing_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.device_validation_recommendations) or 'none')} | "
                f"{_markdown_cell('; '.join(record.rollback_constraints) or 'none')} |"
            )
        if self.no_signal_task_ids:
            lines.extend(["", f"No-signal tasks: {_markdown_cell(', '.join(self.no_signal_task_ids))}"])
        return "\n".join(lines)


def build_task_mobile_release_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskMobileReleaseReadinessPlan:
    """Build release readiness guidance for mobile-related execution tasks."""
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
    readiness_counts = {
        level: sum(1 for record in records if record.readiness_level == level)
        for level in _READINESS_ORDER
    }
    signal_counts = {
        signal: sum(1 for record in records if signal in record.detected_signals)
        for signal in _SIGNAL_ORDER
    }
    platform_counts = {
        platform: sum(1 for record in records if platform in record.platform)
        for platform in _PLATFORM_ORDER
    }
    return TaskMobileReleaseReadinessPlan(
        plan_id=plan_id,
        records=records,
        mobile_task_ids=tuple(record.task_id for record in records),
        no_signal_task_ids=no_signal_task_ids,
        summary={
            "task_count": len(tasks),
            "mobile_task_count": len(records),
            "no_signal_task_count": len(no_signal_task_ids),
            "readiness_counts": readiness_counts,
            "signal_counts": signal_counts,
            "platform_counts": platform_counts,
        },
    )


def analyze_task_mobile_release_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskMobileReleaseReadinessPlan:
    """Compatibility alias for building mobile release readiness plans."""
    return build_task_mobile_release_readiness_plan(source)


def summarize_task_mobile_release_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskMobileReleaseReadinessPlan:
    """Compatibility alias for building mobile release readiness plans."""
    return build_task_mobile_release_readiness_plan(source)


def summarize_task_mobile_release_readiness_plans(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskMobileReleaseReadinessPlan:
    """Compatibility alias for building mobile release readiness plans."""
    return build_task_mobile_release_readiness_plan(source)


def task_mobile_release_readiness_plan_to_dict(
    result: TaskMobileReleaseReadinessPlan,
) -> dict[str, Any]:
    """Serialize a mobile release readiness plan to a plain dictionary."""
    return result.to_dict()


task_mobile_release_readiness_plan_to_dict.__test__ = False


def task_mobile_release_readiness_plan_to_markdown(
    result: TaskMobileReleaseReadinessPlan,
) -> str:
    """Render a mobile release readiness plan as Markdown."""
    return result.to_markdown()


task_mobile_release_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[MobileReleaseSignal, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    validation_commands: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskMobileReleaseReadinessRecord | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals.signals:
        return None
    platforms = _platforms(signals.signals)
    readiness = _readiness_level(signals)
    return TaskMobileReleaseReadinessRecord(
        task_id=task_id,
        title=title,
        platform=platforms,
        readiness_level=readiness,
        detected_signals=signals.signals,
        release_safeguards=_release_safeguards(signals.signals, signals.validation_commands),
        store_or_signing_checks=_store_or_signing_checks(signals.signals),
        device_validation_recommendations=_device_validation(platforms, signals.signals),
        rollback_constraints=_rollback_constraints(signals.signals),
        open_questions=_open_questions(platforms, signals.signals, readiness),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signals: set[MobileReleaseSignal] = set()
    evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_signals = _path_signals(normalized)
        if path_signals:
            signals.update(path_signals)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signals.add(signal)
                evidence.append(snippet)

    validation_commands = tuple(_validation_commands(task))
    for command in validation_commands:
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(command) or pattern.search(command_text):
                signals.add(signal)
                evidence.append(snippet)

    ordered = tuple(signal for signal in _SIGNAL_ORDER if signal in signals)
    return _Signals(
        signals=ordered,
        evidence=tuple(_dedupe(evidence)),
        validation_commands=validation_commands,
    )


def _path_signals(path: str) -> set[MobileReleaseSignal]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals: set[MobileReleaseSignal] = set()
    if {"ios", "iphone", "ipad"} & parts or name in {"info.plist", "exportoptions.plist"}:
        signals.add("ios")
    if {"android"} & parts or name in {"androidmanifest.xml", "build.gradle", "gradle.properties"}:
        signals.add("android")
    if "react-native" in normalized or "react_native" in normalized or {"rn"} & parts:
        signals.add("react_native")
    if "expo" in parts or name in {"app.json", "app.config.js", "app.config.ts", "eas.json"}:
        signals.add("expo")
    if any(token in text for token in ("app store", "appstore", "app store connect")):
        signals.add("app_store")
    if any(token in text for token in ("play store", "google play", "play console")):
        signals.add("play_store")
    if "testflight" in text or "test flight" in text:
        signals.add("testflight")
    if any(token in text for token in ("signing", "keystore", "provisioning", "certificate", "entitlements")):
        signals.add("signing")
    if name in {"info.plist", "androidmanifest.xml", "privacyinfo.xcprivacy"} or "permission" in text:
        signals.add("permissions")
    if any(token in text for token in ("push", "apns", "fcm", "notifications")):
        signals.add("push_notifications")
    if any(token in text for token in ("deep link", "deeplink", "universal link", "app link", "url scheme")):
        signals.add("deep_links")
    if any(token in text for token in ("version", "build number", "version code")):
        signals.add("versioning")
    if any(token in text for token in ("crashlytics", "sentry", "bugsnag", "dsym", "proguard", "mapping")):
        signals.add("crash_reporting")
    return signals


def _platforms(signals: tuple[MobileReleaseSignal, ...]) -> tuple[MobilePlatform, ...]:
    platforms: set[MobilePlatform] = set()
    if "ios" in signals or "app_store" in signals or "testflight" in signals:
        platforms.add("ios")
    if "android" in signals or "play_store" in signals:
        platforms.add("android")
    if "react_native" in signals:
        platforms.add("react_native")
    if "expo" in signals:
        platforms.update(("ios", "android", "expo", "react_native"))
    if not platforms and {"push_notifications", "deep_links", "permissions", "crash_reporting"} & set(signals):
        platforms.update(("ios", "android"))
    return tuple(platform for platform in _PLATFORM_ORDER if platform in platforms)


def _readiness_level(signals: _Signals) -> MobileReadinessLevel:
    detected = set(signals.signals)
    has_validation = bool(signals.validation_commands)
    has_store_or_signing = bool(
        {"app_store", "play_store", "testflight", "signing", "versioning"} & detected
    )
    has_release_surface = bool(
        {"permissions", "push_notifications", "deep_links", "crash_reporting"} & detected
    )
    if has_store_or_signing and not has_validation:
        return "needs_store_or_signing_plan"
    if has_store_or_signing:
        return "needs_release_safeguards"
    if has_release_surface and not has_validation:
        return "needs_device_validation"
    return "ready"


def _release_safeguards(
    signals: tuple[MobileReleaseSignal, ...],
    validation_commands: tuple[str, ...],
) -> tuple[str, ...]:
    safeguards = [
        "Confirm release owner, target app version, build number, and release channel before implementation starts.",
        "Capture mobile QA sign-off against production-like builds before store or staged rollout.",
    ]
    if {"app_store", "play_store", "testflight", "signing", "versioning"} & set(signals):
        safeguards.append(
            "Prepare store submission, signing credentials, build metadata, review notes, and staged rollout gates."
        )
    if "permissions" in signals:
        safeguards.append(
            "Verify permission prompts, platform privacy declarations, and denied-permission behavior."
        )
    if "push_notifications" in signals:
        safeguards.append("Validate APNs or FCM credentials, token refresh, foreground, background, and opt-out behavior.")
    if "deep_links" in signals:
        safeguards.append("Validate universal links, app links, URL schemes, cold start, and unauthenticated routing.")
    if "crash_reporting" in signals:
        safeguards.append("Confirm crash reporting, symbol uploads, and release health alerts for the new build.")
    if validation_commands:
        safeguards.append("Run the detected validation commands against release builds before submission.")
    return tuple(_dedupe(safeguards))


def _store_or_signing_checks(signals: tuple[MobileReleaseSignal, ...]) -> tuple[str, ...]:
    checks: list[str] = []
    if "ios" in signals or "app_store" in signals or "testflight" in signals:
        checks.extend(
            [
                "Check App Store Connect metadata, privacy nutrition labels, review notes, and TestFlight tester scope.",
                "Verify iOS bundle identifier, entitlements, provisioning profiles, certificates, and build number.",
            ]
        )
    if "android" in signals or "play_store" in signals:
        checks.extend(
            [
                "Check Play Console listing, data safety form, release track, staged rollout percentage, and review notes.",
                "Verify Android applicationId, versionCode, signing key, upload key, AAB generation, and Play App Signing status.",
            ]
        )
    if "expo" in signals:
        checks.append("Verify Expo EAS build profile, credentials, runtime version, update channel, and submit profile.")
    if "react_native" in signals and "expo" not in signals:
        checks.append("Verify React Native native project changes are included in both iOS and Android release builds.")
    if "signing" in signals and not checks:
        checks.append("Identify the required mobile signing credentials and rotation or recovery path.")
    if "versioning" in signals:
        checks.append("Confirm semantic app version, build number or versionCode monotonicity, and release notes.")
    return tuple(_dedupe(checks))


def _device_validation(
    platforms: tuple[MobilePlatform, ...],
    signals: tuple[MobileReleaseSignal, ...],
) -> tuple[str, ...]:
    matrix: list[str] = []
    platform_set = set(platforms)
    if "ios" in platform_set or "expo" in platform_set or "react_native" in platform_set:
        matrix.append("Test current and previous supported iOS versions on iPhone small, iPhone large, and iPad where supported.")
    if "android" in platform_set or "expo" in platform_set or "react_native" in platform_set:
        matrix.append("Test current and previous supported Android versions across low-memory, mid-tier, and flagship devices.")
    if "permissions" in signals:
        matrix.append("Exercise first-run, denied, limited, revoked, and settings-change permission states.")
    if "push_notifications" in signals:
        matrix.append("Exercise fresh install, upgraded install, logged-out, foreground, background, and terminated push states.")
    if "deep_links" in signals:
        matrix.append("Exercise cold start, warm app, installed, not-installed, authenticated, and unauthenticated deep-link flows.")
    if "crash_reporting" in signals:
        matrix.append("Trigger non-fatal and crash test events and verify symbolicated reporting per platform.")
    return tuple(_dedupe(matrix))


def _rollback_constraints(signals: tuple[MobileReleaseSignal, ...]) -> tuple[str, ...]:
    constraints = [
        "Mobile binaries cannot be instantly rolled back after store approval; prepare staged rollout pause and hotfix paths.",
    ]
    if "expo" in signals:
        constraints.append("Expo OTA updates can mitigate JavaScript issues only when the native runtime version remains compatible.")
    if "react_native" in signals:
        constraints.append("React Native native module changes require a new binary and cannot be fixed by JavaScript rollback alone.")
    if "permissions" in signals:
        constraints.append("Permission copy or entitlement mistakes may require store review and cannot be silently repaired for existing installs.")
    if "push_notifications" in signals:
        constraints.append("Push credential or payload regressions can affect already-installed clients across active app versions.")
    if "deep_links" in signals:
        constraints.append("Deep-link association mistakes may be cached by the OS and need server and binary coordination.")
    return tuple(_dedupe(constraints))


def _open_questions(
    platforms: tuple[MobilePlatform, ...],
    signals: tuple[MobileReleaseSignal, ...],
    readiness: MobileReadinessLevel,
) -> tuple[str, ...]:
    questions = [
        "Which release owner will approve store submission, staged rollout, and release-health monitoring?",
        "Which app versions, OS versions, device classes, and upgrade paths are in scope for validation?",
    ]
    if readiness != "ready":
        questions.append("What validation command or manual QA evidence will block submission until passing?")
    if {"app_store", "play_store", "testflight", "signing", "versioning"} & set(signals):
        questions.append("Are signing credentials, store metadata, privacy declarations, and review notes already available?")
    if "expo" in platforms:
        questions.append("Does the change require a new Expo runtime binary or can it ship through an OTA update channel?")
    if "push_notifications" in signals:
        questions.append("Which APNs or FCM environment, topics, and payload variants must be tested?")
    if "deep_links" in signals:
        questions.append("Which domains, schemes, and route parameters must remain backward compatible?")
    return tuple(_dedupe(questions))


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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes"):
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
                if any(pattern.search(key_text) for pattern in _TEXT_SIGNAL_PATTERNS.values()):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in _TEXT_SIGNAL_PATTERNS.values()):
                texts.append((field, str(key)))
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
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "MobilePlatform",
    "MobileReadinessLevel",
    "MobileReleaseSignal",
    "TaskMobileReleaseReadinessPlan",
    "TaskMobileReleaseReadinessRecord",
    "analyze_task_mobile_release_readiness",
    "build_task_mobile_release_readiness_plan",
    "summarize_task_mobile_release_readiness",
    "summarize_task_mobile_release_readiness_plans",
    "task_mobile_release_readiness_plan_to_dict",
    "task_mobile_release_readiness_plan_to_markdown",
]
