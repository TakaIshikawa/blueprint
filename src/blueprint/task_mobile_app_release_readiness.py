"""Assess mobile app release readiness for execution tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import (
    SimpleReadinessPlan,
    SimpleReadinessRecord,
    build_simple_readiness_plan,
)


TaskMobileAppReleaseReadinessPlan = SimpleReadinessPlan
TaskMobileAppReleaseReadinessRecord = SimpleReadinessRecord

_SIGNALS = {
    "mobile_app_release": re.compile(
        r"\b(?:mobile app release|app release|ios release|android release|app store|play store|"
        r"testflight|google play|mobile rollout|release build|production build)\b",
        re.I,
    ),
    "mobile_platform": re.compile(
        r"\b(?:ios|iphone|ipad|android|react native|react-native|expo|apk|aab|xcode|gradle)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "mobile_app_release": re.compile(r"appstore|app_store|playstore|play_store|testflight|release|fastlane", re.I),
    "mobile_platform": re.compile(r"ios|android|mobile|expo|eas|xcode|gradle|react.?native", re.I),
}
_CRITERIA = {
    "platform_scope": re.compile(
        r"\b(?:ios|iphone|ipad|android|react native|react-native|expo|cross[- ]platform|both platforms|"
        r"platform scope|target platforms?)\b",
        re.I,
    ),
    "store_submission_steps": re.compile(
        r"\b(?:(?:app store|app store connect|play store|google play|play console|testflight|store submission|review notes|"
        r"fastlane|eas submit).{0,80}(?:submit|submission|review|metadata|listing|track|tester|approval|steps?)|"
        r"(?:submit|submission|review|metadata|listing|track|tester|approval|steps?).{0,80}(?:app store|app store connect|play store|google play|play console|testflight|store))\b",
        re.I,
    ),
    "version_build_numbers": re.compile(
        r"\b(?:version(?: name| code)?|build number|build id|cfbundleversion|cfbundleshortversionstring|"
        r"versioncode|versionname|semantic version|release number)\b",
        re.I,
    ),
    "phased_rollout": re.compile(
        r"\b(?:phased rollout|staged rollout|gradual rollout|rollout percentage|release track|production track|"
        r"\d+% rollout|increase to \d+%|pause rollout|ramp plan)\b",
        re.I,
    ),
    "device_os_test_matrix": re.compile(
        r"\b(?:(?:device|os|operating system|ios|android).{0,80}(?:matrix|coverage|versions?|devices?|upgrade path|"
        r"regression|qa)|(?:matrix|coverage|versions?|devices?|upgrade path).{0,80}(?:device|os|ios|android)|"
        r"test matrix|supported devices|supported os)\b",
        re.I,
    ),
    "crash_analytics_monitoring": re.compile(
        r"\b(?:(?:crash|analytics|crashlytics|sentry|bugsnag|firebase|monitoring|dashboard|alert).{0,80}"
        r"(?:release|build|crash|analytics|monitoring|dashboard|alert|health)|"
        r"release health|crash-free|crash free|symbolication|dsym|proguard mapping)\b",
        re.I,
    ),
    "rollback_hotfix_path": re.compile(
        r"\b(?:(?:rollback|roll back|hotfix|fix forward|pause rollout|revert|expedited review).{0,80}"
        r"(?:path|plan|steps?|trigger|store|binary|release)|"
        r"(?:path|plan|steps?|trigger).{0,80}(?:rollback|hotfix|fix forward|pause rollout)|"
        r"emergency patch|new binary)\b",
        re.I,
    ),
    "stakeholder_owner": re.compile(
        r"\b(?:(?:owner|dri|release manager|mobile lead|qa lead|product owner|stakeholder|approver|on[- ]call).{0,80}"
        r"(?:approve|sign[- ]off|responsib|handoff|owner|release)|"
        r"(?:approve|sign[- ]off|responsib|handoff).{0,80}(?:owner|dri|release manager|mobile lead|qa lead|stakeholder))\b",
        re.I,
    ),
    "release_evidence": re.compile(
        r"\b(?:(?:evidence|artifact|qa sign[- ]off|test result|validation|screenshot|store receipt|submission receipt).{0,80}"
        r"(?:release|build|store|qa|pass|attached|artifact)|"
        r"(?:release|build|store|qa).{0,80}(?:evidence|artifact|sign[- ]off|test result|validation|receipt)|"
        r"release checklist|validation artifact)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "platform_scope": "Identify the mobile platform scope, including iOS, Android, Expo, React Native, or supported device families.",
    "store_submission_steps": "Document App Store, TestFlight, Play Console, track, metadata, review-note, and submission steps.",
    "version_build_numbers": "Specify app version, build number, versionCode, versionName, or other monotonic release identifiers.",
    "phased_rollout": "Define phased or staged rollout percentages, tracks, pause criteria, and ramp checkpoints.",
    "device_os_test_matrix": "Provide the device and OS test matrix, including supported versions, device classes, and upgrade paths.",
    "crash_analytics_monitoring": "Add crash reporting, analytics, release-health dashboards, alerts, and symbol upload checks.",
    "rollback_hotfix_path": "Define rollback limits, rollout pause behavior, hotfix path, expedited review, and fix-forward triggers.",
    "stakeholder_owner": "Name the release owner, mobile lead, QA approver, stakeholder, or on-call role responsible for approval.",
    "release_evidence": "Capture release evidence such as QA sign-off, validation artifacts, store receipts, screenshots, or test results.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:mobile app release|app store|play store|ios release|android release|mobile rollout)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)


def build_task_mobile_app_release_readiness_plan(source: Any) -> TaskMobileAppReleaseReadinessPlan:
    """Build criterion-level readiness findings for mobile app release tasks."""
    return build_simple_readiness_plan(
        source,
        title="Task Mobile App Release Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_mobile_app_release_readiness(source: Any) -> TaskMobileAppReleaseReadinessPlan:
    return build_task_mobile_app_release_readiness_plan(source)


def summarize_task_mobile_app_release_readiness(source: Any) -> TaskMobileAppReleaseReadinessPlan:
    return build_task_mobile_app_release_readiness_plan(source)


def extract_task_mobile_app_release_readiness(source: Any) -> TaskMobileAppReleaseReadinessPlan:
    return build_task_mobile_app_release_readiness_plan(source)


def generate_task_mobile_app_release_readiness(source: Any) -> TaskMobileAppReleaseReadinessPlan:
    return build_task_mobile_app_release_readiness_plan(source)


def recommend_task_mobile_app_release_readiness(source: Any) -> TaskMobileAppReleaseReadinessPlan:
    return build_task_mobile_app_release_readiness_plan(source)


def task_mobile_app_release_readiness_plan_to_dict(
    result: TaskMobileAppReleaseReadinessPlan,
) -> dict[str, Any]:
    return result.to_dict()


task_mobile_app_release_readiness_plan_to_dict.__test__ = False


def task_mobile_app_release_readiness_plan_to_dicts(result: Any) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [item.to_dict() for item in result]


task_mobile_app_release_readiness_plan_to_dicts.__test__ = False


def task_mobile_app_release_readiness_plan_to_markdown(
    result: TaskMobileAppReleaseReadinessPlan,
) -> str:
    return result.to_markdown()


task_mobile_app_release_readiness_plan_to_markdown.__test__ = False
