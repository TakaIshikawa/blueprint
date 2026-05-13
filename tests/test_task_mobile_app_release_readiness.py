import json

from blueprint.task_mobile_app_release_readiness import (
    TaskMobileAppReleaseReadinessPlan,
    TaskMobileAppReleaseReadinessRecord,
    analyze_task_mobile_app_release_readiness,
    build_task_mobile_app_release_readiness_plan,
    extract_task_mobile_app_release_readiness,
    generate_task_mobile_app_release_readiness,
    recommend_task_mobile_app_release_readiness,
    task_mobile_app_release_readiness_plan_to_dict,
    task_mobile_app_release_readiness_plan_to_dicts,
    task_mobile_app_release_readiness_plan_to_markdown,
)


def test_ready_mobile_app_release_plan_has_all_criteria():
    result = build_task_mobile_app_release_readiness_plan(
        _plan(
            [
                _task(
                    "mobile-ready",
                    "Prepare iOS and Android app release",
                    (
                        "Platform scope includes iOS and Android React Native production builds. "
                        "App Store Connect, TestFlight, and Play Console submission steps include metadata, review notes, and tracks. "
                        "Version 4.8.0 uses iOS build number 382 and Android versionCode 382. "
                        "Staged rollout starts at 5% then increases to 25% and 100% with pause criteria. "
                        "Device and OS test matrix covers supported iOS versions, Android versions, tablets, and upgrade paths. "
                        "Crashlytics, analytics monitoring, release health dashboard, alerts, dSYM, and ProGuard mapping are checked. "
                        "Rollback and hotfix path pauses rollout, requests expedited review, and ships a new binary if needed. "
                        "Mobile lead owner, QA lead approver, and product stakeholder sign-off are required. "
                        "Release evidence includes QA sign-off, validation artifacts, screenshots, and store submission receipts."
                    ),
                    files_or_modules=["ios/fastlane/AppStore.release.yml", "android/fastlane/PlayStore.release.yml"],
                )
            ]
        )
    )

    record = result.records[0]

    assert isinstance(result, TaskMobileAppReleaseReadinessPlan)
    assert isinstance(record, TaskMobileAppReleaseReadinessRecord)
    assert record.readiness == "ready"
    assert record.present_criteria == (
        "platform_scope",
        "store_submission_steps",
        "version_build_numbers",
        "phased_rollout",
        "device_os_test_matrix",
        "crash_analytics_monitoring",
        "rollback_hotfix_path",
        "stakeholder_owner",
        "release_evidence",
    )
    assert record.missing_criteria == ()
    assert result.impacted_task_ids == ("mobile-ready",)


def test_partial_mobile_app_release_plan_reports_missing_release_controls():
    result = analyze_task_mobile_app_release_readiness(
        _plan(
            [
                _task(
                    "mobile-partial",
                    "Submit Android app release",
                    (
                        "Android Play Store submission will update the store listing and review notes. "
                        "VersionName 4.8.0 and versionCode 382 are set."
                    ),
                )
            ]
        )
    )

    record = result.records[0]

    assert record.readiness == "partial"
    assert record.present_criteria == (
        "platform_scope",
        "store_submission_steps",
        "version_build_numbers",
    )
    assert record.missing_criteria == (
        "phased_rollout",
        "device_os_test_matrix",
        "crash_analytics_monitoring",
        "rollback_hotfix_path",
        "stakeholder_owner",
        "release_evidence",
    )
    assert record.recommended_follow_up_actions == (
        "Define phased or staged rollout percentages, tracks, pause criteria, and ramp checkpoints.",
        "Provide the device and OS test matrix, including supported versions, device classes, and upgrade paths.",
        "Add crash reporting, analytics, release-health dashboards, alerts, and symbol upload checks.",
        "Define rollback limits, rollout pause behavior, hotfix path, expedited review, and fix-forward triggers.",
        "Name the release owner, mobile lead, QA approver, stakeholder, or on-call role responsible for approval.",
        "Capture release evidence such as QA sign-off, validation artifacts, store receipts, screenshots, or test results.",
    )
    assert any("Play Store submission" in item for item in record.evidence)


def test_absent_mobile_app_release_plan_is_ignored_and_serializes():
    plan = _plan(
        [
            _task("mobile-no-impact", "Update labels", "No mobile app release changes are in scope."),
            _task("api-docs", "Edit API docs", "Adjust account endpoint examples."),
        ]
    )

    result = recommend_task_mobile_app_release_readiness(plan)
    payload = task_mobile_app_release_readiness_plan_to_dict(result)
    markdown = task_mobile_app_release_readiness_plan_to_markdown(result)

    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.ignored_task_ids == ("mobile-no-impact", "api-docs")
    assert result.summary["impacted_task_count"] == 0
    assert json.loads(json.dumps(payload)) == payload
    assert task_mobile_app_release_readiness_plan_to_dicts(result) == []
    assert extract_task_mobile_app_release_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_mobile_app_release_readiness(plan).to_dict() == result.to_dict()
    assert markdown.startswith("# Task Mobile App Release Readiness")
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "impacted_task_ids",
        "ignored_task_ids",
        "summary",
    ]


def _plan(tasks):
    return {"id": "plan-mobile-app-release", "tasks": tasks}


def _task(task_id, title, description, **extra):
    return {"id": task_id, "title": title, "description": description, **extra}
