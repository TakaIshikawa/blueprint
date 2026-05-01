import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_mobile_release_readiness import (
    TaskMobileReleaseReadinessPlan,
    TaskMobileReleaseReadinessRecord,
    analyze_task_mobile_release_readiness,
    build_task_mobile_release_readiness_plan,
    summarize_task_mobile_release_readiness,
    summarize_task_mobile_release_readiness_plans,
    task_mobile_release_readiness_plan_to_dict,
    task_mobile_release_readiness_plan_to_markdown,
)


def test_ios_store_signing_permissions_push_deep_links_and_crash_reporting_are_detected():
    result = build_task_mobile_release_readiness_plan(
        _plan(
            [
                _task(
                    "task-ios-release",
                    title="Prepare iOS App Store release for push and deep links",
                    description=(
                        "Update iOS permissions in Info.plist, APNs push notifications, "
                        "universal links, TestFlight, App Store Connect, signing certificate, "
                        "app version, dSYM symbolication, and Sentry crash reporting."
                    ),
                    files_or_modules=[
                        "ios/Runner/Info.plist",
                        "ios/fastlane/AppStoreConnect.release.yml",
                    ],
                    acceptance_criteria=[
                        "QA validates denied location permission, push opt-out, and cold-start deep links."
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskMobileReleaseReadinessPlan)
    assert result.plan_id == "plan-mobile"
    assert result.mobile_task_ids == ("task-ios-release",)
    record = result.records[0]
    assert isinstance(record, TaskMobileReleaseReadinessRecord)
    assert record.platform == ("ios",)
    assert record.readiness_level == "needs_store_or_signing_plan"
    assert record.detected_signals == (
        "ios",
        "app_store",
        "testflight",
        "signing",
        "permissions",
        "push_notifications",
        "deep_links",
        "versioning",
        "crash_reporting",
    )
    assert any("App Store Connect metadata" in value for value in record.store_or_signing_checks)
    assert any("provisioning profiles" in value for value in record.store_or_signing_checks)
    assert any("iPhone small" in value for value in record.device_validation_recommendations)
    assert any("Mobile binaries cannot be instantly rolled back" in value for value in record.rollback_constraints)
    assert any("validation command" in value for value in record.open_questions)
    assert record.evidence[:2] == (
        "files_or_modules: ios/Runner/Info.plist",
        "files_or_modules: ios/fastlane/AppStoreConnect.release.yml",
    )
    assert result.summary["signal_counts"]["push_notifications"] == 1
    assert result.summary["platform_counts"]["ios"] == 1


def test_mixed_android_react_native_and_expo_plan_tracks_platforms_and_no_signal_tasks():
    result = analyze_task_mobile_release_readiness(
        _plan(
            [
                _task(
                    "task-android",
                    title="Submit Android Play Store version",
                    description=(
                        "Build Android AAB with new versionCode, verify keystore signing, "
                        "and publish to the Play Console staged rollout."
                    ),
                    files_or_modules=["android/app/build.gradle", "android/app/src/main/AndroidManifest.xml"],
                    test_command="poetry run pytest tests/mobile/test_android_release.py",
                ),
                _task(
                    "task-expo",
                    title="Ship Expo React Native push update",
                    description=(
                        "Use Expo EAS build and submit profiles for React Native. "
                        "Validate FCM push notifications and app links."
                    ),
                    files_or_modules=["app.json", "eas.json", "src/mobile/deep_links.ts"],
                ),
                _task(
                    "task-api",
                    title="Add account endpoint",
                    description="Create backend route for account reads.",
                    files_or_modules=["src/api/accounts.py"],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}

    assert result.mobile_task_ids == ("task-android", "task-expo")
    assert result.no_signal_task_ids == ("task-api",)
    assert by_id["task-android"].platform == ("android",)
    assert by_id["task-android"].readiness_level == "needs_release_safeguards"
    assert by_id["task-android"].detected_signals == (
        "android",
        "play_store",
        "signing",
        "permissions",
        "versioning",
    )
    assert any("Play Console listing" in value for value in by_id["task-android"].store_or_signing_checks)
    assert by_id["task-expo"].platform == ("ios", "android", "react_native", "expo")
    assert "expo" in by_id["task-expo"].detected_signals
    assert "react_native" in by_id["task-expo"].detected_signals
    assert any("Expo EAS build profile" in value for value in by_id["task-expo"].store_or_signing_checks)
    assert any("Android versions" in value for value in by_id["task-expo"].device_validation_recommendations)
    assert result.summary["mobile_task_count"] == 2
    assert result.summary["no_signal_task_count"] == 1


def test_empty_invalid_and_no_signal_inputs_are_stable():
    no_signal = build_task_mobile_release_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="Adjust labels and helper text.",
                    files_or_modules=["src/blueprint/ui/dashboard_copy.py"],
                )
            ]
        )
    )
    empty = build_task_mobile_release_readiness_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_mobile_release_readiness_plan(17)

    assert no_signal.records == ()
    assert no_signal.mobile_task_ids == ()
    assert no_signal.no_signal_task_ids == ("task-copy",)
    assert "No-signal tasks: task-copy" in no_signal.to_markdown()
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert empty.no_signal_task_ids == ()
    assert empty.summary["task_count"] == 0
    assert "No mobile release readiness records" in empty.to_markdown()
    assert invalid.plan_id is None
    assert invalid.records == ()
    assert invalid.summary["task_count"] == 0


def test_serialization_markdown_deduplication_and_aliases_are_deterministic():
    task_dict = _task(
        "task-pipes",
        title="React Native release | signing",
        description="React Native Android signing and Play Store signing.",
        files_or_modules={
            "main": "android/app/release-signing.gradle",
            "duplicate": "android/app/release-signing.gradle",
            "none": None,
        },
        acceptance_criteria={
            "qa": "Run app version and crash reporting checks before Play Store submission."
        },
        metadata={
            "deep_links": [{"route": "app links and deep links"}, None, 7],
            "validation_commands": {"test": ["poetry run pytest tests/mobile/test_release.py"]},
        },
        test_command="poetry run pytest tests/mobile/test_release.py",
    )
    original = copy.deepcopy(task_dict)

    result = summarize_task_mobile_release_readiness(_plan([task_dict]))
    payload = task_mobile_release_readiness_plan_to_dict(result)
    markdown = task_mobile_release_readiness_plan_to_markdown(result)
    alias = summarize_task_mobile_release_readiness_plans(_plan([task_dict]))
    record = result.records[0]

    assert task_dict == original
    assert result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "records",
        "mobile_task_ids",
        "no_signal_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "platform",
        "readiness_level",
        "detected_signals",
        "release_safeguards",
        "store_or_signing_checks",
        "device_validation_recommendations",
        "rollback_constraints",
        "open_questions",
        "evidence",
    ]
    assert payload["summary"]["task_count"] == 1
    assert payload["summary"]["mobile_task_count"] == 1
    assert len(record.evidence) == len(set(record.evidence))
    assert record.evidence.count("files_or_modules: android/app/release-signing.gradle") == 1
    assert record.readiness_level == "needs_release_safeguards"
    assert alias.to_dict() == result.to_dict()
    assert markdown.startswith("# Task Mobile Release Readiness Plan: plan-mobile")
    assert "React Native release \\| signing" in markdown


def test_execution_plan_model_and_object_like_task_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add mobile crash reporting",
        description="Enable Crashlytics and upload dSYM and ProGuard mapping files.",
        files_or_modules=["src/mobile/crash_reporting.ts"],
        acceptance_criteria=["Verify symbolication on iOS and Android."],
        metadata={"validation_commands": {"test": ["poetry run pytest tests/mobile/test_crash.py"]}},
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Add TestFlight deep links",
            description="Update iOS universal links for TestFlight build.",
            files_or_modules=["ios/App/AppDelegate.swift"],
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan([task_model.model_dump(mode="python")], plan_id="plan-model")
    )

    first = build_task_mobile_release_readiness_plan([object_task])
    second = build_task_mobile_release_readiness_plan(plan_model)

    assert first.records[0].task_id == "task-object"
    assert first.records[0].platform == ("ios", "android")
    assert first.records[0].readiness_level == "ready"
    assert "validation_commands: poetry run pytest tests/mobile/test_crash.py" in first.records[0].evidence
    assert second.plan_id == "plan-model"
    assert second.records[0].task_id == "task-model"
    assert second.records[0].platform == ("ios",)


def _plan(tasks, plan_id="plan-mobile"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-mobile",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    test_command=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if test_command is not None:
        task["test_command"] = test_command
    return task
