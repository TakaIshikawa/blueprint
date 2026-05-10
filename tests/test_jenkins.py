"""Tests for Jenkins integration."""

from __future__ import annotations

import pytest

from blueprint.cicd.jenkins import (
    JenkinsBuildStatus,
    JenkinsIntegration,
    extract_task_id_from_job,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def integration() -> JenkinsIntegration:
    return JenkinsIntegration()


BUILD_SUCCESS = {
    "name": "task_TASK100_build",
    "build": {
        "number": 42,
        "phase": "FINALIZED",
        "status": "SUCCESS",
        "full_url": "https://jenkins.example.com/job/my-job/42/",
        "duration": 30000,
        "parameters": [
            {"name": "BRANCH", "value": "main"},
        ],
    },
}

BUILD_UNSTABLE = {
    "name": "task_TASK200_test",
    "build": {
        "number": 43,
        "phase": "FINALIZED",
        "status": "UNSTABLE",
        "duration": 45000,
    },
}

BUILD_FAILURE = {
    "name": "my-job",
    "build": {
        "number": 44,
        "phase": "FINALIZED",
        "status": "FAILURE",
        "parameters": [{"name": "TASK_ID", "value": "TASK-300"}],
    },
}

BUILD_STARTED = {
    "name": "my-job",
    "build": {
        "number": 45,
        "phase": "STARTED",
        "status": "",
    },
}

TEST_REPORT = {
    "totalCount": 100,
    "passCount": 95,
    "failCount": 3,
    "skipCount": 2,
    "duration": 12.5,
    "suites": [
        {
            "cases": [
                {"className": "TestAuth", "name": "test_login", "status": "PASSED"},
                {"className": "TestAuth", "name": "test_logout", "status": "FAILED", "errorDetails": "Timeout"},
                {"className": "TestDB", "name": "test_query", "status": "REGRESSION", "errorDetails": "NPE"},
            ]
        }
    ],
}


# ---------------------------------------------------------------------------
# Task ID extraction
# ---------------------------------------------------------------------------


class TestTaskExtraction:
    def test_from_parameters(self) -> None:
        assert extract_task_id_from_job("my-job", {"TASK_ID": "T1"}) == "T1"
        assert extract_task_id_from_job("my-job", {"BLUEPRINT_TASK": "T2"}) == "T2"

    def test_from_job_name(self) -> None:
        assert extract_task_id_from_job("task_ABC_build") == "ABC"
        assert extract_task_id_from_job("blueprint_XYZ_test") == "XYZ"

    def test_no_match(self) -> None:
        assert extract_task_id_from_job("regular-job") is None


# ---------------------------------------------------------------------------
# Build webhook
# ---------------------------------------------------------------------------


class TestBuildWebhook:
    def test_handle_success(self, integration: JenkinsIntegration) -> None:
        build = integration.handle_build_event(BUILD_SUCCESS)
        assert build.status == JenkinsBuildStatus.STABLE
        assert build.build_number == 42
        assert build.task_id == "TASK100"
        assert build.duration_ms == 30000

    def test_handle_unstable(self, integration: JenkinsIntegration) -> None:
        build = integration.handle_build_event(BUILD_UNSTABLE)
        assert build.status == JenkinsBuildStatus.UNSTABLE
        assert build.task_id == "TASK200"

    def test_handle_failure(self, integration: JenkinsIntegration) -> None:
        build = integration.handle_build_event(BUILD_FAILURE)
        assert build.status == JenkinsBuildStatus.FAILED
        assert build.task_id == "TASK-300"

    def test_handle_started(self, integration: JenkinsIntegration) -> None:
        build = integration.handle_build_event(BUILD_STARTED)
        assert build.status == JenkinsBuildStatus.IN_PROGRESS

    def test_parameter_extraction(self, integration: JenkinsIntegration) -> None:
        build = integration.handle_build_event(BUILD_SUCCESS)
        assert build.parameters["BRANCH"] == "main"

    def test_builds_for_task(self, integration: JenkinsIntegration) -> None:
        integration.handle_build_event(BUILD_SUCCESS)
        builds = integration.get_builds_for_task("TASK100")
        assert len(builds) == 1

    def test_list_builds(self, integration: JenkinsIntegration) -> None:
        integration.handle_build_event(BUILD_SUCCESS)
        integration.handle_build_event(BUILD_FAILURE)

        all_builds = integration.list_builds()
        assert len(all_builds) == 2

        stable = integration.list_builds(status=JenkinsBuildStatus.STABLE)
        assert len(stable) == 1


# ---------------------------------------------------------------------------
# Test result parsing
# ---------------------------------------------------------------------------


class TestResultParsing:
    def test_parse_results(self, integration: JenkinsIntegration) -> None:
        build = integration.handle_build_event(BUILD_SUCCESS)
        result = integration.parse_test_results(build.build_id, TEST_REPORT)

        assert result.total_tests == 100
        assert result.passed == 95
        assert result.failed == 3
        assert result.skipped == 2
        assert result.duration_ms == 12500
        assert len(result.failure_details) == 2

    def test_get_test_result(self, integration: JenkinsIntegration) -> None:
        build = integration.handle_build_event(BUILD_SUCCESS)
        integration.parse_test_results(build.build_id, TEST_REPORT)
        result = integration.get_test_result(build.build_id)
        assert result is not None
        assert result.total_tests == 100


# ---------------------------------------------------------------------------
# Artifacts
# ---------------------------------------------------------------------------


class TestArtifacts:
    def test_register_and_get(self, integration: JenkinsIntegration) -> None:
        build = integration.handle_build_event(BUILD_SUCCESS)
        art = integration.register_artifact(
            build.build_id, "app.jar", size_bytes=1024, url="https://j.com/art"
        )
        assert art.filename == "app.jar"

        arts = integration.get_artifacts(build.build_id)
        assert len(arts) == 1

    def test_empty_artifacts(self, integration: JenkinsIntegration) -> None:
        assert integration.get_artifacts("nonexistent") == []


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------


class TestPipelineStages:
    def test_record_and_get_stages(self, integration: JenkinsIntegration) -> None:
        build = integration.handle_build_event(BUILD_SUCCESS)
        integration.record_pipeline_stage(build.build_id, "Build", "SUCCESS", duration_ms=5000)
        integration.record_pipeline_stage(build.build_id, "Test", "SUCCESS", duration_ms=10000)

        stages = integration.get_stages(build.build_id)
        assert len(stages) == 2
        assert stages[0].name == "Build"


# ---------------------------------------------------------------------------
# Build queue
# ---------------------------------------------------------------------------


class TestBuildQueue:
    def test_queue_management(self, integration: JenkinsIntegration) -> None:
        entry = integration.add_to_queue("my-job", {"BRANCH": "main"})
        assert entry["job_name"] == "my-job"

        queue = integration.get_queue()
        assert len(queue) == 1


# ---------------------------------------------------------------------------
# Build metrics
# ---------------------------------------------------------------------------


class TestBuildMetrics:
    def test_metrics(self, integration: JenkinsIntegration) -> None:
        integration.handle_build_event(BUILD_SUCCESS)
        integration.handle_build_event(BUILD_UNSTABLE)
        integration.handle_build_event(BUILD_FAILURE)

        metrics = integration.calculate_build_metrics()
        assert metrics.total_builds == 3
        assert metrics.stable_count == 1
        assert metrics.unstable_count == 1
        assert metrics.failed_count == 1
        assert metrics.flakiness_score == pytest.approx(1 / 3)

    def test_empty_metrics(self, integration: JenkinsIntegration) -> None:
        metrics = integration.calculate_build_metrics()
        assert metrics.total_builds == 0
        assert metrics.success_rate == 0.0


# ---------------------------------------------------------------------------
# Auth token
# ---------------------------------------------------------------------------


class TestAuthToken:
    def test_no_token(self) -> None:
        i = JenkinsIntegration()
        assert i.verify_auth_token("any") is True

    def test_valid_token(self) -> None:
        i = JenkinsIntegration(auth_token="secret")
        assert i.verify_auth_token("secret") is True

    def test_invalid_token(self) -> None:
        i = JenkinsIntegration(auth_token="secret")
        assert i.verify_auth_token("wrong") is False
