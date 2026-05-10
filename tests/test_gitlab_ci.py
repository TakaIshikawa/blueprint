"""Tests for GitLab CI integration."""

from __future__ import annotations

import pytest

from blueprint.cicd.gitlab_ci import (
    GitLabCIIntegration,
    JobStatus,
    PipelineStatus,
    extract_task_id_from_branch,
    extract_task_id_from_commit,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def integration() -> GitLabCIIntegration:
    return GitLabCIIntegration()


PIPELINE_SUCCESS = {
    "object_attributes": {
        "id": 1001,
        "ref": "task/TASK-50",
        "sha": "aaa111",
        "status": "success",
        "source": "push",
        "duration": 120,
        "finished_at": "2025-01-01T10:02:00Z",
        "url": "https://gitlab.com/org/repo/-/pipelines/1001",
    },
    "project": {"id": 42},
}

PIPELINE_FAILURE = {
    "object_attributes": {
        "id": 1002,
        "ref": "task/TASK-60",
        "sha": "bbb222",
        "status": "failed",
        "source": "push",
        "duration": 60,
    },
    "project": {"id": 42},
}

JOB_EVENT = {
    "build_id": 5001,
    "build_name": "test",
    "build_stage": "test",
    "build_status": "success",
    "build_duration": 45.5,
    "build_finished_at": "2025-01-01T10:01:00Z",
    "pipeline_id": 1001,
}

MR_EVENT = {
    "object_attributes": {
        "iid": 77,
        "title": "Fix login bug",
        "source_branch": "task/TASK-50",
        "state": "opened",
        "url": "https://gitlab.com/org/repo/-/merge_requests/77",
    },
    "project": {"id": 42},
}


# ---------------------------------------------------------------------------
# Commit/branch parsing
# ---------------------------------------------------------------------------


class TestParsing:
    def test_extract_from_commit(self) -> None:
        assert extract_task_id_from_commit("[task:TASK-50] fix login") == "TASK-50"
        assert extract_task_id_from_commit("[bp:ABC] refactor") == "ABC"
        assert extract_task_id_from_commit("blueprint:XYZ update docs") == "XYZ"
        assert extract_task_id_from_commit("normal commit") is None

    def test_extract_from_branch(self) -> None:
        assert extract_task_id_from_branch("task/TASK-50") == "TASK-50"
        assert extract_task_id_from_branch("bp-fix123") == "fix123"
        assert extract_task_id_from_branch("feature/login") is None


# ---------------------------------------------------------------------------
# Pipeline webhook
# ---------------------------------------------------------------------------


class TestPipelineWebhook:
    def test_handle_success(self, integration: GitLabCIIntegration) -> None:
        pipe = integration.handle_pipeline_event(PIPELINE_SUCCESS)
        assert pipe.status == PipelineStatus.PASSED
        assert pipe.task_id == "TASK-50"
        assert pipe.gitlab_pipeline_id == 1001
        assert pipe.duration_seconds == 120.0

    def test_handle_failure(self, integration: GitLabCIIntegration) -> None:
        pipe = integration.handle_pipeline_event(PIPELINE_FAILURE)
        assert pipe.status == PipelineStatus.FAILED
        assert pipe.task_id == "TASK-60"

    def test_auto_update_on_success(self, integration: GitLabCIIntegration) -> None:
        integration.handle_pipeline_event(PIPELINE_SUCCESS)
        updates = integration.get_task_updates()
        assert len(updates) == 1
        assert updates[0]["task_id"] == "TASK-50"
        assert updates[0]["status"] == "completed"

    def test_auto_update_on_failure(self, integration: GitLabCIIntegration) -> None:
        integration.handle_pipeline_event(PIPELINE_FAILURE)
        updates = integration.get_task_updates()
        assert len(updates) == 1
        assert updates[0]["status"] == "blocked"

    def test_list_pipelines(self, integration: GitLabCIIntegration) -> None:
        integration.handle_pipeline_event(PIPELINE_SUCCESS)
        integration.handle_pipeline_event(PIPELINE_FAILURE)

        all_pipes = integration.list_pipelines()
        assert len(all_pipes) == 2

        passed = integration.list_pipelines(status=PipelineStatus.PASSED)
        assert len(passed) == 1

        by_task = integration.list_pipelines(task_id="TASK-50")
        assert len(by_task) == 1

    def test_pipeline_with_commit_task_id(self, integration: GitLabCIIntegration) -> None:
        event = {
            "object_attributes": {
                "id": 2000,
                "ref": "main",
                "sha": "ccc",
                "status": "success",
            },
            "commits": [{"message": "[task:CMT-1] deploy fix"}],
            "project": {"id": 1},
        }
        pipe = integration.handle_pipeline_event(event)
        assert pipe.task_id == "CMT-1"


# ---------------------------------------------------------------------------
# Job tracking
# ---------------------------------------------------------------------------


class TestJobTracking:
    def test_handle_job(self, integration: GitLabCIIntegration) -> None:
        job = integration.handle_job_event(JOB_EVENT)
        assert job.name == "test"
        assert job.stage == "test"
        assert job.status == JobStatus.SUCCESS
        assert job.duration_seconds == 45.5

    def test_jobs_for_pipeline(self, integration: GitLabCIIntegration) -> None:
        # Create pipeline first so job can link
        pipe = integration.handle_pipeline_event(PIPELINE_SUCCESS)
        job_event = dict(JOB_EVENT, pipeline_id=1001)
        integration.handle_job_event(job_event)

        jobs = integration.get_jobs_for_pipeline(pipe.pipeline_id)
        assert len(jobs) == 1


# ---------------------------------------------------------------------------
# Merge request linking
# ---------------------------------------------------------------------------


class TestMergeRequestLinking:
    def test_link_mr_to_task(self, integration: GitLabCIIntegration) -> None:
        link = integration.handle_merge_request_event(MR_EVENT)
        assert link is not None
        assert link.task_id == "TASK-50"
        assert link.mr_iid == 77
        assert link.title == "Fix login bug"

    def test_no_link_for_non_task_branch(self, integration: GitLabCIIntegration) -> None:
        event = {
            "object_attributes": {
                "iid": 88,
                "title": "Feature",
                "source_branch": "feature/login",
                "state": "opened",
            },
            "project": {"id": 1},
        }
        link = integration.handle_merge_request_event(event)
        assert link is None

    def test_get_links_for_task(self, integration: GitLabCIIntegration) -> None:
        integration.handle_merge_request_event(MR_EVENT)
        links = integration.get_mr_links_for_task("TASK-50")
        assert len(links) == 1


# ---------------------------------------------------------------------------
# Deployment tracking
# ---------------------------------------------------------------------------


class TestDeployment:
    def test_handle_deployment(self, integration: GitLabCIIntegration) -> None:
        dep = integration.handle_deployment_event({
            "environment": "production",
            "ref": "v1.0.0",
            "status": "success",
        })
        assert dep.environment == "production"
        assert dep.ref == "v1.0.0"


# ---------------------------------------------------------------------------
# Issue sync
# ---------------------------------------------------------------------------


class TestIssueSync:
    def test_sync_issue(self, integration: GitLabCIIntegration) -> None:
        issue = integration.sync_issue_to_blueprint({
            "iid": 10,
            "title": "Bug report",
            "description": "Something broken",
            "state": "opened",
            "labels": ["bug"],
        })
        assert issue["source"] == "gitlab"
        assert issue["title"] == "Bug report"
        assert integration.get_synced_issues()[0]["gitlab_iid"] == 10


# ---------------------------------------------------------------------------
# Pipeline metrics
# ---------------------------------------------------------------------------


class TestMetrics:
    def test_metrics(self, integration: GitLabCIIntegration) -> None:
        integration.handle_pipeline_event(PIPELINE_SUCCESS)
        integration.handle_pipeline_event(PIPELINE_FAILURE)

        metrics = integration.calculate_pipeline_metrics()
        assert metrics.total_pipelines == 2
        assert metrics.passed_count == 1
        assert metrics.failed_count == 1

    def test_empty_metrics(self, integration: GitLabCIIntegration) -> None:
        metrics = integration.calculate_pipeline_metrics()
        assert metrics.total_pipelines == 0

    def test_failure_rate_by_stage(self, integration: GitLabCIIntegration) -> None:
        # Need at least one pipeline so metrics calculate
        integration.handle_pipeline_event(PIPELINE_SUCCESS)
        integration.handle_job_event({"build_name": "test", "build_stage": "test", "build_status": "success", "build_id": 1})
        integration.handle_job_event({"build_name": "test2", "build_stage": "test", "build_status": "failed", "build_id": 2})

        metrics = integration.calculate_pipeline_metrics()
        assert "test" in metrics.failure_rate_by_stage
        assert metrics.failure_rate_by_stage["test"] == 0.5


# ---------------------------------------------------------------------------
# Webhook verification
# ---------------------------------------------------------------------------


class TestWebhookToken:
    def test_no_token_allows_all(self) -> None:
        i = GitLabCIIntegration()
        assert i.verify_webhook_token("anything") is True

    def test_valid_token(self) -> None:
        i = GitLabCIIntegration(webhook_token="secret")
        assert i.verify_webhook_token("secret") is True

    def test_invalid_token(self) -> None:
        i = GitLabCIIntegration(webhook_token="secret")
        assert i.verify_webhook_token("wrong") is False
