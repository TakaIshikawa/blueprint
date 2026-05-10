"""Tests for GitHub Actions CI/CD integration."""

from __future__ import annotations

import pytest

from blueprint.cicd.github_actions import (
    BuildStatus,
    DeploymentStatus,
    GitHubActionsIntegration,
    extract_task_id_from_branch,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def integration() -> GitHubActionsIntegration:
    return GitHubActionsIntegration()


WORKFLOW_RUN_SUCCESS = {
    "action": "completed",
    "workflow_run": {
        "id": 12345,
        "name": "CI",
        "head_branch": "task/TASK-100",
        "head_sha": "abc123",
        "conclusion": "success",
        "html_url": "https://github.com/org/repo/actions/runs/12345",
        "run_started_at": "2025-01-01T10:00:00Z",
        "updated_at": "2025-01-01T10:05:00Z",
    },
}

WORKFLOW_RUN_FAILURE = {
    "action": "completed",
    "workflow_run": {
        "id": 12346,
        "name": "CI",
        "head_branch": "task/TASK-200",
        "head_sha": "def456",
        "conclusion": "failure",
        "html_url": "https://github.com/org/repo/actions/runs/12346",
    },
}

DEPLOYMENT_EVENT = {
    "deployment": {
        "id": 555,
        "environment": "production",
        "ref": "task/TASK-100",
        "sha": "abc123",
    },
    "deployment_status": {
        "state": "success",
        "target_url": "https://prod.example.com",
    },
}


# ---------------------------------------------------------------------------
# Branch task ID extraction
# ---------------------------------------------------------------------------


class TestBranchParsing:
    def test_task_prefix(self) -> None:
        assert extract_task_id_from_branch("task/TASK-100") == "TASK-100"

    def test_blueprint_prefix(self) -> None:
        assert extract_task_id_from_branch("blueprint/fix-123") == "fix-123"

    def test_bp_prefix(self) -> None:
        assert extract_task_id_from_branch("bp-abc") == "abc"

    def test_no_match(self) -> None:
        assert extract_task_id_from_branch("feature/login") is None


# ---------------------------------------------------------------------------
# Workflow run webhook
# ---------------------------------------------------------------------------


class TestWorkflowRun:
    def test_handle_success(self, integration: GitHubActionsIntegration) -> None:
        build = integration.handle_workflow_run(WORKFLOW_RUN_SUCCESS)
        assert build.status == BuildStatus.SUCCESS
        assert build.task_id == "TASK-100"
        assert build.run_id == 12345
        assert build.workflow_name == "CI"
        assert build.duration_seconds is not None
        assert build.duration_seconds == 300.0

    def test_handle_failure(self, integration: GitHubActionsIntegration) -> None:
        build = integration.handle_workflow_run(WORKFLOW_RUN_FAILURE)
        assert build.status == BuildStatus.FAILURE
        assert build.task_id == "TASK-200"

    def test_auto_complete_on_success(self, integration: GitHubActionsIntegration) -> None:
        integration.handle_workflow_run(WORKFLOW_RUN_SUCCESS)
        completed = integration.get_auto_completed_tasks()
        assert "TASK-100" in completed

    def test_issue_created_on_failure(self, integration: GitHubActionsIntegration) -> None:
        integration.handle_workflow_run(WORKFLOW_RUN_FAILURE)
        issues = integration.get_failure_issues()
        assert len(issues) == 1
        assert issues[0]["task_id"] == "TASK-200"
        assert "failure" in issues[0]["title"].lower()

    def test_builds_for_task(self, integration: GitHubActionsIntegration) -> None:
        integration.handle_workflow_run(WORKFLOW_RUN_SUCCESS)
        builds = integration.get_builds_for_task("TASK-100")
        assert len(builds) == 1

    def test_handle_in_progress(self, integration: GitHubActionsIntegration) -> None:
        event = {
            "action": "in_progress",
            "workflow_run": {
                "id": 999,
                "name": "CI",
                "head_branch": "main",
                "head_sha": "xyz",
            },
        }
        build = integration.handle_workflow_run(event)
        assert build.status == BuildStatus.IN_PROGRESS


# ---------------------------------------------------------------------------
# Deployment tracking
# ---------------------------------------------------------------------------


class TestDeployment:
    def test_handle_deployment(self, integration: GitHubActionsIntegration) -> None:
        dep = integration.handle_deployment(DEPLOYMENT_EVENT)
        assert dep.environment == "production"
        assert dep.status == DeploymentStatus.SUCCESS
        assert dep.task_id == "TASK-100"

    def test_list_deployments(self, integration: GitHubActionsIntegration) -> None:
        integration.handle_deployment(DEPLOYMENT_EVENT)
        deps = integration.get_deployments()
        assert len(deps) == 1

        by_env = integration.get_deployments(environment="production")
        assert len(by_env) == 1

        empty = integration.get_deployments(environment="staging")
        assert len(empty) == 0


# ---------------------------------------------------------------------------
# Status checks
# ---------------------------------------------------------------------------


class TestStatusChecks:
    def test_create_status_check(self, integration: GitHubActionsIntegration) -> None:
        check = integration.create_status_check(
            "abc123",
            "success",
            "All tasks completed",
        )
        assert check.state == "success"
        assert check.commit_sha == "abc123"


# ---------------------------------------------------------------------------
# Deployment gate
# ---------------------------------------------------------------------------


class TestDeploymentGate:
    def test_gate_allowed(self, integration: GitHubActionsIntegration) -> None:
        integration.handle_workflow_run(WORKFLOW_RUN_SUCCESS)
        result = integration.check_deployment_gate(["TASK-100"])
        assert result["allowed"] is True

    def test_gate_blocked(self, integration: GitHubActionsIntegration) -> None:
        result = integration.check_deployment_gate(["TASK-100", "TASK-200"])
        assert result["allowed"] is False
        assert len(result["pending"]) == 2


# ---------------------------------------------------------------------------
# Build metrics
# ---------------------------------------------------------------------------


class TestBuildMetrics:
    def test_metrics(self, integration: GitHubActionsIntegration) -> None:
        integration.handle_workflow_run(WORKFLOW_RUN_SUCCESS)
        integration.handle_workflow_run(WORKFLOW_RUN_FAILURE)

        metrics = integration.calculate_build_metrics()
        assert metrics.total_builds == 2
        assert metrics.success_count == 1
        assert metrics.failure_count == 1
        assert metrics.success_rate == 0.5

    def test_empty_metrics(self, integration: GitHubActionsIntegration) -> None:
        metrics = integration.calculate_build_metrics()
        assert metrics.total_builds == 0
        assert metrics.success_rate == 0.0


# ---------------------------------------------------------------------------
# Webhook verification
# ---------------------------------------------------------------------------


class TestWebhookVerification:
    def test_no_secret_allows_all(self) -> None:
        integration = GitHubActionsIntegration()
        assert integration.verify_webhook_signature(b"anything", "any-sig") is True

    def test_valid_signature(self) -> None:
        import hashlib
        import hmac

        secret = "my-secret"
        body = b'{"action":"completed"}'
        sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        integration = GitHubActionsIntegration(webhook_secret=secret)
        assert integration.verify_webhook_signature(body, sig) is True

    def test_invalid_signature(self) -> None:
        integration = GitHubActionsIntegration(webhook_secret="my-secret")
        assert integration.verify_webhook_signature(b"body", "sha256=bad") is False
