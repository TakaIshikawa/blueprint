"""GitHub Actions integration for CI/CD workflow tracking."""

from __future__ import annotations

import hashlib
import hmac
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Build and deployment models
# ---------------------------------------------------------------------------


class BuildStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILURE = "failure"
    CANCELLED = "cancelled"


class DeploymentStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILURE = "failure"


@dataclass(frozen=True, slots=True)
class BuildRecord:
    """Tracks a GitHub Actions workflow run linked to a task."""

    build_id: str
    run_id: int
    workflow_name: str
    status: BuildStatus
    task_id: str = ""
    branch: str = ""
    commit_sha: str = ""
    started_at: str = field(default_factory=_now_iso)
    completed_at: str | None = None
    duration_seconds: float | None = None
    url: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DeploymentRecord:
    """Tracks a deployment with environment and version."""

    deployment_id: str
    environment: str
    version: str
    status: DeploymentStatus
    task_id: str = ""
    commit_sha: str = ""
    deployed_at: str = field(default_factory=_now_iso)
    url: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StatusCheck:
    """GitHub status check showing plan status."""

    check_id: str
    context: str
    state: str  # pending, success, failure, error
    description: str
    target_url: str = ""
    commit_sha: str = ""
    created_at: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class BuildMetrics:
    """Aggregated build metrics."""

    total_builds: int
    success_count: int
    failure_count: int
    avg_duration_seconds: float
    success_rate: float


# ---------------------------------------------------------------------------
# Branch naming convention for PR-to-task linking
# ---------------------------------------------------------------------------

BRANCH_TASK_PREFIXES = ("task/", "task-", "blueprint/", "bp-")


def extract_task_id_from_branch(branch: str) -> str | None:
    """Extract a task ID from a branch name using naming conventions."""
    for prefix in BRANCH_TASK_PREFIXES:
        if branch.startswith(prefix):
            remainder = branch[len(prefix):]
            return remainder.split("/")[0].split("-")[0] if "/" in remainder else remainder
    return None


# ---------------------------------------------------------------------------
# Main integration class
# ---------------------------------------------------------------------------


class GitHubActionsIntegration:
    """Manages GitHub Actions workflow events and build tracking."""

    def __init__(self, *, webhook_secret: str = ""):
        self._builds: dict[str, BuildRecord] = {}
        self._deployments: dict[str, DeploymentRecord] = {}
        self._status_checks: list[StatusCheck] = []
        self._task_completions: list[str] = []  # task IDs auto-completed
        self._issues_created: list[dict[str, Any]] = []
        self._webhook_secret = webhook_secret

    # -- Webhook receiver ---------------------------------------------------

    def verify_webhook_signature(self, payload_body: bytes, signature: str) -> bool:
        """Verify GitHub webhook signature (X-Hub-Signature-256)."""
        if not self._webhook_secret:
            return True
        expected = "sha256=" + hmac.new(
            self._webhook_secret.encode("utf-8"),
            payload_body,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)

    def handle_workflow_run(self, event: dict[str, Any]) -> BuildRecord:
        """Handle a workflow_run webhook event."""
        workflow_run = event.get("workflow_run", event)
        action = event.get("action", "")

        status_map = {
            "queued": BuildStatus.PENDING,
            "in_progress": BuildStatus.IN_PROGRESS,
            "completed": BuildStatus.SUCCESS,
        }

        conclusion = workflow_run.get("conclusion")
        if conclusion == "failure":
            build_status = BuildStatus.FAILURE
        elif conclusion == "cancelled":
            build_status = BuildStatus.CANCELLED
        elif action in status_map:
            build_status = status_map[action]
        else:
            build_status = BuildStatus.PENDING

        run_id = workflow_run.get("id", 0)
        branch = workflow_run.get("head_branch", "")
        task_id = extract_task_id_from_branch(branch) or ""

        # Calculate duration
        duration = None
        started = workflow_run.get("run_started_at")
        updated = workflow_run.get("updated_at")
        if started and updated and conclusion:
            try:
                start_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                duration = (end_dt - start_dt).total_seconds()
            except (ValueError, TypeError):
                pass

        build = BuildRecord(
            build_id=_gen_id("bld"),
            run_id=run_id,
            workflow_name=workflow_run.get("name", ""),
            status=build_status,
            task_id=task_id,
            branch=branch,
            commit_sha=workflow_run.get("head_sha", ""),
            completed_at=_now_iso() if conclusion else None,
            duration_seconds=duration,
            url=workflow_run.get("html_url", ""),
        )
        self._builds[build.build_id] = build

        # Auto-complete task on success
        if build_status == BuildStatus.SUCCESS and task_id:
            self._task_completions.append(task_id)

        # Create issue on failure
        if build_status == BuildStatus.FAILURE and task_id:
            issue = {
                "title": f"Build failure: {build.workflow_name}",
                "task_id": task_id,
                "build_id": build.build_id,
                "branch": branch,
                "commit_sha": build.commit_sha,
                "url": build.url,
                "created_at": _now_iso(),
            }
            self._issues_created.append(issue)

        return build

    def handle_deployment(self, event: dict[str, Any]) -> DeploymentRecord:
        """Handle a deployment webhook event."""
        deployment = event.get("deployment", event)
        status = event.get("deployment_status", {})

        state = status.get("state", "pending")
        status_map = {
            "pending": DeploymentStatus.PENDING,
            "in_progress": DeploymentStatus.IN_PROGRESS,
            "success": DeploymentStatus.SUCCESS,
            "failure": DeploymentStatus.FAILURE,
        }

        branch = deployment.get("ref", "")
        task_id = extract_task_id_from_branch(branch) or ""

        record = DeploymentRecord(
            deployment_id=_gen_id("dpl"),
            environment=deployment.get("environment", ""),
            version=deployment.get("ref", ""),
            status=status_map.get(state, DeploymentStatus.PENDING),
            task_id=task_id,
            commit_sha=deployment.get("sha", ""),
            url=status.get("target_url", ""),
        )
        self._deployments[record.deployment_id] = record
        return record

    # -- Status checks ------------------------------------------------------

    def create_status_check(
        self,
        commit_sha: str,
        state: str,
        description: str,
        *,
        context: str = "blueprint/plan-status",
        target_url: str = "",
    ) -> StatusCheck:
        """Create a GitHub status check showing plan status."""
        check = StatusCheck(
            check_id=_gen_id("chk"),
            context=context,
            state=state,
            description=description,
            target_url=target_url,
            commit_sha=commit_sha,
        )
        self._status_checks.append(check)
        return check

    # -- Deployment gate ----------------------------------------------------

    def check_deployment_gate(self, task_ids: list[str]) -> dict[str, Any]:
        """Check if all required tasks are completed before allowing deployment."""
        completed = set(self._task_completions)
        pending = [t for t in task_ids if t not in completed]
        return {
            "allowed": len(pending) == 0,
            "completed": [t for t in task_ids if t in completed],
            "pending": pending,
        }

    # -- Query methods ------------------------------------------------------

    def get_build(self, build_id: str) -> BuildRecord | None:
        return self._builds.get(build_id)

    def get_builds_for_task(self, task_id: str) -> list[BuildRecord]:
        return [b for b in self._builds.values() if b.task_id == task_id]

    def get_deployment(self, deployment_id: str) -> DeploymentRecord | None:
        return self._deployments.get(deployment_id)

    def get_deployments(
        self,
        *,
        environment: str | None = None,
    ) -> list[DeploymentRecord]:
        results = list(self._deployments.values())
        if environment:
            results = [d for d in results if d.environment == environment]
        return results

    def get_auto_completed_tasks(self) -> list[str]:
        return list(self._task_completions)

    def get_failure_issues(self) -> list[dict[str, Any]]:
        return list(self._issues_created)

    # -- Metrics ------------------------------------------------------------

    def calculate_build_metrics(self) -> BuildMetrics:
        """Calculate aggregated build metrics."""
        builds = list(self._builds.values())
        if not builds:
            return BuildMetrics(
                total_builds=0,
                success_count=0,
                failure_count=0,
                avg_duration_seconds=0.0,
                success_rate=0.0,
            )

        success = [b for b in builds if b.status == BuildStatus.SUCCESS]
        failures = [b for b in builds if b.status == BuildStatus.FAILURE]
        durations = [b.duration_seconds for b in builds if b.duration_seconds is not None]
        avg_dur = sum(durations) / len(durations) if durations else 0.0

        return BuildMetrics(
            total_builds=len(builds),
            success_count=len(success),
            failure_count=len(failures),
            avg_duration_seconds=avg_dur,
            success_rate=len(success) / len(builds) if builds else 0.0,
        )


__all__ = [
    "BuildStatus",
    "DeploymentStatus",
    "BuildRecord",
    "DeploymentRecord",
    "StatusCheck",
    "BuildMetrics",
    "GitHubActionsIntegration",
    "extract_task_id_from_branch",
]
