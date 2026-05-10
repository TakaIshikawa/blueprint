"""GitLab CI integration for pipeline tracking."""

from __future__ import annotations

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
# Pipeline and job models
# ---------------------------------------------------------------------------


class PipelineStatus(str, Enum):
    CREATED = "created"
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"  # called "success" in GitLab, mapped here
    FAILED = "failed"
    CANCELED = "canceled"
    SKIPPED = "skipped"
    MANUAL = "manual"


class JobStatus(str, Enum):
    CREATED = "created"
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELED = "canceled"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class PipelineRecord:
    """Tracks a GitLab CI pipeline."""

    pipeline_id: str
    gitlab_pipeline_id: int
    project_id: int
    ref: str
    status: PipelineStatus
    sha: str = ""
    source: str = ""
    started_at: str = field(default_factory=_now_iso)
    finished_at: str | None = None
    duration_seconds: float | None = None
    url: str = ""
    task_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class JobRecord:
    """Tracks a GitLab CI job within a pipeline."""

    job_id: str
    gitlab_job_id: int
    pipeline_id: str
    name: str
    stage: str
    status: JobStatus
    started_at: str = field(default_factory=_now_iso)
    finished_at: str | None = None
    duration_seconds: float | None = None
    log_url: str = ""
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MergeRequestLink:
    """Links a GitLab merge request to a blueprint task."""

    link_id: str
    mr_iid: int
    project_id: int
    task_id: str
    title: str = ""
    source_branch: str = ""
    state: str = "opened"
    url: str = ""
    created_at: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class EnvironmentDeployment:
    """Tracks a deployment to a GitLab environment."""

    deployment_id: str
    environment: str
    ref: str
    sha: str = ""
    status: str = "success"
    deployed_at: str = field(default_factory=_now_iso)
    url: str = ""


@dataclass(frozen=True, slots=True)
class PipelineMetrics:
    """Aggregated pipeline metrics."""

    total_pipelines: int
    passed_count: int
    failed_count: int
    avg_duration_seconds: float
    failure_rate_by_stage: dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Commit message parsing for task mapping
# ---------------------------------------------------------------------------

COMMIT_TASK_PATTERNS = ("[task:", "[bp:", "blueprint:")


def extract_task_id_from_commit(message: str) -> str | None:
    """Extract task ID from commit message conventions."""
    lower = message.lower()
    for pattern in COMMIT_TASK_PATTERNS:
        idx = lower.find(pattern)
        if idx >= 0:
            start = idx + len(pattern)
            rest = message[start:].strip()
            # Extract until ] or whitespace
            end = 0
            for ch in rest:
                if ch in ("]", " ", "\n"):
                    break
                end += 1
            if end > 0:
                return rest[:end]
    return None


def extract_task_id_from_branch(branch: str) -> str | None:
    """Extract task ID from branch naming conventions."""
    prefixes = ("task/", "task-", "blueprint/", "bp-")
    for prefix in prefixes:
        if branch.startswith(prefix):
            remainder = branch[len(prefix):]
            return remainder.split("/")[0]
    return None


# ---------------------------------------------------------------------------
# Main integration class
# ---------------------------------------------------------------------------


class GitLabCIIntegration:
    """Manages GitLab CI pipeline events and tracking."""

    def __init__(self, *, webhook_token: str = ""):
        self._pipelines: dict[str, PipelineRecord] = {}
        self._jobs: dict[str, JobRecord] = {}
        self._mr_links: dict[str, MergeRequestLink] = {}
        self._deployments: dict[str, EnvironmentDeployment] = {}
        self._task_updates: list[dict[str, Any]] = []
        self._issues_synced: list[dict[str, Any]] = []
        self._webhook_token = webhook_token

    # -- Webhook receiver ---------------------------------------------------

    def verify_webhook_token(self, token: str) -> bool:
        """Verify GitLab webhook secret token."""
        if not self._webhook_token:
            return True
        return token == self._webhook_token

    def handle_pipeline_event(self, event: dict[str, Any]) -> PipelineRecord:
        """Handle a pipeline webhook event."""
        attrs = event.get("object_attributes", event)

        status_map = {
            "created": PipelineStatus.CREATED,
            "pending": PipelineStatus.PENDING,
            "running": PipelineStatus.RUNNING,
            "success": PipelineStatus.PASSED,
            "failed": PipelineStatus.FAILED,
            "canceled": PipelineStatus.CANCELED,
            "skipped": PipelineStatus.SKIPPED,
            "manual": PipelineStatus.MANUAL,
        }

        raw_status = attrs.get("status", "created")
        status = status_map.get(raw_status, PipelineStatus.CREATED)
        ref = attrs.get("ref", "")
        sha = attrs.get("sha", "")

        # Try to extract task ID from ref or commit
        task_id = extract_task_id_from_branch(ref) or ""
        if not task_id:
            # Try last commit message
            commits = event.get("commits", [])
            if commits:
                task_id = extract_task_id_from_commit(commits[-1].get("message", "")) or ""

        duration = attrs.get("duration")

        pipeline = PipelineRecord(
            pipeline_id=_gen_id("pipe"),
            gitlab_pipeline_id=attrs.get("id", 0),
            project_id=event.get("project", {}).get("id", 0),
            ref=ref,
            status=status,
            sha=sha,
            source=attrs.get("source", ""),
            finished_at=attrs.get("finished_at"),
            duration_seconds=float(duration) if duration else None,
            url=attrs.get("url", ""),
            task_id=task_id,
        )
        self._pipelines[pipeline.pipeline_id] = pipeline

        # Auto-update task based on outcome
        if task_id and status in (PipelineStatus.PASSED, PipelineStatus.FAILED):
            task_status = "completed" if status == PipelineStatus.PASSED else "blocked"
            self._task_updates.append({
                "task_id": task_id,
                "status": task_status,
                "pipeline_id": pipeline.pipeline_id,
                "updated_at": _now_iso(),
            })

        return pipeline

    def handle_job_event(self, event: dict[str, Any]) -> JobRecord:
        """Handle a job webhook event."""
        attrs = event.get("object_attributes", event) if "object_attributes" in event else event

        status_map = {
            "created": JobStatus.CREATED,
            "pending": JobStatus.PENDING,
            "running": JobStatus.RUNNING,
            "success": JobStatus.SUCCESS,
            "failed": JobStatus.FAILED,
            "canceled": JobStatus.CANCELED,
            "skipped": JobStatus.SKIPPED,
        }

        raw_status = attrs.get("status", attrs.get("build_status", "created"))
        status = status_map.get(raw_status, JobStatus.CREATED)
        duration = attrs.get("build_duration", attrs.get("duration"))

        # Find matching pipeline
        pipeline_id = ""
        pipeline_name = attrs.get("pipeline_id", "")
        if pipeline_name:
            for pid, p in self._pipelines.items():
                if p.gitlab_pipeline_id == pipeline_name:
                    pipeline_id = pid
                    break

        artifacts = []
        for a in attrs.get("artifacts_file", []) if isinstance(attrs.get("artifacts_file"), list) else []:
            artifacts.append({"filename": a.get("filename", ""), "size": a.get("size", 0)})

        job = JobRecord(
            job_id=_gen_id("job"),
            gitlab_job_id=attrs.get("build_id", attrs.get("id", 0)),
            pipeline_id=pipeline_id,
            name=attrs.get("build_name", attrs.get("name", "")),
            stage=attrs.get("build_stage", attrs.get("stage", "")),
            status=status,
            finished_at=attrs.get("build_finished_at", attrs.get("finished_at")),
            duration_seconds=float(duration) if duration else None,
            log_url=attrs.get("build_url", ""),
            artifacts=artifacts,
        )
        self._jobs[job.job_id] = job
        return job

    # -- Merge request linking ----------------------------------------------

    def handle_merge_request_event(self, event: dict[str, Any]) -> MergeRequestLink | None:
        """Handle a merge request webhook event and link to tasks."""
        attrs = event.get("object_attributes", event)
        source_branch = attrs.get("source_branch", "")
        task_id = extract_task_id_from_branch(source_branch)
        if not task_id:
            return None

        link = MergeRequestLink(
            link_id=_gen_id("mrl"),
            mr_iid=attrs.get("iid", 0),
            project_id=event.get("project", {}).get("id", attrs.get("project_id", 0)),
            task_id=task_id,
            title=attrs.get("title", ""),
            source_branch=source_branch,
            state=attrs.get("state", "opened"),
            url=attrs.get("url", ""),
        )
        self._mr_links[link.link_id] = link
        return link

    # -- Deployment tracking ------------------------------------------------

    def handle_deployment_event(self, event: dict[str, Any]) -> EnvironmentDeployment:
        """Handle a deployment webhook event."""
        deployment = EnvironmentDeployment(
            deployment_id=_gen_id("gdpl"),
            environment=event.get("environment", ""),
            ref=event.get("ref", ""),
            sha=event.get("sha", event.get("commit_url", "").split("/")[-1] if event.get("commit_url") else ""),
            status=event.get("status", "success"),
            url=event.get("deployable_url", ""),
        )
        self._deployments[deployment.deployment_id] = deployment
        return deployment

    # -- Issue sync ---------------------------------------------------------

    def sync_issue_to_blueprint(self, gitlab_issue: dict[str, Any]) -> dict[str, Any]:
        """Map a GitLab issue to a blueprint issue."""
        issue = {
            "source": "gitlab",
            "gitlab_iid": gitlab_issue.get("iid", 0),
            "title": gitlab_issue.get("title", ""),
            "description": gitlab_issue.get("description", ""),
            "state": gitlab_issue.get("state", "opened"),
            "labels": gitlab_issue.get("labels", []),
            "synced_at": _now_iso(),
        }
        self._issues_synced.append(issue)
        return issue

    # -- Query methods ------------------------------------------------------

    def get_pipeline(self, pipeline_id: str) -> PipelineRecord | None:
        return self._pipelines.get(pipeline_id)

    def list_pipelines(
        self,
        *,
        status: PipelineStatus | None = None,
        task_id: str | None = None,
    ) -> list[PipelineRecord]:
        results = list(self._pipelines.values())
        if status:
            results = [p for p in results if p.status == status]
        if task_id:
            results = [p for p in results if p.task_id == task_id]
        return results

    def get_jobs_for_pipeline(self, pipeline_id: str) -> list[JobRecord]:
        return [j for j in self._jobs.values() if j.pipeline_id == pipeline_id]

    def get_mr_links_for_task(self, task_id: str) -> list[MergeRequestLink]:
        return [l for l in self._mr_links.values() if l.task_id == task_id]

    def get_task_updates(self) -> list[dict[str, Any]]:
        return list(self._task_updates)

    def get_synced_issues(self) -> list[dict[str, Any]]:
        return list(self._issues_synced)

    # -- Metrics ------------------------------------------------------------

    def calculate_pipeline_metrics(self) -> PipelineMetrics:
        """Calculate aggregated pipeline metrics."""
        pipelines = list(self._pipelines.values())
        if not pipelines:
            return PipelineMetrics(
                total_pipelines=0,
                passed_count=0,
                failed_count=0,
                avg_duration_seconds=0.0,
            )

        passed = [p for p in pipelines if p.status == PipelineStatus.PASSED]
        failed = [p for p in pipelines if p.status == PipelineStatus.FAILED]
        durations = [p.duration_seconds for p in pipelines if p.duration_seconds is not None]
        avg_dur = sum(durations) / len(durations) if durations else 0.0

        # Failure rate by stage
        stage_totals: dict[str, int] = {}
        stage_failures: dict[str, int] = {}
        for job in self._jobs.values():
            if job.stage:
                stage_totals[job.stage] = stage_totals.get(job.stage, 0) + 1
                if job.status == JobStatus.FAILED:
                    stage_failures[job.stage] = stage_failures.get(job.stage, 0) + 1

        failure_rate_by_stage = {
            stage: stage_failures.get(stage, 0) / total
            for stage, total in stage_totals.items()
            if total > 0
        }

        return PipelineMetrics(
            total_pipelines=len(pipelines),
            passed_count=len(passed),
            failed_count=len(failed),
            avg_duration_seconds=avg_dur,
            failure_rate_by_stage=failure_rate_by_stage,
        )


__all__ = [
    "PipelineStatus",
    "JobStatus",
    "PipelineRecord",
    "JobRecord",
    "MergeRequestLink",
    "EnvironmentDeployment",
    "PipelineMetrics",
    "GitLabCIIntegration",
    "extract_task_id_from_commit",
    "extract_task_id_from_branch",
]
