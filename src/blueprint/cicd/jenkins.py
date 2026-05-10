"""Jenkins integration for build job tracking."""

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
# Build models
# ---------------------------------------------------------------------------


class JenkinsBuildStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    STABLE = "stable"      # SUCCESS
    UNSTABLE = "unstable"  # Tests failed but build completed
    FAILED = "failed"
    ABORTED = "aborted"


@dataclass(frozen=True, slots=True)
class JenkinsBuild:
    """Tracks a Jenkins build job."""

    build_id: str
    job_name: str
    build_number: int
    status: JenkinsBuildStatus
    task_id: str = ""
    url: str = ""
    duration_ms: int | None = None
    timestamp: str = field(default_factory=_now_iso)
    parameters: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TestResult:
    """Parsed test result from a Jenkins build."""

    build_id: str
    total_tests: int
    passed: int
    failed: int
    skipped: int
    duration_ms: int = 0
    failure_details: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class BuildArtifact:
    """An artifact from a Jenkins build."""

    artifact_id: str
    build_id: str
    filename: str
    relative_path: str = ""
    size_bytes: int = 0
    url: str = ""


@dataclass(frozen=True, slots=True)
class PipelineStage:
    """A stage in a Jenkins pipeline build."""

    stage_id: str
    build_id: str
    name: str
    status: str  # SUCCESS, FAILURE, IN_PROGRESS
    duration_ms: int = 0
    started_at: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class JenkinsBuildMetrics:
    """Aggregated build metrics."""

    total_builds: int
    stable_count: int
    unstable_count: int
    failed_count: int
    success_rate: float
    avg_duration_ms: float
    flakiness_score: float  # ratio of unstable builds


# ---------------------------------------------------------------------------
# Job name parsing for task linking
# ---------------------------------------------------------------------------


def extract_task_id_from_job(
    job_name: str,
    parameters: dict[str, str] | None = None,
) -> str | None:
    """Extract task ID from job name or build parameters."""
    # Check parameters first
    if parameters:
        for key in ("TASK_ID", "BLUEPRINT_TASK", "task_id"):
            if key in parameters:
                return parameters[key]

    # Check job name conventions
    prefixes = ("task_", "blueprint_", "bp_")
    for prefix in prefixes:
        if job_name.lower().startswith(prefix):
            return job_name[len(prefix):].split("_")[0]

    return None


# ---------------------------------------------------------------------------
# Main integration class
# ---------------------------------------------------------------------------


class JenkinsIntegration:
    """Manages Jenkins build events and job tracking."""

    def __init__(self, *, auth_token: str = ""):
        self._builds: dict[str, JenkinsBuild] = {}
        self._test_results: dict[str, TestResult] = {}
        self._artifacts: dict[str, list[BuildArtifact]] = {}
        self._stages: dict[str, list[PipelineStage]] = {}
        self._queue: list[dict[str, Any]] = []
        self._auth_token = auth_token

    # -- Webhook receiver ---------------------------------------------------

    def verify_auth_token(self, token: str) -> bool:
        """Verify Jenkins webhook authentication token."""
        if not self._auth_token:
            return True
        return token == self._auth_token

    def handle_build_event(self, event: dict[str, Any]) -> JenkinsBuild:
        """Handle a Jenkins build webhook notification."""
        build_data = event.get("build", event)
        job_name = event.get("name", event.get("job_name", ""))

        # Map Jenkins status
        phase = build_data.get("phase", "").upper()
        status_val = build_data.get("status", "").upper()

        if phase == "STARTED" or status_val == "":
            build_status = JenkinsBuildStatus.IN_PROGRESS
        elif status_val == "SUCCESS":
            build_status = JenkinsBuildStatus.STABLE
        elif status_val == "UNSTABLE":
            build_status = JenkinsBuildStatus.UNSTABLE
        elif status_val == "FAILURE":
            build_status = JenkinsBuildStatus.FAILED
        elif status_val == "ABORTED":
            build_status = JenkinsBuildStatus.ABORTED
        else:
            build_status = JenkinsBuildStatus.IN_PROGRESS

        # Extract parameters
        params: dict[str, str] = {}
        for p in build_data.get("parameters", []):
            if isinstance(p, dict) and "name" in p:
                params[p["name"]] = str(p.get("value", ""))

        task_id = extract_task_id_from_job(job_name, params) or ""

        build = JenkinsBuild(
            build_id=_gen_id("jbld"),
            job_name=job_name,
            build_number=build_data.get("number", 0),
            status=build_status,
            task_id=task_id,
            url=build_data.get("full_url", build_data.get("url", "")),
            duration_ms=build_data.get("duration"),
            parameters=params,
        )
        self._builds[build.build_id] = build
        return build

    # -- Test result parsing ------------------------------------------------

    def parse_test_results(
        self,
        build_id: str,
        report: dict[str, Any],
    ) -> TestResult:
        """Parse test results from a Jenkins test report."""
        failures: list[dict[str, str]] = []
        for suite in report.get("suites", []):
            for case in suite.get("cases", []):
                if case.get("status") in ("FAILED", "REGRESSION"):
                    failures.append({
                        "class_name": case.get("className", ""),
                        "test_name": case.get("name", ""),
                        "error": case.get("errorDetails", ""),
                    })

        result = TestResult(
            build_id=build_id,
            total_tests=report.get("totalCount", 0),
            passed=report.get("passCount", 0),
            failed=report.get("failCount", 0),
            skipped=report.get("skipCount", 0),
            duration_ms=int(report.get("duration", 0) * 1000),
            failure_details=failures,
        )
        self._test_results[build_id] = result
        return result

    # -- Artifacts ----------------------------------------------------------

    def register_artifact(
        self,
        build_id: str,
        filename: str,
        *,
        relative_path: str = "",
        size_bytes: int = 0,
        url: str = "",
    ) -> BuildArtifact:
        """Register a build artifact."""
        artifact = BuildArtifact(
            artifact_id=_gen_id("art"),
            build_id=build_id,
            filename=filename,
            relative_path=relative_path,
            size_bytes=size_bytes,
            url=url,
        )
        self._artifacts.setdefault(build_id, []).append(artifact)
        return artifact

    def get_artifacts(self, build_id: str) -> list[BuildArtifact]:
        return self._artifacts.get(build_id, [])

    # -- Pipeline stages ----------------------------------------------------

    def record_pipeline_stage(
        self,
        build_id: str,
        name: str,
        status: str,
        *,
        duration_ms: int = 0,
    ) -> PipelineStage:
        """Record a pipeline stage execution."""
        stage = PipelineStage(
            stage_id=_gen_id("stg"),
            build_id=build_id,
            name=name,
            status=status,
            duration_ms=duration_ms,
        )
        self._stages.setdefault(build_id, []).append(stage)
        return stage

    def get_stages(self, build_id: str) -> list[PipelineStage]:
        return self._stages.get(build_id, [])

    # -- Build queue --------------------------------------------------------

    def add_to_queue(self, job_name: str, parameters: dict[str, str] | None = None) -> dict[str, Any]:
        """Add a build to the queue."""
        entry = {
            "queue_id": _gen_id("q"),
            "job_name": job_name,
            "parameters": parameters or {},
            "queued_at": _now_iso(),
        }
        self._queue.append(entry)
        return entry

    def get_queue(self) -> list[dict[str, Any]]:
        return list(self._queue)

    # -- Query methods ------------------------------------------------------

    def get_build(self, build_id: str) -> JenkinsBuild | None:
        return self._builds.get(build_id)

    def get_builds_for_task(self, task_id: str) -> list[JenkinsBuild]:
        return [b for b in self._builds.values() if b.task_id == task_id]

    def list_builds(
        self,
        *,
        job_name: str | None = None,
        status: JenkinsBuildStatus | None = None,
    ) -> list[JenkinsBuild]:
        results = list(self._builds.values())
        if job_name:
            results = [b for b in results if b.job_name == job_name]
        if status:
            results = [b for b in results if b.status == status]
        return results

    def get_test_result(self, build_id: str) -> TestResult | None:
        return self._test_results.get(build_id)

    # -- Metrics ------------------------------------------------------------

    def calculate_build_metrics(self) -> JenkinsBuildMetrics:
        """Calculate aggregated build metrics."""
        builds = list(self._builds.values())
        if not builds:
            return JenkinsBuildMetrics(
                total_builds=0,
                stable_count=0,
                unstable_count=0,
                failed_count=0,
                success_rate=0.0,
                avg_duration_ms=0.0,
                flakiness_score=0.0,
            )

        stable = [b for b in builds if b.status == JenkinsBuildStatus.STABLE]
        unstable = [b for b in builds if b.status == JenkinsBuildStatus.UNSTABLE]
        failed = [b for b in builds if b.status == JenkinsBuildStatus.FAILED]
        durations = [b.duration_ms for b in builds if b.duration_ms is not None]
        avg_dur = sum(durations) / len(durations) if durations else 0.0

        total = len(builds)
        flakiness = len(unstable) / total if total > 0 else 0.0

        return JenkinsBuildMetrics(
            total_builds=total,
            stable_count=len(stable),
            unstable_count=len(unstable),
            failed_count=len(failed),
            success_rate=len(stable) / total if total > 0 else 0.0,
            avg_duration_ms=avg_dur,
            flakiness_score=flakiness,
        )


__all__ = [
    "JenkinsBuildStatus",
    "JenkinsBuild",
    "TestResult",
    "BuildArtifact",
    "PipelineStage",
    "JenkinsBuildMetrics",
    "JenkinsIntegration",
    "extract_task_id_from_job",
]
