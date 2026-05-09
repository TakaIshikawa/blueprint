"""Generate API deprecation readiness matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_secrets_rotation_readiness_matrix import (
    _candidate_texts,
    _dedupe,
    _evidence_snippet,
    _markdown_cell,
    _optional_text,
    _source_payload,
    _task_id,
)


ApiDeprecationReadiness = Literal["ready", "partial", "blocked"]

_READINESS_ORDER: dict[ApiDeprecationReadiness, int] = {"blocked": 0, "partial": 1, "ready": 2}

# Pattern matching for API deprecation signals
_DEPRECATION_RE = re.compile(
    r"\b(?:deprecat(?:e|es|ed|ing|ion)|sunset|retire|retiring|end[- ]of[- ]life|eol|"
    r"remove|removal|breaking change|obsolete|legacy)",
    re.I,
)
_API_RE = re.compile(
    r"\b(?:api|endpoint|route|rest|graphql|webhook|sdk|rpc|grpc|v[0-9]+|version|"
    r"grace period|dual[- ]write)",  # Include migration-specific patterns
    re.I,
)
_ENDPOINT_RE = re.compile(
    r"\b(?:endpoint|endpoints|route|routes|api (?:surface|path|method)|"
    r"/v[0-9]+|/api/|graphql|webhook)",
    re.I,
)
_TIMELINE_RE = re.compile(
    r"\b(?:sunset date|deadline|timeline|schedule|migration window|grace period|"
    r"notice period|deprecation date|removal date|end date|q[1-4]|[0-9]{4}-[0-9]{2})",
    re.I,
)
_MIGRATION_PATH_RE = re.compile(
    r"\b(?:migration (?:path|plan|guide|steps)|migrate to|replacement|alternative|"
    r"new version|v[0-9]+ replaces|upgrade (?:to|path)|transition)",
    re.I,
)
_REPLACEMENT_RE = re.compile(
    r"\b(?:replacement|alternative|new (?:api|endpoint|version)|"
    r"successor|v[0-9]+ (?:replaces|instead)|use .* instead)",
    re.I,
)
_BREAKING_CHANGE_RE = re.compile(
    r"\b(?:breaking change|incompatible|backwards incompatible|"
    r"non[- ]backward[s]?[- ]compatible|contract change)",
    re.I,
)
_CLIENT_IMPACT_RE = re.compile(
    r"\b(?:client|clients|consumer|consumers|customer|customers|partner|partners|"
    r"integration|integrations|sdk user|downstream|affected|impact|usage|traffic)",
    re.I,
)
_COMMUNICATION_RE = re.compile(
    r"\b(?:communication(?:\s+plan)?(?:\s+ready)?|notify|notification|announcement|email|changelog|"
    r"release notes|developer portal|blog|documentation|docs|guide)",
    re.I,
)
_GRACE_PERIOD_RE = re.compile(
    r"\b(?:grace period|migration window|overlap|compatibility window|"
    r"dual[- ](?:read|write)|parallel|transition period)",
    re.I,
)
_METRICS_RE = re.compile(
    r"\b(?:metric|metrics|monitor|monitoring|tracking|analytics|telemetry|"
    r"usage (?:data|stats)|adoption|traffic|dashboard|alert)",
    re.I,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|revert|restore|fallback|abort|disable|"
    r"extend (?:deadline|window)|unblock|re-enable)",
    re.I,
)


@dataclass(frozen=True, slots=True)
class ApiDeprecationMatrixRow:
    """API deprecation readiness signals for one execution task."""

    task_id: str
    title: str
    deprecated_endpoints: str = "missing"
    sunset_timeline: str = "missing"
    migration_path: str = "missing"
    replacement_api: str = "missing"
    breaking_changes: str = "missing"
    client_impact: str = "missing"
    communication_plan: str = "missing"
    grace_period: str = "missing"
    metrics_tracking: str = "missing"
    rollback_scenario: str = "missing"
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: ApiDeprecationReadiness = "partial"
    readiness_score: float = 0.5
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "deprecated_endpoints": self.deprecated_endpoints,
            "sunset_timeline": self.sunset_timeline,
            "migration_path": self.migration_path,
            "replacement_api": self.replacement_api,
            "breaking_changes": self.breaking_changes,
            "client_impact": self.client_impact,
            "communication_plan": self.communication_plan,
            "grace_period": self.grace_period,
            "metrics_tracking": self.metrics_tracking,
            "rollback_scenario": self.rollback_scenario,
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "readiness_score": self.readiness_score,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class ApiDeprecationMatrix:
    """Plan-level API deprecation readiness matrix."""

    plan_id: str | None = None
    rows: tuple[ApiDeprecationMatrixRow, ...] = field(default_factory=tuple)
    deprecation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_deprecation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)
    recommendations: tuple[str, ...] = field(default_factory=tuple)

    @property
    def records(self) -> tuple[ApiDeprecationMatrixRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "deprecation_task_ids": list(self.deprecation_task_ids),
            "no_deprecation_task_ids": list(self.no_deprecation_task_ids),
            "summary": dict(self.summary),
            "recommendations": list(self.recommendations),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return deprecation rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the API deprecation matrix as deterministic Markdown."""
        title = "# API Deprecation Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"

        lines = [title, ""]

        if not self.rows:
            lines.append("No API deprecation tasks were identified.")
            return "\n".join(lines)

        # Main matrix table
        lines.extend([
            "## Deprecation Readiness",
            "",
            "| Task | Endpoints | Timeline | Migration Path | Replacement | Client Impact | Communication | Readiness | Score |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ])

        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(row.task_id)}` | "
                f"{row.deprecated_endpoints} | "
                f"{row.sunset_timeline} | "
                f"{row.migration_path} | "
                f"{row.replacement_api} | "
                f"{row.client_impact} | "
                f"{row.communication_plan} | "
                f"{row.readiness} | "
                f"{row.readiness_score:.1f} |"
            )

        # Add gaps section
        lines.extend(["", "## Gaps and Recommendations", ""])

        blocked_rows = [r for r in self.rows if r.readiness == "blocked"]
        partial_rows = [r for r in self.rows if r.readiness == "partial"]

        if blocked_rows:
            lines.append("### Blocked Tasks")
            for row in blocked_rows:
                lines.append(f"- **{row.task_id}**: {'; '.join(row.gaps)}")
            lines.append("")

        if partial_rows:
            lines.append("### Partial Readiness")
            for row in partial_rows:
                if row.gaps:
                    lines.append(f"- **{row.task_id}**: {'; '.join(row.gaps)}")
            lines.append("")

        if self.recommendations:
            lines.append("### General Recommendations")
            for rec in self.recommendations:
                lines.append(f"- {rec}")

        return "\n".join(lines)


class ApiDeprecationMatrixGenerator:
    """Generator for API deprecation matrices from execution plans."""

    def generate(
        self,
        source: (
            Mapping[str, Any]
            | ExecutionPlan
            | ExecutionTask
            | Iterable[Mapping[str, Any] | ExecutionTask]
        ),
    ) -> ApiDeprecationMatrix:
        """
        Generate API deprecation matrix from execution plan or tasks.

        Args:
            source: Execution plan, task, or iterable of tasks

        Returns:
            ApiDeprecationMatrix with readiness assessment
        """
        return build_api_deprecation_matrix(source)

    def extract_deprecation_signals(
        self,
        task: Mapping[str, Any],
    ) -> dict[str, str]:
        """
        Extract deprecation signals from a task.

        Args:
            task: Task mapping

        Returns:
            Dictionary of signal names to status ("present" or "missing")
        """
        texts = _candidate_texts(task)
        return {
            "deprecated_endpoints": _status(_ENDPOINT_RE, texts),
            "sunset_timeline": _status(_TIMELINE_RE, texts),
            "migration_path": _status(_MIGRATION_PATH_RE, texts),
            "replacement_api": _status(_REPLACEMENT_RE, texts),
            "breaking_changes": _status(_BREAKING_CHANGE_RE, texts),
            "client_impact": _status(_CLIENT_IMPACT_RE, texts),
            "communication_plan": _status(_COMMUNICATION_RE, texts),
            "grace_period": _status(_GRACE_PERIOD_RE, texts),
            "metrics_tracking": _status(_METRICS_RE, texts),
            "rollback_scenario": _status(_ROLLBACK_RE, texts),
        }

    def calculate_readiness_score(self, signals: dict[str, str]) -> float:
        """
        Calculate deprecation readiness score.

        Args:
            signals: Dictionary of signal names to status

        Returns:
            Score from 0.0 to 1.0 based on present signals
        """
        # Weight different signals by importance
        weights = {
            "deprecated_endpoints": 1.5,  # Critical
            "sunset_timeline": 1.5,  # Critical
            "client_impact": 1.5,  # Critical
            "communication_plan": 1.5,  # Critical
            "migration_path": 1.2,  # Important
            "replacement_api": 1.2,  # Important
            "grace_period": 1.0,
            "metrics_tracking": 1.0,
            "rollback_scenario": 1.0,
            "breaking_changes": 0.8,  # Nice to have explicit mention
        }

        total_weight = sum(weights.values())
        achieved_weight = sum(
            weights[signal]
            for signal, status in signals.items()
            if status == "present" and signal in weights
        )

        return achieved_weight / total_weight if total_weight > 0 else 0.0


def build_api_deprecation_matrix(source: Any) -> ApiDeprecationMatrix:
    """
    Build API deprecation readiness matrix from execution plan or tasks.

    Args:
        source: Execution plan, task, or iterable of tasks

    Returns:
        ApiDeprecationMatrix with readiness assessment
    """
    plan_id, tasks = _source_payload(source)
    rows: list[ApiDeprecationMatrixRow] = []
    no_deprecation_task_ids: list[str] = []

    generator = ApiDeprecationMatrixGenerator()

    for index, task in enumerate(tasks, start=1):
        row = _task_row(task, index, generator)
        if row:
            rows.append(row)
        else:
            no_deprecation_task_ids.append(_task_id(task, index))

    # Sort by task_id to maintain stable ordering
    rows.sort(key=lambda row: row.task_id)
    result = tuple(rows)

    recommendations = _generate_recommendations(result)

    return ApiDeprecationMatrix(
        plan_id=plan_id,
        rows=result,
        deprecation_task_ids=tuple(row.task_id for row in result),
        no_deprecation_task_ids=tuple(no_deprecation_task_ids),
        summary=_summary(len(tasks), result),
        recommendations=recommendations,
    )


def generate_api_deprecation_matrix(source: Any) -> ApiDeprecationMatrix:
    """Generate API deprecation matrix from execution plan or tasks."""
    return build_api_deprecation_matrix(source)


def analyze_api_deprecation_matrix(source: Any) -> ApiDeprecationMatrix:
    """Analyze source and return API deprecation matrix."""
    if isinstance(source, ApiDeprecationMatrix):
        return source
    return build_api_deprecation_matrix(source)


def api_deprecation_matrix_to_dict(matrix: ApiDeprecationMatrix) -> dict[str, Any]:
    """Serialize API deprecation matrix to dictionary."""
    return matrix.to_dict()


api_deprecation_matrix_to_dict.__test__ = False


def api_deprecation_matrix_to_dicts(
    matrix: ApiDeprecationMatrix | Iterable[ApiDeprecationMatrixRow],
) -> list[dict[str, Any]]:
    """Serialize API deprecation matrix rows to list of dictionaries."""
    if isinstance(matrix, ApiDeprecationMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


api_deprecation_matrix_to_dicts.__test__ = False


def api_deprecation_matrix_to_markdown(matrix: ApiDeprecationMatrix) -> str:
    """Render API deprecation matrix as Markdown."""
    return matrix.to_markdown()


api_deprecation_matrix_to_markdown.__test__ = False


def _task_row(
    task: Mapping[str, Any],
    index: int,
    generator: ApiDeprecationMatrixGenerator,
) -> ApiDeprecationMatrixRow | None:
    """Extract API deprecation row from a task if relevant."""
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)

    # Check if task is related to API deprecation
    # Accept if either deprecated explicitly OR has API + migration/sunset signals
    has_deprecation = _DEPRECATION_RE.search(context)
    has_api = _API_RE.search(context)
    has_migration = _MIGRATION_PATH_RE.search(context) or _TIMELINE_RE.search(context)

    if not (has_api and (has_deprecation or has_migration)):
        return None

    # Extract signals
    signals = generator.extract_deprecation_signals(task)

    # Define labels for missing signals (only important ones count as gaps)
    # breaking_changes is nice-to-have but not required for readiness
    important_labels = {
        "deprecated_endpoints": "deprecated endpoint specification",
        "sunset_timeline": "sunset timeline or deadline",
        "migration_path": "migration path documentation",
        "replacement_api": "replacement API specification",
        "client_impact": "client impact assessment",
        "communication_plan": "communication plan",
        "grace_period": "grace period or overlap window",
        "metrics_tracking": "metrics tracking plan",
        "rollback_scenario": "rollback scenario",
    }

    # Calculate gaps (excluding breaking_changes as it's nice-to-have)
    gaps = tuple(
        f"Missing {label}."
        for field, label in important_labels.items()
        if signals[field] == "missing"
    )

    # Calculate readiness score
    readiness_score = generator.calculate_readiness_score(signals)

    # Determine overall readiness
    readiness: ApiDeprecationReadiness = "ready"

    # Critical requirements for API deprecation
    critical_fields = {
        "deprecated_endpoints": signals["deprecated_endpoints"],
        "sunset_timeline": signals["sunset_timeline"],
        "client_impact": signals["client_impact"],
        "communication_plan": signals["communication_plan"],
    }

    critical_missing_count = sum(
        1 for status in critical_fields.values() if status == "missing"
    )

    # Blocked if 2 or more critical fields are missing
    if critical_missing_count >= 2:
        readiness = "blocked"
    # Partial if any critical field is missing or readiness score is low
    elif critical_missing_count > 0 or readiness_score < 0.7:
        readiness = "partial"

    # Extract evidence
    evidence = tuple(
        _dedupe(
            _evidence_snippet(field, text)
            for field, text in texts
            if _API_RE.search(text) or _DEPRECATION_RE.search(text)
        )
    )

    return ApiDeprecationMatrixRow(
        task_id=_task_id(task, index),
        title=_optional_text(task.get("title")) or _task_id(task, index),
        gaps=gaps,
        readiness=readiness,
        readiness_score=readiness_score,
        evidence=evidence,
        **signals,
    )


def _status(pattern: re.Pattern[str], texts: Iterable[tuple[str, str]]) -> str:
    """Check if pattern matches any text, return 'present' or 'missing'."""
    return "present" if any(pattern.search(text) for _, text in texts) else "missing"


def _summary(task_count: int, rows: Iterable[ApiDeprecationMatrixRow]) -> dict[str, Any]:
    """Generate summary statistics for the deprecation matrix."""
    row_list = list(rows)

    avg_score = (
        sum(row.readiness_score for row in row_list) / len(row_list)
        if row_list
        else 0.0
    )

    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "deprecation_task_count": len(row_list),
        "no_deprecation_task_count": task_count - len(row_list),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "average_readiness_score": round(avg_score, 2),
        "blocked_count": sum(1 for row in row_list if row.readiness == "blocked"),
        "partial_count": sum(1 for row in row_list if row.readiness == "partial"),
        "ready_count": sum(1 for row in row_list if row.readiness == "ready"),
    }


def _generate_recommendations(rows: tuple[ApiDeprecationMatrixRow, ...]) -> tuple[str, ...]:
    """Generate recommendations for smooth API deprecation processes."""
    recommendations: list[str] = []

    if not rows:
        return tuple(recommendations)

    # Check for common gaps
    blocked_count = sum(1 for row in rows if row.readiness == "blocked")
    missing_timelines = sum(
        1 for row in rows if row.sunset_timeline == "missing"
    )
    missing_communication = sum(
        1 for row in rows if row.communication_plan == "missing"
    )
    missing_client_impact = sum(
        1 for row in rows if row.client_impact == "missing"
    )
    missing_migration = sum(
        1 for row in rows if row.migration_path == "missing"
    )

    if blocked_count > 0:
        recommendations.append(
            f"{blocked_count} task(s) are blocked. Address critical gaps before proceeding."
        )

    if missing_timelines > len(rows) * 0.3:
        recommendations.append(
            "Establish clear sunset timelines with specific dates for all deprecations."
        )

    if missing_communication > len(rows) * 0.3:
        recommendations.append(
            "Develop communication plans including changelogs, emails, and developer documentation."
        )

    if missing_client_impact > len(rows) * 0.3:
        recommendations.append(
            "Assess client impact by identifying affected consumers and usage patterns."
        )

    if missing_migration > len(rows) * 0.3:
        recommendations.append(
            "Document migration paths with step-by-step guides for transitioning to replacement APIs."
        )

    # General best practices
    recommendations.extend([
        "Ensure adequate grace periods for clients to migrate (typically 3-6 months).",
        "Implement metrics tracking to monitor deprecated API usage and migration progress.",
        "Prepare rollback scenarios in case deprecation needs to be postponed.",
        "Coordinate with support teams to handle client questions during transition.",
    ])

    return tuple(recommendations)


__all__ = [
    "ApiDeprecationMatrixGenerator",
    "ApiDeprecationMatrix",
    "ApiDeprecationMatrixRow",
    "ApiDeprecationReadiness",
    "build_api_deprecation_matrix",
    "generate_api_deprecation_matrix",
    "analyze_api_deprecation_matrix",
    "api_deprecation_matrix_to_dict",
    "api_deprecation_matrix_to_dicts",
    "api_deprecation_matrix_to_markdown",
]
