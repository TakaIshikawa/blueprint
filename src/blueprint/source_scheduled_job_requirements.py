"""Extract source-level scheduled job requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ScheduledJobConcern = Literal[
    "cadence",
    "timezone",
    "retry",
    "missed_run",
    "concurrency",
    "rerun",
    "idempotency",
    "ownership",
]
ScheduledJobConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CONCERN_ORDER: tuple[ScheduledJobConcern, ...] = (
    "cadence",
    "timezone",
    "retry",
    "missed_run",
    "concurrency",
    "rerun",
    "idempotency",
    "ownership",
)
_CONFIDENCE_ORDER: dict[ScheduledJobConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_PLANNING_NOTES: dict[ScheduledJobConcern, str] = {
    "cadence": "Define the schedule trigger, cron expression, execution window, and scheduler source of truth.",
    "timezone": "Specify UTC versus local-time behavior, DST handling, and tenant or region timezone inputs.",
    "retry": "Plan retry attempts, backoff, terminal failure handling, and alerting after exhaustion.",
    "missed_run": "Define whether missed runs are caught up, skipped, coalesced, or escalated after downtime.",
    "concurrency": "Enforce overlap prevention, locks, max concurrency, and behavior for long-running executions.",
    "rerun": "Provide controlled manual rerun, replay, or backfill paths with authorization and audit evidence.",
    "idempotency": "Make scheduled executions idempotent with dedupe keys, replay safety, and duplicate suppression.",
    "ownership": "Assign operational owner, on-call path, runbook, alerts, and escalation responsibility.",
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_CRON_RE = re.compile(
    r"(?<!\S)(?:cron(?: expression| schedule)?\s*:?\s*)?"
    r"((?:\S+\s+){4,6}\S+)(?!\S)",
    re.I,
)
_CRON_TOKEN_RE = re.compile(r"^(?:\*|\?|\d{1,2}|\d{1,2}-\d{1,2}|\d{1,2}/\d{1,2}|\*/\d{1,2}|[A-Z]{3})(?:,(?:\*|\?|\d{1,2}|\d{1,2}-\d{1,2}|\d{1,2}/\d{1,2}|\*/\d{1,2}|[A-Z]{3}))*$", re.I)
_JOB_CONTEXT_RE = re.compile(
    r"\b(?:cron|crontab|schedule(?:d|r)? jobs?|scheduled task|recurring jobs?|periodic jobs?|"
    r"background jobs?|batch jobs?|worker jobs?|nightly jobs?|daily jobs?|hourly jobs?|"
    r"maintenance jobs?|sync jobs?|reconciliation jobs?|digest jobs?|etl jobs?|scheduler|"
    r"[a-z][a-z0-9_-]*(?:\s+[a-z][a-z0-9_-]*){0,3}\s+jobs?|"
    r"run every|runs every|run nightly|runs nightly|run daily|runs daily|run hourly|runs hourly|"
    r"execute every|executes every|fire every|fires every|backfill job|manual rerun)\b",
    re.I,
)
_INCIDENTAL_SCHEDULE_RE = re.compile(
    r"\b(?:project|release|delivery|planning|roadmap|meeting|interview|launch|milestone|timeline|staff|team)\s+schedule\b|"
    r"\bschedule\s+(?:a meeting|meetings|interviews?|review|reviews?|planning|launch|release|workshop)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:cron|schedule|scheduled[_ -]?job|scheduler|cadence|time[_ -]?window|timezone|time[_ -]?zone|"
    r"retry|missed[_ -]?run|misfire|concurrency|overlap|rerun|re[_ -]?run|backfill|replay|"
    r"idempot|dedupe|owner|ownership|on[_ -]?call|runbook|operations?|ops)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|support|allow|"
    r"provide|define|document|record|track|audit|alert|notify|page|retry|rerun|backfill|skip|"
    r"catch up|prevent|limit|lock|dedupe|idempotent|acceptance|done when|cannot ship)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,140}\b(?:cron|scheduled jobs?|background jobs?|recurring jobs?|"
    r"batch jobs?|scheduler|schedule semantics)\b.{0,140}\b(?:required|needed|in scope|planned|changes?|impact|work)\b|"
    r"\b(?:cron|scheduled jobs?|background jobs?|recurring jobs?|batch jobs?|scheduler|schedule semantics)\b"
    r".{0,140}\b(?:not required|not needed|out of scope|no changes?|no work|non[- ]?goal)\b",
    re.I,
)
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "scope",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "operations",
    "operational_requirements",
    "scheduler",
    "schedules",
    "scheduled_jobs",
    "cron",
    "jobs",
    "background_jobs",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
    "domain",
    "status",
}
_CONCERN_PATTERNS: dict[ScheduledJobConcern, re.Pattern[str]] = {
    "cadence": re.compile(
        r"\b(?:cron|cadence|schedule|every\s+\d+\s+(?:minutes?|hours?|days?|weeks?|months?)|"
        r"hourly|daily|nightly|weekly|monthly|quarterly|annually|weekday|weekend|"
        r"at\s+\d{1,2}:\d{2}|time window|maintenance window)\b",
        re.I,
    ),
    "timezone": re.compile(r"\b(?:timezone|time zone|utc|local time|tenant time|regional time|dst|daylight saving)\b", re.I),
    "retry": re.compile(r"\b(?:retry|retries|backoff|max attempts?|attempts?|exponential backoff|dead letter|dlq|terminal failure)\b", re.I),
    "missed_run": re.compile(r"\b(?:missed runs?|missed executions?|misfire|downtime|catch up|catch-up|skip missed|late runs?|coalesce)\b", re.I),
    "concurrency": re.compile(r"\b(?:concurrency|concurrent|overlap|overlapping|single instance|mutex|lock|lease|max parallel|max concurrent)\b", re.I),
    "rerun": re.compile(r"\b(?:manual rerun|rerun|re-run|run now|replay|backfill|restart job|operator trigger)\b", re.I),
    "idempotency": re.compile(r"\b(?:idempotent|idempotency|dedupe|deduplicate|duplicate safe|replay safe|exactly once|at least once)\b", re.I),
    "ownership": re.compile(r"\b(?:owner|ownership|owned by|on-call|on call|runbook|ops|operations|sre|support|alert|page|escalate)\b", re.I),
}
_FIELD_CONCERN_PATTERNS: dict[ScheduledJobConcern, re.Pattern[str]] = {
    concern: re.compile(concern.replace("_", r"[_ -]?"), re.I) for concern in _CONCERN_ORDER
}
_CADENCE_DETAIL_RE = re.compile(
    r"\b(?:cron(?: expression| schedule)?|cadence|schedule|runs?|run|executes?|execute)\s*:?\s*"
    r"([^.;\n]*(?:\*/\d+|every\s+\d+\s+(?:minutes?|hours?|days?|weeks?|months?)|hourly|daily|nightly|weekly|monthly|quarterly|annually|weekday|weekend|at\s+\d{1,2}:\d{2})[^.;\n]*)",
    re.I,
)
_TIME_WINDOW_RE = re.compile(r"\b(?:time window|maintenance window|between|from)\s*:?\s*([^.;\n]*(?:\d{1,2}:\d{2}|am|pm|utc|local)[^.;\n]*)", re.I)
_TIMEZONE_DETAIL_RE = re.compile(r"\b(?:timezone|time zone|in|using)\s*:?\s*((?:the\s+)?(?:UTC|local time|tenant time(?:zone)?|account time(?:zone)?|regional time|[A-Z][A-Za-z_]+/[A-Z][A-Za-z_]+|DST|daylight saving)[^.;,\n]*)", re.I)
_RETRY_DETAIL_RE = re.compile(r"\b(?:retry|retries|retry policy|backoff|max attempts?)\s*:?\s*([^.;,\n]*(?:\d+\s+attempts?|attempts?|backoff|exponential|linear|dead letter|dlq)[^.;,\n]*)", re.I)
_MISSED_DETAIL_RE = re.compile(r"\b(?:missed runs?|missed executions?|misfire|downtime)\s*:?\s*([^.;,\n]*(?:catch up|catch-up|skip|coalesce|replay|backfill|alert|escalate)[^.;,\n]*)", re.I)
_CONCURRENCY_DETAIL_RE = re.compile(r"\b(?:concurrency|concurrent|overlapping|overlap|lock|single instance|max concurrent)\s*:?\s*([^.;,\n]*(?:single|one|1|lock|mutex|lease|prevent|limit|max|concurrent|overlap)[^.;,\n]*)", re.I)
_RERUN_DETAIL_RE = re.compile(r"\b(?:manual rerun|rerun|re-run|run now|replay|backfill)\s*:?\s*([^.;,\n]*(?:manual|operator|admin|ops|authorized|audit|replay|backfill|run now)[^.;,\n]*)", re.I)
_IDEMPOTENCY_DETAIL_RE = re.compile(r"\b(?:idempotent|idempotency|dedupe|deduplicate|duplicate safe|replay safe)\s*:?\s*([^.;,\n]*(?:idempotent|dedupe|duplicate|replay safe|key|exactly once|at least once)[^.;,\n]*)", re.I)
_OWNER_DETAIL_RE = re.compile(r"\b(?:owner|owned by|ownership|on-call|on call|runbook|escalate to)\s*:?\s*([^.;,\n]*(?:ops|operations|sre|platform|data|support|finance|compliance|team|on-call|on call)[^.;,\n]*)", re.I)
_PAGE_OWNER_RE = re.compile(r"\b(?:page|alert|notify|escalate to)\s+(?:the\s+)?([^.;,\n]*(?:ops|operations|sre|platform|data|support|finance|compliance|team|on-call|on call)[^.;,\n]*)", re.I)


@dataclass(frozen=True, slots=True)
class SourceScheduledJobRequirement:
    """One source-backed scheduled job requirement."""

    source_brief_id: str | None
    concern: ScheduledJobConcern
    requirement_text: str
    cadence: str | None = None
    cron_expression: str | None = None
    time_window: str | None = None
    timezone_behavior: str | None = None
    retry_policy: str | None = None
    missed_run_handling: str | None = None
    concurrency_limit: str | None = None
    idempotency_expectation: str | None = None
    rerun_control: str | None = None
    owner: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: ScheduledJobConfidence = "medium"
    planning_note: str = ""

    @property
    def requirement_category(self) -> ScheduledJobConcern:
        """Compatibility alias matching category-oriented reports."""
        return self.concern

    @property
    def planning_notes(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting plural planning notes."""
        return (self.planning_note,)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "concern": self.concern,
            "requirement_text": self.requirement_text,
            "cadence": self.cadence,
            "cron_expression": self.cron_expression,
            "time_window": self.time_window,
            "timezone_behavior": self.timezone_behavior,
            "retry_policy": self.retry_policy,
            "missed_run_handling": self.missed_run_handling,
            "concurrency_limit": self.concurrency_limit,
            "idempotency_expectation": self.idempotency_expectation,
            "rerun_control": self.rerun_control,
            "owner": self.owner,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceScheduledJobRequirementsReport:
    """Source-level scheduled job requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceScheduledJobRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceScheduledJobRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceScheduledJobRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return scheduled job requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Scheduled Job Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        concern_counts = self.summary.get("concern_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Concern counts: "
            + ", ".join(f"{concern} {concern_counts.get(concern, 0)}" for concern in _CONCERN_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No scheduled job requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Concern | Requirement | Cadence | Cron | Window | Timezone | Retry | Missed Runs | Concurrency | Idempotency | Rerun | Owner | Source Field | Matched Terms | Confidence | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.concern)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.cadence or '')} | "
                f"{_markdown_cell(requirement.cron_expression or '')} | "
                f"{_markdown_cell(requirement.time_window or '')} | "
                f"{_markdown_cell(requirement.timezone_behavior or '')} | "
                f"{_markdown_cell(requirement.retry_policy or '')} | "
                f"{_markdown_cell(requirement.missed_run_handling or '')} | "
                f"{_markdown_cell(requirement.concurrency_limit or '')} | "
                f"{_markdown_cell(requirement.idempotency_expectation or '')} | "
                f"{_markdown_cell(requirement.rerun_control or '')} | "
                f"{_markdown_cell(requirement.owner or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(', '.join(requirement.matched_terms))} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_scheduled_job_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceScheduledJobRequirementsReport:
    """Extract source-level scheduled job requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceScheduledJobRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_scheduled_job_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceScheduledJobRequirementsReport:
    """Compatibility alias for building a scheduled job requirements report."""
    return build_source_scheduled_job_requirements(source)


def generate_source_scheduled_job_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceScheduledJobRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_scheduled_job_requirements(source)


def derive_source_scheduled_job_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceScheduledJobRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_scheduled_job_requirements(source)


def summarize_source_scheduled_job_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceScheduledJobRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted scheduled job requirements."""
    if isinstance(source_or_result, SourceScheduledJobRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_scheduled_job_requirements(source_or_result).summary


def source_scheduled_job_requirements_to_dict(report: SourceScheduledJobRequirementsReport) -> dict[str, Any]:
    """Serialize a scheduled job requirements report to a plain dictionary."""
    return report.to_dict()


source_scheduled_job_requirements_to_dict.__test__ = False


def source_scheduled_job_requirements_to_dicts(
    requirements: (
        tuple[SourceScheduledJobRequirement, ...]
        | list[SourceScheduledJobRequirement]
        | SourceScheduledJobRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize scheduled job requirement records to dictionaries."""
    if isinstance(requirements, SourceScheduledJobRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_scheduled_job_requirements_to_dicts.__test__ = False


def source_scheduled_job_requirements_to_markdown(report: SourceScheduledJobRequirementsReport) -> str:
    """Render a scheduled job requirements report as Markdown."""
    return report.to_markdown()


source_scheduled_job_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    concern: ScheduledJobConcern
    requirement_text: str
    cadence: str | None
    cron_expression: str | None
    time_window: str | None
    timezone_behavior: str | None
    retry_policy: str | None
    missed_run_handling: str | None
    concurrency_limit: str | None
    idempotency_expectation: str | None
    rerun_control: str | None
    owner: str | None
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: ScheduledJobConfidence


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_id(payload), payload
    return None, {}


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return _optional_text(payload.get("id")) or _optional_text(payload.get("source_brief_id")) or _optional_text(payload.get("source_id"))


def _candidates_for_briefs(brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            searchable = _searchable_text(segment.source_field, segment.text)
            if _NEGATED_RE.search(searchable) or _is_incidental_schedule(segment):
                continue
            concerns = _concerns(segment)
            for concern in concerns:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        concern=concern,
                        requirement_text=_requirement_text(segment.text),
                        cadence=_field_value_detail("cadence", segment.text)
                        or _field_value_detail("schedule", segment.text)
                        or _match_detail(_CADENCE_DETAIL_RE, segment.text)
                        or _natural_cadence(segment.text),
                        cron_expression=_cron_expression(segment.text),
                        time_window=_field_value_detail("time_window", segment.text) or _match_detail(_TIME_WINDOW_RE, segment.text),
                        timezone_behavior=_field_value_detail("timezone", segment.text)
                        or _field_value_detail("time_zone", segment.text)
                        or _match_detail(_TIMEZONE_DETAIL_RE, segment.text)
                        or _detail(_CONCERN_PATTERNS["timezone"], segment.text),
                        retry_policy=_field_value_detail("retry_policy", segment.text)
                        or _field_value_detail("retry", segment.text)
                        or _match_detail(_RETRY_DETAIL_RE, segment.text),
                        missed_run_handling=_field_value_detail("missed_run_handling", segment.text)
                        or _field_value_detail("missed_runs", segment.text)
                        or _match_detail(_MISSED_DETAIL_RE, segment.text)
                        or _detail(_CONCERN_PATTERNS["missed_run"], segment.text),
                        concurrency_limit=_field_value_detail("concurrency_limit", segment.text)
                        or _field_value_detail("concurrency", segment.text)
                        or _match_detail(_CONCURRENCY_DETAIL_RE, segment.text),
                        idempotency_expectation=_field_value_detail("idempotency", segment.text)
                        or _match_detail(_IDEMPOTENCY_DETAIL_RE, segment.text),
                        rerun_control=_field_value_detail("rerun_control", segment.text)
                        or _field_value_detail("rerun", segment.text)
                        or _match_detail(_RERUN_DETAIL_RE, segment.text),
                        owner=_field_value_detail("owner", segment.text)
                        or _field_value_detail("ownership", segment.text)
                        or _match_detail(_OWNER_DETAIL_RE, segment.text)
                        or _match_detail(_PAGE_OWNER_RE, segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        matched_terms=_matched_terms(concern, searchable),
                        confidence=_confidence(concern, segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceScheduledJobRequirement]:
    grouped: dict[tuple[str | None, ScheduledJobConcern, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.source_brief_id, candidate.concern, _dedupe_requirement_key(candidate.requirement_text)),
            [],
        ).append(candidate)

    requirements: list[SourceScheduledJobRequirement] = []
    for (_source_brief_id, concern, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceScheduledJobRequirement(
                source_brief_id=best.source_brief_id,
                concern=concern,
                requirement_text=best.requirement_text,
                cadence=_first_detail(item.cadence for item in items),
                cron_expression=_first_detail(item.cron_expression for item in items),
                time_window=_first_detail(item.time_window for item in items),
                timezone_behavior=_first_detail(item.timezone_behavior for item in items),
                retry_policy=_first_detail(item.retry_policy for item in items),
                missed_run_handling=_first_detail(item.missed_run_handling for item in items),
                concurrency_limit=_first_detail(item.concurrency_limit for item in items),
                idempotency_expectation=_first_detail(item.idempotency_expectation for item in items),
                rerun_control=_first_detail(item.rerun_control for item in items),
                owner=_first_detail(item.owner for item in items),
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                matched_terms=tuple(sorted(_dedupe(term for item in items for term in item.matched_terms), key=str.casefold)),
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                planning_note=_PLANNING_NOTES[concern],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _CONCERN_ORDER.index(requirement.concern),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.requirement_text.casefold(),
            requirement.source_field or "",
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        if _has_structured_shape(value):
            for evidence in _structured_segments(value):
                segments.append(_Segment(source_field, evidence, True))
            return
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text) or _JOB_CONTEXT_RE.search(key_text))
            _append_value(segments, f"{source_field}.{key}", value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        raw_text = str(value) if isinstance(value, str) else text
        for segment_text, segment_context in _segments(raw_text, field_context):
            segments.append(_Segment(source_field, segment_text, segment_context))


def _segments(value: str, inherited_context: bool) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    section_context = inherited_context
    for raw_line in value.splitlines() or [value]:
        line = raw_line.strip()
        if not line:
            continue
        heading = _HEADING_RE.match(line)
        if heading:
            title = _clean_text(heading.group("title"))
            section_context = inherited_context or bool(_JOB_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if _JOB_CONTEXT_RE.search(part) or _cron_expression(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text and not _NEGATED_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _concerns(segment: _Segment) -> tuple[ScheduledJobConcern, ...]:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    if _is_incidental_schedule(segment):
        return ()
    has_job_context = bool(_JOB_CONTEXT_RE.search(searchable) or _cron_expression(segment.text))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    if not (has_job_context or has_structured_context):
        return ()
    if not (_REQUIREMENT_RE.search(searchable) or has_structured_context or _cron_expression(segment.text)):
        return ()
    field_concerns = [concern for concern in _CONCERN_ORDER if _FIELD_CONCERN_PATTERNS[concern].search(field_words)]
    text_concerns = [concern for concern in _CONCERN_ORDER if _CONCERN_PATTERNS[concern].search(segment.text)]
    if _cron_expression(segment.text) and "cadence" not in text_concerns:
        text_concerns.insert(0, "cadence")
    if "cadence" in text_concerns and not _has_actionable_cadence(segment.text):
        text_concerns = [concern for concern in text_concerns if concern != "cadence"]
    if "ownership" in field_concerns + text_concerns and not has_job_context and not re.search(r"\b(?:cron|schedule|scheduler|job)\b", field_words, re.I):
        field_concerns = [concern for concern in field_concerns if concern != "ownership"]
        text_concerns = [concern for concern in text_concerns if concern != "ownership"]
    return tuple(_dedupe(field_concerns + text_concerns))


def _has_actionable_cadence(text: str) -> bool:
    if _cron_expression(text):
        return True
    return bool(
        re.search(
            r"\b(?:cron|cadence|schedule|(?:must|should|shall|to)?\s*run|executes?|execute|every\s+\d+\s+(?:minutes?|hours?|days?|weeks?|months?)|at\s+\d{1,2}:\d{2}|time window|maintenance window)\b",
            text,
            re.I,
        )
    )


def _confidence(concern: ScheduledJobConcern, segment: _Segment) -> ScheduledJobConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(segment.text))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    has_concern = bool(_CONCERN_PATTERNS[concern].search(segment.text) or _FIELD_CONCERN_PATTERNS[concern].search(field_words))
    detail_count = sum(
        1
        for value in (
            _cron_expression(segment.text),
            _natural_cadence(segment.text),
            _match_detail(_TIMEZONE_DETAIL_RE, searchable),
            _match_detail(_RETRY_DETAIL_RE, segment.text),
            _match_detail(_CONCURRENCY_DETAIL_RE, segment.text),
            _match_detail(_OWNER_DETAIL_RE, segment.text),
        )
        if value
    )
    if has_concern and has_explicit_requirement and has_structured_context and detail_count >= 1:
        return "high"
    if has_concern and (has_explicit_requirement or has_structured_context):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceScheduledJobRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "concern_counts": {
            concern: sum(1 for requirement in requirements if requirement.concern == concern)
            for concern in _CONCERN_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "concerns": [
            concern
            for concern in _CONCERN_ORDER
            if any(requirement.concern == concern for requirement in requirements)
        ],
        "has_cron_expression": any(requirement.cron_expression for requirement in requirements),
        "requires_timezone_behavior": any(requirement.timezone_behavior for requirement in requirements),
        "requires_retry_policy": any(requirement.retry_policy for requirement in requirements),
        "requires_missed_run_handling": any(requirement.missed_run_handling for requirement in requirements),
        "requires_concurrency_limit": any(requirement.concurrency_limit for requirement in requirements),
        "requires_rerun_control": any(requirement.rerun_control for requirement in requirements),
        "requires_idempotency": any(requirement.idempotency_expectation for requirement in requirements),
        "requires_ownership": any(requirement.owner for requirement in requirements),
        "status": "ready_for_scheduled_job_planning" if requirements else "no_scheduled_job_language",
    }


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    if any(isinstance(value, (Mapping, list, tuple, set)) for value in item.values()):
        return False
    return bool(
        keys
        & {
            "concern",
            "requirement_category",
            "cadence",
            "cron",
            "cron_expression",
            "time_window",
            "timezone",
            "time_zone",
            "retry",
            "retry_policy",
            "missed_runs",
            "missed_run_handling",
            "concurrency",
            "concurrency_limit",
            "rerun",
            "rerun_control",
            "idempotency",
            "owner",
            "ownership",
        }
    )


def _structured_segments(item: Mapping[str, Any]) -> list[str]:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(value)
        if text:
            parts.append(f"{key}: {text}")
    return ["; ".join(parts)] if parts else []


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "target_user",
        "buyer",
        "summary",
        "body",
        "description",
        "problem",
        "problem_statement",
        "goal",
        "goals",
        "mvp_goal",
        "context",
        "workflow_context",
        "product_surface",
        "requirements",
        "constraints",
        "scope",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "operations",
        "operational_requirements",
        "scheduler",
        "schedules",
        "scheduled_jobs",
        "cron",
        "jobs",
        "background_jobs",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, int]:
    return (
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int("[" in candidate.source_field),
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        -_source_index(candidate.source_field),
    )


def _is_incidental_schedule(segment: _Segment) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    if _JOB_CONTEXT_RE.search(searchable) or _cron_expression(segment.text):
        return False
    return bool(_INCIDENTAL_SCHEDULE_RE.search(searchable))


def _cron_expression(text: str) -> str | None:
    raw_tokens = [_clean_text(token).strip("'\"`.,;:()[]") for token in text.split()]
    tokens = [token for token in raw_tokens if token]
    starts = [index + 1 for index, token in enumerate(tokens) if token.casefold() == "cron"]
    starts.extend(range(0, max(len(tokens) - 4, 0)))
    for start in _dedupe(starts):
        expression_tokens: list[str] = []
        for token in tokens[start:]:
            if not _CRON_TOKEN_RE.match(token):
                break
            expression_tokens.append(token)
            if len(expression_tokens) == 5:
                return " ".join(expression_tokens).casefold()
            if len(expression_tokens) == 7:
                break
    return None


def _natural_cadence(text: str) -> str | None:
    patterns = (
        r"\bevery\s+\d+\s+(?:minutes?|hours?|days?|weeks?|months?)\b",
        r"\b(?:hourly|daily|nightly|weekly|monthly|quarterly|annually)\b",
        r"\b(?:every weekday|weekdays|weekends)\b",
        r"\bat\s+\d{1,2}:\d{2}\s*(?:utc|local time|am|pm)?\b",
    )
    for pattern in patterns:
        if match := re.search(pattern, text, re.I):
            return _clean_text(match.group(0)).casefold()
    return None


def _matched_terms(concern: ScheduledJobConcern, text: str) -> tuple[str, ...]:
    terms = [match.group(0).casefold() for match in _CONCERN_PATTERNS[concern].finditer(text)]
    if concern == "cadence" and (cron := _cron_expression(text)):
        terms.append(cron)
    return tuple(sorted(_dedupe(_clean_text(term) for term in terms), key=str.casefold))


def _detail(pattern: re.Pattern[str], text: str) -> str | None:
    if not (match := pattern.search(text)):
        return None
    return _clean_text(match.group(0)).casefold()


def _match_detail(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if not match:
        return None
    return _clean_text(match.group(1)).rstrip(".").casefold()


def _field_value_detail(field_name: str, text: str) -> str | None:
    pattern = re.compile(rf"\b{re.escape(field_name)}:\s*([^;]+)", re.I)
    if not (match := pattern.search(text)):
        return None
    return _clean_text(match.group(1)).rstrip(".").casefold()


def _first_detail(values: Iterable[str | None]) -> str | None:
    for value in values:
        if value:
            return value
    return None


def _source_index(source_field: str) -> int:
    match = re.search(r"\[(\d+)\]", source_field)
    return int(match.group(1)) if match else 0


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _clean_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _clean_text(value)
    return [text] if text else []


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _searchable_text(source_field: str, text: str) -> str:
    return f"{_field_words(source_field)} {text}".replace("_", " ").replace("-", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(value)
    return text or None


def _requirement_text(value: str) -> str:
    text = _clean_text(value)
    if len(text) > 220:
        text = f"{text[:217].rstrip()}..."
    return text


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


def _dedupe_requirement_key(value: str) -> str:
    text = _clean_text(value).casefold()
    return _SPACE_RE.sub(" ", text).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "ScheduledJobConcern",
    "ScheduledJobConfidence",
    "SourceScheduledJobRequirement",
    "SourceScheduledJobRequirementsReport",
    "build_source_scheduled_job_requirements",
    "derive_source_scheduled_job_requirements",
    "extract_source_scheduled_job_requirements",
    "generate_source_scheduled_job_requirements",
    "source_scheduled_job_requirements_to_dict",
    "source_scheduled_job_requirements_to_dicts",
    "source_scheduled_job_requirements_to_markdown",
    "summarize_source_scheduled_job_requirements",
]
