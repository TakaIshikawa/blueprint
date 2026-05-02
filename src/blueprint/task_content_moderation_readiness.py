"""Plan content moderation readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


ContentSurface = Literal[
    "comments",
    "reviews",
    "profile_content",
    "images",
    "attachments",
    "chat_messages",
    "community_posts",
]
ModerationSafeguard = Literal[
    "policy_definition",
    "reporting_flow",
    "reviewer_queue",
    "automated_detection",
    "appeal_process",
    "audit_log",
    "abuse_metrics",
    "test_coverage",
]
ModerationRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SURFACE_ORDER: tuple[ContentSurface, ...] = (
    "comments",
    "reviews",
    "profile_content",
    "images",
    "attachments",
    "chat_messages",
    "community_posts",
)
_SAFEGUARD_ORDER: tuple[ModerationSafeguard, ...] = (
    "policy_definition",
    "reporting_flow",
    "reviewer_queue",
    "automated_detection",
    "appeal_process",
    "audit_log",
    "abuse_metrics",
    "test_coverage",
)
_RISK_ORDER: dict[ModerationRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}

_PATH_SURFACE_PATTERNS: dict[ContentSurface, tuple[re.Pattern[str], ...]] = {
    "comments": (re.compile(r"(?:^|/)(?:comments?|commenting|threads?)(?:/|$)|comments?", re.I),),
    "reviews": (re.compile(r"(?:^|/)(?:reviews?|ratings?)(?:/|$)|reviews?", re.I),),
    "profile_content": (
        re.compile(
            r"(?:^|/)(?:profiles?|profile[-_]?content|profile[-_]?bios?|bios?|about[-_]?me)(?:/|$)|profile[-_]?bio",
            re.I,
        ),
    ),
    "images": (
        re.compile(r"(?:^|/)(?:images?|photos?|media|gallery|avatars?)(?:/|$)|image|photo", re.I),
    ),
    "attachments": (
        re.compile(r"(?:^|/)(?:attachments?|uploads?|files?|documents?)(?:/|$)|attachment|upload", re.I),
    ),
    "chat_messages": (
        re.compile(r"(?:^|/)(?:chat|chats|messages?|dm|dms|inbox)(?:/|$)|chat[-_]?messages?", re.I),
    ),
    "community_posts": (
        re.compile(
            r"(?:^|/)(?:community|posts?|forums?|topics?|discussions?|ugc|user[-_]?content)(?:/|$)|community[-_]?posts?",
            re.I,
        ),
    ),
}
_TEXT_SURFACE_PATTERNS: dict[ContentSurface, tuple[re.Pattern[str], ...]] = {
    "comments": (
        re.compile(r"\b(?:comments?|commenting|reply threads?|discussion replies|user replies)\b", re.I),
    ),
    "reviews": (
        re.compile(r"\b(?:reviews?|ratings?|star ratings?|product feedback|merchant feedback)\b", re.I),
    ),
    "profile_content": (
        re.compile(
            r"\b(?:profile content|profile bios?|user bios?|public bios?|about me|display names?|usernames?|profile text)\b",
            re.I,
        ),
    ),
    "images": (
        re.compile(
            r"\b(?:images?|photos?|pictures?|avatars?|media uploads?|user[- ]generated media|uploaded media|gallery)\b",
            re.I,
        ),
    ),
    "attachments": (
        re.compile(r"\b(?:attachments?|uploaded files?|file uploads?|documents?|user files?)\b", re.I),
    ),
    "chat_messages": (
        re.compile(
            r"\b(?:chat messages?|direct messages?|dms?|private messages?|inbox messages?|conversation messages?)\b",
            re.I,
        ),
    ),
    "community_posts": (
        re.compile(
            r"\b(?:community posts?|user posts?|forum posts?|discussion posts?|public posts?|threads?|ugc|user[- ]generated content)\b",
            re.I,
        ),
    ),
}
_SAFEGUARD_PATTERNS: dict[ModerationSafeguard, tuple[re.Pattern[str], ...]] = {
    "policy_definition": (
        re.compile(
            r"\b(?:moderation policy|content policy|community guidelines?|acceptable use policy|policy definition|policy taxonomy|prohibited content)\b",
            re.I,
        ),
    ),
    "reporting_flow": (
        re.compile(
            r"\b(?:reporting flow|report abuse|user reports?|flag content|flagging flow|abuse report|report button|report queue)\b",
            re.I,
        ),
    ),
    "reviewer_queue": (
        re.compile(
            r"\b(?:reviewer queue|moderation queue|manual review|human review|moderator queue|review workflow|triage queue)\b",
            re.I,
        ),
    ),
    "automated_detection": (
        re.compile(
            r"\b(?:automated detection|auto[- ]?moderation|automated moderation|toxicity detection|spam detection|nsfw detection|classifier|rules engine|keyword filters?)\b",
            re.I,
        ),
    ),
    "appeal_process": (
        re.compile(
            r"\b(?:appeal process|appeals?|appeal workflow|appeal review|user appeal|dispute moderation|contest decision)\b",
            re.I,
        ),
    ),
    "audit_log": (
        re.compile(
            r"\b(?:audit logs?|audit trail|moderation logs?|decision logs?|enforcement logs?|log moderation decisions?|case history)\b",
            re.I,
        ),
    ),
    "abuse_metrics": (
        re.compile(
            r"\b(?:abuse metrics?|moderation metrics?|safety metrics?|report rate|violation rate|false positives?|sla metrics?|queue backlog|trust and safety dashboard)\b",
            re.I,
        ),
    ),
    "test_coverage": (
        re.compile(
            r"\b(?:test coverage|unit tests?|integration tests?|e2e tests?|end[- ]to[- ]end tests?|moderation tests?|abuse tests?|policy tests?|reporting tests?|appeal tests?)\b",
            re.I,
        ),
    ),
}
_RECOMMENDED_CHECKS: dict[ModerationSafeguard, str] = {
    "policy_definition": "Define content policy categories, enforcement actions, severity, and allowed edge cases for the exposed surface.",
    "reporting_flow": "Add a user reporting or flagging flow with abuse categories, reporter context, and duplicate handling.",
    "reviewer_queue": "Route reports, automated hits, and escalations into a reviewer queue with ownership and SLA expectations.",
    "automated_detection": "Plan automated detection for spam, abuse, unsafe media, or policy violations before content spreads.",
    "appeal_process": "Define how users appeal moderation decisions and how appeal outcomes are reviewed and recorded.",
    "audit_log": "Log moderation reports, decisions, reviewer actions, appeal outcomes, and policy versions for investigation.",
    "abuse_metrics": "Track moderation volume, abuse rates, queue backlog, decision latency, appeals, and false positives.",
    "test_coverage": "Cover reporting, detection, review, appeal, audit, metrics, and policy edge cases with tests.",
}


@dataclass(frozen=True, slots=True)
class TaskContentModerationReadinessRecord:
    """Moderation readiness guidance for one task exposing user content."""

    task_id: str
    title: str
    content_surfaces: tuple[ContentSurface, ...]
    present_safeguards: tuple[ModerationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[ModerationSafeguard, ...] = field(default_factory=tuple)
    risk_level: ModerationRiskLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "content_surfaces": list(self.content_surfaces),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskContentModerationReadinessPlan:
    """Plan-level content moderation readiness review."""

    plan_id: str | None = None
    records: tuple[TaskContentModerationReadinessRecord, ...] = field(default_factory=tuple)
    moderation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def recommendations(self) -> tuple[TaskContentModerationReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "moderation_task_ids": list(self.moderation_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render content moderation readiness as deterministic Markdown."""
        title = "# Task Content Moderation Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Moderation task count: {self.summary.get('moderation_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No content moderation readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(
                    ["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Surfaces | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.content_surfaces) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_content_moderation_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskContentModerationReadinessPlan:
    """Build readiness records for tasks that expose user-generated content."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    moderation_task_ids = tuple(record.task_id for record in records)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskContentModerationReadinessPlan(
        plan_id=plan_id,
        records=records,
        moderation_task_ids=moderation_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_content_moderation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskContentModerationReadinessPlan:
    """Compatibility alias for building content moderation readiness plans."""
    return build_task_content_moderation_readiness_plan(source)


def summarize_task_content_moderation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskContentModerationReadinessPlan:
    """Compatibility alias for building content moderation readiness plans."""
    return build_task_content_moderation_readiness_plan(source)


def extract_task_content_moderation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskContentModerationReadinessPlan:
    """Compatibility alias for building content moderation readiness plans."""
    return build_task_content_moderation_readiness_plan(source)


def generate_task_content_moderation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskContentModerationReadinessPlan:
    """Compatibility alias for generating content moderation readiness plans."""
    return build_task_content_moderation_readiness_plan(source)


def recommend_task_content_moderation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskContentModerationReadinessPlan:
    """Compatibility alias for recommending content moderation readiness plans."""
    return build_task_content_moderation_readiness_plan(source)


def task_content_moderation_readiness_plan_to_dict(
    result: TaskContentModerationReadinessPlan,
) -> dict[str, Any]:
    """Serialize a content moderation readiness plan to a plain dictionary."""
    return result.to_dict()


task_content_moderation_readiness_plan_to_dict.__test__ = False


def task_content_moderation_readiness_plan_to_dicts(
    result: TaskContentModerationReadinessPlan | Iterable[TaskContentModerationReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize content moderation readiness records to plain dictionaries."""
    if isinstance(result, TaskContentModerationReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_content_moderation_readiness_plan_to_dicts.__test__ = False


def task_content_moderation_readiness_plan_to_markdown(
    result: TaskContentModerationReadinessPlan,
) -> str:
    """Render a content moderation readiness plan as Markdown."""
    return result.to_markdown()


task_content_moderation_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    surfaces: tuple[ContentSurface, ...] = field(default_factory=tuple)
    surface_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[ModerationSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskContentModerationReadinessRecord | None:
    signals = _signals(task)
    if not signals.surfaces:
        return None

    missing = tuple(
        safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards
    )
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskContentModerationReadinessRecord(
        task_id=task_id,
        title=title,
        content_surfaces=signals.surfaces,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals.surfaces, signals.present_safeguards, missing),
        evidence=tuple(_dedupe([*signals.surface_evidence, *signals.safeguard_evidence])),
        recommended_checks=tuple(_RECOMMENDED_CHECKS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    surface_hits: set[ContentSurface] = set()
    safeguard_hits: set[ModerationSafeguard] = set()
    surface_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_surfaces = _path_surfaces(normalized)
        if path_surfaces:
            surface_hits.update(path_surfaces)
            surface_evidence.append(f"files_or_modules: {path}")
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(searchable) or pattern.search(normalized) for pattern in patterns):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_surface = False
        for surface, patterns in _TEXT_SURFACE_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                surface_hits.add(surface)
                matched_surface = True
        if matched_surface:
            surface_evidence.append(snippet)
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(text) for pattern in patterns):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    for command in _validation_commands(task):
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        for safeguard, patterns in _SAFEGUARD_PATTERNS.items():
            if any(pattern.search(command) or pattern.search(command_text) for pattern in patterns):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    return _Signals(
        surfaces=tuple(surface for surface in _SURFACE_ORDER if surface in surface_hits),
        surface_evidence=tuple(_dedupe(surface_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_surfaces(path: str) -> set[ContentSurface]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    surfaces: set[ContentSurface] = set()
    for surface, patterns in _PATH_SURFACE_PATTERNS.items():
        if any(pattern.search(normalized) or pattern.search(text) for pattern in patterns):
            surfaces.add(surface)
    name = PurePosixPath(normalized).name
    if re.search(r"\bcomments?\b", text) or name.startswith("comment"):
        surfaces.add("comments")
    if re.search(r"\breviews?\b|\bratings?\b", text):
        surfaces.add("reviews")
    if re.search(r"\bprofile\b.*\bbio\b|\bbio\b", text):
        surfaces.add("profile_content")
    if re.search(r"\bchat\b|\bmessages?\b", text):
        surfaces.add("chat_messages")
    if re.search(r"\bcommunity\b|\bposts?\b|\bforums?\b|\bugc\b", text):
        surfaces.add("community_posts")
    return surfaces


def _risk_level(
    surfaces: tuple[ContentSurface, ...],
    present: tuple[ModerationSafeguard, ...],
    missing: tuple[ModerationSafeguard, ...],
) -> ModerationRiskLevel:
    if not missing:
        return "low"

    surface_set = set(surfaces)
    missing_set = set(missing)
    present_set = set(present)
    public_surface = bool(surface_set & {"comments", "reviews", "profile_content", "images", "community_posts"})
    interactive_surface = bool(surface_set & {"comments", "reviews", "chat_messages", "community_posts"})
    high_impact_surface = bool(surface_set & {"images", "attachments", "chat_messages", "community_posts"})

    if {"policy_definition", "reporting_flow", "reviewer_queue"} & missing_set and high_impact_surface:
        return "high"
    if public_surface and {"policy_definition", "reporting_flow"} <= missing_set:
        return "high"
    if (
        interactive_surface
        and len(missing) >= 5
        and not {"policy_definition", "reporting_flow", "reviewer_queue"} <= present_set
    ):
        return "high"
    if {"policy_definition", "reporting_flow", "reviewer_queue"} <= present_set:
        return "medium"
    return "medium"


def _summary(
    records: tuple[TaskContentModerationReadinessRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "moderation_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "surface_counts": {
            surface: sum(1 for record in records if surface in record.content_surfaces)
            for surface in sorted({surface for record in records for surface in record.content_surfaces})
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
        "validation_commands",
        "validation_command",
        "test_commands",
        "validation_plan",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    for source_field, text in _metadata_texts(task.get("validation_plan"), prefix="validation_plan"):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _metadata_key_is_signal(value: str) -> bool:
    return any(
        pattern.search(value)
        for patterns in (*_TEXT_SURFACE_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
        for pattern in patterns
    )


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
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
    text = _optional_text(value)
    return [text] if text else []


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


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
    "ContentSurface",
    "ModerationRiskLevel",
    "ModerationSafeguard",
    "TaskContentModerationReadinessPlan",
    "TaskContentModerationReadinessRecord",
    "analyze_task_content_moderation_readiness",
    "build_task_content_moderation_readiness_plan",
    "extract_task_content_moderation_readiness",
    "generate_task_content_moderation_readiness",
    "recommend_task_content_moderation_readiness",
    "summarize_task_content_moderation_readiness",
    "task_content_moderation_readiness_plan_to_dict",
    "task_content_moderation_readiness_plan_to_dicts",
    "task_content_moderation_readiness_plan_to_markdown",
]
