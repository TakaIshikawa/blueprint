"""Build manual QA coverage matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ManualQANeed = Literal[
    "visual_review",
    "cross_browser_check",
    "mobile_device_check",
    "accessibility_spot_check",
    "migration_verification",
    "admin_workflow_verification",
    "rollback_rehearsal",
]
ManualQACoverageStatus = Literal["covered", "missing_coverage"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_NEED_ORDER: dict[ManualQANeed, int] = {
    "visual_review": 0,
    "cross_browser_check": 1,
    "mobile_device_check": 2,
    "accessibility_spot_check": 3,
    "migration_verification": 4,
    "admin_workflow_verification": 5,
    "rollback_rehearsal": 6,
}
_STATUS_ORDER: tuple[ManualQACoverageStatus, ...] = ("covered", "missing_coverage")
_OWNER_KEY_RE = re.compile(r"\b(?:owner|dri|responsible|team|lead|qa|reviewer)\b", re.I)

_NEED_PATTERNS: dict[ManualQANeed, re.Pattern[str]] = {
    "visual_review": re.compile(
        r"\b(?:visual|screenshot|pixel|layout|ui|ux|frontend|front end|component|"
        r"page|view|theme|styling|css|copy|image|icon|chart|dashboard)\b",
        re.I,
    ),
    "cross_browser_check": re.compile(
        r"\b(?:cross[- ]?browser|browser|chrome|firefox|safari|edge|webkit|"
        r"chromium|responsive browser|browser matrix)\b",
        re.I,
    ),
    "mobile_device_check": re.compile(
        r"\b(?:mobile|ios|android|iphone|ipad|tablet|react native|react-native|"
        r"expo|touch|gesture|viewport|small screen)\b",
        re.I,
    ),
    "accessibility_spot_check": re.compile(
        r"\b(?:accessibility|a11y|screen reader|voiceover|talkback|keyboard|focus|"
        r"aria|contrast|wcag|alt text|assistive)\b",
        re.I,
    ),
    "migration_verification": re.compile(
        r"\b(?:migration|migrate|backfill|schema change|data migration|cutover|"
        r"import|export|existing (?:customers|users|accounts|records)|reconcile|"
        r"reconciliation)\b",
        re.I,
    ),
    "admin_workflow_verification": re.compile(
        r"\b(?:admin|administrator|operator|tenant owner|workspace owner|settings|"
        r"configuration|permission|permissions|role|roles|rbac|access control|"
        r"management console)\b",
        re.I,
    ),
    "rollback_rehearsal": re.compile(
        r"\b(?:rollback|roll back|revert|kill switch|feature flag|flagged|canary|"
        r"launch watch|hotfix|incident|runbook|disaster recovery|drill)\b",
        re.I,
    ),
}

_COVERAGE_RE = re.compile(
    r"\b(?:manual qa|manual test|manual testing|qa pass|qa sign[- ]?off|qa coverage|"
    r"exploratory test|exploratory testing|test on device|device lab|real device|"
    r"browser matrix|cross[- ]?browser|screenshot review|visual review|"
    r"accessibility spot check|a11y spot check|screen reader check|migration verification|"
    r"admin workflow verification|rollback rehearsal|rollback drill|runbook rehearsal)\b",
    re.I,
)
_COVERAGE_FIELD_RE = re.compile(
    r"\b(?:acceptance|criteria|definition|qa|manual|validation|test|coverage|dod|"
    r"review|signoff|sign[- ]?off)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PlanManualQACoverageRecord:
    """Manual QA obligation for one relevant execution task."""

    task_id: str
    title: str
    qa_needs: tuple[ManualQANeed, ...] = field(default_factory=tuple)
    detected_signals: tuple[str, ...] = field(default_factory=tuple)
    coverage_status: ManualQACoverageStatus = "missing_coverage"
    missing_coverage_notes: tuple[str, ...] = field(default_factory=tuple)
    owner_hints: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "qa_needs": list(self.qa_needs),
            "detected_signals": list(self.detected_signals),
            "coverage_status": self.coverage_status,
            "missing_coverage_notes": list(self.missing_coverage_notes),
            "owner_hints": list(self.owner_hints),
        }


@dataclass(frozen=True, slots=True)
class PlanManualQACoverageMatrix:
    """Plan-level matrix of manual QA obligations not covered by automated commands."""

    plan_id: str | None = None
    records: tuple[PlanManualQACoverageRecord, ...] = field(default_factory=tuple)
    manual_qa_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "manual_qa_task_ids": list(self.manual_qa_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return manual QA records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Manual QA Coverage Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        need_counts = self.summary.get("qa_need_counts", {})
        status_counts = self.summary.get("coverage_status_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Manual QA task count: {self.summary.get('manual_qa_task_count', 0)}",
            f"- Missing coverage task count: {self.summary.get('missing_coverage_task_count', 0)}",
            "- Coverage status counts: "
            + ", ".join(f"{status} {status_counts.get(status, 0)}" for status in _STATUS_ORDER),
            "- QA need counts: "
            + ", ".join(f"{need} {need_counts.get(need, 0)}" for need in _NEED_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No manual QA coverage obligations were detected."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | QA Needs | Coverage | Missing Coverage Notes | Owner Hints | Detected Signals |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{_markdown_cell('; '.join(record.qa_needs))} | "
                f"{record.coverage_status} | "
                f"{_markdown_cell('; '.join(record.missing_coverage_notes) or 'none')} | "
                f"{_markdown_cell('; '.join(record.owner_hints) or 'none')} | "
                f"{_markdown_cell('; '.join(record.detected_signals) or 'none')} |"
            )
        return "\n".join(lines)


def build_plan_manual_qa_coverage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanManualQACoverageMatrix:
    """Build a plan-level matrix of tasks requiring manual QA coverage."""
    plan_id, plan_context, tasks = _source_payload(source)
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        if (record := _record(task, index, plan_context)) is not None
    ]
    records.sort(
        key=lambda record: (
            0 if record.coverage_status == "missing_coverage" else 1,
            min(_NEED_ORDER[need] for need in record.qa_needs),
            record.task_id,
            record.title.casefold(),
        )
    )
    result = tuple(records)
    coverage_counts = {
        status: sum(1 for record in result if record.coverage_status == status)
        for status in _STATUS_ORDER
    }
    need_counts = {
        need: sum(1 for record in result if need in record.qa_needs) for need in _NEED_ORDER
    }
    missing_count = coverage_counts["missing_coverage"]
    return PlanManualQACoverageMatrix(
        plan_id=plan_id,
        records=result,
        manual_qa_task_ids=tuple(record.task_id for record in result),
        summary={
            "task_count": len(tasks),
            "manual_qa_task_count": len(result),
            "missing_coverage_task_count": missing_count,
            "coverage_status_counts": coverage_counts,
            "qa_need_counts": need_counts,
            "status": (
                "no_manual_qa_obligations"
                if not result
                else "missing_manual_qa_coverage"
                if missing_count
                else "covered"
            ),
        },
    )


def summarize_plan_manual_qa_coverage_matrix(
    source_or_matrix: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | PlanManualQACoverageMatrix
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanManualQACoverageMatrix:
    """Build the manual QA coverage matrix, accepting an existing matrix unchanged."""
    if isinstance(source_or_matrix, PlanManualQACoverageMatrix):
        return source_or_matrix
    return build_plan_manual_qa_coverage_matrix(source_or_matrix)


def generate_plan_manual_qa_coverage_matrix(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanManualQACoverageMatrix:
    """Compatibility alias for building plan manual QA coverage matrices."""
    return build_plan_manual_qa_coverage_matrix(source)


def plan_manual_qa_coverage_matrix_to_dict(
    matrix: PlanManualQACoverageMatrix,
) -> dict[str, Any]:
    """Serialize a manual QA coverage matrix to a plain dictionary."""
    return matrix.to_dict()


plan_manual_qa_coverage_matrix_to_dict.__test__ = False


def plan_manual_qa_coverage_matrix_to_dicts(
    source: PlanManualQACoverageMatrix | Iterable[PlanManualQACoverageRecord],
) -> list[dict[str, Any]]:
    """Serialize manual QA coverage records to plain dictionaries."""
    if isinstance(source, PlanManualQACoverageMatrix):
        return source.to_dicts()
    return [record.to_dict() for record in source]


plan_manual_qa_coverage_matrix_to_dicts.__test__ = False


def plan_manual_qa_coverage_matrix_to_markdown(matrix: PlanManualQACoverageMatrix) -> str:
    """Render a manual QA coverage matrix as Markdown."""
    return matrix.to_markdown()


plan_manual_qa_coverage_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    needs: tuple[ManualQANeed, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    owner_hints: tuple[str, ...] = field(default_factory=tuple)


def _record(
    task: Mapping[str, Any],
    index: int,
    plan_context: tuple[tuple[str, str], ...],
) -> PlanManualQACoverageRecord | None:
    signals = _signals(task, plan_context)
    if not signals.needs:
        return None

    coverage_context = _coverage_context(task, plan_context)
    covered_needs = tuple(need for need in signals.needs if _need_covered(need, coverage_context))
    status: ManualQACoverageStatus = "covered" if len(covered_needs) == len(signals.needs) else "missing_coverage"
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    return PlanManualQACoverageRecord(
        task_id=task_id,
        title=title,
        qa_needs=signals.needs,
        detected_signals=signals.evidence,
        coverage_status=status,
        missing_coverage_notes=(
            ()
            if status == "covered"
            else tuple(_MISSING_COVERAGE_NOTES[need] for need in signals.needs if need not in covered_needs)
        ),
        owner_hints=signals.owner_hints,
    )


def _signals(task: Mapping[str, Any], plan_context: tuple[tuple[str, str], ...]) -> _Signals:
    needs: list[ManualQANeed] = []
    evidence: list[str] = []
    owner_hints: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        path_needs = _path_needs(path)
        if path_needs:
            needs.extend(path_needs)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in (*_candidate_texts(task), *plan_context):
        owner_hints.extend(_owner_hints(source_field, text))
        if _coverage_only_field(source_field):
            continue
        matched = _text_needs(text)
        if matched:
            needs.extend(matched)
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        needs=tuple(_ordered_dedupe(needs, tuple(_NEED_ORDER))),
        evidence=tuple(_dedupe(evidence)),
        owner_hints=tuple(_dedupe(owner_hints)),
    )


def _path_needs(original: str) -> list[ManualQANeed]:
    normalized = _normalized_path(original).casefold()
    if not normalized:
        return []
    path = PurePosixPath(normalized)
    parts = set(path.parts)
    name = path.name
    suffix = path.suffix
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    needs: list[ManualQANeed] = []
    if suffix in {".tsx", ".jsx", ".vue", ".svelte", ".html", ".css", ".scss"} or bool(
        {"ui", "frontend", "components", "component", "views", "pages", "templates"} & parts
    ):
        needs.append("visual_review")
    if {"browser", "browsers", "playwright", "e2e"} & parts or "browser" in name:
        needs.append("cross_browser_check")
    if {"ios", "android", "mobile", "react-native", "react_native", "expo"} & parts:
        needs.append("mobile_device_check")
    if any(token in text for token in ("accessibility", "a11y", "aria", "focus", "keyboard", "contrast")):
        needs.append("accessibility_spot_check")
    if bool({"migrations", "migration", "backfills", "imports", "exports"} & parts) or any(
        token in text for token in ("migration", "backfill", "cutover", "reconcile")
    ):
        needs.append("migration_verification")
    if bool({"admin", "admins", "settings", "permissions", "rbac"} & parts):
        needs.append("admin_workflow_verification")
    if any(token in text for token in ("rollback", "runbook", "kill switch", "feature flag", "canary")):
        needs.append("rollback_rehearsal")
    return _ordered_dedupe(needs, tuple(_NEED_ORDER))


def _text_needs(text: str) -> list[ManualQANeed]:
    return [need for need in _NEED_ORDER if _NEED_PATTERNS[need].search(text)]


def _coverage_context(task: Mapping[str, Any], plan_context: tuple[tuple[str, str], ...]) -> tuple[str, ...]:
    values: list[str] = []
    for source_field, text in (*_candidate_texts(task), *plan_context):
        normalized = source_field.casefold().replace("-", " ").replace("_", " ")
        if _COVERAGE_FIELD_RE.search(normalized):
            values.append(text)
    return tuple(values)


def _need_covered(need: ManualQANeed, coverage_context: tuple[str, ...]) -> bool:
    for text in coverage_context:
        if _COVERAGE_RE.search(text) and (
            "manual qa" in text.casefold()
            or "qa pass" in text.casefold()
            or "qa sign" in text.casefold()
            or _NEED_PATTERNS[need].search(text)
            or _SPECIFIC_COVERAGE_PATTERNS[need].search(text)
        ):
            return True
    return False


_SPECIFIC_COVERAGE_PATTERNS: dict[ManualQANeed, re.Pattern[str]] = {
    "visual_review": re.compile(r"\b(?:visual review|screenshot review|pixel review)\b", re.I),
    "cross_browser_check": re.compile(r"\b(?:cross[- ]?browser|browser matrix)\b", re.I),
    "mobile_device_check": re.compile(r"\b(?:real device|device lab|test on device|mobile qa)\b", re.I),
    "accessibility_spot_check": re.compile(
        r"\b(?:accessibility spot check|a11y spot check|screen reader check)\b", re.I
    ),
    "migration_verification": re.compile(r"\b(?:migration verification|cutover verification)\b", re.I),
    "admin_workflow_verification": re.compile(r"\b(?:admin workflow verification|admin qa)\b", re.I),
    "rollback_rehearsal": re.compile(r"\b(?:rollback rehearsal|rollback drill|runbook rehearsal)\b", re.I),
}

_MISSING_COVERAGE_NOTES: dict[ManualQANeed, str] = {
    "visual_review": "Add manual visual QA evidence for changed UI, layout, copy, screenshots, or visual states.",
    "cross_browser_check": "Add manual cross-browser coverage for the supported browser matrix.",
    "mobile_device_check": "Add manual mobile or real-device QA coverage for affected device classes.",
    "accessibility_spot_check": "Add a targeted manual accessibility spot check for keyboard, focus, screen reader, or contrast behavior.",
    "migration_verification": "Add manual migration or cutover verification for existing data and affected records.",
    "admin_workflow_verification": "Add manual admin workflow verification for roles, permissions, configuration, or operator paths.",
    "rollback_rehearsal": "Add rollback rehearsal evidence covering flags, runbooks, revert steps, and production recovery expectations.",
}


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, tuple[tuple[str, str], ...], list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return (
            _optional_text(payload.get("id")),
            tuple(_plan_context(payload)),
            [task.model_dump(mode="python") for task in source.tasks],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return (
                _optional_text(payload.get("id")),
                tuple(_plan_context(payload)),
                _task_payloads(payload.get("tasks")),
            )
        return None, (), [dict(source)]
    if _looks_like_task(source):
        return None, (), [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return (
            _optional_text(payload.get("id")),
            tuple(_plan_context(payload)),
            _task_payloads(payload.get("tasks")),
        )

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, (), []

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
    return None, (), tasks


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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _plan_context(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "target_engine",
        "target_repo",
        "project_type",
        "test_strategy",
        "handoff_prompt",
        "generation_prompt",
    ):
        if text := _optional_text(plan.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("milestones", "metadata", "implementation_brief", "brief"):
        for source_field, text in _metadata_texts(plan.get(field_name), prefix=field_name):
            texts.append((source_field, text))
    return texts


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
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
        "criteria",
        "definition_of_done",
        "risks",
        "risk",
        "risk_level",
        "test_command",
        "validation_commands",
        "status",
        "metadata",
        "tags",
        "labels",
        "notes",
        "blocked_reason",
        "tasks",
        "milestones",
        "implementation_brief",
        "brief",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
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
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "depends_on",
        "tags",
        "labels",
        "notes",
        "validation_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return tuple(texts)


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _text_needs(key_text) and not _OWNER_KEY_RE.search(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _text_needs(key_text):
                texts.append((field, str(key)))
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


def _owner_hints(source_field: str, text: str) -> list[str]:
    key = source_field.rsplit(".", maxsplit=1)[-1]
    key_text = key.replace("_", " ").replace("-", " ")
    if not (_OWNER_KEY_RE.search(source_field) or _OWNER_KEY_RE.search(key_text)):
        return []
    return [_clean_text(text)[:120].rstrip()]


def _coverage_only_field(source_field: str) -> bool:
    normalized = source_field.casefold().replace("-", "_").replace(" ", "_")
    return normalized.startswith(
        (
            "acceptance_criteria",
            "criteria",
            "definition_of_done",
            "metadata.acceptance",
            "metadata.criteria",
            "metadata.qa",
            "metadata.manual",
            "metadata.validation",
            "test_command",
            "validation_commands",
        )
    )


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
    text = _clean_text(str(value))
    return [text] if text else []


def _ordered_dedupe(items: Iterable[_T], order: tuple[_T, ...]) -> list[_T]:
    seen = set(items)
    return [item for item in order if item in seen]


def _dedupe(items: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return tuple(result)


def _evidence_snippet(source_field: str, text: str) -> str:
    return f"{source_field}: {_snippet(text)}"


def _snippet(text: str, limit: int = 180) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "..."


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/").strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip()


def _markdown_cell(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ").strip()


__all__ = [
    "ManualQACoverageStatus",
    "ManualQANeed",
    "PlanManualQACoverageMatrix",
    "PlanManualQACoverageRecord",
    "build_plan_manual_qa_coverage_matrix",
    "generate_plan_manual_qa_coverage_matrix",
    "plan_manual_qa_coverage_matrix_to_dict",
    "plan_manual_qa_coverage_matrix_to_dicts",
    "plan_manual_qa_coverage_matrix_to_markdown",
    "summarize_plan_manual_qa_coverage_matrix",
]
