"""Build privacy review matrices from implementation briefs and execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask, ImplementationBrief


PrivacyReviewCategory = Literal[
    "pii",
    "customer_data",
    "consent",
    "retention",
    "deletion",
    "audit_logs",
    "analytics",
    "gdpr",
    "ccpa",
    "hipaa",
    "dpa",
    "cross_border_transfer",
]
PrivacyReviewStatus = Literal["requires_review", "requires_legal_review"]

_SPACE_RE = re.compile(r"\s+")
_CATEGORY_ORDER: dict[PrivacyReviewCategory, int] = {
    "pii": 0,
    "customer_data": 1,
    "consent": 2,
    "retention": 3,
    "deletion": 4,
    "audit_logs": 5,
    "analytics": 6,
    "gdpr": 7,
    "ccpa": 8,
    "hipaa": 9,
    "dpa": 10,
    "cross_border_transfer": 11,
}
_CATEGORY_PATTERNS: dict[PrivacyReviewCategory, re.Pattern[str]] = {
    "pii": re.compile(
        r"\b(?:pii|personal data|personally identifiable|email addresses?|phone numbers?|"
        r"mailing addresses?|home addresses?|ssn|social security|date of birth|dob|"
        r"full names?|identity data|profile data|user data)\b",
        re.IGNORECASE,
    ),
    "customer_data": re.compile(
        r"\b(?:customer data|customer records?|account data|tenant data|user records?|"
        r"customer profiles?|crm data|support data)\b",
        re.IGNORECASE,
    ),
    "consent": re.compile(
        r"\b(?:consent|opt[- ]?in|opt[- ]?out|unsubscribe|preference center|"
        r"privacy choice|tracking permission|marketing permission)\b",
        re.IGNORECASE,
    ),
    "retention": re.compile(
        r"\b(?:retention|retain|retained|ttl|time to live|archive|archival|"
        r"expiration|expire|purge schedule|data lifecycle)\b",
        re.IGNORECASE,
    ),
    "deletion": re.compile(
        r"\b(?:delete|deletion|erase|erasure|right to be forgotten|account removal|"
        r"data removal|purge|hard delete|soft delete|tombstone)\b",
        re.IGNORECASE,
    ),
    "audit_logs": re.compile(
        r"\b(?:audit logs?|audit trail|audit event|audit records?|access logs?|"
        r"compliance logs?|logging|logged)\b",
        re.IGNORECASE,
    ),
    "analytics": re.compile(
        r"\b(?:analytics|tracking|track event|tracked event|product metrics|"
        r"usage metrics|behavioral data|funnel|attribution)\b",
        re.IGNORECASE,
    ),
    "gdpr": re.compile(r"\b(?:gdpr|general data protection regulation)\b", re.IGNORECASE),
    "ccpa": re.compile(r"\b(?:ccpa|cpra|california privacy)\b", re.IGNORECASE),
    "hipaa": re.compile(
        r"\b(?:hipaa|phi|protected health|health data|medical records?|patient data)\b",
        re.IGNORECASE,
    ),
    "dpa": re.compile(
        r"\b(?:dpa|data processing agreement|data processing addendum|processor agreement|"
        r"subprocessor agreement)\b",
        re.IGNORECASE,
    ),
    "cross_border_transfer": re.compile(
        r"\b(?:cross[- ]border|international transfer|data transfer|transfer impact|"
        r"sccs?|standard contractual clauses|data residency|region transfer|"
        r"outside (?:the )?(?:eu|eea|european union|united states|us))\b",
        re.IGNORECASE,
    ),
}
_LEGAL_REVIEW_CATEGORIES: frozenset[PrivacyReviewCategory] = frozenset(
    {"gdpr", "ccpa", "hipaa", "dpa", "cross_border_transfer"}
)


@dataclass(frozen=True, slots=True)
class PlanPrivacyReviewMatrixRow:
    """One privacy review row grouped by privacy category."""

    category: PrivacyReviewCategory
    review_status: PrivacyReviewStatus
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_reviewers: tuple[str, ...] = field(default_factory=tuple)
    required_review_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "review_status": self.review_status,
            "affected_task_ids": list(self.affected_task_ids),
            "evidence": list(self.evidence),
            "recommended_reviewers": list(self.recommended_reviewers),
            "required_review_questions": list(self.required_review_questions),
        }


@dataclass(frozen=True, slots=True)
class PlanPrivacyReviewMatrix:
    """Privacy review matrix for a brief, plan, or brief-plus-plan pair."""

    brief_id: str | None = None
    plan_id: str | None = None
    rows: tuple[PlanPrivacyReviewMatrixRow, ...] = field(default_factory=tuple)

    @property
    def summary(self) -> dict[str, Any]:
        """Return compact rollup counts in stable key order."""
        return {
            "category_count": len(self.rows),
            "affected_task_count": len(
                {task_id for row in self.rows for task_id in row.affected_task_ids}
            ),
            "legal_review_count": sum(
                1 for row in self.rows if row.review_status == "requires_legal_review"
            ),
        }

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "plan_id": self.plan_id,
            "summary": self.summary,
            "rows": [row.to_dict() for row in self.rows],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return privacy review rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Privacy Review Matrix"
        identifiers = ", ".join(
            value
            for value in (
                f"brief {self.brief_id}" if self.brief_id else "",
                f"plan {self.plan_id}" if self.plan_id else "",
            )
            if value
        )
        if identifiers:
            title = f"{title}: {identifiers}"
        lines = [title]
        if not self.rows:
            lines.extend(["", "No privacy review rows were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Category | Status | Affected Tasks | Evidence | Reviewers | "
                    "Required Questions |"
                ),
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{row.category} | "
                f"{row.review_status} | "
                f"{_markdown_cell(', '.join(row.affected_task_ids) or 'brief')} | "
                f"{_markdown_cell('; '.join(row.evidence))} | "
                f"{_markdown_cell(', '.join(row.recommended_reviewers))} | "
                f"{_markdown_cell('; '.join(row.required_review_questions))} |"
            )
        return "\n".join(lines)


def build_plan_privacy_review_matrix(
    brief: Mapping[str, Any] | ImplementationBrief | ExecutionPlan | None = None,
    plan: Mapping[str, Any] | ExecutionPlan | None = None,
) -> PlanPrivacyReviewMatrix:
    """Build privacy review rows from an implementation brief and/or execution plan."""
    brief_payload: dict[str, Any] = {}
    plan_payload: dict[str, Any] = {}

    if plan is None and _looks_like_plan(brief):
        plan_payload = _plan_payload(brief)
    else:
        brief_payload = _brief_payload(brief)
        plan_payload = _plan_payload(plan)

    builders: dict[PrivacyReviewCategory, _RowBuilder] = {
        category: _RowBuilder(category)
        for category in _CATEGORY_ORDER
    }

    for source_field, text in _brief_texts(brief_payload):
        _add_signals(builders, source_field, text, task_id=None)

    tasks = _task_payloads(plan_payload.get("tasks"))
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        for source_field, text in _task_texts(task):
            _add_signals(builders, source_field, text, task_id=task_id)

    for source_field, text in _plan_texts(plan_payload):
        _add_signals(builders, source_field, text, task_id=None)

    rows = tuple(
        _row(builder)
        for builder in sorted(builders.values(), key=lambda item: _CATEGORY_ORDER[item.category])
        if builder.evidence
    )
    return PlanPrivacyReviewMatrix(
        brief_id=_optional_text(brief_payload.get("id")),
        plan_id=_optional_text(plan_payload.get("id")),
        rows=rows,
    )


def plan_privacy_review_matrix_to_dict(
    matrix: PlanPrivacyReviewMatrix,
) -> dict[str, Any]:
    """Serialize a privacy review matrix to a plain dictionary."""
    return matrix.to_dict()


plan_privacy_review_matrix_to_dict.__test__ = False


def plan_privacy_review_matrix_to_markdown(
    matrix: PlanPrivacyReviewMatrix,
) -> str:
    """Render a privacy review matrix as Markdown."""
    return matrix.to_markdown()


plan_privacy_review_matrix_to_markdown.__test__ = False


@dataclass(slots=True)
class _RowBuilder:
    category: PrivacyReviewCategory
    task_ids: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)


def _row(builder: _RowBuilder) -> PlanPrivacyReviewMatrixRow:
    category = builder.category
    return PlanPrivacyReviewMatrixRow(
        category=category,
        review_status=(
            "requires_legal_review" if category in _LEGAL_REVIEW_CATEGORIES else "requires_review"
        ),
        affected_task_ids=tuple(_dedupe(builder.task_ids)),
        evidence=tuple(_dedupe(builder.evidence)),
        recommended_reviewers=_reviewers(category),
        required_review_questions=_questions(category),
    )


def _add_signals(
    builders: dict[PrivacyReviewCategory, _RowBuilder],
    source_field: str,
    text: str,
    *,
    task_id: str | None,
) -> None:
    snippet = _evidence(source_field, text)
    for category, pattern in _CATEGORY_PATTERNS.items():
        if not pattern.search(text):
            continue
        builder = builders[category]
        if task_id:
            builder.task_ids.append(task_id)
        builder.evidence.append(snippet)


def _reviewers(category: PrivacyReviewCategory) -> tuple[str, ...]:
    reviewers: dict[PrivacyReviewCategory, tuple[str, ...]] = {
        "pii": ("privacy", "security", "data owner"),
        "customer_data": ("privacy", "data owner", "support operations"),
        "consent": ("privacy", "legal", "product"),
        "retention": ("privacy", "legal", "data platform"),
        "deletion": ("privacy", "legal", "data platform"),
        "audit_logs": ("privacy", "security", "compliance"),
        "analytics": ("privacy", "product analytics", "legal"),
        "gdpr": ("privacy", "legal"),
        "ccpa": ("privacy", "legal"),
        "hipaa": ("privacy", "legal", "security"),
        "dpa": ("legal", "privacy", "vendor management"),
        "cross_border_transfer": ("legal", "privacy", "data protection officer"),
    }
    return reviewers[category]


def _questions(category: PrivacyReviewCategory) -> tuple[str, ...]:
    questions: dict[PrivacyReviewCategory, tuple[str, ...]] = {
        "pii": (
            "Which exact personal data fields are collected, stored, displayed, or transformed?",
            "Can the implementation minimize, mask, or redact any personal data before processing?",
        ),
        "customer_data": (
            "Which customer accounts, tenants, or records are affected by this change?",
            "Who owns approval for the customer data purpose and access pattern?",
        ),
        "consent": (
            "What consent state is required before collection, tracking, processing, or sharing?",
            "What happens when consent is absent, withdrawn, or stale?",
        ),
        "retention": (
            "What retention period, purge trigger, and archival behavior apply?",
            "How will the implementation prove retained data is removed on schedule?",
        ),
        "deletion": (
            "Which primary stores, caches, logs, exports, and third parties require deletion handling?",
            "How will deletion be verified and prevented from being rehydrated?",
        ),
        "audit_logs": (
            "Which actor, action, target, result, and timestamp fields belong in audit evidence?",
            "Which sensitive fields must be redacted from audit or access logs?",
        ),
        "analytics": (
            "Which analytics event fields are necessary and which direct identifiers can be removed?",
            "Is consent or regional gating required before analytics events are emitted?",
        ),
        "gdpr": (
            "Which GDPR lawful basis, data subject rights, and minimization constraints apply?",
            "Does this change need a DPIA or DPO sign-off before implementation?",
        ),
        "ccpa": (
            "Does this change affect California notice, opt-out, deletion, or access obligations?",
            "Are sale, sharing, or sensitive personal information rules implicated?",
        ),
        "hipaa": (
            "Does this change process PHI or require HIPAA administrative, technical, or audit controls?",
            "Is a covered entity, business associate, or minimum necessary review required?",
        ),
        "dpa": (
            "Which processor, subprocessor, or vendor agreement covers the affected data?",
            "Do the outbound fields, purpose, retention, and subprocessors match the DPA?",
        ),
        "cross_border_transfer": (
            "Which regions send and receive the data, and what transfer mechanism applies?",
            "Are data residency, SCC, or transfer impact assessment requirements satisfied?",
        ),
    }
    return questions[category]


def _brief_texts(brief: Mapping[str, Any]) -> list[tuple[str, str]]:
    if not brief:
        return []
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "domain",
        "target_user",
        "buyer",
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "product_surface",
        "architecture_notes",
        "data_requirements",
        "validation_plan",
        "generation_prompt",
    ):
        if text := _optional_text(brief.get(field_name)):
            texts.append((f"brief.{field_name}", text))
    for field_name in (
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "risks",
        "definition_of_done",
    ):
        for index, text in enumerate(_strings(brief.get(field_name))):
            texts.append((f"brief.{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(brief.get("metadata"), prefix="brief.metadata"):
        texts.append((source_field, text))
    return texts


def _plan_texts(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    if not plan:
        return []
    texts: list[tuple[str, str]] = []
    for field_name in ("target_repo", "project_type", "test_strategy", "handoff_prompt"):
        if text := _optional_text(plan.get(field_name)):
            texts.append((f"plan.{field_name}", text))
    for source_field, text in _metadata_texts(plan.get("metadata"), prefix="plan.metadata"):
        texts.append((source_field, text))
    return texts


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    task_id = _optional_text(task.get("id")) or "task"
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((f"task.{task_id}.{field_name}", text))
    for field_name in ("files_or_modules", "files", "acceptance_criteria", "tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"task.{task_id}.{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(
        task.get("metadata"),
        prefix=f"task.{task_id}.metadata",
    ):
        texts.append((source_field, text))
    return texts


def _brief_payload(value: Mapping[str, Any] | ImplementationBrief | None) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, ImplementationBrief):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump") and not isinstance(value, ExecutionPlan):
        dumped = value.model_dump(mode="python")
        return dict(dumped) if isinstance(dumped, Mapping) else {}
    if isinstance(value, Mapping):
        try:
            dumped = ImplementationBrief.model_validate(value).model_dump(mode="python")
            return dict(dumped) if isinstance(dumped, Mapping) else {}
        except (TypeError, ValueError, ValidationError):
            return dict(value)
    return {}


def _plan_payload(value: Mapping[str, Any] | ExecutionPlan | None) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, ExecutionPlan):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        dumped = value.model_dump(mode="python")
        return dict(dumped) if isinstance(dumped, Mapping) else {}
    if isinstance(value, Mapping):
        try:
            dumped = ExecutionPlan.model_validate(value).model_dump(mode="python")
            return dict(dumped) if isinstance(dumped, Mapping) else {}
        except (TypeError, ValueError, ValidationError):
            return dict(value)
    return {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            dumped = item.model_dump(mode="python")
            if isinstance(dumped, Mapping):
                tasks.append(dict(dumped))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _looks_like_plan(value: Any) -> bool:
    if isinstance(value, ExecutionPlan):
        return True
    if isinstance(value, ImplementationBrief) or value is None:
        return False
    if isinstance(value, Mapping):
        return "tasks" in value or "implementation_brief_id" in value
    return hasattr(value, "tasks")


def _metadata_texts(value: Any, *, prefix: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            texts.extend(_metadata_texts(value[key], prefix=f"{prefix}.{key}"))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            texts.extend(_metadata_texts(item, prefix=f"{prefix}[{index}]"))
        return texts
    if text := _optional_text(value):
        return [(prefix, text)]
    return []


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


def _evidence(source_field: str, text: str) -> str:
    return f"{source_field}: {_clean_sentence(text)}"


def _clean_sentence(value: str) -> str:
    return _SPACE_RE.sub(" ", str(value)).strip().strip("`'\",;:()[]{}").rstrip(".")


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Any) -> tuple[Any, ...]:
    seen: set[Any] = set()
    result: list[Any] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return tuple(result)


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "PlanPrivacyReviewMatrix",
    "PlanPrivacyReviewMatrixRow",
    "PrivacyReviewCategory",
    "PrivacyReviewStatus",
    "build_plan_privacy_review_matrix",
    "plan_privacy_review_matrix_to_dict",
    "plan_privacy_review_matrix_to_markdown",
]
