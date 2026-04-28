"""Shared JSON extraction, repair, and validation helpers for LLM output."""

from __future__ import annotations

import json
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.llm.json_parser import build_json_candidates, write_debug_response


class JsonRepairError(ValueError):
    """Raised when an LLM response cannot be repaired into valid JSON."""

    def __init__(
        self,
        message: str,
        *,
        context: str,
        stage: str,
        snippet: str,
        debug_file: str | None = None,
    ) -> None:
        self.context = context
        self.stage = stage
        self.snippet = snippet
        self.debug_file = debug_file
        super().__init__(message)


class PlanTaskSpec(BaseModel):
    """Task shape expected from plan generation responses."""

    model_config = ConfigDict(extra="forbid")

    title: str
    description: str
    owner_type: str | None = None
    suggested_engine: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    files_or_modules: list[str] = Field(default_factory=list)
    acceptance_criteria: list[str]
    estimated_complexity: str | None = None
    estimated_hours: float | None = None
    risk_level: str | None = None
    test_command: str | None = None


class PlanMilestoneSpec(BaseModel):
    """Milestone shape expected from plan generation responses."""

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str
    tasks: list[PlanTaskSpec] = Field(default_factory=list)


class MilestoneOutlineSpec(BaseModel):
    """Milestone outline shape used during staged generation."""

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str


class PlanGenerationResponse(BaseModel):
    """Complete plan generation response shape."""

    model_config = ConfigDict(extra="forbid")

    target_engine: str | None = None
    target_repo: str | None = None
    project_type: str | None = None
    milestones: list[PlanMilestoneSpec]
    test_strategy: str | None = None
    handoff_prompt: str | None = None


class MilestoneOutlineResponse(BaseModel):
    """Milestone-only response used by staged generation."""

    model_config = ConfigDict(extra="forbid")

    milestones: list[MilestoneOutlineSpec]


class MilestoneTasksResponse(BaseModel):
    """Task-only response used by staged generation."""

    model_config = ConfigDict(extra="forbid")

    tasks: list[PlanTaskSpec]


class PlanMetadataResponse(BaseModel):
    """Metadata-only response used by staged generation."""

    model_config = ConfigDict(extra="forbid")

    target_engine: str | None = None
    target_repo: str | None = None
    project_type: str | None = None
    test_strategy: str | None = None
    handoff_prompt: str | None = None


def parse_and_validate_llm_json(
    content: str,
    schema: type[BaseModel],
    *,
    context: str,
) -> dict[str, Any]:
    """
    Extract, repair, and validate JSON from an LLM response.

    The helper is intentionally conservative: it repairs only the cases the
    generators need to tolerate in normal operation, and it raises an actionable
    error when the response is still unrecoverable.
    """
    candidates = build_json_candidates(content)
    last_error: Exception | None = None
    last_stage = "direct JSON parse"
    last_snippet = _truncate(content)

    for candidate in candidates:
        for repair_stage, repaired in _repair_variants(candidate.content):
            candidate_stage = f"{candidate.stage} -> {repair_stage}"
            try:
                parsed = json.loads(repaired)
            except json.JSONDecodeError as exc:
                last_error = exc
                last_stage = candidate_stage
                last_snippet = _truncate(repaired)
                continue

            try:
                validated = schema.model_validate(parsed)
            except ValidationError as exc:
                last_error = exc
                last_stage = f"{candidate_stage} -> schema validation"
                last_snippet = _truncate(repaired)
                continue

            return validated.model_dump(mode="python", exclude_none=False)

    debug_file = write_debug_response(content)
    message = (
        f"Failed to parse LLM response for {context} after trying multiple repair "
        f"strategies.\n"
        f"Last stage: {last_stage}\n"
        f"Snippet: {last_snippet}\n"
        f"Response saved to: {debug_file}"
    )
    if last_error is not None:
        message = f"{message}\nLast error: {last_error}"
    raise JsonRepairError(
        message,
        context=context,
        stage=last_stage,
        snippet=last_snippet,
        debug_file=debug_file,
    )


def validate_execution_plan_payload(
    plan: dict[str, Any],
    tasks: list[dict[str, Any]],
) -> None:
    """Validate the final plan/task payload before persistence."""
    payload = dict(plan)
    payload["tasks"] = tasks
    try:
        ExecutionPlan.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(
            "Constructed execution plan payload failed validation before persistence.\n"
            f"Validation error: {exc}"
        ) from exc


def register_task_aliases(
    task_id_map: dict[str, str],
    *,
    milestone_name: str,
    milestone_index: int,
    task_index: int,
    task_id: str,
) -> None:
    """Register common dependency aliases for a generated task."""
    aliases = {
        f"{milestone_name}:{task_index}",
        f"Milestone {milestone_index + 1}:{task_index}",
    }
    if ":" in milestone_name:
        aliases.add(f"{milestone_name.split(':', 1)[0]}:{task_index}")

    for alias in aliases:
        task_id_map[alias] = task_id


def _repair_variants(candidate: str) -> list[tuple[str, str]]:
    """Produce a small set of safe repair attempts for one candidate."""
    variants: list[tuple[str, str]] = []

    def add_variant(stage: str, text: str) -> None:
        normalized = text.strip()
        if normalized and normalized not in {variant for _, variant in variants}:
            variants.append((stage, normalized))

    add_variant("original", candidate)

    stripped = _strip_language_hint(candidate)
    if stripped != candidate:
        add_variant("removed language hint", stripped)

    comma_repaired = _strip_trailing_commas(stripped)
    if comma_repaired != stripped:
        add_variant("removed trailing commas", comma_repaired)

    comma_repaired_original = _strip_trailing_commas(candidate)
    if comma_repaired_original != candidate:
        add_variant("removed trailing commas", comma_repaired_original)

    return variants


def _strip_language_hint(candidate: str) -> str:
    """Remove a stray leading language hint from a code-block extraction."""
    stripped = candidate.strip()
    if stripped.startswith("json\n"):
        return stripped[5:].strip()
    if stripped.startswith("json\r\n"):
        return stripped[6:].strip()
    return stripped


def _strip_trailing_commas(candidate: str) -> str:
    """Remove the trailing commas most commonly emitted by LLM JSON output."""
    return re.sub(r",(\s*[}\]])", r"\1", candidate)


def _truncate(text: str, limit: int = 500) -> str:
    """Create a compact snippet for error messages."""
    compact = text.strip().replace("\r", "\\r").replace("\n", "\\n")
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit]}..."
