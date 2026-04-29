"""Pydantic domain models for Blueprint records."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


BriefStatus = Literal[
    "draft",
    "ready_for_planning",
    "planned",
    "queued",
    "in_progress",
    "implemented",
    "validated",
    "paused",
    "rejected",
]
PlanStatus = Literal["draft", "ready", "queued", "in_progress", "completed", "failed"]
TaskStatus = Literal["pending", "in_progress", "completed", "blocked", "skipped"]
ExportFormat = Literal[
    "json",
    "yaml",
    "markdown",
    "mermaid",
    "dot",
    "csv",
    "xml",
    "icalendar",
    "jsonl",
    "html",
]


class DomainModel(BaseModel):
    """Shared validation behavior for domain records."""

    model_config = ConfigDict(extra="forbid")


class SourceBrief(DomainModel):
    """Normalized source brief from any upstream system."""

    id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    domain: str | None = None
    summary: str = Field(min_length=1)
    source_project: str = Field(min_length=1)
    source_entity_type: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    source_payload: dict[str, Any]
    source_links: dict[str, Any]
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ImplementationBrief(DomainModel):
    """Engine-neutral implementation specification."""

    id: str = Field(min_length=1)
    source_brief_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    domain: str | None = None
    target_user: str | None = None
    buyer: str | None = None
    workflow_context: str | None = None
    problem_statement: str = Field(min_length=1)
    mvp_goal: str = Field(min_length=1)
    product_surface: str | None = None
    scope: list[str]
    non_goals: list[str]
    assumptions: list[str]
    architecture_notes: str | None = None
    data_requirements: str | None = None
    integration_points: list[str] = Field(default_factory=list)
    risks: list[str]
    validation_plan: str = Field(min_length=1)
    definition_of_done: list[str]
    status: BriefStatus = "draft"
    created_at: datetime | None = None
    updated_at: datetime | None = None
    generation_model: str | None = None
    generation_tokens: int | None = Field(default=None, ge=0)
    generation_prompt: str | None = None


class ExecutionTask(DomainModel):
    """Individual task within an execution plan."""

    id: str = Field(min_length=1)
    execution_plan_id: str | None = None
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    milestone: str | None = None
    owner_type: str | None = None
    suggested_engine: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    files_or_modules: list[str] | None = None
    acceptance_criteria: list[str]
    estimated_complexity: str | None = None
    estimated_hours: float | None = Field(default=None, ge=0)
    risk_level: str | None = None
    test_command: str | None = None
    status: TaskStatus = "pending"
    metadata: dict[str, Any] = Field(default_factory=dict)
    blocked_reason: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ExecutionPlan(DomainModel):
    """Task-level execution plan for an implementation brief."""

    id: str = Field(min_length=1)
    implementation_brief_id: str = Field(min_length=1)
    target_engine: str | None = None
    target_repo: str | None = None
    project_type: str | None = None
    milestones: list[dict[str, Any]]
    test_strategy: str | None = None
    handoff_prompt: str | None = None
    status: PlanStatus = "draft"
    created_at: datetime | None = None
    updated_at: datetime | None = None
    generation_model: str | None = None
    generation_tokens: int | None = Field(default=None, ge=0)
    generation_prompt: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    tasks: list[ExecutionTask] = Field(default_factory=list)


class StatusEvent(DomainModel):
    """Audit event for a status transition."""

    id: str = Field(min_length=1)
    entity_type: Literal["brief", "plan", "task"]
    entity_id: str = Field(min_length=1)
    old_status: str = Field(min_length=1)
    new_status: str = Field(min_length=1)
    reason: str | None = None
    created_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExportRecord(DomainModel):
    """Record of a rendered execution-plan export."""

    id: str = Field(min_length=1)
    execution_plan_id: str = Field(min_length=1)
    target_engine: str = Field(min_length=1)
    export_format: ExportFormat
    output_path: str = Field(min_length=1)
    exported_at: datetime | None = None
    export_metadata: dict[str, Any] | None = None
