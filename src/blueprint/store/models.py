"""SQLAlchemy models for Blueprint database."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class SourceBriefModel(Base):
    """Normalized source brief from any design system."""

    __tablename__ = "source_briefs"
    __table_args__ = (
        UniqueConstraint(
            "source_project",
            "source_entity_type",
            "source_id",
            name="uq_source_briefs_source_key",
        ),
    )

    id = Column(String, primary_key=True)  # sb-{uuid12}
    title = Column(String, nullable=False)
    domain = Column(String, nullable=True)
    summary = Column(Text, nullable=False)

    # Source metadata
    source_project = Column(String, nullable=False)  # max | graph | manual | etc.
    source_entity_type = Column(String, nullable=False)  # design_brief | note | issue | etc.
    source_id = Column(String, nullable=False)  # Original ID in source system

    # Flexible payload - preserves original structure
    source_payload = Column(JSON, nullable=False)  # Raw data from source
    source_links = Column(JSON, nullable=False)  # Links back to source (URLs, paths)

    # Metadata
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    implementation_briefs = relationship("ImplementationBriefModel", back_populates="source_brief")


class ImplementationBriefModel(Base):
    """Engine-neutral implementation specification."""

    __tablename__ = "implementation_briefs"

    id = Column(String, primary_key=True)  # ib-{uuid12}
    source_brief_id = Column(String, ForeignKey("source_briefs.id"), nullable=False)

    title = Column(String, nullable=False)
    domain = Column(String, nullable=True)

    # Context
    target_user = Column(Text, nullable=True)
    buyer = Column(Text, nullable=True)
    workflow_context = Column(Text, nullable=True)
    problem_statement = Column(Text, nullable=False)

    # Scope
    mvp_goal = Column(Text, nullable=False)
    product_surface = Column(Text, nullable=True)  # UI, API, CLI, library, etc.
    scope = Column(JSON, nullable=False)  # List of what's in scope
    non_goals = Column(JSON, nullable=False)  # List of what's explicitly out
    assumptions = Column(JSON, nullable=False)  # List of assumptions

    # Technical
    architecture_notes = Column(Text, nullable=True)
    data_requirements = Column(Text, nullable=True)
    integration_points = Column(JSON, nullable=True)  # List of systems to integrate with

    # Quality
    risks = Column(JSON, nullable=False)  # List of key risks
    validation_plan = Column(Text, nullable=False)
    definition_of_done = Column(JSON, nullable=False)  # List of completion criteria

    # Status
    status = Column(
        String,
        nullable=False,
        default="draft",
    )  # draft | ready_for_planning | planned | queued | in_progress | implemented | validated | paused | rejected

    # Metadata
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # LLM generation metadata
    generation_model = Column(String, nullable=True)  # Model used for generation
    generation_tokens = Column(Integer, nullable=True)  # Tokens used
    generation_prompt = Column(Text, nullable=True)  # Prompt used (for debugging)

    # Relationships
    source_brief = relationship("SourceBriefModel", back_populates="implementation_briefs")
    execution_plans = relationship("ExecutionPlanModel", back_populates="implementation_brief")


class ExecutionPlanModel(Base):
    """Task-level execution plan for an implementation brief."""

    __tablename__ = "execution_plans"

    id = Column(String, primary_key=True)  # plan-{uuid12}
    implementation_brief_id = Column(String, ForeignKey("implementation_briefs.id"), nullable=False)

    # Target
    target_engine = Column(String, nullable=True)  # relay | smoothie | codex | claude_code | manual
    target_repo = Column(String, nullable=True)  # Repo name or path
    project_type = Column(String, nullable=True)  # python_library | web_app | cli_tool | etc.

    # Plan structure
    milestones = Column(JSON, nullable=False)  # List of milestone dicts
    test_strategy = Column(Text, nullable=True)
    handoff_prompt = Column(Text, nullable=True)  # Context for execution engine

    # Status
    status = Column(
        String,
        nullable=False,
        default="draft",
    )  # draft | ready | queued | in_progress | completed | failed

    # Metadata
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # LLM generation metadata
    generation_model = Column(String, nullable=True)
    generation_tokens = Column(Integer, nullable=True)
    generation_prompt = Column(Text, nullable=True)

    # Relationships
    implementation_brief = relationship("ImplementationBriefModel", back_populates="execution_plans")
    tasks = relationship("ExecutionTaskModel", back_populates="execution_plan")
    exports = relationship("ExportRecordModel", back_populates="execution_plan")


class ExecutionTaskModel(Base):
    """Individual task within an execution plan."""

    __tablename__ = "execution_tasks"

    id = Column(String, primary_key=True)  # task-{uuid12}
    execution_plan_id = Column(String, ForeignKey("execution_plans.id"), nullable=False)

    # Task definition
    title = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    milestone = Column(String, nullable=True)  # Which milestone this belongs to

    # Assignment
    owner_type = Column(String, nullable=True)  # human | agent | either
    suggested_engine = Column(String, nullable=True)  # relay | smoothie | codex | claude_code

    # Dependencies
    depends_on = Column(JSON, nullable=False, default=list)  # List of task IDs

    # Context
    files_or_modules = Column(JSON, nullable=True)  # List of files/modules to work on
    acceptance_criteria = Column(JSON, nullable=False)  # List of AC items
    estimated_complexity = Column(String, nullable=True)  # low | medium | high

    # Status
    status = Column(
        String,
        nullable=False,
        default="pending",
    )  # pending | in_progress | completed | blocked | skipped

    # Metadata
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    execution_plan = relationship("ExecutionPlanModel", back_populates="tasks")


class ExportRecordModel(Base):
    """Record of exports to execution engines."""

    __tablename__ = "export_records"

    id = Column(String, primary_key=True)  # exp-{uuid12}
    execution_plan_id = Column(String, ForeignKey("execution_plans.id"), nullable=False)

    # Export details
    target_engine = Column(String, nullable=False)  # relay | smoothie | codex | claude_code
    export_format = Column(String, nullable=False)  # json | yaml | markdown
    output_path = Column(String, nullable=False)  # Where the export was written

    # Metadata
    exported_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    export_metadata = Column(JSON, nullable=True)  # Additional export context

    # Relationships
    execution_plan = relationship("ExecutionPlanModel", back_populates="exports")
