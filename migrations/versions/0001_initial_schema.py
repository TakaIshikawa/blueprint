"""Initial Blueprint schema.

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-04-28
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the initial Blueprint schema."""
    op.create_table(
        "source_briefs",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("domain", sa.String(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("source_project", sa.String(), nullable=False),
        sa.Column("source_entity_type", sa.String(), nullable=False),
        sa.Column("source_id", sa.String(), nullable=False),
        sa.Column("source_payload", sa.JSON(), nullable=False),
        sa.Column("source_links", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_project",
            "source_entity_type",
            "source_id",
            name="uq_source_briefs_source_key",
        ),
    )
    op.create_table(
        "implementation_briefs",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("source_brief_id", sa.String(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("domain", sa.String(), nullable=True),
        sa.Column("target_user", sa.Text(), nullable=True),
        sa.Column("buyer", sa.Text(), nullable=True),
        sa.Column("workflow_context", sa.Text(), nullable=True),
        sa.Column("problem_statement", sa.Text(), nullable=False),
        sa.Column("mvp_goal", sa.Text(), nullable=False),
        sa.Column("product_surface", sa.Text(), nullable=True),
        sa.Column("scope", sa.JSON(), nullable=False),
        sa.Column("non_goals", sa.JSON(), nullable=False),
        sa.Column("assumptions", sa.JSON(), nullable=False),
        sa.Column("architecture_notes", sa.Text(), nullable=True),
        sa.Column("data_requirements", sa.Text(), nullable=True),
        sa.Column("integration_points", sa.JSON(), nullable=True),
        sa.Column("risks", sa.JSON(), nullable=False),
        sa.Column("validation_plan", sa.Text(), nullable=False),
        sa.Column("definition_of_done", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("generation_model", sa.String(), nullable=True),
        sa.Column("generation_tokens", sa.Integer(), nullable=True),
        sa.Column("generation_prompt", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["source_brief_id"], ["source_briefs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "execution_plans",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("implementation_brief_id", sa.String(), nullable=False),
        sa.Column("target_engine", sa.String(), nullable=True),
        sa.Column("target_repo", sa.String(), nullable=True),
        sa.Column("project_type", sa.String(), nullable=True),
        sa.Column("milestones", sa.JSON(), nullable=False),
        sa.Column("test_strategy", sa.Text(), nullable=True),
        sa.Column("handoff_prompt", sa.Text(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("generation_model", sa.String(), nullable=True),
        sa.Column("generation_tokens", sa.Integer(), nullable=True),
        sa.Column("generation_prompt", sa.Text(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["implementation_brief_id"], ["implementation_briefs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "execution_tasks",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("execution_plan_id", sa.String(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("milestone", sa.String(), nullable=True),
        sa.Column("owner_type", sa.String(), nullable=True),
        sa.Column("suggested_engine", sa.String(), nullable=True),
        sa.Column("depends_on", sa.JSON(), nullable=False),
        sa.Column("files_or_modules", sa.JSON(), nullable=True),
        sa.Column("acceptance_criteria", sa.JSON(), nullable=False),
        sa.Column("estimated_complexity", sa.String(), nullable=True),
        sa.Column("estimated_hours", sa.Float(), nullable=True),
        sa.Column("risk_level", sa.String(), nullable=True),
        sa.Column("test_command", sa.Text(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["execution_plan_id"], ["execution_plans.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "export_records",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("execution_plan_id", sa.String(), nullable=False),
        sa.Column("target_engine", sa.String(), nullable=False),
        sa.Column("export_format", sa.String(), nullable=False),
        sa.Column("output_path", sa.String(), nullable=False),
        sa.Column("exported_at", sa.DateTime(), nullable=False),
        sa.Column("export_metadata", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["execution_plan_id"], ["execution_plans.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "status_events",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("entity_type", sa.String(), nullable=False),
        sa.Column("entity_id", sa.String(), nullable=False),
        sa.Column("old_status", sa.String(), nullable=False),
        sa.Column("new_status", sa.String(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Drop the initial Blueprint schema."""
    op.drop_table("status_events")
    op.drop_table("export_records")
    op.drop_table("execution_tasks")
    op.drop_table("execution_plans")
    op.drop_table("implementation_briefs")
    op.drop_table("source_briefs")
