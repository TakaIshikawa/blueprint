"""Database access layer for Blueprint."""

from pathlib import Path
from typing import Any

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from blueprint.store.models import (
    Base,
    ExecutionPlanModel,
    ExecutionTaskModel,
    ExportRecordModel,
    ImplementationBriefModel,
    SourceBriefModel,
)


class Store:
    """Blueprint database store."""

    def __init__(self, db_path: str):
        """Initialize store with database path."""
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.SessionLocal = sessionmaker(bind=self.engine)

    def init_db(self):
        """Initialize database tables."""
        # Create parent directory if it doesn't exist
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        Base.metadata.create_all(self.engine)

    def get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionLocal()

    # ============================================================================
    # Source Briefs
    # ============================================================================

    def insert_source_brief(self, brief_dict: dict[str, Any]) -> str:
        """Insert a source brief and return its ID."""
        with self.get_session() as session:
            brief = SourceBriefModel(**brief_dict)
            session.add(brief)
            session.commit()
            return brief.id

    def get_source_brief(self, brief_id: str) -> dict[str, Any] | None:
        """Get a source brief by ID."""
        with self.get_session() as session:
            brief = session.query(SourceBriefModel).filter_by(id=brief_id).first()
            if not brief:
                return None
            return self._source_brief_to_dict(brief)

    def get_source_brief_by_source(
        self,
        source_project: str,
        source_entity_type: str,
        source_id: str,
    ) -> dict[str, Any] | None:
        """Get a source brief by its upstream source identity."""
        with self.get_session() as session:
            brief = (
                session.query(SourceBriefModel)
                .filter_by(
                    source_project=source_project,
                    source_entity_type=source_entity_type,
                    source_id=source_id,
                )
                .order_by(SourceBriefModel.created_at.asc())
                .first()
            )
            if not brief:
                return None
            return self._source_brief_to_dict(brief)

    def upsert_source_brief(
        self,
        brief_dict: dict[str, Any],
        *,
        replace: bool = False,
        skip_existing: bool = False,
    ) -> str:
        """
        Insert or reuse a source brief keyed by its upstream source identity.

        When replace is true, the existing local SourceBrief ID is preserved and
        all import-owned fields are refreshed from brief_dict. Otherwise the
        existing row is returned unchanged.
        """
        if replace and skip_existing:
            raise ValueError("replace and skip_existing cannot both be true")

        source_project = brief_dict["source_project"]
        source_entity_type = brief_dict["source_entity_type"]
        source_id = brief_dict["source_id"]

        with self.get_session() as session:
            existing = (
                session.query(SourceBriefModel)
                .filter_by(
                    source_project=source_project,
                    source_entity_type=source_entity_type,
                    source_id=source_id,
                )
                .order_by(SourceBriefModel.created_at.asc())
                .first()
            )

            if not existing:
                brief = SourceBriefModel(**brief_dict)
                session.add(brief)
                session.commit()
                return brief.id

            if replace:
                for field in (
                    "title",
                    "domain",
                    "summary",
                    "source_payload",
                    "source_links",
                ):
                    setattr(existing, field, brief_dict[field])
                existing.updated_at = brief_dict.get("updated_at") or datetime.utcnow()
                session.commit()

            return existing.id

    def list_source_briefs(
        self,
        source_project: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List source briefs with optional filtering."""
        with self.get_session() as session:
            query = session.query(SourceBriefModel)
            if source_project:
                query = query.filter_by(source_project=source_project)
            query = query.order_by(SourceBriefModel.created_at.desc()).limit(limit)
            return [self._source_brief_to_dict(b) for b in query.all()]

    def _source_brief_to_dict(self, brief: SourceBriefModel) -> dict[str, Any]:
        """Convert SQLAlchemy model to dict."""
        return {
            "id": brief.id,
            "title": brief.title,
            "domain": brief.domain,
            "summary": brief.summary,
            "source_project": brief.source_project,
            "source_entity_type": brief.source_entity_type,
            "source_id": brief.source_id,
            "source_payload": brief.source_payload,
            "source_links": brief.source_links,
            "created_at": brief.created_at.isoformat() if brief.created_at else None,
            "updated_at": brief.updated_at.isoformat() if brief.updated_at else None,
        }

    # ============================================================================
    # Implementation Briefs
    # ============================================================================

    def insert_implementation_brief(self, brief_dict: dict[str, Any]) -> str:
        """Insert an implementation brief and return its ID."""
        with self.get_session() as session:
            brief = ImplementationBriefModel(**brief_dict)
            session.add(brief)
            session.commit()
            return brief.id

    def get_implementation_brief(self, brief_id: str) -> dict[str, Any] | None:
        """Get an implementation brief by ID."""
        with self.get_session() as session:
            brief = session.query(ImplementationBriefModel).filter_by(id=brief_id).first()
            if not brief:
                return None
            return self._implementation_brief_to_dict(brief)

    def list_implementation_briefs(
        self,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List implementation briefs with optional filtering."""
        with self.get_session() as session:
            query = session.query(ImplementationBriefModel)
            if status:
                query = query.filter_by(status=status)
            query = query.order_by(ImplementationBriefModel.created_at.desc()).limit(limit)
            return [self._implementation_brief_to_dict(b) for b in query.all()]

    def update_implementation_brief_status(self, brief_id: str, status: str) -> bool:
        """Update implementation brief status."""
        with self.get_session() as session:
            brief = session.query(ImplementationBriefModel).filter_by(id=brief_id).first()
            if not brief:
                return False
            brief.status = status
            session.commit()
            return True

    def _implementation_brief_to_dict(self, brief: ImplementationBriefModel) -> dict[str, Any]:
        """Convert SQLAlchemy model to dict."""
        return {
            "id": brief.id,
            "source_brief_id": brief.source_brief_id,
            "title": brief.title,
            "domain": brief.domain,
            "target_user": brief.target_user,
            "buyer": brief.buyer,
            "workflow_context": brief.workflow_context,
            "problem_statement": brief.problem_statement,
            "mvp_goal": brief.mvp_goal,
            "product_surface": brief.product_surface,
            "scope": brief.scope,
            "non_goals": brief.non_goals,
            "assumptions": brief.assumptions,
            "architecture_notes": brief.architecture_notes,
            "data_requirements": brief.data_requirements,
            "integration_points": brief.integration_points,
            "risks": brief.risks,
            "validation_plan": brief.validation_plan,
            "definition_of_done": brief.definition_of_done,
            "status": brief.status,
            "created_at": brief.created_at.isoformat() if brief.created_at else None,
            "updated_at": brief.updated_at.isoformat() if brief.updated_at else None,
            "generation_model": brief.generation_model,
            "generation_tokens": brief.generation_tokens,
        }

    # ============================================================================
    # Execution Plans
    # ============================================================================

    def insert_execution_plan(
        self, plan_dict: dict[str, Any], tasks: list[dict[str, Any]]
    ) -> str:
        """Insert an execution plan with tasks and return plan ID."""
        with self.get_session() as session:
            plan = ExecutionPlanModel(**plan_dict)
            session.add(plan)
            session.flush()  # Get plan ID

            # Insert tasks
            for task_dict in tasks:
                task_dict["execution_plan_id"] = plan.id
                task = ExecutionTaskModel(**task_dict)
                session.add(task)

            session.commit()
            return plan.id

    def get_execution_plan(self, plan_id: str) -> dict[str, Any] | None:
        """Get an execution plan by ID with its tasks."""
        with self.get_session() as session:
            plan = session.query(ExecutionPlanModel).filter_by(id=plan_id).first()
            if not plan:
                return None
            return self._execution_plan_to_dict(plan)

    def list_execution_plans(
        self,
        brief_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List execution plans with optional filtering."""
        with self.get_session() as session:
            query = session.query(ExecutionPlanModel)
            if brief_id:
                query = query.filter_by(implementation_brief_id=brief_id)
            if status:
                query = query.filter_by(status=status)
            query = query.order_by(ExecutionPlanModel.created_at.desc()).limit(limit)
            return [self._execution_plan_to_dict(p) for p in query.all()]

    def update_execution_plan_status(self, plan_id: str, status: str) -> bool:
        """Update execution plan status."""
        with self.get_session() as session:
            plan = session.query(ExecutionPlanModel).filter_by(id=plan_id).first()
            if not plan:
                return False
            plan.status = status
            session.commit()
            return True

    def _execution_plan_to_dict(self, plan: ExecutionPlanModel) -> dict[str, Any]:
        """Convert SQLAlchemy model to dict with tasks."""
        return {
            "id": plan.id,
            "implementation_brief_id": plan.implementation_brief_id,
            "target_engine": plan.target_engine,
            "target_repo": plan.target_repo,
            "project_type": plan.project_type,
            "milestones": plan.milestones,
            "test_strategy": plan.test_strategy,
            "handoff_prompt": plan.handoff_prompt,
            "status": plan.status,
            "created_at": plan.created_at.isoformat() if plan.created_at else None,
            "updated_at": plan.updated_at.isoformat() if plan.updated_at else None,
            "generation_model": plan.generation_model,
            "generation_tokens": plan.generation_tokens,
            "tasks": [self._task_to_dict(t) for t in plan.tasks],
        }

    def _task_to_dict(self, task: ExecutionTaskModel) -> dict[str, Any]:
        """Convert task model to dict."""
        return {
            "id": task.id,
            "execution_plan_id": task.execution_plan_id,
            "title": task.title,
            "description": task.description,
            "milestone": task.milestone,
            "owner_type": task.owner_type,
            "suggested_engine": task.suggested_engine,
            "depends_on": task.depends_on,
            "files_or_modules": task.files_or_modules,
            "acceptance_criteria": task.acceptance_criteria,
            "estimated_complexity": task.estimated_complexity,
            "status": task.status,
            "created_at": task.created_at.isoformat() if task.created_at else None,
            "updated_at": task.updated_at.isoformat() if task.updated_at else None,
        }

    # ============================================================================
    # Execution Tasks
    # ============================================================================

    def get_execution_task(self, task_id: str) -> dict[str, Any] | None:
        """Get an execution task by ID."""
        with self.get_session() as session:
            task = session.query(ExecutionTaskModel).filter_by(id=task_id).first()
            if not task:
                return None
            return self._task_to_dict(task)

    def list_execution_tasks(
        self,
        plan_id: str | None = None,
        status: str | None = None,
        milestone: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List execution tasks with optional filtering."""
        with self.get_session() as session:
            query = session.query(ExecutionTaskModel)
            if plan_id:
                query = query.filter_by(execution_plan_id=plan_id)
            if status:
                query = query.filter_by(status=status)
            if milestone:
                query = query.filter_by(milestone=milestone)
            query = query.order_by(ExecutionTaskModel.created_at.asc()).limit(limit)
            return [self._task_to_dict(t) for t in query.all()]

    def update_execution_task_status(self, task_id: str, status: str) -> bool:
        """Update execution task status."""
        with self.get_session() as session:
            task = session.query(ExecutionTaskModel).filter_by(id=task_id).first()
            if not task:
                return False
            task.status = status
            session.commit()
            return True

    # ============================================================================
    # Export Records
    # ============================================================================

    def insert_export_record(self, export_dict: dict[str, Any]) -> str:
        """Insert an export record and return its ID."""
        with self.get_session() as session:
            export = ExportRecordModel(**export_dict)
            session.add(export)
            session.commit()
            return export.id

    def list_export_records(
        self,
        plan_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List export records with optional filtering."""
        with self.get_session() as session:
            query = session.query(ExportRecordModel)
            if plan_id:
                query = query.filter_by(execution_plan_id=plan_id)
            query = query.order_by(ExportRecordModel.exported_at.desc()).limit(limit)
            return [self._export_to_dict(e) for e in query.all()]

    def _export_to_dict(self, export: ExportRecordModel) -> dict[str, Any]:
        """Convert export model to dict."""
        return {
            "id": export.id,
            "execution_plan_id": export.execution_plan_id,
            "target_engine": export.target_engine,
            "export_format": export.export_format,
            "output_path": export.output_path,
            "exported_at": export.exported_at.isoformat() if export.exported_at else None,
            "export_metadata": export.export_metadata,
        }


def init_db(db_path: str):
    """Initialize database at path."""
    store = Store(db_path)
    store.init_db()
    return store
