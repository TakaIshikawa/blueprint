"""Database access layer for Blueprint."""

from copy import deepcopy
from pathlib import Path
from typing import Any
import uuid

from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import inspect as inspect_db
from sqlalchemy import or_
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from blueprint.domain import (
    ExecutionPlan,
    ExecutionTask,
    ExportRecord,
    ImplementationBrief,
    SourceBrief,
    StatusEvent,
)
from blueprint.store.models import (
    Base,
    ExecutionPlanModel,
    ExecutionTaskModel,
    ExportRecordModel,
    ImplementationBriefModel,
    SourceBriefModel,
    StatusEventModel,
)


class Store:
    """Blueprint database store."""

    def __init__(self, db_path: str):
        """Initialize store with database path."""
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.SessionLocal = sessionmaker(bind=self.engine)
        if Path(self.db_path).exists():
            self._migrate_schema()

    def init_db(self):
        """Initialize database tables."""
        # Create parent directory if it doesn't exist
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        Base.metadata.create_all(self.engine)
        self._migrate_schema()

    def _migrate_schema(self):
        """Apply lightweight migrations for existing SQLite databases."""
        inspector = inspect_db(self.engine)
        table_names = inspector.get_table_names()
        if "status_events" not in table_names:
            StatusEventModel.__table__.create(self.engine, checkfirst=True)

        if "execution_plans" in table_names:
            plan_columns = {
                column["name"] for column in inspector.get_columns("execution_plans")
            }
            if "metadata" not in plan_columns:
                with self.engine.begin() as connection:
                    connection.execute(
                        text("ALTER TABLE execution_plans ADD COLUMN metadata JSON")
                    )

        if "execution_tasks" not in table_names:
            return

        task_columns = {column["name"] for column in inspector.get_columns("execution_tasks")}
        if "metadata" not in task_columns:
            with self.engine.begin() as connection:
                connection.execute(
                    text("ALTER TABLE execution_tasks ADD COLUMN metadata JSON")
                )

    def get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionLocal()

    # ============================================================================
    # Source Briefs
    # ============================================================================

    def insert_source_brief(self, brief_dict: dict[str, Any]) -> str:
        """Insert a source brief and return its ID."""
        brief_payload = self._validated_source_brief_payload(brief_dict)
        with self.get_session() as session:
            brief = SourceBriefModel(**brief_payload)
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

        brief_payload = self._validated_source_brief_payload(brief_dict)
        source_project = brief_payload["source_project"]
        source_entity_type = brief_payload["source_entity_type"]
        source_id = brief_payload["source_id"]

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
                brief = SourceBriefModel(**brief_payload)
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
                    setattr(existing, field, brief_payload.get(field))
                existing.updated_at = brief_payload.get("updated_at") or datetime.utcnow()
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
        brief_payload = self._validated_implementation_brief_payload(brief_dict)
        with self.get_session() as session:
            brief = ImplementationBriefModel(**brief_payload)
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

    def update_implementation_brief_status(
        self,
        brief_id: str,
        status: str,
        reason: str | None = None,
    ) -> bool:
        """Update implementation brief status."""
        with self.get_session() as session:
            brief = session.query(ImplementationBriefModel).filter_by(id=brief_id).first()
            if not brief:
                return False
            old_status = brief.status
            brief.status = status
            self._add_status_event(
                session,
                entity_type="brief",
                entity_id=brief_id,
                old_status=old_status,
                new_status=status,
                reason=reason,
            )
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
            "generation_prompt": brief.generation_prompt,
        }

    # ============================================================================
    # Execution Plans
    # ============================================================================

    def insert_execution_plan(
        self, plan_dict: dict[str, Any], tasks: list[dict[str, Any]]
    ) -> str:
        """Insert an execution plan with tasks and return plan ID."""
        plan_payload = self._validated_execution_plan_payload(plan_dict)
        with self.get_session() as session:
            plan = ExecutionPlanModel(**plan_payload)
            session.add(plan)
            session.flush()  # Get plan ID

            # Insert tasks
            for task_dict in tasks:
                task_payload = self._validated_execution_task_payload(
                    task_dict,
                    execution_plan_id=plan.id,
                )
                task = ExecutionTaskModel(**task_payload)
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

    def clone_execution_plan(
        self,
        plan_id: str,
        reset_statuses: bool = True,
    ) -> str | None:
        """Clone an execution plan and its tasks, returning the new plan ID."""
        with self.get_session() as session:
            source_plan = session.query(ExecutionPlanModel).filter_by(id=plan_id).first()
            if not source_plan:
                return None

            task_id_map = {
                task.id: self._generate_execution_task_id()
                for task in source_plan.tasks
            }
            cloned_plan_id = self._generate_execution_plan_id()
            source_metadata = deepcopy(source_plan.plan_metadata or {})
            lineage = dict(source_metadata.get("lineage") or {})
            lineage.update(
                {
                    "cloned_from_plan_id": plan_id,
                    "task_id_map": task_id_map,
                }
            )
            source_metadata["lineage"] = lineage

            cloned_plan = ExecutionPlanModel(
                id=cloned_plan_id,
                implementation_brief_id=source_plan.implementation_brief_id,
                target_engine=source_plan.target_engine,
                target_repo=source_plan.target_repo,
                project_type=source_plan.project_type,
                milestones=deepcopy(source_plan.milestones),
                test_strategy=source_plan.test_strategy,
                handoff_prompt=source_plan.handoff_prompt,
                status="draft" if reset_statuses else source_plan.status,
                generation_model=source_plan.generation_model,
                generation_tokens=source_plan.generation_tokens,
                generation_prompt=source_plan.generation_prompt,
                plan_metadata=source_metadata,
            )
            session.add(cloned_plan)
            session.flush()

            for source_task in source_plan.tasks:
                task_metadata = deepcopy(source_task.task_metadata or {})
                if reset_statuses:
                    task_metadata.pop("blocked_reason", None)
                cloned_task = ExecutionTaskModel(
                    id=task_id_map[source_task.id],
                    execution_plan_id=cloned_plan_id,
                    title=source_task.title,
                    description=source_task.description,
                    milestone=source_task.milestone,
                    owner_type=source_task.owner_type,
                    suggested_engine=source_task.suggested_engine,
                    depends_on=[
                        task_id_map.get(dependency_id, dependency_id)
                        for dependency_id in (source_task.depends_on or [])
                    ],
                    files_or_modules=deepcopy(source_task.files_or_modules),
                    acceptance_criteria=deepcopy(source_task.acceptance_criteria),
                    estimated_complexity=source_task.estimated_complexity,
                    status="pending" if reset_statuses else source_task.status,
                    task_metadata=task_metadata,
                )
                session.add(cloned_task)

            session.commit()
            return cloned_plan_id

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

    def search_execution_plans(
        self,
        query: str,
        status: str | None = None,
        target_engine: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Search execution plans and tasks with deterministic ranking."""
        search_text = query.strip()
        if not search_text or limit <= 0:
            return []

        normalized_query = search_text.lower()
        like_query = f"%{normalized_query}%"

        with self.get_session() as session:
            sql_query = session.query(ExecutionPlanModel).outerjoin(ExecutionTaskModel)
            if status:
                sql_query = sql_query.filter(ExecutionPlanModel.status == status)
            if target_engine:
                sql_query = sql_query.filter(
                    ExecutionPlanModel.target_engine == target_engine
                )

            sql_query = sql_query.filter(
                or_(
                    func.lower(ExecutionPlanModel.id).like(like_query),
                    func.lower(func.coalesce(ExecutionPlanModel.target_repo, "")).like(
                        like_query
                    ),
                    func.lower(func.coalesce(ExecutionPlanModel.project_type, "")).like(
                        like_query
                    ),
                    func.lower(func.coalesce(ExecutionPlanModel.handoff_prompt, "")).like(
                        like_query
                    ),
                    func.lower(func.coalesce(ExecutionPlanModel.test_strategy, "")).like(
                        like_query
                    ),
                    func.lower(ExecutionTaskModel.title).like(like_query),
                    func.lower(ExecutionTaskModel.description).like(like_query),
                    func.lower(func.coalesce(ExecutionTaskModel.acceptance_criteria, "")).like(
                        like_query
                    ),
                    func.lower(func.coalesce(ExecutionTaskModel.files_or_modules, "")).like(
                        like_query
                    ),
                )
            )

            candidate_plans = (
                sql_query.distinct()
                .order_by(ExecutionPlanModel.id.asc())
                .limit(max(limit * 10, limit))
                .all()
            )

            ranked_results = [
                result
                for plan in candidate_plans
                if (
                    result := self._build_execution_plan_search_result(
                        plan,
                        normalized_query,
                    )
                )
            ]
            ranked_results.sort(
                key=lambda result: (
                    -result["score"],
                    result["first_match_order"],
                    result["plan_id"],
                )
            )
            for result in ranked_results:
                result.pop("score", None)
                result.pop("first_match_order", None)
            return ranked_results[:limit]

    def update_execution_plan_status(
        self,
        plan_id: str,
        status: str,
        reason: str | None = None,
    ) -> bool:
        """Update execution plan status."""
        with self.get_session() as session:
            plan = session.query(ExecutionPlanModel).filter_by(id=plan_id).first()
            if not plan:
                return False
            old_status = plan.status
            plan.status = status
            self._add_status_event(
                session,
                entity_type="plan",
                entity_id=plan_id,
                old_status=old_status,
                new_status=status,
                reason=reason,
            )
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
            "generation_prompt": plan.generation_prompt,
            "metadata": plan.plan_metadata or {},
            "tasks": [self._task_to_dict(t) for t in plan.tasks],
        }

    def _build_execution_plan_search_result(
        self,
        plan: ExecutionPlanModel,
        normalized_query: str,
    ) -> dict[str, Any] | None:
        """Build ranked search metadata for a candidate plan."""
        matches: list[dict[str, Any]] = []
        matched_task_ids: list[str] = []
        matched_fields: list[str] = []
        score = 0
        first_match_order = 999

        plan_fields: list[tuple[str, Any, int, int]] = [
            ("id", plan.id, 100, 0),
            ("target_repo", plan.target_repo, 70, 1),
            ("project_type", plan.project_type, 60, 2),
            ("handoff_prompt", plan.handoff_prompt, 40, 3),
            ("test_strategy", plan.test_strategy, 40, 4),
        ]
        for field, value, weight, order in plan_fields:
            match = self._build_search_match(field, value, normalized_query)
            if not match:
                continue
            matches.append(match)
            matched_fields.append(field)
            score += weight
            first_match_order = min(first_match_order, order)

        task_fields: list[tuple[str, str, int, int]] = [
            ("title", "tasks.title", 55, 10),
            ("description", "tasks.description", 35, 11),
            ("acceptance_criteria", "tasks.acceptance_criteria", 30, 12),
            ("files_or_modules", "tasks.files_or_modules", 30, 13),
        ]
        for task in sorted(plan.tasks, key=lambda current_task: current_task.id):
            for attribute, field, weight, order in task_fields:
                match = self._build_search_match(
                    field,
                    getattr(task, attribute),
                    normalized_query,
                    task_id=task.id,
                )
                if not match:
                    continue
                matches.append(match)
                matched_fields.append(field)
                if task.id not in matched_task_ids:
                    matched_task_ids.append(task.id)
                score += weight
                first_match_order = min(first_match_order, order)

        if not matches:
            return None

        return {
            "plan_id": plan.id,
            "status": plan.status,
            "target_engine": plan.target_engine,
            "matched_task_ids": matched_task_ids,
            "matched_fields": sorted(set(matched_fields)),
            "matches": matches,
            "score": score,
            "first_match_order": first_match_order,
        }

    def _build_search_match(
        self,
        field: str,
        value: Any,
        normalized_query: str,
        task_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Return match metadata for a field when it contains the query."""
        searchable_text = self._stringify_search_value(value)
        if normalized_query not in searchable_text.lower():
            return None

        match: dict[str, Any] = {
            "field": field,
            "snippet": self._search_snippet(searchable_text, normalized_query),
        }
        if task_id:
            match["task_id"] = task_id
        return match

    def _stringify_search_value(self, value: Any) -> str:
        """Flatten scalar and JSON values into stable searchable text."""
        if value is None:
            return ""
        if isinstance(value, (list, tuple, set)):
            return " ".join(self._stringify_search_value(item) for item in value)
        if isinstance(value, dict):
            return " ".join(
                self._stringify_search_value(value[key])
                for key in sorted(value)
            )
        return str(value)

    def _search_snippet(
        self,
        text_value: str,
        normalized_query: str,
        context_chars: int = 36,
    ) -> str:
        """Return a short deterministic snippet around the first match."""
        normalized_text = text_value.lower()
        index = normalized_text.find(normalized_query)
        if index == -1:
            return text_value[: context_chars * 2].strip()

        start = max(index - context_chars, 0)
        end = min(index + len(normalized_query) + context_chars, len(text_value))
        prefix = "..." if start > 0 else ""
        suffix = "..." if end < len(text_value) else ""
        return f"{prefix}{text_value[start:end].strip()}{suffix}"

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
            "metadata": task.task_metadata or {},
            "blocked_reason": (task.task_metadata or {}).get("blocked_reason"),
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

    def update_execution_task_status(
        self,
        task_id: str,
        status: str,
        blocked_reason: str | None = None,
        reason: str | None = None,
    ) -> bool:
        """Update execution task status."""
        with self.get_session() as session:
            task = session.query(ExecutionTaskModel).filter_by(id=task_id).first()
            if not task:
                return False
            old_status = task.status
            task.status = status
            if blocked_reason is not None:
                task_metadata = dict(task.task_metadata or {})
                if blocked_reason:
                    task_metadata["blocked_reason"] = blocked_reason
                else:
                    task_metadata.pop("blocked_reason", None)
                task.task_metadata = task_metadata
            event_metadata = {}
            if blocked_reason:
                event_metadata["blocked_reason"] = blocked_reason
            self._add_status_event(
                session,
                entity_type="task",
                entity_id=task_id,
                old_status=old_status,
                new_status=status,
                reason=reason,
                metadata=event_metadata,
            )
            session.commit()
            return True

    def update_execution_task_dependencies(
        self,
        plan_id: str,
        task_id: str,
        depends_on: list[str],
    ) -> bool:
        """Update a task dependency list without changing status or metadata."""
        with self.get_session() as session:
            task = (
                session.query(ExecutionTaskModel)
                .filter_by(id=task_id, execution_plan_id=plan_id)
                .first()
            )
            if not task:
                return False

            task.depends_on = list(depends_on)
            session.commit()
            return True

    # ============================================================================
    # Status History
    # ============================================================================

    def list_status_events(
        self,
        entity_id: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List status history events for an entity."""
        with self.get_session() as session:
            query = (
                session.query(StatusEventModel)
                .filter_by(entity_id=entity_id)
                .order_by(StatusEventModel.created_at.asc())
                .limit(limit)
            )
            return [self._status_event_to_dict(event) for event in query.all()]

    def _add_status_event(
        self,
        session: Session,
        *,
        entity_type: str,
        entity_id: str,
        old_status: str,
        new_status: str,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record a status event when the status actually changes."""
        if old_status == new_status:
            return

        payload = self._validated_status_event_payload(
            {
                "id": self._generate_status_event_id(),
                "entity_type": entity_type,
                "entity_id": entity_id,
                "old_status": old_status,
                "new_status": new_status,
                "reason": reason,
                "metadata": metadata or {},
            }
        )
        session.add(StatusEventModel(**payload))

    def _status_event_to_dict(self, event: StatusEventModel) -> dict[str, Any]:
        """Convert status event model to dict."""
        return {
            "id": event.id,
            "entity_type": event.entity_type,
            "entity_id": event.entity_id,
            "old_status": event.old_status,
            "new_status": event.new_status,
            "reason": event.reason,
            "created_at": event.created_at.isoformat() if event.created_at else None,
            "metadata": event.event_metadata or {},
        }

    def _generate_status_event_id(self) -> str:
        """Generate a status event ID."""
        return f"se-{uuid.uuid4().hex[:12]}"

    def _generate_execution_plan_id(self) -> str:
        """Generate an execution plan ID."""
        return f"plan-{uuid.uuid4().hex[:12]}"

    def _generate_execution_task_id(self) -> str:
        """Generate an execution task ID."""
        return f"task-{uuid.uuid4().hex[:12]}"

    # ============================================================================
    # Export Records
    # ============================================================================

    def insert_export_record(self, export_dict: dict[str, Any]) -> str:
        """Insert an export record and return its ID."""
        export_payload = self._validated_export_record_payload(export_dict)
        with self.get_session() as session:
            export = ExportRecordModel(**export_payload)
            session.add(export)
            session.commit()
            return export.id

    def list_export_records(
        self,
        plan_id: str | None = None,
        limit: int | None = 50,
    ) -> list[dict[str, Any]]:
        """List export records with optional filtering."""
        with self.get_session() as session:
            query = session.query(ExportRecordModel)
            if plan_id:
                query = query.filter_by(execution_plan_id=plan_id)
            query = query.order_by(ExportRecordModel.exported_at.desc())
            if limit is not None:
                query = query.limit(limit)
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

    def _validated_source_brief_payload(self, brief_dict: dict[str, Any]) -> dict[str, Any]:
        """Validate and normalize a SourceBrief for database insertion."""
        return SourceBrief.model_validate(brief_dict).model_dump(
            mode="python",
            exclude_none=True,
        )

    def _validated_implementation_brief_payload(
        self,
        brief_dict: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate and normalize an ImplementationBrief for database insertion."""
        return ImplementationBrief.model_validate(brief_dict).model_dump(
            mode="python",
            exclude_none=True,
        )

    def _validated_execution_plan_payload(self, plan_dict: dict[str, Any]) -> dict[str, Any]:
        """Validate and normalize an ExecutionPlan for database insertion."""
        payload = ExecutionPlan.model_validate(plan_dict).model_dump(
            mode="python",
            exclude={"tasks"},
            exclude_none=True,
        )
        payload["plan_metadata"] = payload.pop("metadata", {})
        return payload

    def _validated_execution_task_payload(
        self,
        task_dict: dict[str, Any],
        *,
        execution_plan_id: str,
    ) -> dict[str, Any]:
        """Validate and normalize an ExecutionTask for database insertion."""
        task_payload = dict(task_dict)
        task_payload["execution_plan_id"] = execution_plan_id
        task = ExecutionTask.model_validate(task_payload)
        payload = task.model_dump(
            mode="python",
            exclude={"blocked_reason"},
            exclude_none=True,
        )
        payload["task_metadata"] = payload.pop("metadata", {})
        return payload

    def _validated_export_record_payload(self, export_dict: dict[str, Any]) -> dict[str, Any]:
        """Validate and normalize an ExportRecord for database insertion."""
        return ExportRecord.model_validate(export_dict).model_dump(
            mode="python",
            exclude_none=True,
        )

    def _validated_status_event_payload(
        self,
        event_dict: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate and normalize a StatusEvent for database insertion."""
        payload = StatusEvent.model_validate(event_dict).model_dump(
            mode="python",
            exclude_none=True,
        )
        payload["event_metadata"] = payload.pop("metadata", {})
        return payload


def init_db(db_path: str):
    """Initialize database at path."""
    store = Store(db_path)
    store.init_db()
    return store
