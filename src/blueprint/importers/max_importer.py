"""Max design brief importer."""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

from blueprint.importers.base import SourceImporter


def generate_source_brief_id() -> str:
    """Generate a unique source brief ID."""
    return f"sb-{uuid.uuid4().hex[:12]}"


class MaxImporter(SourceImporter):
    """Import design briefs from Max database."""

    def __init__(self, max_db_path: str):
        """
        Initialize Max importer.

        Args:
            max_db_path: Path to Max SQLite database
        """
        self.max_db_path = max_db_path
        if not Path(max_db_path).exists():
            raise FileNotFoundError(f"Max database not found at {max_db_path}")

        self.engine = create_engine(f"sqlite:///{max_db_path}")

    def validate_source(self, source_id: str) -> bool:
        """Check if a Max design brief exists."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT COUNT(*) FROM design_briefs WHERE id = :id"),
                {"id": source_id},
            )
            return result.scalar() > 0

    def list_available(self, limit: int = 50, status: str | None = None) -> list[dict[str, Any]]:
        """
        List available Max design briefs.

        Args:
            limit: Maximum number of briefs to return
            status: Optional status filter (candidate, active, completed, shelved)

        Returns:
            List of brief summaries with id, title, status, readiness_score
        """
        query = """
            SELECT id, title, domain, theme, design_status, readiness_score, created_at
            FROM design_briefs
        """

        if status:
            query += " WHERE design_status = :status"

        query += " ORDER BY readiness_score DESC, created_at DESC LIMIT :limit"

        with self.engine.connect() as conn:
            if status:
                result = conn.execute(text(query), {"status": status, "limit": limit})
            else:
                result = conn.execute(text(query), {"limit": limit})

            briefs = []
            for row in result:
                briefs.append({
                    "id": row[0],
                    "title": row[1],
                    "domain": row[2],
                    "theme": row[3],
                    "status": row[4],
                    "readiness_score": row[5],
                    "created_at": row[6],
                })

            return briefs

    def import_from_source(self, source_id: str) -> dict[str, Any]:
        """
        Import a Max design brief and convert to SourceBrief format.

        Args:
            source_id: Max design brief ID (dbf-xxx)

        Returns:
            Dictionary ready for insertion into source_briefs table

        Raises:
            ValueError: If design brief not found
        """
        # Fetch the design brief
        brief = self._fetch_design_brief(source_id)
        if not brief:
            raise ValueError(f"Design brief not found in Max: {source_id}")

        # Fetch source ideas (buildable units that back this brief)
        source_ideas = self._fetch_source_ideas(source_id)

        # Create SourceBrief structure
        source_brief = {
            "id": generate_source_brief_id(),
            "title": brief["title"],
            "domain": brief["domain"],
            "summary": self._create_summary(brief, source_ideas),
            "source_project": "max",
            "source_entity_type": "design_brief",
            "source_id": source_id,
            "source_payload": {
                "design_brief": brief,
                "source_ideas": source_ideas,
            },
            "source_links": {
                "max_db": self.max_db_path,
                "brief_id": source_id,
            },
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }

        return source_brief

    def _fetch_design_brief(self, brief_id: str) -> dict[str, Any] | None:
        """Fetch design brief from Max database."""
        query = text("""
            SELECT
                id, title, domain, theme, readiness_score, lead_idea_id,
                merged_product_concept, synthesis_rationale, why_this_now,
                mvp_scope, first_milestones, validation_plan, risks,
                buyer, specific_user, workflow_context,
                source_idea_ids, design_status, created_at, updated_at
            FROM design_briefs
            WHERE id = :id
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"id": brief_id})
            row = result.fetchone()

            if not row:
                return None

            import json

            return {
                "id": row[0],
                "title": row[1],
                "domain": row[2],
                "theme": row[3],
                "readiness_score": row[4],
                "lead_idea_id": row[5],
                "merged_product_concept": row[6],
                "synthesis_rationale": row[7],
                "why_this_now": row[8],
                "mvp_scope": json.loads(row[9]) if row[9] else [],
                "first_milestones": json.loads(row[10]) if row[10] else [],
                "validation_plan": row[11],
                "risks": json.loads(row[12]) if row[12] else [],
                "buyer": row[13],
                "specific_user": row[14],
                "workflow_context": row[15],
                "source_idea_ids": json.loads(row[16]) if row[16] else [],
                "design_status": row[17],
                "created_at": row[18],
                "updated_at": row[19],
            }

    def _fetch_source_ideas(self, brief_id: str) -> list[dict[str, Any]]:
        """Fetch source buildable units for a design brief."""
        query = text("""
            SELECT
                dbs.idea_id, dbs.role, dbs.rank,
                bu.title, bu.one_liner, bu.status, bu.domain, bu.category,
                bu.problem, bu.solution, bu.value_proposition,
                bu.buyer, bu.specific_user, bu.workflow_context,
                bu.quality_score, bu.novelty_score, bu.usefulness_score,
                bu.tech_approach, bu.suggested_stack, bu.domain_risks
            FROM design_brief_sources dbs
            JOIN buildable_units bu ON dbs.idea_id = bu.id
            WHERE dbs.brief_id = :brief_id
            ORDER BY dbs.role, dbs.rank
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"brief_id": brief_id})

            import json
            source_ideas = []

            for row in result:
                source_ideas.append({
                    "id": row[0],
                    "role": row[1],  # lead | supporting | source
                    "rank": row[2],
                    "title": row[3],
                    "one_liner": row[4],
                    "status": row[5],
                    "domain": row[6],
                    "category": row[7],
                    "problem": row[8],
                    "solution": row[9],
                    "value_proposition": row[10],
                    "buyer": row[11],
                    "specific_user": row[12],
                    "workflow_context": row[13],
                    "quality_score": row[14],
                    "novelty_score": row[15],
                    "usefulness_score": row[16],
                    "tech_approach": row[17],
                    "suggested_stack": json.loads(row[18]) if row[18] else None,
                    "domain_risks": json.loads(row[19]) if row[19] else [],
                })

            return source_ideas

    def _create_summary(self, brief: dict[str, Any], source_ideas: list[dict[str, Any]]) -> str:
        """Create a concise summary from the design brief."""
        summary_parts = []

        # Start with the merged product concept
        if brief.get("merged_product_concept"):
            summary_parts.append(brief["merged_product_concept"])

        # Add why this now
        if brief.get("why_this_now"):
            summary_parts.append(f"\nMarket timing: {brief['why_this_now']}")

        # Add readiness score
        if brief.get("readiness_score"):
            summary_parts.append(f"\nReadiness: {brief['readiness_score']:.1f}/100")

        # Add lead idea one-liner if available
        lead_ideas = [idea for idea in source_ideas if idea["role"] == "lead"]
        if lead_ideas:
            lead = lead_ideas[0]
            summary_parts.append(f"\nLead idea: {lead['one_liner']}")

        return "\n".join(summary_parts)
