"""Analyze schema evolution strategy for execution-plan tasks involving data model changes."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for schema evolution concepts
_FIELD_CHANGES_RE = re.compile(
    r"\b(?:add(?:ing)?\s+(?:column|field|nullable|tenant_id)|remove\s+(?:column|field)|drop\s+(?:column|field)|"
    r"(?:column|field)\s+(?:addition|removal|deletion)|"
    r"new\s+(?:column|field)|delete\s+(?:column|field)|"
    r"rename\s+(?:column|field)|modify\s+(?:column|field))\b",
    re.I,
)
_TYPE_CHANGES_RE = re.compile(
    r"\b(?:change\s+(?:type|datatype|data\s+type)|alter\s+(?:type|datatype|column\s+type)|"
    r"type\s+(?:change|modification|migration)|"
    r"(?:string|varchar|text|integer|bigint|boolean|timestamp)\s+to\s+(?:string|varchar|text|integer|bigint|boolean|timestamp)|"
    r"widen\s+type|narrow\s+type|type\s+conversion)\b",
    re.I,
)
_CONSTRAINT_CHANGES_RE = re.compile(
    r"\b(?:add\s+constraint|remove\s+constraint|drop\s+constraint|"
    r"(?:not\s+null|unique|foreign\s+key|primary\s+key|check)\s+constraint|"
    r"constraint\s+(?:addition|removal|modification)|"
    r"add\s+(?:not\s+null|unique|foreign\s+key|primary\s+key|check)|"
    r"remove\s+(?:not\s+null|unique|foreign\s+key|check)|"
    r"change\s+primary\s+key|modify\s+primary\s+key)\b",
    re.I,
)
_INDEX_UPDATES_RE = re.compile(
    r"\b(?:add\s+index|create\s+index|drop\s+index|remove\s+index|"
    r"index\s+(?:addition|creation|removal|deletion|migration)|"
    r"(?:add|create|drop|remove)\s+(?:composite\s+)?index(?:es)?|"
    r"reindex|rebuild\s+index)\b",
    re.I,
)
_BACKWARDS_COMPATIBILITY_RE = re.compile(
    r"\b(?:backwards?\s+compatib(?:le|ility)|backward[- ]compatible|"
    r"maintain\s+compatib(?:le|ility)|preserve\s+compatib(?:le|ility)|"
    r"non[- ]breaking\s+change|breaking\s+change(?:s)?|"
    r"avoid\s+breaking\s+change|prevent\s+breaking\s+change|"
    r"compatib(?:le|ility)\s+(?:mode|layer|approach))\b",
    re.I,
)
_MIGRATION_SCRIPTS_RE = re.compile(
    r"\b(?:migration\s+script|database\s+migration|schema\s+migration|"
    r"(?:up|down)\s+migration|migration\s+(?:file|sql|command)|"
    r"(?:alembic|flyway|liquibase|knex|sequelize|django)\s+migration|"
    r"write\s+migration|create\s+migration|generate\s+migration|"
    r"run\s+migration|execute\s+migration)\b",
    re.I,
)
_DUAL_WRITE_RE = re.compile(
    r"\b(?:dual[- ]write|write\s+to\s+both|parallel\s+write|"
    r"write\s+old\s+and\s+new|simultaneous\s+write|"
    r"transition\s+period|migration\s+period|coexistence\s+period)\b",
    re.I,
)
_ROLLBACK_STRATEGY_RE = re.compile(
    r"\b(?:rollback\s+(?:plan|strategy|procedure|approach)|"
    r"revert\s+(?:migration|change|schema)|"
    r"rollback\s+(?:migration|change|schema)|down\s+migration|"
    r"rollback\s+safety|safe\s+rollback|"
    r"(?:un)?apply\s+(?:migration|change)|"
    r"(?:reverse|undo)\s+migration)\b",
    re.I,
)
_ZERO_DOWNTIME_RE = re.compile(
    r"\b(?:zero[- ]downtime|no[- ]downtime|without\s+downtime|"
    r"downtime[- ]free|online\s+(?:migration|schema\s+change)|"
    r"live\s+(?:migration|schema\s+change)|"
    r"blue[- ]green\s+(?:migration|deployment)|"
    r"rolling\s+(?:migration|deployment|update))\b",
    re.I,
)
_TESTING_COVERAGE_RE = re.compile(
    r"\b(?:test\s+migration|migration\s+test(?:ing)?|"
    r"schema\s+(?:change|migration)\s+test(?:ing)?|"
    r"test\s+(?:backwards?\s+)?compatib(?:le|ility)|"
    r"test\s+rollback|rollback\s+test(?:ing)?|"
    r"migration\s+(?:validation|verification)|"
    r"test\s+data\s+migration|data\s+migration\s+test(?:ing)?)\b",
    re.I,
)
_EXPAND_CONTRACT_RE = re.compile(
    r"\b(?:expand[- ]contract|expand\s+and\s+contract|"
    r"expand\s+phase|contract\s+phase|"
    r"additive\s+change|subtractive\s+change|"
    r"three[- ]phase\s+migration|multi[- ]phase\s+migration)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SchemaEvolutionStrategyAnalysis:
    """Schema evolution strategy analysis for a change brief."""

    field_changes_identified: bool = False
    type_changes_identified: bool = False
    constraint_changes_identified: bool = False
    index_updates_planned: bool = False
    backwards_compatibility_considered: bool = False
    migration_scripts_planned: bool = False
    dual_write_strategy_defined: bool = False
    rollback_strategy_defined: bool = False
    zero_downtime_approach: bool = False
    testing_coverage_planned: bool = False
    expand_contract_pattern: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "field_changes_identified": self.field_changes_identified,
            "type_changes_identified": self.type_changes_identified,
            "constraint_changes_identified": self.constraint_changes_identified,
            "index_updates_planned": self.index_updates_planned,
            "backwards_compatibility_considered": self.backwards_compatibility_considered,
            "migration_scripts_planned": self.migration_scripts_planned,
            "dual_write_strategy_defined": self.dual_write_strategy_defined,
            "rollback_strategy_defined": self.rollback_strategy_defined,
            "zero_downtime_approach": self.zero_downtime_approach,
            "testing_coverage_planned": self.testing_coverage_planned,
            "expand_contract_pattern": self.expand_contract_pattern,
        }

    @property
    def readiness_score(self) -> float:
        """
        Calculate readiness score based on migration safety, compatibility, and rollback plan.

        Returns:
            Float between 0.0 and 1.0 representing overall schema evolution readiness.
        """
        # Core migration elements (30% weight)
        core_score = sum([
            self.migration_scripts_planned,
            self.field_changes_identified or self.type_changes_identified or self.constraint_changes_identified,
        ]) / 2.0 * 0.3

        # Safety and compatibility (40% weight)
        safety_score = sum([
            self.backwards_compatibility_considered,
            self.rollback_strategy_defined,
            self.testing_coverage_planned,
            self.zero_downtime_approach,
        ]) / 4.0 * 0.4

        # Advanced patterns (30% weight)
        advanced_score = sum([
            self.dual_write_strategy_defined,
            self.expand_contract_pattern,
            self.index_updates_planned,
        ]) / 3.0 * 0.3

        return core_score + safety_score + advanced_score

    @property
    def recommendations(self) -> list[str]:
        """
        Generate recommendations for safe schema evolution.

        Returns:
            List of actionable recommendations for improving schema evolution safety.
        """
        recs = []

        if not self.migration_scripts_planned:
            recs.append("Define migration scripts with clear up/down paths")

        if not self.backwards_compatibility_considered:
            recs.append("Assess backwards compatibility impact on existing code and clients")

        if not self.rollback_strategy_defined:
            recs.append("Document rollback procedure for reverting schema changes")

        if not self.testing_coverage_planned:
            recs.append("Add tests for schema migrations including rollback scenarios")

        if not self.dual_write_strategy_defined and (
            self.type_changes_identified or self.field_changes_identified
        ):
            recs.append("Consider dual-write period for transitioning between old and new schema")

        if not self.expand_contract_pattern and self.constraint_changes_identified:
            recs.append("Use expand-contract pattern for constraint modifications")

        if not self.zero_downtime_approach and (
            self.type_changes_identified or self.constraint_changes_identified
        ):
            recs.append("Plan zero-downtime migration strategy using phased rollout")

        if not self.index_updates_planned and (
            self.field_changes_identified or self.type_changes_identified
        ):
            recs.append("Review and update indexes affected by schema changes")

        return recs


def analyze_schema_evolution_strategy(change_brief: Mapping[str, Any]) -> SchemaEvolutionStrategyAnalysis:
    """
    Analyze schema evolution strategy from a change brief.

    Args:
        change_brief: A mapping containing change information with fields like
                     'title', 'description', 'acceptance_criteria', etc.

    Returns:
        SchemaEvolutionStrategyAnalysis with boolean flags and readiness metrics.
    """
    if not isinstance(change_brief, Mapping):
        return SchemaEvolutionStrategyAnalysis()

    searchable_text = _extract_searchable_text(change_brief)

    return SchemaEvolutionStrategyAnalysis(
        field_changes_identified=bool(_FIELD_CHANGES_RE.search(searchable_text)),
        type_changes_identified=bool(_TYPE_CHANGES_RE.search(searchable_text)),
        constraint_changes_identified=bool(_CONSTRAINT_CHANGES_RE.search(searchable_text)),
        index_updates_planned=bool(_INDEX_UPDATES_RE.search(searchable_text)),
        backwards_compatibility_considered=bool(_BACKWARDS_COMPATIBILITY_RE.search(searchable_text)),
        migration_scripts_planned=bool(_MIGRATION_SCRIPTS_RE.search(searchable_text)),
        dual_write_strategy_defined=bool(_DUAL_WRITE_RE.search(searchable_text)),
        rollback_strategy_defined=bool(_ROLLBACK_STRATEGY_RE.search(searchable_text)),
        zero_downtime_approach=bool(_ZERO_DOWNTIME_RE.search(searchable_text)),
        testing_coverage_planned=bool(_TESTING_COVERAGE_RE.search(searchable_text)),
        expand_contract_pattern=bool(_EXPAND_CONTRACT_RE.search(searchable_text)),
    )


def _extract_searchable_text(payload: Mapping[str, Any]) -> str:
    """Extract and normalize text from common change brief fields."""
    field_names = (
        "title",
        "description",
        "summary",
        "body",
        "acceptance_criteria",
        "acceptance",
        "requirements",
        "constraints",
        "approach",
        "implementation",
        "notes",
        "risks",
        "testing_strategy",
        "rollback_plan",
    )
    parts: list[str] = []
    for field_name in field_names:
        value = payload.get(field_name)
        if value is not None:
            parts.extend(_strings(value))
    return _SPACE_RE.sub(" ", " ".join(parts))


def _strings(value: Any) -> list[str]:
    """Extract strings from various data structures."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=str):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        strings = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    return [str(value)]


__all__ = [
    "SchemaEvolutionStrategyAnalysis",
    "analyze_schema_evolution_strategy",
]
