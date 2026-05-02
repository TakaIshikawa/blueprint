import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_migration_cutover_requirements import (
    SourceMigrationCutoverRequirement,
    SourceMigrationCutoverRequirementsReport,
    build_source_migration_cutover_requirements,
    derive_source_migration_cutover_requirements,
    extract_source_migration_cutover_requirements,
    generate_source_migration_cutover_requirements,
    source_migration_cutover_requirements_to_dict,
    source_migration_cutover_requirements_to_dicts,
    source_migration_cutover_requirements_to_markdown,
    summarize_source_migration_cutover_requirements,
)


def test_explicit_migration_plan_extracts_all_cutover_requirement_categories():
    result = build_source_migration_cutover_requirements(
        _source_brief(
            source_payload={
                "migration_plan": {
                    "migration": "Data migration must copy legacy account data into the new store.",
                    "backfill": "Backfill historical invoices for 90 days before launch.",
                    "cutover": "Cutover should happen during the phase 2 go-live window.",
                    "dual_write": "Dual-write must write to both old and new stores until validation passes.",
                    "import_export": "CSV import and data export are required for migration files.",
                    "downtime": "The maintenance window allows 30 minutes of downtime.",
                    "rollback": "Rollback plan must restore previous routing if the cutover fails.",
                    "reconciliation": "Reconciliation requires row counts and checksum parity after migration.",
                }
            }
        )
    )

    assert isinstance(result, SourceMigrationCutoverRequirementsReport)
    assert result.source_id == "source-migration"
    assert all(isinstance(record, SourceMigrationCutoverRequirement) for record in result.records)
    assert [record.concern_category for record in result.records] == [
        "data_migration",
        "backfill",
        "cutover",
        "dual_write",
        "import_export",
        "downtime",
        "rollback",
        "reconciliation",
    ]
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"] == {
        "data_migration": 1,
        "backfill": 1,
        "cutover": 1,
        "dual_write": 1,
        "import_export": 1,
        "downtime": 1,
        "rollback": 1,
        "reconciliation": 1,
    }
    assert result.summary["confidence_counts"] == {"high": 8, "medium": 0, "low": 0}
    assert any(
        "source_payload.migration_plan.reconciliation" in evidence
        for record in result.records
        for evidence in record.evidence
    )
    by_category = {record.concern_category: record for record in result.records}
    assert by_category["backfill"].value == "90 days"
    assert by_category["downtime"].value == "30 minutes"


def test_implicit_cutover_constraints_are_detected_from_brief_models_and_objects():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "The go-live must include a read-only window while traffic shifts to the new database.",
                "Mirror writes should continue until data parity is verified.",
            ],
            definition_of_done=[
                "Legacy data is copied into the target system before launch.",
            ],
        )
    )
    text = build_source_migration_cutover_requirements(
        """
# Release Constraints

- Switchover requires a maintenance window.
- Bulk export must remain available for migration files.
"""
    )
    object_result = build_source_migration_cutover_requirements(
        SimpleNamespace(
            id="object-cutover",
            summary="A catch-up job backfills late-arriving orders.",
            metadata={"rollback": "Fallback plan should route users back to the old service."},
        )
    )

    model_records = extract_source_migration_cutover_requirements(brief)

    assert {record.concern_category for record in model_records} == {
        "data_migration",
        "cutover",
        "dual_write",
        "downtime",
        "reconciliation",
    }
    assert [record.concern_category for record in text.records] == [
        "cutover",
        "import_export",
        "downtime",
    ]
    assert [record.concern_category for record in object_result.records] == [
        "backfill",
        "rollback",
    ]


def test_duplicate_category_matches_collapse_predictably_with_strongest_evidence():
    result = build_source_migration_cutover_requirements(
        {
            "id": "dupe-migration",
            "source_payload": {
                "cutover": {
                    "window": "Cutover must happen during a 2 hour maintenance window.",
                    "same_window": "Cutover must happen during a 2 hour maintenance window.",
                    "soft_window": "Cutover is part of the migration rollout.",
                },
                "acceptance_criteria": [
                    "Cutover must happen during a 2 hour maintenance window.",
                    "Rollback plan must restore previous routing.",
                ],
            },
        }
    )

    assert [record.concern_category for record in result.records] == [
        "cutover",
        "downtime",
        "rollback",
    ]
    cutover = result.records[0]
    assert cutover.evidence == (
        "source_payload.acceptance_criteria[0]: Cutover must happen during a 2 hour maintenance window.",
        "source_payload.cutover.soft_window: Cutover is part of the migration rollout.",
    )
    assert cutover.value == "2 hour"
    assert cutover.confidence == "high"
    assert result.summary["concern_categories"] == ["cutover", "downtime", "rollback"]


def test_rollback_reconciliation_serialization_markdown_aliases_and_no_mutation():
    source = _source_brief(
        source_id="migration-model",
        summary="Migration plan must include rollback and reconciliation requirements.",
        source_payload={
            "requirements": [
                "Rollback must restore the previous service if migration fails.",
                "Reconciliation requires data parity checks and diff reports.",
                "Dual-write notes must escape old | new store labels.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_migration_cutover_requirements(source)
    model_result = generate_source_migration_cutover_requirements(model)
    derived = derive_source_migration_cutover_requirements(model)
    payload = source_migration_cutover_requirements_to_dict(model_result)
    markdown = source_migration_cutover_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_migration_cutover_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_migration_cutover_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_migration_cutover_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_migration_cutover_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "concern_category",
        "value",
        "evidence",
        "confidence",
        "source_id",
    ]
    assert [(record.source_id, record.concern_category) for record in model_result.records] == [
        ("migration-model", "dual_write"),
        ("migration-model", "rollback"),
        ("migration-model", "reconciliation"),
    ]
    assert markdown == model_result.to_markdown()
    assert "| Category | Value | Confidence | Source | Evidence |" in markdown
    assert "old \\| new store labels" in markdown


def test_no_match_negated_invalid_and_malformed_inputs_return_stable_empty_reports():
    class BriefLike:
        id = "object-empty"
        summary = "No migration, cutover, rollback, or downtime work is required for this copy update."

    empty = build_source_migration_cutover_requirements(
        _source_brief(source_id="empty-migration", summary="Update onboarding copy only.")
    )
    repeat = build_source_migration_cutover_requirements(
        _source_brief(source_id="empty-migration", summary="Update onboarding copy only.")
    )
    negated = build_source_migration_cutover_requirements(BriefLike())
    malformed = build_source_migration_cutover_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_migration_cutover_requirements(42)

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "data_migration": 0,
            "backfill": 0,
            "cutover": 0,
            "dual_write": 0,
            "import_export": 0,
            "downtime": 0,
            "rollback": 0,
            "reconciliation": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "concern_categories": [],
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-migration"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No migration cutover requirements were found" in empty.to_markdown()
    assert negated.records == ()
    assert malformed.records == ()
    assert invalid.records == ()


def _source_brief(
    *,
    source_id="source-migration",
    title="Migration cutover requirements",
    domain="data",
    summary="General migration cutover requirements.",
    source_payload=None,
    source_links=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "impl-migration",
        "source_brief_id": "source-migration",
        "title": "Migration cutover handling",
        "domain": "data",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Migration cutover and rollback planning.",
        "problem_statement": "Operators need predictable data migration sequencing.",
        "mvp_goal": "Ship migration cutover constraints.",
        "product_surface": "admin migration tooling",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run migration cutover validation.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
