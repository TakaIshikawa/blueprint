from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_impact_tags import (
    SourceImpactTag,
    derive_source_impact_tags,
    source_impact_tags_to_dicts,
)


def test_source_brief_input_derives_known_tags_with_evidence():
    tags = derive_source_impact_tags(
        SourceBrief(
            id="sb-impact",
            title="Customer Portal",
            summary="Build a frontend dashboard that calls an API endpoint for customer data.",
            source_project="manual",
            source_entity_type="note",
            source_id="note-1",
            source_payload={
                "details": "Add login token handling and metrics for slow API responses.",
                "nested": {"notes": "Update the README with rollout documentation."},
            },
            source_links={},
        )
    )

    by_tag = {tag.tag: tag for tag in tags}

    assert {"api", "auth", "data", "docs", "observability", "ui"} <= set(by_tag)
    assert all(isinstance(tag, SourceImpactTag) for tag in tags)
    assert any("frontend dashboard" in evidence for evidence in by_tag["ui"].evidence)
    assert any("login token" in evidence for evidence in by_tag["auth"].evidence)
    assert by_tag["api"].confidence > by_tag["docs"].confidence


def test_implementation_brief_input_uses_scope_integrations_and_risks():
    tags = derive_source_impact_tags(
        ImplementationBrief(
            id="ib-impact",
            source_brief_id="sb-impact",
            title="Billing Sync",
            problem_statement="Expose a REST API route for billing account updates.",
            mvp_goal="Synchronize billing state.",
            scope=[
                "Create database tables for billing sync state.",
                "Add pytest regression coverage.",
            ],
            non_goals=[],
            assumptions=[],
            integration_points=["Stripe provider webhook integration"],
            risks=["Migration rollback and alert coverage need review."],
            validation_plan="Run integration validation.",
            definition_of_done=["Billing sync is complete."],
        )
    )

    by_tag = {tag.tag: tag for tag in tags}

    assert {"api", "data", "integration", "migration", "observability", "testing"} <= set(by_tag)
    assert any(
        "Stripe provider webhook integration" in item for item in by_tag["integration"].evidence
    )
    assert any("Migration rollback" in item for item in by_tag["migration"].evidence)


def test_dict_payload_collects_mixed_evidence_from_expected_fields_only():
    tags = derive_source_impact_tags(
        {
            "summary": "Add an accessible UI view for reviewing import errors.",
            "problem_statement": "The API client needs better logging around provider failures.",
            "scope": ["Document the recovery runbook.", "Add validation tests."],
            "integration_points": ["External CRM connector"],
            "risks": ["Database backfill may require a migration."],
            "source_payload": {
                "description": "Deploy with Docker and CI updates.",
                "ignored_number": 10,
            },
            "title": "Title mentions secrets but should not be scanned.",
        }
    )

    by_tag = {tag.tag: tag for tag in tags}

    assert {"api", "data", "docs", "infra", "integration", "migration", "testing", "ui"} <= set(
        by_tag
    )
    assert "auth" not in by_tag
    assert any("Deploy with Docker" in item for item in by_tag["infra"].evidence)


def test_empty_optional_fields_and_unrelated_content_return_empty_list():
    assert derive_source_impact_tags({}) == []
    assert (
        derive_source_impact_tags(
            {
                "summary": "",
                "problem_statement": None,
                "scope": [],
                "integration_points": None,
                "risks": [],
                "source_payload": {"note": "Coordinate the next planning conversation."},
            }
        )
        == []
    )


def test_results_are_sorted_by_confidence_then_tag_name_and_serialize():
    tags = derive_source_impact_tags(
        {
            "summary": "API endpoint and REST route. Data query. UI screen.",
            "scope": ["API schema update.", "Data cache update.", "UI component update."],
        }
    )

    assert [tag.tag for tag in tags[:3]] == ["api", "data", "ui"]
    assert [(-tag.confidence, tag.tag) for tag in tags] == sorted(
        (-tag.confidence, tag.tag) for tag in tags
    )
    assert source_impact_tags_to_dicts(tags[:1]) == [
        {
            "tag": tags[0].tag,
            "confidence": tags[0].confidence,
            "evidence": list(tags[0].evidence),
        }
    ]
