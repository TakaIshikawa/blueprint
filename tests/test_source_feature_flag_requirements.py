import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_feature_flag_requirements import (
    SourceFeatureFlagRequirement,
    SourceFeatureFlagRequirementsReport,
    build_source_feature_flag_requirements,
    derive_source_feature_flag_requirements,
    extract_source_feature_flag_requirements,
    generate_source_feature_flag_requirements,
    recommend_source_feature_flag_requirements,
    source_feature_flag_requirements_to_dict,
    source_feature_flag_requirements_to_dicts,
    source_feature_flag_requirements_to_markdown,
    summarize_source_feature_flag_requirements,
)


def test_extracts_feature_flag_categories_from_unstructured_prose_with_evidence():
    result = build_source_feature_flag_requirements(
        _source_brief(
            summary=(
                "Checkout rollout gate must support a 10% canary and beta cohort targeting. "
                "A kill switch must let support turn off the checkout feature and roll back safely. "
                "The checkout experiment needs A/B test variants with a control group. "
                "A feature flag should use remote config defaults and be removed after launch."
            ),
            source_payload={
                "requirements": [
                    "Checkout permission gate requires admin-only entitlements.",
                    "Release owner approval is required before launch.",
                ]
            },
        )
    )

    assert isinstance(result, SourceFeatureFlagRequirementsReport)
    assert all(isinstance(record, SourceFeatureFlagRequirement) for record in result.records)
    assert [record.requirement_category for record in result.records] == [
        "rollout_gate",
        "cohort_targeting",
        "kill_switch",
        "experiment_toggle",
        "config_flag",
        "permission_gate",
        "cleanup_policy",
        "owner_approval",
    ]
    by_category = {record.requirement_category: record for record in result.records}
    assert by_category["rollout_gate"].feature_area == "checkout feature"
    assert by_category["kill_switch"].confidence == "high"
    assert "missing_disable_or_rollback_behavior" not in by_category["kill_switch"].missing_detail_flags
    assert "missing_cleanup_plan" not in by_category["config_flag"].missing_detail_flags
    assert "a/b test" in by_category["experiment_toggle"].matched_terms
    assert any("summary: Checkout rollout gate must support" in item for item in by_category["rollout_gate"].evidence)
    assert any("source_payload.requirements[0]" in item for item in by_category["permission_gate"].evidence)
    assert result.summary["requirement_count"] == 8
    assert result.summary["requirement_category_counts"]["owner_approval"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_scans_nested_metadata_without_mutation_and_merges_duplicate_candidates():
    source = _source_brief(
        source_payload={
            "release_controls": {
                "feature_flags": {
                    "checkout": [
                        "Checkout feature flag must target beta users and internal users.",
                        "Checkout feature flag should target beta users with product owner approval.",
                        "Cleanup: remove the checkout feature flag after migration complete.",
                    ],
                    "safety": {
                        "disable": "Checkout kill switch must stop rollout and turn off the checkout feature.",
                    },
                }
            }
        },
    )
    original = copy.deepcopy(source)

    result = build_source_feature_flag_requirements(source)
    by_category = {record.requirement_category: record for record in result.records}

    assert source == original
    assert [record.requirement_category for record in result.records] == [
        "cohort_targeting",
        "kill_switch",
        "config_flag",
        "cleanup_policy",
        "owner_approval",
    ]
    cohort = by_category["cohort_targeting"]
    assert cohort.feature_area == "checkout feature"
    assert cohort.confidence == "high"
    assert cohort.evidence == tuple(sorted(cohort.evidence, key=str.casefold))
    assert len(cohort.evidence) == 2
    assert any(
        "source_payload.release_controls.feature_flags.checkout[0]" in item
        for item in cohort.evidence
    )
    assert any(
        "source_payload.release_controls.feature_flags.checkout[1]" in item
        for item in cohort.evidence
    )
    assert cohort.source_field_paths == tuple(sorted(cohort.source_field_paths, key=str.casefold))
    assert "missing_rollout_scope" not in cohort.missing_detail_flags
    assert "missing_flag_owner" not in by_category["owner_approval"].missing_detail_flags
    assert result.summary["feature_areas"] == ["checkout feature"]


def test_extracts_from_implementation_brief_plain_text_and_generic_object():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Reporting rollout gate must allow a phased rollout by tenant.",
                "Definition of done: permission gate requires the analyst role entitlement.",
            ],
        )
    )
    text_result = extract_source_feature_flag_requirements(
        "Search experiment should run as an A/B test with variant and control cohorts."
    )
    object_result = build_source_feature_flag_requirements(
        BriefLike(
            id="object-flag",
            summary="Settings feature toggle requires owner approval and cleanup after launch.",
        )
    )

    model_result = generate_source_feature_flag_requirements(brief)

    assert model_result.source_id == "impl-flags"
    assert [record.requirement_category for record in model_result.records] == [
        "rollout_gate",
        "cohort_targeting",
        "permission_gate",
    ]
    assert model_result.records[0].feature_area == "reporting feature"
    assert text_result.source_id is None
    assert [record.requirement_category for record in text_result.records] == [
        "cohort_targeting",
        "experiment_toggle",
    ]
    assert object_result.source_id == "object-flag"
    assert [record.requirement_category for record in object_result.records] == [
        "config_flag",
        "cleanup_policy",
        "owner_approval",
    ]


def test_aliases_serialization_markdown_and_summary_are_stable():
    source = _source_brief(
        source_id="flags-model",
        summary=(
            "Admin rollout gate must be enabled by feature flag for staff only, "
            "with release owner approval and cleanup after launch."
        ),
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_feature_flag_requirements(source)
    generated = generate_source_feature_flag_requirements(model)
    derived = derive_source_feature_flag_requirements(model)
    extracted = extract_source_feature_flag_requirements(model)
    recommended = recommend_source_feature_flag_requirements(model)
    payload = source_feature_flag_requirements_to_dict(generated)
    markdown = source_feature_flag_requirements_to_markdown(generated)

    assert source == original
    assert mapping_result.to_dict() == generated.to_dict()
    assert derived.to_dict() == generated.to_dict()
    assert extracted.to_dict() == generated.to_dict()
    assert recommended.to_dict() == generated.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert generated.records == generated.requirements
    assert generated.findings == generated.requirements
    assert source_feature_flag_requirements_to_dicts(generated) == payload["requirements"]
    assert source_feature_flag_requirements_to_dicts(generated.records) == payload["records"]
    assert summarize_source_feature_flag_requirements(generated) == generated.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "feature_area",
        "requirement_category",
        "missing_detail_flags",
        "evidence",
        "source_field_paths",
        "matched_terms",
        "confidence",
        "planning_note",
    ]
    assert markdown == generated.to_markdown()
    assert markdown.startswith("# Source Feature Flag Requirements Report: flags-model")
    assert "| Source Brief | Feature Area | Category | Confidence | Missing Details |" in markdown


def test_empty_invalid_and_negated_inputs_return_stable_empty_reports():
    result = build_source_feature_flag_requirements(
        _source_brief(
            title="Profile settings",
            summary="No feature flag changes are required for this copy update.",
            source_payload={"requirements": ["Keep form submission behavior unchanged."]},
        )
    )
    malformed = build_source_feature_flag_requirements({"source_payload": {"notes": object()}})
    blank_text = build_source_feature_flag_requirements("")
    unrelated = build_source_feature_flag_requirements(
        _source_brief(
            title="Release notes",
            summary="Publish support documentation after deploy.",
        )
    )

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "requirement_categories": [],
        "requirement_category_counts": {
            "rollout_gate": 0,
            "cohort_targeting": 0,
            "kill_switch": 0,
            "experiment_toggle": 0,
            "config_flag": 0,
            "permission_gate": 0,
            "cleanup_policy": 0,
            "owner_approval": 0,
        },
        "missing_detail_counts": {
            "missing_flag_owner": 0,
            "missing_rollout_scope": 0,
            "missing_disable_or_rollback_behavior": 0,
            "missing_cleanup_plan": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "feature_areas": [],
        "status": "no_feature_flag_language",
    }
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == expected_summary
    assert malformed.summary == expected_summary
    assert blank_text.summary == expected_summary
    assert unrelated.summary == expected_summary
    assert "No source feature flag requirements were inferred" in result.to_markdown()


class BriefLike:
    def __init__(self, *, id, summary):
        self.id = id
        self.summary = summary


def _source_brief(
    *,
    source_id="flags-source",
    title="Feature flag requirements",
    domain="release",
    summary="General feature flag requirements.",
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


def _implementation_brief(
    *,
    source_id="impl-flags",
    title="Reporting rollout",
    summary="Feature flag implementation requirements.",
    scope=None,
):
    return {
        "id": source_id,
        "source_brief_id": source_id,
        "title": title,
        "domain": "analytics",
        "target_user": None,
        "buyer": None,
        "workflow_context": None,
        "problem_statement": summary,
        "mvp_goal": "Ship reporting safely.",
        "product_surface": None,
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Validate feature flag behavior.",
        "definition_of_done": [],
    }
