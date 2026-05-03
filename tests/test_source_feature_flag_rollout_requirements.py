import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_feature_flag_rollout_requirements import (
    SourceFeatureFlagRolloutEvidenceGap,
    SourceFeatureFlagRolloutRequirement,
    SourceFeatureFlagRolloutRequirementsReport,
    build_source_feature_flag_rollout_requirements,
    derive_source_feature_flag_rollout_requirements,
    extract_source_feature_flag_rollout_requirements,
    generate_source_feature_flag_rollout_requirements,
    source_feature_flag_rollout_requirements_to_dict,
    source_feature_flag_rollout_requirements_to_dicts,
    source_feature_flag_rollout_requirements_to_markdown,
    summarize_source_feature_flag_rollout_requirements,
)


def test_structured_feature_flag_payload_extracts_categories_in_order():
    result = build_source_feature_flag_rollout_requirements(
        _source_brief(
            source_payload={
                "feature_flags": {
                    "ownership": "Flag ownership must assign a product owner and engineering owner for approval.",
                    "targeting": "Audience targeting requires beta user cohort, tenant segment, and allowlist exclusions.",
                    "percentages": "Staged rollout percentages should ramp 5%, 25%, 50%, and 100% with hold periods.",
                    "kill_switch": "Kill switch behavior must support emergency off and global disable.",
                    "variants": "Experiment variant tracking must record control group, treatment variant, and exposure events.",
                    "observability": "Observability requires dashboards, metrics, alerts, logs, and health checks.",
                    "rollback": "Rollback criteria require error threshold and conversion drop triggers before revert.",
                    "cleanup": "Cleanup and deprecation must remove stale flags and delete dead code paths.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceFeatureFlagRolloutRequirementsReport)
    assert all(isinstance(record, SourceFeatureFlagRolloutRequirement) for record in result.records)
    assert result.gaps == ()
    assert [record.category for record in result.records] == [
        "flag_ownership",
        "audience_targeting",
        "staged_rollout_percentages",
        "kill_switch_behavior",
        "experiment_variant_tracking",
        "observability",
        "rollback_criteria",
        "cleanup_deprecation",
    ]
    assert by_category["flag_ownership"].value == "product owner, engineering owner, approval"
    assert by_category["audience_targeting"].value == "Audience, targeting, beta user"
    assert by_category["staged_rollout_percentages"].value == "5%, 25%, 50%"
    assert by_category["kill_switch_behavior"].source_field == "source_payload.feature_flags.kill_switch"
    assert by_category["observability"].suggested_owners == ("sre", "analytics")
    assert by_category["rollback_criteria"].planning_notes[0].startswith("Document rollback triggers")
    assert result.summary["requirement_count"] == 8
    assert result.summary["evidence_gap_count"] == 0
    assert result.summary["status"] == "ready_for_planning"


def test_partial_rollout_brief_flags_missing_owner_and_rollback_criteria():
    result = build_source_feature_flag_rollout_requirements(
        _source_brief(
            summary=(
                "Checkout feature flag rollout must target beta tenants. "
                "Staged rollout should ramp to 10% then 50%, with a kill switch and dashboard monitoring."
            ),
        )
    )

    assert [record.category for record in result.records] == [
        "audience_targeting",
        "staged_rollout_percentages",
        "kill_switch_behavior",
        "observability",
    ]
    assert [gap.category for gap in result.evidence_gaps] == [
        "missing_flag_owner",
        "missing_rollback_criteria",
    ]
    assert all(isinstance(gap, SourceFeatureFlagRolloutEvidenceGap) for gap in result.evidence_gaps)
    assert result.summary["status"] == "needs_feature_flag_rollout_detail"
    assert result.summary["evidence_gap_count"] == 2


def test_implementation_brief_plain_text_and_object_inputs_are_supported_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Feature flag ownership requires product owner approval and engineering owner accountability.",
            "Audience targeting should segment beta tenants and internal users.",
        ],
        definition_of_done=[
            "Staged rollout ramps 5% to 25% with metrics and alerts on error rate.",
            "Rollback criteria define latency threshold triggers and cleanup removes stale flags.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    text_result = build_source_feature_flag_rollout_requirements(
        """
# Feature flag rollout

- Kill switch behavior must support emergency off for the release flag.
- Experiment variant tracking records exposure events and treatment assignment.
"""
    )
    object_result = build_source_feature_flag_rollout_requirements(
        SimpleNamespace(
            id="object-feature-flag",
            rollout="Feature flag staged rollout must target allowlist cohort at 10% and monitor dashboards.",
        )
    )
    implementation_result = generate_source_feature_flag_rollout_requirements(implementation)

    assert implementation_payload == original
    assert [record.category for record in text_result.records] == [
        "kill_switch_behavior",
        "experiment_variant_tracking",
    ]
    assert [record.category for record in object_result.records] == [
        "audience_targeting",
        "staged_rollout_percentages",
        "observability",
    ]
    assert {
        "flag_ownership",
        "audience_targeting",
        "staged_rollout_percentages",
        "observability",
        "rollback_criteria",
        "cleanup_deprecation",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.source_id == "implementation-feature-flags"
    assert implementation_result.title == "Feature flag rollout implementation"


def test_sourcebrief_serialization_markdown_aliases_and_dict_helpers_are_stable():
    source = _source_brief(
        source_id="feature-flag-model",
        summary="Feature flag rollout requires owner approval and rollback criteria.",
        source_payload={
            "requirements": [
                "Feature flag owner must be product | engineering.",
                "Feature flag owner must be product | engineering.",
                "Rollback criteria must revert on error threshold and dashboards alert on health checks.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    result = build_source_feature_flag_rollout_requirements(source)
    extracted = extract_source_feature_flag_rollout_requirements(model)
    derived = derive_source_feature_flag_rollout_requirements(model)
    payload = source_feature_flag_rollout_requirements_to_dict(result)
    markdown = source_feature_flag_rollout_requirements_to_markdown(result)

    assert source == original
    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_feature_flag_rollout_requirements(result) == result.summary
    assert source_feature_flag_rollout_requirements_to_dicts(result) == payload["requirements"]
    assert source_feature_flag_rollout_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert result.gaps == result.evidence_gaps
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert result.records[0].suggested_plan_impacts == result.records[0].planning_notes
    assert list(payload) == [
        "source_id",
        "title",
        "requirements",
        "evidence_gaps",
        "summary",
        "records",
        "findings",
        "gaps",
    ]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owners",
        "planning_notes",
    ]
    assert markdown.startswith("# Source Feature Flag Rollout Requirements Report: feature-flag-model")
    assert "product \\| engineering" in markdown


def test_unrelated_negated_invalid_and_repeated_inputs_are_stable_empty_reports():
    class BriefLike:
        id = "object-no-feature-flags"
        summary = "Feature flags, staged rollout, kill switch, and rollback work are out of scope."

    empty = build_source_feature_flag_rollout_requirements(
        _source_brief(
            source_id="empty-feature-flags",
            title="Feature list copy",
            summary="Update feature list copy, country flag icons, and percentage discount labels only.",
        )
    )
    repeat = build_source_feature_flag_rollout_requirements(
        _source_brief(
            source_id="empty-feature-flags",
            title="Feature list copy",
            summary="Update feature list copy, country flag icons, and percentage discount labels only.",
        )
    )
    negated = build_source_feature_flag_rollout_requirements(BriefLike())
    no_scope = build_source_feature_flag_rollout_requirements(
        _source_brief(summary="No feature flags or staged rollout support is required for this release.")
    )
    invalid = build_source_feature_flag_rollout_requirements(b"not text")

    expected_summary = {
        "requirement_count": 0,
        "category_counts": {
            "flag_ownership": 0,
            "audience_targeting": 0,
            "staged_rollout_percentages": 0,
            "kill_switch_behavior": 0,
            "experiment_variant_tracking": 0,
            "observability": 0,
            "rollback_criteria": 0,
            "cleanup_deprecation": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
        "evidence_gap_count": 0,
        "evidence_gaps": [],
        "status": "no_feature_flag_rollout_requirements_found",
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-feature-flags"
    assert empty.requirements == ()
    assert empty.evidence_gaps == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No feature flag rollout requirements were found" in empty.to_markdown()
    assert negated.requirements == ()
    assert no_scope.requirements == ()
    assert invalid.source_id is None
    assert invalid.requirements == ()
    assert invalid.summary == expected_summary


def _source_brief(
    *,
    source_id="source-feature-flags",
    title="Feature flag rollout requirements",
    domain="release",
    summary="General feature flag rollout requirements.",
    source_payload=None,
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
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "implementation-feature-flags",
        "source_brief_id": "source-feature-flags",
        "title": "Feature flag rollout implementation",
        "domain": "release",
        "target_user": "release manager",
        "buyer": "product",
        "workflow_context": "Teams need feature flag rollout requirements before implementation planning.",
        "problem_statement": "Feature flag rollout requirements need to be extracted early.",
        "mvp_goal": "Plan feature flag ownership, rollout controls, and rollback criteria.",
        "product_surface": "release controls",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run feature flag rollout extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
