import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_feature_flag_rollout_requirements import (
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


def test_extracts_explicit_feature_flag_requirements_with_source_evidence():
    result = build_source_feature_flag_rollout_requirements(
        _source_brief(
            summary=(
                "Checkout changes must ship behind a feature flag with flag key checkout_v2. "
                "Acceptance requires LaunchDarkly ownership before launch."
            ),
            source_payload={
                "requirements": [
                    "Feature toggle must default off for existing tenants.",
                ]
            },
        )
    )

    assert isinstance(result, SourceFeatureFlagRolloutRequirementsReport)
    assert all(isinstance(record, SourceFeatureFlagRolloutRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == ["feature_flag"]
    record = result.records[0]
    assert record.source_brief_id == "source-rollout"
    assert "feature flag" in record.matched_terms
    assert any("summary: Checkout changes" in item for item in record.evidence)
    assert any("source_payload.requirements[0]" in item for item in record.evidence)
    assert record.confidence == "high"
    assert result.summary["requirement_count"] == 1
    assert result.summary["requirement_type_counts"]["feature_flag"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_extracts_percentage_staged_and_gradual_rollouts_in_deterministic_order():
    result = build_source_feature_flag_rollout_requirements(
        _source_brief(
            source_payload={
                "rollout": [
                    "Use a staged rollout: wave 1 internal users, wave 2 paid accounts.",
                    "Ramp from 5% of traffic to 25% of traffic after guardrails pass.",
                    "Gradual release should use progressive delivery checkpoints.",
                ]
            }
        )
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert [record.requirement_type for record in result.records] == [
        "staged_rollout",
        "percentage_rollout",
        "gradual_release",
    ]
    assert by_type["staged_rollout"].rollout_value == "wave 1"
    assert by_type["percentage_rollout"].rollout_value == "5% of traffic"
    assert by_type["gradual_release"].planning_note.startswith("Plan progressive exposure")
    assert any(
        "Gradual release should use progressive delivery" in item
        for item in by_type["gradual_release"].evidence
    )
    assert result.summary["requirement_type_counts"]["percentage_rollout"] == 1


def test_extracts_beta_access_and_cohort_targeting_language():
    result = build_source_feature_flag_rollout_requirements(
        _source_brief(
            summary=(
                "Private beta access should target beta customers first. "
                "Rollout rules must allowlist enterprise accounts and exclude free-tier tenants."
            ),
            source_payload={
                "metadata": {
                    "targeting": "Cohort targeting required for preview users in APAC accounts.",
                }
            },
        )
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert [record.requirement_type for record in result.records] == [
        "beta_access",
        "cohort_targeting",
    ]
    assert by_type["beta_access"].target_audience.startswith("beta customers")
    assert "allowlist" in by_type["cohort_targeting"].matched_terms
    assert any("source_payload.metadata.targeting" in item for item in by_type["cohort_targeting"].evidence)
    assert any("beta customers" in audience for audience in result.summary["target_audiences"])


def test_extracts_kill_switch_requirements_from_implementation_brief():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Release must include a kill switch that can instantly disable the new pricing flow.",
                "Definition of done: disable flag path is verified before widening rollout.",
            ],
        )
    )

    result = extract_source_feature_flag_rollout_requirements(brief)
    by_type = {record.requirement_type: record for record in result.records}

    assert result.source_id == "impl-rollout"
    assert "kill_switch" in by_type
    assert "feature_flag" in by_type
    assert by_type["kill_switch"].confidence == "high"
    assert any("kill switch" in item for item in by_type["kill_switch"].evidence)
    assert by_type["kill_switch"].planning_note.startswith("Define emergency disable")


def test_aliases_serialization_markdown_and_no_source_mutation_are_stable():
    source = _source_brief(
        source_id="rollout-model",
        summary="Feature flag must target beta users for a gradual rollout.",
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_feature_flag_rollout_requirements(source)
    generated = generate_source_feature_flag_rollout_requirements(model)
    derived = derive_source_feature_flag_rollout_requirements(model)
    extracted = extract_source_feature_flag_rollout_requirements(model)
    payload = source_feature_flag_rollout_requirements_to_dict(generated)
    markdown = source_feature_flag_rollout_requirements_to_markdown(generated)

    assert source == original
    assert mapping_result.to_dict() == generated.to_dict()
    assert derived.to_dict() == generated.to_dict()
    assert extracted.to_dict() == generated.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert generated.records == generated.requirements
    assert generated.findings == generated.requirements
    assert source_feature_flag_rollout_requirements_to_dicts(generated) == payload["requirements"]
    assert source_feature_flag_rollout_requirements_to_dicts(generated.records) == payload["records"]
    assert summarize_source_feature_flag_rollout_requirements(generated) == generated.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_type",
        "rollout_value",
        "target_audience",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "planning_note",
    ]
    assert markdown == generated.to_markdown()
    assert markdown.startswith("# Source Feature Flag Rollout Requirements Report: rollout-model")


def test_no_match_for_unrelated_deployment_or_release_wording():
    result = build_source_feature_flag_rollout_requirements(
        _source_brief(
            title="Release calendar",
            summary=(
                "Deploy the copy update during the normal release window. "
                "Coordinate rollout notes with support after production deployment."
            ),
            source_payload={
                "requirements": [
                    "No feature flag rollout changes are required.",
                    "Publish the release announcement after deploy completes.",
                ]
            },
        )
    )
    malformed = build_source_feature_flag_rollout_requirements(
        {"source_payload": {"notes": object()}}
    )
    invalid = build_source_feature_flag_rollout_requirements(42)

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "requirement_types": [],
        "requirement_type_counts": {
            "feature_flag": 0,
            "staged_rollout": 0,
            "percentage_rollout": 0,
            "beta_access": 0,
            "cohort_targeting": 0,
            "kill_switch": 0,
            "gradual_release": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "target_audiences": [],
        "status": "no_feature_flag_rollout_language",
    }
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == expected_summary
    assert malformed.summary == expected_summary
    assert invalid.summary == expected_summary
    assert "No source feature flag rollout requirements were inferred" in result.to_markdown()


def _source_brief(
    *,
    source_id="source-rollout",
    title="Feature flag rollout requirements",
    domain="release",
    summary="General rollout requirements.",
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
    source_id="impl-rollout",
    title="Pricing rollout",
    summary="Rollout implementation requirements.",
    scope=None,
):
    return {
        "id": source_id,
        "source_brief_id": source_id,
        "title": title,
        "domain": "billing",
        "target_user": None,
        "buyer": None,
        "workflow_context": None,
        "problem_statement": summary,
        "mvp_goal": "Release pricing safely.",
        "product_surface": None,
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Validate rollout controls.",
        "definition_of_done": [],
    }
