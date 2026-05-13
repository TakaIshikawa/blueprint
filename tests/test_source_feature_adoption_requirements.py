import json

from blueprint.source_feature_adoption_requirements import (
    build_source_feature_adoption_requirements,
    source_feature_adoption_requirements_to_dict,
    source_feature_adoption_requirements_to_dicts,
    source_feature_adoption_requirements_to_markdown,
)


def test_identifies_feature_adoption_categories_with_evidence():
    result = build_source_feature_adoption_requirements(
        _source(
            [
                "Feature adoption activation metric must count activated users after the first value key action.",
                "Feature adoption cohort segmentation must target new admin users and enterprise customer segment.",
                "Feature adoption funnel milestones must track setup step and onboarding complete milestone.",
                "Feature adoption target must reach 35% usage within launch month.",
                "Feature adoption experiment linkage must connect the feature flag experiment, variants, and holdout.",
                "Feature adoption lifecycle messaging must trigger email nudge and push notification campaign.",
                "Feature adoption in-product education must include tooltip, coach mark, guided tour, and checklist.",
                "Feature adoption success reporting must publish a weekly adoption dashboard.",
                "Feature adoption feedback loop must collect survey and interview feedback.",
            ]
        )
    )

    assert [record.requirement_type for record in result.records] == [
        "activation_metric",
        "cohort_segmentation",
        "funnel_milestones",
        "adoption_targets",
        "experiment_linkage",
        "lifecycle_messaging",
        "in_product_education",
        "success_reporting",
        "feedback_loops",
    ]
    assert all(record.evidence for record in result.records)


def test_partial_brief_flags_activation_cohort_reporting_and_feedback_gaps():
    result = build_source_feature_adoption_requirements(
        _source(
            [
                "Feature adoption activation metric is required.",
                "Feature adoption target cohort is required.",
                "Feature adoption success reporting and feedback loop are required.",
            ],
            source_id="feature-adoption-partial",
        )
    )

    assert result.summary["missing_detail_flags"] == [
        "missing_activation_metric",
        "missing_target_cohort",
        "missing_reporting_feedback_loop",
    ]


def test_dict_list_markdown_serializers_and_negated_scope_are_deterministic():
    result = build_source_feature_adoption_requirements(
        [
            _source(["Feature adoption activation metric must track activation event."], source_id="a"),
            _source(["No feature adoption planning changes are required."], source_id="b"),
        ]
    )

    payload = source_feature_adoption_requirements_to_dict(result)
    assert [record["requirement_type"] for record in payload["records"]] == ["activation_metric"]
    assert json.loads(json.dumps(payload, sort_keys=True))["summary"]["requirement_count"] == 1
    assert source_feature_adoption_requirements_to_dicts(result) == payload["records"]
    assert "# Source Feature Adoption Requirements Report" in source_feature_adoption_requirements_to_markdown(result)


def _source(lines, source_id="feature-adoption"):
    return {
        "id": source_id,
        "source_project": "requirements",
        "source_entity_type": "brief",
        "source_id": f"{source_id}-upstream",
        "title": "Feature adoption",
        "summary": "Feature adoption planning",
        "source_payload": {"requirements": lines},
        "source_links": {},
    }
