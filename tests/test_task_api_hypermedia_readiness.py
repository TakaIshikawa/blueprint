import json

from blueprint.task_api_hypermedia_readiness import (
    TaskApiHypermediaReadinessFinding,
    TaskApiHypermediaReadinessPlan,
    build_task_api_hypermedia_readiness_plan,
    derive_task_api_hypermedia_readiness_plan,
    extract_task_api_hypermedia_readiness_findings,
    generate_task_api_hypermedia_readiness_plan,
    summarize_task_api_hypermedia_readiness,
)


def test_hal_links_and_link_generation_detected_with_partial_readiness():
    plan = {
        "id": "plan-hypermedia",
        "tasks": [
            {
                "id": "task-hal",
                "title": "Implement HAL links",
                "description": "API responses must include HAL _links with self and related hrefs.",
                "acceptance_criteria": ["Link generation logic creates valid href values"],
            },
        ],
    }

    result = build_task_api_hypermedia_readiness_plan(plan)
    finding = result.findings[0]

    assert isinstance(result, TaskApiHypermediaReadinessPlan)
    assert isinstance(finding, TaskApiHypermediaReadinessFinding)
    assert finding.task_id == "task-hal"
    assert "hal_links" in finding.detected_signals
    assert "link_generation_tests" not in finding.present_safeguards
    assert "link_generation_tests" in finding.missing_safeguards
    assert finding.readiness in {"weak", "partial"}
    assert len(finding.actionable_remediations) > 0
    assert "link generation" in finding.actionable_remediations[0].casefold()


def test_uri_templates_with_expansion_tests_shows_strong_readiness():
    plan = {
        "id": "plan-templates",
        "tasks": [
            {
                "id": "task-templates",
                "title": "Implement URI template expansion",
                "description": "API must support URI templates per RFC 6570 with variable expansion.",
                "acceptance_criteria": [
                    "URI template expansion tests verify variable substitution and malformed template handling"
                ],
            },
        ],
    }

    result = build_task_api_hypermedia_readiness_plan(plan)
    finding = result.findings[0]

    assert "uri_templates" in finding.detected_signals
    assert "uri_template_expansion_tests" in finding.present_safeguards
    assert result.summary["hypermedia_task_count"] == 1
    assert result.summary["readiness_counts"]["strong"] >= 0


def test_no_hypermedia_impact_and_aliases_are_stable():
    plan = {
        "id": "plan-mixed",
        "tasks": [
            {
                "id": "task-hypermedia",
                "title": "Add hypermedia controls",
                "description": "Hypermedia controls must expose allowed methods and affordances.",
            },
            {
                "id": "task-no-impact",
                "title": "Update user profile",
                "description": "No hypermedia changes are required for this task.",
            },
        ],
    }

    result = build_task_api_hypermedia_readiness_plan(plan)
    extracted = extract_task_api_hypermedia_readiness_findings(plan)
    derived = derive_task_api_hypermedia_readiness_plan(plan)
    generated = generate_task_api_hypermedia_readiness_plan(plan)
    summary = summarize_task_api_hypermedia_readiness(result)
    payload = result.to_dict()

    assert result.hypermedia_task_ids == ("task-hypermedia",)
    assert result.not_applicable_task_ids == ("task-no-impact",)
    assert extracted == result.findings
    assert derived.to_dict() == result.to_dict()
    assert generated.to_dict() == result.to_dict()
    assert summary == result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.findings[0].actionable_gaps == result.findings[0].actionable_remediations


def test_embedded_resources_with_circular_reference_handling():
    plan = {
        "id": "plan-embedded",
        "tasks": [
            {
                "id": "task-embedded",
                "title": "Implement embedded resources",
                "description": "API responses may include _embedded resources to reduce round trips. Circular reference handling prevents infinite recursion with nesting depth limits.",
            },
        ],
    }

    result = build_task_api_hypermedia_readiness_plan(plan)
    finding = result.findings[0]

    assert "embedded_resources" in finding.detected_signals
    assert "circular_reference_handling" in finding.present_safeguards
    assert "circular_reference_handling" not in finding.missing_safeguards
