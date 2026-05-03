import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_sandbox_environment_requirements import (
    SourceSandboxEnvironmentRequirement,
    SourceSandboxEnvironmentRequirementsReport,
    build_source_sandbox_environment_requirements,
    derive_source_sandbox_environment_requirements,
    extract_source_sandbox_environment_requirements,
    generate_source_sandbox_environment_requirements,
    source_sandbox_environment_requirements_to_dict,
    source_sandbox_environment_requirements_to_dicts,
    source_sandbox_environment_requirements_to_markdown,
    summarize_source_sandbox_environment_requirements,
)


def test_structured_payload_lists_and_dicts_extract_environment_requirements():
    result = build_source_sandbox_environment_requirements(
        _source_brief(
            source_payload={
                "environments": {
                    "sandbox": {
                        "category": "sandbox_environment",
                        "environment_types": "sandbox",
                        "requirement": "Sandbox environment must be available with isolated credentials.",
                    },
                    "staging": "Staging environment must support release candidate validation.",
                    "test_data": {
                        "category": "test_data",
                        "requirement": "Test data requires fixture accounts and synthetic data for onboarding cases.",
                    },
                    "refresh": {
                        "category": "refresh_cadence",
                        "refresh_cadence": "weekly refresh",
                        "requirement": "Environment refresh cadence must be weekly refresh from sanitized source data.",
                    },
                },
                "acceptance_criteria": [
                    "Feature parity must match production configuration and feature flags before acceptance testing.",
                    "External integration sandbox must use Stripe sandbox API keys and webhook sandbox callbacks.",
                ],
            }
        )
    )

    assert isinstance(result, SourceSandboxEnvironmentRequirementsReport)
    assert result.source_id == "source-sandbox-environment"
    assert all(isinstance(record, SourceSandboxEnvironmentRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "sandbox_environment",
        "staging_environment",
        "test_data",
        "refresh_cadence",
        "feature_parity",
        "external_integration_sandbox",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["sandbox_environment"].environment_types == ("sandbox",)
    assert by_category["sandbox_environment"].confidence == "high"
    assert by_category["sandbox_environment"].source_field == "source_payload.environments.sandbox"
    assert by_category["refresh_cadence"].refresh_cadence == "weekly refresh"
    assert by_category["external_integration_sandbox"].integration_dependency == "stripe"
    assert by_category["external_integration_sandbox"].owner_suggestion == "integrations"
    assert "third-party sandbox tenants" in by_category["external_integration_sandbox"].planning_note
    assert any("weekly refresh" in evidence for evidence in by_category["refresh_cadence"].evidence)
    assert result.summary["requirement_count"] == 6
    assert result.summary["requires_test_data"] is True
    assert result.summary["requires_refresh_cadence"] is True
    assert result.summary["requires_feature_parity"] is True
    assert result.summary["requires_external_integration_sandbox"] is True
    assert result.summary["status"] == "ready_for_sandbox_environment_planning"


def test_markdown_implementation_and_object_inputs_extract_non_production_requirements():
    text_result = build_source_sandbox_environment_requirements(
        """
# Non-production environments

- Sandbox must be provisioned for migration dry runs.
- Staging should mirror production for feature parity.
- Test accounts and fixture data are required for acceptance testing.
- Refresh cadence must reset the test environment nightly.
"""
    )
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "External integration sandbox must use Salesforce sandbox credentials.",
                "Production data is prohibited in staging; synthetic data only.",
            ],
            definition_of_done=[
                "Staging environment supports UAT before release.",
            ],
        )
    )
    object_result = build_source_sandbox_environment_requirements(
        SimpleNamespace(
            id="object-sandbox",
            environments="Provider sandbox must provide webhook sandbox callbacks for partner validation.",
        )
    )

    assert {
        "sandbox_environment",
        "staging_environment",
        "test_data",
        "refresh_cadence",
        "feature_parity",
    } <= {record.category for record in text_result.records}
    implementation_result = generate_source_sandbox_environment_requirements(implementation)
    assert implementation_result.source_id == "implementation-sandbox-environment"
    assert [record.category for record in implementation_result.records] == [
        "staging_environment",
        "production_data_restriction",
        "external_integration_sandbox",
    ]
    assert implementation_result.records[1].production_data_restriction is True
    assert implementation_result.records[2].integration_dependency == "salesforce"
    assert object_result.records[0].category == "external_integration_sandbox"
    assert object_result.records[0].integration_dependency == "webhook"


def test_production_data_restrictions_are_classified_separately_from_generic_environment_mentions():
    result = build_source_sandbox_environment_requirements(
        _source_brief(
            source_payload={
                "requirements": [
                    "Staging environment must be available for QA validation.",
                    "No production data is allowed in sandbox; customer data must be anonymized.",
                    "Production deployment uses the normal release window.",
                ]
            }
        )
    )

    assert [record.category for record in result.records] == [
        "staging_environment",
        "production_data_restriction",
    ]
    restriction = result.records[1]
    assert restriction.production_data_restriction is True
    assert restriction.environment_types == ("sandbox",)
    assert "production_data_restriction" in result.summary["categories"]
    assert result.summary["requires_production_data_restriction"] is True


def test_no_signal_negated_malformed_and_invalid_inputs_are_stable_empty_reports():
    class BriefLike:
        id = "object-no-sandbox"
        summary = "No sandbox, staging, or non-production environment work is required for this release."

    unrelated = build_source_sandbox_environment_requirements(
        _source_brief(summary="Admin copy update with production deployment notes only.")
    )
    negated = build_source_sandbox_environment_requirements(BriefLike())
    out_of_scope = build_source_sandbox_environment_requirements(
        _source_brief(non_goals=["Sandbox and staging environment requirements are out of scope."])
    )
    malformed = build_source_sandbox_environment_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_sandbox_environment_requirements(42)
    blank = build_source_sandbox_environment_requirements("")

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "sandbox_environment": 0,
            "staging_environment": 0,
            "test_data": 0,
            "refresh_cadence": 0,
            "production_data_restriction": 0,
            "feature_parity": 0,
            "external_integration_sandbox": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
        "environment_types": [],
        "refresh_cadences": [],
        "integration_dependencies": [],
        "requires_production_data_restriction": False,
        "requires_test_data": False,
        "requires_refresh_cadence": False,
        "requires_feature_parity": False,
        "requires_external_integration_sandbox": False,
        "status": "no_sandbox_environment_language",
    }
    assert unrelated.records == ()
    assert unrelated.findings == ()
    assert unrelated.to_dicts() == []
    assert unrelated.summary == expected_summary
    assert "No sandbox environment requirements were found" in unrelated.to_markdown()
    assert negated.records == ()
    assert out_of_scope.records == ()
    assert malformed.records == ()
    assert invalid.records == ()
    assert blank.records == ()


def test_serialization_aliases_markdown_escaping_ordering_and_no_input_mutation_are_stable():
    source = _source_brief(
        source_id="sandbox-model",
        summary="Sandbox must support acceptance testing with fixture accounts.",
        source_payload={
            "requirements": [
                "Sandbox must support acceptance testing with fixture accounts.",
                "Sandbox must support acceptance testing with fixture accounts.",
                "External integration sandbox must use Adyen sandbox credentials | partner note.",
                "Refresh cadence must refresh every 7 days.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_sandbox_environment_requirements(source)
    model_result = extract_source_sandbox_environment_requirements(model)
    generated = generate_source_sandbox_environment_requirements(model)
    derived = derive_source_sandbox_environment_requirements(model)
    payload = source_sandbox_environment_requirements_to_dict(model_result)
    markdown = source_sandbox_environment_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_sandbox_environment_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_sandbox_environment_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_sandbox_environment_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "requirement_text",
        "environment_types",
        "production_data_restriction",
        "refresh_cadence",
        "integration_dependency",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "owner_suggestion",
        "planning_note",
    ]
    assert {
        "sandbox_environment",
        "test_data",
        "refresh_cadence",
        "external_integration_sandbox",
    } <= {record.category for record in model_result.records}
    sandbox = next(record for record in model_result.records if record.category == "sandbox_environment")
    assert sandbox.evidence == (
        "source_payload.requirements[0]: Sandbox must support acceptance testing with fixture accounts.",
    )
    assert sandbox.requirement_category == "sandbox_environment"
    assert sandbox.planning_notes == (sandbox.planning_note,)
    assert sandbox.owner_suggestions == (sandbox.owner_suggestion,)
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Category | Requirement | Environment Types | Production Data Restriction |" in markdown
    assert "Adyen sandbox credentials \\| partner note" in markdown


def _source_brief(
    *,
    source_id="source-sandbox-environment",
    title="Sandbox environment requirements",
    domain="platform",
    summary="General sandbox environment requirements.",
    source_payload=None,
    non_goals=None,
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
    } | ({"non_goals": non_goals} if non_goals is not None else {})


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "implementation-sandbox-environment",
        "source_brief_id": "source-sandbox-environment",
        "title": "Sandbox environment rollout",
        "domain": "platform",
        "target_user": "execution agents",
        "buyer": None,
        "workflow_context": "Execution planning needs explicit non-production environment expectations.",
        "problem_statement": "Migration and integration plans need source-backed environment details.",
        "mvp_goal": "Plan sandbox and staging behavior from source briefs.",
        "product_surface": "platform operations",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for environment coverage.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
