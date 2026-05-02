import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_ux_state_inventory import (
    SourceUxStateInventory,
    SourceUxStateRequirement,
    build_source_ux_state_inventory,
    extract_source_ux_state_inventory,
    source_ux_state_inventory_to_dict,
)


def test_source_brief_mapping_extracts_states_from_text_and_structured_payload():
    source = _source_brief(
        summary=(
            "Dashboard must show a skeleton loading state, an empty state when there are no projects, "
            "and an error state with retry."
        ),
        source_payload={
            "personas": [
                {
                    "role": "Customer admins",
                    "requirements": [
                        "Offline users see cached data with a reconnect message.",
                        "Unauthorized guests see permission denied copy.",
                    ],
                }
            ],
            "constraints": [
                "Partial data is acceptable when one integration is degraded.",
                "Disabled button copy explains unavailable actions.",
            ],
        },
    )

    result = build_source_ux_state_inventory(source)
    by_state = {requirement.state: requirement for requirement in result.requirements}

    assert isinstance(result, SourceUxStateInventory)
    assert {"loading", "empty", "error", "offline", "permission_denied", "partial_data", "disabled"} <= set(by_state)
    assert by_state["offline"].audience == "users"
    assert any("source_payload.personas" in item for item in by_state["offline"].evidence)
    assert by_state["partial_data"].confidence >= 0.84
    assert "partial data UI state" in by_state["partial_data"].suggested_acceptance_criterion


def test_implementation_brief_mapping_extracts_definition_of_done_and_personas():
    result = build_source_ux_state_inventory(
        _implementation_brief(
            target_user="Free users",
            workflow_context="First-time users need onboarding and getting started guidance.",
            definition_of_done=[
                "Success message confirms the settings were saved.",
                "Upgrade required paywall appears when the free plan reaches the plan limit.",
            ],
            assumptions=["Read-only users see disabled controls for actions they cannot perform."],
        )
    )

    by_state = {requirement.state: requirement for requirement in result.requirements}

    assert by_state["first_run"].audience == "new users"
    assert by_state["success"].confidence == 0.9
    assert by_state["upgrade_required"].audience == "free users"
    assert by_state["disabled"].audience == "users"


def test_model_inputs_match_mapping_inputs_without_mutation():
    source = _source_brief(
        summary="Mobile users need loading, success, and error states.",
        source_payload={"notes": ["No results should render an empty state."]},
    )
    original = copy.deepcopy(source)
    source_model = SourceBrief.model_validate(source)

    implementation = _implementation_brief(
        workflow_context="Admins need offline and permission denied states.",
        definition_of_done=["Confirmation appears after submit."],
    )
    implementation_model = ImplementationBrief.model_validate(implementation)

    assert build_source_ux_state_inventory(source).to_dict() == build_source_ux_state_inventory(source_model).to_dict()
    assert (
        build_source_ux_state_inventory(implementation).to_dict()
        == build_source_ux_state_inventory(implementation_model).to_dict()
    )
    assert source == original


def test_duplicate_requirements_merge_by_state_and_audience_without_losing_evidence():
    result = extract_source_ux_state_inventory(
        _source_brief(
            summary="Admins see an error state when reports fail.",
            source_payload={
                "acceptance_criteria": [
                    "Admin users get an actionable error state with retry.",
                    "Admins see retry copy when the report failed.",
                ]
            },
        )
    )

    errors = [requirement for requirement in result.requirements if requirement.state == "error"]

    assert len(errors) == 1
    assert errors[0].audience == "admins"
    assert len(errors[0].evidence) == 3
    assert errors[0].confidence == 0.92
    assert result.summary["state_counts"]["error"] == 1


def test_no_ui_signals_returns_empty_inventory_with_summary_metadata():
    result = build_source_ux_state_inventory(
        _source_brief(
            summary="Add account settings filters and reporting columns.",
            source_payload={"notes": ["Prioritize CSV labels and account metadata."]},
        )
    )

    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "requirement_count": 0,
        "states": [],
        "state_counts": {
            "loading": 0,
            "empty": 0,
            "error": 0,
            "offline": 0,
            "permission_denied": 0,
            "success": 0,
            "partial_data": 0,
            "disabled": 0,
            "first_run": 0,
            "upgrade_required": 0,
        },
        "audiences": [],
        "evidence_count": 0,
    }


def test_stable_serialization_shape_and_json_round_trip():
    result = build_source_ux_state_inventory(
        _implementation_brief(
            target_user="Paid users",
            validation_plan="Verify loading and offline states before syncing resumes.",
            definition_of_done=["Submitted form shows a success message."],
        )
    )
    payload = source_ux_state_inventory_to_dict(result)

    assert isinstance(result.requirements[0], SourceUxStateRequirement)
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "summary"]
    assert list(payload["requirements"][0]) == [
        "state",
        "audience",
        "confidence",
        "evidence",
        "suggested_acceptance_criterion",
    ]
    assert [item["state"] for item in payload["requirements"]] == ["loading", "offline", "success"]
    assert result.records == result.requirements
    assert result.to_dicts() == payload["requirements"]


def _source_brief(*, summary, source_payload=None):
    return {
        "id": "source-ux-states",
        "title": "UX state brief",
        "domain": "product",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "ticket",
        "source_id": "ticket-ux-states",
        "source_payload": source_payload or {},
        "source_links": {},
    }


def _implementation_brief(
    *,
    target_user="Users",
    workflow_context="Users manage account settings.",
    validation_plan="Manual QA verifies critical UI states.",
    definition_of_done=None,
    assumptions=None,
):
    return {
        "id": "implementation-ux-states",
        "source_brief_id": "source-ux-states",
        "title": "Implement UX states",
        "domain": "product",
        "target_user": target_user,
        "buyer": None,
        "workflow_context": workflow_context,
        "problem_statement": "Users need predictable UI feedback.",
        "mvp_goal": "Ship account settings.",
        "product_surface": "web app",
        "scope": [],
        "non_goals": [],
        "assumptions": assumptions or [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": validation_plan,
        "definition_of_done": definition_of_done or [],
        "status": "draft",
    }
