import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_account_deletion_requirements import (
    SourceAccountDeletionRequirement,
    SourceAccountDeletionRequirementsReport,
    build_source_account_deletion_requirements,
    derive_source_account_deletion_requirements,
    extract_source_account_deletion_requirements,
    generate_source_account_deletion_requirements,
    source_account_deletion_requirements_to_dict,
    source_account_deletion_requirements_to_dicts,
    source_account_deletion_requirements_to_markdown,
    summarize_source_account_deletion_requirements,
)


def test_structured_metadata_and_summary_extract_deletion_requirements_in_order():
    result = build_source_account_deletion_requirements(
        _source_brief(
            summary=(
                "Users must be able to delete their account. Use soft delete for 30 days "
                "before hard delete, with customer confirmation when complete."
            ),
            source_payload={
                "privacy": {
                    "erasure": "Right to erasure requires anonymization of profile PII.",
                    "processors": "Downstream processors must delete replicated account data.",
                }
            },
        )
    )

    assert isinstance(result, SourceAccountDeletionRequirementsReport)
    assert all(isinstance(record, SourceAccountDeletionRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "account_deletion",
        "soft_delete",
        "hard_delete",
        "anonymization",
        "restoration_window",
        "downstream_deletion",
        "customer_confirmation",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["account_deletion"].source_brief_id == "source-erasure"
    assert by_type["soft_delete"].window == "for 30 days"
    assert by_type["hard_delete"].confidence == "high"
    assert "processor callbacks" in by_type["downstream_deletion"].suggested_plan_impacts
    assert any("summary: Users must be able" in item for item in by_type["account_deletion"].evidence)
    assert any("source_payload.privacy.erasure" in item for item in by_type["anonymization"].evidence)
    assert result.summary["requirement_type_counts"]["hard_delete"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_distinguishes_workspace_cancellation_audit_and_recovery_signals_from_impl_brief():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Workspace deletion must erase tenant projects after subscription cancellation.",
                "Keep audit logs under legal hold as an audit exception to account erasure.",
                "Allow restore during a 14 day deletion grace period before purge.",
            ]
        )
    )

    result = extract_source_account_deletion_requirements(brief)
    by_type = {record.requirement_type: record for record in result.records}

    assert result.source_id == "impl-erasure"
    assert [record.requirement_type for record in result.records] == [
        "workspace_deletion",
        "hard_delete",
        "cancellation_triggered_deletion",
        "restoration_window",
        "audit_exception",
    ]
    assert by_type["workspace_deletion"].data_scope == "tenant projects"
    assert by_type["cancellation_triggered_deletion"].trigger == "after subscription cancellation"
    assert by_type["restoration_window"].window == "14 day"
    assert by_type["audit_exception"].suggested_plan_impacts == (
        "retention exceptions",
        "audit evidence carve-outs",
    )


def test_aliases_serialization_markdown_and_object_input_are_stable():
    source = _source_brief(
        source_id="object-erasure",
        summary="Account cancellation should trigger deletion and notify customer when complete.",
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    obj = type("BriefObject", (), source)()

    mapping_result = build_source_account_deletion_requirements(source)
    generated = generate_source_account_deletion_requirements(model)
    derived = derive_source_account_deletion_requirements(model)
    extracted = extract_source_account_deletion_requirements(obj)
    payload = source_account_deletion_requirements_to_dict(generated)
    markdown = source_account_deletion_requirements_to_markdown(generated)

    assert source == original
    assert mapping_result.to_dict() == generated.to_dict()
    assert derived.to_dict() == generated.to_dict()
    assert extracted.to_dict() == generated.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert generated.records == generated.requirements
    assert generated.findings == generated.requirements
    assert source_account_deletion_requirements_to_dicts(generated) == payload["requirements"]
    assert source_account_deletion_requirements_to_dicts(generated.records) == payload["records"]
    assert summarize_source_account_deletion_requirements(generated) == generated.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_type",
        "data_scope",
        "trigger",
        "window",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "suggested_plan_impacts",
    ]
    assert markdown == generated.to_markdown()
    assert markdown.startswith("# Source Account Deletion Requirements Report: object-erasure")


def test_invalid_or_no_signal_inputs_return_empty_report_without_raising():
    result = build_source_account_deletion_requirements(
        _source_brief(
            title="Dashboard copy",
            summary="Improve onboarding copy and simplify the settings layout.",
            source_payload={"requirements": ["No account deletion changes are required."]},
        )
    )
    malformed = build_source_account_deletion_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_account_deletion_requirements(42)

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "requirement_types": [],
        "requirement_type_counts": {
            "account_deletion": 0,
            "workspace_deletion": 0,
            "soft_delete": 0,
            "hard_delete": 0,
            "anonymization": 0,
            "cancellation_triggered_deletion": 0,
            "restoration_window": 0,
            "downstream_deletion": 0,
            "audit_exception": 0,
            "customer_confirmation": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "plan_impacts": [],
        "status": "no_account_deletion_language",
    }
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == expected_summary
    assert malformed.summary == expected_summary
    assert invalid.summary == expected_summary
    assert "No source account deletion requirements were inferred" in result.to_markdown()


def _source_brief(
    *,
    source_id="source-erasure",
    title="Account deletion requirements",
    domain="privacy",
    summary="General account deletion requirements.",
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
    source_id="impl-erasure",
    title="Erasure implementation",
    summary="Erasure implementation requirements.",
    scope=None,
):
    return {
        "id": source_id,
        "source_brief_id": source_id,
        "title": title,
        "domain": "privacy",
        "target_user": None,
        "buyer": None,
        "workflow_context": None,
        "problem_statement": summary,
        "mvp_goal": "Implement account erasure safely.",
        "product_surface": None,
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Validate erasure behavior.",
        "definition_of_done": [],
    }
