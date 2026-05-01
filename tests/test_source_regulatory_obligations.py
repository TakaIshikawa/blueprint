import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_regulatory_obligations import (
    SourceRegulatoryObligation,
    SourceRegulatoryObligationsReport,
    build_source_regulatory_obligations,
    source_regulatory_obligations_to_dict,
    source_regulatory_obligations_to_markdown,
)


def test_plain_mapping_fields_infer_regulatory_obligations_with_evidence_and_constraints():
    result = build_source_regulatory_obligations(
        {
            "id": "privacy-brief",
            "title": "Regional checkout privacy",
            "body": "GDPR and CCPA/CPRA require consent before marketing attribution cookies.",
            "goals": ["Keep EU data in approved regional data residency boundaries."],
            "constraints": [
                "HIPAA PHI exports must be encrypted and audit logged.",
                "PCI DSS cardholder data cannot be written to application logs.",
            ],
            "acceptance_criteria": [
                "WCAG 2.2 AA keyboard navigation and screen reader checks pass.",
                "Audit logs capture admin export, deletion, and access events.",
            ],
            "metadata": {"note": "SOC 2 evidence requires access control review."},
        }
    )

    assert isinstance(result, SourceRegulatoryObligationsReport)
    assert result.brief_id == "privacy-brief"
    assert result.summary["confidence_counts"]["high"] >= 3
    assert result.summary["obligation_counts_by_regulation"]["GDPR"] >= 1
    assert result.summary["obligation_counts_by_regulation"]["CCPA/CPRA"] >= 1
    assert result.summary["obligation_counts_by_regulation"]["HIPAA"] >= 1
    assert result.summary["obligation_counts_by_regulation"]["PCI DSS"] >= 1
    assert result.summary["obligation_counts_by_regulation"]["Accessibility"] >= 1
    assert result.summary["suggested_constraint_count"] > 0

    hipaa = _finding(result, "HIPAA", "encryption")
    assert isinstance(hipaa, SourceRegulatoryObligation)
    assert hipaa.confidence == "high"
    assert hipaa.source_field == "constraints[0]"
    assert "PHI exports" in hipaa.evidence
    assert any("HIPAA" in item for item in hipaa.suggested_constraints)
    assert "access controls" in hipaa.required_controls

    accessibility = _finding(result, "Accessibility", "accessibility")
    assert accessibility.source_field == "acceptance_criteria[0]"
    assert any("accessibility standard" in item for item in accessibility.suggested_constraints)


def test_metadata_can_explicitly_provide_obligations_and_required_controls():
    result = build_source_regulatory_obligations(
        {
            "id": "explicit-brief",
            "title": "Vendor onboarding",
            "summary": "Connect a new data processor.",
            "metadata": {
                "obligations": [
                    {
                        "regulation": "DPA",
                        "type": "vendor_agreement",
                        "evidence": "DPA must be signed before sharing customer personal data.",
                        "confidence": "high",
                        "suggested_constraints": ["Block export until Legal records the signed DPA."],
                        "unresolved_questions": ["Which subprocessors are in scope?"],
                    }
                ],
                "required_controls": [
                    {
                        "framework": "SOC 2",
                        "control": "Quarterly access review",
                        "description": "SOC 2 requires access review evidence for the new processor.",
                    }
                ],
            },
        }
    )

    assert result.summary["explicit_obligation_count"] == 2
    dpa = _finding(result, "DPA", "vendor_agreement")
    soc2 = _finding(result, "SOC 2", "access_control")
    assert dpa.explicit is True
    assert dpa.source_field == "metadata.obligations[0]"
    assert dpa.suggested_constraints == ("Block export until Legal records the signed DPA.",)
    assert dpa.unresolved_questions == ("Which subprocessors are in scope?",)
    assert soc2.explicit is True
    assert soc2.required_controls == ("Quarterly access review",)
    assert soc2.source_field == "metadata.required_controls[0]"


def test_source_and_implementation_brief_models_and_objects_are_supported():
    source_result = build_source_regulatory_obligations(
        SourceBrief.model_validate(
            {
                "id": "source-model",
                "title": "Children accounts",
                "domain": "consumer",
                "summary": "COPPA parental consent is required for under 13 account creation.",
                "source_project": "notion",
                "source_entity_type": "brief",
                "source_id": "KIDS-7",
                "source_payload": {
                    "requirements": ["Store consent receipts and support deletion requests."],
                    "metadata": {"required_controls": ["COPPA parental consent evidence"]},
                },
                "source_links": {},
            }
        )
    )
    implementation_result = build_source_regulatory_obligations(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=["Retain financial records for seven years and keep immutable audit logs."],
                data_requirements="Regional data handling must keep EU data in EU storage.",
            )
        )
    )
    object_result = build_source_regulatory_obligations(
        SimpleNamespace(
            id="object-brief",
            title="Privacy settings",
            body="Do not sell opt-out must be available for California privacy requests.",
        )
    )

    assert _finding(source_result, "COPPA", "consent").confidence == "high"
    assert source_result.summary["explicit_obligation_count"] == 1
    assert _finding(implementation_result, "Financial Retention", "retention").source_field == "scope[0]"
    assert _finding(implementation_result, "Regional Data Handling", "data_residency").source_field == "data_requirements"
    assert _finding(object_result, "CCPA/CPRA", "consent").confidence == "high"


def test_serialization_markdown_empty_state_and_json_ordering_are_stable():
    result = build_source_regulatory_obligations(
        {
            "id": "pipes",
            "title": "GDPR | consent",
            "body": "GDPR requires consent | audit evidence?",
        }
    )
    payload = source_regulatory_obligations_to_dict(result)
    markdown = source_regulatory_obligations_to_markdown(result)
    empty = build_source_regulatory_obligations({"id": "empty", "title": "Profile polish", "body": "Update labels."})
    invalid = build_source_regulatory_obligations(17)

    assert result.to_dicts() == payload["obligations"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "obligations", "summary"]
    assert list(payload["obligations"][0]) == [
        "regulation",
        "obligation_type",
        "confidence",
        "source_field",
        "evidence",
        "suggested_constraints",
        "unresolved_questions",
        "required_controls",
        "explicit",
    ]
    assert markdown.startswith("# Source Regulatory Obligations Report: pipes")
    assert "## Summary" in markdown
    assert "| Regulation | Type | Confidence | Source | Evidence | Suggested Constraints | Questions | Controls |" in markdown
    assert "GDPR \\| consent" in markdown
    assert result.summary["unresolved_question_count"] >= 1
    assert empty.brief_id == "empty"
    assert empty.obligations == ()
    assert empty.summary["confidence_counts"] == {"high": 0, "medium": 0, "low": 0}
    assert "No regulatory obligations were found" in empty.to_markdown()
    assert invalid.brief_id is None
    assert invalid.obligations == ()


def _finding(result, regulation, obligation_type):
    return next(
        obligation
        for obligation in result.obligations
        if obligation.regulation == regulation and obligation.obligation_type == obligation_type
    )


def _implementation_brief(*, scope=None, data_requirements=None):
    return {
        "id": "impl-brief",
        "source_brief_id": "source-brief",
        "title": "Ledger archive",
        "domain": "finance",
        "target_user": "Ops",
        "buyer": "Finance",
        "workflow_context": "Record review",
        "problem_statement": "Finance needs compliant archive handling.",
        "mvp_goal": "Ship archive controls.",
        "product_surface": "Admin dashboard",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": data_requirements,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run compliance smoke tests.",
        "definition_of_done": [],
        "status": "draft",
    }
