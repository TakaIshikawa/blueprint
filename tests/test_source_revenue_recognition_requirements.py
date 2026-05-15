import json

from blueprint.domain.models import SourceBrief
from blueprint.source_revenue_recognition_requirements import (
    build_source_revenue_recognition_requirements,
    derive_source_revenue_recognition_requirements,
    extract_source_revenue_recognition_requirements,
    generate_source_revenue_recognition_requirements,
    source_revenue_recognition_requirements_to_dict,
    source_revenue_recognition_requirements_to_dicts,
    source_revenue_recognition_requirements_to_markdown,
    summarize_source_revenue_recognition_requirements,
)


def test_extracts_revenue_recognition_categories_from_source_brief():
    model = SourceBrief.model_validate(_source([
        "Revenue recognition rule must recognize revenue ratably over time under ASC 606.",
        "Revenue recognition performance obligation must map setup, subscription, and service deliverables.",
        "Revenue recognition contract term must capture start date, end date, and annual term length.",
        "Revenue recognition deferral schedule must amortize monthly to deferred revenue account periods.",
        "Revenue recognition modification handling must use prospective treatment on modification date.",
        "Revenue recognition accounting export must send GL journal export files to NetSuite.",
        "Revenue recognition audit evidence must attach contract, invoice, calculation, and audit log.",
        "Revenue recognition compliance review must require controller quarterly sign-off.",
    ]))
    result = build_source_revenue_recognition_requirements(model)

    assert [record.requirement_type for record in result.records] == ["recognition_rule", "performance_obligation", "contract_term", "deferral_schedule", "modification_handling", "accounting_export", "audit_evidence", "compliance_review"]
    assert result.summary["missing_detail_flags"] == []


def test_partial_revenue_recognition_requirements_need_detail():
    result = derive_source_revenue_recognition_requirements("Revenue recognition rule is required. Revenue recognition performance obligation is required. Revenue recognition deferral schedule is required. Revenue recognition modification handling is required. Revenue recognition accounting export is required. Revenue recognition compliance review is required.")

    assert result.summary["missing_detail_flags"] == ["missing_recognition_rules", "missing_obligations", "missing_deferral_schedule", "missing_modification_handling", "missing_accounting_export", "missing_compliance_review"]


def test_helpers_serializers_negation_and_invalid_inputs_are_stable():
    report = extract_source_revenue_recognition_requirements(_source(["Revenue recognition audit evidence must attach invoice and calculation audit log."], "revenue-model"))
    payload = source_revenue_recognition_requirements_to_dict(report)

    assert generate_source_revenue_recognition_requirements("Revenue recognition accounting export must send ERP ledger journals.").summary["requirement_count"] == 1
    assert summarize_source_revenue_recognition_requirements(report)["requirement_count"] == 1
    assert build_source_revenue_recognition_requirements("").records == ()
    assert build_source_revenue_recognition_requirements(3.14).records == ()
    assert build_source_revenue_recognition_requirements("No revenue recognition changes are required.").records == ()
    assert json.loads(json.dumps(payload))["source_id"] == "revenue-model"
    assert source_revenue_recognition_requirements_to_dicts(report) == payload["records"]
    assert "Source Revenue Recognition Requirements Report" in source_revenue_recognition_requirements_to_markdown(report)


def _source(lines, source_id="revenue-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "source_id": f"{source_id}-upstream", "title": "Revenue recognition", "summary": "Revenue recognition planning", "source_payload": {"requirements": lines}, "source_links": {}}
