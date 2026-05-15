from blueprint.source_partner_certification_requirements import build_source_partner_certification_requirements


def test_extracts_all_partner_certification_categories_with_evidence():
    result = build_source_partner_certification_requirements(_source([
        "Partner certification certification criteria must include security, quality, and performance checklist pass criteria.",
        "Partner certification test environment must provide sandbox tenant credentials and staging access.",
        "Partner certification submission artifacts must include screenshots, logs, test results, and evidence package.",
        "Partner certification review owner must assign security and solutions reviewer approvers.",
        "Partner certification version compatibility must list API version v2 and supported SDK minimum versions.",
        "Partner certification security questionnaire must collect SOC 2, ISO, privacy, and vendor responses.",
        "Partner certification remediation loop must track defects, fixes, retest workflow, and issue SLA.",
        "Partner certification renewal cadence must schedule annual recertification renewal.",
        "Partner certification launch approval must require go-live sign-off and marketplace launch gate.",
    ]))

    assert [record.requirement_type for record in result.records] == ["certification_criteria", "test_environment", "submission_artifacts", "review_owner", "version_compatibility", "security_questionnaire", "remediation_loop", "renewal_cadence", "launch_approval"]
    assert all(record.evidence for record in result.records)


def test_partial_brief_flags_certification_criteria_submission_artifacts_and_launch_approval_details():
    result = build_source_partner_certification_requirements("Partner certification certification criteria are required. Partner certification submission artifacts are required. Partner certification launch approval is required.")

    assert result.summary["missing_detail_flags"] == ["missing_certification_criteria", "missing_submission_artifacts", "missing_launch_approval"]


def test_negated_partner_certification_scope_is_ignored():
    assert build_source_partner_certification_requirements("No partner certification changes are required.").records == ()


def _source(lines, source_id="partner-certification"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Partner certification", "summary": "Partner certification planning", "source_payload": {"requirements": lines}, "source_links": {}}
