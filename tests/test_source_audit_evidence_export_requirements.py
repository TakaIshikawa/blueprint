from blueprint.source_audit_evidence_export_requirements import build_source_audit_evidence_export_requirements


def test_extracts_all_audit_evidence_export_categories_with_evidence():
    result = build_source_audit_evidence_export_requirements(_source([
        "Audit evidence export evidence scope must include logs, controls, tickets, policies, and documents.",
        "Audit evidence export export format must package PDF, CSV, JSON, and ZIP files.",
        "Audit evidence export requester role must allow auditor, admin, and compliance roles.",
        "Audit evidence export approval gate must require manager and compliance review before export.",
        "Audit evidence export redaction policy must mask PII, secrets, and sensitive tokens.",
        "Audit evidence export retention period must retain exports for 365 days before expiry.",
        "Audit evidence export chain-of-custody metadata must include hash, timestamp, actor, and signature.",
        "Audit evidence export delivery channel must provide secure download link and SFTP delivery.",
        "Audit evidence export access logging must log actor, timestamp, IP, and download access.",
    ]))

    assert [record.requirement_type for record in result.records] == ["evidence_scope", "export_format", "requester_role", "approval_gate", "redaction_policy", "retention_period", "chain_of_custody", "delivery_channel", "access_logging"]
    assert all(record.evidence for record in result.records)


def test_partial_brief_flags_evidence_scope_redaction_policy_and_access_logging_details():
    result = build_source_audit_evidence_export_requirements("Audit evidence export evidence scope is required. Audit evidence export redaction policy is required. Audit evidence export access logging is required.")

    assert result.summary["missing_detail_flags"] == ["missing_evidence_scope", "missing_redaction_policy", "missing_access_logging"]


def test_negated_audit_evidence_export_scope_is_ignored():
    assert build_source_audit_evidence_export_requirements("No audit evidence export changes are required.").records == ()


def _source(lines, source_id="audit-evidence-export"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Audit evidence export", "summary": "Audit evidence export planning", "source_payload": {"requirements": lines}, "source_links": {}}
