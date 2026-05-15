from blueprint.source_usage_anomaly_requirements import build_source_usage_anomaly_requirements


def test_extracts_all_usage_anomaly_categories_with_evidence():
    result = build_source_usage_anomaly_requirements(_source([
        "Usage anomaly baseline window must compare against a rolling 30 day historical baseline.",
        "Usage anomaly anomaly threshold must alert on a 40 percent deviation above or below normal.",
        "Usage anomaly monitored metric must watch API calls, sessions, and active users.",
        "Usage anomaly account dimension must evaluate every account, tenant, and workspace.",
        "Usage anomaly alert destination must route Slack, email, and webhook alerts.",
        "Usage anomaly suppression rules must mute maintenance windows and dedupe cooldowns.",
        "Usage anomaly investigation workflow must create analyst triage tasks and runbook playbooks.",
        "Usage anomaly reporting retention must retain audit reports and dashboard exports for 180 days.",
    ]))

    assert [record.requirement_type for record in result.records] == ["baseline_window", "anomaly_threshold", "monitored_metric", "entity_dimension", "alert_destination", "suppression_rules", "investigation_workflow", "reporting_retention"]
    assert all(record.evidence for record in result.records)


def test_partial_brief_flags_baseline_threshold_and_investigation_workflow_details():
    result = build_source_usage_anomaly_requirements("Usage anomaly baseline window is required. Usage anomaly anomaly threshold is required. Usage anomaly investigation workflow is required.")

    assert result.summary["missing_detail_flags"] == ["missing_baseline_window", "missing_threshold", "missing_investigation_workflow"]


def test_negated_usage_anomaly_scope_is_ignored():
    assert build_source_usage_anomaly_requirements("No usage anomaly detection changes are required.").records == ()


def _source(lines, source_id="usage-anomaly"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Usage anomaly detection", "summary": "Usage anomaly detection planning", "source_payload": {"requirements": lines}, "source_links": {}}
