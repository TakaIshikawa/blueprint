from blueprint.source_trial_expiration_requirements import build_source_trial_expiration_requirements


def test_extracts_all_trial_expiration_categories_with_evidence():
    result = build_source_trial_expiration_requirements(_source([
        "Trial expiration expiration source must use billing subscription trial end date as source of truth.",
        "Trial expiration grace period must allow a 7 day extension window.",
        "Trial expiration notification cadence must send email and in-app reminders 7 days and 1 day before expiry.",
        "Trial expiration conversion CTA must show upgrade, checkout, and contact sales buttons.",
        "Trial expiration data retention must retain trial data for 90 days before purge.",
        "Trial expiration feature lock behavior must switch expired accounts to read-only locked access.",
        "Trial expiration support exception must allow support agents to approve admin override extensions.",
        "Trial expiration analytics reporting must publish conversion funnel dashboard metrics.",
    ]))

    assert [record.requirement_type for record in result.records] == ["expiration_source", "grace_period", "notification_cadence", "conversion_cta", "data_retention", "feature_lock_behavior", "support_exception", "analytics_reporting"]
    assert all(record.evidence for record in result.records)


def test_partial_brief_flags_expiration_source_notification_cadence_and_feature_lock_behavior_details():
    result = build_source_trial_expiration_requirements("Trial expiration expiration source is required. Trial expiration notification cadence is required. Trial expiration feature lock behavior is required.")

    assert result.summary["missing_detail_flags"] == ["missing_expiration_source", "missing_notification_cadence", "missing_feature_lock_behavior"]


def test_negated_trial_expiration_scope_is_ignored():
    assert build_source_trial_expiration_requirements("No trial expiration changes are required.").records == ()


def _source(lines, source_id="trial-expiration"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Trial expiration", "summary": "Trial expiration planning", "source_payload": {"requirements": lines}, "source_links": {}}
