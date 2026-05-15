import json

from blueprint.source_entitlement_migration_requirements import (
    build_source_entitlement_migration_requirements,
    derive_source_entitlement_migration_requirements,
    extract_source_entitlement_migration_requirements,
    generate_source_entitlement_migration_requirements,
    source_entitlement_migration_requirements_to_dict,
    source_entitlement_migration_requirements_to_dicts,
    source_entitlement_migration_requirements_to_markdown,
    summarize_source_entitlement_migration_requirements,
)


def test_extracts_all_entitlement_migration_categories_with_evidence():
    result = build_source_entitlement_migration_requirements(_source([
        "Entitlement migration entitlement inventory must list current legacy plan, feature, permission, seat, and SKU grants.",
        "Entitlement migration migration mapping must define source-to-target rules from legacy SKUs to new plan tiers.",
        "Entitlement migration grandfathered accounts must protect legacy accounts until renewal expiration.",
        "Entitlement migration downgrade behavior must use grace periods and read-only access before revoke notifications.",
        "Entitlement migration override policy must require admin approval, reason, expiration, and audit evidence.",
        "Entitlement migration validation/backfill must run a backfill job, reconcile counts, diff results, and validate migrated entitlements.",
        "Entitlement migration rollout phases must use pilot, canary, wave, and GA rollout phases by cohort.",
        "Entitlement migration audit trail must log actor, timestamp, before and after entitlement state, and reason.",
        "Entitlement migration support remediation must provide support tickets, playbook escalation, and correction rollback.",
    ]))

    assert [record.requirement_type for record in result.records] == ["entitlement_inventory", "migration_mapping", "grandfathered_accounts", "downgrade_behavior", "override_policy", "validation_backfill", "rollout_phases", "audit_trail", "support_remediation"]
    assert all(record.evidence for record in result.records)
    assert result.summary["missing_detail_flags"] == []


def test_partial_brief_flags_migration_mapping_validation_backfill_and_support_remediation_details():
    result = derive_source_entitlement_migration_requirements("Entitlement migration migration mapping is required. Entitlement migration validation/backfill is required. Entitlement migration support remediation is required.")

    assert result.summary["missing_detail_flags"] == ["missing_migration_mapping", "missing_validation_backfill", "missing_support_remediation"]


def test_serializers_aliases_and_negated_scope_are_deterministic():
    result = extract_source_entitlement_migration_requirements(_source(["Entitlement migration migration mapping must define source-to-target rules from legacy SKUs to new plan tiers."], "entitlement-1"))
    payload = source_entitlement_migration_requirements_to_dict(result)

    assert generate_source_entitlement_migration_requirements("Entitlement migration audit trail must log actor and timestamp.").summary["requirement_count"] == 1
    assert summarize_source_entitlement_migration_requirements(result)["requirement_count"] == 1
    assert json.loads(json.dumps(payload))["source_id"] == "entitlement-1"
    assert source_entitlement_migration_requirements_to_dicts(result) == payload["records"]
    assert source_entitlement_migration_requirements_to_dicts(result.records) == payload["records"]
    assert "# Source Entitlement Migration Requirements Report: entitlement-1" in source_entitlement_migration_requirements_to_markdown(result)
    assert build_source_entitlement_migration_requirements("No entitlement migration planning changes are required.").records == ()


def _source(lines, source_id="entitlement-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Entitlement migration planning", "summary": "Entitlement migration planning", "source_payload": {"requirements": lines}, "source_links": {}}
