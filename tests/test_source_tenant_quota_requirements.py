from blueprint.source_tenant_quota_requirements import build_source_tenant_quota_requirements


def test_extracts_all_tenant_quota_categories_with_evidence():
    result = build_source_tenant_quota_requirements(_source([
        "Tenant quota quota dimension must meter users, seats, storage GB, API calls, and projects.",
        "Tenant quota default limit must set a default quota of 100 seats.",
        "Tenant quota plan overrides must define free, pro, enterprise, and custom tier limits.",
        "Tenant quota enforcement behavior must block hard limits and throttle API calls at soft limits.",
        "Tenant quota warning threshold must notify by email at 80 percent and 90 percent usage.",
        "Tenant quota overage workflow must support upgrade, purchase, approval, and sales requests.",
        "Tenant quota admin override must allow support admins to create temporary approved overrides.",
        "Tenant quota telemetry reporting must publish usage telemetry dashboard reports and exports.",
        "Tenant quota migration backfill must backfill existing legacy tenants and recalculate quota usage.",
    ]))

    assert [record.requirement_type for record in result.records] == ["quota_dimension", "default_limit", "plan_overrides", "enforcement_behavior", "warning_threshold", "overage_workflow", "admin_override", "telemetry_reporting", "migration_backfill"]
    assert all(record.evidence for record in result.records)


def test_partial_brief_flags_quota_dimension_enforcement_behavior_and_overage_workflow_details():
    result = build_source_tenant_quota_requirements("Tenant quota quota dimension is required. Tenant quota enforcement behavior is required. Tenant quota overage workflow is required.")

    assert result.summary["missing_detail_flags"] == ["missing_quota_dimension", "missing_enforcement_behavior", "missing_overage_workflow"]


def test_negated_tenant_quota_scope_is_ignored():
    assert build_source_tenant_quota_requirements("No tenant quota changes are required.").records == ()


def _source(lines, source_id="tenant-quota"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Tenant quota", "summary": "Tenant quota planning", "source_payload": {"requirements": lines}, "source_links": {}}
