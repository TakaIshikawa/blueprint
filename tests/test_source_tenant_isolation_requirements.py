import copy
import json

from blueprint.domain.models import ImplementationBrief
from blueprint.source_tenant_isolation_requirements import (
    SourceTenantIsolationRequirement,
    SourceTenantIsolationRequirementsReport,
    build_source_tenant_isolation_requirements,
    derive_source_tenant_isolation_requirements,
    extract_source_tenant_isolation_requirements,
    generate_source_tenant_isolation_requirements,
    source_tenant_isolation_requirements_to_dict,
    source_tenant_isolation_requirements_to_dicts,
    source_tenant_isolation_requirements_to_markdown,
    summarize_source_tenant_isolation_requirements,
)


def test_natural_language_extracts_tenant_isolation_categories():
    result = build_source_tenant_isolation_requirements(
        _source_brief(
            summary=(
                "Tenant isolation must prevent cross-tenant access. "
                "Organization membership must be checked before workspace access. "
                "Audit logs must record tenant context for every admin override."
            ),
            source_payload={
                "requirements": [
                    "Row level security must enforce tenant_id predicates on customer records.",
                    "Global search must filter results by tenant boundary.",
                    "Shared cache keys need a namespace per tenant.",
                    "Tenant migration batches must not mix organizations.",
                ]
            },
        )
    )

    assert isinstance(result, SourceTenantIsolationRequirementsReport)
    assert result.source_id == "tenant-src"
    assert result.source_title == "Tenant isolation requirements"
    assert all(isinstance(record, SourceTenantIsolationRequirement) for record in result.records)
    assert [record.category for record in result.requirements] == [
        "tenant_boundary",
        "row_level_security",
        "organization_membership",
        "cross_tenant_search",
        "shared_resource_isolation",
        "admin_override",
        "audit_scope",
        "migration_isolation",
    ]
    by_category = {record.category: record for record in result.requirements}
    assert by_category["tenant_boundary"].confidence == "high"
    assert by_category["row_level_security"].confidence == "high"
    assert by_category["row_level_security"].suggested_owner == "data"
    assert "tenant_id predicates" in by_category["row_level_security"].matched_terms
    assert any(
        path == "source_payload.requirements[0]"
        for path in by_category["row_level_security"].source_field_paths
    )
    assert "tenant-aware indexing" in by_category["cross_tenant_search"].suggested_planning_note
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"]["admin_override"] == 1
    assert result.summary["confidence_counts"]["high"] >= 6
    assert result.summary["source_id"] == "tenant-src"
    assert result.summary["source_title"] == "Tenant isolation requirements"


def test_file_like_paths_and_nested_metadata_are_scanned():
    result = extract_source_tenant_isolation_requirements(
        {
            "id": "paths-only",
            "title": "Path signals",
            "summary": "Tenant isolation review.",
            "source_payload": {
                "files": [
                    "src/db/tenant_rls_policies.sql",
                    "src/search/tenant_index_filters.py",
                    "ops/migrations/per_tenant_backfill_runner.py",
                ],
                "metadata": {
                    "shared_resource_isolation": {
                        "queues": "Shared queues should use prefix per tenant."
                    },
                    "audit_scope": "Audit events require workspace id in audit payloads.",
                },
            },
        }
    )

    by_category = {record.category: record for record in result.records}

    assert [record.category for record in result.records] == [
        "row_level_security",
        "cross_tenant_search",
        "shared_resource_isolation",
        "audit_scope",
        "migration_isolation",
    ]
    assert "source_payload.files[0]" in by_category["row_level_security"].source_field_paths
    assert "source_payload.files[1]" in by_category["cross_tenant_search"].source_field_paths
    assert by_category["shared_resource_isolation"].confidence == "high"
    assert any("prefix per tenant" in term for term in by_category["shared_resource_isolation"].matched_terms)
    assert any(
        "source_payload.metadata.audit_scope" in evidence
        for evidence in by_category["audit_scope"].evidence
    )


def test_implementation_brief_and_mapping_outputs_match_without_mutation():
    source = _implementation_brief(
        scope=[
            "Workspace membership must be checked before reading tenant records.",
            "Support impersonation requires admin override audit logs with tenant id.",
        ],
        data_requirements=(
            "RLS must enforce tenant_id predicates and tenant migration must run per organization."
        ),
    )
    original = copy.deepcopy(source)
    model = ImplementationBrief.model_validate(source)

    mapping_result = build_source_tenant_isolation_requirements(source)
    model_result = generate_source_tenant_isolation_requirements(model)
    derived_result = derive_source_tenant_isolation_requirements(model)
    payload = source_tenant_isolation_requirements_to_dict(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert derived_result.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert source_tenant_isolation_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_tenant_isolation_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_tenant_isolation_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "source_title", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "category",
        "evidence",
        "source_field_paths",
        "matched_terms",
        "confidence",
        "suggested_owner",
        "suggested_planning_note",
    ]


def test_confidence_is_higher_when_requirement_language_and_context_cooccur():
    result = build_source_tenant_isolation_requirements(
        _source_brief(
            summary="Review tenant boundaries for the reporting dashboard.",
            source_payload={
                "notes": [
                    "Tenant boundary.",
                    "Tenant boundary must prevent cross-tenant access to invoices.",
                ]
            },
        )
    )

    tenant_boundary = next(
        record for record in result.requirements if record.category == "tenant_boundary"
    )

    assert tenant_boundary.confidence == "high"
    assert any("must" in term for term in tenant_boundary.matched_terms)


def test_negated_and_out_of_scope_tenant_wording_does_not_create_requirements():
    result = build_source_tenant_isolation_requirements(
        _source_brief(
            title="Single tenant dashboard",
            summary="No tenant isolation or cross-tenant search changes are in scope.",
            source_payload={
                "non_goals": [
                    "Tenant migration support is out of scope.",
                    "No row level security requirements are needed.",
                ],
                "body": "Improve dashboard copy for a single tenant deployment.",
            },
        )
    )

    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "source_count": 1,
        "source_id": "tenant-src",
        "source_title": "Single tenant dashboard",
        "requirement_count": 0,
        "category_counts": {
            "tenant_boundary": 0,
            "row_level_security": 0,
            "organization_membership": 0,
            "cross_tenant_search": 0,
            "shared_resource_isolation": 0,
            "admin_override": 0,
            "audit_scope": 0,
            "migration_isolation": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
    }
    assert "No source tenant isolation requirements were inferred" in result.to_markdown()


def test_markdown_helper_and_multiple_sources_are_stable():
    result = build_source_tenant_isolation_requirements(
        [
            _source_brief(
                source_id="tenant-b",
                title="Search isolation",
                summary="Search results must respect tenant boundary.",
            ),
            _source_brief(
                source_id="tenant-a",
                title="Membership isolation",
                summary="Organization membership must be checked before account access.",
            ),
        ]
    )
    markdown = source_tenant_isolation_requirements_to_markdown(result)

    assert [record.category for record in result.records] == [
        "tenant_boundary",
        "organization_membership",
        "cross_tenant_search",
    ]
    assert result.source_id is None
    assert result.source_title is None
    assert result.summary["source_count"] == 2
    assert markdown.startswith("# Source Tenant Isolation Requirements Report")
    assert "| Category | Confidence | Owner | Source Fields | Matched Terms | Evidence | Planning Note |" in markdown


def test_plain_text_input_is_supported():
    result = build_source_tenant_isolation_requirements(
        "Tenant audit events must include organization id and tenant id."
    )

    assert [(record.category, record.confidence) for record in result.records] == [
        ("tenant_boundary", "high"),
        ("audit_scope", "high"),
    ]


def _source_brief(
    *,
    source_id="tenant-src",
    title="Tenant isolation requirements",
    domain="security",
    summary="General tenant isolation requirements.",
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
    source_id="impl-tenant",
    title="Tenant implementation",
    summary="Tenant implementation requirements.",
    scope=None,
    data_requirements=None,
):
    return {
        "id": source_id,
        "source_brief_id": source_id,
        "title": title,
        "domain": "security",
        "target_user": None,
        "buyer": None,
        "workflow_context": None,
        "problem_statement": summary,
        "mvp_goal": "Implement tenant isolation safely.",
        "product_surface": None,
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": data_requirements,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Validate tenant isolation behavior.",
        "definition_of_done": [],
    }
