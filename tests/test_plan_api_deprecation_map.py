import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_api_deprecation_map import (
    PlanApiDeprecationMap,
    PlanApiDeprecationMapRecord,
    build_plan_api_deprecation_map,
    plan_api_deprecation_map_to_dict,
    plan_api_deprecation_map_to_markdown,
    summarize_plan_api_deprecations,
)


def test_deprecated_endpoints_version_bumps_sdk_upgrades_and_cleanup_are_mapped():
    result = build_plan_api_deprecation_map(
        _plan(
            [
                _task(
                    "task-producer",
                    title="Deprecate v1 invoice endpoint",
                    description=(
                        "Mark the public REST endpoint deprecated while v2 remains backward compatible."
                    ),
                    files_or_modules=["src/api/v1/invoices_endpoint.py"],
                    acceptance_criteria=[
                        "Compatibility tests cover v1 and v2 responses.",
                        "Rollout notes include customer communication.",
                    ],
                ),
                _task(
                    "task-sdk",
                    title="SDK upgrade for billing clients",
                    description="Upgrade the SDK consumers to call API version v2 with fallback handling.",
                    files_or_modules=["src/integrations/billing/client.py"],
                    metadata={"release_notes": "Notify downstream consumers before the SDK upgrade."},
                ),
                _task(
                    "task-cleanup",
                    title="Remove legacy contract",
                    description="Delete the obsolete GraphQL schema after the sunset window.",
                    files_or_modules=["schemas/legacy_invoice_contract.graphql"],
                    metadata={"migration": {"window": "EOL after partners complete migration"}},
                ),
                _task(
                    "task-cache",
                    title="Tune cache expiration",
                    description="Adjust cache TTL for report queries.",
                    files_or_modules=["src/cache/report_cache.py"],
                ),
            ]
        )
    )

    assert result.impacted_task_ids == ("task-cleanup", "task-producer", "task-sdk")
    assert _record(result, "task-producer").impact_category == "cleanup"
    assert _record(result, "task-sdk").impact_category == "migration"
    assert _record(result, "task-cleanup").impact_category == "cleanup"
    assert "endpoint" in _record(result, "task-producer").affected_surfaces
    assert "sdk" in _record(result, "task-sdk").affected_surfaces
    assert "schema" in _record(result, "task-cleanup").affected_surfaces
    assert any(
        check.startswith("Add compatibility tests covering old and new API behavior")
        for check in _record(result, "task-producer").mitigation_checklist
    )
    assert any(
        check.startswith("Record rollout notes")
        for check in _record(result, "task-sdk").mitigation_checklist
    )
    assert any(
        check.startswith("Prepare communication")
        for check in _record(result, "task-sdk").mitigation_checklist
    )
    assert any(
        note.startswith("Confirm legacy contract removal")
        for note in _record(result, "task-cleanup").migration_notes
    )
    assert result.summary["category_counts"] == {
        "producer": 0,
        "consumer": 0,
        "migration": 1,
        "cleanup": 2,
    }


def test_metadata_acceptance_criteria_and_files_are_used_as_evidence():
    result = build_plan_api_deprecation_map(
        _plan(
            [
                _task(
                    "task-webhook",
                    title="Partner webhook compatibility",
                    description="Keep partner webhook handlers backward compatible.",
                    files_or_modules=["src/api/webhooks/partner_v2_handler.py"],
                    acceptance_criteria=[
                        "Compatibility suite verifies old and new webhook payload contracts."
                    ],
                    metadata={
                        "api": {
                            "deprecation": "Legacy payload shape remains supported during migration."
                        }
                    },
                )
            ]
        )
    )

    record = result.records[0]

    assert record.impact_category == "cleanup"
    assert record.affected_surfaces == ("endpoint", "webhook", "compatibility", "api_version:v2")
    assert "metadata.api.deprecation: Legacy payload shape remains supported during migration." in record.evidence
    assert any(
        check == "Verify producer responses, schemas, status codes, and version negotiation remain compatible."
        for check in record.mitigation_checklist
    )


def test_no_deprecation_signals_returns_empty_map_and_stable_summary():
    result = build_plan_api_deprecation_map(
        _plan(
            [
                _task(
                    "task-cache",
                    title="Tune cache expiration",
                    description="Adjust backend cache TTL for expensive report queries.",
                    files_or_modules=["src/cache/report_cache.py"],
                    acceptance_criteria=["Unit tests cover cache refresh behavior."],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.summary == {
        "record_count": 0,
        "impacted_task_count": 0,
        "category_counts": {
            "producer": 0,
            "consumer": 0,
            "migration": 0,
            "cleanup": 0,
        },
        "surface_counts": {},
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Plan API Deprecation Impact Map: plan-api-deprecation",
            "",
            "No API deprecation impacts were inferred.",
        ]
    )


def test_model_input_serializes_stably_and_renders_markdown():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-contract",
                    title="API version bump for account schema",
                    description="Bump API version to v3 and migrate consumers from the legacy contract.",
                    files_or_modules=["src/api/v3/account_schema.json"],
                    acceptance_criteria=["Compatibility tests and rollout notes are complete."],
                )
            ]
        )
    )

    result = summarize_plan_api_deprecations(plan)
    payload = plan_api_deprecation_map_to_dict(result)

    assert isinstance(result, PlanApiDeprecationMap)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert list(payload) == ["plan_id", "records", "impacted_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "task_title",
        "impact_category",
        "affected_surfaces",
        "mitigation_checklist",
        "migration_notes",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert PlanApiDeprecationMapRecord(
        task_id="task-contract",
        task_title="API version bump for account schema",
        impact_category="cleanup",
        affected_surfaces=("endpoint", "schema", "compatibility", "api_version:v3"),
        mitigation_checklist=(
            "Add compatibility tests covering old and new API behavior.",
            "Record rollout notes with sequencing, flags, rollback, and monitoring expectations.",
            "Verify producer responses, schemas, status codes, and version negotiation remain compatible.",
            "Verify consumers, SDK callers, integrations, and downstream clients handle both contracts.",
            "Document migration steps, version support window, and validation evidence.",
            "Gate legacy removal on adoption data, deprecation timeline, and rollback readiness.",
            "Prepare communication for affected owners, consumers, support, and release notes.",
        ),
        migration_notes=(
            "Record the source and target API versions plus the migration sequence.",
            "Confirm legacy contract removal is gated by adoption evidence and rollback expectations.",
            "Keep compatibility expectations explicit for mixed-version producers and consumers.",
        ),
        evidence=(
            "files_or_modules: src/api/v3/account_schema.json",
            "title: API version bump for account schema",
            "description: Bump API version to v3 and migrate consumers from the legacy contract.",
            "acceptance_criteria[0]: Compatibility tests and rollout notes are complete.",
        ),
    ) in result.records
    assert plan_api_deprecation_map_to_markdown(result) == "\n".join(
        [
            "# Plan API Deprecation Impact Map: plan-api-deprecation",
            "",
            "| Task | Impact | Affected Surfaces | Mitigation Checklist | Migration Notes |",
            "| --- | --- | --- | --- | --- |",
            "| task-contract | cleanup | endpoint, schema, compatibility, api_version:v3 | "
            "Add compatibility tests covering old and new API behavior.; "
            "Record rollout notes with sequencing, flags, rollback, and monitoring expectations.; "
            "Verify producer responses, schemas, status codes, and version negotiation remain compatible.; "
            "Verify consumers, SDK callers, integrations, and downstream clients handle both contracts.; "
            "Document migration steps, version support window, and validation evidence.; "
            "Gate legacy removal on adoption data, deprecation timeline, and rollback readiness.; "
            "Prepare communication for affected owners, consumers, support, and release notes. | "
            "Record the source and target API versions plus the migration sequence.; "
            "Confirm legacy contract removal is gated by adoption evidence and rollback expectations.; "
            "Keep compatibility expectations explicit for mixed-version producers and consumers. |",
        ]
    )
    assert plan_api_deprecation_map_to_markdown(result) == result.to_markdown()


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks):
    return {
        "id": "plan-api-deprecation",
        "implementation_brief_id": "brief-api-deprecation",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    tags=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": (
            acceptance_criteria if acceptance_criteria is not None else ["Done"]
        ),
        "status": "pending",
        "metadata": metadata or {},
    }
    if tags is not None:
        task["tags"] = tags
    return task
