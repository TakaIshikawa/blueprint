import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_sandbox_environment_readiness import (
    TaskSandboxEnvironmentReadinessPlan,
    analyze_task_sandbox_environment_readiness,
    build_task_sandbox_environment_readiness_plan,
    recommend_task_sandbox_environment_readiness,
    summarize_task_sandbox_environment_readiness,
    summarize_task_sandbox_environment_readiness_plan,
    task_sandbox_environment_readiness_plan_to_dict,
    task_sandbox_environment_readiness_plan_to_dicts,
    task_sandbox_environment_readiness_plan_to_markdown,
)


def test_complete_sandbox_environment_task_is_ready():
    result = build_task_sandbox_environment_readiness_plan(
        _plan(
            [
                _task(
                    "sandbox-ready",
                    title="Provision tenant sandbox environment",
                    description="Create a tenant sandbox with reset, seed data, isolation, credentials, webhook test endpoints, and maintenance automation.",
                    acceptance_criteria=[
                        "Environment boundaries keep production separation with network boundary and allowed services documented.",
                        "Data isolation uses tenant isolation, a separate database, masked data, and no production data.",
                        "Seed fixture strategy includes fixtures, baseline data, sample data, and test data.",
                        "Credential handling stores sandbox secrets and API keys in the secrets manager with rotated env vars.",
                        "Sandbox reset cleanup behavior supports scheduled reset, cleanup, teardown, rebuild, and TTL expiration.",
                        "Platform team owner is the DRI, approver, and accountable environment owner.",
                        "Validation checks include smoke tests, health checks, provisioning tests, integration tests, and post-reset checks.",
                    ],
                    files_or_modules=["infra/sandbox/provision_environment.tf"],
                ),
                _task("copy", title="Update copy", description="Refresh settings copy."),
            ]
        )
    )

    assert isinstance(result, TaskSandboxEnvironmentReadinessPlan)
    assert result.impacted_task_ids == ("sandbox-ready",)
    assert result.ignored_task_ids == ("copy",)
    record = result.records[0]
    assert record.detected_signals == (
        "sandbox_environment",
        "sandbox_creation",
        "sandbox_reset",
        "sandbox_seed",
        "sandbox_isolation",
        "sandbox_credentials",
        "webhook_test_endpoints",
        "tenant_sandbox",
    )
    assert record.present_criteria == (
        "environment_boundaries",
        "data_isolation",
        "seed_fixture_strategy",
        "credential_handling",
        "reset_cleanup_behavior",
        "owner",
        "validation_checks",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_partial_sandbox_environment_task_reports_stable_follow_up_actions():
    result = analyze_task_sandbox_environment_readiness(
        [
            _task(
                "sandbox-partial",
                title="Reset sandbox environment",
                description="Reset sandbox data and credentials for test endpoints.",
            )
        ]
    )

    record = result.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("credential_handling", "reset_cleanup_behavior")
    assert record.missing_criteria == (
        "environment_boundaries",
        "data_isolation",
        "seed_fixture_strategy",
        "owner",
        "validation_checks",
    )
    assert record.recommended_follow_up_actions == (
        "Define environment boundaries, including production separation, allowed services, network boundaries, ingress, egress, or non-production constraints.",
        "Document data isolation with tenant isolation, separate databases or schemas, masked data, synthetic data, scrubbed data, or no production data.",
        "Add a seed or fixture strategy using seed data, fixtures, baseline data, sample data, known datasets, or test data.",
        "Name the owner, DRI, responsible team, maintainer, platform team, developer experience team, environment owner, approver, or accountable party.",
        "Add validation checks such as smoke tests, health checks, provisioning tests, integration tests, contract tests, pytest, or post-reset checks.",
    )


def test_sandbox_paths_and_nested_metadata_contribute_evidence_without_mutation():
    source = _plan(
        [
            _task(
                "sandbox-paths",
                title="Sandbox templates",
                description="Maintain customer sandbox templates.",
                files_or_modules=[
                    "infra/sandbox_environment/provision.yml",
                    "scripts/sandbox_reset_cleanup.py",
                    "tests/fixtures/sandbox_seed_data.yml",
                    "config/webhook_test_endpoint.yml",
                    "secrets/sandbox_credentials.env",
                ],
                metadata={
                    "sandbox": {
                        "isolation": "Network boundary and tenant isolation prevent production access.",
                        "owner": "Developer experience team owner validates health checks after provisioning.",
                    }
                },
            )
        ]
    )
    original = copy.deepcopy(source)

    result = build_task_sandbox_environment_readiness_plan(ExecutionPlan.model_validate(source))

    assert source == original
    record = result.records[0]
    assert record.detected_signals == (
        "sandbox_environment",
        "sandbox_creation",
        "sandbox_reset",
        "sandbox_seed",
        "sandbox_isolation",
        "sandbox_credentials",
        "webhook_test_endpoints",
        "tenant_sandbox",
    )
    assert record.present_criteria == (
        "environment_boundaries",
        "data_isolation",
        "seed_fixture_strategy",
        "credential_handling",
        "reset_cleanup_behavior",
        "owner",
        "validation_checks",
    )
    assert record.missing_criteria == ()
    assert any("metadata.sandbox.isolation" in item for item in record.evidence)
    assert any("files_or_modules: infra/sandbox_environment/provision.yml" in item for item in record.evidence)
    assert any("files_or_modules[1]: scripts/sandbox_reset_cleanup.py" in item for item in record.evidence)
    assert any("files_or_modules[2]: tests/fixtures/sandbox_seed_data.yml" in item for item in record.evidence)
    assert any("files_or_modules[3]: config/webhook_test_endpoint.yml" in item for item in record.evidence)
    assert any("files_or_modules[4]: secrets/sandbox_credentials.env" in item for item in record.evidence)


def test_no_impact_and_conversion_helpers_are_stable():
    result = summarize_task_sandbox_environment_readiness(
        _plan(
            [
                _task(
                    "sandbox-noop",
                    title="Docs refresh",
                    description="No sandbox environment changes are required for this documentation update.",
                ),
                _task("sandbox-partial", title="Create sandbox", description="Create a developer sandbox."),
            ],
            plan_id="plan-sandbox-environment-sort",
        )
    )

    payload = task_sandbox_environment_readiness_plan_to_dict(result)
    markdown = task_sandbox_environment_readiness_plan_to_markdown(result)

    assert [record.task_id for record in result.records] == ["sandbox-partial"]
    assert result.ignored_task_ids == ("sandbox-noop",)
    assert analyze_task_sandbox_environment_readiness(result) is result
    assert summarize_task_sandbox_environment_readiness_plan(result) is result
    assert recommend_task_sandbox_environment_readiness(result) == result.records
    assert task_sandbox_environment_readiness_plan_to_dicts(result) == payload["records"]
    assert task_sandbox_environment_readiness_plan_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-sandbox-environment-sort"
    assert markdown.startswith("# Task Sandbox Environment Readiness: plan-sandbox-environment-sort")
    assert "| Task | Title | Readiness |" in markdown


def test_invalid_inputs_return_empty_simple_plan():
    assert build_task_sandbox_environment_readiness_plan(42).records == ()
    assert build_task_sandbox_environment_readiness_plan({"id": "bad", "tasks": "not a list"}).records == ()
    assert build_task_sandbox_environment_readiness_plan({"tasks": []}).summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-sandbox-environment"):
    return {"id": plan_id, "implementation_brief_id": "brief-sandbox-environment", "milestones": [], "tasks": tasks}


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
