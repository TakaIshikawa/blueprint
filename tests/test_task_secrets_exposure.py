import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_secrets_exposure import (
    TaskSecretsExposurePlan,
    TaskSecretsExposureRecord,
    build_task_secrets_exposure_plan,
    summarize_task_secrets_exposure,
    task_secrets_exposure_plan_to_dict,
    task_secrets_exposure_plan_to_markdown,
)


def test_secret_related_tasks_are_classified_with_targeted_signals_and_checklist():
    result = build_task_secrets_exposure_plan(
        _plan(
            [
                _task(
                    "task-oauth",
                    title="Provision OAuth client and API keys",
                    description=(
                        "Add OAuth client credentials, rotate API keys, store refresh "
                        "tokens in the vault, and redact token values from logs."
                    ),
                    files_or_modules=[
                        "src/integrations/payments/oauth_client.py",
                        "infra/secrets/payment_credentials.tfvars",
                    ],
                    metadata={"runbook": {"ci": "Add GitHub Actions secrets for deploy token."}},
                ),
                _task(
                    "task-cert",
                    title="Install TLS certificate",
                    description="Load the mTLS client certificate from runtime env vars.",
                    files_or_modules=["certs/client.pem", ".env.production"],
                ),
            ]
        )
    )

    oauth = _record(result, "task-oauth")
    assert oauth.classification == "secrets_review_required"
    assert oauth.detected_signals == (
        "api_keys",
        "tokens",
        "oauth_clients",
        "config_files",
        "ci_cd_secrets",
        "credentials",
        "secret_storage",
        "logging_redaction",
    )
    assert any("description: Add OAuth client credentials" in item for item in oauth.evidence_snippets)
    assert "metadata.runbook.ci: Add GitHub Actions secrets for deploy token." in oauth.evidence_snippets
    assert any(item.startswith("Inventory every secret") for item in oauth.checklist_items)
    assert any("approved secret manager or CI/CD secret store" in item for item in oauth.checklist_items)
    assert any("rotation or revocation" in item for item in oauth.checklist_items)
    assert any("redact or omit secret values" in item for item in oauth.checklist_items)
    assert any("least-privilege" in item for item in oauth.checklist_items)
    assert any("placeholder or sandbox secrets" in item for item in oauth.checklist_items)

    cert = _record(result, "task-cert")
    assert cert.classification == "secrets_review_required"
    assert cert.detected_signals == (
        "certificates",
        "environment_variables",
        "config_files",
    )
    assert result.secrets_review_task_ids == ("task-cert", "task-oauth")
    assert result.no_review_task_ids == ()
    assert result.summary["secrets_review_required_count"] == 2
    assert result.summary["signal_counts"]["certificates"] == 1
    assert result.summary["signal_counts"]["ci_cd_secrets"] == 1


def test_benign_task_has_no_review_needed_classification():
    result = build_task_secrets_exposure_plan(
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

    assert result.records == (
        TaskSecretsExposureRecord(
            task_id="task-cache",
            task_title="Tune cache expiration",
            classification="no_secrets_review_needed",
            detected_signals=(),
            evidence_snippets=(),
            checklist_items=(
                "Confirm no secrets, credentials, tokens, certificates, or environment files are introduced or modified.",
                "Record the validation note with the task evidence before handoff.",
            ),
        ),
    )
    assert result.secrets_review_task_ids == ()
    assert result.no_review_task_ids == ("task-cache",)
    assert result.summary["classification_counts"] == {
        "secrets_review_required": 0,
        "no_secrets_review_needed": 1,
    }


def test_model_input_serializes_stably_and_renders_markdown():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-ci",
                    title="Configure CI secrets",
                    description="Move deployment token to CI secrets and mask it in build logs.",
                    files_or_modules=[".github/workflows/deploy-secrets.yml"],
                    acceptance_criteria=["Pipeline deploys with repository secrets."],
                )
            ]
        )
    )

    result = summarize_task_secrets_exposure(plan)
    payload = task_secrets_exposure_plan_to_dict(result)

    assert isinstance(result, TaskSecretsExposurePlan)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert list(payload) == [
        "plan_id",
        "records",
        "secrets_review_task_ids",
        "no_review_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "task_title",
        "classification",
        "detected_signals",
        "evidence_snippets",
        "checklist_items",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert task_secrets_exposure_plan_to_markdown(result) == "\n".join(
        [
            "# Task Secrets Exposure Plan: plan-secrets",
            "",
            "| Task | Classification | Signals | Evidence | Checklist |",
            "| --- | --- | --- | --- | --- |",
            "| task-ci | secrets_review_required | tokens, config_files, ci_cd_secrets, credentials, secret_storage, logging_redaction | description: Move deployment token to CI secrets and mask it in build logs.; files_or_modules: .github/workflows/deploy-secrets.yml; title: Configure CI secrets; acceptance_criteria[0]: Pipeline deploys with repository secrets. | Inventory every secret, credential, token, certificate, environment variable, and config file touched by the task.; Store secrets only in the approved secret manager or CI/CD secret store; do not commit plaintext values.; Define rotation or revocation steps for new, changed, migrated, or potentially exposed secrets.; Verify logs, errors, telemetry, screenshots, and test output redact or omit secret values.; Restrict read/write access using least-privilege roles, scoped tokens, and environment-specific permissions.; Validate the integration with placeholder or sandbox secrets and document production verification ownership.; Confirm CI/CD secrets are scoped by repository, environment, branch, and deployment role. |",
        ]
    )
    assert task_secrets_exposure_plan_to_markdown(result) == result.to_markdown()


def test_plain_task_iterable_and_single_task_mapping_are_supported():
    iterable_result = build_task_secrets_exposure_plan(
        [
            _task(
                "task-env",
                title="Add .env example",
                description="Document environment variables without real credentials.",
                files_or_modules=["docs/.env.example"],
            )
        ]
    )
    mapping_result = build_task_secrets_exposure_plan(
        _task(
            "task-token",
            title="Rotate API token",
            description="Rotate the partner API token.",
        )
    )

    assert iterable_result.plan_id is None
    assert iterable_result.records[0].classification == "secrets_review_required"
    assert mapping_result.records[0].detected_signals == ("tokens",)


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks):
    return {
        "id": "plan-secrets",
        "implementation_brief_id": "brief-secrets",
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
