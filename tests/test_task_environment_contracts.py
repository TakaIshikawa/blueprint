import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_environment_contracts import (
    TaskEnvironmentContract,
    TaskEnvironmentEvidence,
    build_task_environment_contracts,
    task_environment_contracts_to_dict,
)


def test_builds_contract_from_task_fields_with_stable_fallback_id():
    contracts = build_task_environment_contracts(
        _plan(
            [
                {
                    "title": "Wire local API",
                    "description": (
                        "Read API_KEY and BLUEPRINT_DB_PATH from .env.local before "
                        "calling redis on localhost:6379."
                    ),
                    "files_or_modules": ["config/app.yml", "src/service.py"],
                    "acceptance_criteria": [
                        "Runs against postgres service on 127.0.0.1:5432",
                        "Optional DEBUG_FLAG may be set for verbose logs",
                    ],
                    "test_command": "API_KEY=test poetry run pytest tests/test_api.py",
                    "metadata": {},
                }
            ]
        )
    )

    contract = contracts["task-1"]

    assert contract == TaskEnvironmentContract(
        task_id="task-1",
        required_env_vars=("API_KEY", "BLUEPRINT_DB_PATH"),
        optional_env_vars=("DEBUG_FLAG",),
        config_paths=(".env.local", "config/app.yml"),
        services=("postgres", "redis"),
        ports=(5432, 6379),
        evidence=(
            TaskEnvironmentEvidence(
                signal_type="config_path",
                value=".env.local",
                field="description",
            ),
            TaskEnvironmentEvidence(
                signal_type="config_path",
                value="config/app.yml",
                field="files_or_modules",
            ),
            TaskEnvironmentEvidence(
                signal_type="optional_env_var",
                value="DEBUG_FLAG",
                field="acceptance_criteria",
            ),
            TaskEnvironmentEvidence(
                signal_type="port",
                value=5432,
                field="acceptance_criteria",
            ),
            TaskEnvironmentEvidence(
                signal_type="port",
                value=6379,
                field="description",
            ),
            TaskEnvironmentEvidence(
                signal_type="required_env_var",
                value="API_KEY",
                field="description",
            ),
            TaskEnvironmentEvidence(
                signal_type="required_env_var",
                value="API_KEY",
                field="test_command",
            ),
            TaskEnvironmentEvidence(
                signal_type="required_env_var",
                value="BLUEPRINT_DB_PATH",
                field="description",
            ),
            TaskEnvironmentEvidence(
                signal_type="service",
                value="postgres",
                field="acceptance_criteria",
            ),
            TaskEnvironmentEvidence(
                signal_type="service",
                value="redis",
                field="description",
            ),
        ),
        missing_contract_warnings=(),
    )


def test_nested_metadata_contributes_structured_signals_and_evidence_paths():
    contracts = build_task_environment_contracts(
        _plan(
            [
                _task(
                    "task-metadata",
                    metadata={
                        "runtime": {
                            "env": {
                                "BLUEPRINT_DB_PATH": "required",
                                "TRACE_SAMPLE_RATE": {"required": False},
                            },
                            "optional_env_vars": ["EXPERIMENT_FLAG"],
                            "config_paths": ["config/runtime.toml"],
                            "services": {"redis": {"mode": "local"}},
                            "ports": ["localhost:6380"],
                        },
                        "secrets": ["STRIPE_SECRET_KEY"],
                    },
                )
            ]
        )
    )

    contract = contracts["task-metadata"]

    assert contract.required_env_vars == ("BLUEPRINT_DB_PATH", "STRIPE_SECRET_KEY")
    assert contract.optional_env_vars == ("EXPERIMENT_FLAG", "TRACE_SAMPLE_RATE")
    assert contract.config_paths == ("config/runtime.toml",)
    assert contract.services == ("redis",)
    assert contract.ports == (6380,)
    assert {
        (item.signal_type, item.value, item.field)
        for item in contract.evidence
    } >= {
        ("required_env_var", "BLUEPRINT_DB_PATH", "metadata.runtime.env"),
        ("optional_env_var", "TRACE_SAMPLE_RATE", "metadata.runtime.env"),
        ("optional_env_var", "EXPERIMENT_FLAG", "metadata.runtime.optional_env_vars"),
        ("config_path", "config/runtime.toml", "metadata.runtime.config_paths"),
        ("service", "redis", "metadata.runtime.services"),
        ("port", 6380, "metadata.runtime.ports"),
        ("required_env_var", "STRIPE_SECRET_KEY", "metadata.secrets"),
    }
    assert contract.missing_contract_warnings == ()


def test_deduplicates_and_sorts_contract_values():
    contracts = build_task_environment_contracts(
        _plan(
            [
                _task(
                    "task-sorted",
                    description="Use ZEBRA_KEY and API_KEY with redis on localhost:6379.",
                    acceptance_criteria=[
                        "API_KEY exists",
                        "Use config/z.yml",
                        "Use config/a.yml",
                    ],
                    test_command="API_KEY=x ZEBRA_KEY=y pytest",
                    metadata={
                        "services": ["redis", "postgres"],
                        "ports": [5432, "localhost:6379"],
                        "env": ["API_KEY"],
                    },
                )
            ]
        )
    )

    contract = contracts["task-sorted"]

    assert contract.required_env_vars == ("API_KEY", "ZEBRA_KEY")
    assert contract.config_paths == ("config/a.yml", "config/z.yml")
    assert contract.services == ("postgres", "redis")
    assert contract.ports == (5432, 6379)


def test_secret_and_config_ambiguity_warnings_only_when_no_explicit_contract():
    contracts = build_task_environment_contracts(
        _plan(
            [
                _task(
                    "task-ambiguous",
                    description="Needs an API key and local config before tests run.",
                ),
                _task(
                    "task-explicit",
                    description="Needs an API key from API_KEY and config/app.yml.",
                ),
            ]
        )
    )

    assert contracts["task-ambiguous"].missing_contract_warnings == (
        "Secret-like requirement mentioned without an explicit env var or config path.",
        "Config requirement mentioned without an explicit config path or env var.",
    )
    assert contracts["task-explicit"].missing_contract_warnings == ()


def test_accepts_model_input_and_serializes_to_json_compatible_dicts():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-serialize",
                    description="Run against LOCAL_API_TOKEN and docker on port 8080.",
                    metadata={"config_paths": [".env.test"]},
                )
            ]
        )
    )

    first = build_task_environment_contracts(model)
    second = build_task_environment_contracts(model)
    payload = task_environment_contracts_to_dict(first)

    assert payload == task_environment_contracts_to_dict(second)
    assert list(payload) == ["task-serialize"]
    assert list(payload["task-serialize"]) == [
        "task_id",
        "required_env_vars",
        "optional_env_vars",
        "config_paths",
        "services",
        "ports",
        "evidence",
        "missing_contract_warnings",
    ]
    assert payload["task-serialize"]["required_env_vars"] == ["LOCAL_API_TOKEN"]
    assert payload["task-serialize"]["config_paths"] == [".env.test"]
    assert payload["task-serialize"]["services"] == ["docker"]
    assert payload["task-serialize"]["ports"] == [8080]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks):
    return {
        "id": "plan-env-contracts",
        "implementation_brief_id": "brief-env-contracts",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [{"name": "Implementation"}],
        "test_strategy": "Run focused validation",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "metadata": {},
        "tasks": tasks,
    }


def _task(
    task_id,
    title="Update environment",
    *,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    test_command=None,
    metadata=None,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-env-contracts",
        "title": title,
        "description": description or f"Implement {title}",
        "milestone": "Implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Environment contract is observable"],
        "estimated_complexity": "medium",
        "risk_level": "low",
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
    }
