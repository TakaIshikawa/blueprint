import json

from blueprint.dependency_interface_changes import (
    DependencyInterfaceChangeFinding,
    dependency_interface_changes_to_dict,
    detect_dependency_interface_changes,
)


def test_detects_high_confidence_api_dependency_interface_change():
    findings = detect_dependency_interface_changes(
        _plan(
            [
                _task(
                    "task-api",
                    title="Change public REST API endpoint",
                    description="Rename the account API response field.",
                    files_or_modules=["src/api/accounts.py", "openapi/accounts.yaml"],
                    acceptance_criteria=["OpenAPI contract documents the new endpoint."],
                ),
                _task(
                    "task-client",
                    depends_on=["task-api"],
                    title="Update account API client",
                    files_or_modules=["src/clients/accounts_api.py"],
                    acceptance_criteria=["Client reads the new endpoint response."],
                ),
            ]
        )
    )

    assert findings == (
        DependencyInterfaceChangeFinding(
            upstream_task_id="task-api",
            downstream_task_id="task-client",
            impact_type="api_contract",
            confidence="high",
            evidence=(
                "upstream api signal from acceptance_criteria: "
                "OpenAPI contract documents the new endpoint.",
                "upstream api signal from description: Rename the account API response field.",
                "upstream api signal from files_or_modules: openapi/accounts.yaml",
                "downstream api signal from acceptance_criteria: "
                "Client reads the new endpoint response.",
                "downstream api signal from files_or_modules: src/clients/accounts_api.py",
            ),
            recommended_coordination=(
                "Coordinate the api_contract contract before downstream implementation starts."
            ),
        ),
    )


def test_detects_schema_and_config_contract_edges():
    findings = detect_dependency_interface_changes(
        _plan(
            [
                _task(
                    "task-schema",
                    title="Add database migration",
                    files_or_modules=["migrations/20260501_add_accounts.sql"],
                    acceptance_criteria=["Account table schema has owner_id column."],
                ),
                _task(
                    "task-repository",
                    depends_on=["task-schema"],
                    files_or_modules=["src/models/account.py"],
                    acceptance_criteria=["Repository reads owner_id from account model."],
                ),
                _task(
                    "task-config",
                    title="Introduce runtime config",
                    files_or_modules=["config/runtime.yml"],
                    acceptance_criteria=["Document the feature flag configuration."],
                ),
                _task(
                    "task-worker",
                    depends_on=["task-config"],
                    description="Worker loads config/runtime.yml before processing jobs.",
                ),
            ]
        )
    )

    assert [(finding.impact_type, finding.confidence) for finding in findings] == [
        ("schema_contract", "medium"),
        ("config_contract", "high"),
    ]
    assert findings[0].upstream_task_id == "task-schema"
    assert findings[1].upstream_task_id == "task-config"


def test_low_confidence_unrelated_dependencies_are_reported():
    findings = detect_dependency_interface_changes(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Refresh onboarding copy",
                    description="Update prose in README.",
                    files_or_modules=["README.md"],
                ),
                _task(
                    "task-tests",
                    depends_on=["task-docs"],
                    title="Add smoke tests",
                    files_or_modules=["tests/test_smoke.py"],
                ),
            ]
        )
    )

    assert findings == (
        DependencyInterfaceChangeFinding(
            upstream_task_id="task-docs",
            downstream_task_id="task-tests",
            impact_type="unrelated",
            confidence="low",
            evidence=("No public interface signal found on upstream dependency.",),
            recommended_coordination="No interface-specific coordination detected.",
        ),
    )


def test_missing_dependency_references_produce_low_risk_findings():
    findings = detect_dependency_interface_changes(
        _plan([_task("task-ui", depends_on=["task-missing"])])
    )

    assert findings == (
        DependencyInterfaceChangeFinding(
            upstream_task_id="task-missing",
            downstream_task_id="task-ui",
            impact_type="missing_dependency",
            confidence="low",
            evidence=("depends_on references unknown task 'task-missing'",),
            recommended_coordination=(
                "Resolve the dependency id before using it to coordinate interface work."
            ),
        ),
    )


def test_metadata_provided_contracts_contribute_interface_signals():
    findings = detect_dependency_interface_changes(
        _plan(
            [
                _task(
                    "task-events",
                    metadata={
                        "contracts": {
                            "events": ["billing.invoice.created payload v2"],
                        }
                    },
                ),
                _task(
                    "task-consumer",
                    depends_on=["task-events"],
                    description="Consumer handles invoice event payloads.",
                ),
            ]
        )
    )

    assert findings[0].impact_type == "event_contract"
    assert findings[0].confidence == "high"
    assert any("metadata.contracts.events" in item for item in findings[0].evidence)


def test_dependency_edge_ordering_is_stable():
    findings = detect_dependency_interface_changes(
        _plan(
            [
                _task("task-api", title="API contract", files_or_modules=["src/api/a.py"]),
                _task("task-config", title="Config contract", files_or_modules=["config/app.yml"]),
                _task("task-cli", title="CLI command", files_or_modules=["src/cli.py"]),
                _task("task-z", depends_on=["task-config", "task-api"]),
                _task("task-a", depends_on=["task-cli"]),
            ]
        )
    )

    assert [
        (finding.upstream_task_id, finding.downstream_task_id)
        for finding in findings
    ] == [
        ("task-config", "task-z"),
        ("task-api", "task-z"),
        ("task-cli", "task-a"),
    ]


def test_serialization_is_stable_and_json_compatible():
    finding = DependencyInterfaceChangeFinding(
        upstream_task_id="task-api",
        downstream_task_id="task-client",
        impact_type="api_contract",
        confidence="medium",
        evidence=("upstream api signal from title: API",),
        recommended_coordination=(
            "Confirm whether the api_contract affects downstream assumptions during handoff."
        ),
    )

    payload = dependency_interface_changes_to_dict([finding])

    assert payload == [
        {
            "upstream_task_id": "task-api",
            "downstream_task_id": "task-client",
            "impact_type": "api_contract",
            "confidence": "medium",
            "evidence": ["upstream api signal from title: API"],
            "recommended_coordination": (
                "Confirm whether the api_contract affects downstream assumptions during handoff."
            ),
        }
    ]
    assert list(payload[0]) == [
        "upstream_task_id",
        "downstream_task_id",
        "impact_type",
        "confidence",
        "evidence",
        "recommended_coordination",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks):
    return {
        "id": "plan-interface-changes",
        "implementation_brief_id": "brief-interface-changes",
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
    *,
    title=None,
    description=None,
    depends_on=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-interface-changes",
        "title": title or f"Task {task_id}",
        "description": description or f"Implement {task_id}",
        "milestone": "Implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Task is complete."],
        "estimated_complexity": "medium",
        "risk_level": "low",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
