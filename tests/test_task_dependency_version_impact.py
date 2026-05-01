import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_dependency_version_impact import (
    TaskDependencyVersionImpact,
    generate_task_dependency_version_impact,
    task_dependency_version_impacts_to_dicts,
)


def test_manifest_lockfile_sdk_and_major_upgrade_signals_are_detected():
    records = generate_task_dependency_version_impact(
        _plan(
            [
                _task(
                    "task-stripe",
                    title="Upgrade Stripe SDK to v12",
                    description=(
                        "Upgrade stripe from 11.5.0 to 12.0.0 and migrate deprecated PaymentIntent "
                        "client library APIs."
                    ),
                    files_or_modules=["pyproject.toml", "poetry.lock", "src/payments/stripe_client.py"],
                    acceptance_criteria=[
                        "Review Stripe migration guide and release notes.",
                        "Run poetry run pytest tests/test_payments.py before release.",
                    ],
                    test_command="poetry run pytest tests/test_payments.py",
                    metadata={"rollback": "Pin stripe back to 11.5.0 and restore poetry.lock."},
                )
            ]
        )
    )

    assert len(records) == 1
    record = records[0]
    assert isinstance(record, TaskDependencyVersionImpact)
    assert record.task_id == "task-stripe"
    assert "python package manifest" in record.affected_dependency_surfaces
    assert "poetry lockfile" in record.affected_dependency_surfaces
    assert "sdk or external library api" in record.affected_dependency_surfaces
    assert "stripe" in record.dependency_names
    assert record.compatibility_risk == "high"
    assert any("migration guides" in check for check in record.required_checks)
    assert any("release notes" in check for check in record.required_checks)
    assert "poetry run pytest tests/test_payments.py" in record.validation_commands
    assert any("Pin stripe back" in note for note in record.rollback_notes)
    assert any("files_or_modules: pyproject.toml" == item for item in record.evidence)


def test_runtime_and_package_manager_upgrades_are_high_risk():
    records = generate_task_dependency_version_impact(
        _plan(
            [
                _task(
                    "task-runtime",
                    title="Upgrade Node runtime",
                    description="Move Node 18 to Node 20 and update pnpm package manager constraints.",
                    files_or_modules=[".nvmrc", "package.json", "pnpm-lock.yaml"],
                    metadata={"validation_commands": ["pnpm install --frozen-lockfile", "pnpm test"]},
                )
            ]
        )
    )

    record = records[0]
    assert record.compatibility_risk == "high"
    assert "runtime version" in record.affected_dependency_surfaces
    assert "node package manifest" in record.affected_dependency_surfaces
    assert "pnpm lockfile" in record.affected_dependency_surfaces
    assert "pnpm install --frozen-lockfile" in record.validation_commands
    assert "pnpm test" in record.validation_commands
    assert any("restore the previous runtime" in note for note in record.rollback_notes)
    assert any("runtime support windows" in check for check in record.required_checks)


def test_patch_updates_are_lower_risk_than_major_upgrades_and_deduplicated():
    records = generate_task_dependency_version_impact(
        _plan(
            [
                _task(
                    "task-major",
                    title="Major upgrade pydantic",
                    description="Major upgrade pydantic 1.10.0 to 2.5.0 with migration guide checks.",
                    files_or_modules=["pyproject.toml", "poetry.lock", "pyproject.toml"],
                ),
                _task(
                    "task-patch",
                    title="Patch dependency update",
                    description="Apply patch update requests 2.31.0 to 2.31.1 for a security fix.",
                    files_or_modules=["requirements.txt", "requirements.txt"],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in records}
    risk_order = {"low": 0, "medium": 1, "high": 2}
    assert by_id["task-major"].compatibility_risk == "high"
    assert by_id["task-patch"].compatibility_risk == "medium"
    assert (
        risk_order[by_id["task-patch"].compatibility_risk]
        < risk_order[by_id["task-major"].compatibility_risk]
    )
    assert by_id["task-major"].affected_dependency_surfaces.count("python package manifest") == 1
    assert by_id["task-major"].evidence.count("files_or_modules: pyproject.toml") == 1
    assert by_id["task-patch"].affected_dependency_surfaces == ("python package manifest", "dependency version")


def test_model_input_serializes_stably_without_mutation():
    plan = _plan(
        [
            _task(
                "task-lock",
                title="Regenerate lockfile",
                description="Update poetry lockfile after dependency pin changes.",
                files_or_modules=["poetry.lock"],
            ),
            _task(
                "task-docs",
                title="Document UI settings",
                description="Update documentation only.",
                files_or_modules=["docs/settings.md"],
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    records = generate_task_dependency_version_impact(model)
    payload = task_dependency_version_impacts_to_dicts(records)

    assert plan == original
    assert len(records) == 1
    assert payload == [record.to_dict() for record in records]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload[0]) == [
        "task_id",
        "task_title",
        "affected_dependency_surfaces",
        "dependency_names",
        "compatibility_risk",
        "required_checks",
        "validation_commands",
        "rollback_notes",
        "evidence",
    ]


def test_empty_partial_or_non_model_sources_do_not_raise():
    assert generate_task_dependency_version_impact(_plan([_task("task-general", "General work")])) == ()
    assert generate_task_dependency_version_impact({"tasks": "not a list"}) == ()
    assert generate_task_dependency_version_impact("not a plan") == ()
    assert generate_task_dependency_version_impact(None) == ()


def _plan(tasks):
    return {
        "id": "plan-dependency-version-impact",
        "implementation_brief_id": "brief-dependency-version-impact",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    test_command=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": description or f"Implement {title}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [f"src/blueprint/{task_id}.py"],
        "acceptance_criteria": acceptance_criteria or [f"{title} is complete"],
        "test_command": test_command,
        "metadata": metadata or {},
    }
