import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_license_compliance import (
    TaskLicenseCompliancePlan,
    TaskLicenseComplianceRecord,
    analyze_task_license_compliance,
    build_task_license_compliance_plan,
    summarize_task_license_compliance,
    task_license_compliance_plan_to_dict,
    task_license_compliance_plan_to_markdown,
)


def test_dependency_manifest_addition_requires_license_review_evidence():
    result = build_task_license_compliance_plan(
        _plan(
            [
                _task(
                    "task-python-dep",
                    title="Add PDF parsing dependency",
                    description="Install a new third-party package for parsing statements.",
                    files_or_modules=["pyproject.toml", "poetry.lock"],
                    acceptance_criteria=[
                        "Dependency license scan output is attached before merge."
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskLicenseCompliancePlan)
    assert result.plan_id == "plan-license-compliance"
    assert result.review_task_ids == ("task-python-dep",)
    assert result.summary == {
        "task_count": 1,
        "review_task_count": 1,
        "status_counts": {
            "license_review_required": 1,
            "license_review_recommended": 0,
            "license_review_not_needed": 0,
        },
        "ecosystem_counts": {
            "python": 1,
            "javascript": 0,
            "go": 0,
            "rust": 0,
            "ruby": 0,
            "container": 0,
            "vendored": 0,
        },
    }
    record = result.records[0]
    assert isinstance(record, TaskLicenseComplianceRecord)
    assert record.license_review_status == "license_review_required"
    assert record.detected_ecosystems == ("python",)
    assert "Package manifest or dependency definition files are in scope." in (
        record.review_reasons
    )
    assert record.required_evidence[:2] == (
        "Record package name, version, source, and resolved license for each added or changed third-party component.",
        "Attach dependency license scan output or package-manager license report for the final resolved dependency graph.",
    )
    assert record.suggested_reviewer_roles == (
        "engineering owner",
        "open-source compliance reviewer",
        "legal reviewer",
        "security or dependency management reviewer",
    )
    assert record.evidence == (
        "files_or_modules: pyproject.toml",
        "files_or_modules: poetry.lock",
        "title: Add PDF parsing dependency",
        "description: Install a new third-party package for parsing statements.",
        "acceptance_criteria[0]: Dependency license scan output is attached before merge.",
    )


def test_lockfile_only_change_is_recommended_with_transitive_evidence():
    result = analyze_task_license_compliance(
        _task(
            "task-lockfile",
            title="Refresh npm lockfile",
            description="Update package-lock.json after security resolver changes.",
            files_or_modules=["web/package-lock.json"],
        )
    )

    record = result.records[0]

    assert record.license_review_status == "license_review_recommended"
    assert record.detected_ecosystems == ("javascript",)
    assert record.review_reasons == (
        "Lockfile-only dependency changes can alter transitive package license obligations.",
        "Task text references third-party dependencies, packages, libraries, SDKs, or external modules.",
    )
    assert "Confirm the lockfile diff matches an approved manifest change or document why the transitive update is acceptable." in (
        record.required_evidence
    )
    assert result.summary["status_counts"]["license_review_recommended"] == 1


def test_vendored_code_requires_legal_and_attribution_evidence():
    result = build_task_license_compliance_plan(
        [
            _task(
                "task-vendor",
                title="Vendor markdown sanitizer",
                description="Import vendored source from the upstream project.",
                files_or_modules=["src/third_party/sanitizer/index.js"],
                metadata={"upstream_license": "MIT"},
            )
        ]
    )

    record = result.records[0]

    assert record.license_review_status == "license_review_required"
    assert record.detected_ecosystems == ("vendored",)
    assert record.review_reasons[:2] == (
        "Vendored or third-party source paths may require attribution, redistribution, and license compatibility review.",
        "Task text explicitly mentions license, legal, NOTICE, attribution, SPDX, or named open-source licenses.",
    )
    assert "Attach upstream source URL, commit or release, license file, and NOTICE or attribution update for vendored code." in (
        record.required_evidence
    )
    assert "legal reviewer" in record.suggested_reviewer_roles


def test_documentation_and_test_only_tasks_do_not_require_license_review():
    result = build_task_license_compliance_plan(
        [
            _task(
                "task-docs",
                title="Document dependency injection examples",
                description="Update user-facing docs for configuring test doubles.",
                files_or_modules=["docs/dependency-injection.md"],
            ),
            _task(
                "task-tests",
                title="Add package parser tests",
                description="Test parsing behavior for package labels.",
                files_or_modules=["tests/test_package_parser.py"],
            ),
        ]
    )

    assert [record.license_review_status for record in result.records] == [
        "license_review_not_needed",
        "license_review_not_needed",
    ]
    assert result.review_task_ids == ()
    assert all(record.required_evidence == () for record in result.records)


def test_model_inputs_and_stable_dict_and_markdown_serialization():
    task_dict = _task(
        "task-docker",
        title="Update worker base image",
        description="Replace the Dockerfile base image with a maintained third-party image.",
        files_or_modules=["workers/Dockerfile"],
        metadata={"license_review": "Check container distribution license and notices."},
    )
    original = copy.deepcopy(task_dict)
    task_model = ExecutionTask.model_validate(
        _task(
            "task-go",
            title="Add metrics client library",
            description="Add a new Go module for metrics export.",
            files_or_modules=["go.mod", "go.sum"],
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan([task_dict, task_model.model_dump(mode="python")])
    )

    first = summarize_task_license_compliance(plan_model)
    second = build_task_license_compliance_plan(plan_model)
    payload = task_license_compliance_plan_to_dict(first)
    markdown = task_license_compliance_plan_to_markdown(first)

    assert task_dict == original
    assert payload == task_license_compliance_plan_to_dict(second)
    assert first.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "review_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "license_review_status",
        "detected_ecosystems",
        "review_reasons",
        "required_evidence",
        "suggested_reviewer_roles",
        "evidence",
    ]
    assert markdown.startswith("# Task License Compliance Plan: plan-license-compliance")
    assert "| `task-docker` | license_review_required | container |" in markdown
    assert "| `task-go` | license_review_required | go |" in markdown


def _plan(tasks):
    return {
        "id": "plan-license-compliance",
        "implementation_brief_id": "brief-license-compliance",
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
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
