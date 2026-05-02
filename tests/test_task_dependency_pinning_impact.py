import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_dependency_pinning_impact import (
    TaskDependencyPinningImpactPlan,
    TaskDependencyPinningImpactRecord,
    analyze_task_dependency_pinning_impact,
    build_task_dependency_pinning_impact_plan,
    extract_task_dependency_pinning_impact,
    generate_task_dependency_pinning_impact,
    recommend_task_dependency_pinning_impact,
    summarize_task_dependency_pinning_impact,
    task_dependency_pinning_impact_plan_to_dict,
    task_dependency_pinning_impact_plan_to_dicts,
    task_dependency_pinning_impact_plan_to_markdown,
)


def test_detects_manifest_lockfile_sdk_major_upgrade_and_pinning_risks():
    result = build_task_dependency_pinning_impact_plan(
        _plan(
            [
                _task(
                    "task-stripe",
                    title="Pin Stripe SDK v12 upgrade",
                    description=(
                        "Upgrade Stripe SDK from 11.5.0 to 12.0.0, pin stripe==12.0.0, "
                        "review changelog, run compatibility tests, check security advisories, "
                        "and keep rollback version 11.5.0."
                    ),
                    files_or_modules=["pyproject.toml", "poetry.lock", "src/payments/stripe_client.py"],
                    acceptance_criteria=[
                        "Run poetry lock --no-update and verify lockfile reproducibility.",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskDependencyPinningImpactPlan)
    assert result.pinning_task_ids == ("task-stripe",)
    record = result.records[0]
    assert isinstance(record, TaskDependencyPinningImpactRecord)
    assert record.dependency_surfaces == ("package_manifest", "lockfile", "sdk_client", "dependency_version")
    assert {
        "version_bump",
        "major_upgrade",
        "lockfile_update",
        "sdk_client_upgrade",
    } <= set(record.detected_signals)
    assert record.risk_classifications == ("pinned", "major-upgrade")
    assert record.impact_level == "high"
    assert record.missing_checks == ()
    assert any("files_or_modules: pyproject.toml" == item for item in record.evidence)
    assert result.summary["signal_counts"]["major_upgrade"] == 1
    assert result.summary["risk_counts"]["pinned"] == 1


def test_detects_floating_runtime_image_and_transitive_drift_from_paths_and_text():
    result = build_task_dependency_pinning_impact_plan(
        _plan(
            [
                _task(
                    "task-runtime",
                    title="Move Node base image",
                    description=(
                        "Change Docker base image from node:18-alpine to node:20-alpine and allow "
                        "npm package latest ranges while refreshing transitive dependencies."
                    ),
                    files_or_modules=["Dockerfile", ".nvmrc", "package.json", "package-lock.json"],
                    acceptance_criteria=["Use npm install without lockfile reproducibility for now."],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.dependency_surfaces == (
        "package_manifest",
        "lockfile",
        "docker_image",
        "runtime_version",
        "dependency_version",
    )
    assert {
        "version_bump",
        "major_upgrade",
        "lockfile_update",
        "transitive_dependency_drift",
        "runtime_image_change",
        "runtime_version_change",
    } <= set(record.detected_signals)
    assert record.risk_classifications == ("floating", "major-upgrade", "transitive-drift")
    assert record.impact_level == "high"
    assert {
        "pinned_versions",
        "changelog_review",
        "compatibility_tests",
        "rollback_version",
        "security_advisory_review",
        "lockfile_reproducibility",
    } <= set(record.recommended_checks)
    assert "pinned_versions" in record.missing_checks


def test_partial_controls_and_object_like_inputs_are_stable():
    task = SimpleNamespace(
        id="task-client",
        title="Update generated API client",
        description=(
            "Bump generated client library from 2.3.4 to 2.4.0 with pinned versions, "
            "compatibility tests, rollback version, and security advisory review."
        ),
        files_or_modules=["clients/generated_client/package.json"],
        acceptance_criteria=["Review changelog before release."],
        metadata={"checks": {"lockfile": "Confirm reproducible lockfile generation."}},
    )

    result = analyze_task_dependency_pinning_impact([task])
    record = result.records[0]

    assert record.detected_signals == ("version_bump", "sdk_client_upgrade")
    assert record.risk_classifications == ("pinned",)
    assert record.present_checks == (
        "pinned_versions",
        "changelog_review",
        "compatibility_tests",
        "rollback_version",
        "security_advisory_review",
        "lockfile_reproducibility",
    )
    assert record.missing_checks == ()
    assert record.impact_level == "medium"
    assert any("metadata.checks.lockfile" in item for item in record.evidence)


def test_empty_malformed_and_model_inputs_serialize_without_mutation():
    plan = _plan(
        [
            _task(
                "task-lock",
                title="Regenerate poetry lockfile",
                description="Refresh transitive dependency tree after pin changes.",
                files_or_modules=["poetry.lock"],
            ),
            _task(
                "task-copy",
                title="Update static copy",
                description="Adjust empty state text only.",
                files_or_modules=["src/ui/copy.py"],
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_task_dependency_pinning_impact_plan(model)
    payload = task_dependency_pinning_impact_plan_to_dict(result)

    assert plan == original
    assert result.pinning_task_ids == ("task-lock",)
    assert result.ignored_task_ids == ("task-copy",)
    assert json.loads(json.dumps(payload)) == payload
    assert task_dependency_pinning_impact_plan_to_dicts(result) == [result.records[0].to_dict()]
    assert list(result.records[0].to_dict()) == [
        "task_id",
        "title",
        "impact_level",
        "dependency_surfaces",
        "detected_signals",
        "risk_classifications",
        "present_checks",
        "missing_checks",
        "recommended_checks",
        "evidence",
    ]
    assert build_task_dependency_pinning_impact_plan({"tasks": "not-a-list"}).records == ()
    assert build_task_dependency_pinning_impact_plan("not a plan").records == ()
    assert build_task_dependency_pinning_impact_plan(None).records == ()
    assert "No dependency pinning impact records" in build_task_dependency_pinning_impact_plan(_plan([])).to_markdown()


def test_aliases_markdown_and_single_task_inputs():
    source = _task(
        "task-python",
        title="Pin Python runtime",
        description="Pin Python runtime from 3.11 to 3.12 and review changelog.",
        files_or_modules=["runtime.txt", "requirements.txt"],
    )
    task_model = ExecutionTask.model_validate(source)

    result = build_task_dependency_pinning_impact_plan(task_model)
    markdown = task_dependency_pinning_impact_plan_to_markdown(result)

    assert recommend_task_dependency_pinning_impact(source).to_dict() == build_task_dependency_pinning_impact_plan(source).to_dict()
    assert summarize_task_dependency_pinning_impact(source).to_dict() == build_task_dependency_pinning_impact_plan(source).to_dict()
    assert extract_task_dependency_pinning_impact(source).to_dict() == build_task_dependency_pinning_impact_plan(source).to_dict()
    assert generate_task_dependency_pinning_impact(source).to_dict() == build_task_dependency_pinning_impact_plan(source).to_dict()
    assert markdown == result.to_markdown()
    assert "Task Dependency Pinning Impact" in markdown
    assert "runtime_version_change" in markdown


def _plan(tasks):
    return {
        "id": "plan-dependency-pinning-impact",
        "implementation_brief_id": "brief-dependency-pinning-impact",
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
    metadata=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": description or f"Implement {title}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [f"src/blueprint/{task_id}.py"],
        "acceptance_criteria": acceptance_criteria or [f"{title} is complete"],
        "test_command": None,
        "metadata": metadata or {},
    }
