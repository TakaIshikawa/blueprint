import json

from blueprint.task_release_train_readiness import (
    analyze_task_release_train_readiness,
    build_task_release_train_readiness_plan,
    recommend_task_release_train_readiness,
    task_release_train_readiness_plan_to_dict,
    task_release_train_readiness_plan_to_dicts,
    task_release_train_readiness_plan_to_markdown,
)


def test_complete_release_train_task_is_ready():
    result = build_task_release_train_readiness_plan(
        {
            "id": "plan-release-train",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Release train promotion for scheduled release",
                    "description": "Release calendar includes train cutoff and branch freeze handling.",
                    "acceptance_criteria": [
                        "Train schedule defines release calendar, cadence, and release date.",
                        "Scope cutoff includes feature cutoff, merge cutoff, and inclusion deadline.",
                        "Branch strategy and version strategy cover release branch, tagging strategy, semver, and versioning.",
                        "Approval gates include release gates, go/no-go, sign-off, approver, and quality gate.",
                        "Rollback and hold process cover release hold, stop ship, backout, revert, hotfix, and pause promotion.",
                        "Dependency coordination names dependent teams, upstream systems, downstream systems, and dependency owners.",
                        "Communication plan includes release notes, stakeholder communication, customer communication, and status update.",
                        "Tests include release train tests, promotion tests, and gate tests.",
                    ],
                    "files_or_modules": ["src/release/release_train/train_promotion.py"],
                }
            ],
        }
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "train_schedule",
        "scope_cutoff",
        "branch_version_strategy",
        "approval_gates",
        "rollback_hold_process",
        "dependency_coordination",
        "communication_plan",
        "tests",
    )


def test_partial_release_train_reports_gaps_and_ignores_no_impact():
    result = analyze_task_release_train_readiness(
        [
            {
                "id": "task-partial",
                "title": "Add release train schedule",
                "description": "Release calendar has approval gates and release notes.",
                "metadata": {"release": {"branch": "Branch strategy uses a release branch."}},
                "validation_commands": ["python -m pytest tests/release/test_release_train.py"],
            },
            {
                "id": "task-copy",
                "title": "Release docs cleanup",
                "description": "No release train, scheduled release, train cutoff, release calendar, branch freeze, or train promotion changes are planned.",
            },
        ]
    )

    record = result.records[0]
    assert result.ignored_task_ids == ("task-copy",)
    assert record.readiness == "partial"
    assert record.present_criteria == ("train_schedule", "branch_version_strategy", "approval_gates", "communication_plan", "tests")
    assert record.missing_criteria == ("scope_cutoff", "rollback_hold_process", "dependency_coordination")
    assert any("metadata.release.branch" in item for item in record.evidence)
    assert any("validation_commands[0]" in item for item in record.evidence)


def test_release_train_path_hints_serialization_and_markdown_are_stable():
    result = build_task_release_train_readiness_plan(
        {"id": "plan-path", "tasks": [{"id": "task-path", "title": "Refactor release automation", "files_or_modules": ["src/releases/release_calendar/branch_freeze.py"]}]}
    )
    payload = task_release_train_readiness_plan_to_dict(result)

    assert result.records[0].detected_signals == ("scheduled_release", "branch_freeze")
    assert recommend_task_release_train_readiness(result) == result.records
    assert task_release_train_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-path"
    assert task_release_train_readiness_plan_to_markdown(result).startswith("# Task Release Train Readiness: plan-path")
