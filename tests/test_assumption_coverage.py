import json

from blueprint.assumption_coverage import (
    AssumptionCoverageEntry,
    AssumptionCoverageReport,
    AssumptionTaskEvidence,
    analyze_assumption_coverage,
    assumption_coverage_report_to_dict,
)
from blueprint.domain.models import ExecutionPlan, ImplementationBrief


def test_acceptance_criteria_verify_assumptions_with_strong_evidence():
    report = analyze_assumption_coverage(
        _brief(assumptions=["CSV consumers parse standard quoting"]),
        _plan(
            tasks=[
                _task(
                    "task-export",
                    "Add CSV export",
                    acceptance=[
                        "CSV consumers parse standard quoting for commas and embedded quotes"
                    ],
                    description="Implement exporter",
                )
            ],
        ),
    )

    assert isinstance(report, AssumptionCoverageReport)
    assert isinstance(report.assumptions[0], AssumptionCoverageEntry)
    assert isinstance(report.assumptions[0].evidence[0], AssumptionTaskEvidence)
    assert report.assumptions[0].to_dict() == {
        "assumption_id": "assumption-1",
        "assumption": "CSV consumers parse standard quoting",
        "status": "verified",
        "matched_task_ids": ["task-export"],
        "evidence": [
            {
                "task_id": "task-export",
                "strength": "strong",
                "fields": ["acceptance_criteria"],
            }
        ],
        "suggested_acceptance_criterion": None,
    }
    assert report.summary == {
        "assumption_count": 1,
        "status_counts": {"verified": 1},
        "unverified_count": 0,
    }


def test_test_commands_count_as_strong_verification_evidence():
    report = analyze_assumption_coverage(
        _brief(assumptions=["Assumption coverage helper handles duplicates"]),
        _plan(
            tasks=[
                _task(
                    "task-tests",
                    "Add assumption coverage tests",
                    test_command="poetry run pytest tests/test_assumption_coverage.py "
                    "-k assumption_coverage_helper_handles_duplicates",
                    description="Add regression coverage",
                )
            ],
        ),
    )

    assert report.assumptions[0].status == "verified"
    assert report.assumptions[0].matched_task_ids == ("task-tests",)
    assert report.assumptions[0].evidence[0].to_dict() == {
        "task_id": "task-tests",
        "strength": "strong",
        "fields": ["test_command"],
    }


def test_descriptions_files_and_metadata_partially_cover_assumptions():
    report = analyze_assumption_coverage(
        _brief(
            assumptions=[
                "Local repository metadata is readable",
                "Settings panel uses React components",
                "Audit logs preserve request identifiers",
            ]
        ),
        _plan(
            tasks=[
                _task(
                    "task-repo",
                    "Read repository metadata",
                    description="Load local repository metadata before planning.",
                    acceptance=["Repository reader is complete"],
                ),
                _task(
                    "task-settings",
                    "Update settings",
                    files=["src/components/SettingsPanel.tsx"],
                    description="Adjust empty state",
                ),
                _task(
                    "task-logs",
                    "Add audit logging",
                    metadata={"mitigation": "Audit logs preserve request identifiers"},
                ),
            ],
        ),
    )

    assert [entry.status for entry in report.assumptions] == [
        "partially_covered",
        "partially_covered",
        "partially_covered",
    ]
    assert [entry.matched_task_ids for entry in report.assumptions] == [
        ("task-repo",),
        ("task-settings",),
        ("task-logs",),
    ]
    assert [entry.evidence[0].fields for entry in report.assumptions] == [
        ("title", "description"),
        ("files_or_modules",),
        ("metadata",),
    ]
    assert report.summary["status_counts"] == {"partially_covered": 3}


def test_unverified_assumptions_include_suggested_acceptance_criterion():
    report = analyze_assumption_coverage(
        _brief(assumptions=["Production exports finish within five minutes"]),
        _plan(tasks=[_task("task-unrelated", "Build importer")]),
    )

    entry = report.assumptions[0]

    assert entry.status == "unverified"
    assert entry.matched_task_ids == ()
    assert entry.evidence == ()
    assert entry.suggested_acceptance_criterion == (
        "Acceptance criteria verifies this assumption: "
        "Production exports finish within five minutes"
    )
    assert report.summary == {
        "assumption_count": 1,
        "status_counts": {"unverified": 1},
        "unverified_count": 1,
    }


def test_duplicate_assumptions_are_reported_as_stable_separate_entries():
    report = analyze_assumption_coverage(
        _brief(
            assumptions=[
                "Developers use GitHub Actions",
                "Developers use GitHub Actions",
            ]
        ),
        _plan(
            tasks=[
                _task(
                    "task-actions",
                    "Add workflow export",
                    acceptance=["Developers use GitHub Actions to run exported workflows"],
                )
            ],
        ),
    )

    assert [entry.assumption_id for entry in report.assumptions] == [
        "assumption-1",
        "assumption-2",
    ]
    assert [entry.assumption for entry in report.assumptions] == [
        "Developers use GitHub Actions",
        "Developers use GitHub Actions",
    ]
    assert [entry.status for entry in report.assumptions] == ["verified", "verified"]
    assert [entry.matched_task_ids for entry in report.assumptions] == [
        ("task-actions",),
        ("task-actions",),
    ]


def test_empty_plans_and_malformed_optional_fields_do_not_raise():
    report = analyze_assumption_coverage(
        {
            "id": "brief-partial",
            "assumptions": [
                "Validation artifacts are generated deterministically",
                None,
                "Tasks can omit optional fields",
            ],
            "unexpected": "forces model fallback",
        },
        {
            "id": "plan-partial",
            "tasks": [
                {
                    "id": "task-malformed",
                    "title": None,
                    "description": ["not a string"],
                    "acceptance_criteria": "Tasks can omit optional fields",
                    "test_command": ["pytest"],
                    "files_or_modules": {"path": "src/blueprint/tasks.py"},
                    "metadata": ["not", "a", "mapping"],
                },
                "not a task",
            ],
        },
    )

    assert report.to_dict() == {
        "brief_id": "brief-partial",
        "plan_id": "plan-partial",
        "assumptions": [
            {
                "assumption_id": "assumption-1",
                "assumption": "Validation artifacts are generated deterministically",
                "status": "unverified",
                "matched_task_ids": [],
                "evidence": [],
                "suggested_acceptance_criterion": (
                    "Acceptance criteria verifies this assumption: "
                    "Validation artifacts are generated deterministically"
                ),
            },
            {
                "assumption_id": "assumption-2",
                "assumption": "Tasks can omit optional fields",
                "status": "verified",
                "matched_task_ids": ["task-malformed"],
                "evidence": [
                    {
                        "task_id": "task-malformed",
                        "strength": "strong",
                        "fields": ["acceptance_criteria"],
                    }
                ],
                "suggested_acceptance_criterion": None,
            },
        ],
        "summary": {
            "assumption_count": 2,
            "status_counts": {"unverified": 1, "verified": 1},
            "unverified_count": 1,
        },
    }


def test_missing_assumptions_and_missing_tasks_return_empty_report():
    report = analyze_assumption_coverage({"id": "brief-empty"}, {"id": "plan-empty"})

    assert report.to_dict() == {
        "brief_id": "brief-empty",
        "plan_id": "plan-empty",
        "assumptions": [],
        "summary": {
            "assumption_count": 0,
            "status_counts": {},
            "unverified_count": 0,
        },
    }


def test_accepts_domain_models_and_serializes_stably():
    brief_model = ImplementationBrief.model_validate(
        _brief(assumptions=["Task acceptance criteria mention the assumption"])
    )
    plan_model = ExecutionPlan.model_validate(
        _plan(
            tasks=[
                _task(
                    "task-model",
                    "Model task",
                    acceptance=["Task acceptance criteria mention the assumption"],
                )
            ]
        )
    )

    report = analyze_assumption_coverage(brief_model, plan_model)
    payload = assumption_coverage_report_to_dict(report)

    assert payload == report.to_dict()
    assert list(payload) == ["brief_id", "plan_id", "assumptions", "summary"]
    assert payload["brief_id"] == "brief-assumptions"
    assert payload["plan_id"] == "plan-assumptions"
    assert json.loads(json.dumps(payload)) == payload


def _brief(*, assumptions):
    return {
        "id": "brief-assumptions",
        "source_brief_id": "source-assumptions",
        "title": "Assumption coverage",
        "domain": "planning",
        "target_user": "Agents",
        "buyer": "Engineering",
        "workflow_context": "Execution planning",
        "problem_statement": "Assumptions need explicit coverage.",
        "mvp_goal": "Map assumptions to validating tasks.",
        "product_surface": "Python helper",
        "scope": ["Assumption coverage"],
        "non_goals": [],
        "assumptions": assumptions,
        "architecture_notes": "Use deterministic token matching.",
        "data_requirements": "Brief assumptions and execution plan tasks.",
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run pytest",
        "definition_of_done": ["Assumption coverage is serialized"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _plan(*, tasks):
    return {
        "id": "plan-assumptions",
        "implementation_brief_id": "brief-assumptions",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "library",
        "milestones": [],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Validate assumptions",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    description=None,
    files=None,
    acceptance=None,
    test_command=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title,
        "description": description or f"Implement {title}.",
        "milestone": "Coverage",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files or [],
        "acceptance_criteria": acceptance or [f"{title} is complete"],
        "estimated_complexity": "low",
        "status": "pending",
    }
    if test_command is not None:
        task["test_command"] = test_command
    if metadata is not None:
        task["metadata"] = metadata
    return task
