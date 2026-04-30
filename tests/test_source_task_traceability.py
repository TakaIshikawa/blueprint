import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief, SourceBrief
from blueprint.source_task_traceability import (
    SourceTaskTraceLink,
    build_source_task_traceability_map,
    source_task_traceability_to_dicts,
)


def test_builds_trace_links_for_covered_source_and_implementation_requirements():
    links = build_source_task_traceability_map(_source(), _brief(), _plan())
    by_field = {link.source_field: link for link in links}

    source_summary = by_field["source.summary"]
    assert isinstance(source_summary, SourceTaskTraceLink)
    assert source_summary.coverage_status == "covered"
    assert source_summary.matched_task_ids == ("task-rollout-report",)
    assert source_summary.evidence_snippets == (
        "task-rollout-report description: Build a release readiness dashboard for operators.",
        "task-rollout-report acceptance_criteria: "
        "Operators can review a release readiness dashboard before rollout.",
        "task-rollout-report files_or_modules: src/release/readiness_dashboard.py",
    )

    assert by_field["implementation.scope.001"].to_dict() == {
        "requirement_id": (
            "req-implementation-scope-001-render-release-readiness-dashboard"
        ),
        "source_field": "implementation.scope.001",
        "requirement_text": "Render release readiness dashboard",
        "matched_task_ids": ["task-rollout-report"],
        "evidence_snippets": [
            "task-rollout-report description: "
            "Build a release readiness dashboard for operators.",
            "task-rollout-report acceptance_criteria: "
            "Operators can review a release readiness dashboard before rollout.",
            "task-rollout-report files_or_modules: src/release/readiness_dashboard.py",
        ],
        "coverage_status": "covered",
    }
    assert by_field["implementation.integration_points.001"].matched_task_ids == (
        "task-payments-api",
    )
    assert by_field["implementation.validation_plan"].matched_task_ids == (
        "task-validation",
    )
    assert by_field["implementation.definition_of_done.001"].matched_task_ids == (
        "task-validation",
    )


def test_uncovered_requirements_are_explicitly_marked():
    links = build_source_task_traceability_map(
        _source(),
        {
            **_brief(),
            "scope": ["Render release readiness dashboard", "Publish executive PDF"],
        },
        _plan(),
    )

    uncovered = [
        link for link in links if link.requirement_text == "Publish executive PDF"
    ][0]

    assert uncovered.to_dict() == {
        "requirement_id": "req-implementation-scope-002-publish-executive-pdf",
        "source_field": "implementation.scope.002",
        "requirement_text": "Publish executive PDF",
        "matched_task_ids": [],
        "evidence_snippets": [],
        "coverage_status": "uncovered",
    }


def test_multiple_tasks_can_cover_one_requirement():
    links = build_source_task_traceability_map(
        _source(),
        _brief(),
        {
            **_plan(),
            "tasks": _plan()["tasks"]
            + [
                _task(
                    "task-dashboard-tests",
                    title="Test dashboard rendering",
                    description="Add regression coverage for release readiness dashboard.",
                    acceptance_criteria=[
                        "Release readiness dashboard renders populated status rows."
                    ],
                )
            ],
        },
    )

    scope = [link for link in links if link.source_field == "implementation.scope.001"][0]

    assert scope.coverage_status == "covered"
    assert scope.matched_task_ids == (
        "task-rollout-report",
        "task-dashboard-tests",
    )
    assert scope.evidence_snippets == (
        "task-rollout-report description: "
        "Build a release readiness dashboard for operators.",
        "task-rollout-report acceptance_criteria: "
        "Operators can review a release readiness dashboard before rollout.",
        "task-rollout-report files_or_modules: src/release/readiness_dashboard.py",
        "task-dashboard-tests description: "
        "Add regression coverage for release readiness dashboard.",
        "task-dashboard-tests acceptance_criteria: "
        "Release readiness dashboard renders populated status rows.",
    )


def test_weak_one_word_matches_do_not_cover_requirements():
    links = build_source_task_traceability_map(
        _source(),
        {
            **_brief(),
            "risks": ["User"],
            "scope": ["Review"],
            "integration_points": [],
            "definition_of_done": [],
        },
        {
            **_plan(),
            "tasks": [
                _task(
                    "task-generic",
                    title="User review",
                    description="Review user copy.",
                    acceptance_criteria=["User review is complete."],
                )
            ],
        },
    )

    weak_fields = {
        link.source_field: link
        for link in links
        if link.source_field in {"implementation.risks.001", "implementation.scope.001"}
    }

    assert weak_fields["implementation.risks.001"].coverage_status == "uncovered"
    assert weak_fields["implementation.scope.001"].coverage_status == "uncovered"
    assert weak_fields["implementation.risks.001"].matched_task_ids == ()
    assert weak_fields["implementation.scope.001"].evidence_snippets == ()


def test_accepts_models_and_keeps_requirement_ids_deterministic():
    source_model = SourceBrief.model_validate(_source())
    brief_model = ImplementationBrief.model_validate(_brief())
    plan_model = ExecutionPlan.model_validate(_plan())

    first = build_source_task_traceability_map(source_model, brief_model, plan_model)
    second = build_source_task_traceability_map(_source(), _brief(), _plan())

    assert [link.requirement_id for link in first] == [link.requirement_id for link in second]
    assert [link.requirement_id for link in first] == [
        "req-source-title-release-readiness",
        "req-source-summary-operator-need-release-readiness-dashboard",
        "req-source-source-payload-risk-001-late-payment-api-contract-block-rollout",
        "req-implementation-title-release-readiness-brief",
        "req-implementation-scope-001-render-release-readiness-dashboard",
        "req-implementation-scope-002-add-rollout-blocker-alert",
        "req-implementation-integration-point-001-payment-api",
        "req-implementation-risk-001-payment-api-outage-block-release",
        "req-implementation-validation-plan-run-regression-rollout-smoke-test",
        "req-implementation-definition-done-001-validation-evidence-attached-handoff",
    ]
    assert source_task_traceability_to_dicts(first) == [
        link.to_dict() for link in first
    ]
    assert json.loads(json.dumps(source_task_traceability_to_dicts(first)))


def test_missing_optional_fields_do_not_raise():
    links = build_source_task_traceability_map(
        {"id": "source-sparse", "title": "Sparse source", "summary": "Sparse summary"},
        {"id": "brief-sparse", "title": "Sparse brief"},
        {"id": "plan-sparse", "tasks": [{"id": "task-empty"}]},
    )

    assert [link.source_field for link in links] == [
        "source.title",
        "source.summary",
        "implementation.title",
    ]
    assert all(link.coverage_status == "uncovered" for link in links)


def _source():
    return {
        "id": "source-release-readiness",
        "title": "Release readiness",
        "domain": "release",
        "summary": "Operators need release readiness dashboard.",
        "source_project": "Linear",
        "source_entity_type": "issue",
        "source_id": "REL-101",
        "source_payload": {
            "risks": ["Late Payment API contracts block rollout"],
            "ignored": "Not a requirement anchor",
        },
        "source_links": {},
    }


def _brief():
    return {
        "id": "brief-release-readiness",
        "source_brief_id": "source-release-readiness",
        "title": "Release Readiness Brief",
        "target_user": "operators",
        "buyer": "Engineering",
        "problem_statement": "Operators cannot tell whether a release is ready.",
        "mvp_goal": "Show readiness before rollout.",
        "scope": [
            "Render release readiness dashboard",
            "Add rollout blocker alerts",
        ],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": ["Payment API"],
        "risks": ["Payment API outage can block release"],
        "validation_plan": "Run regression and rollout smoke tests.",
        "definition_of_done": ["Validation evidence attached to handoff"],
        "status": "planned",
    }


def _plan():
    return {
        "id": "plan-release-readiness",
        "implementation_brief_id": "brief-release-readiness",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [{"name": "Implementation"}],
        "test_strategy": "Run pytest",
        "status": "draft",
        "metadata": {},
        "tasks": [
            _task(
                "task-rollout-report",
                title="Build readiness dashboard",
                description="Build a release readiness dashboard for operators.",
                acceptance_criteria=[
                    "Operators can review a release readiness dashboard before rollout."
                ],
                files_or_modules=["src/release/readiness_dashboard.py"],
            ),
            _task(
                "task-blockers",
                title="Add rollout blocker alerts",
                description="Surface rollout blocker alerts to release operators.",
                acceptance_criteria=["Blocker alerts appear before release."],
            ),
            _task(
                "task-payments-api",
                title="Validate Payment API contract",
                description="Check Payment API outage handling before rollout.",
                acceptance_criteria=["Payment API contract failures block release."],
                files_or_modules=["src/integrations/payment_api.py"],
            ),
            _task(
                "task-validation",
                title="Run release validation",
                description="Run regression and rollout smoke tests.",
                acceptance_criteria=[
                    "Validation evidence is attached to the implementation handoff."
                ],
            ),
        ],
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-release-readiness",
        "title": title or f"Task {task_id}",
        "description": description or f"Implement {task_id}",
        "milestone": "Implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Task is complete."],
        "estimated_complexity": "medium",
        "risk_level": "low",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": {},
    }
