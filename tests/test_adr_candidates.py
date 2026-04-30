from blueprint.adr_candidates import (
    ADRCandidate,
    adr_candidates_to_dicts,
    build_adr_candidates,
)
from blueprint.domain.models import ExecutionPlan, ImplementationBrief


def test_architecture_integration_and_data_brief_fields_produce_candidates():
    candidates = build_adr_candidates(
        _implementation_brief(
            architecture_notes="Use an event-driven worker boundary for checkout retries.",
            data_requirements="Order retry ledger; payment token retention",
            integration_points=["Payment API", "Webhook provider"],
        ),
        _execution_plan(tasks=[]),
    )

    assert all(isinstance(candidate, ADRCandidate) for candidate in candidates)
    assert {(candidate.title, candidate.context) for candidate in candidates} == {
        (
            "Decide architecture approach for Checkout Retry",
            "Use an event-driven worker boundary for checkout retries.",
        ),
        (
            "Decide integration boundary for Payment API",
            "Integration point: Payment API",
        ),
        (
            "Decide integration boundary for Webhook provider",
            "Integration point: Webhook provider",
        ),
        (
            "Decide data handling for Order retry ledger",
            "Data requirement: Order retry ledger",
        ),
        (
            "Decide data handling for payment token retention",
            "Data requirement: payment token retention",
        ),
    }
    payment = _candidate(candidates, "Decide integration boundary for Payment API")
    assert payment.decision_prompt.startswith("How should `Payment API` be integrated")
    assert payment.source_refs == ("brief:ib-checkout:integration_points",)


def test_high_risk_and_infrastructure_config_tasks_produce_task_linked_candidates():
    candidates = build_adr_candidates(
        _implementation_brief(architecture_notes=None),
        _execution_plan(
            tasks=[
                _task(
                    "task-safe",
                    "Update button copy",
                    risk_level="low",
                    files_or_modules=["src/ui.py"],
                ),
                _task(
                    "task-security",
                    "Rotate auth tokens",
                    risk_level="high",
                    files_or_modules=["src/security/tokens.py"],
                ),
                _task(
                    "task-config",
                    "Tune worker config",
                    risk_level="low",
                    files_or_modules=["pyproject.toml", ".github/workflows/ci.yml"],
                ),
            ]
        ),
    )

    assert [candidate.title for candidate in candidates] == [
        "Decide implementation approach for Rotate auth tokens",
        "Decide implementation approach for Tune worker config",
    ]
    security = _candidate(candidates, "Decide implementation approach for Rotate auth tokens")
    config = _candidate(candidates, "Decide implementation approach for Tune worker config")
    assert "risk_level: high" in security.context
    assert security.priority > config.priority
    assert security.source_refs == (
        "task:task-security",
        "task:task-security:files_or_modules:src/security/tokens.py",
    )
    assert "touches infrastructure or configuration" in config.context
    assert "task:task-config:files_or_modules:.github/workflows/ci.yml" in config.source_refs


def test_similar_candidates_are_deduplicated_and_source_refs_are_preserved():
    candidates = build_adr_candidates(
        _implementation_brief(
            integration_points=["Stripe API", "stripe api"],
        ),
        _execution_plan(
            tasks=[
                _task(
                    "task-stripe",
                    "Stripe API",
                    risk_level="high",
                    files_or_modules=["src/integrations/stripe.py"],
                ),
                _task(
                    "task-stripe",
                    "Stripe API",
                    risk_level="high",
                    files_or_modules=["src/integrations/stripe.py"],
                ),
            ]
        ),
    )

    stripe_integrations = [
        candidate
        for candidate in candidates
        if candidate.title == "Decide integration boundary for Stripe API"
    ]
    stripe_tasks = [
        candidate
        for candidate in candidates
        if candidate.title == "Decide implementation approach for Stripe API"
    ]

    assert len(stripe_integrations) == 1
    assert len(stripe_tasks) == 1
    assert stripe_integrations[0].source_refs == ("brief:ib-checkout:integration_points",)
    assert stripe_tasks[0].source_refs == (
        "task:task-stripe",
        "task:task-stripe:files_or_modules:src/integrations/stripe.py",
    )


def test_priority_increases_for_irreversible_data_security_and_external_decisions():
    candidates = build_adr_candidates(
        _implementation_brief(
            architecture_notes="Choose reversible UI composition.",
            data_requirements="Irreversible production database migration for customer tokens",
            integration_points=["External OAuth provider"],
        ),
        _execution_plan(tasks=[]),
    )

    architecture = _candidate(
        candidates, "Decide architecture approach for Checkout Retry"
    )
    data = _candidate(
        candidates,
        "Decide data handling for Irreversible production database migration for customer tokens",
    )
    integration = _candidate(
        candidates, "Decide integration boundary for External OAuth provider"
    )

    assert data.priority == 100
    assert integration.priority > architecture.priority
    assert data.priority > integration.priority


def test_model_inputs_and_serialized_payload_are_deterministic():
    brief = ImplementationBrief.model_validate(_implementation_brief())
    plan = ExecutionPlan.model_validate(
        _execution_plan(
            tasks=[
                _task(
                    "task-config",
                    "Tune worker config",
                    files_or_modules=["config/worker.yml"],
                )
            ]
        )
    )

    first = build_adr_candidates(brief, plan)
    second = build_adr_candidates(brief, plan)

    assert adr_candidates_to_dicts(first) == adr_candidates_to_dicts(second)
    assert list(adr_candidates_to_dicts(first)[0]) == [
        "title",
        "context",
        "decision_prompt",
        "source_refs",
        "priority",
    ]


def _candidate(candidates, title):
    return next(candidate for candidate in candidates if candidate.title == title)


def _implementation_brief(
    *,
    architecture_notes="Use the existing exporter interface.",
    data_requirements=None,
    integration_points=None,
):
    return {
        "id": "ib-checkout",
        "source_brief_id": "sb-checkout",
        "title": "Checkout Retry",
        "domain": "payments",
        "target_user": "Support agents",
        "buyer": "Operations",
        "workflow_context": "Failed checkout recovery",
        "problem_statement": "Checkout retry behavior needs a durable implementation plan.",
        "mvp_goal": "Retry failed payment submissions.",
        "product_surface": "Admin",
        "scope": ["Retry failed payment submissions"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": data_requirements,
        "integration_points": [] if integration_points is None else integration_points,
        "risks": [],
        "validation_plan": "Run pytest",
        "definition_of_done": ["Plan is complete"],
        "status": "draft",
    }


def _execution_plan(*, tasks):
    return {
        "id": "plan-checkout",
        "implementation_brief_id": "ib-checkout",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    risk_level="low",
    files_or_modules=None,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-checkout",
        "title": title,
        "description": f"Implement {title}",
        "milestone": "Implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Behavior is covered"],
        "estimated_complexity": "medium",
        "risk_level": risk_level,
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": {},
    }
