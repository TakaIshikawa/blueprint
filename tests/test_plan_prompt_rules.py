from blueprint.generators.plan_generator import PlanGenerator
from blueprint.generators.plan_generator_staged import StagedPlanGenerator


def test_plan_prompt_includes_single_rules_file_text():
    prompt = PlanGenerator.build_prompt(
        _implementation_brief(),
        rules_text="### AGENTS.md\nUse pytest for validation.",
    )

    assert "## Repository Rules" in prompt
    assert "### AGENTS.md" in prompt
    assert "Use pytest for validation." in prompt


def test_plan_prompt_includes_multiple_rules_files_text():
    prompt = PlanGenerator.build_prompt(
        _implementation_brief(),
        rules_text=(
            "### AGENTS.md\nFollow existing module boundaries.\n\n"
            "### .cursorrules\nPrefer small focused edits."
        ),
    )

    assert "## Repository Rules" in prompt
    assert "### AGENTS.md" in prompt
    assert "Follow existing module boundaries." in prompt
    assert "### .cursorrules" in prompt
    assert "Prefer small focused edits." in prompt


def test_plan_prompt_omits_rules_section_without_rules_text():
    prompt = PlanGenerator.build_prompt(_implementation_brief())

    assert "## Repository Rules" not in prompt


def test_staged_plan_prompts_include_rules_text():
    brief = _implementation_brief()
    milestone = {
        "name": "Milestone 1: Foundation",
        "description": "Set up the CLI foundation.",
    }
    rules_text = "### AGENTS.md\nKeep CLI output deterministic."

    milestone_prompt = StagedPlanGenerator.build_prompt(brief, rules_text=rules_text)
    task_prompt = StagedPlanGenerator.build_milestone_tasks_prompt(
        brief,
        milestone,
        rules_text=rules_text,
    )
    metadata_prompt = StagedPlanGenerator.build_plan_metadata_prompt(
        brief,
        rules_text=rules_text,
    )

    assert "## Repository Rules" in milestone_prompt
    assert "Keep CLI output deterministic." in milestone_prompt
    assert "## Repository Rules" in task_prompt
    assert "Keep CLI output deterministic." in task_prompt
    assert "## Repository Rules" in metadata_prompt
    assert "Keep CLI output deterministic." in metadata_prompt


def _implementation_brief():
    return {
        "id": "ib-rules",
        "source_brief_id": "src-rules",
        "title": "Review Queue Assistant",
        "domain": "developer_tools",
        "target_user": "Repository maintainers",
        "buyer": "Engineering managers",
        "workflow_context": "Daily pull request triage",
        "problem_statement": "Maintainers need a deterministic way to identify blocked reviews.",
        "mvp_goal": "Build a CLI that summarizes pending reviews and blockers.",
        "product_surface": "CLI",
        "scope": ["Load review queue data", "Render pending review summary"],
        "non_goals": ["Automated reviewer assignment", "Hosted dashboard"],
        "assumptions": ["Review data is available locally"],
        "architecture_notes": "Use a small command module backed by store reads.",
        "data_requirements": "Pull request IDs, statuses, reviewers, and blocker notes.",
        "integration_points": ["Local repository metadata"],
        "risks": ["Stale data may mislead users; show timestamps in output"],
        "validation_plan": "Run unit tests and manually inspect CLI output.",
        "definition_of_done": [
            "CLI prints queued reviews",
            "Tests cover empty and non-empty queues",
        ],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
