"""Create a test execution plan for exporter development."""

import sys
sys.path.insert(0, 'src')

from blueprint.config import get_config
from blueprint.store import Store
from blueprint.generators.plan_generator import generate_execution_plan_id, generate_task_id

# Initialize
config = get_config()
store = Store(config.db_path)

# Get an implementation brief
briefs = store.list_implementation_briefs()
if not briefs:
    print("No implementation briefs found. Generate one first.")
    sys.exit(1)

brief = briefs[0]
print(f"Creating test plan for: {brief['title']}")

# Create a simple execution plan
plan = {
    "id": generate_execution_plan_id(),
    "implementation_brief_id": brief["id"],
    "target_engine": "claude_code",
    "target_repo": "agent-adversarial-bench",
    "project_type": "python_library",
    "milestones": [
        {
            "name": "Milestone 1: Project Setup",
            "description": "Initialize project structure, dependencies, and core models",
            "tasks": []
        },
        {
            "name": "Milestone 2: Test Case Schema",
            "description": "Define and implement test case data structure",
            "tasks": []
        },
        {
            "name": "Milestone 3: CLI Runner",
            "description": "Build CLI for executing test suites",
            "tasks": []
        }
    ],
    "test_strategy": "Unit tests with pytest, integration tests with real agent endpoints, manual validation of scorecard output",
    "handoff_prompt": "Build a Python library for adversarial testing of AI agents. Focus on deterministic evaluation using canary detection.",
    "status": "draft",
    "generation_model": "manual",
    "generation_tokens": 0,
    "generation_prompt": "Manually created test plan",
}

tasks = [
    {
        "id": generate_task_id(),
        "title": "Initialize Python project structure",
        "description": "Set up Poetry project with pyproject.toml, src/ layout, and basic package structure",
        "milestone": "Milestone 1: Project Setup",
        "owner_type": "agent",
        "suggested_engine": "claude_code",
        "depends_on": [],
        "files_or_modules": ["pyproject.toml", "src/aab/__init__.py"],
        "acceptance_criteria": [
            "poetry install works without errors",
            "Package can be imported: import aab",
            "Basic CLI entry point defined"
        ],
        "estimated_complexity": "low",
        "status": "pending"
    },
    {
        "id": generate_task_id(),
        "title": "Add core dependencies",
        "description": "Add click for CLI, pyyaml for config, httpx for HTTP transport",
        "milestone": "Milestone 1: Project Setup",
        "owner_type": "agent",
        "suggested_engine": "claude_code",
        "depends_on": [],
        "files_or_modules": ["pyproject.toml"],
        "acceptance_criteria": [
            "All dependencies install successfully",
            "No version conflicts"
        ],
        "estimated_complexity": "low",
        "status": "pending"
    },
    {
        "id": generate_task_id(),
        "title": "Define test case schema",
        "description": "Create Pydantic models for test case structure: task_description, attack_category, canary_indicators, rubrics",
        "milestone": "Milestone 2: Test Case Schema",
        "owner_type": "agent",
        "suggested_engine": "claude_code",
        "depends_on": [],
        "files_or_modules": ["src/aab/schema.py"],
        "acceptance_criteria": [
            "TestCase Pydantic model defined with all fields",
            "YAML deserialization works",
            "Schema validation catches malformed test cases"
        ],
        "estimated_complexity": "medium",
        "status": "pending"
    },
    {
        "id": generate_task_id(),
        "title": "Implement CLI skeleton",
        "description": "Build Click CLI with run, list, and validate commands",
        "milestone": "Milestone 3: CLI Runner",
        "owner_type": "agent",
        "suggested_engine": "claude_code",
        "depends_on": [],
        "files_or_modules": ["src/aab/cli.py"],
        "acceptance_criteria": [
            "aab --help shows all commands",
            "aab run --help shows usage",
            "aab list --help shows usage"
        ],
        "estimated_complexity": "low",
        "status": "pending"
    },
]

# Insert into database
plan_id = store.insert_execution_plan(plan, tasks)
print(f"✓ Created test execution plan: {plan_id}")
print(f"  Milestones: {len(plan['milestones'])}")
print(f"  Tasks: {len(tasks)}")
