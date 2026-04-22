"""Create a coherent test execution plan that matches its brief."""

import sys
sys.path.insert(0, 'src')

from blueprint.config import get_config
from blueprint.store import Store
from blueprint.generators.plan_generator import generate_execution_plan_id, generate_task_id

# Initialize
config = get_config()
store = Store(config.db_path)

# Get the fn-call-harness implementation brief
brief_id = "ib-64587ecd044f"
brief = store.get_implementation_brief(brief_id)

if not brief:
    print(f"Brief not found: {brief_id}")
    sys.exit(1)

print(f"Creating coherent plan for: {brief['title']}")

# Create a plan that MATCHES the fn-call-harness brief
plan = {
    "id": generate_execution_plan_id(),
    "implementation_brief_id": brief["id"],
    "target_engine": "claude_code",
    "target_repo": "fn-call-harness",
    "project_type": "python_library",
    "milestones": [
        {
            "name": "Milestone 1: Core Infrastructure",
            "description": "Set up project and implement wrapper interfaces for OpenAI and Anthropic",
        },
        {
            "name": "Milestone 2: Validation & Repair",
            "description": "Build JSON schema validation engine and auto-repair pipeline",
        },
        {
            "name": "Milestone 3: Polish & Ship",
            "description": "Add telemetry, documentation, tests, and publish to PyPI",
        }
    ],
    "test_strategy": "Unit tests for each repair strategy with pytest, integration tests against live OpenAI and Anthropic APIs, >80% code coverage",
    "handoff_prompt": "Build a Python library that wraps LLM clients to provide reliable function calling. Focus on practical repair heuristics for common failure modes.",
    "status": "draft",
    "generation_model": "manual",
    "generation_tokens": 0,
    "generation_prompt": "Manually created coherent test plan",
}

tasks = [
    # Milestone 1
    {
        "id": generate_task_id(),
        "title": "Initialize fn-call-harness Python project",
        "description": "Set up Poetry project with pyproject.toml, src/fn_call_harness/ layout, MIT license, README",
        "milestone": "Milestone 1: Core Infrastructure",
        "owner_type": "agent",
        "suggested_engine": "claude_code",
        "depends_on": [],
        "files_or_modules": ["pyproject.toml", "src/fn_call_harness/__init__.py", "README.md"],
        "acceptance_criteria": [
            "poetry install works without errors",
            "Package can be imported: from fn_call_harness import FunctionCallHarness",
            "README has quickstart example"
        ],
        "estimated_complexity": "low",
        "status": "pending"
    },
    {
        "id": generate_task_id(),
        "title": "Implement OpenAI wrapper class",
        "description": "Create OpenAIHarness that wraps openai.Client, intercepts function call responses, stores call/response for validation",
        "milestone": "Milestone 1: Core Infrastructure",
        "owner_type": "agent",
        "suggested_engine": "claude_code",
        "depends_on": [],
        "files_or_modules": ["src/fn_call_harness/wrappers/openai_wrapper.py"],
        "acceptance_criteria": [
            "OpenAIHarness.__init__ accepts openai.Client instance",
            "chat.completions.create() proxies to wrapped client",
            "Function call responses captured for validation"
        ],
        "estimated_complexity": "medium",
        "status": "pending"
    },
    {
        "id": generate_task_id(),
        "title": "Implement Anthropic wrapper class",
        "description": "Create AnthropicHarness that wraps anthropic.Anthropic, intercepts tool use responses",
        "milestone": "Milestone 1: Core Infrastructure",
        "owner_type": "agent",
        "suggested_engine": "claude_code",
        "depends_on": [],
        "files_or_modules": ["src/fn_call_harness/wrappers/anthropic_wrapper.py"],
        "acceptance_criteria": [
            "AnthropicHarness.__init__ accepts anthropic.Anthropic instance",
            "messages.create() proxies to wrapped client",
            "Tool use blocks captured for validation"
        ],
        "estimated_complexity": "medium",
        "status": "pending"
    },
    # Milestone 2
    {
        "id": generate_task_id(),
        "title": "Build JSON schema validation engine",
        "description": "Create validator using jsonschema library, validate tool args against schemas, format errors for retry prompts",
        "milestone": "Milestone 2: Validation & Repair",
        "owner_type": "agent",
        "suggested_engine": "claude_code",
        "depends_on": [],
        "files_or_modules": ["src/fn_call_harness/validation/validator.py"],
        "acceptance_criteria": [
            "validate(args, schema) returns success/failure with detailed errors",
            "Error messages are actionable for LLM retry prompts",
            "Handles JSON Schema draft 7+"
        ],
        "estimated_complexity": "medium",
        "status": "pending"
    },
    {
        "id": generate_task_id(),
        "title": "Implement auto-repair pipeline",
        "description": "Build chain of repair strategies: extract JSON from markdown, parse double-stringified JSON, coerce types, inject defaults",
        "milestone": "Milestone 2: Validation & Repair",
        "owner_type": "agent",
        "suggested_engine": "claude_code",
        "depends_on": [],
        "files_or_modules": ["src/fn_call_harness/repair/pipeline.py", "src/fn_call_harness/repair/strategies.py"],
        "acceptance_criteria": [
            "Each repair strategy implements try_repair(args, schema) interface",
            "Pipeline applies strategies in order until validation passes",
            "Logs which repairs were applied for debugging"
        ],
        "estimated_complexity": "high",
        "status": "pending"
    },
    {
        "id": generate_task_id(),
        "title": "Add retry logic with error feedback",
        "description": "Implement configurable retry (max 3 attempts) that re-prompts with validation errors appended to conversation",
        "milestone": "Milestone 2: Validation & Repair",
        "owner_type": "agent",
        "suggested_engine": "claude_code",
        "depends_on": [],
        "files_or_modules": ["src/fn_call_harness/retry.py"],
        "acceptance_criteria": [
            "Retry count configurable (default 2)",
            "Validation errors injected as user message for retry",
            "Gives up after max attempts with structured exception"
        ],
        "estimated_complexity": "medium",
        "status": "pending"
    },
    # Milestone 3
    {
        "id": generate_task_id(),
        "title": "Add in-memory telemetry",
        "description": "Track success/failure per model+tool, export as JSON for analysis",
        "milestone": "Milestone 3: Polish & Ship",
        "owner_type": "agent",
        "suggested_engine": "claude_code",
        "depends_on": [],
        "files_or_modules": ["src/fn_call_harness/telemetry.py"],
        "acceptance_criteria": [
            "Counters track: calls, successes, failures, repairs applied",
            "export_telemetry() returns JSON with per-model and per-tool stats",
            "Thread-safe counter implementation"
        ],
        "estimated_complexity": "low",
        "status": "pending"
    },
    {
        "id": generate_task_id(),
        "title": "Write comprehensive tests",
        "description": "Unit tests for all repair strategies, integration tests with live APIs, >80% coverage",
        "milestone": "Milestone 3: Polish & Ship",
        "owner_type": "human",
        "suggested_engine": "manual",
        "depends_on": [],
        "files_or_modules": ["tests/test_repair_strategies.py", "tests/test_integration.py"],
        "acceptance_criteria": [
            "20+ known failure cases with >90% repair success rate",
            "Integration tests run against OpenAI and Anthropic",
            "pytest --cov shows >80% coverage"
        ],
        "estimated_complexity": "high",
        "status": "pending"
    },
    {
        "id": generate_task_id(),
        "title": "Write documentation and publish",
        "description": "Quickstart guide, API reference, 3 examples, publish to PyPI as v0.1.0",
        "milestone": "Milestone 3: Polish & Ship",
        "owner_type": "human",
        "suggested_engine": "manual",
        "depends_on": [],
        "files_or_modules": ["docs/quickstart.md", "docs/api.md", "docs/examples/"],
        "acceptance_criteria": [
            "Docs cover installation, basic usage, all 4 repair strategies",
            "3 real-world examples with before/after code",
            "Published to PyPI and installable via pip"
        ],
        "estimated_complexity": "medium",
        "status": "pending"
    },
]

# Insert into database
plan_id = store.insert_execution_plan(plan, tasks)
print(f"✓ Created coherent execution plan: {plan_id}")
print(f"  Brief: {brief['title']}")
print(f"  Repo: {plan['target_repo']}")
print(f"  Milestones: {len(plan['milestones'])}")
print(f"  Tasks: {len(tasks)}")
print(f"\nTasks match brief content: fn-call-harness specific!")
