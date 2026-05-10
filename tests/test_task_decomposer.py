"""Tests for AI-powered task decomposition."""

from __future__ import annotations

import dataclasses
from unittest.mock import Mock, patch

import pytest

from blueprint.ai.task_decomposer import (
    DecompositionResult,
    Subtask,
    TaskDecomposer,
    TaskType,
)


# Mock response fixtures
MOCK_FEATURE_RESPONSE = """{
  "subtasks": [
    {
      "title": "Design database schema",
      "description": "Create tables for user authentication with email, password hash, and metadata",
      "estimated_effort": "2-4 hours",
      "dependencies": [],
      "required_skills": ["Database design", "SQL"],
      "acceptance_criteria": [
        "Schema supports email and password authentication",
        "Proper indexes for query performance",
        "Migration script created and tested"
      ],
      "risks": ["Schema changes may require data migration"]
    },
    {
      "title": "Implement authentication API endpoints",
      "description": "Create /login and /logout endpoints with JWT token generation",
      "estimated_effort": "4-6 hours",
      "dependencies": ["Design database schema"],
      "required_skills": ["Python", "FastAPI", "JWT"],
      "acceptance_criteria": [
        "Login endpoint validates credentials and returns JWT",
        "Logout endpoint invalidates tokens",
        "Proper error handling for invalid credentials"
      ],
      "risks": ["Token security and expiration handling"]
    },
    {
      "title": "Add authentication middleware",
      "description": "Create middleware to verify JWT tokens on protected routes",
      "estimated_effort": "3-4 hours",
      "dependencies": ["Implement authentication API endpoints"],
      "required_skills": ["Python", "Middleware", "Security"],
      "acceptance_criteria": [
        "Middleware validates JWT on protected routes",
        "Proper error responses for invalid/expired tokens",
        "Performance impact is minimal"
      ],
      "risks": ["Performance overhead on every request"]
    }
  ],
  "overall_risks": [
    "Security vulnerabilities in authentication flow",
    "Token storage and management complexity",
    "Integration with existing user system"
  ],
  "recommendations": [
    "Use established JWT library rather than custom implementation",
    "Add comprehensive security testing",
    "Consider rate limiting for login endpoints"
  ],
  "estimated_total_effort": "3-5 days"
}"""

MOCK_BUG_FIX_RESPONSE = """{
  "subtasks": [
    {
      "title": "Reproduce and isolate the bug",
      "description": "Create minimal test case that triggers the memory leak consistently",
      "estimated_effort": "2-3 hours",
      "dependencies": [],
      "required_skills": ["Debugging", "Profiling"],
      "acceptance_criteria": [
        "Consistent reproduction steps documented",
        "Memory profiling shows the leak pattern"
      ],
      "risks": ["May be difficult to reproduce in isolation"]
    },
    {
      "title": "Identify root cause",
      "description": "Use profiling tools to identify which objects are not being released",
      "estimated_effort": "3-4 hours",
      "dependencies": ["Reproduce and isolate the bug"],
      "required_skills": ["Memory profiling", "Python internals"],
      "acceptance_criteria": [
        "Root cause identified and documented",
        "Specific code path causing leak found"
      ],
      "risks": ["Root cause may be in third-party library"]
    },
    {
      "title": "Implement fix",
      "description": "Fix the memory leak by properly releasing resources",
      "estimated_effort": "2-4 hours",
      "dependencies": ["Identify root cause"],
      "required_skills": ["Python", "Memory management"],
      "acceptance_criteria": [
        "Memory leak resolved in test case",
        "No new memory leaks introduced",
        "Code review approved"
      ],
      "risks": ["Fix may impact performance"]
    }
  ],
  "overall_risks": [
    "Fix may introduce regressions",
    "Leak may be symptom of larger design issue"
  ],
  "recommendations": [
    "Add memory profiling to CI pipeline",
    "Review similar code patterns for same issue"
  ],
  "estimated_total_effort": "1-2 days"
}"""

MOCK_RESEARCH_RESPONSE = """{
  "subtasks": [
    {
      "title": "Survey existing caching solutions",
      "description": "Research Redis, Memcached, and in-memory caching options",
      "estimated_effort": "4-6 hours",
      "dependencies": [],
      "required_skills": ["Research", "System design"],
      "acceptance_criteria": [
        "Comparison matrix of 3+ solutions created",
        "Performance benchmarks documented",
        "Cost analysis completed"
      ],
      "risks": ["May discover no solution fits requirements"]
    },
    {
      "title": "Prototype preferred solution",
      "description": "Build proof-of-concept with top candidate",
      "estimated_effort": "1-2 days",
      "dependencies": ["Survey existing caching solutions"],
      "required_skills": ["Python", "Caching systems"],
      "acceptance_criteria": [
        "Working prototype demonstrates key features",
        "Performance meets requirements",
        "Integration complexity assessed"
      ],
      "risks": ["Prototype may reveal unforeseen issues"]
    }
  ],
  "overall_risks": [
    "Selected solution may not scale",
    "Integration more complex than anticipated"
  ],
  "recommendations": [
    "Start with simplest solution that meets requirements",
    "Plan for migration path if needs change"
  ],
  "estimated_total_effort": "2-3 days"
}"""


@pytest.fixture
def mock_anthropic_client():
    """Create a mock Anthropic client."""
    with patch("blueprint.ai.task_decomposer.Anthropic") as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def decomposer(mock_anthropic_client):
    """Create a TaskDecomposer with mocked API."""
    return TaskDecomposer(api_key="test-api-key")


class TestTaskDecomposerInit:
    """Test TaskDecomposer initialization."""

    def test_init_with_api_key(self, mock_anthropic_client):
        """Test initialization with explicit API key."""
        decomposer = TaskDecomposer(api_key="test-key")
        assert decomposer.api_key == "test-key"
        assert decomposer.model == "claude-sonnet-4-5"
        assert decomposer.temperature == 1.0

    def test_init_with_custom_model(self, mock_anthropic_client):
        """Test initialization with custom model."""
        decomposer = TaskDecomposer(api_key="test-key", model="claude-opus-4-6")
        assert decomposer.model == "claude-opus-4-6"

    def test_init_with_custom_temperature(self, mock_anthropic_client):
        """Test initialization with custom temperature."""
        decomposer = TaskDecomposer(api_key="test-key", temperature=0.7)
        assert decomposer.temperature == 0.7

    def test_init_without_api_key_raises_error(self):
        """Test that missing API key raises ValueError."""
        with patch("blueprint.ai.task_decomposer.os.environ.get", return_value=None):
            with pytest.raises(ValueError, match="API key required"):
                TaskDecomposer()

    def test_init_with_env_var(self, mock_anthropic_client):
        """Test initialization from ANTHROPIC_API_KEY environment variable."""
        with patch("blueprint.ai.task_decomposer.os.environ.get", return_value="env-api-key"):
            decomposer = TaskDecomposer()
            assert decomposer.api_key == "env-api-key"


class TestDecomposeTask:
    """Test task decomposition functionality."""

    def test_decompose_feature_task(self, decomposer, mock_anthropic_client):
        """Test decomposing a feature implementation task."""
        # Setup mock response
        mock_message = Mock()
        mock_block = Mock()
        mock_block.type = "text"
        mock_block.text = MOCK_FEATURE_RESPONSE
        mock_message.content = [mock_block]
        decomposer.client.messages.create.return_value = mock_message

        # Decompose task
        result = decomposer.decompose_task(
            task_description="Add user authentication to the application",
            task_type=TaskType.FEATURE,
        )

        # Verify result structure
        assert isinstance(result, DecompositionResult)
        assert result.original_task == "Add user authentication to the application"
        assert result.task_type == TaskType.FEATURE
        assert len(result.subtasks) == 3
        assert len(result.overall_risks) == 3
        assert len(result.recommendations) == 3
        assert result.estimated_total_effort == "3-5 days"

        # Verify first subtask
        subtask = result.subtasks[0]
        assert subtask.title == "Design database schema"
        assert "authentication" in subtask.description.lower()
        assert subtask.estimated_effort == "2-4 hours"
        assert len(subtask.dependencies) == 0
        assert "Database design" in subtask.required_skills
        assert len(subtask.acceptance_criteria) == 3
        assert len(subtask.risks) == 1

        # Verify API was called correctly
        decomposer.client.messages.create.assert_called_once()
        call_kwargs = decomposer.client.messages.create.call_args[1]
        assert call_kwargs["model"] == "claude-sonnet-4-5"
        assert call_kwargs["temperature"] == 1.0
        assert "Add user authentication" in call_kwargs["messages"][0]["content"]

    def test_decompose_bug_fix_task(self, decomposer, mock_anthropic_client):
        """Test decomposing a bug fix task."""
        # Setup mock response
        mock_message = Mock()
        mock_block = Mock()
        mock_block.type = "text"
        mock_block.text = MOCK_BUG_FIX_RESPONSE
        mock_message.content = [mock_block]
        decomposer.client.messages.create.return_value = mock_message

        # Decompose task
        result = decomposer.decompose_task(
            task_description="Fix memory leak in background worker",
            task_type=TaskType.BUG_FIX,
        )

        # Verify result
        assert result.task_type == TaskType.BUG_FIX
        assert len(result.subtasks) == 3
        assert "Reproduce and isolate the bug" in result.subtasks[0].title
        assert result.estimated_total_effort == "1-2 days"

    def test_decompose_research_task(self, decomposer, mock_anthropic_client):
        """Test decomposing a research task."""
        # Setup mock response
        mock_message = Mock()
        mock_block = Mock()
        mock_block.type = "text"
        mock_block.text = MOCK_RESEARCH_RESPONSE
        mock_message.content = [mock_block]
        decomposer.client.messages.create.return_value = mock_message

        # Decompose task
        result = decomposer.decompose_task(
            task_description="Research caching solutions for API",
            task_type=TaskType.RESEARCH,
        )

        # Verify result
        assert result.task_type == TaskType.RESEARCH
        assert len(result.subtasks) == 2
        assert "Survey existing caching solutions" in result.subtasks[0].title

    def test_decompose_with_string_task_type(self, decomposer, mock_anthropic_client):
        """Test that string task types are converted to enum."""
        # Setup mock response
        mock_message = Mock()
        mock_block = Mock()
        mock_block.type = "text"
        mock_block.text = MOCK_FEATURE_RESPONSE
        mock_message.content = [mock_block]
        decomposer.client.messages.create.return_value = mock_message

        # Decompose with string type
        result = decomposer.decompose_task(
            task_description="Add feature",
            task_type="feature",
        )

        assert result.task_type == TaskType.FEATURE

    def test_decompose_with_context(self, decomposer, mock_anthropic_client):
        """Test decomposing with additional context."""
        # Setup mock response
        mock_message = Mock()
        mock_block = Mock()
        mock_block.type = "text"
        mock_block.text = MOCK_FEATURE_RESPONSE
        mock_message.content = [mock_block]
        decomposer.client.messages.create.return_value = mock_message

        # Decompose with context
        context = "Codebase uses FastAPI and PostgreSQL. Team has 3 Python developers."
        result = decomposer.decompose_task(
            task_description="Add authentication",
            task_type=TaskType.FEATURE,
            context=context,
        )

        # Verify context was included in prompt
        call_kwargs = decomposer.client.messages.create.call_args[1]
        prompt = call_kwargs["messages"][0]["content"]
        assert "FastAPI" in prompt
        assert "PostgreSQL" in prompt

    def test_decompose_with_markdown_json_response(self, decomposer, mock_anthropic_client):
        """Test parsing JSON wrapped in markdown code blocks."""
        # Setup mock response with markdown
        mock_message = Mock()
        mock_block = Mock()
        mock_block.type = "text"
        mock_block.text = f"```json\n{MOCK_FEATURE_RESPONSE}\n```"
        mock_message.content = [mock_block]
        decomposer.client.messages.create.return_value = mock_message

        # Should successfully parse
        result = decomposer.decompose_task(
            task_description="Add feature",
            task_type=TaskType.FEATURE,
        )

        assert len(result.subtasks) == 3

    def test_decompose_with_invalid_json_raises_error(self, decomposer, mock_anthropic_client):
        """Test that invalid JSON raises ValueError."""
        # Setup mock response with invalid JSON
        mock_message = Mock()
        mock_block = Mock()
        mock_block.type = "text"
        mock_block.text = "This is not JSON"
        mock_message.content = [mock_block]
        decomposer.client.messages.create.return_value = mock_message

        # Should raise ValueError
        with pytest.raises(ValueError, match="Failed to parse LLM response"):
            decomposer.decompose_task(
                task_description="Add feature",
                task_type=TaskType.FEATURE,
            )


class TestRefineSubtasks:
    """Test iterative refinement functionality."""

    def test_refine_subtasks(self, decomposer, mock_anthropic_client):
        """Test refining subtasks based on feedback."""
        # Setup initial result
        initial_subtasks = [
            Subtask(
                title="Task 1",
                description="Initial description",
                estimated_effort="1 day",
                dependencies=[],
                required_skills=["Python"],
                acceptance_criteria=["Criterion 1"],
                risks=[],
            )
        ]
        original_result = DecompositionResult(
            original_task="Original task",
            task_type=TaskType.FEATURE,
            subtasks=initial_subtasks,
            overall_risks=[],
            recommendations=[],
            estimated_total_effort="1 day",
        )

        # Setup mock refined response
        refined_response = """{
  "subtasks": [
    {
      "title": "Refined Task 1",
      "description": "More detailed description",
      "estimated_effort": "2 days",
      "dependencies": [],
      "required_skills": ["Python", "Testing"],
      "acceptance_criteria": ["Criterion 1", "Criterion 2"],
      "risks": ["New risk identified"]
    }
  ],
  "overall_risks": ["Overall risk"],
  "recommendations": ["New recommendation"],
  "estimated_total_effort": "2 days"
}"""
        mock_message = Mock()
        mock_block = Mock()
        mock_block.type = "text"
        mock_block.text = refined_response
        mock_message.content = [mock_block]
        decomposer.client.messages.create.return_value = mock_message

        # Refine
        refined_result = decomposer.refine_subtasks(
            original_result=original_result,
            feedback="Please add more detail and testing considerations",
        )

        # Verify refinement
        assert refined_result.original_task == "Original task"
        assert len(refined_result.subtasks) == 1
        assert refined_result.subtasks[0].title == "Refined Task 1"
        assert "Testing" in refined_result.subtasks[0].required_skills
        assert len(refined_result.subtasks[0].acceptance_criteria) == 2
        assert refined_result.estimated_total_effort == "2 days"

        # Verify feedback was included in prompt
        call_kwargs = decomposer.client.messages.create.call_args[1]
        prompt = call_kwargs["messages"][0]["content"]
        assert "add more detail" in prompt
        assert "testing considerations" in prompt


class TestTaskTypeTemplates:
    """Test that all task types have templates."""

    def test_all_task_types_have_templates(self):
        """Verify all TaskType enum values have corresponding templates."""
        for task_type in TaskType:
            assert task_type in TaskDecomposer.TASK_TEMPLATES

    def test_feature_template_structure(self):
        """Verify feature template has expected components."""
        template = TaskDecomposer.TASK_TEMPLATES[TaskType.FEATURE]
        assert "{task_description}" in template
        assert "subtasks" in template
        assert "dependencies" in template
        assert "acceptance_criteria" in template

    def test_bug_fix_template_mentions_investigation(self):
        """Verify bug fix template includes investigation steps."""
        template = TaskDecomposer.TASK_TEMPLATES[TaskType.BUG_FIX]
        assert "investigation" in template.lower() or "reproduce" in template.lower()

    def test_research_template_mentions_deliverables(self):
        """Verify research template focuses on deliverables."""
        template = TaskDecomposer.TASK_TEMPLATES[TaskType.RESEARCH]
        assert "deliverable" in template.lower() or "outcome" in template.lower()


class TestDataClasses:
    """Test dataclass serialization and structure."""

    def test_subtask_to_dict(self):
        """Test Subtask serialization to dictionary."""
        subtask = Subtask(
            title="Test Task",
            description="Test description",
            estimated_effort="2 hours",
            dependencies=["Other task"],
            required_skills=["Python", "Testing"],
            acceptance_criteria=["AC 1", "AC 2"],
            risks=["Risk 1"],
        )

        result = subtask.to_dict()

        assert result["title"] == "Test Task"
        assert result["description"] == "Test description"
        assert result["estimated_effort"] == "2 hours"
        assert result["dependencies"] == ["Other task"]
        assert result["required_skills"] == ["Python", "Testing"]
        assert result["acceptance_criteria"] == ["AC 1", "AC 2"]
        assert result["risks"] == ["Risk 1"]

    def test_decomposition_result_to_dict(self):
        """Test DecompositionResult serialization to dictionary."""
        subtask = Subtask(
            title="Task",
            description="Description",
            estimated_effort="1 day",
            dependencies=[],
            required_skills=["Python"],
            acceptance_criteria=["AC"],
            risks=[],
        )

        result = DecompositionResult(
            original_task="Original",
            task_type=TaskType.FEATURE,
            subtasks=[subtask],
            overall_risks=["Risk"],
            recommendations=["Recommendation"],
            estimated_total_effort="1 day",
        )

        dict_result = result.to_dict()

        assert dict_result["original_task"] == "Original"
        assert dict_result["task_type"] == "feature"
        assert len(dict_result["subtasks"]) == 1
        assert dict_result["subtasks"][0]["title"] == "Task"
        assert dict_result["overall_risks"] == ["Risk"]
        assert dict_result["recommendations"] == ["Recommendation"]
        assert dict_result["estimated_total_effort"] == "1 day"

    def test_subtask_immutability(self):
        """Test that Subtask is immutable (frozen)."""
        subtask = Subtask(
            title="Task",
            description="Description",
            estimated_effort="1 day",
        )

        with pytest.raises(dataclasses.FrozenInstanceError):
            subtask.title = "New title"  # type: ignore[misc]

    def test_decomposition_result_immutability(self):
        """Test that DecompositionResult is immutable (frozen)."""
        result = DecompositionResult(
            original_task="Task",
            task_type=TaskType.FEATURE,
            subtasks=[],
        )

        with pytest.raises(dataclasses.FrozenInstanceError):
            result.original_task = "New task"  # type: ignore[misc]


class TestMultipleContentBlocks:
    """Test handling of multiple content blocks in API response."""

    def test_multiple_text_blocks_concatenated(self, decomposer, mock_anthropic_client):
        """Test that multiple text blocks are concatenated."""
        # Setup mock response with multiple blocks
        # Split the JSON in the middle of a string value for realistic concatenation
        split_point = len(MOCK_FEATURE_RESPONSE) // 2
        mock_message = Mock()
        mock_block1 = Mock()
        mock_block1.type = "text"
        mock_block1.text = MOCK_FEATURE_RESPONSE[:split_point]
        mock_block2 = Mock()
        mock_block2.type = "text"
        mock_block2.text = MOCK_FEATURE_RESPONSE[split_point:]
        mock_message.content = [mock_block1, mock_block2]
        decomposer.client.messages.create.return_value = mock_message

        # Should successfully parse concatenated blocks
        result = decomposer.decompose_task(
            task_description="Add feature",
            task_type=TaskType.FEATURE,
        )

        assert len(result.subtasks) == 3
