"""AI-powered task decomposition for breaking down complex tasks into subtasks."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal

from anthropic import Anthropic


class TaskType(str, Enum):
    """Types of tasks for template selection."""

    FEATURE = "feature"
    BUG_FIX = "bug_fix"
    RESEARCH = "research"
    REFACTORING = "refactoring"
    INFRASTRUCTURE = "infrastructure"


@dataclass(frozen=True, slots=True)
class Subtask:
    """A generated subtask with metadata."""

    title: str
    description: str
    estimated_effort: str  # e.g., "2-4 hours", "1-2 days"
    dependencies: list[str] = field(default_factory=list)
    required_skills: list[str] = field(default_factory=list)
    acceptance_criteria: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "title": self.title,
            "description": self.description,
            "estimated_effort": self.estimated_effort,
            "dependencies": self.dependencies,
            "required_skills": self.required_skills,
            "acceptance_criteria": self.acceptance_criteria,
            "risks": self.risks,
        }


@dataclass(frozen=True, slots=True)
class DecompositionResult:
    """Result of task decomposition analysis."""

    original_task: str
    task_type: TaskType
    subtasks: list[Subtask]
    overall_risks: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    estimated_total_effort: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "original_task": self.original_task,
            "task_type": self.task_type.value,
            "subtasks": [st.to_dict() for st in self.subtasks],
            "overall_risks": self.overall_risks,
            "recommendations": self.recommendations,
            "estimated_total_effort": self.estimated_total_effort,
        }


class TaskDecomposer:
    """
    AI-powered task decomposition using LLM analysis.

    Breaks down complex task descriptions into actionable subtasks with:
    - Effort estimates
    - Dependencies
    - Required skills
    - Acceptance criteria
    - Risk identification
    """

    # Template prompts for different task types
    TASK_TEMPLATES = {
        TaskType.FEATURE: """You are a senior software engineer decomposing a feature implementation task.

Task Description: {task_description}

Analyze this feature request and break it down into concrete, actionable subtasks. For each subtask:
1. Provide a clear title and detailed description
2. Estimate effort (e.g., "2-4 hours", "1-2 days", "3-5 days")
3. Identify dependencies on other subtasks (use subtask titles)
4. List required skills/expertise (e.g., "Python", "React", "Database design")
5. Define 2-4 specific, testable acceptance criteria
6. Identify potential risks or blockers

Also provide:
- Overall risks for the entire feature
- Recommendations for implementation approach
- Total estimated effort range

Return your analysis as a structured JSON object with this exact format:
{{
  "subtasks": [
    {{
      "title": "Subtask title",
      "description": "Detailed description",
      "estimated_effort": "X-Y hours/days",
      "dependencies": ["Other subtask title"],
      "required_skills": ["Skill1", "Skill2"],
      "acceptance_criteria": ["Criterion 1", "Criterion 2"],
      "risks": ["Risk or blocker"]
    }}
  ],
  "overall_risks": ["Overall risk 1", "Overall risk 2"],
  "recommendations": ["Recommendation 1", "Recommendation 2"],
  "estimated_total_effort": "X-Y days/weeks"
}}

Focus on practical, implementable subtasks that a developer can execute.""",
        TaskType.BUG_FIX: """You are a senior software engineer decomposing a bug fix task.

Task Description: {task_description}

Analyze this bug fix and break it down into investigation and resolution steps. For each subtask:
1. Provide a clear title and detailed description
2. Estimate effort (e.g., "1-2 hours", "2-4 hours")
3. Identify dependencies on other subtasks
4. List required skills/expertise (e.g., "Debugging", "System architecture")
5. Define 2-4 specific, testable acceptance criteria
6. Identify potential risks or complications

Also provide:
- Overall risks for the bug fix
- Recommendations for investigation and testing approach
- Total estimated effort range

Return your analysis as a structured JSON object with this exact format:
{{
  "subtasks": [
    {{
      "title": "Subtask title",
      "description": "Detailed description",
      "estimated_effort": "X-Y hours",
      "dependencies": ["Other subtask title"],
      "required_skills": ["Skill1", "Skill2"],
      "acceptance_criteria": ["Criterion 1", "Criterion 2"],
      "risks": ["Risk or blocker"]
    }}
  ],
  "overall_risks": ["Overall risk 1", "Overall risk 2"],
  "recommendations": ["Recommendation 1", "Recommendation 2"],
  "estimated_total_effort": "X-Y hours/days"
}}

Include investigation, root cause analysis, fix implementation, and testing steps.""",
        TaskType.RESEARCH: """You are a senior software engineer decomposing a research task.

Task Description: {task_description}

Analyze this research task and break it down into investigation phases. For each subtask:
1. Provide a clear title and detailed description
2. Estimate effort (e.g., "2-4 hours", "1 day")
3. Identify dependencies on other subtasks
4. List required skills/expertise (e.g., "Research", "Technical writing")
5. Define 2-4 specific deliverables or outcomes
6. Identify potential risks or unknowns

Also provide:
- Overall risks for the research effort
- Recommendations for approach and documentation
- Total estimated effort range

Return your analysis as a structured JSON object with this exact format:
{{
  "subtasks": [
    {{
      "title": "Subtask title",
      "description": "Detailed description",
      "estimated_effort": "X-Y hours/days",
      "dependencies": ["Other subtask title"],
      "required_skills": ["Skill1", "Skill2"],
      "acceptance_criteria": ["Deliverable 1", "Deliverable 2"],
      "risks": ["Risk or unknown"]
    }}
  ],
  "overall_risks": ["Overall risk 1", "Overall risk 2"],
  "recommendations": ["Recommendation 1", "Recommendation 2"],
  "estimated_total_effort": "X-Y days"
}}

Focus on information gathering, analysis, prototyping, and documentation.""",
        TaskType.REFACTORING: """You are a senior software engineer decomposing a refactoring task.

Task Description: {task_description}

Analyze this refactoring effort and break it down into safe, incremental steps. For each subtask:
1. Provide a clear title and detailed description
2. Estimate effort (e.g., "2-4 hours", "1-2 days")
3. Identify dependencies on other subtasks
4. List required skills/expertise (e.g., "Code analysis", "Testing")
5. Define 2-4 specific acceptance criteria (focus on maintaining behavior)
6. Identify potential risks (regression, breaking changes)

Also provide:
- Overall risks for the refactoring
- Recommendations for safe execution and testing
- Total estimated effort range

Return your analysis as a structured JSON object with this exact format:
{{
  "subtasks": [
    {{
      "title": "Subtask title",
      "description": "Detailed description",
      "estimated_effort": "X-Y hours/days",
      "dependencies": ["Other subtask title"],
      "required_skills": ["Skill1", "Skill2"],
      "acceptance_criteria": ["Criterion 1", "Criterion 2"],
      "risks": ["Risk or blocker"]
    }}
  ],
  "overall_risks": ["Overall risk 1", "Overall risk 2"],
  "recommendations": ["Recommendation 1", "Recommendation 2"],
  "estimated_total_effort": "X-Y days"
}}

Emphasize test coverage, incremental changes, and verification steps.""",
        TaskType.INFRASTRUCTURE: """You are a senior software engineer decomposing an infrastructure task.

Task Description: {task_description}

Analyze this infrastructure work and break it down into setup and deployment steps. For each subtask:
1. Provide a clear title and detailed description
2. Estimate effort (e.g., "4-8 hours", "1-2 days")
3. Identify dependencies on other subtasks
4. List required skills/expertise (e.g., "DevOps", "Cloud architecture")
5. Define 2-4 specific acceptance criteria
6. Identify potential risks (downtime, security, configuration issues)

Also provide:
- Overall risks for the infrastructure change
- Recommendations for deployment and rollback strategy
- Total estimated effort range

Return your analysis as a structured JSON object with this exact format:
{{
  "subtasks": [
    {{
      "title": "Subtask title",
      "description": "Detailed description",
      "estimated_effort": "X-Y hours/days",
      "dependencies": ["Other subtask title"],
      "required_skills": ["Skill1", "Skill2"],
      "acceptance_criteria": ["Criterion 1", "Criterion 2"],
      "risks": ["Risk or blocker"]
    }}
  ],
  "overall_risks": ["Overall risk 1", "Overall risk 2"],
  "recommendations": ["Recommendation 1", "Recommendation 2"],
  "estimated_total_effort": "X-Y days/weeks"
}}

Include planning, implementation, testing, and monitoring steps.""",
    }

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "claude-sonnet-4-5",
        temperature: float = 1.0,
    ):
        """
        Initialize the task decomposer.

        Args:
            api_key: Anthropic API key (defaults to ANTHROPIC_API_KEY env var)
            model: Model to use for analysis (default: claude-sonnet-4-5)
            temperature: Temperature for generation (default: 1.0)
        """
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError(
                "API key required: pass api_key parameter or set ANTHROPIC_API_KEY environment variable"
            )

        self.client = Anthropic(api_key=self.api_key)
        self.model = model
        self.temperature = temperature

    def decompose_task(
        self,
        task_description: str,
        task_type: TaskType | Literal["feature", "bug_fix", "research", "refactoring", "infrastructure"] = TaskType.FEATURE,
        context: str | None = None,
    ) -> DecompositionResult:
        """
        Decompose a task description into actionable subtasks.

        Args:
            task_description: High-level description of the task
            task_type: Type of task for template selection
            context: Optional additional context (e.g., codebase info, constraints)

        Returns:
            DecompositionResult with subtasks and analysis
        """
        # Normalize task_type to enum
        if isinstance(task_type, str):
            task_type = TaskType(task_type)

        # Build the prompt from template
        template = self.TASK_TEMPLATES[task_type]
        full_description = task_description
        if context:
            full_description = f"{task_description}\n\nAdditional Context:\n{context}"

        prompt = template.format(task_description=full_description)

        # Call LLM
        response = self._call_llm(prompt)

        # Parse response into structured result
        result = self._parse_response(response, task_description, task_type)
        return result

    def refine_subtasks(
        self,
        original_result: DecompositionResult,
        feedback: str,
    ) -> DecompositionResult:
        """
        Iteratively refine subtasks based on user feedback.

        Args:
            original_result: Previous decomposition result
            feedback: User feedback for refinement

        Returns:
            Updated DecompositionResult
        """
        prompt = f"""You previously decomposed this task:

Original Task: {original_result.original_task}

Your previous analysis generated {len(original_result.subtasks)} subtasks.

User Feedback: {feedback}

Please revise the decomposition based on this feedback. Maintain the same JSON structure:
{{
  "subtasks": [...],
  "overall_risks": [...],
  "recommendations": [...],
  "estimated_total_effort": "..."
}}"""

        response = self._call_llm(prompt)
        result = self._parse_response(response, original_result.original_task, original_result.task_type)
        return result

    def _call_llm(self, prompt: str) -> str:
        """
        Call the Anthropic API with the prompt.

        Args:
            prompt: The prompt to send

        Returns:
            LLM response text
        """
        response = self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            temperature=self.temperature,
            messages=[{"role": "user", "content": prompt}],
        )

        content = ""
        for block in response.content:
            if block.type == "text":
                content += block.text

        return content

    def _parse_response(
        self, response: str, original_task: str, task_type: TaskType
    ) -> DecompositionResult:
        """
        Parse LLM response into structured DecompositionResult.

        Args:
            response: Raw LLM response text
            original_task: Original task description
            task_type: Task type

        Returns:
            Parsed DecompositionResult
        """
        import json

        # Extract JSON from response (handle markdown code blocks)
        response = response.strip()
        if "```json" in response:
            start = response.find("```json") + 7
            end = response.find("```", start)
            response = response[start:end].strip()
        elif "```" in response:
            start = response.find("```") + 3
            end = response.find("```", start)
            response = response[start:end].strip()

        try:
            data = json.loads(response)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse LLM response as JSON: {e}\nResponse: {response[:200]}")

        # Parse subtasks
        subtasks = []
        for st_data in data.get("subtasks", []):
            subtask = Subtask(
                title=st_data.get("title", ""),
                description=st_data.get("description", ""),
                estimated_effort=st_data.get("estimated_effort", ""),
                dependencies=st_data.get("dependencies", []),
                required_skills=st_data.get("required_skills", []),
                acceptance_criteria=st_data.get("acceptance_criteria", []),
                risks=st_data.get("risks", []),
            )
            subtasks.append(subtask)

        return DecompositionResult(
            original_task=original_task,
            task_type=task_type,
            subtasks=subtasks,
            overall_risks=data.get("overall_risks", []),
            recommendations=data.get("recommendations", []),
            estimated_total_effort=data.get("estimated_total_effort", ""),
        )


__all__ = [
    "TaskDecomposer",
    "TaskType",
    "Subtask",
    "DecompositionResult",
]
