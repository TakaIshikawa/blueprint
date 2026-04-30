"""Recommend CI pipeline validation for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


CiPipelineSurface = Literal[
    "github_actions",
    "test_runner_config",
    "python_packaging",
    "container_build",
    "make_targets",
    "pre_commit",
    "release_automation",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_WORKFLOW_RE = re.compile(
    r"\b(?:github actions?|workflow|workflows?|ci pipeline|continuous integration|"
    r"build matrix|actionlint)\b",
    re.IGNORECASE,
)
_TEST_CONFIG_RE = re.compile(
    r"\b(?:pytest|tox|nox|jest|vitest|coverage|test runner|test config|"
    r"addopts|junit|unittest|playwright config)\b",
    re.IGNORECASE,
)
_PACKAGING_RE = re.compile(
    r"\b(?:pyproject|poetry|poetry.lock|dependency|dependencies|package metadata|"
    r"setup.py|setup.cfg|requirements|lockfile|build backend|wheel|sdist)\b",
    re.IGNORECASE,
)
_DOCKER_RE = re.compile(
    r"\b(?:docker|dockerfile|container|image build|compose|buildkit|base image)\b",
    re.IGNORECASE,
)
_MAKE_RE = re.compile(r"\b(?:makefile|make target|make targets|make test|make ci)\b", re.I)
_PRE_COMMIT_RE = re.compile(
    r"\b(?:pre-commit|precommit|lint hook|format hook|commit hook)\b",
    re.IGNORECASE,
)
_RELEASE_RE = re.compile(
    r"\b(?:release automation|release workflow|semantic-release|release-please|"
    r"changelog|version bump|publish|publishing|deploy workflow|tagging)\b",
    re.IGNORECASE,
)
_SURFACE_ORDER: dict[CiPipelineSurface, int] = {
    "github_actions": 0,
    "test_runner_config": 1,
    "python_packaging": 2,
    "container_build": 3,
    "make_targets": 4,
    "pre_commit": 5,
    "release_automation": 6,
}


@dataclass(frozen=True, slots=True)
class TaskCiPipelineImpactRecommendation:
    """CI validation guidance for one execution task and surface."""

    task_id: str
    title: str
    surface: CiPipelineSurface
    failure_mode: str
    validation_command: str
    owner_role: str
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "surface": self.surface,
            "failure_mode": self.failure_mode,
            "validation_command": self.validation_command,
            "owner_role": self.owner_role,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskCiPipelineImpactPlan:
    """CI pipeline impact recommendations for a plan or task collection."""

    plan_id: str | None = None
    recommendations: tuple[TaskCiPipelineImpactRecommendation, ...] = field(
        default_factory=tuple
    )
    ci_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [
                recommendation.to_dict() for recommendation in self.recommendations
            ],
            "ci_task_ids": list(self.ci_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return recommendation records as plain dictionaries."""
        return [recommendation.to_dict() for recommendation in self.recommendations]

    def to_markdown(self) -> str:
        """Render CI pipeline impact recommendations as deterministic Markdown."""
        title = "# Task CI Pipeline Impact Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.recommendations:
            lines.extend(["", "No CI pipeline impacts were derived."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Surface | Failure Mode | Validation Command | Owner Role |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for recommendation in self.recommendations:
            lines.append(
                "| "
                f"`{_markdown_cell(recommendation.task_id)}` | "
                f"{recommendation.surface} | "
                f"{_markdown_cell(recommendation.failure_mode)} | "
                f"`{_markdown_cell(recommendation.validation_command)}` | "
                f"{_markdown_cell(recommendation.owner_role)} |"
            )
        return "\n".join(lines)


def build_task_ci_pipeline_impact_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskCiPipelineImpactPlan:
    """Recommend CI validation for tasks that touch build or workflow behavior."""
    plan_id, tasks = _source_payload(source)
    recommendations = [
        recommendation
        for index, task in enumerate(tasks, start=1)
        for recommendation in _recommendations(task, index)
    ]
    recommendations.sort(
        key=lambda item: (_SURFACE_ORDER[item.surface], item.task_id, item.title.casefold())
    )
    result = tuple(recommendations)
    ci_task_ids = tuple(_dedupe(item.task_id for item in result))
    all_task_ids = tuple(
        _optional_text(task.get("id")) or f"task-{index}"
        for index, task in enumerate(tasks, start=1)
    )
    surface_counts = {
        surface: sum(1 for item in result if item.surface == surface)
        for surface in _SURFACE_ORDER
    }

    return TaskCiPipelineImpactPlan(
        plan_id=plan_id,
        recommendations=result,
        ci_task_ids=ci_task_ids,
        no_impact_task_ids=tuple(task_id for task_id in all_task_ids if task_id not in ci_task_ids),
        summary={
            "task_count": len(tasks),
            "ci_task_count": len(ci_task_ids),
            "recommendation_count": len(result),
            "surface_counts": surface_counts,
        },
    )


def task_ci_pipeline_impact_plan_to_dict(
    result: TaskCiPipelineImpactPlan,
) -> dict[str, Any]:
    """Serialize a CI pipeline impact plan to a plain dictionary."""
    return result.to_dict()


task_ci_pipeline_impact_plan_to_dict.__test__ = False


def task_ci_pipeline_impact_plan_to_markdown(
    result: TaskCiPipelineImpactPlan,
) -> str:
    """Render a CI pipeline impact plan as Markdown."""
    return result.to_markdown()


task_ci_pipeline_impact_plan_to_markdown.__test__ = False


def recommend_task_ci_pipeline_impacts(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskCiPipelineImpactPlan:
    """Compatibility alias for building CI pipeline impact recommendations."""
    return build_task_ci_pipeline_impact_plan(source)


def _recommendations(
    task: Mapping[str, Any],
    index: int,
) -> tuple[TaskCiPipelineImpactRecommendation, ...]:
    signals = _signals(task)
    if not signals:
        return ()

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    return tuple(
        TaskCiPipelineImpactRecommendation(
            task_id=task_id,
            title=title,
            surface=surface,
            failure_mode=_failure_mode(surface),
            validation_command=_validation_command(surface),
            owner_role=_owner_role(surface),
            evidence=tuple(_dedupe(signals[surface])),
        )
        for surface in sorted(signals, key=lambda item: _SURFACE_ORDER[item])
    )


def _signals(task: Mapping[str, Any]) -> dict[CiPipelineSurface, tuple[str, ...]]:
    signals: dict[CiPipelineSurface, list[str]] = {}
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _add_path_signals(signals, path)
    for source_field, text in _task_texts(task):
        _add_text_signals(signals, source_field, text)
    for source_field, text in _metadata_texts(task.get("metadata")):
        _add_text_signals(signals, source_field, text)
    return {
        surface: tuple(_dedupe(evidence))
        for surface, evidence in signals.items()
        if evidence
    }


def _add_path_signals(
    signals: dict[CiPipelineSurface, list[str]],
    original: str,
) -> None:
    normalized = _normalized_path(original)
    folded = normalized.casefold()
    if not folded:
        return
    path = PurePosixPath(folded)
    parts = set(path.parts)
    name = path.name
    evidence = f"files_or_modules: {original}"

    if ".github" in parts and "workflows" in parts:
        _append(signals, "github_actions", evidence)
    if name in {
        "pytest.ini",
        "tox.ini",
        "noxfile.py",
        "jest.config.js",
        "jest.config.ts",
        "vitest.config.js",
        "vitest.config.ts",
        ".coveragerc",
    }:
        _append(signals, "test_runner_config", evidence)
    if name in {
        "pyproject.toml",
        "poetry.lock",
        "setup.py",
        "setup.cfg",
        "requirements.txt",
        "requirements-dev.txt",
        "package.json",
        "package-lock.json",
        "pnpm-lock.yaml",
        "yarn.lock",
    }:
        _append(signals, "python_packaging", evidence)
    if name == "dockerfile" or name.endswith(".dockerfile") or "docker" in parts:
        _append(signals, "container_build", evidence)
    if name == "makefile" or name.endswith(".mk"):
        _append(signals, "make_targets", evidence)
    if name in {".pre-commit-config.yaml", ".pre-commit-config.yml"}:
        _append(signals, "pre_commit", evidence)
    if name in {
        ".releaserc",
        ".releaserc.json",
        "release-please-config.json",
        "semantic-release.config.js",
    } or "release" in name:
        _append(signals, "release_automation", evidence)
    if ".github" in parts and "workflows" in parts and "release" in name:
        _append(signals, "release_automation", evidence)


def _add_text_signals(
    signals: dict[CiPipelineSurface, list[str]],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    if _WORKFLOW_RE.search(text):
        _append(signals, "github_actions", evidence)
    if _TEST_CONFIG_RE.search(text):
        _append(signals, "test_runner_config", evidence)
    if _PACKAGING_RE.search(text):
        _append(signals, "python_packaging", evidence)
    if _DOCKER_RE.search(text):
        _append(signals, "container_build", evidence)
    if _MAKE_RE.search(text):
        _append(signals, "make_targets", evidence)
    if _PRE_COMMIT_RE.search(text):
        _append(signals, "pre_commit", evidence)
    if _RELEASE_RE.search(text):
        _append(signals, "release_automation", evidence)


def _failure_mode(surface: CiPipelineSurface) -> str:
    return {
        "github_actions": "Workflow syntax, job triggers, permissions, or matrix changes can prevent CI from starting or reporting correctly.",
        "test_runner_config": "Test discovery, coverage output, or runner options can change which checks execute in CI.",
        "python_packaging": "Dependency resolution, build metadata, or lockfile drift can break CI setup before tests run.",
        "container_build": "Docker context, base image, or build arguments can break image construction used by CI.",
        "make_targets": "Make target dependencies or shell behavior can break CI commands that delegate to make.",
        "pre_commit": "Hook revisions or stages can block lint and formatting jobs before tests run.",
        "release_automation": "Release triggers, versioning, changelog, or publish steps can fail after CI passes.",
    }[surface]


def _validation_command(surface: CiPipelineSurface) -> str:
    return {
        "github_actions": "actionlint .github/workflows/*.yml .github/workflows/*.yaml",
        "test_runner_config": "poetry run pytest -o addopts=''",
        "python_packaging": "poetry check && poetry lock --check",
        "container_build": "docker build .",
        "make_targets": "make -n test",
        "pre_commit": "poetry run pre-commit run --all-files",
        "release_automation": "gh workflow list && gh release list --limit 5",
    }[surface]


def _owner_role(surface: CiPipelineSurface) -> str:
    return {
        "github_actions": "CI owner",
        "test_runner_config": "test owner",
        "python_packaging": "build owner",
        "container_build": "platform owner",
        "make_targets": "build owner",
        "pre_commit": "developer-experience owner",
        "release_automation": "release owner",
    }[surface]


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "test_command",
        "risk_level",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    for index, text in enumerate(_strings(task.get("tags"))):
        texts.append((f"tags[{index}]", text))
    for index, text in enumerate(_strings(task.get("labels"))):
        texts.append((f"labels[{index}]", text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _append(
    signals: dict[CiPipelineSurface, list[str]],
    surface: CiPipelineSurface,
    evidence: str,
) -> None:
    signals.setdefault(surface, []).append(evidence)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "CiPipelineSurface",
    "TaskCiPipelineImpactPlan",
    "TaskCiPipelineImpactRecommendation",
    "build_task_ci_pipeline_impact_plan",
    "recommend_task_ci_pipeline_impacts",
    "task_ci_pipeline_impact_plan_to_dict",
    "task_ci_pipeline_impact_plan_to_markdown",
]
