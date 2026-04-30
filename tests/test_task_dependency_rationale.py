import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_dependency_rationale import (
    TaskDependencyRationale,
    annotate_dependency_rationales,
    task_dependency_rationales_to_dict,
)


def test_file_overlap_produces_dependency_rationale():
    annotations = annotate_dependency_rationales(
        _plan(
            tasks=[
                _task(
                    "task-schema",
                    "Create Settings Schema",
                    files=["src/blueprint/settings.py"],
                ),
                _task(
                    "task-ui",
                    "Render Settings UI",
                    depends_on=["task-schema"],
                    files=["src/blueprint/settings.py", "src/blueprint/ui.py"],
                ),
            ],
        )
    )

    annotation = annotations[0]

    assert isinstance(annotation, TaskDependencyRationale)
    assert annotation.to_dict() == {
        "task_id": "task-ui",
        "dependency_id": "task-schema",
        "rationale": "both tasks touch the same files or modules",
        "confidence": 0.82,
        "evidence": ["shared files_or_modules: src/blueprint/settings.py"],
    }


def test_milestone_order_explains_cross_milestone_dependencies():
    annotations = annotate_dependency_rationales(
        _plan(
            milestones=[{"id": "foundation"}, {"id": "delivery"}],
            tasks=[
                _task("task-db", "Create Database", milestone="foundation"),
                _task(
                    "task-api",
                    "Build API",
                    milestone="delivery",
                    depends_on=["task-db"],
                    files=["src/blueprint/api.py"],
                ),
            ],
        )
    )

    assert annotations[0].rationale == (
        "task-db belongs to earlier milestone 'foundation' before 'delivery'"
    )
    assert annotations[0].confidence == 0.72
    assert annotations[0].evidence == ("milestone order: foundation precedes delivery",)


def test_explicit_description_mentions_raise_confidence_and_keep_evidence():
    annotations = annotate_dependency_rationales(
        _plan(
            tasks=[
                _task("task-api", "Build Orders API"),
                _task(
                    "task-ui",
                    "Render Orders UI",
                    description="Render after Build Orders API exposes stable response fields.",
                    depends_on=["task-api"],
                ),
            ],
        )
    )

    assert annotations[0].rationale == (
        "task text explicitly references Build Orders API before this work"
    )
    assert annotations[0].confidence == 0.88
    assert annotations[0].evidence == (
        "description mentions Build Orders API: "
        "Render after Build Orders API exposes stable response fields.",
    )


def test_missing_dependency_references_are_low_confidence_annotations():
    annotations = annotate_dependency_rationales(
        _plan(tasks=[_task("task-ui", "Render UI", depends_on=["task-missing"])])
    )

    assert annotations[0].to_dict() == {
        "task_id": "task-ui",
        "dependency_id": "task-missing",
        "rationale": "task-ui references missing dependency task-missing.",
        "confidence": 0.2,
        "evidence": [
            "depends_on includes task-missing, but no task with that id exists.",
        ],
    }


def test_multiple_rationale_sources_have_stable_confidence_and_ordering():
    annotations = annotate_dependency_rationales(
        _plan(
            milestones=[{"id": "setup"}, {"id": "feature"}],
            tasks=[
                _task(
                    "task-api",
                    "Build Billing API",
                    milestone="setup",
                    files=["src/blueprint/billing.py"],
                ),
                _task(
                    "task-ui",
                    "Render Billing UI",
                    description="Render after Build Billing API is available.",
                    milestone="feature",
                    depends_on=["task-api"],
                    files=["src/blueprint/billing.py"],
                    metadata={
                        "dependency_rationales": {
                            "task-api": "task-api defines the data contract.",
                        }
                    },
                ),
            ],
        )
    )

    assert annotations[0].confidence == 0.95
    assert annotations[0].rationale == (
        "metadata states this dependency: task-api defines the data contract.; "
        "task text explicitly references Build Billing API before this work; "
        "both tasks touch the same files or modules; "
        "task-api belongs to earlier milestone 'setup' before 'feature'"
    )
    assert annotations[0].evidence == (
        "metadata dependency rationale for task-api: task-api defines the data contract.",
        "description mentions Build Billing API: Render after Build Billing API is available.",
        "shared files_or_modules: src/blueprint/billing.py",
        "milestone order: setup precedes feature",
    )


def test_annotations_preserve_declared_dependency_order_and_do_not_mutate_input():
    plan = _plan(
        tasks=[
            _task("task-a", "Task A"),
            _task("task-b", "Task B"),
            _task("task-c", "Task C", depends_on=["task-b", "task-a", "task-missing"]),
        ]
    )
    before = copy.deepcopy(plan)

    annotations = annotate_dependency_rationales(plan)

    assert plan == before
    assert [(item.task_id, item.dependency_id) for item in annotations] == [
        ("task-c", "task-b"),
        ("task-c", "task-a"),
        ("task-c", "task-missing"),
    ]


def test_accepts_execution_plan_models_and_serializes_stably():
    plan_model = ExecutionPlan.model_validate(
        _plan(
            tasks=[
                _task("task-api", "Build API"),
                _task("task-ui", "Build UI", depends_on=["task-api"]),
            ],
        )
    )

    annotations = annotate_dependency_rationales(plan_model)
    payload = task_dependency_rationales_to_dict(annotations)

    assert payload == [annotation.to_dict() for annotation in annotations]
    assert list(payload[0]) == [
        "task_id",
        "dependency_id",
        "rationale",
        "confidence",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(*, tasks, milestones=None):
    return {
        "id": "plan-dependency-rationale",
        "implementation_brief_id": "brief-dependency-rationale",
        "milestones": [] if milestones is None else milestones,
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    description=None,
    milestone=None,
    depends_on=None,
    files=None,
    acceptance=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": description or f"Implement {title}.",
        "milestone": milestone,
        "depends_on": [] if depends_on is None else depends_on,
        "files_or_modules": files or [f"src/blueprint/{task_id}.py"],
        "acceptance_criteria": acceptance or [f"{title} is complete"],
        "metadata": metadata or {},
    }
