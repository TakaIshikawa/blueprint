"""Semantic audit checks for execution plan and brief coherence."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal


Severity = Literal["error", "warning"]

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = {
    "a",
    "an",
    "and",
    "by",
    "for",
    "from",
    "in",
    "into",
    "of",
    "on",
    "or",
    "the",
    "through",
    "to",
    "using",
    "via",
    "with",
}
_SURFACE_FAMILIES = {
    "cli": "cli",
    "command line": "cli",
    "command-line": "cli",
    "terminal": "cli",
    "shell": "cli",
    "web": "web",
    "ui": "web",
    "user interface": "web",
    "interface": "web",
    "frontend": "web",
    "front-end": "web",
    "app": "web",
    "screen": "web",
    "view": "web",
    "library": "library",
    "package": "library",
    "sdk": "library",
    "module": "library",
    "api": "api",
    "service": "api",
    "endpoint": "api",
    "server": "server",
    "mcp": "mcp",
    "integration": "integration",
    "connector": "integration",
    "adapter": "integration",
}
_PROJECT_TYPE_FAMILIES = {
    "cli_tool": "cli",
    "web_app": "web",
    "python_library": "library",
    "library": "library",
    "api_service": "api",
    "api": "api",
    "mcp_server": "mcp",
    "server": "server",
    "integration": "integration",
}


@dataclass(frozen=True)
class BriefPlanCoherenceIssue:
    """A single semantic coherence finding."""

    severity: Severity
    code: str
    message: str
    scope_item: str | None = None
    task_id: str | None = None
    milestone: str | None = None
    project_type: str | None = None
    product_surface: str | None = None
    validation_item: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable issue payload."""
        payload: dict[str, Any] = {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
        }
        if self.scope_item is not None:
            payload["scope_item"] = self.scope_item
        if self.task_id is not None:
            payload["task_id"] = self.task_id
        if self.milestone is not None:
            payload["milestone"] = self.milestone
        if self.project_type is not None:
            payload["project_type"] = self.project_type
        if self.product_surface is not None:
            payload["product_surface"] = self.product_surface
        if self.validation_item is not None:
            payload["validation_item"] = self.validation_item
        return payload


@dataclass(frozen=True)
class BriefPlanCoherenceResult:
    """Semantic audit result for an execution plan and its implementation brief."""

    plan_id: str
    implementation_brief_id: str
    issues: list[BriefPlanCoherenceIssue] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "warning")

    @property
    def ok(self) -> bool:
        return self.error_count == 0

    def issues_by_severity(self) -> dict[str, list[BriefPlanCoherenceIssue]]:
        return {
            "error": [issue for issue in self.issues if issue.severity == "error"],
            "warning": [issue for issue in self.issues if issue.severity == "warning"],
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "implementation_brief_id": self.implementation_brief_id,
            "ok": self.ok,
            "summary": {
                "errors": self.error_count,
                "warnings": self.warning_count,
            },
            "issues": [issue.to_dict() for issue in self.issues],
        }


def audit_brief_plan_coherence(
    plan: dict[str, Any],
    implementation_brief: dict[str, Any],
) -> BriefPlanCoherenceResult:
    """Check whether a plan meaningfully reflects the linked implementation brief."""
    plan_id = str(plan.get("id") or "")
    implementation_brief_id = str(implementation_brief.get("id") or "")
    issues: list[BriefPlanCoherenceIssue] = []

    delivery_texts = _plan_delivery_texts(plan)
    validation_texts = _plan_validation_texts(plan)
    scope_items = [str(item) for item in _list_of_strings(implementation_brief.get("scope"))]
    definition_of_done = [
        str(item) for item in _list_of_strings(implementation_brief.get("definition_of_done"))
    ]
    product_surface = str(implementation_brief.get("product_surface") or "").strip()
    project_type = str(plan.get("project_type") or "").strip()
    validation_plan = str(implementation_brief.get("validation_plan") or "").strip()

    issues.extend(_scope_coverage_issues(scope_items, delivery_texts))
    surface_issue = _product_surface_issue(product_surface, project_type)
    if surface_issue is not None:
        issues.append(surface_issue)
    issues.extend(_validation_coverage_issues(validation_plan, validation_texts))
    issues.extend(_definition_of_done_issues(definition_of_done, validation_texts))

    return BriefPlanCoherenceResult(
        plan_id=plan_id,
        implementation_brief_id=implementation_brief_id,
        issues=issues,
    )


def _scope_coverage_issues(
    scope_items: list[str],
    delivery_texts: list[str],
) -> list[BriefPlanCoherenceIssue]:
    issues: list[BriefPlanCoherenceIssue] = []
    for scope_item in scope_items:
        if not _is_item_covered(scope_item, delivery_texts):
            issues.append(
                BriefPlanCoherenceIssue(
                    severity="error",
                    code="scope_item_uncovered",
                    scope_item=scope_item,
                    message=(
                        f"Scope item is not reflected in milestone or task titles/descriptions: "
                        f"{scope_item}"
                    ),
                )
            )
    return issues


def _product_surface_issue(
    product_surface: str,
    project_type: str,
) -> BriefPlanCoherenceIssue | None:
    if not product_surface or not project_type:
        return None

    surface_family = _classify_surface(product_surface, _SURFACE_FAMILIES)
    project_family = _classify_surface(project_type, _PROJECT_TYPE_FAMILIES)
    if surface_family is None or project_family is None or surface_family == project_family:
        return None

    return BriefPlanCoherenceIssue(
        severity="error",
        code="product_surface_conflict",
        product_surface=product_surface,
        project_type=project_type,
        message=(
            f"Brief product surface {product_surface!r} conflicts with plan project type "
            f"{project_type!r}"
        ),
    )


def _validation_coverage_issues(
    validation_plan: str,
    validation_texts: list[str],
) -> list[BriefPlanCoherenceIssue]:
    issues: list[BriefPlanCoherenceIssue] = []

    if not validation_plan.strip():
        issues.append(
            BriefPlanCoherenceIssue(
                severity="warning",
                code="missing_validation_strategy",
                message="Implementation brief has no validation_plan to carry into execution",
            )
        )
    elif not _is_item_covered(validation_plan, validation_texts):
        issues.append(
            BriefPlanCoherenceIssue(
                severity="warning",
                code="validation_plan_uncovered",
                validation_item=validation_plan,
                message=(
                    "Brief validation plan is not reflected in milestone, task, or strategy text: "
                    f"{validation_plan}"
                ),
            )
        )

    return issues


def _definition_of_done_issues(
    definition_of_done: list[str],
    validation_texts: list[str],
) -> list[BriefPlanCoherenceIssue]:
    issues: list[BriefPlanCoherenceIssue] = []
    for item in definition_of_done:
        if not _is_item_covered(item, validation_texts):
            issues.append(
                BriefPlanCoherenceIssue(
                    severity="warning",
                    code="definition_of_done_uncovered",
                    validation_item=item,
                    message=(
                        "Definition-of-done item is not reflected in the execution plan: "
                        f"{item}"
                    ),
                )
            )
    return issues


def _plan_delivery_texts(plan: dict[str, Any]) -> list[str]:
    texts: list[str] = []
    for milestone in _list_of_dicts(plan.get("milestones")):
        texts.extend(
            [
                str(milestone.get("name") or ""),
                str(milestone.get("description") or ""),
            ]
        )
    for task in _list_of_dicts(plan.get("tasks")):
        texts.extend(
            [
                str(task.get("title") or ""),
                str(task.get("description") or ""),
            ]
        )
    return [text for text in texts if text.strip()]


def _plan_validation_texts(plan: dict[str, Any]) -> list[str]:
    texts = _plan_delivery_texts(plan)
    texts.extend(
        [
            str(plan.get("test_strategy") or ""),
            str(plan.get("handoff_prompt") or ""),
        ]
    )
    return [text for text in texts if text.strip()]


def _is_item_covered(item: str, texts: list[str]) -> bool:
    item = item.strip()
    if not item:
        return True

    item_phrase = _normalized_phrase(item)
    item_tokens = _normalized_tokens(item)
    if not item_tokens:
        return any(item_phrase and item_phrase in _normalized_phrase(text) for text in texts)

    for text in texts:
        if item_phrase and item_phrase in _normalized_phrase(text):
            return True
        text_tokens = _normalized_tokens(text)
        overlap = len(item_tokens & text_tokens)
        coverage = overlap / len(item_tokens)
        if coverage >= 0.6:
            return True

    return False


def _normalized_phrase(value: str) -> str:
    return " ".join(_normalized_token_list(value))


def _normalized_tokens(value: str) -> set[str]:
    return set(_normalized_token_list(value))


def _normalized_token_list(value: str) -> list[str]:
    tokens: list[str] = []
    for raw_token in _TOKEN_RE.findall(value.lower()):
        token = _normalize_token(raw_token)
        if token and token not in _STOPWORDS:
            tokens.append(token)
    return tokens


def _normalize_token(token: str) -> str:
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def _classify_surface(value: str, families: dict[str, str]) -> str | None:
    lowered = value.lower()
    for needle, family in families.items():
        if needle in lowered:
            return family
    return None


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _list_of_strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str) or item is not None]
