"""Release readiness gate for execution plan handoff."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import re
from pathlib import Path
from typing import Any, Literal


ReleaseReadinessSeverity = Literal["blocking", "warning"]

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}
_IMPLEMENTATION_OWNER_TYPES = {"agent", "either", "implementation"}
_HIGH_RISK_LEVELS = {"high", "critical", "severe"}
_RISK_MATCH_THRESHOLD = 0.6


@dataclass(frozen=True)
class ReleaseReadinessFinding:
    """A single release readiness issue."""

    severity: ReleaseReadinessSeverity
    code: str
    message: str
    category: str
    task_id: str | None = None
    task_title: str | None = None
    dependency_id: str | None = None
    export_target: str | None = None
    risk: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "severity": self.severity,
            "code": self.code,
            "category": self.category,
            "message": self.message,
        }
        if self.task_id is not None:
            payload["task_id"] = self.task_id
        if self.task_title is not None:
            payload["task_title"] = self.task_title
        if self.dependency_id is not None:
            payload["dependency_id"] = self.dependency_id
        if self.export_target is not None:
            payload["export_target"] = self.export_target
        if self.risk is not None:
            payload["risk"] = self.risk
        return payload


@dataclass(frozen=True)
class ReleaseReadinessResult:
    """Release readiness result for a plan and its linked brief."""

    plan_id: str
    implementation_brief_id: str
    findings: list[ReleaseReadinessFinding] = field(default_factory=list)

    @property
    def blocking_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "blocking")

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "warning")

    @property
    def ok(self) -> bool:
        return self.blocking_count == 0

    def findings_by_severity(self) -> dict[str, list[ReleaseReadinessFinding]]:
        return {
            "blocking": [finding for finding in self.findings if finding.severity == "blocking"],
            "warning": [finding for finding in self.findings if finding.severity == "warning"],
        }

    def to_dict(self) -> dict[str, Any]:
        by_category = Counter(finding.category for finding in self.findings)
        return {
            "plan_id": self.plan_id,
            "implementation_brief_id": self.implementation_brief_id,
            "ok": self.ok,
            "summary": {
                "blocking": self.blocking_count,
                "warnings": self.warning_count,
                "findings": len(self.findings),
                "by_category": dict(sorted(by_category.items())),
            },
            "findings": [finding.to_dict() for finding in self.findings],
        }


def audit_release_readiness(
    plan: dict[str, Any],
    implementation_brief: dict[str, Any],
    export_records: list[dict[str, Any]] | None = None,
) -> ReleaseReadinessResult:
    """Check whether a plan is ready to release to implementation agents."""
    tasks = _list_of_dicts(plan.get("tasks"))
    task_ids = {str(task.get("id") or "") for task in tasks if task.get("id")}

    findings: list[ReleaseReadinessFinding] = []
    findings.extend(_blocked_task_findings(tasks))
    findings.extend(_missing_acceptance_findings(tasks))
    findings.extend(_missing_file_impact_findings(tasks))
    findings.extend(_dependency_findings(tasks, task_ids))
    findings.extend(_validation_strategy_findings(plan))
    findings.extend(_uncovered_high_risk_findings(implementation_brief, tasks))
    findings.extend(_required_export_findings(plan, export_records or []))

    return ReleaseReadinessResult(
        plan_id=str(plan.get("id") or ""),
        implementation_brief_id=str(implementation_brief.get("id") or ""),
        findings=findings,
    )


def _blocked_task_findings(tasks: list[dict[str, Any]]) -> list[ReleaseReadinessFinding]:
    findings: list[ReleaseReadinessFinding] = []
    for task in tasks:
        if str(task.get("status") or "") != "blocked":
            continue
        task_id = str(task.get("id") or "")
        reason = str(task.get("blocked_reason") or "").strip()
        message = f"Task {task_id} is blocked"
        if reason:
            message += f": {reason}"
        findings.append(
            ReleaseReadinessFinding(
                severity="blocking",
                code="blocked_task",
                category="tasks",
                task_id=task_id,
                task_title=_task_title(task),
                message=message + ".",
            )
        )
    return findings


def _missing_acceptance_findings(
    tasks: list[dict[str, Any]],
) -> list[ReleaseReadinessFinding]:
    return [
        ReleaseReadinessFinding(
            severity="blocking",
            code="missing_acceptance_criteria",
            category="tasks",
            task_id=str(task.get("id") or ""),
            task_title=_task_title(task),
            message=f"Task {task.get('id') or ''} has no acceptance criteria.",
        )
        for task in tasks
        if not _string_list(task.get("acceptance_criteria"))
    ]


def _missing_file_impact_findings(
    tasks: list[dict[str, Any]],
) -> list[ReleaseReadinessFinding]:
    findings: list[ReleaseReadinessFinding] = []
    for task in tasks:
        if not _is_implementation_task(task):
            continue
        if _string_list(task.get("files_or_modules")):
            continue
        task_id = str(task.get("id") or "")
        findings.append(
            ReleaseReadinessFinding(
                severity="blocking",
                code="missing_files_or_modules",
                category="tasks",
                task_id=task_id,
                task_title=_task_title(task),
                message=(
                    f"Implementation task {task_id} does not identify files or modules " "to touch."
                ),
            )
        )
    return findings


def _dependency_findings(
    tasks: list[dict[str, Any]],
    task_ids: set[str],
) -> list[ReleaseReadinessFinding]:
    findings: list[ReleaseReadinessFinding] = []
    for task in tasks:
        task_id = str(task.get("id") or "")
        for dependency_id in _string_list(task.get("depends_on")):
            if dependency_id == task_id:
                findings.append(
                    ReleaseReadinessFinding(
                        severity="blocking",
                        code="self_dependency",
                        category="dependencies",
                        task_id=task_id,
                        task_title=_task_title(task),
                        dependency_id=dependency_id,
                        message=f"Task {task_id} depends on itself.",
                    )
                )
            elif dependency_id not in task_ids:
                findings.append(
                    ReleaseReadinessFinding(
                        severity="blocking",
                        code="unresolved_dependency",
                        category="dependencies",
                        task_id=task_id,
                        task_title=_task_title(task),
                        dependency_id=dependency_id,
                        message=f"Task {task_id} depends on missing task {dependency_id}.",
                    )
                )
    return findings


def _validation_strategy_findings(
    plan: dict[str, Any],
) -> list[ReleaseReadinessFinding]:
    if _has_text(plan.get("test_strategy")):
        return []
    return [
        ReleaseReadinessFinding(
            severity="blocking",
            code="missing_validation_strategy",
            category="validation",
            message="Plan has no validation or test strategy.",
        )
    ]


def _uncovered_high_risk_findings(
    implementation_brief: dict[str, Any],
    tasks: list[dict[str, Any]],
) -> list[ReleaseReadinessFinding]:
    findings: list[ReleaseReadinessFinding] = []
    for risk in _high_risks(implementation_brief.get("risks")):
        if _risk_matches_any_task(risk, tasks):
            continue
        findings.append(
            ReleaseReadinessFinding(
                severity="blocking",
                code="uncovered_high_risk",
                category="risk",
                risk=risk,
                message=f"High-risk brief item is not covered by any plan task: {risk}",
            )
        )
    return findings


def _required_export_findings(
    plan: dict[str, Any],
    export_records: list[dict[str, Any]],
) -> list[ReleaseReadinessFinding]:
    metadata = plan.get("metadata")
    if not isinstance(metadata, dict):
        return []

    required_targets = _string_list(metadata.get("required_exports"))
    if not required_targets:
        return []

    records_by_target: dict[str, list[dict[str, Any]]] = {}
    for record in export_records:
        target = str(record.get("target_engine") or "")
        if target:
            records_by_target.setdefault(target, []).append(record)

    findings: list[ReleaseReadinessFinding] = []
    for target in required_targets:
        target_records = records_by_target.get(target, [])
        if not target_records:
            findings.append(
                ReleaseReadinessFinding(
                    severity="blocking",
                    code="missing_required_export",
                    category="exports",
                    export_target=target,
                    message=f"Required export target has not been rendered: {target}.",
                )
            )
            continue

        if not any(_export_record_complete(record) for record in target_records):
            findings.append(
                ReleaseReadinessFinding(
                    severity="blocking",
                    code="incomplete_required_export",
                    category="exports",
                    export_target=target,
                    message=f"Required export target has no present artifact: {target}.",
                )
            )
    return findings


def _export_record_complete(record: dict[str, Any]) -> bool:
    output_path = str(record.get("output_path") or "").strip()
    if not output_path:
        return False
    return Path(output_path).exists()


def _high_risks(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []

    risks: list[str] = []
    for item in value:
        if isinstance(item, dict):
            level = str(
                item.get("severity")
                or item.get("risk_level")
                or item.get("level")
                or item.get("priority")
                or ""
            ).lower()
            text = str(item.get("risk") or item.get("title") or item.get("description") or "")
            if level in _HIGH_RISK_LEVELS and text.strip():
                risks.append(text.strip())
        elif isinstance(item, str):
            normalized = item.lower()
            if normalized.startswith(("high:", "high -", "critical:", "critical -")):
                risks.append(item.strip())
    return risks


def _risk_matches_any_task(risk: str, tasks: list[dict[str, Any]]) -> bool:
    risk_tokens = _tokens(risk)
    if not risk_tokens:
        return False
    for task in tasks:
        task_text = " ".join(
            [
                str(task.get("title") or ""),
                str(task.get("description") or ""),
                " ".join(_string_list(task.get("acceptance_criteria"))),
            ]
        )
        task_tokens = _tokens(task_text)
        if (
            task_tokens
            and len(risk_tokens & task_tokens) / len(risk_tokens) >= _RISK_MATCH_THRESHOLD
        ):
            return True
    return False


def _tokens(value: str) -> set[str]:
    return {
        token for token in _TOKEN_RE.findall(value.lower()) if token and token not in _STOPWORDS
    }


def _is_implementation_task(task: dict[str, Any]) -> bool:
    owner_type = str(task.get("owner_type") or "").lower()
    if owner_type in _IMPLEMENTATION_OWNER_TYPES:
        return True
    return _has_text(task.get("suggested_engine"))


def _task_title(task: dict[str, Any]) -> str:
    return str(task.get("title") or "")


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if _has_text(item)]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())
