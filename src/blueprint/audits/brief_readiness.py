"""Readiness audit for implementation briefs before plan generation."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal


Severity = Literal["blocking", "warning"]

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_GENERIC_RISK_PHRASES = {
    "risk",
    "risks",
    "unknown",
    "unknown risks",
    "none",
    "n/a",
    "na",
    "tbd",
    "todo",
    "edge cases",
    "technical risk",
    "implementation risk",
    "security",
    "performance",
    "testing",
}


@dataclass(frozen=True)
class BriefReadinessFinding:
    """A single actionable readiness finding."""

    severity: Severity
    code: str
    field: str
    message: str
    remediation: str
    value: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "severity": self.severity,
            "code": self.code,
            "field": self.field,
            "message": self.message,
            "remediation": self.remediation,
        }
        if self.value is not None:
            payload["value"] = self.value
        return payload


@dataclass(frozen=True)
class BriefReadinessResult:
    """Readiness audit result for an implementation brief."""

    brief_id: str
    findings: list[BriefReadinessFinding] = field(default_factory=list)

    @property
    def blocking_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "blocking")

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "warning")

    @property
    def passed(self) -> bool:
        return self.blocking_count == 0

    def findings_by_severity(self) -> dict[str, list[BriefReadinessFinding]]:
        return {
            "blocking": [
                finding for finding in self.findings if finding.severity == "blocking"
            ],
            "warning": [
                finding for finding in self.findings if finding.severity == "warning"
            ],
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "brief_id": self.brief_id,
            "passed": self.passed,
            "findings": [finding.to_dict() for finding in self.findings],
            "summary": {
                "blocking": self.blocking_count,
                "warning": self.warning_count,
            },
        }


def audit_brief_readiness(implementation_brief: dict[str, Any]) -> BriefReadinessResult:
    """Check whether an implementation brief has enough detail for planning."""
    brief_id = str(implementation_brief.get("id") or "")
    findings: list[BriefReadinessFinding] = []

    scope = _non_empty_strings(implementation_brief.get("scope"))
    non_goals = _non_empty_strings(implementation_brief.get("non_goals"))
    definition_of_done = _non_empty_strings(
        implementation_brief.get("definition_of_done")
    )
    risks = _non_empty_strings(implementation_brief.get("risks"))

    if not scope:
        findings.append(
            BriefReadinessFinding(
                severity="blocking",
                code="missing_scope",
                field="scope",
                message="Brief has no in-scope deliverables.",
                remediation="Add concrete scope items that describe what the plan must build.",
            )
        )

    if not definition_of_done:
        findings.append(
            BriefReadinessFinding(
                severity="blocking",
                code="missing_definition_of_done",
                field="definition_of_done",
                message="Brief has no definition of done.",
                remediation=(
                    "Add completion criteria that make implementation success observable."
                ),
            )
        )

    if not _has_text(implementation_brief.get("validation_plan")):
        findings.append(
            BriefReadinessFinding(
                severity="blocking",
                code="missing_validation_plan",
                field="validation_plan",
                message="Brief has no validation plan.",
                remediation="Describe the test, review, or verification steps for the work.",
            )
        )

    if not risks:
        findings.append(
            BriefReadinessFinding(
                severity="blocking",
                code="missing_risks",
                field="risks",
                message="Brief has no implementation risks.",
                remediation=(
                    "Add specific risks that could affect implementation or rollout."
                ),
            )
        )
    else:
        for risk in risks:
            if _is_generic_risk(risk):
                findings.append(
                    BriefReadinessFinding(
                        severity="warning",
                        code="generic_risk",
                        field="risks",
                        value=risk,
                        message=f"Risk is too generic to guide planning: {risk}",
                        remediation=(
                            "Replace it with a concrete failure mode, constraint, or "
                            "integration concern."
                        ),
                    )
                )

    if not _has_text(implementation_brief.get("product_surface")):
        findings.append(
            BriefReadinessFinding(
                severity="blocking",
                code="missing_product_surface",
                field="product_surface",
                message="Brief does not identify the product surface.",
                remediation=(
                    "Set the surface being changed, such as CLI, web UI, API, or library."
                ),
            )
        )

    findings.extend(_duplicate_findings("scope", scope))
    findings.extend(_duplicate_findings("non_goals", non_goals))
    findings.extend(_scope_non_goal_overlap_findings(scope, non_goals))

    return BriefReadinessResult(brief_id=brief_id, findings=findings)


def _duplicate_findings(
    field_name: str,
    values: list[str],
) -> list[BriefReadinessFinding]:
    findings: list[BriefReadinessFinding] = []
    seen: set[str] = set()
    reported: set[str] = set()
    for value in values:
        key = _normalized_phrase(value)
        if not key:
            continue
        if key in seen and key not in reported:
            reported.add(key)
            findings.append(
                BriefReadinessFinding(
                    severity="warning",
                    code=f"duplicate_{field_name}_entry",
                    field=field_name,
                    value=value,
                    message=f"{field_name} contains a duplicate entry: {value}",
                    remediation="Remove or merge duplicate entries so each item is distinct.",
                )
            )
        seen.add(key)
    return findings


def _scope_non_goal_overlap_findings(
    scope: list[str],
    non_goals: list[str],
) -> list[BriefReadinessFinding]:
    scope_by_key = {_normalized_phrase(value): value for value in scope}
    findings: list[BriefReadinessFinding] = []
    reported: set[str] = set()
    for non_goal in non_goals:
        key = _normalized_phrase(non_goal)
        if not key or key not in scope_by_key or key in reported:
            continue
        reported.add(key)
        findings.append(
            BriefReadinessFinding(
                severity="warning",
                code="scope_non_goal_overlap",
                field="non_goals",
                value=non_goal,
                message=(
                    "Entry appears in both scope and non_goals: "
                    f"{scope_by_key[key]}"
                ),
                remediation=(
                    "Keep the item in scope or non_goals, but not both."
                ),
            )
        )
    return findings


def _is_generic_risk(value: str) -> bool:
    phrase = _normalized_phrase(value)
    tokens = _normalized_tokens(value)
    return phrase in _GENERIC_RISK_PHRASES or len(tokens) < 3


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _non_empty_strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _normalized_phrase(value: str) -> str:
    return " ".join(_normalized_tokens(value))


def _normalized_tokens(value: str) -> list[str]:
    return _TOKEN_RE.findall(value.lower())
