"""Risk coverage audit for implementation briefs and execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "be",
    "by",
    "can",
    "for",
    "from",
    "if",
    "in",
    "into",
    "is",
    "may",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}
_MIN_TOKEN_COVERAGE = 0.6


@dataclass(frozen=True)
class RiskCoverageItem:
    """Coverage details for a single implementation risk."""

    risk: str
    matching_task_ids: list[str] = field(default_factory=list)

    @property
    def covered(self) -> bool:
        return bool(self.matching_task_ids)

    def to_dict(self) -> dict[str, Any]:
        return {
            "risk": self.risk,
            "matching_task_ids": self.matching_task_ids,
        }


@dataclass(frozen=True)
class RiskCoverageResult:
    """Audit result for implementation risk coverage."""

    brief_id: str
    plan_id: str
    risks: list[RiskCoverageItem] = field(default_factory=list)

    @property
    def covered_risks(self) -> list[RiskCoverageItem]:
        return [risk for risk in self.risks if risk.covered]

    @property
    def uncovered_risks(self) -> list[RiskCoverageItem]:
        return [risk for risk in self.risks if not risk.covered]

    @property
    def coverage_ratio(self) -> float:
        if not self.risks:
            return 1.0
        return len(self.covered_risks) / len(self.risks)

    @property
    def ok(self) -> bool:
        return not self.uncovered_risks

    def to_dict(self) -> dict[str, Any]:
        return {
            "brief_id": self.brief_id,
            "plan_id": self.plan_id,
            "coverage_ratio": self.coverage_ratio,
            "covered_risks": [risk.to_dict() for risk in self.covered_risks],
            "uncovered_risks": [risk.to_dict() for risk in self.uncovered_risks],
        }


def audit_risk_coverage(
    implementation_brief: dict[str, Any],
    plan: dict[str, Any],
) -> RiskCoverageResult:
    """Check whether each brief risk is reflected by at least one execution task."""
    brief_id = str(implementation_brief.get("id") or "")
    plan_id = str(plan.get("id") or "")
    tasks = _list_of_dicts(plan.get("tasks"))

    risk_items: list[RiskCoverageItem] = []
    for risk in _list_of_strings(implementation_brief.get("risks")):
        matching_task_ids: list[str] = []
        for task in tasks:
            task_id = str(task.get("id") or "")
            if task_id and _risk_matches_task(risk, task):
                matching_task_ids.append(task_id)
        risk_items.append(
            RiskCoverageItem(
                risk=risk,
                matching_task_ids=matching_task_ids,
            )
        )

    return RiskCoverageResult(
        brief_id=brief_id,
        plan_id=plan_id,
        risks=risk_items,
    )


def _risk_matches_task(risk: str, task: dict[str, Any]) -> bool:
    for text in _task_risk_texts(task):
        if _risk_matches_text(risk, text):
            return True
    return False


def _risk_matches_text(risk: str, text: str) -> bool:
    risk_phrase = _normalized_phrase(risk)
    text_phrase = _normalized_phrase(text)
    if risk_phrase and risk_phrase in text_phrase:
        return True

    risk_tokens = _normalized_tokens(risk)
    if not risk_tokens:
        return False

    text_tokens = _normalized_tokens(text)
    if not text_tokens:
        return False

    return len(risk_tokens & text_tokens) / len(risk_tokens) >= _MIN_TOKEN_COVERAGE


def _task_risk_texts(task: dict[str, Any]) -> list[str]:
    texts = [str(task.get("description") or "")]
    texts.extend(_list_of_strings(task.get("acceptance_criteria")))

    metadata = task.get("metadata")
    if isinstance(metadata, dict):
        texts.extend(_metadata_mitigation_texts(metadata.get("mitigation")))

    return [text for text in texts if text.strip()]


def _metadata_mitigation_texts(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    if isinstance(value, dict):
        return [str(item) for item in value.values() if item is not None]
    return [str(value)]


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


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _list_of_strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str) or item is not None]
