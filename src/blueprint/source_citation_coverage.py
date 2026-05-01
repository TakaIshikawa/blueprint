"""Evaluate citation coverage for normalized SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


CitationCoverageStatus = Literal["covered", "partial", "missing"]
SourceCitationCoverageStatus = Literal["covered", "partial", "missing", "empty"]

_SPACE_RE = re.compile(r"\s+")
_URL_RE = re.compile(r"\b(?:https?://|mailto:|ftp://)[^\s<>)]+", re.IGNORECASE)
_MARKDOWN_LINK_RE = re.compile(r"\[[^\]]+\]\((?:https?://|/|\.{1,2}/|#)[^)]+\)")
_FOOTNOTE_REF_RE = re.compile(r"(?:\[\^[^\]]+\]|\[\d+\])")
_FOOTNOTE_DEF_RE = re.compile(r"^\s*\[\^[^\]]+\]:", re.MULTILINE)
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_REFERENCE_KEY_RE = re.compile(
    r"(?:^|_)(?:citation|citations|source|sources|source_url|source_urls|"
    r"reference|references|ref|refs|evidence|link|links|url|urls|html_url|"
    r"file_path|path|footnote|footnotes)(?:$|_)",
    re.IGNORECASE,
)
_CLAIM_FIELD_RE = re.compile(
    r"(?:summary|assumptions?|requirements?|acceptance|criteria|scope|goal|"
    r"problem|decision|decisions|risk|risks|constraint|constraints|non_goals?|"
    r"notes?|finding|findings|claim|claims|data_requirements?)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class SourceCitationClaim:
    """One claim candidate and its inferred citation coverage."""

    source_brief_id: str
    path: str
    evidence_text: str
    coverage_status: CitationCoverageStatus
    citation_evidence: tuple[str, ...] = field(default_factory=tuple)
    remediation_suggestion: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "path": self.path,
            "evidence_text": self.evidence_text,
            "coverage_status": self.coverage_status,
            "citation_evidence": list(self.citation_evidence),
            "remediation_suggestion": self.remediation_suggestion,
        }


@dataclass(frozen=True, slots=True)
class SourceCitationCoverage:
    """Citation coverage rollup for one or more source briefs."""

    source_brief_ids: tuple[str, ...] = field(default_factory=tuple)
    claims: tuple[SourceCitationClaim, ...] = field(default_factory=tuple)

    @property
    def coverage_score(self) -> float:
        """Return weighted citation coverage as a deterministic score from 0 to 1."""
        if not self.claims:
            return 1.0
        weighted = sum(
            1.0
            if claim.coverage_status == "covered"
            else 0.5
            if claim.coverage_status == "partial"
            else 0.0
            for claim in self.claims
        )
        return round(weighted / len(self.claims), 4)

    @property
    def status(self) -> SourceCitationCoverageStatus:
        """Return the aggregate citation coverage status."""
        if not self.claims:
            return "empty"
        statuses = {claim.coverage_status for claim in self.claims}
        if statuses == {"covered"}:
            return "covered"
        if statuses == {"missing"}:
            return "missing"
        return "partial"

    @property
    def summary(self) -> dict[str, Any]:
        """Return compact rollup counts in stable key order."""
        return {
            "source_brief_count": len(self.source_brief_ids),
            "claim_count": len(self.claims),
            "covered_count": sum(1 for claim in self.claims if claim.coverage_status == "covered"),
            "partial_count": sum(1 for claim in self.claims if claim.coverage_status == "partial"),
            "missing_count": sum(1 for claim in self.claims if claim.coverage_status == "missing"),
        }

    @property
    def uncited_claim_candidates(self) -> tuple[SourceCitationClaim, ...]:
        """Return claims that still need stronger traceability."""
        return tuple(
            claim
            for claim in self.claims
            if claim.coverage_status in {"partial", "missing"}
        )

    @property
    def remediation_suggestions(self) -> tuple[str, ...]:
        """Return deterministic remediation suggestions for uncovered claims."""
        suggestions = [
            claim.remediation_suggestion
            for claim in self.uncited_claim_candidates
            if claim.remediation_suggestion
        ]
        return tuple(_dedupe(suggestions))

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_ids": list(self.source_brief_ids),
            "status": self.status,
            "coverage_score": self.coverage_score,
            "summary": self.summary,
            "claims": [claim.to_dict() for claim in self.claims],
            "uncited_claim_candidates": [
                claim.to_dict() for claim in self.uncited_claim_candidates
            ],
            "remediation_suggestions": list(self.remediation_suggestions),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return claim coverage rows as plain dictionaries."""
        return [claim.to_dict() for claim in self.claims]


def analyze_source_citation_coverage(
    source_briefs: Mapping[str, Any]
    | SourceBrief
    | list[Mapping[str, Any] | SourceBrief]
    | tuple[Mapping[str, Any] | SourceBrief, ...],
) -> SourceCitationCoverage:
    """Analyze whether SourceBrief claim candidates have traceable citations."""
    briefs = _source_brief_payloads(source_briefs)
    claims: list[SourceCitationClaim] = []
    source_brief_ids: list[str] = []

    for index, brief in enumerate(briefs, start=1):
        source_brief_id = _text(brief.get("id")) or f"source-brief-{index}"
        source_brief_ids.append(source_brief_id)
        context = _CitationContext.from_brief(brief)
        for candidate in _claim_candidates(brief):
            claims.append(_claim(source_brief_id, candidate, context))

    return SourceCitationCoverage(
        source_brief_ids=tuple(source_brief_ids),
        claims=tuple(sorted(claims, key=_claim_sort_key)),
    )


def source_citation_coverage_to_dict(
    coverage: SourceCitationCoverage,
) -> dict[str, Any]:
    """Serialize source citation coverage to a plain dictionary."""
    return coverage.to_dict()


@dataclass(frozen=True, slots=True)
class _ClaimCandidate:
    path: str
    text: str
    local_references: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class _CitationContext:
    brief_references: tuple[str, ...] = field(default_factory=tuple)
    payload_references: tuple[str, ...] = field(default_factory=tuple)
    footnote_definitions: tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_brief(cls, brief: Mapping[str, Any]) -> "_CitationContext":
        source_links = _mapping(brief.get("source_links"))
        source_payload = _mapping(brief.get("source_payload"))
        brief_references = tuple(
            _dedupe(
                [
                    reference
                    for _, reference in _reference_values(source_links, prefix="source_links")
                ]
            )
        )
        payload_references = tuple(
            _dedupe(
                [
                    f"{path}: {value}"
                    for path, value in _reference_values(
                        source_payload, prefix="source_payload"
                    )
                ]
            )
        )
        footnotes = tuple(
            _dedupe(
                [
                    f"{path}: {match.group(0).strip()}"
                    for path, text in _text_values(brief)
                    for match in _FOOTNOTE_DEF_RE.finditer(text)
                ]
            )
        )
        return cls(
            brief_references=brief_references,
            payload_references=payload_references,
            footnote_definitions=footnotes,
        )


def _claim(
    source_brief_id: str,
    candidate: _ClaimCandidate,
    context: _CitationContext,
) -> SourceCitationClaim:
    inline_references = tuple(_dedupe(_inline_citations(candidate.text)))
    local_references = tuple(_dedupe(candidate.local_references))
    if inline_references:
        return SourceCitationClaim(
            source_brief_id=source_brief_id,
            path=candidate.path,
            evidence_text=candidate.text,
            coverage_status="covered",
            citation_evidence=inline_references,
            remediation_suggestion=None,
        )
    if local_references:
        return SourceCitationClaim(
            source_brief_id=source_brief_id,
            path=candidate.path,
            evidence_text=candidate.text,
            coverage_status="covered",
            citation_evidence=local_references,
            remediation_suggestion=None,
        )
    if _FOOTNOTE_REF_RE.search(candidate.text) and context.footnote_definitions:
        return SourceCitationClaim(
            source_brief_id=source_brief_id,
            path=candidate.path,
            evidence_text=candidate.text,
            coverage_status="covered",
            citation_evidence=context.footnote_definitions,
            remediation_suggestion=None,
        )
    if context.payload_references:
        return SourceCitationClaim(
            source_brief_id=source_brief_id,
            path=candidate.path,
            evidence_text=candidate.text,
            coverage_status="partial",
            citation_evidence=context.payload_references,
            remediation_suggestion=(
                f"Attach a citation directly to claim at {candidate.path} or move the relevant "
                "payload reference next to the claim."
            ),
        )
    if context.brief_references:
        return SourceCitationClaim(
            source_brief_id=source_brief_id,
            path=candidate.path,
            evidence_text=candidate.text,
            coverage_status="partial",
            citation_evidence=context.brief_references,
            remediation_suggestion=(
                f"Attach a claim-level citation at {candidate.path}; the brief has only "
                "record-level source links."
            ),
        )
    return SourceCitationClaim(
        source_brief_id=source_brief_id,
        path=candidate.path,
        evidence_text=candidate.text,
        coverage_status="missing",
        citation_evidence=(),
        remediation_suggestion=(
            f"Add a source link, markdown link, footnote, or payload reference for {candidate.path}."
        ),
    )


def _claim_candidates(brief: Mapping[str, Any]) -> list[_ClaimCandidate]:
    candidates: list[_ClaimCandidate] = []
    for field_name in ("summary", "assumptions", "requirements"):
        candidates.extend(_candidates_from_value(brief.get(field_name), path=field_name))

    payload = _mapping(brief.get("source_payload"))
    candidates.extend(_payload_claim_candidates(payload, path="source_payload"))
    return _dedupe_candidates(candidates)


def _payload_claim_candidates(value: Any, *, path: str) -> list[_ClaimCandidate]:
    candidates: list[_ClaimCandidate] = []
    if isinstance(value, Mapping):
        local_refs = tuple(
            _dedupe(
                [
                    f"{path}.{key}: {reference}"
                    for key, child in value.items()
                    if _is_reference_key(str(key))
                    for reference in _reference_texts(child)
                ]
            )
        )
        for key in sorted(value, key=str):
            child_path = f"{path}.{key}"
            child = value[key]
            if _is_reference_key(str(key)):
                continue
            if _is_claim_field(str(key)):
                for candidate in _candidates_from_value(child, path=child_path):
                    candidates.append(
                        _ClaimCandidate(
                            path=candidate.path,
                            text=candidate.text,
                            local_references=local_refs,
                        )
                    )
            else:
                candidates.extend(_payload_claim_candidates(child, path=child_path))
        return candidates
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            candidates.extend(_payload_claim_candidates(item, path=f"{path}[{index}]"))
    return candidates


def _candidates_from_value(value: Any, *, path: str) -> list[_ClaimCandidate]:
    if isinstance(value, str):
        return [
            _ClaimCandidate(path=path, text=text)
            for text in _claim_texts(value)
        ]
    if isinstance(value, Mapping):
        candidates: list[_ClaimCandidate] = []
        for key in sorted(value, key=str):
            candidates.extend(_candidates_from_value(value[key], path=f"{path}.{key}"))
        return candidates
    if isinstance(value, (list, tuple)):
        candidates = []
        for index, item in enumerate(value):
            candidates.extend(_candidates_from_value(item, path=f"{path}[{index}]"))
        return candidates
    return []


def _claim_texts(value: str) -> list[str]:
    texts: list[str] = []
    for part in _SENTENCE_SPLIT_RE.split(value):
        text = _normalize_text(part)
        if _is_claim_text(text):
            texts.append(text)
    return texts


def _inline_citations(text: str) -> list[str]:
    markdown_matches = list(_MARKDOWN_LINK_RE.finditer(text))
    markdown_spans = [match.span() for match in markdown_matches]
    citations = [
        *[match.group(0) for match in markdown_matches],
        *[
            match.group(0).rstrip(".,;")
            for match in _URL_RE.finditer(text)
            if not _inside_any_span(match.span(), markdown_spans)
        ],
        *[match.group(0) for match in _FOOTNOTE_REF_RE.finditer(text)],
    ]
    return citations


def _reference_values(value: Any, *, prefix: str) -> list[tuple[str, str]]:
    references: list[tuple[str, str]] = []

    def append(current: Any, path: str, reference_context: bool) -> None:
        if isinstance(current, Mapping):
            for key in sorted(current, key=str):
                append(current[key], f"{path}.{key}", reference_context or _is_reference_key(str(key)))
            return
        if isinstance(current, (list, tuple)):
            for index, item in enumerate(current):
                append(item, f"{path}[{index}]", reference_context)
            return
        if not reference_context:
            return
        for reference in _reference_texts(current):
            references.append((path, reference))

    append(value, prefix, False)
    return references


def _reference_texts(value: Any) -> list[str]:
    if isinstance(value, str):
        text = _normalize_text(value)
        if not text:
            return []
        inline = _inline_citations(text)
        return inline or [text]
    if isinstance(value, int | float | bool):
        return [str(value)]
    if isinstance(value, Mapping):
        references: list[str] = []
        for key in sorted(value, key=str):
            references.extend(_reference_texts(value[key]))
        return references
    if isinstance(value, (list, tuple)):
        references = []
        for item in value:
            references.extend(_reference_texts(item))
        return references
    return []


def _text_values(value: Any, *, path: str = "") -> list[tuple[str, str]]:
    if isinstance(value, str):
        text = _normalize_text(value)
        return [(path, text)] if text else []
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=str):
            child_path = f"{path}.{key}" if path else str(key)
            texts.extend(_text_values(value[key], path=child_path))
        return texts
    if isinstance(value, (list, tuple)):
        texts = []
        for index, item in enumerate(value):
            texts.extend(_text_values(item, path=f"{path}[{index}]"))
        return texts
    return []


def _source_brief_payloads(
    source_briefs: Mapping[str, Any]
    | SourceBrief
    | list[Mapping[str, Any] | SourceBrief]
    | tuple[Mapping[str, Any] | SourceBrief, ...],
) -> list[dict[str, Any]]:
    if isinstance(source_briefs, SourceBrief) or isinstance(source_briefs, Mapping):
        return [_source_brief_payload(source_briefs)]
    return [_source_brief_payload(source_brief) for source_brief in source_briefs]


def _source_brief_payload(source_brief: Mapping[str, Any] | SourceBrief) -> dict[str, Any]:
    if hasattr(source_brief, "model_dump"):
        return source_brief.model_dump(mode="python")
    try:
        return SourceBrief.model_validate(source_brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(source_brief)


def _dedupe_candidates(candidates: list[_ClaimCandidate]) -> list[_ClaimCandidate]:
    deduped: list[_ClaimCandidate] = []
    seen: set[tuple[str, str]] = set()
    for candidate in candidates:
        key = (candidate.path, candidate.text.casefold())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _dedupe(values: list[str] | tuple[str, ...]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _normalize_text(value)
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(text)
    return deduped


def _claim_sort_key(claim: SourceCitationClaim) -> tuple[Any, ...]:
    return (
        claim.source_brief_id,
        claim.path,
        claim.evidence_text.casefold(),
        claim.coverage_status,
    )


def _is_claim_text(text: str) -> bool:
    if len(text) < 12:
        return False
    if not any(character.isalpha() for character in text):
        return False
    if _URL_RE.fullmatch(text):
        return False
    return True


def _is_reference_key(key: str) -> bool:
    return bool(_REFERENCE_KEY_RE.search(key))


def _is_claim_field(key: str) -> bool:
    return bool(_CLAIM_FIELD_RE.search(key))


def _inside_any_span(span: tuple[int, int], spans: list[tuple[int, int]]) -> bool:
    start, end = span
    return any(start >= existing_start and end <= existing_end for existing_start, existing_end in spans)


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _text(value: Any) -> str | None:
    if isinstance(value, str):
        text = _normalize_text(value)
        return text or None
    return None


def _normalize_text(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip()


__all__ = [
    "CitationCoverageStatus",
    "SourceCitationClaim",
    "SourceCitationCoverage",
    "SourceCitationCoverageStatus",
    "analyze_source_citation_coverage",
    "source_citation_coverage_to_dict",
]
