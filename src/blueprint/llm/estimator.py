"""Deterministic prompt size and input cost estimation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from math import ceil
import re

from blueprint.llm.providers.anthropic import AnthropicLLMProvider


APPROX_CHARS_PER_TOKEN = 4

INPUT_COST_PER_MILLION_TOKENS_USD = {
    "claude-opus-4-6": 15.00,
    "claude-sonnet-4-5": 3.00,
}


@dataclass(frozen=True)
class PromptEstimate:
    """Prompt size and estimated input cost."""

    model: str
    resolved_model: str
    characters: int
    words: int
    estimated_tokens: int
    estimated_input_cost_usd: float

    def to_dict(self) -> dict[str, str | int | float]:
        """Return a JSON-serializable representation."""
        return asdict(self)


def estimate_prompt(prompt: str, model: str) -> PromptEstimate:
    """Estimate prompt size and input cost for a supported model alias or model name."""
    resolved_model = resolve_estimation_model(model)
    characters = len(prompt)
    words = count_words(prompt)
    estimated_tokens = estimate_tokens(prompt)
    estimated_input_cost_usd = estimate_input_cost_usd(
        estimated_tokens,
        resolved_model=resolved_model,
    )

    return PromptEstimate(
        model=model,
        resolved_model=resolved_model,
        characters=characters,
        words=words,
        estimated_tokens=estimated_tokens,
        estimated_input_cost_usd=estimated_input_cost_usd,
    )


def resolve_estimation_model(model: str) -> str:
    """Resolve CLI aliases to the provider model names used for cost lookup."""
    return AnthropicLLMProvider.resolve_model(model)


def count_words(prompt: str) -> int:
    """Count whitespace-delimited words deterministically."""
    return len(re.findall(r"\S+", prompt))


def estimate_tokens(prompt: str) -> int:
    """Approximate tokens using a deterministic characters-per-token heuristic."""
    if not prompt:
        return 0
    return ceil(len(prompt) / APPROX_CHARS_PER_TOKEN)


def estimate_input_cost_usd(estimated_tokens: int, *, resolved_model: str) -> float:
    """Estimate input cost in USD from the model input token price table."""
    cost_per_million = INPUT_COST_PER_MILLION_TOKENS_USD.get(resolved_model, 0.0)
    return round((estimated_tokens / 1_000_000) * cost_per_million, 6)
