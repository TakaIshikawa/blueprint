"""Backward-compatible LLM client import."""

from blueprint.llm.providers.anthropic import AnthropicLLMProvider


LLMClient = AnthropicLLMProvider
