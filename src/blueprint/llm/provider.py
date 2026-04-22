"""Provider interface for LLM access."""

from typing import Any, Protocol, TypedDict


class LLMUsage(TypedDict):
    """Token usage returned by an LLM provider."""

    input_tokens: int
    output_tokens: int
    total_tokens: int


class LLMResponse(TypedDict):
    """Normalized response returned by an LLM provider."""

    content: str
    model: str
    usage: LLMUsage


class LLMProvider(Protocol):
    """Interface implemented by all LLM providers."""

    default_model: str

    def generate(
        self,
        prompt: str,
        model: str | None = None,
        temperature: float = 1.0,
        max_tokens: int = 4096,
        system: str | None = None,
    ) -> LLMResponse:
        """Generate a response for a prompt."""
        ...

    @classmethod
    def resolve_model(cls, model_alias: str) -> str:
        """Resolve a provider-specific model alias to a full model name."""
        ...


class LLMProviderConfig(TypedDict):
    """Configuration required to create an LLM provider."""

    provider: str
    api_key: str
    default_model: str


def create_llm_provider(config: LLMProviderConfig) -> LLMProvider:
    """Create an LLM provider from configuration."""
    provider = config["provider"]
    if provider == "anthropic":
        from blueprint.llm.providers.anthropic import AnthropicLLMProvider

        return AnthropicLLMProvider(
            api_key=config["api_key"],
            default_model=config["default_model"],
        )

    raise ValueError(f"Unsupported LLM provider configured: {provider}")


def resolve_model_alias(provider: type[LLMProvider] | LLMProvider, model_alias: str) -> str:
    """Resolve a model alias using the provider implementation."""
    resolver = getattr(provider, "resolve_model", None)
    if resolver is None:
        return model_alias
    return resolver(model_alias)
