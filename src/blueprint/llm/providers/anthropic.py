"""Anthropic LLM provider implementation."""

from anthropic import Anthropic

from blueprint.llm.provider import LLMResponse


class AnthropicLLMProvider:
    """Anthropic API implementation of the LLM provider interface."""

    MODELS = {
        "opus": "claude-opus-4-6",
        "sonnet": "claude-sonnet-4-5",
    }

    def __init__(self, api_key: str, default_model: str = "claude-opus-4-6"):
        """Initialize the Anthropic provider."""
        self.client = Anthropic(api_key=api_key)
        self.default_model = default_model

    def generate(
        self,
        prompt: str,
        model: str | None = None,
        temperature: float = 1.0,
        max_tokens: int = 4096,
        system: str | None = None,
    ) -> LLMResponse:
        """Generate a normalized response from Anthropic."""
        model = model or self.default_model
        kwargs = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": "user", "content": prompt}],
        }

        if system:
            kwargs["system"] = system

        response = self.client.messages.create(**kwargs)

        content = ""
        for block in response.content:
            if block.type == "text":
                content += block.text

        return {
            "content": content,
            "model": response.model,
            "usage": {
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
                "total_tokens": (
                    response.usage.input_tokens + response.usage.output_tokens
                ),
            },
        }

    @classmethod
    def resolve_model(cls, model_alias: str) -> str:
        """Resolve an Anthropic model alias to a full model name."""
        return cls.MODELS.get(model_alias, model_alias)
