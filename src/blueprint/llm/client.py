"""Anthropic API client wrapper for Blueprint."""

from typing import Any

from anthropic import Anthropic


class LLMClient:
    """Wrapper for Anthropic API with Blueprint-specific conveniences."""

    # Model name mapping
    MODELS = {
        "opus": "claude-opus-4-6",
        "sonnet": "claude-sonnet-4-5",
    }

    def __init__(self, api_key: str, default_model: str = "claude-opus-4-6"):
        """
        Initialize LLM client.

        Args:
            api_key: Anthropic API key
            default_model: Default model to use
        """
        self.client = Anthropic(api_key=api_key)
        self.default_model = default_model

    def generate(
        self,
        prompt: str,
        model: str | None = None,
        temperature: float = 1.0,
        max_tokens: int = 4096,
        system: str | None = None,
    ) -> dict[str, Any]:
        """
        Generate a response from the LLM.

        Args:
            prompt: User prompt
            model: Model to use (defaults to default_model)
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate
            system: Optional system prompt

        Returns:
            Dictionary with:
                - content: Generated text
                - model: Model used
                - usage: Token usage stats
        """
        model = model or self.default_model

        # Build message
        messages = [{"role": "user", "content": prompt}]

        # Call API
        kwargs = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": messages,
        }

        if system:
            kwargs["system"] = system

        response = self.client.messages.create(**kwargs)

        # Extract content
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
                "total_tokens": response.usage.input_tokens + response.usage.output_tokens,
            },
        }

    @classmethod
    def resolve_model(cls, model_alias: str) -> str:
        """
        Resolve a model alias to full model name.

        Args:
            model_alias: Short name like "opus" or "sonnet", or full model name

        Returns:
            Full model name
        """
        return cls.MODELS.get(model_alias, model_alias)
