from types import SimpleNamespace

from blueprint.generators.brief_generator import BriefGenerator
from blueprint.generators.plan_generator import PlanGenerator
from blueprint.generators.plan_generator_staged import StagedPlanGenerator
from blueprint.generators.plan_reviser import PlanReviser
from blueprint.llm.client import LLMClient
from blueprint.llm.provider import create_llm_provider
from blueprint.llm.providers.anthropic import AnthropicLLMProvider


def test_anthropic_provider_normalizes_generate_response(monkeypatch):
    fake_messages = FakeMessages()

    class FakeAnthropic:
        def __init__(self, api_key):
            self.api_key = api_key
            self.messages = fake_messages

    monkeypatch.setattr("blueprint.llm.providers.anthropic.Anthropic", FakeAnthropic)

    provider = AnthropicLLMProvider(api_key="test-key", default_model="claude-default")
    response = provider.generate(
        prompt="Build a plan",
        model="claude-test",
        temperature=0.2,
        max_tokens=123,
        system="System prompt",
    )

    assert fake_messages.kwargs == {
        "model": "claude-test",
        "max_tokens": 123,
        "temperature": 0.2,
        "messages": [{"role": "user", "content": "Build a plan"}],
        "system": "System prompt",
    }
    assert response == {
        "content": "Hello world",
        "model": "claude-test",
        "usage": {
            "input_tokens": 3,
            "output_tokens": 4,
            "total_tokens": 7,
        },
    }


def test_create_llm_provider_uses_anthropic_by_default(monkeypatch):
    class FakeAnthropic:
        def __init__(self, api_key):
            self.api_key = api_key
            self.messages = FakeMessages()

    monkeypatch.setattr("blueprint.llm.providers.anthropic.Anthropic", FakeAnthropic)

    provider = create_llm_provider(
        {
            "provider": "anthropic",
            "api_key": "test-key",
            "default_model": "claude-default",
        }
    )

    assert isinstance(provider, AnthropicLLMProvider)
    assert provider.default_model == "claude-default"


def test_legacy_llm_client_alias_points_to_default_provider():
    assert LLMClient is AnthropicLLMProvider
    assert LLMClient.resolve_model("sonnet") == "claude-sonnet-4-5"


def test_generators_depend_on_provider_interface():
    provider = FakeProvider()

    assert BriefGenerator(provider).llm is provider
    assert PlanGenerator(provider).llm is provider
    assert StagedPlanGenerator(provider).llm is provider
    assert PlanReviser(provider).llm is provider


class FakeMessages:
    def __init__(self):
        self.kwargs = None

    def create(self, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace(
            content=[
                SimpleNamespace(type="text", text="Hello "),
                SimpleNamespace(type="tool_use", text="ignored"),
                SimpleNamespace(type="text", text="world"),
            ],
            model=kwargs["model"],
            usage=SimpleNamespace(input_tokens=3, output_tokens=4),
        )


class FakeProvider:
    default_model = "fake-model"

    def generate(
        self,
        prompt,
        model=None,
        temperature=1.0,
        max_tokens=4096,
        system=None,
    ):
        return {
            "content": "{}",
            "model": model or self.default_model,
            "usage": {
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
            },
        }

    @classmethod
    def resolve_model(cls, model_alias):
        return model_alias
