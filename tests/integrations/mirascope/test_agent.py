"""Tests for DurableAgent and DurableDeps classes."""

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from edda.integrations.mirascope.agent import (
    DurableAgent,
    DurableDeps,
    _chat_activity,
)


class TestDurableDeps:
    """Tests for DurableDeps class."""

    def test_basic_creation(self) -> None:
        """Test creating DurableDeps with basic data."""

        @dataclass
        class MyDeps:
            name: str
            count: int

        deps = DurableDeps(data=MyDeps(name="test", count=42))
        assert deps.data.name == "test"
        assert deps.data.count == 42
        assert deps.history == []

    def test_to_dict_with_dataclass(self) -> None:
        """Test serialization with dataclass data."""

        @dataclass
        class MyDeps:
            name: str
            items: list[str]

        deps = DurableDeps(data=MyDeps(name="test", items=["a", "b"]))
        result = deps.to_dict()

        assert result["data"]["name"] == "test"
        assert result["data"]["items"] == ["a", "b"]
        assert result["history"] == []

    def test_to_dict_with_dict(self) -> None:
        """Test serialization with dict data."""
        deps = DurableDeps(data={"key": "value", "count": 5})
        result = deps.to_dict()

        assert result["data"]["key"] == "value"
        assert result["data"]["count"] == 5

    def test_history_management(self) -> None:
        """Test adding messages to history."""
        deps: DurableDeps[dict[str, str]] = DurableDeps(data={"key": "value"})

        deps.add_system_message("You are helpful")
        deps.add_user_message("Hello")
        deps.add_assistant_message("Hi there!")

        assert len(deps.history) == 3
        assert deps.history[0] == {"role": "system", "content": "You are helpful"}
        assert deps.history[1] == {"role": "user", "content": "Hello"}
        assert deps.history[2] == {"role": "assistant", "content": "Hi there!"}

    def test_clear_history(self) -> None:
        """Test clearing conversation history."""
        deps: DurableDeps[dict[str, str]] = DurableDeps(data={"key": "value"})
        deps.add_user_message("Hello")
        deps.add_assistant_message("Hi")

        assert len(deps.history) == 2

        deps.clear_history()
        assert deps.history == []


class TestDurableAgent:
    """Tests for DurableAgent class."""

    def test_agent_creation(self) -> None:
        """Test creating a DurableAgent instance."""
        mock_ctx = MagicMock()

        class MyAgent(DurableAgent[dict[str, str]]):
            model = "anthropic/claude-sonnet-4-20250514"

        agent = MyAgent(mock_ctx)
        assert agent.model == "anthropic/claude-sonnet-4-20250514"
        assert agent._turn_count == 0

    def test_default_get_tools(self) -> None:
        """Test that default get_tools returns None."""
        mock_ctx = MagicMock()

        class MyAgent(DurableAgent[dict[str, str]]):
            pass

        agent = MyAgent(mock_ctx)
        assert agent.get_tools() is None

    def test_custom_get_tools(self) -> None:
        """Test overriding get_tools."""
        mock_ctx = MagicMock()

        def my_tool(query: str) -> str:
            return f"Result for {query}"

        class MyAgent(DurableAgent[dict[str, str]]):
            def get_tools(self) -> list:
                return [my_tool]

        agent = MyAgent(mock_ctx)
        tools = agent.get_tools()
        assert tools is not None
        assert len(tools) == 1

    @pytest.mark.asyncio
    async def test_chat_basic(self) -> None:
        """Test basic chat functionality."""
        mock_response = MagicMock()
        mock_response.content = "Hello from LLM!"
        mock_response.model = "claude-sonnet-4-20250514"
        mock_response.usage = None
        mock_response.tool_calls = None

        mock_llm = MagicMock()
        mock_call_func = AsyncMock(return_value=mock_response)
        mock_llm.call.return_value = lambda f: mock_call_func
        mock_llm.messages = MagicMock()
        mock_llm.messages.system = lambda x: MagicMock(role="system", content=x)
        mock_llm.messages.user = lambda x: MagicMock(role="user", content=x)
        mock_llm.messages.assistant = lambda x: MagicMock(role="assistant", content=x)
        mock_llm.Context = lambda deps: MagicMock(deps=deps)

        with patch(
            "edda.integrations.mirascope.agent._import_mirascope",
            return_value=mock_llm,
        ):
            mock_ctx = MagicMock()

            class MyAgent(DurableAgent[dict[str, str]]):
                model = "anthropic/claude-sonnet-4-20250514"

            agent = MyAgent(mock_ctx)  # noqa: F841

            # Call the internal function directly (bypassing activity wrapper)
            result = await _chat_activity.func(
                mock_ctx,
                model="anthropic/claude-sonnet-4-20250514",
                messages=[{"role": "user", "content": "Hello!"}],
                tools=None,
                response_model=None,
                deps_dict={"data": {}, "history": []},
                turn=1,
            )

            assert result["content"] == "Hello from LLM!"
            assert result["provider"] == "anthropic"

    @pytest.mark.asyncio
    async def test_chat_updates_history(self) -> None:
        """Test that chat updates DurableDeps history."""
        mock_response = MagicMock()
        mock_response.content = "Response!"
        mock_response.model = "claude-sonnet-4-20250514"
        mock_response.usage = None
        mock_response.tool_calls = None

        mock_llm = MagicMock()
        mock_call_func = AsyncMock(return_value=mock_response)
        mock_llm.call.return_value = lambda f: mock_call_func
        mock_llm.messages = MagicMock()
        mock_llm.messages.system = lambda x: MagicMock(role="system", content=x)
        mock_llm.messages.user = lambda x: MagicMock(role="user", content=x)
        mock_llm.messages.assistant = lambda x: MagicMock(role="assistant", content=x)
        mock_llm.Context = lambda deps: MagicMock(deps=deps)

        with patch(
            "edda.integrations.mirascope.agent._import_mirascope",
            return_value=mock_llm,
        ):
            mock_ctx = MagicMock()

            # Mock the _chat_activity to return our response
            class MyAgent(DurableAgent[dict[str, str]]):
                model = "anthropic/claude-sonnet-4-20250514"

            agent = MyAgent(mock_ctx)

            # Patch the module-level _chat_activity to avoid actual activity execution
            with patch(
                "edda.integrations.mirascope.agent._chat_activity",
                new=AsyncMock(
                    return_value={
                        "content": "Response!",
                        "provider": "anthropic",
                        "model": "claude-sonnet-4-20250514",
                    }
                ),
            ):
                deps: DurableDeps[dict[str, str]] = DurableDeps(data={"key": "value"})

                await agent.chat(deps, "Hello!")

                # Check history was updated
                assert len(deps.history) == 2
                assert deps.history[0] == {"role": "user", "content": "Hello!"}
                assert deps.history[1] == {"role": "assistant", "content": "Response!"}

    def test_messages_to_dict(self) -> None:
        """Test message conversion to dict format."""
        mock_ctx = MagicMock()

        class MyAgent(DurableAgent[dict[str, str]]):
            pass

        agent = MyAgent(mock_ctx)

        # Test with dict messages
        dict_messages = [
            {"role": "system", "content": "System prompt"},
            {"role": "user", "content": "Hello"},
        ]
        result = agent._messages_to_dict(dict_messages)
        assert result == dict_messages

        # Test with mock message objects
        mock_msg = MagicMock()
        mock_msg.role = "user"
        mock_msg.content = "Test message"

        result = agent._messages_to_dict([mock_msg])
        assert result == [{"role": "user", "content": "Test message"}]

    @pytest.mark.asyncio
    async def test_chat_with_tool_calls(self) -> None:
        """Test chat that returns tool calls."""
        mock_response = MagicMock()
        mock_response.content = ""
        mock_response.model = "claude-sonnet-4-20250514"
        mock_response.usage = None
        mock_response.tool_calls = [
            MagicMock(model_dump=lambda: {"name": "get_weather", "args": {"city": "Tokyo"}})
        ]

        mock_llm = MagicMock()
        mock_call_func = AsyncMock(return_value=mock_response)
        mock_llm.call.return_value = lambda f: mock_call_func
        mock_llm.messages = MagicMock()
        mock_llm.messages.system = lambda x: MagicMock(role="system", content=x)
        mock_llm.messages.user = lambda x: MagicMock(role="user", content=x)
        mock_llm.messages.assistant = lambda x: MagicMock(role="assistant", content=x)
        mock_llm.Context = lambda deps: MagicMock(deps=deps)

        with patch(
            "edda.integrations.mirascope.agent._import_mirascope",
            return_value=mock_llm,
        ):
            mock_ctx = MagicMock()

            class MyAgent(DurableAgent[dict[str, str]]):
                model = "anthropic/claude-sonnet-4-20250514"

            MyAgent(mock_ctx)  # noqa: F841 - Just to verify creation works

            result = await _chat_activity.func(
                mock_ctx,
                model="anthropic/claude-sonnet-4-20250514",
                messages=[{"role": "user", "content": "What's the weather?"}],
                tools=None,
                response_model=None,
                deps_dict={"data": {}, "history": []},
                turn=1,
            )

            assert result["tool_calls"] is not None
            assert len(result["tool_calls"]) == 1
            assert result["tool_calls"][0]["name"] == "get_weather"

    @pytest.mark.asyncio
    async def test_chat_with_response_model(self) -> None:
        """Test chat with structured output."""
        from pydantic import BaseModel

        class WeatherInfo(BaseModel):
            city: str
            temperature: int
            condition: str

        mock_response = WeatherInfo(city="Tokyo", temperature=22, condition="Sunny")

        mock_llm = MagicMock()
        mock_call_func = AsyncMock(return_value=mock_response)
        mock_llm.call.return_value = lambda f: mock_call_func
        mock_llm.messages = MagicMock()
        mock_llm.messages.system = lambda x: MagicMock(role="system", content=x)
        mock_llm.messages.user = lambda x: MagicMock(role="user", content=x)
        mock_llm.messages.assistant = lambda x: MagicMock(role="assistant", content=x)
        mock_llm.Context = lambda deps: MagicMock(deps=deps)

        with patch(
            "edda.integrations.mirascope.agent._import_mirascope",
            return_value=mock_llm,
        ):
            mock_ctx = MagicMock()

            class MyAgent(DurableAgent[dict[str, str]]):
                model = "anthropic/claude-sonnet-4-20250514"
                response_model = WeatherInfo

            MyAgent(mock_ctx)  # noqa: F841 - Just to verify creation works

            result = await _chat_activity.func(
                mock_ctx,
                model="anthropic/claude-sonnet-4-20250514",
                messages=[{"role": "user", "content": "Weather in Tokyo?"}],
                tools=None,
                response_model=WeatherInfo,
                deps_dict={"data": {}, "history": []},
                turn=1,
            )

            assert "structured_output" in result
            assert result["structured_output"]["city"] == "Tokyo"
            assert result["structured_output"]["temperature"] == 22


class TestDurableAgentCustomization:
    """Tests for DurableAgent customization."""

    def test_custom_model(self) -> None:
        """Test setting custom model."""
        mock_ctx = MagicMock()

        class GPT4Agent(DurableAgent[dict[str, str]]):
            model = "openai/gpt-4"

        agent = GPT4Agent(mock_ctx)
        assert agent.model == "openai/gpt-4"

    @pytest.mark.asyncio
    async def test_custom_build_prompt(self) -> None:
        """Test overriding build_prompt."""
        mock_llm = MagicMock()
        mock_llm.messages = MagicMock()
        mock_llm.messages.system = lambda x: {"role": "system", "content": x}
        mock_llm.messages.user = lambda x: {"role": "user", "content": x}
        mock_llm.Context = lambda deps: MagicMock(deps=deps)

        with patch(
            "edda.integrations.mirascope.agent._import_mirascope",
            return_value=mock_llm,
        ):
            mock_ctx = MagicMock()

            @dataclass
            class RAGDeps:
                documents: list[str]

            class RAGAgent(DurableAgent[RAGDeps]):
                model = "anthropic/claude-sonnet-4-20250514"

                def build_prompt(self, ctx, message):
                    llm = mock_llm
                    docs_str = "\n".join(ctx.deps.documents)
                    return [
                        llm.messages.system(f"Documents:\n{docs_str}"),
                        llm.messages.user(message),
                    ]

            agent = RAGAgent(mock_ctx)
            deps = RAGDeps(documents=["Doc 1", "Doc 2"])
            llm_ctx = mock_llm.Context(deps=deps)

            messages = agent.build_prompt(llm_ctx, "Search for info")

            assert len(messages) == 2
            assert messages[0]["role"] == "system"
            assert "Doc 1" in messages[0]["content"]
            assert messages[1]["role"] == "user"
            assert messages[1]["content"] == "Search for info"


class TestImportError:
    """Tests for import error handling."""

    def test_import_error_message(self) -> None:
        """Test that import error provides helpful message."""
        with patch(
            "edda.integrations.mirascope.agent._import_mirascope",
            side_effect=ImportError("Mirascope not installed"),
        ):
            mock_ctx = MagicMock()

            class MyAgent(DurableAgent[dict[str, str]]):
                pass

            agent = MyAgent(mock_ctx)

            with pytest.raises(ImportError, match="Mirascope not installed"):
                agent.build_prompt(MagicMock(), "test")
