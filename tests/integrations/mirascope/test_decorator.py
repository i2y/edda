"""Tests for @durable_call decorator (V2 API)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from edda.integrations.mirascope.decorator import durable_call


class TestDurableCallDecorator:
    """Tests for the @durable_call decorator."""

    def test_decorator_creates_activity(self) -> None:
        """Test that decorator creates an activity-wrapped function."""
        mock_llm = MagicMock()
        mock_llm.call.return_value = lambda f: f

        with patch(
            "edda.integrations.mirascope.decorator._import_mirascope",
            return_value=mock_llm,
        ):

            @durable_call("anthropic/claude-sonnet-4-20250514")
            async def test_func(prompt: str) -> str:
                return prompt

            # Check that the function is marked as an activity
            assert hasattr(test_func, "_is_activity")
            assert test_func._is_activity is True

    def test_decorator_stores_metadata(self) -> None:
        """Test that decorator stores provider/model metadata."""
        mock_llm = MagicMock()
        mock_llm.call.return_value = lambda f: f

        with patch(
            "edda.integrations.mirascope.decorator._import_mirascope",
            return_value=mock_llm,
        ):

            @durable_call(
                "openai/gpt-4",
                tools=[lambda x: x],
            )
            async def test_func(prompt: str) -> str:
                return prompt

            # V2: provider is extracted from model string
            assert test_func._provider == "openai"
            assert test_func._model == "openai/gpt-4"
            assert test_func._tools is not None

    @pytest.mark.asyncio
    async def test_decorator_calls_mirascope(self) -> None:
        """Test that decorated function calls Mirascope."""
        mock_response = MagicMock()
        mock_response.content = "LLM response"
        mock_response.model = "claude-sonnet-4-20250514"
        mock_response.usage = None
        mock_response.tool_calls = None

        mock_decorated = AsyncMock(return_value=mock_response)
        mock_llm = MagicMock()
        mock_llm.call.return_value = lambda f: mock_decorated

        with patch(
            "edda.integrations.mirascope.decorator._import_mirascope",
            return_value=mock_llm,
        ):

            @durable_call("anthropic/claude-sonnet-4-20250514")
            async def summarize(text: str) -> str:
                return f"Summarize: {text}"

            # Call the function (bypass activity wrapper for unit test)
            mock_ctx = MagicMock()
            result = await summarize.func(mock_ctx, "test text")

            assert result["content"] == "LLM response"
            assert result["provider"] == "anthropic"

    @pytest.mark.asyncio
    async def test_decorator_with_tools(self) -> None:
        """Test decorator with tools parameter."""
        mock_response = MagicMock()
        mock_response.content = ""
        mock_response.model = "claude-sonnet-4-20250514"
        mock_response.usage = None
        mock_response.tool_calls = [
            MagicMock(model_dump=lambda: {"name": "get_weather", "args": {}})
        ]
        mock_response.stop_reason = "tool_use"

        mock_decorated = AsyncMock(return_value=mock_response)
        mock_llm = MagicMock()
        mock_llm.call.return_value = lambda f: mock_decorated

        def get_weather(city: str) -> str:
            """Get weather for a city."""
            return f"Sunny in {city}"

        with patch(
            "edda.integrations.mirascope.decorator._import_mirascope",
            return_value=mock_llm,
        ):

            @durable_call(
                "anthropic/claude-sonnet-4-20250514",
                tools=[get_weather],
            )
            async def weather_bot(query: str) -> str:
                return query

            mock_ctx = MagicMock()
            result = await weather_bot.func(mock_ctx, "What's the weather in Tokyo?")

            assert result["tool_calls"] is not None
            assert len(result["tool_calls"]) == 1

    @pytest.mark.asyncio
    async def test_decorator_with_response_model(self) -> None:
        """Test decorator with response_model for structured output."""
        from pydantic import BaseModel

        class BookInfo(BaseModel):
            title: str
            author: str

        mock_response = BookInfo(title="Test Book", author="Test Author")

        mock_decorated = AsyncMock(return_value=mock_response)
        mock_llm = MagicMock()
        mock_llm.call.return_value = lambda f: mock_decorated

        with patch(
            "edda.integrations.mirascope.decorator._import_mirascope",
            return_value=mock_llm,
        ):

            @durable_call(
                "anthropic/claude-sonnet-4-20250514",
                response_model=BookInfo,
            )
            async def extract_book(text: str) -> str:
                return f"Extract from: {text}"

            mock_ctx = MagicMock()
            result = await extract_book.func(mock_ctx, "The Great Gatsby by F. Scott Fitzgerald")

            assert "structured_output" in result
            assert result["structured_output"]["title"] == "Test Book"
            assert result["structured_output"]["author"] == "Test Author"

    @pytest.mark.asyncio
    async def test_decorator_with_sync_function(self) -> None:
        """Test decorator with synchronous original function."""
        mock_response = MagicMock()
        mock_response.content = "Sync response"
        mock_response.model = "claude-sonnet-4-20250514"
        mock_response.usage = None
        mock_response.tool_calls = None

        # Sync mock
        mock_decorated = MagicMock(return_value=mock_response)
        mock_llm = MagicMock()
        mock_llm.call.return_value = lambda f: mock_decorated

        with patch(
            "edda.integrations.mirascope.decorator._import_mirascope",
            return_value=mock_llm,
        ):

            @durable_call("anthropic/claude-sonnet-4-20250514")
            def sync_func(prompt: str) -> str:
                return prompt

            mock_ctx = MagicMock()
            result = await sync_func.func(mock_ctx, "test")

            assert result["content"] == "Sync response"

    def test_provider_extraction_from_model_string(self) -> None:
        """Test that provider is correctly extracted from model string."""
        mock_llm = MagicMock()
        mock_llm.call.return_value = lambda f: f

        with patch(
            "edda.integrations.mirascope.decorator._import_mirascope",
            return_value=mock_llm,
        ):
            # Test various provider/model formats
            @durable_call("anthropic/claude-sonnet-4-20250514")
            async def func1(prompt: str) -> str:
                return prompt

            assert func1._provider == "anthropic"

            @durable_call("openai/gpt-4")
            async def func2(prompt: str) -> str:
                return prompt

            assert func2._provider == "openai"

            @durable_call("google/gemini-pro")
            async def func3(prompt: str) -> str:
                return prompt

            assert func3._provider == "google"

            # Model without slash
            @durable_call("gpt-4")
            async def func4(prompt: str) -> str:
                return prompt

            assert func4._provider == "unknown"


class TestImportError:
    """Tests for import error handling."""

    def test_import_error_on_decoration(self) -> None:
        """Test that import error is raised when Mirascope is not installed."""
        with (
            patch(
                "edda.integrations.mirascope.decorator._import_mirascope",
                side_effect=ImportError(
                    "Mirascope not installed. Install with: pip install 'mirascope[anthropic]'"
                ),
            ),
            pytest.raises(ImportError, match="Mirascope not installed"),
        ):

            @durable_call("anthropic/claude-sonnet-4-20250514")
            async def test_func(prompt: str) -> str:
                return prompt
