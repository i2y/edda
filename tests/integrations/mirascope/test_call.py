"""Tests for call() and call_with_messages() functions (V2 API)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from edda.integrations.mirascope.call import call, call_with_messages


class TestCallFunction:
    """Tests for the call() function."""

    @pytest.mark.asyncio
    async def test_call_basic(self) -> None:
        """Test basic call functionality with mocked Mirascope."""
        # Mock the Mirascope response
        mock_response = MagicMock()
        mock_response.content = "Hello from LLM!"
        # Mirascope V2: model is an object, model_id is the string
        mock_response.model_id = "claude-sonnet-4-20250514"
        mock_response.usage = None
        mock_response.tool_calls = None

        # Mock the llm.call decorator and messages
        mock_llm = MagicMock()
        mock_call_func = AsyncMock(return_value=mock_response)
        mock_llm.call.return_value = lambda f: mock_call_func

        # V2: llm.messages.system/user
        mock_llm.messages = MagicMock()
        mock_llm.messages.system = lambda x: {"role": "system", "content": x}
        mock_llm.messages.user = lambda x: {"role": "user", "content": x}

        with patch(
            "edda.integrations.mirascope.call._import_mirascope",
            return_value=mock_llm,
        ):
            # Create a mock context
            mock_ctx = MagicMock()

            # Call the function (bypassing activity wrapper for unit test)
            # V2: Use unified model string
            result = await call.func(
                mock_ctx,
                model="anthropic/claude-sonnet-4-20250514",
                prompt="Hello!",
            )

            assert result["content"] == "Hello from LLM!"
            assert result["provider"] == "anthropic"
            assert result["model"] == "claude-sonnet-4-20250514"

    @pytest.mark.asyncio
    async def test_call_with_system_prompt(self) -> None:
        """Test call with system prompt."""
        mock_response = MagicMock()
        mock_response.content = "Response"
        mock_response.model = "gpt-4"
        mock_response.usage = None
        mock_response.tool_calls = None

        mock_llm = MagicMock()
        mock_call_func = AsyncMock(return_value=mock_response)
        mock_llm.call.return_value = lambda f: mock_call_func

        mock_llm.messages = MagicMock()
        mock_llm.messages.system = lambda x: {"role": "system", "content": x}
        mock_llm.messages.user = lambda x: {"role": "user", "content": x}

        with patch(
            "edda.integrations.mirascope.call._import_mirascope",
            return_value=mock_llm,
        ):
            mock_ctx = MagicMock()

            # V2: Use unified model string
            result = await call.func(
                mock_ctx,
                model="openai/gpt-4",
                prompt="What is 2+2?",
                system="You are a math tutor.",
            )

            assert result["content"] == "Response"
            assert result["provider"] == "openai"

    @pytest.mark.asyncio
    async def test_provider_extraction(self) -> None:
        """Test that provider is correctly extracted from model string."""
        mock_response = MagicMock()
        mock_response.content = "Response"
        mock_response.model = "gemini-pro"
        mock_response.usage = None
        mock_response.tool_calls = None

        mock_llm = MagicMock()
        mock_call_func = AsyncMock(return_value=mock_response)
        mock_llm.call.return_value = lambda f: mock_call_func

        mock_llm.messages = MagicMock()
        mock_llm.messages.system = lambda x: {"role": "system", "content": x}
        mock_llm.messages.user = lambda x: {"role": "user", "content": x}

        with patch(
            "edda.integrations.mirascope.call._import_mirascope",
            return_value=mock_llm,
        ):
            mock_ctx = MagicMock()

            result = await call.func(
                mock_ctx,
                model="google/gemini-pro",
                prompt="Hello!",
            )

            assert result["provider"] == "google"


class TestCallWithMessages:
    """Tests for the call_with_messages() function."""

    @pytest.mark.asyncio
    async def test_call_with_messages_basic(self) -> None:
        """Test call_with_messages with a conversation history."""
        mock_response = MagicMock()
        mock_response.content = "The weather is sunny."
        mock_response.model = "claude-sonnet-4-20250514"
        mock_response.usage = None
        mock_response.tool_calls = None

        mock_llm = MagicMock()
        mock_call_func = AsyncMock(return_value=mock_response)
        mock_llm.call.return_value = lambda f: mock_call_func

        # V2: llm.messages.system/user/assistant
        mock_llm.messages = MagicMock()
        mock_llm.messages.system = lambda x: {"role": "system", "content": x}
        mock_llm.messages.user = lambda x: {"role": "user", "content": x}
        mock_llm.messages.assistant = lambda x: {"role": "assistant", "content": x}

        with patch(
            "edda.integrations.mirascope.call._import_mirascope",
            return_value=mock_llm,
        ):
            mock_ctx = MagicMock()

            messages = [
                {"role": "system", "content": "You are a weather assistant."},
                {"role": "user", "content": "What's the weather?"},
                {"role": "assistant", "content": "Where would you like to know?"},
                {"role": "user", "content": "Tokyo"},
            ]

            # V2: Use unified model string
            result = await call_with_messages.func(
                mock_ctx,
                model="anthropic/claude-sonnet-4-20250514",
                messages=messages,
            )

            assert result["content"] == "The weather is sunny."
            assert result["provider"] == "anthropic"


class TestImportError:
    """Tests for import error handling."""

    @pytest.mark.asyncio
    async def test_import_error_message(self) -> None:
        """Test that import error provides helpful message."""
        with patch(
            "edda.integrations.mirascope.call._import_mirascope",
            side_effect=ImportError("Mirascope not installed"),
        ):
            mock_ctx = MagicMock()

            with pytest.raises(ImportError, match="Mirascope not installed"):
                # V2: Use unified model string
                await call.func(
                    mock_ctx,
                    model="anthropic/claude-sonnet-4-20250514",
                    prompt="Test",
                )
