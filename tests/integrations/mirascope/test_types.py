"""Tests for DurableResponse type."""

from edda.integrations.mirascope.types import DurableResponse


class TestDurableResponse:
    """Tests for DurableResponse class."""

    def test_create_basic(self) -> None:
        """Test creating a basic DurableResponse."""
        response = DurableResponse(
            content="Hello, world!",
            model="claude-sonnet-4-20250514",
            provider="anthropic",
        )
        assert response.content == "Hello, world!"
        assert response.model == "claude-sonnet-4-20250514"
        assert response.provider == "anthropic"
        assert response.usage is None
        assert response.tool_calls is None
        assert response.stop_reason is None
        assert response.raw == {}

    def test_create_with_all_fields(self) -> None:
        """Test creating a DurableResponse with all fields."""
        response = DurableResponse(
            content="Hello!",
            model="gpt-4",
            provider="openai",
            usage={"input_tokens": 10, "output_tokens": 5},
            tool_calls=[{"name": "get_weather", "args": {"city": "Tokyo"}}],
            stop_reason="end_turn",
            raw={"id": "msg_123"},
        )
        assert response.content == "Hello!"
        assert response.usage == {"input_tokens": 10, "output_tokens": 5}
        assert response.tool_calls == [{"name": "get_weather", "args": {"city": "Tokyo"}}]
        assert response.stop_reason == "end_turn"
        assert response.raw == {"id": "msg_123"}

    def test_to_dict(self) -> None:
        """Test converting to dictionary."""
        response = DurableResponse(
            content="Test",
            model="claude-sonnet-4-20250514",
            provider="anthropic",
            usage={"total": 15},
        )
        result = response.to_dict()
        assert result == {
            "content": "Test",
            "model": "claude-sonnet-4-20250514",
            "provider": "anthropic",
            "usage": {"total": 15},
            "tool_calls": None,
            "stop_reason": None,
            "raw": {},
        }

    def test_from_dict(self) -> None:
        """Test creating from dictionary."""
        data = {
            "content": "Hello",
            "model": "claude-sonnet-4-20250514",
            "provider": "anthropic",
            "usage": {"input_tokens": 5},
            "tool_calls": None,
            "stop_reason": "end_turn",
            "raw": {},
        }
        response = DurableResponse.from_dict(data)
        assert response.content == "Hello"
        assert response.model == "claude-sonnet-4-20250514"
        assert response.provider == "anthropic"
        assert response.usage == {"input_tokens": 5}
        assert response.stop_reason == "end_turn"

    def test_from_dict_missing_fields(self) -> None:
        """Test creating from dictionary with missing optional fields."""
        data = {
            "content": "Test",
            "model": "model",
            "provider": "provider",
        }
        response = DurableResponse.from_dict(data)
        assert response.content == "Test"
        assert response.usage is None
        assert response.tool_calls is None
        assert response.stop_reason is None
        assert response.raw == {}

    def test_roundtrip(self) -> None:
        """Test roundtrip serialization."""
        original = DurableResponse(
            content="Round trip test",
            model="claude-sonnet-4-20250514",
            provider="anthropic",
            usage={"input_tokens": 10, "output_tokens": 20},
            tool_calls=[{"name": "test", "args": {}}],
            stop_reason="end_turn",
            raw={"extra": "data"},
        )
        data = original.to_dict()
        restored = DurableResponse.from_dict(data)

        assert restored.content == original.content
        assert restored.model == original.model
        assert restored.provider == original.provider
        assert restored.usage == original.usage
        assert restored.tool_calls == original.tool_calls
        assert restored.stop_reason == original.stop_reason
        assert restored.raw == original.raw

    def test_has_tool_calls_true(self) -> None:
        """Test has_tool_calls property when tool calls exist."""
        response = DurableResponse(
            content="",
            model="model",
            provider="provider",
            tool_calls=[{"name": "tool"}],
        )
        assert response.has_tool_calls is True

    def test_has_tool_calls_false(self) -> None:
        """Test has_tool_calls property when no tool calls."""
        response = DurableResponse(
            content="",
            model="model",
            provider="provider",
            tool_calls=None,
        )
        assert response.has_tool_calls is False

    def test_has_tool_calls_empty_list(self) -> None:
        """Test has_tool_calls property with empty list."""
        response = DurableResponse(
            content="",
            model="model",
            provider="provider",
            tool_calls=[],
        )
        assert response.has_tool_calls is False


class TestFromMirascope:
    """Tests for from_mirascope class method."""

    def test_from_mirascope_basic(self) -> None:
        """Test converting from a mock Mirascope response."""

        class MockResponse:
            content = "Hello from LLM"
            model = "claude-sonnet-4-20250514"
            usage = None
            tool_calls = None

        response = DurableResponse.from_mirascope(MockResponse(), "anthropic")
        assert response.content == "Hello from LLM"
        assert response.model == "claude-sonnet-4-20250514"
        assert response.provider == "anthropic"

    def test_from_mirascope_with_usage(self) -> None:
        """Test converting with usage data."""

        class MockUsage:
            def model_dump(self) -> dict:
                return {"input_tokens": 10, "output_tokens": 5}

        class MockResponse:
            content = "Test"
            model = "gpt-4"
            usage = MockUsage()
            tool_calls = None

        response = DurableResponse.from_mirascope(MockResponse(), "openai")
        assert response.usage == {"input_tokens": 10, "output_tokens": 5}

    def test_from_mirascope_with_dict_usage(self) -> None:
        """Test converting with usage as dictionary."""

        class MockResponse:
            content = "Test"
            model = "gpt-4"
            usage = {"input_tokens": 15, "output_tokens": 10}
            tool_calls = None

        response = DurableResponse.from_mirascope(MockResponse(), "openai")
        assert response.usage == {"input_tokens": 15, "output_tokens": 10}

    def test_from_mirascope_with_tool_calls(self) -> None:
        """Test converting with tool calls."""

        class MockToolCall:
            def model_dump(self) -> dict:
                return {"name": "get_weather", "args": {"city": "Tokyo"}, "id": "tc_1"}

        class MockResponse:
            content = ""
            model = "claude-sonnet-4-20250514"
            usage = None
            tool_calls = [MockToolCall()]
            stop_reason = "tool_use"

        response = DurableResponse.from_mirascope(MockResponse(), "anthropic")
        assert response.tool_calls == [
            {"name": "get_weather", "args": {"city": "Tokyo"}, "id": "tc_1"}
        ]
        assert response.stop_reason == "tool_use"

    def test_from_mirascope_with_stop_reason(self) -> None:
        """Test converting with stop_reason."""

        class MockResponse:
            content = "Done"
            model = "model"
            usage = None
            tool_calls = None
            stop_reason = "max_tokens"

        response = DurableResponse.from_mirascope(MockResponse(), "provider")
        assert response.stop_reason == "max_tokens"

    def test_from_mirascope_with_finish_reason_fallback(self) -> None:
        """Test converting with finish_reason fallback."""

        class MockResponse:
            content = "Done"
            model = "model"
            usage = None
            tool_calls = None
            finish_reason = "stop"

        response = DurableResponse.from_mirascope(MockResponse(), "provider")
        assert response.stop_reason == "stop"

    def test_from_mirascope_none_content(self) -> None:
        """Test converting with None content."""

        class MockResponse:
            content = None
            model = "model"
            usage = None
            tool_calls = None

        response = DurableResponse.from_mirascope(MockResponse(), "provider")
        assert response.content == ""

    def test_from_mirascope_tool_call_without_model_dump(self) -> None:
        """Test converting tool calls without model_dump method."""

        class MockToolCall:
            name = "search"
            args = {"query": "test"}
            id = "tc_123"

        class MockResponse:
            content = ""
            model = "model"
            usage = None
            tool_calls = [MockToolCall()]

        response = DurableResponse.from_mirascope(MockResponse(), "provider")
        assert response.tool_calls == [
            {"name": "search", "args": {"query": "test"}, "id": "tc_123"}
        ]

    def test_from_mirascope_tool_call_alternate_attributes(self) -> None:
        """Test converting tool calls with alternate attribute names."""

        class MockToolCall:
            tool_name = "calculate"
            arguments = {"x": 1, "y": 2}
            tool_call_id = "tc_456"

        class MockResponse:
            content = ""
            model = "model"
            usage = None
            tool_calls = [MockToolCall()]

        response = DurableResponse.from_mirascope(MockResponse(), "provider")
        assert response.tool_calls is not None
        assert response.tool_calls[0]["name"] == "calculate"
        assert response.tool_calls[0]["args"] == {"x": 1, "y": 2}
        assert response.tool_calls[0]["id"] == "tc_456"

    def test_from_mirascope_v2_text_object_content(self) -> None:
        """Test converting Mirascope V2 response with Text object content.

        Mirascope V2 returns content as a list of Text objects like:
        [Text(type='text', text='Hello!')]
        """

        class MockText:
            type = "text"
            text = "Hello from V2!"

        class MockResponse:
            content = [MockText()]
            model = "claude-sonnet-4-20250514"
            usage = None
            tool_calls = None

        response = DurableResponse.from_mirascope(MockResponse(), "anthropic")
        assert response.content == "Hello from V2!"

    def test_from_mirascope_v2_multiple_text_objects(self) -> None:
        """Test converting response with multiple Text objects."""

        class MockText:
            def __init__(self, text: str):
                self.type = "text"
                self.text = text

        class MockResponse:
            content = [MockText("Hello, "), MockText("world!")]
            model = "claude-sonnet-4-20250514"
            usage = None
            tool_calls = None

        response = DurableResponse.from_mirascope(MockResponse(), "anthropic")
        assert response.content == "Hello, world!"

    def test_from_mirascope_content_as_list_of_strings(self) -> None:
        """Test converting response with content as a list of strings."""

        class MockResponse:
            content = ["Part 1", "Part 2"]
            model = "model"
            usage = None
            tool_calls = None

        response = DurableResponse.from_mirascope(MockResponse(), "provider")
        assert response.content == "Part 1Part 2"

    def test_from_mirascope_content_no_content_attribute(self) -> None:
        """Test converting response without content attribute."""

        class MockResponse:
            model = "model"
            usage = None
            tool_calls = None

            def __str__(self) -> str:
                return "Stringified response"

        response = DurableResponse.from_mirascope(MockResponse(), "provider")
        assert response.content == "Stringified response"

    def test_from_mirascope_v2_model_id(self) -> None:
        """Test extracting model from Mirascope V2 model_id attribute.

        Mirascope V2 returns model as a Model object, but provides
        model_id as a string.
        """

        class MockModel:
            pass

        class MockResponse:
            content = "Test"
            model = MockModel()  # V2: model is an object
            model_id = "anthropic/claude-sonnet-4-20250514"  # V2: model_id is a string
            usage = None
            tool_calls = None

        response = DurableResponse.from_mirascope(MockResponse(), "anthropic")
        assert response.model == "anthropic/claude-sonnet-4-20250514"

    def test_from_mirascope_v2_raw_usage(self) -> None:
        """Test extracting usage from Mirascope V2 response.raw.usage.

        Mirascope V2 stores usage in response.raw.usage instead of
        response.usage.
        """

        class MockRawUsage:
            def model_dump(self) -> dict:
                return {"input_tokens": 10, "output_tokens": 5}

        class MockRaw:
            usage = MockRawUsage()

        class MockResponse:
            content = "Test"
            model_id = "anthropic/claude-sonnet-4-20250514"
            raw = MockRaw()
            tool_calls = None

        response = DurableResponse.from_mirascope(MockResponse(), "anthropic")
        assert response.usage == {"input_tokens": 10, "output_tokens": 5}

    def test_from_mirascope_v2_raw_stop_reason(self) -> None:
        """Test extracting stop_reason from Mirascope V2 response.raw."""

        class MockRaw:
            stop_reason = "end_turn"
            usage = None

        class MockResponse:
            content = "Test"
            model_id = "anthropic/claude-sonnet-4-20250514"
            raw = MockRaw()
            tool_calls = None

        response = DurableResponse.from_mirascope(MockResponse(), "anthropic")
        assert response.stop_reason == "end_turn"

    def test_from_mirascope_v2_full_response(self) -> None:
        """Test converting a complete Mirascope V2 response."""

        class MockText:
            type = "text"
            text = "Hello from V2!"

        class MockRawUsage:
            def model_dump(self) -> dict:
                return {
                    "input_tokens": 15,
                    "output_tokens": 8,
                    "cache_read_input_tokens": 0,
                }

        class MockRaw:
            usage = MockRawUsage()
            stop_reason = "end_turn"

        class MockModel:
            pass

        class MockResponse:
            content = [MockText()]
            model = MockModel()
            model_id = "anthropic/claude-sonnet-4-20250514"
            raw = MockRaw()
            tool_calls = []
            finish_reason = None

        response = DurableResponse.from_mirascope(MockResponse(), "anthropic")
        assert response.content == "Hello from V2!"
        assert response.model == "anthropic/claude-sonnet-4-20250514"
        assert response.provider == "anthropic"
        assert response.usage == {
            "input_tokens": 15,
            "output_tokens": 8,
            "cache_read_input_tokens": 0,
        }
        assert response.stop_reason == "end_turn"
        assert response.tool_calls is None  # Empty list converted to None
