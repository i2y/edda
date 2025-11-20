"""
Tests for serialization layer (JSON).
"""

import pytest


class TestJSONSerializer:
    """Tests for JSON serializer."""

    def test_content_type(self, json_serializer):
        """Test content type property."""
        assert json_serializer.content_type == "application/json"

    def test_serialize_dict(self, json_serializer):
        """Test serializing a dictionary."""
        data = {"order_id": "order-123", "amount": 100}
        result = json_serializer.serialize(data)

        assert isinstance(result, bytes)
        assert b"order_id" in result
        assert b"order-123" in result

    def test_serialize_list(self, json_serializer):
        """Test serializing a list."""
        data = [1, 2, 3, "test"]
        result = json_serializer.serialize(data)

        assert isinstance(result, bytes)
        assert b"test" in result

    def test_serialize_nested(self, json_serializer):
        """Test serializing nested data."""
        data = {
            "order": {"id": "123", "items": [{"name": "item1"}, {"name": "item2"}]},
            "total": 100.50,
        }
        result = json_serializer.serialize(data)

        assert isinstance(result, bytes)
        assert b"item1" in result

    def test_deserialize_dict(self, json_serializer):
        """Test deserializing to dictionary."""
        original = {"order_id": "order-123", "amount": 100}
        serialized = json_serializer.serialize(original)
        result = json_serializer.deserialize(serialized)

        assert result == original

    def test_deserialize_list(self, json_serializer):
        """Test deserializing to list."""
        original = [1, 2, 3, "test"]
        serialized = json_serializer.serialize(original)
        result = json_serializer.deserialize(serialized)

        assert result == original

    def test_serialize_invalid_data(self, json_serializer):
        """Test serializing invalid data raises error."""

        class NonSerializable:
            pass

        with pytest.raises(ValueError, match="Failed to serialize"):
            json_serializer.serialize(NonSerializable())

    def test_deserialize_invalid_data(self, json_serializer):
        """Test deserializing invalid JSON raises error."""
        invalid_json = b"{ invalid json }"

        with pytest.raises(ValueError, match="Failed to deserialize"):
            json_serializer.deserialize(invalid_json)

    def test_to_dict_from_dict(self, json_serializer):
        """Test to_dict and from_dict round trip."""
        original = {"key": "value", "number": 42}
        as_dict = json_serializer.to_dict(original)
        result = json_serializer.from_dict(as_dict)

        assert result == original

    def test_to_dict_string(self, json_serializer):
        """Test to_dict with JSON string."""
        json_str = '{"key": "value"}'
        result = json_serializer.to_dict(json_str)

        assert result == {"key": "value"}

    def test_to_dict_non_dict(self, json_serializer):
        """Test to_dict with non-dict value."""
        result = json_serializer.to_dict(42)

        # Should wrap in dict
        assert result == {"value": 42}


class TestSerializationRoundTrip:
    """Tests for complete serialization round trips."""

    def test_json_round_trip_complex_data(self, json_serializer):
        """Test JSON serialization round trip with complex data."""
        original = {
            "workflow_id": "wf-123",
            "status": "running",
            "data": {
                "order": {"id": "order-456", "total": 123.45},
                "items": [
                    {"name": "Item 1", "quantity": 2},
                    {"name": "Item 2", "quantity": 1},
                ],
            },
            "timestamps": {
                "created": "2025-01-01T00:00:00Z",
                "updated": "2025-01-01T01:00:00Z",
            },
        }

        # Serialize and deserialize
        serialized = json_serializer.serialize(original)
        result = json_serializer.deserialize(serialized)

        assert result == original

    def test_json_unicode_handling(self, json_serializer):
        """Test JSON serializer handles Unicode correctly."""
        original = {
            "name": "テスト",  # Japanese
            "emoji": "🎉",
            "chinese": "测试",
        }

        serialized = json_serializer.serialize(original)
        result = json_serializer.deserialize(serialized)

        assert result == original

    def test_json_special_characters(self, json_serializer):
        """Test JSON serializer handles special characters."""
        original = {
            "quote": 'He said "Hello"',
            "backslash": "C:\\Windows\\System32",
            "newline": "Line 1\nLine 2",
            "tab": "Column1\tColumn2",
        }

        serialized = json_serializer.serialize(original)
        result = json_serializer.deserialize(serialized)

        assert result == original
