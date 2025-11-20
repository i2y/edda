"""Tests for Pydantic utilities."""

from datetime import datetime

import pytest
from pydantic import BaseModel, ValidationError

from edda.pydantic_utils import (
    extract_pydantic_model_from_annotation,
    from_json_dict,
    is_pydantic_instance,
    is_pydantic_model,
    to_json_dict,
)


# Test models
class User(BaseModel):
    """User model for testing."""

    name: str
    age: int


class Order(BaseModel):
    """Order model for testing."""

    order_id: str
    user: User
    total: float
    created_at: datetime


class TestIsPydanticModel:
    """Tests for is_pydantic_model function."""

    def test_is_pydantic_model_with_class(self):
        """Test that Pydantic model class is detected."""
        assert is_pydantic_model(User) is True
        assert is_pydantic_model(Order) is True

    def test_is_pydantic_model_with_instance(self):
        """Test that Pydantic model instance is NOT detected as class."""
        user = User(name="Alice", age=30)
        assert is_pydantic_model(user) is False

    def test_is_pydantic_model_with_non_pydantic(self):
        """Test that non-Pydantic types are not detected."""
        assert is_pydantic_model(str) is False
        assert is_pydantic_model(int) is False
        assert is_pydantic_model(dict) is False
        assert is_pydantic_model("string") is False
        assert is_pydantic_model(123) is False


class TestIsPydanticInstance:
    """Tests for is_pydantic_instance function."""

    def test_is_pydantic_instance_with_instance(self):
        """Test that Pydantic model instance is detected."""
        user = User(name="Alice", age=30)
        order = Order(
            order_id="ORD-123",
            user=user,
            total=99.99,
            created_at=datetime(2025, 1, 1, 12, 0, 0),
        )
        assert is_pydantic_instance(user) is True
        assert is_pydantic_instance(order) is True

    def test_is_pydantic_instance_with_class(self):
        """Test that Pydantic model class is NOT detected as instance."""
        assert is_pydantic_instance(User) is False
        assert is_pydantic_instance(Order) is False

    def test_is_pydantic_instance_with_non_pydantic(self):
        """Test that non-Pydantic objects are not detected."""
        assert is_pydantic_instance("string") is False
        assert is_pydantic_instance(123) is False
        assert is_pydantic_instance({"name": "Bob"}) is False
        assert is_pydantic_instance([1, 2, 3]) is False


class TestToJsonDict:
    """Tests for to_json_dict function."""

    def test_to_json_dict_with_simple_pydantic_model(self):
        """Test converting simple Pydantic model to dict."""
        user = User(name="Alice", age=30)
        result = to_json_dict(user)
        assert result == {"name": "Alice", "age": 30}
        assert isinstance(result, dict)

    def test_to_json_dict_with_nested_pydantic_model(self):
        """Test converting nested Pydantic model to dict."""
        user = User(name="Bob", age=25)
        order = Order(
            order_id="ORD-456",
            user=user,
            total=149.99,
            created_at=datetime(2025, 2, 1, 10, 30, 0),
        )
        result = to_json_dict(order)
        assert result["order_id"] == "ORD-456"
        assert result["user"] == {"name": "Bob", "age": 25}
        assert result["total"] == 149.99
        assert result["created_at"] == "2025-02-01T10:30:00"  # JSON-serialized datetime

    def test_to_json_dict_with_dict(self):
        """Test that dict is recursively processed."""
        data = {"name": "Charlie", "age": 40}
        result = to_json_dict(data)
        assert result == data
        # Note: result is a new dict due to recursive processing

    def test_to_json_dict_with_list_of_pydantic_models(self):
        """Test that list of Pydantic models is converted."""
        user1 = User(name="Alice", age=30)
        user2 = User(name="Bob", age=25)
        result = to_json_dict([user1, user2])
        assert result == [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

    def test_to_json_dict_with_dict_containing_pydantic_models(self):
        """Test that dict containing Pydantic models is recursively converted."""
        user = User(name="Charlie", age=35)
        data = {"user": user, "count": 5}
        result = to_json_dict(data)
        assert result == {"user": {"name": "Charlie", "age": 35}, "count": 5}

    def test_to_json_dict_with_non_dict_non_pydantic(self):
        """Test that non-dict, non-Pydantic objects are returned as-is."""
        assert to_json_dict("string") == "string"
        assert to_json_dict(123) == 123
        assert to_json_dict([1, 2, 3]) == [1, 2, 3]


class TestFromJsonDict:
    """Tests for from_json_dict function."""

    def test_from_json_dict_simple(self):
        """Test converting dict to simple Pydantic model."""
        data = {"name": "Alice", "age": 30}
        user = from_json_dict(data, User)
        assert isinstance(user, User)
        assert user.name == "Alice"
        assert user.age == 30

    def test_from_json_dict_nested(self):
        """Test converting dict to nested Pydantic model."""
        data = {
            "order_id": "ORD-789",
            "user": {"name": "Dave", "age": 35},
            "total": 199.99,
            "created_at": "2025-03-01T15:45:00",
        }
        order = from_json_dict(data, Order)
        assert isinstance(order, Order)
        assert order.order_id == "ORD-789"
        assert isinstance(order.user, User)
        assert order.user.name == "Dave"
        assert order.user.age == 35
        assert order.total == 199.99
        assert order.created_at == datetime(2025, 3, 1, 15, 45, 0)

    def test_from_json_dict_with_validation_error(self):
        """Test that ValidationError is raised for invalid data."""
        data = {"name": "Eve"}  # Missing required 'age' field
        with pytest.raises(ValidationError) as exc_info:
            from_json_dict(data, User)
        assert "age" in str(exc_info.value)

    def test_from_json_dict_with_type_error(self):
        """Test that ValidationError is raised for wrong type."""
        data = {"name": "Frank", "age": "thirty"}  # age should be int
        with pytest.raises(ValidationError) as exc_info:
            from_json_dict(data, User)
        assert "age" in str(exc_info.value)


class TestExtractPydanticModelFromAnnotation:
    """Tests for extract_pydantic_model_from_annotation function."""

    def test_extract_pydantic_model_direct(self):
        """Test extracting direct Pydantic model annotation."""
        result = extract_pydantic_model_from_annotation(User)
        assert result is User

        result = extract_pydantic_model_from_annotation(Order)
        assert result is Order

    def test_extract_pydantic_model_optional(self):
        """Test extracting Pydantic model from Optional annotation."""
        result = extract_pydantic_model_from_annotation(User | None)
        assert result is User

    def test_extract_pydantic_model_union(self):
        """Test extracting Pydantic model from Union annotation (Python 3.10+)."""
        result = extract_pydantic_model_from_annotation(User | None)
        assert result is User

    def test_extract_pydantic_model_from_non_pydantic(self):
        """Test that None is returned for non-Pydantic annotations."""
        assert extract_pydantic_model_from_annotation(str) is None
        assert extract_pydantic_model_from_annotation(int) is None
        assert extract_pydantic_model_from_annotation(dict) is None
        assert extract_pydantic_model_from_annotation(list[User]) is None  # Generic

    def test_extract_pydantic_model_from_complex_union(self):
        """Test extracting Pydantic model from complex Union."""
        result = extract_pydantic_model_from_annotation(User | str | None)
        assert result is User


class TestRoundTrip:
    """Tests for round-trip conversion (Pydantic → dict → Pydantic)."""

    def test_roundtrip_simple_model(self):
        """Test round-trip conversion for simple model."""
        original = User(name="Grace", age=28)
        data = to_json_dict(original)
        restored = from_json_dict(data, User)
        assert restored == original

    def test_roundtrip_nested_model(self):
        """Test round-trip conversion for nested model."""
        original = Order(
            order_id="ORD-999",
            user=User(name="Henry", age=42),
            total=299.99,
            created_at=datetime(2025, 4, 1, 8, 0, 0),
        )
        data = to_json_dict(original)
        restored = from_json_dict(data, Order)
        assert restored == original
