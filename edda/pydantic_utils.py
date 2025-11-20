"""Pydantic integration utilities for Edda framework.

This module provides utilities for detecting Pydantic models and converting
between Pydantic models and JSON-compatible dictionaries.
"""

import types
from enum import Enum
from typing import Any, TypeVar, Union, cast, get_args, get_origin

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


def is_pydantic_model(obj: Any) -> bool:
    """Check if an object is a Pydantic model class.

    Args:
        obj: Object to check

    Returns:
        True if obj is a Pydantic model class, False otherwise

    Examples:
        >>> from pydantic import BaseModel
        >>> class User(BaseModel):
        ...     name: str
        >>> is_pydantic_model(User)
        True
        >>> is_pydantic_model(User(name="Alice"))
        False
        >>> is_pydantic_model(str)
        False
    """
    try:
        return isinstance(obj, type) and issubclass(obj, BaseModel)
    except TypeError:
        # issubclass raises TypeError for non-class objects
        return False


def is_pydantic_instance(obj: Any) -> bool:
    """Check if an object is a Pydantic model instance.

    Args:
        obj: Object to check

    Returns:
        True if obj is a Pydantic model instance, False otherwise

    Examples:
        >>> from pydantic import BaseModel
        >>> class User(BaseModel):
        ...     name: str
        >>> is_pydantic_instance(User(name="Alice"))
        True
        >>> is_pydantic_instance(User)
        False
        >>> is_pydantic_instance("Alice")
        False
    """
    return isinstance(obj, BaseModel)


def to_json_dict(obj: Any) -> Any:
    """Convert a Pydantic model or Enum to a JSON-compatible value.

    Recursively handles lists and dicts containing Pydantic models or Enums.

    Args:
        obj: Object to convert (Pydantic model, Enum, list, dict, or primitive)

    Returns:
        JSON-compatible value (dict for Pydantic, primitive for Enum, or original value)

    Examples:
        >>> from pydantic import BaseModel
        >>> from enum import Enum
        >>> class User(BaseModel):
        ...     name: str
        ...     age: int
        >>> class Status(Enum):
        ...     ACTIVE = "active"
        >>> user = User(name="Alice", age=30)
        >>> to_json_dict(user)
        {'name': 'Alice', 'age': 30}
        >>> to_json_dict(Status.ACTIVE)
        'active'
        >>> to_json_dict([user])
        [{'name': 'Alice', 'age': 30}]
        >>> to_json_dict({'name': 'Bob'})
        {'name': 'Bob'}
    """
    if is_pydantic_instance(obj):
        return obj.model_dump(mode="json")
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, list):
        return [to_json_dict(item) for item in obj]
    if isinstance(obj, dict):
        return {key: to_json_dict(value) for key, value in obj.items()}
    return obj


def from_json_dict(data: dict[str, Any], model: type[T]) -> T:
    """Convert a JSON dictionary to a Pydantic model instance.

    Args:
        data: JSON-compatible dictionary
        model: Pydantic model class

    Returns:
        Pydantic model instance

    Raises:
        ValidationError: If data does not match model schema

    Examples:
        >>> from pydantic import BaseModel
        >>> class User(BaseModel):
        ...     name: str
        ...     age: int
        >>> data = {'name': 'Alice', 'age': 30}
        >>> user = from_json_dict(data, User)
        >>> user.name
        'Alice'
        >>> user.age
        30
    """
    return model.model_validate(data)


def extract_pydantic_model_from_annotation(annotation: Any) -> type[BaseModel] | None:
    """Extract Pydantic model class from a type annotation.

    Handles various annotation patterns:
    - Direct: User
    - Optional: User | None, Optional[User]
    - Generic: list[User], dict[str, User]

    Args:
        annotation: Type annotation to analyze

    Returns:
        Pydantic model class if found, None otherwise

    Examples:
        >>> from pydantic import BaseModel
        >>> from typing import Optional
        >>> class User(BaseModel):
        ...     name: str
        >>> extract_pydantic_model_from_annotation(User)
        <class 'User'>
        >>> extract_pydantic_model_from_annotation(User | None)
        <class 'User'>
        >>> extract_pydantic_model_from_annotation(Optional[User])
        <class 'User'>
        >>> extract_pydantic_model_from_annotation(str)
        None
    """
    # Direct Pydantic model
    if is_pydantic_model(annotation):
        return cast(type[BaseModel], annotation)

    # Handle Union types (e.g., User | None, Optional[User])
    # Only check for Union types, not other generics (list, dict, etc.)
    origin = get_origin(annotation)
    # Check for typing.Union (Optional[X]) and types.UnionType (X | Y in Python 3.10+)
    if origin is Union or origin is types.UnionType:
        args = get_args(annotation)
        for arg in args:
            if is_pydantic_model(arg):
                return cast(type[BaseModel], arg)

    return None


# ============================================================================
# Enum utilities
# ============================================================================


def is_enum_class(obj: Any) -> bool:
    """Check if an object is an Enum class.

    Args:
        obj: Object to check

    Returns:
        True if obj is an Enum class, False otherwise

    Examples:
        >>> from enum import Enum
        >>> class Status(Enum):
        ...     ACTIVE = "active"
        >>> is_enum_class(Status)
        True
        >>> is_enum_class(Status.ACTIVE)
        False
        >>> is_enum_class(str)
        False
    """
    try:
        return isinstance(obj, type) and issubclass(obj, Enum)
    except TypeError:
        # issubclass raises TypeError for non-class objects
        return False


def is_enum_instance(obj: Any) -> bool:
    """Check if an object is an Enum instance.

    Args:
        obj: Object to check

    Returns:
        True if obj is an Enum instance, False otherwise

    Examples:
        >>> from enum import Enum
        >>> class Status(Enum):
        ...     ACTIVE = "active"
        >>> is_enum_instance(Status.ACTIVE)
        True
        >>> is_enum_instance(Status)
        False
        >>> is_enum_instance("active")
        False
    """
    return isinstance(obj, Enum)


def extract_enum_from_annotation(annotation: Any) -> type[Enum] | None:
    """Extract Enum class from a type annotation.

    Handles various annotation patterns:
    - Direct: Status
    - Optional: Status | None, Optional[Status]

    Args:
        annotation: Type annotation to analyze

    Returns:
        Enum class if found, None otherwise

    Examples:
        >>> from enum import Enum
        >>> from typing import Optional
        >>> class Status(Enum):
        ...     ACTIVE = "active"
        >>> extract_enum_from_annotation(Status)
        <class 'Status'>
        >>> extract_enum_from_annotation(Status | None)
        <class 'Status'>
        >>> extract_enum_from_annotation(Optional[Status])
        <class 'Status'>
        >>> extract_enum_from_annotation(str)
        None
    """
    # Direct Enum class
    if is_enum_class(annotation):
        return cast(type[Enum], annotation)

    # Handle Union types (e.g., Status | None, Optional[Status])
    origin = get_origin(annotation)
    # Check for typing.Union (Optional[X]) and types.UnionType (X | Y in Python 3.10+)
    if origin is Union or origin is types.UnionType:
        args = get_args(annotation)
        for arg in args:
            if is_enum_class(arg):
                return cast(type[Enum], arg)

    return None


def enum_value_to_enum(value: Any, enum_class: type[Enum]) -> Enum:
    """Convert a value (str or int) to Enum instance.

    Args:
        value: Value to convert (must match an Enum member's value)
        enum_class: Enum class to convert to

    Returns:
        Enum instance

    Raises:
        ValueError: If value does not match any Enum member

    Examples:
        >>> from enum import Enum
        >>> class Status(Enum):
        ...     ACTIVE = "active"
        ...     INACTIVE = "inactive"
        >>> enum_value_to_enum("active", Status)
        <Status.ACTIVE: 'active'>
        >>> enum_value_to_enum("invalid", Status)
        Traceback (most recent call last):
        ...
        ValueError: Cannot convert 'invalid' to Status
    """
    # Try by value first (most common case)
    for member in enum_class:
        if member.value == value:
            return member

    # Try by name (fallback for string values matching member names)
    if isinstance(value, str):
        # Try exact match first
        if hasattr(enum_class, value):
            return cast(Enum, getattr(enum_class, value))
        # Try uppercase
        if hasattr(enum_class, value.upper()):
            return cast(Enum, getattr(enum_class, value.upper()))

    raise ValueError(f"Cannot convert {value!r} to {enum_class.__name__}")
