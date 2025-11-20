"""Tests for workflow parameter extraction from type hints."""

import sys
from pathlib import Path

# Add kairo to path for direct import
sys.path.insert(0, str(Path(__file__).parent.parent))

from edda import WorkflowContext

# Direct import to avoid NiceGUI dependency
from edda.viewer_ui import data_service
from edda.workflow import workflow

WorkflowDataService = data_service.WorkflowDataService


class TestSagaParameterExtraction:
    """Test parameter extraction from workflow function signatures."""

    def test_extract_basic_types(self) -> None:
        """Test extraction of basic parameter types (int, str, float, bool)."""

        @workflow
        async def test_saga(
            ctx: WorkflowContext,
            user_id: int,
            name: str,
            amount: float,
            is_active: bool,
        ) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        assert len(params_info) == 4
        assert params_info["user_id"] == {
            "name": "user_id",
            "type": "int",
            "required": True,
            "default": None,
        }
        assert params_info["name"] == {
            "name": "name",
            "type": "str",
            "required": True,
            "default": None,
        }
        assert params_info["amount"] == {
            "name": "amount",
            "type": "float",
            "required": True,
            "default": None,
        }
        assert params_info["is_active"] == {
            "name": "is_active",
            "type": "bool",
            "required": True,
            "default": None,
        }

    def test_extract_with_defaults(self) -> None:
        """Test extraction with default values."""

        @workflow
        async def test_saga(
            ctx: WorkflowContext,
            user_id: int,
            name: str = "John",
            amount: float = 100.0,
            is_active: bool = False,
        ) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        assert len(params_info) == 4
        assert params_info["user_id"]["required"] is True
        assert params_info["user_id"]["default"] is None
        assert params_info["name"]["required"] is False
        assert params_info["name"]["default"] == "John"
        assert params_info["amount"]["required"] is False
        assert params_info["amount"]["default"] == 100.0
        assert params_info["is_active"]["required"] is False
        assert params_info["is_active"]["default"] is False

    def test_extract_complex_types(self) -> None:
        """Test extraction of complex types (list, dict) - should fallback to 'json'."""

        @workflow
        async def test_saga(
            ctx: WorkflowContext,
            items: list,
            metadata: dict,
        ) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        assert len(params_info) == 2
        assert params_info["items"]["type"] == "json"
        assert params_info["metadata"]["type"] == "json"

    def test_extract_optional_types(self) -> None:
        """Test extraction of Optional types."""

        @workflow
        async def test_saga(
            ctx: WorkflowContext,
            user_id: int,
            name: str | None = None,
        ) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        assert len(params_info) == 2
        # Optional[str] should be detected as 'str' type
        assert params_info["name"]["type"] == "str"
        assert params_info["name"]["required"] is False
        assert params_info["name"]["default"] is None

    def test_extract_union_types(self) -> None:
        """Test extraction of Union types (Python 3.10+ style)."""

        @workflow
        async def test_saga(
            ctx: WorkflowContext,
            reason: str | None = None,
        ) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        assert len(params_info) == 1
        # str | None should be detected as 'str' type
        assert params_info["reason"]["type"] == "str"
        assert params_info["reason"]["required"] is False
        assert params_info["reason"]["default"] is None

    def test_skip_workflow_context(self) -> None:
        """Test that WorkflowContext parameter is skipped."""

        @workflow
        async def test_saga(ctx: WorkflowContext, user_id: int) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        # Only user_id should be extracted, ctx should be skipped
        assert len(params_info) == 1
        assert "ctx" not in params_info
        assert "user_id" in params_info

    def test_extract_no_parameters(self) -> None:
        """Test workflow with no parameters (only WorkflowContext)."""

        @workflow
        async def test_saga(ctx: WorkflowContext) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        # No parameters should be extracted
        assert len(params_info) == 0

    def test_nonexistent_workflow(self) -> None:
        """Test extraction for nonexistent saga."""
        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("nonexistent_workflow")

        # Should return empty dict
        assert params_info == {}

    def test_extract_enum_type(self) -> None:
        """Test extraction of Enum types."""
        from enum import Enum

        class OrderStatus(Enum):
            PENDING = "pending"
            APPROVED = "approved"
            REJECTED = "rejected"

        @workflow
        async def test_saga(ctx: WorkflowContext, status: OrderStatus) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        assert len(params_info) == 1
        assert params_info["status"]["type"] == "enum"
        assert params_info["status"]["enum_class"] == OrderStatus
        assert len(params_info["status"]["enum_values"]) == 3
        assert ("PENDING", "pending") in params_info["status"]["enum_values"]
        assert ("APPROVED", "approved") in params_info["status"]["enum_values"]
        assert ("REJECTED", "rejected") in params_info["status"]["enum_values"]

    def test_extract_list_basic_types(self) -> None:
        """Test extraction of list[basic_type]."""

        @workflow
        async def test_saga(
            ctx: WorkflowContext,
            names: list[str],
            quantities: list[int],
            prices: list[float],
        ) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        assert len(params_info) == 3
        assert params_info["names"]["type"] == "list"
        assert params_info["names"]["item_type"] == "str"
        assert params_info["quantities"]["type"] == "list"
        assert params_info["quantities"]["item_type"] == "int"
        assert params_info["prices"]["type"] == "list"
        assert params_info["prices"]["item_type"] == "float"

    def test_extract_list_dict(self) -> None:
        """Test extraction of list[dict]."""
        from typing import Any

        @workflow
        async def test_saga(ctx: WorkflowContext, items: list[dict[str, Any]]) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        assert len(params_info) == 1
        assert params_info["items"]["type"] == "list"
        assert params_info["items"]["item_type"] == "dict"

    def test_extract_dict_types(self) -> None:
        """Test extraction of dict[K, V]."""

        @workflow
        async def test_saga(
            ctx: WorkflowContext,
            metadata: dict[str, str],
            counts: dict[str, int],
            scores: dict[str, float],
        ) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        assert len(params_info) == 3
        assert params_info["metadata"]["type"] == "dict"
        assert params_info["metadata"]["key_type"] == "str"
        assert params_info["metadata"]["value_type"] == "str"
        assert params_info["counts"]["type"] == "dict"
        assert params_info["counts"]["key_type"] == "str"
        assert params_info["counts"]["value_type"] == "int"
        assert params_info["scores"]["type"] == "dict"
        assert params_info["scores"]["key_type"] == "str"
        assert params_info["scores"]["value_type"] == "float"

    def test_extract_optional_enum(self) -> None:
        """Test extraction of Optional[Enum]."""
        from enum import Enum

        class Priority(Enum):
            LOW = 1
            MEDIUM = 2
            HIGH = 3

        @workflow
        async def test_saga(
            ctx: WorkflowContext,
            priority: Priority | None = None,
        ) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("test_saga")

        assert len(params_info) == 1
        assert params_info["priority"]["type"] == "enum"
        assert params_info["priority"]["required"] is False
        assert params_info["priority"]["enum_class"] == Priority

    def test_extract_complex_workflow(self) -> None:
        """Test extraction of complex saga with multiple advanced types."""
        from enum import Enum
        from typing import Any

        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        @workflow
        async def complex_workflow(
            ctx: WorkflowContext,
            order_id: str,
            status: Status,
            items: list[dict[str, Any]],
            metadata: dict[str, str],
            quantities: list[int],
            priority: Status | None = None,
        ) -> dict:
            pass

        service = WorkflowDataService(storage=None)  # type: ignore
        params_info = service.get_workflow_parameters("complex_workflow")

        assert len(params_info) == 6
        assert params_info["order_id"]["type"] == "str"
        assert params_info["status"]["type"] == "enum"
        assert params_info["items"]["type"] == "list"
        assert params_info["items"]["item_type"] == "dict"
        assert params_info["metadata"]["type"] == "dict"
        assert params_info["quantities"]["type"] == "list"
        assert params_info["quantities"]["item_type"] == "int"
        assert params_info["priority"]["type"] == "enum"
        assert params_info["priority"]["required"] is False
