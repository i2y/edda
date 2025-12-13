"""
Data service for retrieving workflow instance data from storage.
"""

import inspect
import json
import logging
from datetime import datetime
from typing import Any, cast

from edda.pydantic_utils import is_pydantic_model
from edda.storage.protocol import StorageProtocol
from edda.workflow import get_all_workflows

logger = logging.getLogger(__name__)


class WorkflowDataService:
    """Service for retrieving workflow instance data using StorageProtocol."""

    def __init__(self, storage: StorageProtocol):
        """
        Initialize data service.

        Args:
            storage: Storage instance implementing StorageProtocol
        """
        self.storage = storage

    async def get_all_instances(self, limit: int = 50) -> list[dict[str, Any]]:
        """
        Get all workflow instances.

        Args:
            limit: Maximum number of instances to return

        Returns:
            List of workflow instance dictionaries
        """
        result = await self.storage.list_instances(limit=limit)
        return cast(list[dict[str, Any]], result["instances"])

    async def get_instances_paginated(
        self,
        page_size: int = 20,
        page_token: str | None = None,
        status_filter: str | None = None,
        search_query: str | None = None,
        started_after: datetime | None = None,
        started_before: datetime | None = None,
        input_filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Get workflow instances with cursor-based pagination and filtering.

        Args:
            page_size: Number of instances per page (10, 20, or 50)
            page_token: Cursor for pagination (from previous response)
            status_filter: Filter by status (e.g., "running", "completed", "failed")
            search_query: Search by workflow name or instance ID (partial match)
            started_after: Filter instances started after this datetime
            started_before: Filter instances started before this datetime
            input_filters: Filter by input data values. Keys are JSON paths
                (e.g., "order_id"), values are expected values (exact match).

        Returns:
            Dictionary containing:
            - instances: List of workflow instances
            - next_page_token: Cursor for next page, or None
            - has_more: Boolean indicating if more pages exist
        """
        # Use search_query for both workflow_name_filter and instance_id_filter
        result = await self.storage.list_instances(
            limit=page_size,
            page_token=page_token,
            status_filter=status_filter,
            workflow_name_filter=search_query,
            instance_id_filter=search_query,
            started_after=started_after,
            started_before=started_before,
            input_filters=input_filters,
        )
        return result

    async def get_workflow_compensations(self, instance_id: str) -> dict[str, dict[str, Any]]:
        """
        Get registered compensations for a workflow instance.

        Args:
            instance_id: Workflow instance ID

        Returns:
            Dictionary mapping activity_id to compensation info:
            {activity_id: {"activity_name": str, "args": dict}}
        """
        compensations_list = await self.storage.get_compensations(instance_id)

        # Create a mapping of activity_id -> compensation info for quick lookup
        compensations_map: dict[str, dict[str, Any]] = {}
        for comp in compensations_list:
            activity_id = comp.get("activity_id")
            if activity_id is not None:
                compensations_map[activity_id] = {
                    "activity_name": comp.get("activity_name"),
                    "args": comp.get("args", {}),
                }

        return compensations_map

    async def get_instance_detail(self, instance_id: str) -> dict[str, Any]:
        """
        Get detailed information about a workflow instance.

        Args:
            instance_id: Workflow instance ID

        Returns:
            Dictionary containing instance, history, and compensation data
        """
        # Get instance basic info using StorageProtocol
        instance = await self.storage.get_instance(instance_id)

        if not instance:
            return {"instance": None, "history": []}

        # Get execution history using StorageProtocol
        history_rows = await self.storage.get_history(instance_id)

        # Transform workflow_history format to viewer format
        history = []
        workflow_name = instance.get("workflow_name", "unknown")

        for row in history_rows:
            # event_data is already parsed as dict by StorageProtocol
            event_data = row.get("event_data", {})

            # Determine status from event_type
            event_type = row.get("event_type", "")
            if event_type == "ActivityCompleted":
                status = "completed"
            elif event_type == "ActivityFailed":
                status = "failed"
            elif event_type == "CompensationExecuted":
                status = "compensated"
            elif event_type == "CompensationFailed":
                status = "compensation_failed"
            elif event_type == "EventReceived":
                status = "event_received"
            else:
                status = "running"

            # Prepare activity name with prefix for compensation
            activity_id = row.get("activity_id")
            activity_name = event_data.get("activity_name", activity_id or "unknown")
            if event_type in ("CompensationExecuted", "CompensationFailed"):
                activity_name = f"Compensate: {activity_name}"

            history.append(
                {
                    "activity_id": activity_id,
                    "workflow_name": workflow_name,
                    "activity_name": activity_name,
                    "status": status,
                    "input_data": json.dumps(event_data.get("input", event_data.get("kwargs", {}))),
                    "output_data": json.dumps(event_data.get("result")),
                    "executed_at": row.get("created_at"),
                    "error": event_data.get("error_message"),  # Fixed: use correct field name
                    "error_type": event_data.get("error_type"),
                    "stack_trace": event_data.get("stack_trace"),
                }
            )

        # Get compensation information
        compensations = await self.get_workflow_compensations(instance_id)

        return {
            "instance": instance,
            "history": history,
            "compensations": compensations,
        }

    async def get_activity_detail(
        self, instance_id: str, activity_id: str
    ) -> dict[str, Any] | None:
        """
        Get detailed information about a specific activity execution.

        Args:
            instance_id: Workflow instance ID
            activity_id: Activity ID

        Returns:
            Activity detail dictionary or None if not found
        """
        # Get full history and find the specific activity
        history = await self.storage.get_history(instance_id)

        # Find the activity in history
        activity_row = None
        for row in history:
            if row.get("activity_id") == activity_id:
                activity_row = row
                break

        if not activity_row:
            return None

        # event_data is already parsed as dict by StorageProtocol
        event_data = activity_row.get("event_data", {})

        # Determine status from event_type
        event_type = activity_row.get("event_type", "")
        if event_type == "ActivityCompleted":
            status = "completed"
        elif event_type == "ActivityFailed":
            status = "failed"
        elif event_type == "CompensationExecuted":
            status = "compensated"
        elif event_type == "CompensationFailed":
            status = "compensation_failed"
        else:
            status = "running"

        # Parse input and output
        # For compensations, input is in 'kwargs', for activities it's in 'input'
        if event_type in ("CompensationExecuted", "CompensationFailed"):
            input_data = event_data.get("kwargs", {})
            output_data = None  # Compensations don't record output
        else:
            input_data = event_data.get("input", {})
            output_data = event_data.get("result")

        return {
            "activity_id": activity_id,
            "activity_name": event_data.get("activity_name", activity_id or "unknown"),
            "status": status,
            "input": input_data,
            "output": output_data,
            "executed_at": activity_row.get("created_at"),
            "error": event_data.get(
                "error_message"
            ),  # Fixed: was "error", should be "error_message"
            "error_type": event_data.get("error_type"),
            "stack_trace": event_data.get("stack_trace"),
        }

    def get_workflow_source(self, workflow_name: str) -> str | None:
        """
        Get source code for a workflow by name.

        Args:
            workflow_name: Name of the workflow

        Returns:
            Source code as string, or None if not found or error occurred
        """
        try:
            # Get all registered workflows
            all_workflows = get_all_workflows()

            # Find the workflow by name
            if workflow_name not in all_workflows:
                return None

            workflow = all_workflows[workflow_name]

            # Get source code from the workflow function
            source_code = inspect.getsource(workflow.func)
            return source_code

        except (OSError, TypeError) as e:
            # OSError: source not available (e.g., interactive shell)
            # TypeError: not a module, class, method, function, etc.
            logger.warning("Could not get source for %s: %s", workflow_name, e)
            return None

    async def get_activity_executions(
        self, instance_id: str, activity_name: str
    ) -> list[dict[str, Any]]:
        """
        Get all executions of a specific activity (for activities executed multiple times).

        Args:
            instance_id: Workflow instance ID
            activity_name: Activity name

        Returns:
            List of execution details, ordered by execution time
        """
        # Get full history
        history = await self.storage.get_history(instance_id)

        executions = []
        for row in history:
            event_data = row.get("event_data", {})
            if event_data.get("activity_name") == activity_name:
                # Determine status from event_type
                event_type = row.get("event_type", "")
                if event_type == "ActivityCompleted":
                    status = "completed"
                elif event_type == "ActivityFailed":
                    status = "failed"
                else:
                    status = "running"

                executions.append(
                    {
                        "activity_id": row.get("activity_id"),
                        "activity_name": activity_name,
                        "status": status,
                        "input": event_data.get("input", {}),
                        "output": event_data.get("result"),
                        "executed_at": row.get("created_at"),
                        "error": event_data.get(
                            "error_message"
                        ),  # Fixed: was "error", should be "error_message"
                        "error_type": event_data.get("error_type"),
                        "stack_trace": event_data.get("stack_trace"),
                    }
                )

        # Already sorted by execution time (created_at) via get_history()
        return executions

    async def cancel_workflow(self, instance_id: str, edda_app_url: str) -> tuple[bool, str]:
        """
        Cancel a workflow via EddaApp API.

        Args:
            instance_id: Workflow instance ID
            edda_app_url: EddaApp API base URL (e.g., "http://localhost:8001")

        Returns:
            Tuple of (success: bool, message: str)
        """
        import httpx

        try:
            logger.debug("Attempting to cancel workflow: %s", instance_id)
            logger.debug("API URL: %s/cancel/%s", edda_app_url, instance_id)

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{edda_app_url}/cancel/{instance_id}",
                    timeout=10.0,
                )

                logger.debug("Response status: %s", response.status_code)
                logger.debug("Response body: %s", response.text)

                if 200 <= response.status_code < 300:
                    return True, "Workflow cancelled successfully"
                elif response.status_code == 400:
                    error_msg = response.json().get("error", "Unknown error")
                    return False, f"Cannot cancel: {error_msg}"
                elif response.status_code == 404:
                    return False, "Workflow not found"
                else:
                    return False, f"Server error: HTTP {response.status_code}"

        except httpx.ConnectError as e:
            error_msg = f"Cannot connect to EddaApp at {edda_app_url}. Is it running?"
            logger.warning("Connection error: %s", e)
            return False, error_msg

        except httpx.TimeoutException as e:
            error_msg = "Request timed out. The server may be busy."
            logger.warning("Timeout error: %s", e)
            return False, error_msg

        except Exception as e:
            error_msg = f"Unexpected error: {type(e).__name__}: {str(e)}"
            logger.error("Unexpected error: %s", e, exc_info=True)
            return False, error_msg

    def get_all_workflows(self) -> dict[str, Any]:
        """
        Get all registered workflows.

        Returns:
            Dictionary mapping workflow name to workflow instance
        """
        from edda.workflow import get_all_workflows

        return get_all_workflows()

    def get_workflow_parameters(self, workflow_name: str) -> dict[str, Any]:
        """
        Extract parameter information from a workflow's function signature.

        Args:
            workflow_name: Name of the workflow

        Returns:
            Dictionary mapping parameter name to parameter info:
            {
                "param_name": {
                    "name": str,
                    "type": str,  # "int", "str", "float", "bool", "enum", "list", "dict", "json"
                    "required": bool,
                    "default": Any | None,
                    # Enum specific
                    "enum_class": Type[Enum] | None,
                    "enum_values": list[tuple[str, Any]],
                    # list specific
                    "item_type": str | None,  # "str", "int", "enum", "dict", etc.
                    "item_enum_class": Type[Enum] | None,
                    # dict specific
                    "key_type": str | None,
                    "value_type": str | None,
                }
            }
        """
        from typing import get_args, get_origin

        all_workflows = self.get_all_workflows()
        if workflow_name not in all_workflows:
            return {}

        workflow = all_workflows[workflow_name]
        sig = inspect.signature(workflow.func)

        params_info = {}
        for param_name, param in sig.parameters.items():
            # Skip WorkflowContext parameter (usually the first parameter)
            if (
                param.annotation.__name__ == "WorkflowContext"
                if hasattr(param.annotation, "__name__")
                else False
            ):
                continue

            if param.annotation == inspect.Parameter.empty:
                # No annotation, fallback to JSON
                params_info[param_name] = {
                    "name": param_name,
                    "type": "json",
                    "required": param.default == inspect.Parameter.empty,
                    "default": param.default if param.default != inspect.Parameter.empty else None,
                }
                continue

            # Get the type annotation
            annotation = param.annotation

            # Handle typing.Optional[T] and Union[T, None] → extract T
            annotation_type_str = str(type(annotation))
            origin = get_origin(annotation)
            origin_str = str(origin) if origin else ""

            # Python 3.10+ uses types.UnionType (no __origin__ attribute)
            # typing.Union uses __origin__ = typing.Union
            is_union = (
                "UnionType" in annotation_type_str or "Union" in origin_str or origin is type(None)
            )

            if is_union:
                # Optional[T] is Union[T, None]
                args = get_args(annotation)
                if args:
                    # Get the first non-None type
                    annotation = next((arg for arg in args if arg is not type(None)), annotation)
                    origin = get_origin(annotation)

            # Extract type information
            param_info = self._extract_type_info(annotation, origin)
            param_info["name"] = param_name
            param_info["required"] = param.default == inspect.Parameter.empty
            param_info["default"] = (
                param.default if param.default != inspect.Parameter.empty else None
            )

            params_info[param_name] = param_info

        # If there's only one parameter and it's a Pydantic model,
        # expand its fields to provide individual form inputs instead of JSON textarea
        if len(params_info) == 1:
            single_param_name, single_param_info = next(iter(params_info.items()))
            if single_param_info.get("type") == "pydantic":
                # Try to expand Pydantic model fields
                expanded_params = self._expand_pydantic_fields(
                    single_param_info["json_schema"],
                    single_param_info.get("required", True),
                    single_param_info.get("model_class"),
                )
                # If expansion succeeded (has fields), use expanded params
                # and store original model info for reconstruction
                if expanded_params:
                    # Mark all expanded params with original model name for reconstruction
                    for field_info in expanded_params.values():
                        field_info["_pydantic_model_name"] = single_param_name
                        field_info["_pydantic_model_class"] = single_param_info.get("model_class")
                    return expanded_params

        return params_info

    def _extract_type_info(self, annotation: Any, origin: Any = None) -> dict[str, Any]:
        """
        Extract detailed type information from annotation.

        Args:
            annotation: Type annotation
            origin: Result of typing.get_origin(annotation)

        Returns:
            Dictionary with type information
        """
        from enum import Enum
        from typing import get_args, get_origin

        if origin is None:
            origin = get_origin(annotation)

        # Check if it's a Pydantic model
        if is_pydantic_model(annotation):
            try:
                # Generate JSON Schema from Pydantic model
                schema = annotation.model_json_schema()
                return {
                    "type": "pydantic",
                    "model_class": annotation,
                    "model_name": annotation.__name__,
                    "json_schema": schema,
                }
            except Exception as e:
                # Fallback to JSON if schema generation fails
                logger.warning("Failed to generate JSON Schema for %s: %s", annotation, e)
                return {"type": "json"}

        # Check if it's an Enum
        if inspect.isclass(annotation) and issubclass(annotation, Enum):
            return {
                "type": "enum",
                "enum_class": annotation,
                "enum_values": [(member.name, member.value) for member in annotation],
            }

        # Check if it's a list
        if origin is list:
            args = get_args(annotation)
            if args:
                item_type_info = self._get_simple_type_name(args[0])
                return {
                    "type": "list",
                    "item_type": item_type_info["type"],
                    "item_enum_class": item_type_info.get("enum_class"),
                    "item_enum_values": item_type_info.get("enum_values"),
                }
            else:
                # list without type parameter, fallback to JSON
                return {"type": "json"}

        # Check if it's a dict
        if origin is dict:
            args = get_args(annotation)
            if args and len(args) >= 2:
                key_type_info = self._get_simple_type_name(args[0])
                value_type_info = self._get_simple_type_name(args[1])
                return {
                    "type": "dict",
                    "key_type": key_type_info["type"],
                    "value_type": value_type_info["type"],
                }
            else:
                # dict without type parameters, fallback to JSON
                return {"type": "json"}

        # Basic types
        type_str = getattr(annotation, "__name__", str(annotation))
        if type_str in ("int", "str", "float", "bool"):
            return {"type": type_str}

        # Fallback to JSON for unknown types
        return {"type": "json"}

    def _get_simple_type_name(self, annotation: Any) -> dict[str, Any]:
        """
        Get simple type name for list/dict item types.

        Args:
            annotation: Type annotation

        Returns:
            Dictionary with type info
        """
        from enum import Enum
        from typing import get_origin

        origin = get_origin(annotation)

        # Check if it's a Pydantic model
        if is_pydantic_model(annotation):
            return {
                "type": "pydantic",
                "model_name": annotation.__name__,
            }

        # Check if it's an Enum
        if inspect.isclass(annotation) and issubclass(annotation, Enum):
            return {
                "type": "enum",
                "enum_class": annotation,
                "enum_values": [(member.name, member.value) for member in annotation],
            }

        # Check if it's dict (for list[dict])
        if origin is dict:
            return {"type": "dict"}

        # Basic types
        type_str = getattr(annotation, "__name__", str(annotation))
        if type_str in ("int", "str", "float", "bool"):
            return {"type": type_str}

        # Fallback to JSON
        return {"type": "json"}

    def _resolve_ref(self, schema: dict[str, Any], ref_path: str) -> dict[str, Any]:
        """
        Resolve JSON Schema $ref reference.

        Pydantic v2 uses $defs for nested models:
        {"$ref": "#/$defs/ShippingAddress"}

        Args:
            schema: Full JSON Schema containing $defs
            ref_path: Reference path like "#/$defs/ShippingAddress"

        Returns:
            Resolved schema definition
        """
        if not ref_path.startswith("#/"):
            # Only local refs supported
            logger.warning(f"Unsupported $ref: {ref_path} (only local refs supported)")
            return {}

        # Remove "#/" and split by "/"
        parts = ref_path[2:].split("/")
        current = schema
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part, {})
            else:
                logger.warning(f"Cannot resolve $ref part '{part}' in {ref_path}")
                return {}

        return current if isinstance(current, dict) else {}

    def _expand_pydantic_fields(
        self,
        json_schema: dict[str, Any],
        _is_required: bool,
        model_class: Any = None,
        _parent_field_name: str | None = None,
        nested_level: int = 0,
        root_schema: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Expand Pydantic model fields from JSON Schema to individual parameters.

        Args:
            json_schema: Pydantic model's JSON Schema (can be nested schema)
            is_required: Whether the original parameter was required
            model_class: The Pydantic model class (for Enum extraction)
            parent_field_name: Parent field name for nested models
            nested_level: Nesting depth (0 = root, 1 = nested, 2+ = fallback to JSON)
            root_schema: Top-level schema for $ref resolution (same as json_schema at root)

        Returns:
            Dictionary mapping field name to field info, or empty dict if no flat fields
        """
        from enum import Enum

        # Set root_schema at the beginning if not provided (root level)
        if root_schema is None:
            root_schema = json_schema

        expanded = {}

        # Extract properties from JSON Schema
        properties = json_schema.get("properties", {})
        required_fields = set(json_schema.get("required", []))

        for field_name, field_schema in properties.items():
            field_type = field_schema.get("type")
            field_schema.get("format")

            # Determine parameter type
            param_info: dict[str, Any] = {
                "name": field_name,
                "required": field_name in required_fields,
                "default": field_schema.get("default"),
            }

            # First, try to get Enum from model_class annotations (most reliable)
            enum_class = None
            if model_class and hasattr(model_class, "__annotations__"):
                field_annotation = model_class.__annotations__.get(field_name)
                if (
                    field_annotation
                    and inspect.isclass(field_annotation)
                    and issubclass(field_annotation, Enum)
                ):
                    enum_class = field_annotation

            # Check for Enum (JSON Schema: {"enum": [...]})
            if enum_class or "enum" in field_schema:
                if enum_class:
                    param_info["type"] = "enum"
                    param_info["enum_class"] = enum_class
                    param_info["enum_values"] = [
                        (member.name, member.value) for member in enum_class
                    ]
                elif "enum" in field_schema:
                    # Fallback: treat as str with choices
                    param_info["type"] = "str"
                    param_info["enum_values"] = field_schema["enum"]

            # Basic types
            elif field_type == "string":
                param_info["type"] = "str"
            elif field_type == "integer":
                param_info["type"] = "int"
            elif field_type == "number":
                param_info["type"] = "float"
            elif field_type == "boolean":
                param_info["type"] = "bool"

            # Nested models - expand if 1 level deep, otherwise JSON textarea
            elif (
                field_type == "object"
                or "anyOf" in field_schema
                or "allOf" in field_schema
                or "$ref" in field_schema
            ):
                # Nested Pydantic model (e.g., ShippingAddress)
                # Can be represented as {"type": "object"} or {"$ref": "#/$defs/..."} or {"allOf": [...]}

                # Resolve $ref if present (use root_schema for $defs lookup)
                nested_schema = field_schema
                if "$ref" in field_schema:
                    resolved = self._resolve_ref(root_schema, field_schema["$ref"])
                    if resolved:
                        nested_schema = resolved
                    else:
                        # Failed to resolve - fallback to JSON textarea
                        param_info["type"] = "json"
                        param_info["json_schema"] = field_schema
                        param_info["description"] = field_schema.get(
                            "description", f"JSON object for {field_name}"
                        )
                        expanded[field_name] = param_info
                        continue

                # Check nesting level: 0 = root, 1 = nested (expand), 2+ = too deep (JSON textarea)
                if nested_level >= 1:
                    # Too deep (2+ levels) - fallback to JSON textarea
                    param_info["type"] = "json"
                    param_info["json_schema"] = field_schema
                    param_info["description"] = field_schema.get(
                        "description", f"JSON object for {field_name} (nested)"
                    )
                else:
                    # Level 0 → 1: Recursively expand nested model fields
                    nested_expanded = self._expand_pydantic_fields(
                        json_schema=nested_schema,
                        _is_required=field_name in required_fields,
                        model_class=None,  # No model_class for nested (can't extract Enum reliably)
                        _parent_field_name=field_name,
                        nested_level=nested_level + 1,
                        root_schema=root_schema,  # Pass root schema for $ref resolution
                    )

                    if nested_expanded:
                        # Successfully expanded - add all nested fields with metadata
                        for nested_field_name, nested_field_info in nested_expanded.items():
                            # Mark with parent field name and nested level for reconstruction
                            nested_field_info["_parent_field"] = field_name
                            nested_field_info["_nested_level"] = nested_level + 1
                            # Add nested field with qualified name (parent.child)
                            qualified_name = f"{field_name}.{nested_field_name}"
                            expanded[qualified_name] = nested_field_info
                        # Skip adding the parent field itself (it's been expanded)
                        continue
                    else:
                        # No fields expanded - fallback to JSON textarea
                        param_info["type"] = "json"
                        param_info["json_schema"] = field_schema
                        param_info["description"] = field_schema.get(
                            "description", f"JSON object for {field_name}"
                        )
            elif field_type == "array":
                items_schema = field_schema.get("items", {})

                # Resolve $ref if present
                if "$ref" in items_schema:
                    resolved_items_schema = self._resolve_ref(root_schema, items_schema["$ref"])
                    if resolved_items_schema:
                        items_schema = resolved_items_schema

                # Check if items is a Pydantic model (object with properties)
                if items_schema.get("type") == "object" and "properties" in items_schema:
                    # list[PydanticModel] - expand as dynamic list
                    param_info["type"] = "list_of_pydantic"
                    param_info["json_schema"] = field_schema
                    param_info["item_schema"] = items_schema

                    # Recursively expand nested model fields
                    item_fields = self._expand_pydantic_fields(
                        json_schema=items_schema,
                        _is_required=True,  # Items in list are required
                        model_class=None,
                        _parent_field_name=None,
                        nested_level=0,  # Reset nesting level for list items
                        root_schema=root_schema,
                    )
                    param_info["item_fields"] = item_fields
                    param_info["description"] = field_schema.get(
                        "description", f"Dynamic list of {items_schema.get('title', 'items')}"
                    )
                else:
                    # list[str], list[int], list[dict], etc. - fallback to JSON textarea
                    param_info["type"] = "json"
                    param_info["json_schema"] = field_schema
                    items_type = items_schema.get("type", "object")
                    param_info["description"] = field_schema.get(
                        "description", f"JSON array for {field_name} (items: {items_type})"
                    )

            # Unknown type - skip
            else:
                continue

            # Add field to expanded params
            expanded[field_name] = param_info

        return expanded

    async def start_workflow(
        self, workflow_name: str, params: dict[str, Any], edda_app_url: str
    ) -> tuple[bool, str, str | None]:
        """
        Start a workflow by sending CloudEvent to EddaApp.

        This method creates a CloudEvent and sends it to EddaApp,
        which will trigger the workflow execution.

        Args:
            workflow_name: Name of the workflow to start
            params: Parameters to pass to the workflow
            edda_app_url: EddaApp API base URL (e.g., "http://localhost:8001")

        Returns:
            Tuple of (success: bool, message: str, instance_id: str | None)
        """
        import uuid

        import httpx
        from cloudevents.conversion import to_structured
        from cloudevents.http import CloudEvent

        try:
            logger.debug("Attempting to start workflow: %s", workflow_name)
            logger.debug("Sending CloudEvent to: %s", edda_app_url)
            logger.debug("Params: %s", params)

            # Verify workflow exists
            all_workflows = self.get_all_workflows()
            if workflow_name not in all_workflows:
                return False, f"Workflow '{workflow_name}' not found in registry", None

            # Create CloudEvent
            attributes = {
                "type": workflow_name,  # Workflow name is the event type
                "source": "edda.viewer",
                "id": str(uuid.uuid4()),
            }
            event = CloudEvent(attributes, data=params)

            # Convert to HTTP format (structured content mode)
            headers, body = to_structured(event)

            logger.debug("CloudEvent ID: %s", attributes["id"])
            logger.debug("CloudEvent type: %s", workflow_name)

            # Send CloudEvent to EddaApp
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    edda_app_url,
                    headers=headers,
                    content=body,
                    timeout=10.0,
                )

                logger.debug("Response status: %s", response.status_code)
                logger.debug("Response body: %s", response.text)

                if 200 <= response.status_code < 300:
                    # CloudEvent accepted (200 OK or 202 Accepted)
                    # Note: We can't get instance_id from response because EddaApp
                    # returns immediately after accepting the event.
                    # The workflow will be executed asynchronously.
                    return True, f"Workflow '{workflow_name}' started successfully", None
                else:
                    return False, f"Server error: HTTP {response.status_code}", None

        except httpx.ConnectError as e:
            error_msg = f"Cannot connect to EddaApp at {edda_app_url}. Is it running?"
            logger.warning("Connection error: %s", e)
            return False, error_msg, None

        except httpx.TimeoutException as e:
            error_msg = "Request timed out. The server may be busy."
            logger.warning("Timeout error: %s", e)
            return False, error_msg, None

        except Exception as e:
            error_msg = f"Unexpected error: {type(e).__name__}: {str(e)}"
            logger.error("Unexpected error: %s", e, exc_info=True)
            return False, error_msg, None
