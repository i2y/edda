"""
NiceGUI application for interactive workflow instance visualization.
"""

import asyncio
import contextlib
import json
from typing import Any

from nicegui import app, ui  # type: ignore[import-not-found]
from nicegui.element import Element  # type: ignore[import-not-found]
from nicegui.events import GenericEventArguments  # type: ignore[import-not-found]

from edda import EddaApp
from edda.viewer_ui.components import generate_hybrid_mermaid, generate_interactive_mermaid
from edda.viewer_ui.data_service import WorkflowDataService


def start_viewer(edda_app: EddaApp, port: int = 8080, reload: bool = False) -> None:
    """
    Start the NiceGUI workflow viewer.

    Args:
        edda_app: EddaApp instance
        port: Port to run the server on
        reload: Enable auto-reload for development
    """

    # Initialize storage on NiceGUI startup (to use the correct event loop)
    @app.on_startup  # type: ignore[misc]
    async def init_storage() -> None:
        await edda_app.storage.initialize()

    service = WorkflowDataService(edda_app.storage)
    detail_containers: dict[str, Element] = {}

    def _render_execution_detail(detail: dict[str, Any]) -> None:
        """
        Render execution detail UI (helper function).

        Args:
            detail: Execution detail dictionary
        """
        status = detail["status"]
        if status == "completed":
            ui.badge("Completed", color="green").classes("text-lg")
        elif status == "running":
            ui.badge("Running", color="yellow").classes("text-lg")
        elif status == "failed":
            ui.badge("Failed", color="red").classes("text-lg")
        else:
            ui.badge(status, color="gray").classes("text-lg")

        ui.label(f"Executed: {detail['executed_at']}").classes("text-sm text-gray-600 mt-2")

        ui.markdown("#### Input")
        with ui.card().classes("w-full bg-gray-50 p-4"):
            ui.code(json.dumps(detail["input"], indent=2)).classes("w-full")

        if detail["output"] is not None:
            ui.markdown("#### Output")
            with ui.card().classes("w-full bg-gray-50 p-4"):
                ui.code(json.dumps(detail["output"], indent=2)).classes("w-full")

        if detail["error"]:
            ui.markdown("#### Error")
            with ui.card().classes("w-full bg-red-50 border-red-200 p-4"):
                if detail.get("error_type"):
                    ui.label(f"Type: {detail['error_type']}").classes("text-red-700 font-bold")
                ui.label(detail["error"]).classes("text-red-700 font-mono text-sm mt-2")

                # Show stack trace if available
                if detail.get("stack_trace"):
                    with ui.expansion("Stack Trace", icon="bug_report").classes("mt-4 w-full"):
                        ui.code(detail["stack_trace"]).classes("w-full text-xs")

    async def handle_activity_click(event: GenericEventArguments) -> None:
        """Handle activity click event from Mermaid diagram with multi-execution support."""
        try:
            # Event args are now passed as separate arguments: [instance_id, activity_id]
            if isinstance(event.args, list) and len(event.args) >= 2:
                instance_id = event.args[0]
                activity_id = event.args[1]
            else:
                ui.notify(f"Unexpected event.args format: {event.args}", type="warning")
                return

            # Get single activity detail
            detail = await service.get_activity_detail(instance_id, activity_id)

            if not detail:
                ui.notify("Activity not found", type="negative")
                return

            # Get all executions of the same activity
            activity_name = detail["activity_name"]
            all_executions = await service.get_activity_executions(instance_id, activity_name)

            container = detail_containers.get(instance_id)
            if not container:
                ui.notify("Detail container not found", type="warning")
                return

            container.clear()
            with container:
                # Header
                if len(all_executions) > 1:
                    ui.label(f"{activity_name} (Executed {len(all_executions)} times)").classes(
                        "text-2xl font-bold"
                    )
                else:
                    ui.label(f"{activity_name}: {detail['activity_id']}").classes(
                        "text-2xl font-bold"
                    )

                # Multiple executions: use tabs
                if len(all_executions) > 1:
                    # Find current execution index
                    current_index = next(
                        (
                            i
                            for i, ex in enumerate(all_executions)
                            if ex["activity_id"] == activity_id
                        ),
                        0,
                    )

                    with ui.tabs() as tabs:
                        for i, exec_detail in enumerate(all_executions):
                            is_current = exec_detail["activity_id"] == activity_id
                            # Add arrow indicator for currently clicked activity
                            label = f"{'→ ' if is_current else ''}Execution #{i + 1} ({exec_detail['activity_id']})"
                            ui.tab(f"exec{i}", label=label)

                    with ui.tab_panels(tabs, value=f"exec{current_index}"):
                        for i, exec_detail in enumerate(all_executions):
                            with ui.tab_panel(f"exec{i}"):
                                _render_execution_detail(exec_detail)
                else:
                    # Single execution: render directly
                    _render_execution_detail(detail)

            ui.notify(f"Loaded {activity_name}: {activity_id}", type="positive")

        except Exception as e:
            ui.notify(f"Error loading activity detail: {e}", type="negative")

    # Register global event handler (keep both for backward compatibility during migration)
    app.on_connect(lambda: ui.on("step_click", handle_activity_click))
    app.on_connect(lambda: ui.on("activity_click", handle_activity_click))

    # Define index page
    @ui.page("/")  # type: ignore[misc]
    async def index_page() -> None:
        """Workflow instances list page."""
        # Header with title and start button
        with ui.row().classes("w-full items-center justify-between mb-4"):
            ui.markdown("# Edda Workflow Instances")

            # Start New Workflow button and dialog
            with ui.dialog() as start_dialog, ui.card().style("min-width: 500px"):
                ui.label("Start New Workflow").classes("text-xl font-bold mb-4")

                # Get all available workflows
                all_workflows = service.get_all_workflows()
                workflow_names = list(all_workflows.keys())

                if not workflow_names:
                    ui.label("No workflows registered").classes("text-red-500")
                    ui.button("Close", on_click=start_dialog.close)
                else:
                    # Workflow selection
                    workflow_select = ui.select(
                        workflow_names,
                        label="Select Workflow",
                        value=workflow_names[0] if workflow_names else None,
                    ).classes("w-full mb-4")

                    # Container for dynamic parameter fields
                    params_container = ui.column().classes("w-full mb-4")

                    # Store input field references
                    param_fields: dict[str, Any] = {}

                    # Factory functions for creating field managers with proper closures
                    # These must be defined outside the loop to avoid closure issues

                    def create_nested_dict_field(initial_dict: Any = None) -> Any:
                        """Create a nested dict field with dynamic key-value pairs.

                        This is used for list[dict] items, creating a mini dict editor
                        for each list item.
                        """

                        class DictFieldContainer:
                            """Container for dict field with dynamic key-value pairs."""

                            def __init__(self) -> None:
                                dict_data = initial_dict if isinstance(initial_dict, dict) else {}
                                self.pairs = [[k, v] for k, v in dict_data.items()]
                                self.pair_fields: list[list[Any]] = []

                                @ui.refreshable  # type: ignore[misc]
                                def dict_items_ui() -> None:
                                    """Refreshable UI for dict key-value pairs."""
                                    self.pair_fields.clear()
                                    for i in range(len(self.pairs)):
                                        with ui.row().classes("w-full gap-2"):
                                            k_field = (
                                                ui.input(
                                                    label="Key",
                                                    value=(
                                                        str(self.pairs[i][0])
                                                        if self.pairs[i][0] is not None
                                                        else ""
                                                    ),
                                                )
                                                .classes("flex-1")
                                                .props("dense")
                                            )
                                            v_field = (
                                                ui.input(
                                                    label="Value",
                                                    value=(
                                                        str(self.pairs[i][1])
                                                        if self.pairs[i][1] is not None
                                                        else ""
                                                    ),
                                                )
                                                .classes("flex-1")
                                                .props("dense")
                                            )
                                            self.pair_fields.append([k_field, v_field])
                                            ui.button(
                                                icon="delete",
                                                on_click=lambda idx=i: self.remove_pair(idx),
                                            ).props("flat dense size=sm color=negative")

                                self.dict_items_ui = dict_items_ui

                                # Create the UI
                                with ui.column().classes(
                                    "w-full gap-1 p-2 border rounded bg-gray-50"
                                ):
                                    dict_items_ui()
                                    ui.button(
                                        "Add Field", icon="add", on_click=self.add_pair
                                    ).props("flat dense size=sm color=primary")

                            def add_pair(self) -> None:
                                """Add a new key-value pair."""
                                self.pairs.append(["", ""])
                                self.dict_items_ui.refresh()

                            def remove_pair(self, idx: int) -> None:
                                """Remove a key-value pair."""
                                if 0 <= idx < len(self.pairs):
                                    self.pairs.pop(idx)
                                    self.dict_items_ui.refresh()

                            @property
                            def value(self) -> dict[str, Any]:
                                """Get the current dict value."""
                                result: dict[str, Any] = {}
                                for k_field, v_field in self.pair_fields:
                                    if hasattr(k_field, "value") and hasattr(v_field, "value"):
                                        k = k_field.value
                                        v = v_field.value
                                        if k:  # Only add if key is not empty
                                            result[k] = v
                                return result

                        return DictFieldContainer()

                    def create_list_field_manager(
                        param_item_type: str, param_initial_items: list[Any]
                    ) -> tuple[Any, Any, Any]:
                        """Factory function to create list field manager with proper closure."""
                        items = list(param_initial_items)  # Value storage
                        fields: list[Any] = []  # Field reference storage

                        def create_field(item_value: Any = None) -> Any:
                            """Create a single list item field."""
                            if param_item_type == "str":
                                return (
                                    ui.input(
                                        value=str(item_value) if item_value is not None else ""
                                    )
                                    .classes("w-full")
                                    .props("dense")
                                )
                            elif param_item_type == "int":
                                return (
                                    ui.number(
                                        value=item_value if item_value is not None else 0,
                                        format="%.0f",
                                    )
                                    .classes("w-full")
                                    .props("dense")
                                )
                            elif param_item_type == "float":
                                return (
                                    ui.number(
                                        value=item_value if item_value is not None else 0.0,
                                        step=0.01,
                                        format="%.2f",
                                    )
                                    .classes("w-full")
                                    .props("dense")
                                )
                            elif param_item_type == "bool":
                                return ui.checkbox(
                                    value=item_value if item_value is not None else False
                                )
                            elif param_item_type == "dict":
                                # For list[dict], create nested key-value editor
                                return create_nested_dict_field(item_value)
                            else:
                                # Fallback to JSON
                                return (
                                    ui.textarea(
                                        value=(
                                            json.dumps(item_value) if item_value is not None else ""
                                        )
                                    )
                                    .classes("w-full")
                                    .props("dense")
                                )

                        # Create a refreshable component for the list
                        @ui.refreshable  # type: ignore[misc]
                        def list_items_ui() -> None:
                            """Refreshable UI for list items."""
                            fields.clear()
                            for i in range(len(items)):
                                with ui.row().classes("w-full items-center gap-2"):
                                    field = create_field(items[i])
                                    fields.append(field)
                                    # Capture index in default argument
                                    ui.button(
                                        icon="delete", on_click=lambda idx=i: remove_item(idx)
                                    ).props("flat dense size=sm color=negative")

                        def add_item() -> None:
                            """Add a new item to the list."""
                            items.append(None)
                            list_items_ui.refresh()

                        def remove_item(index: int) -> None:
                            """Remove an item from the list."""
                            if 0 <= index < len(items):
                                items.pop(index)
                                list_items_ui.refresh()

                        def get_value() -> list[Any]:
                            """Get the current list value."""
                            result = []
                            for field in fields:
                                if hasattr(field, "value"):
                                    val = field.value
                                    # Parse dict items
                                    if param_item_type == "dict" and isinstance(val, str):
                                        with contextlib.suppress(json.JSONDecodeError):
                                            val = json.loads(val)
                                    result.append(val)
                            return result

                        return (add_item, list_items_ui, get_value)

                    def create_dict_field_manager(
                        param_key_type: str,
                        param_value_type: str,
                        param_initial_dict: dict[Any, Any],
                    ) -> tuple[Any, Any, Any]:
                        """Factory function to create dict field manager with proper closure."""
                        pairs = [
                            [k, v] for k, v in param_initial_dict.items()
                        ]  # Value storage [[key, value], ...]
                        pair_fields: list[Any] = (
                            []
                        )  # Field reference storage [[key_field, value_field], ...]

                        def create_key_field(key_value: Any = None) -> Any:
                            """Create a key field."""
                            if param_key_type == "str":
                                return (
                                    ui.input(
                                        label="Key",
                                        value=str(key_value) if key_value is not None else "",
                                    )
                                    .classes("flex-1")
                                    .props("dense")
                                )
                            elif param_key_type == "int":
                                return (
                                    ui.number(
                                        label="Key",
                                        value=key_value if key_value is not None else 0,
                                        format="%.0f",
                                    )
                                    .classes("flex-1")
                                    .props("dense")
                                )
                            else:
                                return (
                                    ui.input(
                                        label="Key",
                                        value=str(key_value) if key_value is not None else "",
                                    )
                                    .classes("flex-1")
                                    .props("dense")
                                )

                        def create_value_field(val: Any = None) -> Any:
                            """Create a value field."""
                            if param_value_type == "str":
                                return (
                                    ui.input(
                                        label="Value", value=str(val) if val is not None else ""
                                    )
                                    .classes("flex-1")
                                    .props("dense")
                                )
                            elif param_value_type == "int":
                                return (
                                    ui.number(
                                        label="Value",
                                        value=val if val is not None else 0,
                                        format="%.0f",
                                    )
                                    .classes("flex-1")
                                    .props("dense")
                                )
                            elif param_value_type == "float":
                                return (
                                    ui.number(
                                        label="Value",
                                        value=val if val is not None else 0.0,
                                        step=0.01,
                                        format="%.2f",
                                    )
                                    .classes("flex-1")
                                    .props("dense")
                                )
                            else:
                                return (
                                    ui.input(
                                        label="Value", value=str(val) if val is not None else ""
                                    )
                                    .classes("flex-1")
                                    .props("dense")
                                )

                        # Create a refreshable component for the pairs
                        @ui.refreshable  # type: ignore[misc]
                        def dict_pairs_ui() -> None:
                            """Refreshable UI for dict pairs."""
                            pair_fields.clear()
                            for i in range(len(pairs)):
                                with ui.row().classes("w-full items-center gap-2"):
                                    key_field = create_key_field(pairs[i][0])
                                    value_field = create_value_field(pairs[i][1])
                                    pair_fields.append([key_field, value_field])
                                    # Capture index in default argument
                                    ui.button(
                                        icon="delete", on_click=lambda idx=i: remove_pair(idx)
                                    ).props("flat dense size=sm color=negative")

                        def add_pair() -> None:
                            """Add a new key-value pair."""
                            pairs.append([None, None])
                            dict_pairs_ui.refresh()

                        def remove_pair(index: int) -> None:
                            """Remove a key-value pair."""
                            if 0 <= index < len(pairs):
                                pairs.pop(index)
                                dict_pairs_ui.refresh()

                        def get_value() -> dict[Any, Any]:
                            """Get the current dict value."""
                            result = {}
                            for key_field, value_field in pair_fields:
                                if hasattr(key_field, "value") and hasattr(value_field, "value"):
                                    k = key_field.value
                                    v = value_field.value
                                    if k:  # Only add if key is not empty
                                        result[k] = v
                            return result

                        return (add_pair, dict_pairs_ui, get_value)

                    def create_list_of_pydantic_manager(
                        param_item_fields: dict[str, dict[str, Any]],
                        param_initial_items: list[Any] | None = None,
                    ) -> tuple[Any, Any, Any]:
                        """Factory function to create list[PydanticModel] field manager with proper closure."""
                        # Initialize with at least one empty item
                        items: list[dict[str, Any]] = (
                            list(param_initial_items)
                            if param_initial_items
                            else [{}]  # Start with one empty item
                        )
                        item_field_refs: list[dict[str, Any]] = []  # Field references for each item

                        def create_item_fields(item_data: dict[str, Any]) -> dict[str, Any]:
                            """Create fields for a single Pydantic model item."""
                            fields = {}
                            for field_name, field_info in param_item_fields.items():
                                field_type = field_info["type"]
                                required = field_info.get("required", True)
                                default = field_info.get("default")
                                # Use item_data value if available, otherwise use default
                                value = item_data.get(field_name, default)

                                # Add * for required fields
                                label = f"{field_name} *" if required else field_name

                                if field_type == "int":
                                    field = (
                                        ui.number(
                                            label=label,
                                            value=value if value is not None else None,
                                            format="%.0f",
                                        )
                                        .classes("w-full")
                                        .props("dense")
                                    )
                                elif field_type == "float":
                                    field = (
                                        ui.number(
                                            label=label,
                                            value=value if value is not None else None,
                                            step=0.01,
                                            format="%.2f",
                                        )
                                        .classes("w-full")
                                        .props("dense")
                                    )
                                elif field_type == "bool":
                                    field = ui.checkbox(
                                        text=label,
                                        value=value if value is not None else False,
                                    ).props("dense")
                                elif field_type == "str":
                                    field = (
                                        ui.input(
                                            label=label,
                                            value=value if value is not None else "",
                                        )
                                        .classes("w-full")
                                        .props("dense")
                                    )
                                elif field_type == "enum":
                                    # Enum field
                                    enum_values = field_info.get("enum_values", [])
                                    options = {val: name for name, val in enum_values}
                                    default_value = None
                                    if value is not None:
                                        default_value = (
                                            value.value if hasattr(value, "value") else value
                                        )
                                    field = (
                                        ui.select(
                                            options=options,
                                            label=label,
                                            value=default_value,
                                        )
                                        .classes("w-full")
                                        .props("dense")
                                    )
                                else:
                                    # Fallback to input
                                    field = (
                                        ui.input(
                                            label=label,
                                            value=str(value) if value is not None else "",
                                        )
                                        .classes("w-full")
                                        .props("dense")
                                    )

                                fields[field_name] = field

                            return fields

                        # Create a refreshable component for the list of items
                        @ui.refreshable  # type: ignore[misc]
                        def list_items_ui() -> None:
                            """Refreshable UI for list of Pydantic model items."""
                            item_field_refs.clear()
                            for i in range(len(items)):
                                # Each item in a bordered container
                                with ui.column().classes(
                                    "w-full border rounded p-2 mb-2 bg-gray-50"
                                ):
                                    with ui.row().classes(
                                        "w-full items-center justify-between mb-2"
                                    ):
                                        ui.label(f"Item {i + 1}").classes("font-semibold text-sm")
                                        # Remove button (capture index in default argument)
                                        ui.button(
                                            icon="delete",
                                            on_click=lambda idx=i: remove_item(idx),
                                        ).props("flat dense size=sm color=negative")

                                    # Create fields for this item
                                    item_fields = create_item_fields(items[i])
                                    item_field_refs.append(item_fields)

                        def add_item() -> None:
                            """Add a new item to the list."""
                            items.append({})  # Add empty dict
                            list_items_ui.refresh()

                        def remove_item(index: int) -> None:
                            """Remove an item from the list."""
                            if 0 <= index < len(items):
                                items.pop(index)
                                list_items_ui.refresh()

                        def get_value() -> list[dict[str, Any]]:
                            """Get the current list value as list of dicts."""
                            result = []
                            for item_fields in item_field_refs:
                                item_data = {}
                                for field_name, field in item_fields.items():
                                    if hasattr(field, "value"):
                                        item_data[field_name] = field.value
                                result.append(item_data)
                            return result

                        return (add_item, list_items_ui, get_value)

                    def update_parameter_fields() -> None:
                        """Update parameter input fields based on selected workflow."""
                        selected_workflow = workflow_select.value
                        if not selected_workflow:
                            return

                        # Get parameter information
                        params_info = service.get_workflow_parameters(selected_workflow)

                        # Clear existing fields
                        params_container.clear()
                        param_fields.clear()

                        with params_container:
                            if not params_info:
                                ui.label("No parameters required").classes(
                                    "text-sm text-gray-500 italic"
                                )
                            else:
                                ui.label("Parameters:").classes("text-sm font-semibold mb-2")

                                # Group fields by parent for nested model display
                                root_fields = {}
                                nested_groups: dict[str, dict[str, Any]] = {}

                                for param_name, info in params_info.items():
                                    if "_parent_field" in info:
                                        # Nested field - group by parent
                                        parent = info["_parent_field"]
                                        if parent not in nested_groups:
                                            nested_groups[parent] = {}
                                        nested_groups[parent][param_name] = info
                                    else:
                                        # Root-level field
                                        root_fields[param_name] = info

                                # Helper function to create a single field
                                def create_field_ui(param_name: str, info: dict[str, Any]) -> None:
                                    param_type = info["type"]
                                    required = info["required"]
                                    default = info["default"]

                                    # Generate label (use simple name for nested fields)
                                    if "_parent_field" in info:
                                        # For nested fields like "shipping_address.street", show just "street"
                                        simple_name = param_name.split(".")[-1]
                                        label = simple_name
                                    else:
                                        label = param_name

                                    if required:
                                        label = f"{label} * [{param_type}]"  # * for required fields
                                    else:
                                        default_str = (
                                            str(default) if default is not None else "none"
                                        )
                                        label = f"{label} (optional, default: {default_str}) [{param_type}]"

                                    # Declare field variable with Any type for mypy
                                    field: Any

                                    # Generate appropriate input field based on type
                                    if param_type == "int":
                                        field = ui.number(
                                            label=label,
                                            value=default if default is not None else None,
                                            format="%.0f",
                                        ).classes("w-full")
                                        param_fields[param_name] = {
                                            "field": field,
                                            "type": param_type,
                                            "info": info,
                                        }

                                    elif param_type == "float":
                                        field = ui.number(
                                            label=label,
                                            value=default if default is not None else None,
                                            step=0.01,
                                            format="%.2f",
                                        ).classes("w-full")
                                        param_fields[param_name] = {
                                            "field": field,
                                            "type": param_type,
                                            "info": info,
                                        }

                                    elif param_type == "bool":
                                        field = ui.checkbox(
                                            text=label,
                                            value=default if default is not None else False,
                                        )
                                        param_fields[param_name] = {
                                            "field": field,
                                            "type": param_type,
                                            "info": info,
                                        }

                                    elif param_type == "str":
                                        field = ui.input(
                                            label=label,
                                            value=default if default is not None else "",
                                        ).classes("w-full")
                                        param_fields[param_name] = {
                                            "field": field,
                                            "type": param_type,
                                            "info": info,
                                        }

                                    elif param_type == "enum":
                                        # Enum type - dropdown
                                        enum_values = info.get("enum_values", [])
                                        # NiceGUI ui.select expects: {key: label}, where key is the internal value
                                        # We use enum value as key (what we send to CloudEvents) and name as label
                                        options = {value: name for name, value in enum_values}

                                        # Determine default value (should match a key in options)
                                        default_value = None
                                        if default is not None:
                                            # default might be an Enum member
                                            if hasattr(default, "value"):
                                                default_value = default.value
                                            else:
                                                default_value = default

                                        field = ui.select(
                                            options=options,
                                            label=label,
                                            value=default_value,
                                        ).classes("w-full")
                                        param_fields[param_name] = {
                                            "field": field,
                                            "type": param_type,
                                            "info": info,
                                        }

                                    elif param_type == "list":
                                        # List type - dynamic list with add/remove buttons
                                        # Get item type and initial items
                                        item_type = info.get("item_type", "json")
                                        initial_items = (
                                            default
                                            if default is not None and isinstance(default, list)
                                            else []
                                        )

                                        # Create field manager with proper closure (defined outside loop)
                                        add_item_fn, list_items_ui_fn, get_list_value = (
                                            create_list_field_manager(item_type, initial_items)
                                        )

                                        # Create container for list items
                                        with ui.column().classes("w-full border rounded p-2"):
                                            ui.label(label).classes("font-semibold mb-2")

                                            # Render list items using refreshable UI
                                            list_items_ui_fn()

                                            # Add item button (no need to capture container)
                                            ui.button(
                                                "+ Add Item", on_click=add_item_fn, icon="add"
                                            ).props("flat size=sm").classes("mt-2")

                                        param_fields[param_name] = {
                                            "type": param_type,
                                            "info": info,
                                            "get_value": get_list_value,
                                        }

                                    elif param_type == "dict":
                                        # Dict type - dynamic key-value pairs
                                        # Get key/value types and initial dict
                                        key_type = info.get("key_type", "str")
                                        value_type = info.get("value_type", "json")
                                        initial_dict = (
                                            default
                                            if default is not None and isinstance(default, dict)
                                            else {}
                                        )

                                        # Create field manager with proper closure (defined outside loop)
                                        add_pair_fn, dict_pairs_ui_fn, get_dict_value = (
                                            create_dict_field_manager(
                                                key_type, value_type, initial_dict
                                            )
                                        )

                                        # Create container for dict pairs
                                        with ui.column().classes("w-full border rounded p-2"):
                                            ui.label(label).classes("font-semibold mb-2")

                                            # Render dict pairs using refreshable UI
                                            dict_pairs_ui_fn()

                                            # Add pair button (no need to capture container)
                                            ui.button(
                                                "+ Add Pair", on_click=add_pair_fn, icon="add"
                                            ).props("flat size=sm").classes("mt-2")

                                        param_fields[param_name] = {
                                            "type": param_type,
                                            "info": info,
                                            "get_value": get_dict_value,
                                        }

                                    elif param_type == "list_of_pydantic":
                                        # dynamic list with sub-forms
                                        item_fields = info.get("item_fields", {})
                                        initial_items = (
                                            default
                                            if default is not None and isinstance(default, list)
                                            else None
                                        )

                                        # Create field manager with proper closure
                                        add_item_fn, list_items_ui_fn, get_list_value = (
                                            create_list_of_pydantic_manager(
                                                item_fields, initial_items
                                            )
                                        )

                                        # Create container for list of items
                                        with ui.column().classes("w-full border rounded p-3"):
                                            ui.label(label).classes("font-semibold mb-2")

                                            # Render list items using refreshable UI
                                            list_items_ui_fn()

                                            # Add item button
                                            ui.button(
                                                "+ Add Item", on_click=add_item_fn, icon="add"
                                            ).props("flat size=sm").classes("mt-2")

                                        param_fields[param_name] = {
                                            "type": param_type,
                                            "info": info,
                                            "get_value": get_list_value,
                                        }

                                    elif param_type == "json":
                                        # JSON textarea for nested models and complex types
                                        description = info.get("description", "")
                                        json_schema = info.get("json_schema", {})
                                        schema_type = json_schema.get("type", "")

                                        # Generate example JSON based on schema
                                        placeholder = ""
                                        if schema_type == "object":
                                            properties = json_schema.get("properties", {})
                                            if properties:
                                                example = dict.fromkeys(
                                                    list(properties.keys())[:3], "..."
                                                )
                                                placeholder = json.dumps(example, indent=2)
                                            else:
                                                placeholder = '{"key": "value"}'
                                        elif schema_type == "array":
                                            items = json_schema.get("items", {})
                                            items_type = items.get("type", "object")
                                            if items_type == "object":
                                                placeholder = '[{"key": "value"}]'
                                            elif items_type == "string":
                                                placeholder = '["item1", "item2"]'
                                            elif items_type == "integer":
                                                placeholder = "[1, 2, 3]"
                                            else:
                                                placeholder = "[]"
                                        else:
                                            placeholder = '{"key": "value"}'

                                        field = (
                                            ui.textarea(
                                                label=f"{label} (JSON)"
                                                + (f" - {description}" if description else ""),
                                                placeholder=placeholder,
                                                value=(
                                                    json.dumps(default, indent=2)
                                                    if default is not None
                                                    else ""
                                                ),
                                            )
                                            .classes("w-full")
                                            .props("rows=6")
                                        )
                                        param_fields[param_name] = {
                                            "field": field,
                                            "type": param_type,
                                            "info": info,
                                        }

                                    else:
                                        # Fallback to JSON textarea for unknown types
                                        field = ui.textarea(
                                            label=f"{label} (JSON)",
                                            placeholder='{"key": "value"}',
                                            value=(
                                                json.dumps(default) if default is not None else ""
                                            ),
                                        ).classes("w-full")
                                        param_fields[param_name] = {
                                            "field": field,
                                            "type": param_type,
                                            "info": info,
                                        }

                                # End of create_field_ui helper function

                                # Render root fields first
                                for param_name, info in root_fields.items():
                                    create_field_ui(param_name, info)

                                # Render nested field groups with visual grouping
                                for parent_name, nested_fields in nested_groups.items():
                                    # Create a visually grouped container for nested model
                                    with ui.column().classes(
                                        "w-full border rounded p-3 bg-gray-50 mt-2"
                                    ):
                                        # Parent field label
                                        ui.label(f"{parent_name} [nested model]").classes(
                                            "text-sm font-semibold text-gray-700 mb-2"
                                        )

                                        # Render all nested fields
                                        for nested_param_name, nested_info in nested_fields.items():
                                            create_field_ui(nested_param_name, nested_info)

                    # Initial parameter fields generation
                    update_parameter_fields()

                    # Update fields when workflow selection changes
                    workflow_select.on_value_change(lambda _: update_parameter_fields())

                    # Action buttons
                    with ui.row().classes("w-full gap-2"):

                        async def handle_start() -> None:
                            """Handle workflow start."""
                            try:
                                selected_workflow = workflow_select.value
                                if not selected_workflow:
                                    ui.notify("Please select a workflow", type="negative")
                                    return

                                # Collect parameter values from fields
                                params: dict[str, Any] = {}
                                for param_name, field_info in param_fields.items():
                                    param_type = field_info["type"]

                                    # Get value based on field type
                                    if "get_value" in field_info:
                                        # list or dict with custom getter
                                        value = field_info["get_value"]()
                                    elif "field" in field_info:
                                        # Basic types with field.value
                                        value = field_info["field"].value
                                    else:
                                        continue

                                    # Skip empty optional fields
                                    if value is None or value == "":
                                        continue

                                    # Type conversion
                                    if param_type == "json":
                                        # Parse JSON for complex types
                                        try:
                                            params[param_name] = (
                                                json.loads(value)
                                                if isinstance(value, str)
                                                else value
                                            )
                                        except json.JSONDecodeError as e:
                                            ui.notify(
                                                f"Invalid JSON for {param_name}: {e}",
                                                type="negative",
                                            )
                                            return
                                    elif param_type == "enum":
                                        # Enum values are already in the correct format
                                        params[param_name] = value
                                    elif param_type == "list":
                                        # List values are already parsed
                                        params[param_name] = value
                                    elif param_type == "dict":
                                        # Dict values are already parsed
                                        params[param_name] = value
                                    elif param_type == "list_of_pydantic":
                                        # list[PydanticModel] values are already parsed as list[dict]
                                        # Filter out empty items (all fields are empty/None)
                                        if isinstance(value, list):
                                            filtered_items = []
                                            for item in value:
                                                if isinstance(item, dict):
                                                    # Check if item has any non-empty values
                                                    has_value = any(
                                                        v is not None and v != ""
                                                        for v in item.values()
                                                    )
                                                    if has_value:
                                                        filtered_items.append(item)
                                            # Only add if there are non-empty items
                                            if filtered_items:
                                                params[param_name] = filtered_items
                                        else:
                                            params[param_name] = value
                                    else:
                                        # Direct value for basic types (int, str, float, bool)
                                        params[param_name] = value

                                # Reconstruct nested model structure
                                # Check for nested fields (_parent_field metadata)
                                nested_field_groups: dict[str, dict[str, Any]] = {}
                                root_params: dict[str, Any] = {}

                                for param_name, field_info in param_fields.items():
                                    if "_parent_field" in field_info["info"]:
                                        # Nested field - extract parent and simple name
                                        parent = field_info["info"]["_parent_field"]
                                        # param_name is like "shipping_address.street"
                                        simple_name = param_name.split(".")[-1]

                                        if parent not in nested_field_groups:
                                            nested_field_groups[parent] = {}

                                        # Get value from params (already collected above)
                                        if param_name in params:
                                            nested_field_groups[parent][simple_name] = params[
                                                param_name
                                            ]
                                    else:
                                        # Root-level field - keep as is
                                        if param_name in params:
                                            root_params[param_name] = params[param_name]

                                # Rebuild params with nested structure
                                # Filter out empty nested models
                                params = root_params.copy()
                                for parent, nested_fields in nested_field_groups.items():
                                    # Check if nested model has any non-empty values
                                    has_value = any(
                                        v is not None and v != "" for v in nested_fields.values()
                                    )
                                    # Only add nested model if it has non-empty values
                                    if has_value:
                                        params[parent] = nested_fields

                                # Reconstruct Pydantic model from expanded fields
                                # Check if any field has _pydantic_model_name (indicates expanded fields)
                                pydantic_model_name = None
                                for field_info in param_fields.values():
                                    if "_pydantic_model_name" in field_info.get("info", {}):
                                        pydantic_model_name = field_info["info"][
                                            "_pydantic_model_name"
                                        ]
                                        break

                                if pydantic_model_name:
                                    # All expanded fields should be reconstructed into original model structure
                                    # params = {field1: value1, field2: value2, ...}
                                    # → {model_name: {field1: value1, field2: value2, ...}}
                                    params = {pydantic_model_name: params}

                                # Get EddaApp URL from environment or use default
                                import os

                                edda_app_url = os.getenv("EDDA_APP_URL", "http://localhost:8001")

                                ui.notify(
                                    f"Starting workflow '{selected_workflow}'...", type="info"
                                )

                                # Start workflow
                                success, message, _ = await service.start_workflow(
                                    selected_workflow, params, edda_app_url
                                )

                                if success:
                                    ui.notify(message, type="positive")
                                    start_dialog.close()
                                    # Refresh page after a short delay
                                    await asyncio.sleep(1)
                                    ui.navigate.reload()
                                else:
                                    ui.notify(f"Failed to start: {message}", type="negative")

                            except Exception as e:
                                ui.notify(f"Error: {e}", type="negative")

                        ui.button("Start", on_click=handle_start, color="positive")
                        ui.button("Cancel", on_click=start_dialog.close)

            ui.button(
                "Start New Workflow",
                on_click=start_dialog.open,
                icon="play_arrow",
                color="positive",
            )

        ui.label("Click on an instance to view execution details").classes("text-gray-600 mb-4")

        instances = await service.get_all_instances(limit=100)

        if not instances:
            ui.label("No workflow instances found").classes("text-gray-500 italic mt-8")
            ui.label("Run some workflows first, or click 'Start New Workflow' above!").classes(
                "text-sm text-gray-400"
            )
            return

        with ui.column().classes("w-full gap-2"):
            for inst in instances:
                with (
                    ui.link(target=f'/workflow/{inst["instance_id"]}').classes(
                        "no-underline w-full"
                    ),
                    ui.card().classes("w-full cursor-pointer hover:shadow-lg transition-shadow"),
                    ui.row().classes("w-full items-center justify-between"),
                ):
                    with ui.column():
                        ui.label(inst["workflow_name"]).classes("text-xl font-bold")
                        ui.label(f'ID: {inst["instance_id"][:16]}...').classes(
                            "text-sm text-gray-500"
                        )
                        ui.label(f'Started: {inst["started_at"]}').classes("text-xs text-gray-400")

                    status = inst["status"]
                    if status == "completed":
                        ui.badge("✅ Completed", color="green")
                    elif status == "running":
                        ui.badge("⏳ Running", color="yellow")
                    elif status == "failed":
                        ui.badge("❌ Failed", color="red")
                    elif status == "waiting_for_event":
                        ui.badge("⏸️ Waiting (Event)", color="blue")
                    elif status == "waiting_for_timer":
                        ui.badge("⏱️ Waiting (Timer)", color="cyan")
                    elif status == "cancelled":
                        ui.badge("🚫 Cancelled", color="orange")
                    else:
                        ui.badge(status, color="gray")

    # Define detail page
    @ui.page("/workflow/{instance_id}")  # type: ignore[misc]
    async def workflow_detail_page(instance_id: str) -> None:
        """Workflow instance detail page with interactive Mermaid diagram."""
        data = await service.get_instance_detail(instance_id)
        instance = data.get("instance")
        history = data.get("history", [])
        compensations = data.get("compensations", {})

        if not instance:
            ui.label("Workflow instance not found").classes("text-red-500 text-xl mt-8")
            ui.button("← Back to list", on_click=lambda: ui.navigate.to("/"))
            return

        # Header with back button and cancel button
        with ui.row().classes("w-full items-center justify-between mb-4"):
            ui.markdown("# Edda Workflow Viewer")
            with ui.row().classes("gap-2"):
                # Cancel button (only show for running/waiting workflows)
                status = instance["status"]
                if status in ["running", "waiting_for_event", "waiting_for_timer"]:

                    async def handle_cancel() -> None:
                        """Handle workflow cancellation."""
                        # Show confirmation dialog with longer timeout
                        try:
                            result = await ui.run_javascript(
                                'confirm("Are you sure you want to cancel this workflow?")',
                                timeout=5.0,  # Increase timeout to 5 seconds
                            )
                        except Exception as e:
                            # If JavaScript fails, proceed anyway
                            print(f"Warning: JavaScript confirmation failed: {e}")
                            result = True  # Proceed with cancel

                        if result:
                            # Call cancel API
                            edda_url = "http://localhost:8001"
                            success, message = await service.cancel_workflow(instance_id, edda_url)

                            if success:
                                ui.notify(message, type="positive")
                                # Refresh page after short delay
                                await asyncio.sleep(0.5)
                                ui.navigate.to(f"/workflow/{instance_id}")
                            else:
                                ui.notify(message, type="negative")

                    ui.button("🚫 Cancel Workflow", on_click=handle_cancel).props("color=orange")

                ui.button("← Back to List", on_click=lambda: ui.navigate.to("/")).props("flat")

        # Workflow basic info card (full width at top)
        with ui.card().classes("w-full mb-4"):
            ui.label(instance["workflow_name"]).classes("text-2xl font-bold")

            with ui.row().classes("gap-4 items-center flex-wrap"):
                status = instance["status"]
                if status == "completed":
                    ui.badge("✅ Completed", color="green")
                elif status == "running":
                    ui.badge("⏳ Running", color="yellow")
                elif status == "failed":
                    ui.badge("❌ Failed", color="red")
                elif status == "waiting_for_event":
                    ui.badge("⏸️ Waiting (Event)", color="blue")
                elif status == "waiting_for_timer":
                    ui.badge("⏱️ Waiting (Timer)", color="cyan")
                elif status == "cancelled":
                    ui.badge("🚫 Cancelled", color="orange")
                elif status == "compensating":
                    ui.badge("🔄 Compensating", color="purple")
                else:
                    ui.badge(status, color="gray")

                ui.label(f"Started: {instance['started_at']}").classes("text-sm text-gray-600")
                ui.label(f"Updated: {instance['updated_at']}").classes("text-sm text-gray-600")

            ui.label(f"Instance ID: {instance_id}").classes("text-xs text-gray-500 font-mono mt-2")

            # Input Parameters section
            input_data = instance.get("input_data")
            if input_data:
                with ui.expansion("📥 Input Parameters", icon="input").classes("w-full mt-3"):
                    try:
                        import json

                        # Check if input_data is already a dict or needs parsing
                        if isinstance(input_data, dict):
                            formatted_input = json.dumps(input_data, indent=2)
                        else:
                            formatted_input = json.dumps(json.loads(input_data), indent=2)
                        ui.code(formatted_input, language="json").classes("w-full")
                    except Exception:
                        # If anything fails, display as string
                        ui.code(str(input_data)).classes("w-full")

            # Output Result section (only for completed workflows)
            if status == "completed":
                output_data = instance.get("output_data")
                if output_data:
                    with ui.expansion("📤 Output Result", icon="output").classes("w-full mt-2"):
                        try:
                            import json

                            # Check if output_data is already a dict or needs parsing
                            if isinstance(output_data, dict):
                                formatted_output = json.dumps(output_data, indent=2)
                            else:
                                formatted_output = json.dumps(json.loads(output_data), indent=2)
                            ui.code(formatted_output, language="json").classes("w-full")
                        except Exception:
                            # If anything fails, display as string
                            ui.code(str(output_data)).classes("w-full")

            # Error Details section (only for failed workflows)
            if status == "failed":
                output_data = instance.get("output_data")
                if output_data:
                    try:
                        import json

                        # Parse output_data if it's a JSON string
                        if isinstance(output_data, str):
                            error_data = json.loads(output_data)
                        else:
                            error_data = output_data

                        # Check if we have detailed error information
                        if isinstance(error_data, dict) and (
                            "error_message" in error_data or "error_type" in error_data
                        ):
                            with ui.card().classes("w-full mt-2 bg-red-50 border-red-200"):
                                ui.markdown("### ❌ Error Details")

                                # Error type (if available)
                                if error_data.get("error_type"):
                                    ui.label(f"Type: {error_data['error_type']}").classes(
                                        "text-red-700 font-bold text-lg"
                                    )

                                # Error message
                                error_msg = error_data.get("error_message", "Unknown error")
                                ui.label(error_msg).classes("text-red-700 font-mono text-sm mt-2")

                                # Stack trace (expandable section)
                                if error_data.get("stack_trace"):
                                    with ui.expansion("📋 Stack Trace", icon="bug_report").classes(
                                        "mt-4 w-full"
                                    ):
                                        ui.code(error_data["stack_trace"]).classes("w-full text-xs")
                        else:
                            # Fallback: old format (just "error" field)
                            with ui.card().classes("w-full mt-2 bg-red-50 border-red-200"):
                                ui.markdown("### ❌ Error")
                                error_msg = error_data.get("error", str(error_data))
                                ui.label(error_msg).classes("text-red-700 font-mono text-sm")

                    except Exception:
                        # If parsing fails, show as plain text
                        with ui.card().classes("w-full mt-2 bg-red-50 border-red-200"):
                            ui.markdown("### ❌ Error")
                            ui.label(str(output_data)).classes("text-red-700 font-mono text-sm")

        # Main 2-pane layout (Execution Flow + Activity Details)
        with ui.row().style("width: 100%; height: calc(100vh - 250px); gap: 1rem; display: flex;"):
            # Left pane: Execution Flow
            with ui.column().style("flex: 1; overflow: auto; padding-right: 1rem;"):
                ui.markdown("## Execution Flow")
                ui.label("Click on an activity to view details →").classes(
                    "text-gray-600 text-sm mb-2"
                )

                if history:
                    # Get workflow source code for hybrid diagram
                    # Priority: 1) DB (instance.source_code), 2) Global registry (fallback)
                    workflow_name = instance.get("workflow_name")
                    source_code = instance.get("source_code")

                    # Fallback to global registry if DB doesn't have source code
                    if not source_code or source_code.startswith("# Source code not available"):
                        source_code = (
                            service.get_workflow_source(workflow_name) if workflow_name else None
                        )

                    if source_code:
                        # Generate hybrid diagram (static analysis + execution history)
                        mermaid_code = generate_hybrid_mermaid(
                            workflow_name,
                            instance_id,
                            history,
                            source_code,
                            compensations,
                            workflow_status=instance["status"],
                        )
                    else:
                        # Fallback to history-only diagram
                        mermaid_code = generate_interactive_mermaid(instance_id, history)

                    ui.mermaid(mermaid_code, config={"securityLevel": "loose"}).classes("w-full")
                else:
                    ui.label("No execution history available").classes("text-gray-500 italic")

            # Right pane: Activity Details
            with ui.column().style(
                "flex: 1; overflow: auto; padding: 1rem; background: #f9fafb; border-left: 2px solid #e5e7eb; border-radius: 0.5rem;"
            ):
                ui.markdown("## Activity Details")
                ui.label("Click on an activity in the diagram to view details").classes(
                    "text-gray-500 italic mb-4"
                )

                detail_container = ui.column().classes("w-full")
                detail_containers[instance_id] = detail_container

    # Register shutdown handler to clean up EddaApp resources
    async def shutdown_handler() -> None:
        """Clean up EddaApp resources on shutdown."""
        await edda_app.shutdown()

    app.on_shutdown(shutdown_handler)

    # Start server
    ui.run(port=port, title="Edda Workflow Viewer", reload=reload)
