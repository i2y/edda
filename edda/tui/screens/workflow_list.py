"""Workflow list screen for TUI viewer."""

from datetime import datetime
from typing import TYPE_CHECKING, Any

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label, LoadingIndicator

from edda.tui.theme import format_status, get_status_color

if TYPE_CHECKING:
    from edda.viewer_ui.data_service import WorkflowDataService


# Status filter cycle order
STATUS_FILTERS: list[str | None] = [
    None,  # All
    "running",
    "completed",
    "failed",
    "waiting_for_event",
    "waiting_for_timer",
    "waiting_for_message",
    "compensating",
    "cancelled",
]


class WorkflowListScreen(Screen[None]):
    """Screen showing list of workflow instances."""

    BINDINGS = [
        Binding("r", "refresh", "Refresh"),
        Binding("enter", "view_detail", "View Detail"),
        Binding("f", "cycle_filter", "Filter"),
        Binding("q", "quit", "Quit"),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
    ]

    DEFAULT_CSS = """
    WorkflowListScreen {
        layout: vertical;
    }

    #status-bar {
        height: 1;
        background: $surface;
        padding: 0 1;
    }

    #status-bar Label {
        margin-right: 2;
    }

    #filter-label {
        color: $text-muted;
    }

    #table-container {
        height: 1fr;
    }

    #workflow-table {
        height: 1fr;
        scrollbar-gutter: stable;
    }

    #workflow-table > .datatable--cursor {
        background: $accent;
    }

    #loading {
        align: center middle;
    }

    LoadingIndicator {
        height: 3;
    }
    """

    def __init__(
        self,
        data_service: "WorkflowDataService",
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        super().__init__(name=name, id=id, classes=classes)
        self.data_service = data_service
        self._instances: list[dict[str, Any]] = []
        self._status_filter_index = 0
        self._last_refresh: datetime | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="status-bar"):
            yield Label("Filter: All", id="filter-label")
            yield Label("", id="refresh-time")
        with Container(id="table-container"):
            yield LoadingIndicator(id="loading")
            table: DataTable[str] = DataTable(id="workflow-table", cursor_type="row")
            table.display = False
            yield table
        yield Footer()

    def on_mount(self) -> None:
        """Called when screen is mounted."""
        table: DataTable[str] = self.query_one("#workflow-table", DataTable)
        table.add_columns("Instance ID", "Workflow", "Status", "Started", "Updated")
        self.action_refresh()

    @property
    def current_filter(self) -> str | None:
        """Get current status filter."""
        return STATUS_FILTERS[self._status_filter_index]

    def action_refresh(self) -> None:
        """Refresh workflow list."""
        self.run_worker(self._load_data(), exclusive=True)

    async def _load_data(self) -> None:
        """Load workflow instances from storage."""
        loading = self.query_one("#loading", LoadingIndicator)
        table = self.query_one("#workflow-table", DataTable)

        loading.display = True
        table.display = False

        try:
            result = await self.data_service.get_instances_paginated(
                page_size=100,
                status_filter=self.current_filter,
            )
            self._instances = result.get("instances", [])
            self._last_refresh = datetime.now()
            self._populate_table()
        finally:
            loading.display = False
            table.display = True

        # Update refresh time
        refresh_label = self.query_one("#refresh-time", Label)
        if self._last_refresh:
            refresh_label.update(f"Last refresh: {self._last_refresh.strftime('%H:%M:%S')}")

        # Focus table after loading so it's immediately interactive
        table.focus()

    def _populate_table(self) -> None:
        """Populate table with workflow instances."""
        table = self.query_one("#workflow-table", DataTable)
        table.clear()

        for instance in self._instances:
            instance_id = instance.get("instance_id", "")
            workflow_name = instance.get("workflow_name", "")
            status = instance.get("status", "unknown")

            # Format timestamps
            started_at = instance.get("started_at")
            updated_at = instance.get("updated_at")

            started_str = self._format_datetime(started_at)
            updated_str = self._format_datetime(updated_at)

            # Truncate instance ID for display
            display_id = instance_id[:12] + "..." if len(instance_id) > 15 else instance_id

            # Format status with color
            status_text = format_status(status)

            table.add_row(
                display_id,
                workflow_name,
                status_text,
                started_str,
                updated_str,
                key=instance_id,
            )

    def _format_datetime(self, dt: datetime | str | None) -> str:
        """Format datetime for display."""
        if dt is None:
            return "-"
        if isinstance(dt, str):
            try:
                parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
                return parsed.strftime("%Y-%m-%d %H:%M")
            except ValueError:
                return dt
        return dt.strftime("%Y-%m-%d %H:%M")

    def action_view_detail(self) -> None:
        """View selected workflow detail."""
        table: DataTable[str] = self.query_one("#workflow-table", DataTable)
        if table.row_count == 0:
            return

        # Get the row key (instance_id) for the current cursor row
        cursor_row = table.cursor_row
        if cursor_row < len(self._instances):
            instance_id = self._instances[cursor_row].get("instance_id", "")
            if instance_id:
                self._navigate_to_detail(instance_id)

    def _navigate_to_detail(self, instance_id: str) -> None:
        """Navigate to workflow detail screen."""
        from edda.tui.screens.workflow_detail import WorkflowDetailScreen

        self.app.push_screen(WorkflowDetailScreen(self.data_service, instance_id))

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle row selection (mouse click or Enter key)."""
        # Get the row index from the event
        row_index = event.cursor_row
        if row_index < len(self._instances):
            instance_id = self._instances[row_index].get("instance_id", "")
            if instance_id:
                self._navigate_to_detail(instance_id)

    def action_cycle_filter(self) -> None:
        """Cycle through status filters."""
        self._status_filter_index = (self._status_filter_index + 1) % len(STATUS_FILTERS)
        current = self.current_filter

        # Update filter label
        filter_label = self.query_one("#filter-label", Label)
        if current is None:
            filter_label.update("Filter: All")
        else:
            color = get_status_color(current)
            filter_label.update(Text.assemble("Filter: ", (current, f"bold {color}")))

        # Refresh data with new filter
        self.action_refresh()

    def action_cursor_down(self) -> None:
        """Move cursor down (vim-style)."""
        table = self.query_one("#workflow-table", DataTable)
        table.action_cursor_down()

    def action_cursor_up(self) -> None:
        """Move cursor up (vim-style)."""
        table = self.query_one("#workflow-table", DataTable)
        table.action_cursor_up()

    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()
