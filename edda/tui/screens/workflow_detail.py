"""Workflow detail screen for TUI viewer."""

# ruff: noqa: SIM117

import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, VerticalScroll
from textual.screen import Screen
from textual.widgets import (
    DataTable,
    Footer,
    Label,
    Static,
    TabbedContent,
    TabPane,
)

from edda.tui.theme import format_status, get_status_color
from edda.tui.widgets.json_viewer import JSONViewer

if TYPE_CHECKING:
    from edda.viewer_ui.data_service import WorkflowDataService


class WorkflowDetailScreen(Screen[None]):
    """Screen showing workflow instance details."""

    BINDINGS = [
        Binding("escape", "go_back", "Back"),
        Binding("backspace", "go_back", "Back", show=False),
        Binding("r", "refresh", "Refresh"),
        Binding("q", "quit", "Quit"),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
    ]

    DEFAULT_CSS = """
    WorkflowDetailScreen {
        layout: vertical;
    }

    #info-bar {
        height: 3;
        padding: 0 1;
        background: $surface;
        border-bottom: solid $primary;
    }

    #workflow-name {
        text-style: bold;
    }

    #info-row {
        height: 1;
    }

    #info-row Label {
        margin-right: 2;
    }

    #tabs {
        height: 1fr;
    }

    TabbedContent ContentSwitcher {
        height: 1fr;
    }

    /* History pane */
    #history-scroll {
        height: 1fr;
    }

    #history-table {
        height: auto;
        max-height: 50%;
        scrollbar-gutter: stable;
    }

    #history-table > .datatable--cursor {
        background: $accent;
    }

    #activity-detail {
        height: auto;
        max-height: 10;
        border-top: solid $primary;
        padding: 1;
        margin-top: 1;
    }

    #activity-detail-title {
        text-style: bold;
        margin-bottom: 1;
    }

    .json-section {
        margin-top: 1;
    }

    /* Input/Output pane */
    #io-scroll {
        height: 1fr;
    }

    .section-title {
        text-style: bold;
        color: $text-muted;
        margin-top: 1;
        margin-bottom: 1;
    }

    JSONViewer {
        height: auto;
        max-height: 10;
        margin-bottom: 1;
        padding: 1;
        background: $surface;
        border: solid $primary;
    }

    /* Compensations pane */
    #compensations-list {
        height: 1fr;
    }

    #comp-content {
        height: auto;
    }
    """

    def __init__(
        self,
        data_service: "WorkflowDataService",
        instance_id: str,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        super().__init__(name=name, id=id, classes=classes)
        self.data_service = data_service
        self.instance_id = instance_id
        self._instance: dict[str, Any] | None = None
        self._history: list[dict[str, Any]] = []
        self._compensations: dict[str, Any] = {}
        self._selected_activity: dict[str, Any] | None = None

    def compose(self) -> ComposeResult:
        with Container(id="info-bar"):
            yield Label("Loading...", id="workflow-name")
            with Horizontal(id="info-row"):
                yield Label("", id="status-label")
                yield Label("", id="instance-id-label")
                yield Label("", id="started-label")

        with TabbedContent(id="tabs"):
            with TabPane("History", id="history-pane"):
                with VerticalScroll(id="history-scroll"):
                    yield DataTable(id="history-table", cursor_type="row")
                    with VerticalScroll(id="activity-detail"):
                        yield Label("Select a row to view details", id="activity-detail-title")
                        yield Label("Input:", classes="section-title")
                        yield JSONViewer(id="activity-input", classes="json-section")
                        yield Label("Output:", classes="section-title")
                        yield JSONViewer(id="activity-output", classes="json-section")

            with TabPane("Input/Output", id="io-pane"):
                with VerticalScroll(id="io-scroll"):
                    yield Label("Workflow Input:", classes="section-title")
                    yield JSONViewer(id="workflow-input")
                    yield Label("Workflow Output:", classes="section-title")
                    yield JSONViewer(id="workflow-output")

            with TabPane("Compensations", id="comp-pane"):
                with VerticalScroll(id="compensations-list"):
                    yield Static("No compensations registered", id="comp-content")

        yield Footer()

    def on_mount(self) -> None:
        """Called when screen is mounted."""
        # Setup history table columns
        table = self.query_one("#history-table", DataTable)
        table.add_columns("Activity", "Status", "Executed At")

        # Hide activity detail initially
        detail = self.query_one("#activity-detail", VerticalScroll)
        detail.display = False

        self.action_refresh()

    def get_title(self) -> str:
        """Get screen title."""
        if self._instance:
            return f"Workflow: {self._instance.get('workflow_name', 'unknown')}"
        return "Workflow Detail"

    def action_refresh(self) -> None:
        """Refresh workflow detail."""
        self.run_worker(self._load_data(), exclusive=True)

    async def _load_data(self) -> None:
        """Load workflow detail from storage."""
        result = await self.data_service.get_instance_detail(self.instance_id)
        self._instance = result.get("instance")
        self._history = result.get("history", [])
        self._compensations = result.get("compensations", {})
        self._update_ui()

        # Focus history table after loading
        table = self.query_one("#history-table", DataTable)
        table.focus()

    def _update_ui(self) -> None:
        """Update UI with loaded data."""
        if not self._instance:
            name_label = self.query_one("#workflow-name", Label)
            name_label.update("Workflow not found")
            return

        # Update info bar
        name_label = self.query_one("#workflow-name", Label)
        name_label.update(self._instance.get("workflow_name", "unknown"))

        status = self._instance.get("status", "unknown")
        status_label = self.query_one("#status-label", Label)
        color = get_status_color(status)
        status_label.update(Text.assemble("Status: ", (status, f"bold {color}")))

        instance_label = self.query_one("#instance-id-label", Label)
        instance_label.update(f"ID: {self.instance_id[:20]}...")

        started_at = self._instance.get("started_at")
        started_label = self.query_one("#started-label", Label)
        started_label.update(f"Started: {self._format_datetime(started_at)}")

        # Populate history table
        self._populate_history_table()

        # Update input/output
        input_data = self._instance.get("input_data")
        output_data = self._instance.get("output_data")

        input_viewer = self.query_one("#workflow-input", JSONViewer)
        output_viewer = self.query_one("#workflow-output", JSONViewer)

        input_viewer.update_data(input_data)
        output_viewer.update_data(output_data)

        # Update compensations
        self._update_compensations()

    def _populate_history_table(self) -> None:
        """Populate history table."""
        table = self.query_one("#history-table", DataTable)
        table.clear()

        for entry in self._history:
            activity_name = entry.get("activity_name", "unknown")
            status = entry.get("status", "unknown")
            executed_at = entry.get("executed_at")

            status_text = format_status(status)
            executed_str = self._format_datetime(executed_at)

            activity_id = entry.get("activity_id", "")
            table.add_row(activity_name, status_text, executed_str, key=activity_id)

    def _update_compensations(self) -> None:
        """Update compensations display."""
        content = self.query_one("#comp-content", Static)

        if not self._compensations:
            content.update("No compensations registered")
            return

        lines = []
        for activity_id, comp in self._compensations.items():
            activity_name = comp.get("activity_name", "unknown")
            args = comp.get("args", {})
            lines.append(f"[bold]{activity_name}[/bold] ({activity_id})")
            if args:
                args_str = json.dumps(args, indent=2, ensure_ascii=False)
                lines.append(f"  Args: {args_str}")
            lines.append("")

        content.update("\n".join(lines))

    def _format_datetime(self, dt: datetime | str | None) -> str:
        """Format datetime for display."""
        if dt is None:
            return "-"
        if isinstance(dt, str):
            try:
                parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
                return parsed.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return dt
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle row selection in history table."""
        if event.data_table.id != "history-table":
            return

        row_key = event.row_key
        if row_key is None:
            return

        activity_id = str(row_key.value)

        # Find activity in history
        for entry in self._history:
            if entry.get("activity_id") == activity_id:
                self._show_activity_detail(entry)
                break

    def _show_activity_detail(self, entry: dict[str, Any]) -> None:
        """Show activity detail panel."""
        self._selected_activity = entry

        detail = self.query_one("#activity-detail", VerticalScroll)
        detail.display = True

        title = self.query_one("#activity-detail-title", Label)
        activity_name = entry.get("activity_name", "unknown")
        status = entry.get("status", "unknown")
        color = get_status_color(status)
        title.update(
            Text.assemble(
                "Activity: ",
                (activity_name, "bold"),
                "  Status: ",
                (status, f"bold {color}"),
            )
        )

        # Parse input/output from JSON strings
        input_viewer = self.query_one("#activity-input", JSONViewer)
        output_viewer = self.query_one("#activity-output", JSONViewer)

        input_data = entry.get("input_data")
        output_data = entry.get("output_data")

        # Parse if string
        if isinstance(input_data, str):
            try:
                input_data = json.loads(input_data)
            except json.JSONDecodeError:
                input_data = {"raw": input_data}

        if isinstance(output_data, str):
            try:
                output_data = json.loads(output_data)
            except json.JSONDecodeError:
                output_data = {"raw": output_data}

        input_viewer.update_data(input_data)
        output_viewer.update_data(output_data)

        # Show error if present
        error = entry.get("error")
        if error:
            output_viewer.update_data({"error": error, "error_type": entry.get("error_type")})

    def action_go_back(self) -> None:
        """Go back to list screen."""
        self.app.pop_screen()

    def action_cursor_down(self) -> None:
        """Move cursor down (vim-style)."""
        table = self.query_one("#history-table", DataTable)
        table.action_cursor_down()

    def action_cursor_up(self) -> None:
        """Move cursor up (vim-style)."""
        table = self.query_one("#history-table", DataTable)
        table.action_cursor_up()

    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit()
