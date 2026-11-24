"""JSON viewer widget for TUI."""

import json
from typing import Any

from rich.syntax import Syntax
from textual.widgets import Static


class JSONViewer(Static):
    """Widget to display formatted JSON with syntax highlighting."""

    DEFAULT_CSS = """
    JSONViewer {
        padding: 1;
        background: $surface;
        border: solid $primary;
    }
    """

    def __init__(
        self,
        data: dict[str, Any] | list[Any] | None = None,
        *,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        super().__init__(name=name, id=id, classes=classes)
        self._data = data

    def on_mount(self) -> None:
        """Called when widget is mounted."""
        self._render_json()

    def update_data(self, data: dict[str, Any] | list[Any] | None) -> None:
        """Update the displayed JSON data."""
        self._data = data
        self._render_json()

    def _render_json(self) -> None:
        """Render JSON data with syntax highlighting."""
        if self._data is None:
            self.update("(no data)")
            return

        try:
            formatted = json.dumps(self._data, indent=2, ensure_ascii=False, default=str)
            syntax = Syntax(
                formatted,
                "json",
                theme="monokai",
                line_numbers=False,
                word_wrap=True,
            )
            self.update(syntax)
        except (TypeError, ValueError) as e:
            self.update(f"(error formatting JSON: {e})")
