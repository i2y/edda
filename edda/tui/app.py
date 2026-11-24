"""Main TUI application for Edda workflow viewer."""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import TYPE_CHECKING, Any

from textual.app import App

from edda.tui.screens.workflow_list import WorkflowListScreen

if TYPE_CHECKING:
    from edda.viewer_ui.data_service import WorkflowDataService


class WorkflowTUIApp(App[None]):
    """Edda Workflow Viewer TUI Application."""

    TITLE = "Edda TUI Viewer"
    CSS = """
    Screen {
        background: $surface;
    }
    """

    def __init__(self, data_service: WorkflowDataService) -> None:
        super().__init__()
        self.data_service = data_service

    def on_mount(self) -> None:
        """Called when app is mounted."""
        self.push_screen(WorkflowListScreen(self.data_service))


def start_tui(edda_app: Any) -> None:
    """
    Start the TUI viewer.

    Args:
        edda_app: EddaApp instance with configured storage
    """
    from edda.viewer_ui.data_service import WorkflowDataService

    async def init_and_run() -> None:
        await edda_app.storage.initialize()
        service = WorkflowDataService(edda_app.storage)
        app = WorkflowTUIApp(service)
        await app.run_async()

    asyncio.run(init_and_run())


def main() -> None:
    """CLI entry point for edda-tui command."""
    # Check textual is installed
    try:
        import textual  # noqa: F401
    except ImportError:
        print("Error: TUI dependencies are not installed.", file=sys.stderr)
        print("Please install with: uv sync --extra tui", file=sys.stderr)
        sys.exit(1)

    # Parse arguments
    parser = argparse.ArgumentParser(
        description="Edda Workflow Viewer TUI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  edda-tui --db demo.db
  edda-tui -d myapp.db -m myapp.workflows
        """,
    )
    parser.add_argument(
        "--db",
        "-d",
        default="demo.db",
        help="SQLite database file path (default: demo.db)",
    )
    parser.add_argument(
        "-m",
        "--import-module",
        action="append",
        dest="import_modules",
        help="Python module to import (for workflow registration)",
    )
    args = parser.parse_args()

    # Import specified modules (for workflow registration)
    if args.import_modules:
        for module_name in args.import_modules:
            try:
                __import__(module_name)
            except ImportError as e:
                print(f"Warning: Could not import {module_name}: {e}", file=sys.stderr)

    # Create EddaApp with storage
    from edda import EddaApp

    edda_app = EddaApp(
        service_name="edda-tui",
        db_url=f"sqlite:///{args.db}",
    )

    start_tui(edda_app)


if __name__ == "__main__":
    main()
