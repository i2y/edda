"""
Edda Workflow Viewer - Interactive web interface for workflow visualization.

This application provides an interactive web interface to visualize
workflow instances with clickable Mermaid diagrams.

Installation:
    # Install Edda core only (viewer command available but won't run)
    pip install edda-framework

    # Install with viewer dependencies (recommended)
    uv sync --extra viewer
    # or
    pip install edda-framework[viewer]

Usage:
    # View existing workflows (no module imports)
    uv run edda-viewer

    # Import demo_app workflows
    uv run edda-viewer --import-module demo_app

    # Import custom workflows
    uv run edda-viewer -m my_workflows

    # Multiple modules
    uv run edda-viewer -m demo_app -m my_workflows

    # Custom database and port
    uv run edda-viewer --db my.db --port 9000 -m my_workflows

    # Using Just (project root)
    just viewer                              # No modules
    just viewer-demo                         # With demo_app
    just viewer demo.db 8080 "-m demo_app"  # Custom

Alternative (direct script execution):
    # Run the viewer script directly
    uv run python viewer_app.py -m demo_app

    # Or with auto-reload for development
    nicegui viewer_app.py --reload

Then open http://localhost:<port> in your browser.
"""

import argparse
import os
import sys

from edda import EddaApp
from edda.viewer_ui import start_viewer


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Edda Workflow Viewer - Interactive web interface for workflow visualization",
        epilog="Example: edda-viewer --db my.db --port 9000 -m demo_app -m my_workflows",
    )
    parser.add_argument(
        "--db",
        "-d",
        type=str,
        default="demo.db",
        help="Database file path (default: demo.db)",
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=8080,
        help="Port number for the web server (default: 8080)",
    )
    parser.add_argument(
        "--import-module",
        "-m",
        type=str,
        action="append",
        dest="import_modules",
        help="Python module to import (can be specified multiple times). "
        "Workflows decorated with @workflow in these modules will be available in the viewer. "
        "Example: --import-module demo_app",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the Edda Viewer CLI."""
    # Step 1: Check if nicegui is installed
    try:
        import nicegui  # noqa: F401
    except ImportError:
        print("Error: Viewer dependencies are not installed.", file=sys.stderr)
        print("", file=sys.stderr)
        print("Please install viewer dependencies with one of:", file=sys.stderr)
        print("  uv sync --extra viewer", file=sys.stderr)
        print("  uv pip install edda-framework[viewer]", file=sys.stderr)
        print("  pip install edda-framework[viewer]", file=sys.stderr)
        sys.exit(1)

    # Step 2: Parse arguments
    args = parse_args()

    # Step 3: Import specified modules
    if args.import_modules:
        print("Importing modules...")
        for module_name in args.import_modules:
            try:
                __import__(module_name)
                print(f"  ✓ Imported: {module_name}")
            except ImportError as e:
                print(
                    f"  ✗ Warning: Could not import module '{module_name}': {e}",
                    file=sys.stderr,
                )
                print("    Continuing without this module...", file=sys.stderr)
        print()

    # Step 4: Determine database URL (prioritize environment variable)
    db_url = os.getenv("EDDA_DB_URL")
    if db_url is None:
        # Fallback to --db option (SQLite)
        db_url = f"sqlite:///{args.db}"
        db_display = args.db
    else:
        # Hide password in display
        db_display = db_url.split("@")[-1] if "@" in db_url else db_url

    # Create Edda app (for database access only)
    edda_app = EddaApp(
        service_name="viewer",
        db_url=db_url,
    )

    # Step 5: Start the viewer (storage will be initialized on startup)
    print("Starting Edda Viewer...")
    print(f"  Database: {db_display}")
    print(f"  Port: {args.port}")
    print(f"  URL: http://localhost:{args.port}")
    print()

    start_viewer(edda_app, port=args.port)


if __name__ == "__main__":
    main()
