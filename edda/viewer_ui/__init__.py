"""
NiceGUI-based workflow instance viewer for Edda framework.

This module provides an interactive web interface to visualize
workflow instances with clickable Mermaid diagrams.
"""

from typing import Any


# Lazy import to avoid NiceGUI dependency during testing
def __getattr__(name: str) -> Any:
    if name == "start_viewer":
        from edda.viewer_ui.app import start_viewer

        return start_viewer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["start_viewer"]
