"""
Textual-based TUI workflow viewer for Edda framework.

This module provides a terminal user interface to visualize
workflow instances and their execution history.
"""

from typing import Any


# Lazy import to avoid Textual dependency during testing
def __getattr__(name: str) -> Any:
    if name == "start_tui":
        from edda.tui.app import start_tui

        return start_tui
    if name == "main":
        from edda.tui.app import main

        return main
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["start_tui", "main"]
