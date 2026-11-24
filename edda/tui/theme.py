"""Theme and color definitions for TUI viewer."""

from rich.style import Style
from rich.text import Text

# Status colors for terminal display
STATUS_COLORS: dict[str, str] = {
    "completed": "green",
    "running": "yellow",
    "failed": "red",
    "waiting_for_event": "blue",
    "waiting_for_timer": "cyan",
    "waiting_for_message": "blue",
    "compensating": "magenta",
    "cancelled": "bright_black",
    "recurred": "green",
    # Activity-specific statuses
    "compensated": "magenta",
    "compensation_failed": "red",
    "event_received": "green",
}

# Rich styles for status display
STATUS_STYLES: dict[str, Style] = {
    status: Style(color=color, bold=True) for status, color in STATUS_COLORS.items()
}

# Short status labels for table display
STATUS_LABELS: dict[str, str] = {
    "completed": "OK",
    "running": "RUN",
    "failed": "ERR",
    "waiting_for_event": "WAIT",
    "waiting_for_timer": "TIMER",
    "waiting_for_message": "MSG",
    "compensating": "COMP",
    "cancelled": "CNCL",
    "recurred": "RECUR",
    # Activity-specific
    "compensated": "COMP",
    "compensation_failed": "CERR",
    "event_received": "RECV",
}


def get_status_style(status: str) -> Style:
    """Get Rich style for a status."""
    return STATUS_STYLES.get(status, Style())


def get_status_color(status: str) -> str:
    """Get color name for a status."""
    return STATUS_COLORS.get(status, "white")


def get_status_label(status: str) -> str:
    """Get short label for a status."""
    return STATUS_LABELS.get(status, status.upper()[:4])


def format_status(status: str) -> Text:
    """Format status as colored Rich Text."""
    label = get_status_label(status)
    color = get_status_color(status)
    return Text(f"[{label}]", style=f"bold {color}")


def format_status_markup(status: str) -> str:
    """Format status as Rich markup string for DataTable."""
    label = get_status_label(status)
    color = get_status_color(status)
    return f"[bold {color}][{label}][/bold {color}]"
