"""
Theme configuration for Edda Workflow Viewer.

Provides centralized color palette definitions and helper functions
for light and dark mode support.
"""

from typing import Any, cast

# Tailwind CSS compatible color palette
COLORS: dict[str, dict[str, Any]] = {
    "light": {
        # Background colors
        "bg_page": "#FFFFFF",
        "bg_surface": "#F8FAFC",  # Slate 50
        "bg_elevated": "#FFFFFF",
        "border": "#E2E8F0",  # Slate 200
        # Text colors
        "text_primary": "#0F172A",  # Slate 900
        "text_secondary": "#64748B",  # Slate 500
        "text_muted": "#94A3B8",  # Slate 400
        # Status colors (bg, stroke, text)
        "completed": {"bg": "#ECFDF5", "stroke": "#10B981", "text": "#065F46"},
        "running": {"bg": "#FEF3C7", "stroke": "#F59E0B", "text": "#92400E"},
        "failed": {"bg": "#FEE2E2", "stroke": "#EF4444", "text": "#991B1B"},
        "waiting_event": {"bg": "#DBEAFE", "stroke": "#3B82F6", "text": "#1E40AF"},
        "waiting_timer": {"bg": "#E0F2FE", "stroke": "#0EA5E9", "text": "#075985"},
        "cancelled": {"bg": "#FFEDD5", "stroke": "#F97316", "text": "#9A3412"},
        "compensating": {"bg": "#F3E8FF", "stroke": "#A855F7", "text": "#6B21A8"},
        "not_executed": {"bg": "#F1F5F9", "stroke": "#CBD5E1", "text": "#64748B"},
        "event_received": {"bg": "#CFFAFE", "stroke": "#06B6D4", "text": "#0E7490"},
        "compensation_failed": {"bg": "#FEE2E2", "stroke": "#B91C1C", "text": "#7F1D1D"},
        # Mermaid diagram specific
        "condition": {"bg": "#FEF3C7", "stroke": "#F59E0B"},
        "loop": {"bg": "#FDF4FF", "stroke": "#D946EF"},
        "match": {"bg": "#ECFDF5", "stroke": "#10B981"},
        "merge": {"bg": "#FFFFFF", "stroke": "#E2E8F0"},
        "edge_executed": "#10B981",
        "edge_not_executed": "#CBD5E1",
        "edge_compensation": "#A855F7",
    },
    "dark": {
        # Background colors
        "bg_page": "#0F172A",  # Slate 900
        "bg_surface": "#1E293B",  # Slate 800
        "bg_elevated": "#334155",  # Slate 700
        "border": "#475569",  # Slate 600
        # Text colors
        "text_primary": "#F1F5F9",  # Slate 100
        "text_secondary": "#94A3B8",  # Slate 400
        "text_muted": "#64748B",  # Slate 500
        # Status colors (bg, stroke, text)
        "completed": {"bg": "#064E3B", "stroke": "#34D399", "text": "#A7F3D0"},
        "running": {"bg": "#78350F", "stroke": "#FBBF24", "text": "#FDE68A"},
        "failed": {"bg": "#7F1D1D", "stroke": "#F87171", "text": "#FECACA"},
        "waiting_event": {"bg": "#1E3A8A", "stroke": "#60A5FA", "text": "#BFDBFE"},
        "waiting_timer": {"bg": "#0C4A6E", "stroke": "#38BDF8", "text": "#BAE6FD"},
        "cancelled": {"bg": "#7C2D12", "stroke": "#FB923C", "text": "#FED7AA"},
        "compensating": {"bg": "#581C87", "stroke": "#C084FC", "text": "#E9D5FF"},
        "not_executed": {"bg": "#334155", "stroke": "#64748B", "text": "#94A3B8"},
        "event_received": {"bg": "#164E63", "stroke": "#22D3EE", "text": "#A5F3FC"},
        "compensation_failed": {"bg": "#7F1D1D", "stroke": "#FCA5A5", "text": "#FECACA"},
        # Mermaid diagram specific
        "condition": {"bg": "#78350F", "stroke": "#FBBF24"},
        "loop": {"bg": "#4C1D95", "stroke": "#E879F9"},
        "match": {"bg": "#064E3B", "stroke": "#34D399"},
        "merge": {"bg": "#1E293B", "stroke": "#475569"},
        "edge_executed": "#34D399",
        "edge_not_executed": "#64748B",
        "edge_compensation": "#C084FC",
    },
}


def get_status_color(status: str, is_dark: bool) -> dict[str, str]:
    """
    Get color configuration for a workflow status.

    Args:
        status: Status name (e.g., "completed", "running", "failed")
        is_dark: Whether dark mode is enabled

    Returns:
        Dictionary with 'bg', 'stroke', and 'text' colors
    """
    theme = "dark" if is_dark else "light"
    status_key = status.lower().replace(" ", "_").replace("-", "_")

    # Handle special status mappings
    status_mapping = {
        "waiting": "waiting_event",
        "waiting_for_event": "waiting_event",
        "waiting_for_timer": "waiting_timer",
        "compensated": "compensating",
    }
    status_key = status_mapping.get(status_key, status_key)

    return cast(dict[str, str], COLORS[theme].get(status_key, COLORS[theme]["not_executed"]))


def get_mermaid_style(status: str, is_dark: bool) -> str:
    """
    Get Mermaid style string for a status.

    Args:
        status: Status name
        is_dark: Whether dark mode is enabled

    Returns:
        Mermaid style string (e.g., "fill:#ECFDF5,stroke:#10B981,stroke-width:2px")
    """
    colors = get_status_color(status, is_dark)
    return f"fill:{colors['bg']},stroke:{colors['stroke']},stroke-width:2px"


def get_mermaid_node_style(node_type: str, is_dark: bool) -> str:
    """
    Get Mermaid style for structural nodes (condition, loop, merge).

    Args:
        node_type: Node type (e.g., "condition", "loop", "merge")
        is_dark: Whether dark mode is enabled

    Returns:
        Mermaid style string
    """
    theme = "dark" if is_dark else "light"
    colors = COLORS[theme].get(node_type, COLORS[theme]["merge"])
    stroke_width = "1px" if node_type == "merge" else "2px"
    return f"fill:{colors['bg']},stroke:{colors['stroke']},stroke-width:{stroke_width}"


def get_edge_color(edge_type: str, is_dark: bool) -> str:
    """
    Get edge (arrow) color for Mermaid diagrams.

    Args:
        edge_type: Edge type ("executed", "not_executed", "compensation")
        is_dark: Whether dark mode is enabled

    Returns:
        Color hex code
    """
    theme = "dark" if is_dark else "light"
    key = f"edge_{edge_type}"
    return cast(str, COLORS[theme].get(key, COLORS[theme]["edge_not_executed"]))


# Tailwind CSS class mappings for UI components
# Note: Background colors are controlled via CSS in app.py for proper dark mode support
# NiceGUI adds 'dark' class to body, but Tailwind's dark: prefix expects it on html
TAILWIND_CLASSES = {
    "card": "border",  # Background handled by CSS
    "card_hover": "",  # Hover handled by CSS
    "surface": "",  # Background handled by CSS
    "text_primary": "text-slate-900 dark:text-slate-100",
    "text_secondary": "text-slate-500 dark:text-slate-400",
    "text_muted": "text-slate-400 dark:text-slate-500",
    "border": "border-slate-200 dark:border-slate-700",
    "input": "border-slate-300 dark:border-slate-600",  # Background handled by CSS
    "code_block": "border",  # Background handled by CSS
}

# Status badge Tailwind classes
STATUS_BADGE_CLASSES = {
    "completed": ("bg-emerald-50 text-emerald-700 dark:bg-emerald-900/50 dark:text-emerald-300"),
    "running": "bg-amber-50 text-amber-700 dark:bg-amber-900/50 dark:text-amber-300",
    "failed": "bg-red-50 text-red-700 dark:bg-red-900/50 dark:text-red-300",
    "waiting_event": "bg-blue-50 text-blue-700 dark:bg-blue-900/50 dark:text-blue-300",
    "waiting_timer": "bg-sky-50 text-sky-700 dark:bg-sky-900/50 dark:text-sky-300",
    "cancelled": ("bg-orange-50 text-orange-700 dark:bg-orange-900/50 dark:text-orange-300"),
    "compensating": ("bg-purple-50 text-purple-700 dark:bg-purple-900/50 dark:text-purple-300"),
    "not_executed": ("bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-400"),
}


def get_status_badge_classes(status: str) -> str:
    """
    Get Tailwind CSS classes for a status badge.

    Args:
        status: Status name

    Returns:
        Tailwind CSS class string
    """
    status_key = status.lower().replace(" ", "_").replace("-", "_")

    # Handle special status mappings
    status_mapping = {
        "waiting": "waiting_event",
        "waiting_for_event": "waiting_event",
        "waiting_for_timer": "waiting_timer",
        "compensated": "compensating",
    }
    status_key = status_mapping.get(status_key, status_key)

    base_classes = "px-3 py-1 rounded-full text-sm font-medium"
    status_classes = STATUS_BADGE_CLASSES.get(status_key, STATUS_BADGE_CLASSES["not_executed"])
    return f"{base_classes} {status_classes}"
