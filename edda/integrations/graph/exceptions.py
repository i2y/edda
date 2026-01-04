"""Exceptions for durable graph integration."""


class GraphExecutionError(Exception):
    """Raised when a graph node execution fails."""

    def __init__(self, message: str, node_name: str | None = None) -> None:
        self.node_name = node_name
        super().__init__(message)
