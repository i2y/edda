"""Exceptions for LlamaIndex Workflow integration."""


class WorkflowExecutionError(Exception):
    """Error during workflow execution."""

    def __init__(self, message: str, step_name: str | None = None) -> None:
        self.step_name = step_name
        super().__init__(message)


class WorkflowReplayError(Exception):
    """Error during workflow replay."""

    pass
