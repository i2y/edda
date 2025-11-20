"""
Edda framework exceptions.

This module defines custom exception classes used throughout the framework.
"""


class WorkflowCancelledException(Exception):
    """
    Raised when a workflow has been cancelled.

    This exception is raised when an activity attempts to execute after
    the workflow has been cancelled by a user or external system.
    """

    pass
