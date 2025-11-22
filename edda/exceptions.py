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


class TerminalError(Exception):
    """
    Raised to indicate a non-retryable error.

    Activities can raise this exception to immediately stop retry attempts.
    This is useful for errors that will never succeed (e.g., invalid input,
    authorization failure, resource not found).

    The original exception is accessible via the __cause__ attribute.

    Example:
        @activity
        async def validate_user(ctx: WorkflowContext, user_id: str):
            user = await fetch_user(user_id)
            if not user:
                # Don't retry - user doesn't exist
                raise TerminalError(f"User {user_id} not found")
            return user
    """

    pass


class RetryExhaustedError(Exception):
    """
    Raised when an activity exhausts all retry attempts.

    The original exception can be accessed via the __cause__ attribute.

    Attributes:
        __cause__: The original exception (from the last retry attempt)

    Example:
        try:
            await my_activity(ctx)
        except RetryExhaustedError as e:
            # Error message: "Activity failed after 5 attempts: ..."
            print(f"Retries exhausted: {e}")

            # Access original error via __cause__
            original_error = e.__cause__
            if isinstance(original_error, NetworkError):
                logger.error("Network issue detected")
    """

    pass
