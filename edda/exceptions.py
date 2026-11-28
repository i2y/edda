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


class FSConflictError(Exception):
    """Optimistic-concurrency conflict on the shared actor filesystem."""


class ChannelPermissionError(Exception):
    """A sealed channel operation lacked a valid capability token."""


class SandboxedError(Exception):
    """An operation forbidden to a sandboxed workflow was attempted."""


class FSForbiddenError(Exception):
    """Permission denied on the shared actor filesystem."""


class FSOutsideActivityError(Exception):
    """FS operation attempted outside an activity body or during replay."""


class TaskConflictError(Exception):
    """Task state transition lost the race (already claimed/completed)."""


class TaskNotFoundError(Exception):
    """Task ID does not exist."""


class ActorNameTakenError(ValueError):
    """An actor name is already bound to another instance."""


class NotOwnerError(Exception):
    """An ownership-scoped operation was attempted by a non-owner."""


class CursorNotFoundError(Exception):
    """A channel cursor does not refer to an existing message."""


class InvalidArgumentError(ValueError):
    """A kernel primitive received an invalid argument."""


class AppNotRunningError(Exception):
    """The app refuses an incoming event: it is not initialized, or it shuts
    down. The sender sends the event again (HTTP 503)."""


class RunAbortedError(BaseException):
    """The current workflow run must stop without a recorded outcome.

    Unexpected primitive/storage failures are not workflow outcomes. This
    exception derives from BaseException so ordinary ``except Exception``
    handlers do not swallow it, and carries the original error as __cause__.
    """

    def __init__(self, original_error: BaseException):
        self.original_error = original_error
        self.__cause__ = original_error
        super().__init__(f"workflow run aborted: {original_error}")


_KERNEL_ERROR_CODES: dict[type[BaseException], str] = {
    TaskConflictError: "task_conflict",
    TaskNotFoundError: "task_not_found",
    ActorNameTakenError: "actor_name_taken",
    FSConflictError: "fs_conflict",
    SandboxedError: "sandboxed",
    ChannelPermissionError: "channel_forbidden",
    NotOwnerError: "not_owner",
    CursorNotFoundError: "cursor_not_found",
    InvalidArgumentError: "invalid_argument",
}

_KERNEL_ERRORS_BY_CODE = {code: cls for cls, code in _KERNEL_ERROR_CODES.items()}


def kernel_error_code(error: BaseException) -> str | None:
    """Return the stable cross-port code for a kernel-defined error."""
    for cls, code in _KERNEL_ERROR_CODES.items():
        if isinstance(error, cls):
            return code
    return None


def kernel_error_class(code: str | None) -> type[BaseException] | None:
    """Return the edda class for a stable cross-port kernel error code."""
    if code is None:
        return None
    return _KERNEL_ERRORS_BY_CODE.get(code)
