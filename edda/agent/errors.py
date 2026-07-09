"""Exceptions raised by :mod:`edda.agent`.

The hierarchy is designed around Edda's retry semantics:

- Plain :class:`PortError` subclasses are ordinary exceptions, so the activity's
  retry policy decides whether to retry them.
- Errors that also inherit :class:`edda.exceptions.TerminalError` are never
  retried (see :meth:`edda.retry.RetryPolicy.is_retryable`). Retrying will not
  install ``bubblewrap`` and will not fix an invalid sandbox configuration.

Note:
    Errors cached in the workflow history are re-raised on replay as a generic
    ``Exception("PortFailedError: ...")`` - this is framework-wide Edda behaviour,
    not specific to ports. Do not ``isinstance``-match port errors in workflow
    code that must survive replay.
"""

from __future__ import annotations

from edda.exceptions import TerminalError

__all__ = [
    "PortConfigError",
    "PortError",
    "PortFailedError",
    "PortTimeoutError",
    "SandboxUnavailableError",
]


class PortError(Exception):
    """Base class for every port failure.

    Raised directly when the runner subprocess dies without producing a result
    or speaks a protocol the parent cannot understand. Retryable by default.
    """


class PortFailedError(PortError):
    """The external program exited with a non-zero status while ``check=True``.

    Retryable, which is what lets a plain ``RetryPolicy`` absorb rate limits and
    other transient CLI failures.

    Attributes:
        exit_code: Exit status of the external program. Negative values are
            ``-signal`` (e.g. ``-9`` for ``SIGKILL``).
        stdout_tail: Tail of the program's stdout, for diagnostics.
        stderr_tail: Tail of the program's stderr, for diagnostics.
    """

    def __init__(
        self,
        message: str,
        *,
        exit_code: int,
        stdout_tail: str = "",
        stderr_tail: str = "",
    ) -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.stdout_tail = stdout_tail
        self.stderr_tail = stderr_tail


class PortTimeoutError(PortError):
    """The external program did not finish within ``timeout`` seconds.

    The whole process tree is killed before this is raised. Retryable, but note
    that the default :class:`~edda.retry.RetryPolicy` budget (``max_duration``
    of 300s) is already spent by a single default-length timeout.
    """


class SandboxUnavailableError(PortError, TerminalError):
    """OS-level sandboxing was requested but cannot be enforced.

    Raised when ``sandbox-runtime`` is not installed, its system dependencies
    (``rg``, and on Linux ``bwrap`` and ``socat``) are missing, or the platform
    is unsupported. The external program is **not** executed.

    This is a :class:`~edda.exceptions.TerminalError`: retrying cannot install a
    missing binary. Pass ``sandbox=UNSANDBOXED`` to run with process isolation
    only, accepting the loss of the second wall.
    """


class PortConfigError(PortError, TerminalError):
    """The port or sandbox configuration is invalid.

    Raised when the sandbox policy is rejected by ``sandbox-runtime``'s
    validation, when the runner is asked for an unknown backend, or when the
    host platform cannot support the process-isolation guarantees port relies on.

    This is a :class:`~edda.exceptions.TerminalError`: a bad domain pattern will
    still be bad on the next attempt.
    """
