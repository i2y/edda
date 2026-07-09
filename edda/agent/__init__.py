"""Process isolation for semi-trusted external programs.

``edda.agent`` runs an AI agent CLI, an MCP server, or any other program you do
not fully trust as an Edda activity - in its own OS process, behind an
OS-enforced sandbox.

This subpackage is entirely opt-in and imports nothing from ``edda`` beyond the
public core. Nothing in ``edda`` imports it back, so ``import edda`` is unchanged
by its presence and ``edda.agent`` could be extracted into its own distribution
without touching the core. That boundary is enforced by a test.

Install the optional dependency with ``pip install 'edda-framework[agent]'``.
Note that only the throwaway runner subprocess ever imports ``sandbox_runtime``;
the Edda worker does not. A missing sandbox therefore surfaces at call time as
:class:`~edda.agent.errors.SandboxUnavailableError`, not as an import error.

Example:
    >>> from edda import workflow, WorkflowContext
    >>> from edda.agent import port, DEFAULT_BROAD
    >>>
    >>> claude = port("claude -p --output-format json", sandbox=DEFAULT_BROAD, timeout=300)
    >>>
    >>> @workflow
    ... async def issue_driven(ctx: WorkflowContext, spec: str) -> str:
    ...     result = await claude(ctx, stdin=spec)
    ...     return result.stdout
"""

from edda.agent.errors import (
    PortConfigError,
    PortError,
    PortFailedError,
    PortTimeoutError,
    SandboxUnavailableError,
)
from edda.agent.port import (
    EXTERN_EVENT_TYPE,
    EXTERN_TAG,
    PortActivity,
    PortResult,
    port,
)
from edda.agent.sandbox import (
    DEFAULT_BROAD,
    READONLY,
    SECRET_PATHS,
    TIGHT,
    UNSANDBOXED,
    SandboxPolicy,
    UnsandboxedType,
    check_srt_shape,
)

__all__ = [
    "DEFAULT_BROAD",
    "EXTERN_EVENT_TYPE",
    "EXTERN_TAG",
    "READONLY",
    "SECRET_PATHS",
    "TIGHT",
    "UNSANDBOXED",
    "PortActivity",
    "PortConfigError",
    "PortError",
    "PortFailedError",
    "PortResult",
    "PortTimeoutError",
    "SandboxPolicy",
    "SandboxUnavailableError",
    "UnsandboxedType",
    "check_srt_shape",
    "port",
]
