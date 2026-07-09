"""Sandbox policies for :mod:`edda.agent`.

A :class:`SandboxPolicy` is plain, immutable data. It is rendered into the
configuration dict understood by `sandbox-runtime`_ (srt) and handed to the
runner subprocess, which is the only process that ever imports srt.

srt is *allowlist-first* where it matters:

- **Network** is denied by default. ``allowed_domains`` opens specific hosts.
  srt's own validator rejects ``"*"`` and ``"*.com"``, so "allow the whole
  internet" is not expressible - that property is deliberately preserved here.
- **Writes** are denied by default. ``allow_write`` opens specific paths.
- **Reads** are allowed by default; ``deny_read`` closes specific paths. This is
  srt's model, not ours, and it is why every preset carries :data:`SECRET_PATHS`.

.. _sandbox-runtime: https://github.com/anthropic-experimental/sandbox-runtime
"""

from __future__ import annotations

import copy
import dataclasses
import os
import re
from collections.abc import Mapping
from typing import Any, ClassVar, Final, final

__all__ = [
    "DEFAULT_BROAD",
    "READONLY",
    "SECRET_PATHS",
    "TIGHT",
    "UNSANDBOXED",
    "SandboxPolicy",
    "UnsandboxedType",
    "check_srt_shape",
]

# Top-level keys a raw passthrough dict may carry. A deliberate subset of srt's
# ``SandboxRuntimeConfig`` schema: only a shallow shape check is possible in the
# parent process, which cannot import srt, so anything that could subvert the
# sandbox rather than merely restrict it is refused here.
_SRT_PASSTHROUGH_KEYS: Final[frozenset[str]] = frozenset(
    {
        "network",
        "filesystem",
        "ignore_violations",
        "enable_weaker_nested_sandbox",
        "mandatory_deny_search_depth",
        "allow_pty",
        "resource_limits",
    }
)

# ``ripgrep.command`` is executed as a binary - unsandboxed, with the worker's
# full authority - while srt builds the Linux profile. A "sandbox restriction"
# dict must never be able to smuggle in an executable, so this key is refused
# even though srt itself accepts it. srt's own default (``rg`` on PATH) is used.
_SRT_FORBIDDEN_KEYS: Final[frozenset[str]] = frozenset({"ripgrep"})

_GLOB_CHARS: Final[str] = "*?["
_TOKEN_RE: Final[re.Pattern[str]] = re.compile(r"\{cwd\}|\{home\}")

#: Credential stores denied for reading by every preset.
#:
#: Literal paths only: srt supports git-style globs on macOS but **silently
#: drops glob entries on Linux**, so a glob-based policy would be weaker on
#: Linux than on macOS. Literal paths are equally strong on both.
#:
#: ``~/.claude`` is deliberately absent. A sandboxed ``claude`` CLI must read
#: its own credentials and settings to run at all; an agent can always read the
#: credentials it is executing with.
SECRET_PATHS: Final[tuple[str, ...]] = (
    "{home}/.ssh",  # SSH private keys
    "{home}/.aws",  # AWS credentials
    "{home}/.gnupg",  # GPG private keys
    "{home}/.config/gcloud",  # GCP credentials
    "{home}/.kube",  # kubeconfigs (cluster credentials)
    "{home}/.netrc",  # plaintext passwords for curl/git/ftp
    "{home}/.pypirc",  # PyPI upload tokens
    "{home}/.docker",  # container registry auth
    "{home}/.config/gh",  # GitHub CLI OAuth token
)


def _resolve_path(path: str, cwd: str) -> str:
    """Resolve a policy path token into an absolute path.

    ``{cwd}`` and ``{home}`` are substituted, ``~`` is expanded, relative paths
    are anchored at ``cwd``, and symlinks are resolved (matching what srt does
    internally, so the profile and the running process agree on macOS where
    ``/tmp`` and ``/var`` are symlinks).

    Tokens are substituted in a single pass, so a token that appears inside a
    substituted value (e.g. a ``cwd`` that itself contains ``{home}``) is left
    alone rather than re-expanded. A regex is used rather than ``str.format`` so
    that stray braces in a real path do not raise.

    Args:
        path: Policy path, possibly containing ``{cwd}``/``{home}``/``~``.
        cwd: Absolute working directory of the sandboxed process.

    Returns:
        An absolute path. Glob patterns keep their wildcards.
    """
    tokens = {"{cwd}": cwd, "{home}": os.path.expanduser("~")}
    expanded = _TOKEN_RE.sub(lambda match: tokens[match.group(0)], path)
    expanded = os.path.expanduser(expanded)
    if not os.path.isabs(expanded):
        expanded = os.path.join(cwd, expanded)
    if any(char in expanded for char in _GLOB_CHARS):
        # realpath() would mangle wildcards; normalise only.
        return os.path.abspath(expanded)
    return os.path.realpath(expanded)


@dataclasses.dataclass(frozen=True, slots=True)
class SandboxPolicy:
    """An OS-enforced filesystem and network policy for a port.

    Paths may contain the tokens ``{cwd}`` (the port's working directory) and
    ``{home}``, resolved when the policy is rendered for a call.

    Attributes:
        allow_write: Paths the program may write to. Empty means "no writes to
            your files" - but see the note below.
        deny_read: Paths the program may not read.
        deny_write: Paths the program may not write, even inside ``allow_write``.
        allowed_domains: Hosts the program may reach. Empty means no network at
            all. Wildcards must be of the form ``*.example.com``.
        denied_domains: Hosts to reject, checked before ``allowed_domains``.
        allow_local_binding: Whether the program may bind local ports.

    Note:
        ``allow_write`` is not the whole story, and the extra writable paths come
        from srt, not from here:

        - srt unions its own default write paths into every profile:
          ``/dev/null``, ``/dev/stdout``, ``/dev/stderr``, ``/dev/tty``,
          ``/tmp/claude``, ``/private/tmp/claude``, ``~/.npm/_logs`` and
          ``~/.claude/debug``. It also forces ``TMPDIR=/tmp/claude`` for the
          sandboxed process.
        - **On macOS only**, whenever any write restriction is in force, srt also
          allows writes anywhere under the *parent* of the current ``TMPDIR`` when
          that looks like ``/var/folders/XX/YYY/T`` - i.e. the whole per-user
          temporary and cache tree. The scope is taken from the environment of the
          process that builds the profile, which for a port is its runner.

        So even ``READONLY`` permits those device, scratch and cache writes.
        Everything that matters - your project, your home directory, ``/etc`` -
        stays read-only unless you list it.

    Note:
        Every allowed domain is also an exfiltration channel - data can leave in
        a URL or a request body. Prefer :data:`TIGHT` or :data:`READONLY` when
        the program does not need a broad allowlist.
    """

    allow_write: tuple[str, ...] = ()
    deny_read: tuple[str, ...] = ()
    deny_write: tuple[str, ...] = ()
    allowed_domains: tuple[str, ...] = ()
    denied_domains: tuple[str, ...] = ()
    allow_local_binding: bool = False

    def __post_init__(self) -> None:
        """Reject the one policy that would defeat srt's allowlist model.

        Raises:
            ValueError: If ``"*"`` appears in ``allowed_domains``.
        """
        if "*" in self.allowed_domains:
            raise ValueError(
                "sandbox-runtime is deny-by-default for network access; allowing "
                'the whole internet with "*" is not supported. List the domains '
                "the program actually needs, or use wildcards like '*.example.com'."
            )

    def to_srt_config(self, cwd: str) -> dict[str, Any]:
        """Render this policy as a ``sandbox-runtime`` configuration dict.

        Args:
            cwd: Absolute working directory of the sandboxed process, used to
                resolve the ``{cwd}`` token.

        Returns:
            A plain dict matching srt's ``SandboxRuntimeConfig`` schema. It is
            validated by the runner subprocess, which is the only process that
            can import srt.
        """
        return {
            "network": {
                "allowed_domains": list(self.allowed_domains),
                "denied_domains": list(self.denied_domains),
                "allow_local_binding": self.allow_local_binding,
            },
            "filesystem": {
                "deny_read": [_resolve_path(p, cwd) for p in self.deny_read],
                "allow_write": [_resolve_path(p, cwd) for p in self.allow_write],
                "deny_write": [_resolve_path(p, cwd) for p in self.deny_write],
            },
        }

    def without_network(self) -> SandboxPolicy:
        """Return a copy with every domain revoked.

        Returns:
            A policy identical to this one but with no network access.
        """
        return dataclasses.replace(self, allowed_domains=())

    def deny(self, *paths: str) -> SandboxPolicy:
        """Return a copy that additionally denies reading and writing ``paths``.

        Args:
            *paths: Paths to deny. May contain ``{cwd}``/``{home}`` tokens.

        Returns:
            A strictly tighter policy.
        """
        return dataclasses.replace(
            self,
            deny_read=(*self.deny_read, *paths),
            deny_write=(*self.deny_write, *paths),
        )


@final
class UnsandboxedType:
    """Type of the :data:`UNSANDBOXED` sentinel.

    A dedicated singleton type rather than ``None`` or ``False``: it cannot be
    produced by accident, it survives a truthiness test, it forces mypy to make
    callers handle it explicitly, and - most importantly - ``sandbox=UNSANDBOXED``
    is impossible to miss in code review.
    """

    __slots__ = ()

    _instance: ClassVar[UnsandboxedType | None] = None

    def __new__(cls) -> UnsandboxedType:
        """Return the single instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        """Return the sentinel's canonical name."""
        return "UNSANDBOXED"


#: Run the program with process isolation only - no OS-enforced sandbox.
#:
#: The first wall (a separate, killable OS process) still stands. The second
#: wall (filesystem and network enforcement) does not. Using this logs a warning
#: and records ``PortResult.sandboxed = False``.
UNSANDBOXED: Final[UnsandboxedType] = UnsandboxedType()


#: No writes to your files, no network at all, credentials unreadable.
#:
#: See :class:`SandboxPolicy` for the device and scratch paths srt permits
#: regardless.
READONLY: Final[SandboxPolicy] = SandboxPolicy(
    deny_read=SECRET_PATHS,
)

#: Writes confined to the working directory; only the Claude API reachable.
#:
#: ``claude -p`` works. Its telemetry endpoints are denied, which is harmless
#: but shows up as sandbox violations on macOS.
TIGHT: Final[SandboxPolicy] = SandboxPolicy(
    allow_write=("{cwd}",),
    deny_read=SECRET_PATHS,
    allowed_domains=("api.anthropic.com",),
)

#: The default: broad enough for a typical agent coding session, not unlimited.
#:
#: Writes are confined to the working directory and credentials stay unreadable.
#: The domain list is curated and visible - nothing is allowed that is not
#: written below. Note that ``sentry.io`` and other third-party crash-report
#: sinks are deliberately absent, and that ``github.com`` must be listed
#: separately from ``*.github.com`` because srt's wildcards do not match the
#: bare domain.
DEFAULT_BROAD: Final[SandboxPolicy] = SandboxPolicy(
    allow_write=("{cwd}",),
    deny_read=SECRET_PATHS,
    allowed_domains=(
        "api.anthropic.com",  # Claude API - required by `claude -p`
        "console.anthropic.com",  # OAuth token refresh for subscription logins
        "statsig.anthropic.com",  # claude CLI feature flags (Anthropic-operated)
        "github.com",  # git over HTTPS, releases
        "*.github.com",  # api.github.com, codeload.github.com
        "*.githubusercontent.com",  # raw/objects CDN
        "pypi.org",  # pip index
        "files.pythonhosted.org",  # pip downloads
        "registry.npmjs.org",  # npm metadata and tarballs
    ),
)


def check_srt_shape(config: Mapping[str, Any]) -> dict[str, Any]:
    """Shallow-check a raw ``sandbox-runtime`` configuration dict.

    The parent process cannot import srt, so this only catches obvious mistakes
    before a subprocess is spawned. The runner validates the dict properly with
    srt's own pydantic model.

    Path tokens (``{cwd}``, ``{home}``) are **not** resolved for raw dicts -
    supply absolute paths. Note that a raw dict carries none of the
    :data:`SECRET_PATHS` a preset would, so ``sandboxed=True`` with a raw dict
    only means "OS enforcement is active", not "restrictive".

    Args:
        config: An srt-shaped mapping.

    Returns:
        A deep copy of ``config`` as a plain dict.

    Raises:
        ValueError: If ``config`` is not a mapping, carries a forbidden or
            unknown top-level key, or omits ``network`` or ``filesystem``.
    """
    if not isinstance(config, Mapping):
        raise ValueError(f"sandbox config must be a mapping, got {type(config).__name__}")

    forbidden = sorted(set(config) & _SRT_FORBIDDEN_KEYS)
    if forbidden:
        raise ValueError(
            f"sandbox config may not set: {', '.join(forbidden)}. "
            "srt executes ripgrep.command as a binary while building the sandbox, "
            "so it is refused here; the default 'rg' on PATH is used."
        )

    unknown = sorted(set(config) - _SRT_PASSTHROUGH_KEYS - _SRT_FORBIDDEN_KEYS)
    if unknown:
        raise ValueError(
            f"unknown sandbox config keys: {', '.join(unknown)}. "
            f"Expected a subset of: {', '.join(sorted(_SRT_PASSTHROUGH_KEYS))}"
        )

    missing = sorted({"network", "filesystem"} - set(config))
    if missing:
        raise ValueError(f"sandbox config is missing required keys: {', '.join(missing)}")

    return copy.deepcopy(dict(config))
