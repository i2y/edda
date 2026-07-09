"""The second wall: `sandbox-runtime` really is enforcing the policy.

Every test here is hermetic - the network cases prove *denial*, which the sandbox
decides locally, so they pass offline. Proving that an allowed domain is
reachable would need the real internet, so that is deliberately out of scope.

Secrets are fabricated inside `tmp_path`. Nothing here reads the developer's
actual `~/.ssh`.
"""

import asyncio
import os
import pathlib
import re
import shutil
import sys
import uuid

import pytest

from edda.agent import SandboxPolicy, port
from edda.exceptions import RetryExhaustedError
from edda.retry import RetryPolicy

from .conftest import extern_meta, needs_posix, needs_srt

pytestmark = [needs_posix, needs_srt]

needs_curl = pytest.mark.skipif(shutil.which("curl") is None, reason="curl is not installed")

NO_NETWORK = SandboxPolicy()
DISJOINT_NETWORK = SandboxPolicy(allowed_domains=("api.anthropic.com",))


@pytest.fixture
def workdir(tmp_path):
    """A real working directory. Symlinks resolved: macOS `/var` is `/private/var`."""
    return os.path.realpath(tmp_path)


def sandboxed(command, policy, workdir, **kwargs):
    return port(command, sandbox=policy, cwd=workdir, check=False, **kwargs)


class TestSandboxEngages:
    async def test_a_sandboxed_echo_reports_that_it_was_sandboxed(
        self, ctx, workdir, agent_storage
    ):
        result = await sandboxed(["echo", "hello"], NO_NETWORK, workdir)(ctx)

        assert result.exit_code == 0
        assert result.stdout.strip() == "hello"
        assert result.sandboxed is True

        meta = await extern_meta(agent_storage, ctx.instance_id)
        assert meta["sandboxed"] is True
        assert meta["srt_version"]


class TestFilesystemEnforcement:
    async def test_a_denied_read_fails_while_the_control_read_succeeds(
        self, ctx, tmp_path, workdir
    ):
        """The control read is the point: it proves the deny rule caused the failure."""
        secret = tmp_path / "id_rsa"
        secret.write_text("TOPSECRET")
        control = tmp_path / "control.txt"
        control.write_text("READABLE")

        policy = SandboxPolicy(deny_read=(str(secret),))

        denied = await sandboxed(["cat", str(secret)], policy, workdir)(ctx, activity_id="deny:1")
        assert denied.exit_code != 0
        assert "TOPSECRET" not in denied.stdout

        allowed = await sandboxed(["cat", str(control)], policy, workdir)(
            ctx, activity_id="allow:1"
        )
        assert allowed.exit_code == 0
        assert allowed.stdout.strip() == "READABLE"

    async def test_a_write_outside_allow_write_fails(self, ctx, tmp_path, workdir):
        """`/tmp` is denied on both platforms: macOS allows only `/tmp/claude`, and
        Linux bind-mounts `/` read-only. It cannot be `tmp_path.parent` - see
        `test_macos_also_allows_writes_to_the_tmpdir_parent`.
        """
        outside = pathlib.Path("/tmp") / f"edda-port-escape-{uuid.uuid4().hex}.txt"
        inside = tmp_path / "allowed.txt"
        policy = SandboxPolicy(allow_write=("{cwd}",))

        try:
            escaped = await sandboxed(["sh", "-c", f"echo x > {outside}"], policy, workdir)(
                ctx, activity_id="escape:1"
            )
            assert escaped.exit_code != 0
            assert not outside.exists()
        finally:
            outside.unlink(missing_ok=True)

        contained = await sandboxed(["sh", "-c", f"echo x > {inside}"], policy, workdir)(
            ctx, activity_id="contain:1"
        )
        assert contained.exit_code == 0
        assert inside.read_text() == "x\n"

    @pytest.mark.skipif(sys.platform != "darwin", reason="macOS-only srt behaviour")
    async def test_macos_also_allows_writes_to_the_tmpdir_parent(self, ctx, workdir):
        """Pin a widening that `allow_write` does not describe.

        On macOS, whenever write restrictions are enabled at all, srt adds a blanket
        `(allow file-write* (subpath <TMPDIR parent>))` rule - the per-user
        `/var/folders/XX/YYY` tree, taken from the *runner's* `TMPDIR`. It is not part
        of `get_default_write_paths()` and it is not visible in any policy. If this
        test ever fails, srt tightened, and `SandboxPolicy`'s docstring should follow.
        """
        tmpdir = os.environ.get("TMPDIR", "")
        if not re.match(r"^/(private/)?var/folders/[^/]{2}/[^/]+/T/?$", tmpdir):
            pytest.skip("TMPDIR does not match the pattern srt special-cases")

        target = pathlib.Path(re.sub(r"/T/?$", "", tmpdir)) / f"edda-probe-{uuid.uuid4().hex}"
        try:
            result = await sandboxed(
                ["sh", "-c", f"echo x > {target}"], SandboxPolicy(allow_write=("{cwd}",)), workdir
            )(ctx)
            assert result.exit_code == 0, "srt no longer auto-allows the TMPDIR parent"
            assert target.exists()
        finally:
            target.unlink(missing_ok=True)


class TestViolationReporting:
    @pytest.mark.skipif(sys.platform != "darwin", reason="srt reports violations on macOS only")
    async def test_a_denied_read_is_reported_as_a_violation(
        self, ctx, tmp_path, workdir, agent_storage
    ):
        """srt's own `get_violations_for_command()` can never match - see
        `_runner._collect_violations`. What the store holds is still ours, because
        this call had a runner process to itself.
        """
        secret = tmp_path / "id_rsa"
        secret.write_text("TOPSECRET")
        policy = SandboxPolicy(deny_read=(str(secret),))

        # `log stream` needs a moment to go live; the denial waits for it.
        result = await sandboxed(["sh", "-c", f"sleep 1; cat {secret}"], policy, workdir)(ctx)
        assert result.exit_code != 0

        meta = await extern_meta(agent_storage, ctx.instance_id)
        assert any("deny" in line for line in meta["violations"]), meta["violations"]
        assert any("id_rsa" in line for line in meta["violations"])

    @pytest.mark.skipif(sys.platform != "darwin", reason="srt reports violations on macOS only")
    async def test_violations_reach_the_failure_message(self, ctx, tmp_path, workdir):
        secret = tmp_path / "id_rsa"
        secret.write_text("TOPSECRET")
        p = port(
            ["sh", "-c", f"sleep 1; cat {secret}"],
            sandbox=SandboxPolicy(deny_read=(str(secret),)),
            cwd=workdir,
            retry_policy=RetryPolicy(max_attempts=1),
        )
        with pytest.raises(RetryExhaustedError) as excinfo:
            await p(ctx)

        message = str(excinfo.value.__cause__)
        assert "sandbox violations:" in message
        assert "Operation not permitted" in message


class TestNetworkEnforcement:
    @needs_curl
    async def test_no_allowlist_means_no_network(self, ctx, workdir):
        """With an empty allowlist srt starts no proxy and the profile denies all."""
        result = await sandboxed(
            ["curl", "-sS", "--max-time", "10", "https://example.com"], NO_NETWORK, workdir
        )(ctx)
        assert result.exit_code != 0

    @needs_curl
    async def test_a_disjoint_allowlist_is_refused_by_the_proxy(self, ctx, workdir):
        """The proxy runs, and rejects the CONNECT locally - no upstream dial."""
        result = await sandboxed(
            ["curl", "-sS", "--max-time", "10", "https://example.com"], DISJOINT_NETWORK, workdir
        )(ctx)
        assert result.exit_code != 0


class TestPerCallIsolation:
    async def test_concurrent_ports_enforce_their_own_policies(self, ctx, tmp_path, workdir):
        """The reason each port gets its own runner process.

        `SandboxManager` keeps its config in module globals, so two policies could
        never coexist in one process. Here they must, and they do.
        """
        secret_a = tmp_path / "a.txt"
        secret_a.write_text("AAA")
        secret_b = tmp_path / "b.txt"
        secret_b.write_text("BBB")

        # Both read A. Only the first forbids it.
        denies_a = sandboxed(
            ["cat", str(secret_a)], SandboxPolicy(deny_read=(str(secret_a),)), workdir
        )
        denies_b = sandboxed(
            ["cat", str(secret_a)], SandboxPolicy(deny_read=(str(secret_b),)), workdir
        )

        blocked, permitted = await asyncio.gather(
            denies_a(ctx, activity_id="cat:a"),
            denies_b(ctx, activity_id="cat:b"),
        )

        assert blocked.exit_code != 0
        assert "AAA" not in blocked.stdout
        assert permitted.exit_code == 0
        assert permitted.stdout.strip() == "AAA"
