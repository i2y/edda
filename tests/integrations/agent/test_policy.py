"""Sandbox policies are plain data, and must read as plain data."""

import dataclasses
import importlib.util
import os

import pytest

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

PRESETS = {"READONLY": READONLY, "TIGHT": TIGHT, "DEFAULT_BROAD": DEFAULT_BROAD}


class TestPathResolution:
    def test_tokens_and_tilde_resolve_to_absolute_paths(self, tmp_path):
        policy = SandboxPolicy(
            allow_write=("{cwd}", "{cwd}/build"),
            deny_read=("{home}/.ssh", "~/.aws", "relative/dir"),
        )
        rendered = policy.to_srt_config(str(tmp_path))

        every = rendered["filesystem"]["allow_write"] + rendered["filesystem"]["deny_read"]
        for path in every:
            assert os.path.isabs(path), path
            assert "{" not in path and "~" not in path, path

        assert rendered["filesystem"]["allow_write"][0] == os.path.realpath(tmp_path)
        assert rendered["filesystem"]["deny_read"][0] == os.path.realpath(
            os.path.expanduser("~/.ssh")
        )
        # A relative path is anchored at the port's working directory.
        assert rendered["filesystem"]["deny_read"][2] == os.path.realpath(
            os.path.join(tmp_path, "relative/dir")
        )

    def test_braces_in_a_real_path_do_not_raise(self, tmp_path):
        policy = SandboxPolicy(allow_write=("{cwd}/weird{name}",))
        rendered = policy.to_srt_config(str(tmp_path))
        assert rendered["filesystem"]["allow_write"][0].endswith("weird{name}")

    def test_a_token_inside_cwd_is_not_re_expanded(self):
        """Single-pass substitution: a `{home}` produced by the `{cwd}` value must
        stay literal, so a deny rule never resolves to the wrong path (fail-open).
        """
        policy = SandboxPolicy(deny_read=("{cwd}/secret",))
        # cwd literally contains the text "{home}".
        resolved = policy.to_srt_config("/data/{home}/proj")["filesystem"]["deny_read"][0]
        assert resolved == os.path.realpath("/data/{home}/proj/secret")
        assert os.path.expanduser("~") not in resolved

    def test_globs_keep_their_wildcards(self, tmp_path):
        policy = SandboxPolicy(deny_read=("{home}/secrets/*.pem",))
        assert rendered_glob(policy, tmp_path).endswith("/secrets/*.pem")


def rendered_glob(policy, tmp_path):
    return policy.to_srt_config(str(tmp_path))["filesystem"]["deny_read"][0]


class TestPresets:
    @pytest.mark.parametrize("name", sorted(PRESETS))
    def test_every_preset_denies_the_secret_paths(self, name):
        assert PRESETS[name].deny_read == SECRET_PATHS

    @pytest.mark.parametrize("name", sorted(PRESETS))
    def test_presets_use_literal_paths_only(self, name, tmp_path):
        """srt silently drops glob entries on Linux; a preset must not rely on them."""
        config = PRESETS[name].to_srt_config(str(tmp_path))["filesystem"]
        for paths in config.values():
            for path in paths:
                assert not any(char in path for char in "*?["), (name, path)

    def test_readonly_grants_no_writes_and_no_network(self):
        assert READONLY.allow_write == ()
        assert READONLY.allowed_domains == ()

    def test_tight_reaches_only_the_claude_api(self):
        assert TIGHT.allowed_domains == ("api.anthropic.com",)
        assert TIGHT.allow_write == ("{cwd}",)

    def test_default_broad_is_curated_not_unlimited(self):
        assert DEFAULT_BROAD.allow_write == ("{cwd}",)
        assert "*" not in DEFAULT_BROAD.allowed_domains
        # Third-party crash-report sinks are exfiltration channels; keep them out.
        assert not any("sentry" in domain for domain in DEFAULT_BROAD.allowed_domains)
        # srt wildcards do not match the bare domain, so both must be listed.
        assert {"github.com", "*.github.com"} <= set(DEFAULT_BROAD.allowed_domains)

    def test_claude_config_stays_readable(self):
        """A sandboxed agent must be able to read the credentials it runs with."""
        assert not any(".claude" in path for path in SECRET_PATHS)


class TestPolicyAlgebra:
    def test_star_is_rejected(self):
        with pytest.raises(ValueError, match="deny-by-default"):
            SandboxPolicy(allowed_domains=("*",))

    def test_policies_are_frozen(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            DEFAULT_BROAD.allowed_domains = ()  # type: ignore[misc]

    def test_without_network_only_narrows(self):
        tightened = DEFAULT_BROAD.without_network()
        assert tightened.allowed_domains == ()
        assert tightened.allow_write == DEFAULT_BROAD.allow_write
        assert DEFAULT_BROAD.allowed_domains, "the original must not be mutated"

    def test_deny_narrows_both_directions(self):
        tightened = TIGHT.deny("/etc/shadow")
        assert "/etc/shadow" in tightened.deny_read
        assert "/etc/shadow" in tightened.deny_write
        assert "/etc/shadow" not in TIGHT.deny_read


class TestUnsandboxedSentinel:
    def test_it_is_a_singleton_and_says_so(self):
        assert UnsandboxedType() is UNSANDBOXED
        assert repr(UNSANDBOXED) == "UNSANDBOXED"

    def test_it_is_not_a_policy(self):
        assert not isinstance(UNSANDBOXED, SandboxPolicy)


class TestRawConfigPassthrough:
    def test_a_valid_config_is_copied(self):
        source = {"network": {"allowed_domains": []}, "filesystem": {"allow_write": ["/tmp/x"]}}
        copied = check_srt_shape(source)
        assert copied == source
        copied["filesystem"]["allow_write"].append("/etc")
        assert source["filesystem"]["allow_write"] == ["/tmp/x"]

    def test_unknown_keys_are_rejected(self):
        with pytest.raises(ValueError, match="unknown sandbox config keys: nope"):
            check_srt_shape({"network": {}, "filesystem": {}, "nope": 1})

    def test_ripgrep_is_refused_as_an_exec_vector(self):
        """srt runs ripgrep.command as a binary while building the Linux profile;
        a "sandbox restriction" dict must not be able to smuggle in an executable.
        """
        with pytest.raises(ValueError, match="may not set: ripgrep"):
            check_srt_shape(
                {
                    "network": {"allowed_domains": []},
                    "filesystem": {},
                    "ripgrep": {"command": "/tmp/evil"},
                }
            )

    def test_inert_passthrough_keys_are_allowed(self):
        cfg = {
            "network": {"allowed_domains": []},
            "filesystem": {"allow_write": ["/work"]},
            "resource_limits": {"max_memory_mb": 512},
            "allow_pty": True,
        }
        assert check_srt_shape(cfg) == cfg

    def test_required_keys_are_required(self):
        with pytest.raises(ValueError, match="missing required keys: filesystem"):
            check_srt_shape({"network": {}})

    def test_non_mappings_are_rejected(self):
        with pytest.raises(ValueError, match="must be a mapping"):
            check_srt_shape(["network"])  # type: ignore[arg-type]


@pytest.mark.skipif(
    importlib.util.find_spec("sandbox_runtime") is None,
    reason="sandbox-runtime is not installed",
)
@pytest.mark.parametrize("name", sorted(PRESETS))
def test_presets_validate_against_the_real_srt_schema(name, tmp_path):
    """Catch schema drift within the `<0.3` cap, before a runner ever spawns."""
    from sandbox_runtime import SandboxRuntimeConfig

    SandboxRuntimeConfig(**PRESETS[name].to_srt_config(str(tmp_path)))
