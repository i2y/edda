"""The `edda` core must never depend on `edda.agent`.

`edda.agent` runs external programs in separate processes, which contradicts the
core's promise of being a lightweight in-process library. It is opt-in, and it
must stay extractable into its own distribution. A single import in the wrong
direction would end that, quietly, so the boundary is checked mechanically
rather than by review.

These tests need no optional dependency: `edda.agent`'s own import surface is
stdlib plus the Edda core. Only its runner subprocess imports `sandbox_runtime`.
"""

import ast
import subprocess
import sys
from pathlib import Path

CORE_ROOT = Path(__file__).resolve().parent.parent / "edda"
AGENT_ROOT = CORE_ROOT / "agent"
FORBIDDEN = "edda.agent"


def _core_sources() -> list[Path]:
    """Every shipped core module, i.e. all of `edda/` except `edda/agent/`."""
    return [
        path
        for path in sorted(CORE_ROOT.rglob("*.py"))
        if AGENT_ROOT not in path.parents and path != AGENT_ROOT
    ]


def _module_of(path: Path) -> str:
    """Return the dotted module name of a file inside the `edda` package."""
    relative = path.relative_to(CORE_ROOT.parent).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve_relative(path: Path, node: ast.ImportFrom) -> str:
    """Resolve `from ..x import y` to an absolute module name."""
    package = _module_of(path)
    if path.name != "__init__.py":
        package = package.rpartition(".")[0]
    for _ in range(node.level - 1):
        package = package.rpartition(".")[0]
    return f"{package}.{node.module}" if node.module else package


def _targets_agent(module: str) -> bool:
    return module == FORBIDDEN or module.startswith(f"{FORBIDDEN}.")


def _violations(path: Path) -> list[str]:
    """Return `file:line` for each import of `edda.agent` in `path`."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            modules = [
                _resolve_relative(path, node) if node.level else (node.module or ""),
            ]
        else:
            continue
        if any(_targets_agent(module) for module in modules):
            found.append(f"{path}:{node.lineno}")
    return found


def test_violations_detects_every_import_form(tmp_path, monkeypatch):
    """Prove the guard can fail: it must catch each way of importing the agent.

    A boundary check that cannot detect a breach is worse than none at all.
    """
    package = tmp_path / "edda"
    (package / "sub").mkdir(parents=True)
    monkeypatch.setattr(sys.modules[__name__], "CORE_ROOT", package)

    offending = package / "sub" / "bad.py"
    offending.write_text(
        "import edda.agent\n"
        "import edda.agent.port\n"
        "from edda.agent import port\n"
        "from edda.agent.sandbox import TIGHT\n"
        "from ..agent import port as p2\n"
        "from ..agent.errors import PortError\n",
        encoding="utf-8",
    )
    assert len(_violations(offending)) == 6

    innocent = package / "sub" / "good.py"
    innocent.write_text(
        '"""Mentions edda.agent in prose only."""\n'
        "import edda.activity\n"
        "from edda.retry import RetryPolicy\n"
        "from . import sibling\n"
        'NAME = "edda.agent"  # a string, not an import\n',
        encoding="utf-8",
    )
    assert _violations(innocent) == []


def test_core_sources_are_discovered():
    """Guard the guard: an empty walk would make the boundary test vacuous."""
    sources = _core_sources()
    assert len(sources) > 20
    assert not any(AGENT_ROOT in path.parents for path in sources)
    # edda/tui ships in the wheel, so it must obey the boundary too.
    assert any(path.parts[-2] == "tui" for path in sources)


def test_no_core_to_agent_imports():
    """No module under `edda/` (outside `edda/agent/`) may import `edda.agent`.

    Parsed with `ast`, not grep, so a mention in a docstring or a comment cannot
    trip it. Imports performed dynamically at call time are out of scope here;
    `test_import_edda_does_not_load_agent` catches the ones that fire on import.
    """
    offenders = [line for path in _core_sources() for line in _violations(path)]
    assert offenders == [], "edda core must not import edda.agent:\n" + "\n".join(offenders)


def _python(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_import_edda_does_not_load_agent():
    """`import edda` must not pull `edda.agent` into `sys.modules`.

    Run in a subprocess: this test session has already imported `edda.agent`.
    """
    result = _python(
        "import edda, sys;"
        " loaded = [m for m in sys.modules if m == 'edda.agent' or m.startswith('edda.agent.')];"
        " print(loaded);"
        " sys.exit(1 if loaded else 0)"
    )
    assert result.returncode == 0, f"import edda loaded: {result.stdout.strip()}"


def test_agent_import_surface():
    """`from edda.agent import port` works, and does not import the sandbox."""
    from edda.agent import DEFAULT_BROAD, UNSANDBOXED, PortResult, port

    assert callable(port)
    assert PortResult is not None
    assert DEFAULT_BROAD is not None
    assert repr(UNSANDBOXED) == "UNSANDBOXED"


def test_importing_agent_does_not_import_sandbox_runtime():
    """Only the runner subprocess may import `sandbox_runtime`.

    The Edda worker stays clear of srt's module-global state, its signal-handler
    hijack, and its research-preview API.
    """
    result = _python(
        "import edda.agent, sys;" " sys.exit(1 if 'sandbox_runtime' in sys.modules else 0)"
    )
    assert result.returncode == 0, "importing edda.agent must not import sandbox_runtime"
