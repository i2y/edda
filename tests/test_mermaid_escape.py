"""Regression tests for Mermaid label escaping.

Covers issue #7: Edda viewer fails when an f-string is used as the
``event_type`` argument of ``wait_event``. The AST analyzer extracts the
unparsed source ``f'workflow.continue:{session_id}'`` which contains
``{`` / ``}`` characters. Without escaping, those characters break the
Mermaid hexagon node syntax (``id{{label}}``) and the chart fails to
render.
"""

from edda.viewer_ui.components import HybridMermaidGenerator
from edda.visualizer.ast_analyzer import WorkflowAnalyzer
from edda.visualizer.mermaid_generator import MermaidGenerator, escape_mermaid_label


class TestEscapeMermaidLabel:
    """Direct tests for the ``escape_mermaid_label`` helper."""

    def test_replaces_curly_braces(self):
        assert escape_mermaid_label("foo{bar}baz") == "foo(bar)baz"

    def test_replaces_brackets_and_quotes(self):
        assert escape_mermaid_label('a["b"][c]') == "a.b.c"

    def test_truncates_long_text(self):
        text = "x" * 100
        out = escape_mermaid_label(text, max_len=20)
        assert len(out) == 20
        assert out.endswith("...")

    def test_preserves_simple_text(self):
        assert escape_mermaid_label("workflow.continue") == "workflow.continue"

    def test_handles_fstring_source(self):
        # ast.unparse() of f"workflow.continue:{session_id}" produces this
        unparsed = "f'workflow.continue:{session_id}'"
        out = escape_mermaid_label(unparsed)
        assert "{" not in out
        assert "}" not in out


class TestAstAnalyzerFstring:
    """Confirm the AST analyzer returns an f-string source representation
    (containing ``{`` and ``}``) for non-Constant ``event_type`` arguments.
    This documents the input we have to escape downstream."""

    def test_fstring_event_type_extracted_as_source(self):
        source = '''
@workflow
async def fstring_workflow(ctx, session_id: str):
    """Workflow that waits with an f-string event_type."""
    await wait_event(ctx, event_type=f"workflow.continue:{session_id}")
'''
        workflows = WorkflowAnalyzer().analyze(source)
        assert len(workflows) == 1
        steps = workflows[0]["steps"]
        wait_steps = [s for s in steps if s.get("type") == "wait_event"]
        assert len(wait_steps) == 1

        event_type = wait_steps[0]["event_type"]
        # AST analyzer returns the source representation of the f-string,
        # not the runtime-evaluated value.
        assert "{" in event_type
        assert "}" in event_type
        assert "session_id" in event_type

    def test_constant_event_type_extracted_directly(self):
        source = '''
@workflow
async def const_workflow(ctx):
    await wait_event(ctx, event_type="workflow.continue")
'''
        workflows = WorkflowAnalyzer().analyze(source)
        wait_steps = [s for s in workflows[0]["steps"] if s.get("type") == "wait_event"]
        assert wait_steps[0]["event_type"] == "workflow.continue"


def _wait_event_label_chunks(mermaid: str) -> list[str]:
    """Extract the label inside hexagon nodes ``id{{...}}`` for inspection."""
    chunks: list[str] = []
    for line in mermaid.splitlines():
        stripped = line.strip()
        # Hexagon syntax: <id>{{<label>}}
        idx = stripped.find("{{")
        end = stripped.rfind("}}")
        if idx != -1 and end != -1 and end > idx:
            chunks.append(stripped[idx + 2 : end])
    return chunks


class TestMermaidGeneratorWaitEvent:
    """Verify ``MermaidGenerator`` (base) escapes f-string event_type."""

    def test_fstring_event_type_does_not_break_mermaid(self):
        workflow = {
            "name": "fstring_workflow",
            "steps": [
                {
                    "type": "wait_event",
                    "event_type": "f'workflow.continue:{session_id}'",
                    "timeout": None,
                }
            ],
        }
        mermaid = MermaidGenerator().generate(workflow)

        # No curly braces should leak into the hexagon label region
        for label in _wait_event_label_chunks(mermaid):
            assert "{" not in label, f"unexpected {{ in label: {label!r}"
            assert "}" not in label, f"unexpected }} in label: {label!r}"

        # The recognizable identifier should still survive (read-only check)
        assert "session_id" in mermaid

    def test_normal_event_type_renders(self):
        workflow = {
            "name": "normal_workflow",
            "steps": [
                {
                    "type": "wait_event",
                    "event_type": "payment.completed",
                    "timeout": 30,
                }
            ],
        }
        mermaid = MermaidGenerator().generate(workflow)
        assert "wait_event:<br/>payment.completed" in mermaid
        assert "timeout: 30s" in mermaid


class TestHybridMermaidGeneratorWaitEvent:
    """Verify ``HybridMermaidGenerator`` (viewer UI) also escapes."""

    def test_fstring_event_type_does_not_break_mermaid(self):
        workflow = {
            "name": "fstring_workflow",
            "steps": [
                {
                    "type": "wait_event",
                    "event_type": "f'workflow.continue:{session_id}'",
                    "timeout": None,
                }
            ],
        }
        gen = HybridMermaidGenerator(
            instance_id="inst-1",
            executed_activities=set(),
        )
        mermaid = gen.generate(workflow)

        for label in _wait_event_label_chunks(mermaid):
            assert "{" not in label, f"unexpected {{ in label: {label!r}"
            assert "}" not in label, f"unexpected }} in label: {label!r}"

        assert "session_id" in mermaid


class TestEndToEndWaitEventEscape:
    """Source code -> AST analyzer -> Mermaid (no broken syntax)."""

    def test_fstring_workflow_source_to_mermaid_is_valid(self):
        source = '''
@workflow
async def fstring_workflow(ctx, session_id: str):
    await wait_event(ctx, event_type=f"workflow.continue:{session_id}")
'''
        workflows = WorkflowAnalyzer().analyze(source)
        mermaid = MermaidGenerator().generate(workflows[0])

        for label in _wait_event_label_chunks(mermaid):
            assert "{" not in label
            assert "}" not in label
