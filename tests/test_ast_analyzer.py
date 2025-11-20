"""
Tests for AST analyzer - match-case statement support.

Tests the ability to analyze Python 3.10+ match-case statements
in workflow definitions.
"""

import sys

import pytest

from edda.visualizer.ast_analyzer import WorkflowAnalyzer

# Skip all tests in this module if Python < 3.10
pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 10), reason="match-case statements require Python 3.10+"
)


class TestMatchCaseAnalysis:
    """Test suite for match-case statement analysis."""

    def test_simple_match_case(self):
        """Test basic match-case with literal patterns."""
        source = '''
@workflow
async def match_workflow(ctx, status: str):
    """Workflow with simple match-case."""
    match status:
        case "pending":
            await send_notification(ctx)
        case "approved":
            await process_order(ctx)
        case _:
            await log_unknown(ctx)
'''

        analyzer = WorkflowAnalyzer()
        workflows = analyzer.analyze(source)

        assert len(workflows) == 1
        workflow = workflows[0]
        assert workflow["name"] == "match_workflow"

        # Should have one match block in steps
        steps = workflow["steps"]
        assert len(steps) == 1
        assert steps[0]["type"] == "match"

        match_block = steps[0]
        assert match_block["subject"] == "status"
        assert len(match_block["cases"]) == 3

        # Check each case
        case1 = match_block["cases"][0]
        assert case1["pattern"] == "'pending'"
        assert case1["guard"] is None
        assert len(case1["body"]) == 1
        assert case1["body"][0]["type"] == "activity"
        assert case1["body"][0]["activity_name"] == "send_notification"

        case2 = match_block["cases"][1]
        assert case2["pattern"] == "'approved'"
        assert len(case2["body"]) == 1

        case3 = match_block["cases"][2]
        assert case3["pattern"] == "_"
        assert len(case3["body"]) == 1

    def test_match_case_with_guard(self):
        """Test match-case with guard conditions (if clauses)."""
        source = '''
@workflow
async def guarded_match(ctx, value: int):
    """Workflow with guarded match-case."""
    match value:
        case x if x > 100:
            await handle_large(ctx, x)
        case x if x > 0:
            await handle_positive(ctx, x)
        case _:
            await handle_other(ctx, value)
'''

        analyzer = WorkflowAnalyzer()
        workflows = analyzer.analyze(source)

        workflow = workflows[0]
        match_block = workflow["steps"][0]

        assert len(match_block["cases"]) == 3

        # Case with guard "x > 100"
        case1 = match_block["cases"][0]
        assert case1["pattern"] == "x"
        assert case1["guard"] is not None
        assert "100" in case1["guard"]

        # Case with guard "x > 0"
        case2 = match_block["cases"][1]
        assert case2["pattern"] == "x"
        assert case2["guard"] is not None
        assert "0" in case2["guard"]

    def test_match_case_with_or_patterns(self):
        """Test match-case with OR patterns (|)."""
        source = '''
@workflow
async def or_pattern_match(ctx, status: str):
    """Workflow with OR patterns."""
    match status:
        case "approved" | "accepted":
            await process_order(ctx)
        case "rejected" | "cancelled":
            await send_rejection(ctx)
        case _:
            await log_unknown(ctx)
'''

        analyzer = WorkflowAnalyzer()
        workflows = analyzer.analyze(source)

        workflow = workflows[0]
        match_block = workflow["steps"][0]

        assert len(match_block["cases"]) == 3

        # OR pattern should be unparsed as "'approved' | 'accepted'"
        case1 = match_block["cases"][0]
        assert "approved" in case1["pattern"]
        assert "accepted" in case1["pattern"]
        assert "|" in case1["pattern"]

    def test_match_case_empty_body(self):
        """Test match-case with empty case body (pass)."""
        source = '''
@workflow
async def empty_case_match(ctx, value: int):
    """Workflow with empty case."""
    match value:
        case 0:
            pass
        case _:
            await handle_value(ctx, value)
'''

        analyzer = WorkflowAnalyzer()
        workflows = analyzer.analyze(source)

        workflow = workflows[0]
        match_block = workflow["steps"][0]

        assert len(match_block["cases"]) == 2

        # Empty case should have no body steps
        case1 = match_block["cases"][0]
        assert case1["pattern"] == "0"
        assert len(case1["body"]) == 0

    def test_match_case_multiple_activities(self):
        """Test match-case with multiple activities in case body."""
        source = '''
@workflow
async def multi_activity_match(ctx, status: str):
    """Workflow with multiple activities per case."""
    match status:
        case "pending":
            await validate(ctx)
            await send_notification(ctx)
            await log_status(ctx, status)
        case _:
            await handle_default(ctx)
'''

        analyzer = WorkflowAnalyzer()
        workflows = analyzer.analyze(source)

        workflow = workflows[0]
        match_block = workflow["steps"][0]

        # First case should have 3 activities
        case1 = match_block["cases"][0]
        assert len(case1["body"]) == 3
        assert case1["body"][0]["activity_name"] == "validate"
        assert case1["body"][1]["activity_name"] == "send_notification"
        assert case1["body"][2]["activity_name"] == "log_status"

    def test_match_case_with_complex_subject(self):
        """Test match-case with complex subject expression."""
        source = '''
@workflow
async def complex_subject_match(ctx, data: dict):
    """Workflow with complex subject."""
    match data.get("status", "unknown"):
        case "active":
            await process_active(ctx)
        case _:
            await process_other(ctx)
'''

        analyzer = WorkflowAnalyzer()
        workflows = analyzer.analyze(source)

        workflow = workflows[0]
        match_block = workflow["steps"][0]

        # Subject should be the dict.get() expression
        assert "data" in match_block["subject"]
        assert "get" in match_block["subject"]

    def test_nested_match_case(self):
        """Test nested match-case statements."""
        source = '''
@workflow
async def nested_match(ctx, category: str, status: str):
    """Workflow with nested match."""
    match category:
        case "order":
            match status:
                case "pending":
                    await process_pending_order(ctx)
                case _:
                    await process_other_order(ctx)
        case _:
            await handle_other_category(ctx)
'''

        analyzer = WorkflowAnalyzer()
        workflows = analyzer.analyze(source)

        workflow = workflows[0]
        outer_match = workflow["steps"][0]

        assert outer_match["type"] == "match"
        assert len(outer_match["cases"]) == 2

        # First case should contain another match
        inner_case_body = outer_match["cases"][0]["body"]
        assert len(inner_case_body) == 1
        assert inner_case_body[0]["type"] == "match"

        inner_match = inner_case_body[0]
        assert inner_match["subject"] == "status"
        assert len(inner_match["cases"]) == 2


class TestMatchCaseMermaidGeneration:
    """Test Mermaid diagram generation for match-case."""

    def test_mermaid_generation_basic_match(self):
        """Test that match-case generates valid Mermaid syntax."""
        from edda.visualizer.mermaid_generator import MermaidGenerator

        workflow = {
            "name": "test_match",
            "args": ["status"],
            "steps": [
                {
                    "type": "match",
                    "subject": "status",
                    "cases": [
                        {
                            "pattern": "'pending'",
                            "guard": None,
                            "body": [
                                {
                                    "type": "activity",
                                    "function": "notify",
                                    "activity_name": "notify",
                                }
                            ],
                        },
                        {"pattern": "_", "guard": None, "body": []},
                    ],
                }
            ],
        }

        generator = MermaidGenerator()
        mermaid = generator.generate(workflow)

        # Should contain match keyword
        assert "match status" in mermaid
        # Should contain case patterns
        assert "case" in mermaid
        assert "pending" in mermaid

    def test_mermaid_generation_guarded_match(self):
        """Test Mermaid generation with guard conditions."""
        from edda.visualizer.mermaid_generator import MermaidGenerator

        workflow = {
            "name": "test_guarded",
            "args": ["value"],
            "steps": [
                {
                    "type": "match",
                    "subject": "value",
                    "cases": [
                        {
                            "pattern": "x",
                            "guard": "x > 100",
                            "body": [
                                {
                                    "type": "activity",
                                    "function": "handle",
                                    "activity_name": "handle",
                                }
                            ],
                        }
                    ],
                }
            ],
        }

        generator = MermaidGenerator()
        mermaid = generator.generate(workflow)

        # Should contain guard condition
        assert "if" in mermaid
        assert "100" in mermaid
