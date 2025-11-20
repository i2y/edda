"""
AST-based workflow analyzer for Edda framework.

This module analyzes Python source code to extract workflow definitions
and their control flow using the Abstract Syntax Tree.
"""

import ast
from typing import Any


class WorkflowAnalyzer(ast.NodeVisitor):
    """
    AST visitor that analyzes @workflow decorated workflows.

    This visitor extracts workflow structure including:
    - Activity calls
    - Compensation registrations
    - Event waits
    - Conditional branches
    - Exception handling
    """

    def __init__(self) -> None:
        """Initialize the workflow analyzer."""
        self.workflows: list[dict[str, Any]] = []
        self.current_workflow: dict[str, Any] | None = None

    def analyze(self, source_code: str) -> list[dict[str, Any]]:
        """
        Analyze Python source code and extract workflow definitions.

        Args:
            source_code: Python source code as string

        Returns:
            List of workflow dictionaries containing structure information
        """
        tree = ast.parse(source_code)
        self.visit(tree)
        return self.workflows

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """
        Visit function definition nodes.

        Detects @workflow decorated functions and analyzes their body.

        Args:
            node: Function definition AST node
        """
        self._process_function(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """
        Visit async function definition nodes.

        Detects @workflow decorated async functions and analyzes their body.

        Args:
            node: Async function definition AST node
        """
        self._process_function(node)
        self.generic_visit(node)

    def _process_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """
        Process function definition (sync or async).

        Args:
            node: Function definition node
        """
        if self._has_workflow_decorator(node):
            steps: list[dict[str, Any]] = []
            workflow = {
                "name": node.name,
                "args": [arg.arg for arg in node.args.args[1:]],  # Skip 'ctx'
                "steps": steps,
                "docstring": ast.get_docstring(node),
            }
            self.current_workflow = workflow
            self.workflows.append(workflow)

            # Analyze function body
            self._analyze_body(node.body, steps)
            self.current_workflow = None

    def _has_workflow_decorator(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """
        Check if function has @workflow decorator.

        Args:
            node: Function definition node

        Returns:
            True if function is decorated with @workflow
        """
        for decorator in node.decorator_list:
            # Check for @workflow (simple decorator)
            if isinstance(decorator, ast.Name) and decorator.id == "workflow":
                return True
            # Check for @workflow(...) (decorator with arguments)
            if (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "workflow"
            ):
                return True
        return False

    def _analyze_body(self, body: list[ast.stmt], steps: list[dict[str, Any]]) -> None:
        """
        Analyze function body to extract workflow steps.

        Args:
            body: List of AST statement nodes
            steps: List to append extracted steps to
        """
        for stmt in body:
            # Skip docstring
            if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant):
                continue

            # await activity() or await function()
            if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Await):
                call = stmt.value.value
                if isinstance(call, ast.Call):
                    step = self._extract_call_info(call)
                    if step:
                        steps.append(step)

            # result = await activity()
            elif isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Await):
                call = stmt.value.value
                if isinstance(call, ast.Call):
                    step = self._extract_call_info(call)
                    if step:
                        # Store variable name if assigned
                        if stmt.targets and isinstance(stmt.targets[0], ast.Name):
                            step["result_var"] = stmt.targets[0].id
                        steps.append(step)

            # if/elif/else conditional
            elif isinstance(stmt, ast.If):
                # Check if this is an if-elif chain (render as multi-branch)
                if self._is_elif_chain(stmt):
                    multi_condition = self._flatten_elif_chain(stmt)
                    steps.append(multi_condition)
                else:
                    # Simple if-else (render as binary condition)
                    if_branch: list[dict[str, Any]] = []
                    else_branch: list[dict[str, Any]] = []
                    condition = {
                        "type": "condition",
                        "test": self._unparse_safely(stmt.test),
                        "if_branch": if_branch,
                        "else_branch": else_branch,
                    }
                    self._analyze_body(stmt.body, if_branch)
                    self._analyze_body(stmt.orelse, else_branch)
                    steps.append(condition)

            # try/except exception handling
            elif isinstance(stmt, ast.Try):
                try_body: list[dict[str, Any]] = []
                finally_body: list[dict[str, Any]] = []
                except_handlers: list[dict[str, Any]] = []
                try_block = {
                    "type": "try",
                    "try_body": try_body,
                    "except_handlers": except_handlers,
                    "finally_body": finally_body,
                }
                self._analyze_body(stmt.body, try_body)

                for handler in stmt.handlers:
                    except_body: list[dict[str, Any]] = []
                    self._analyze_body(handler.body, except_body)
                    exception_type = (
                        self._unparse_safely(handler.type) if handler.type else "Exception"
                    )
                    except_handlers.append({"exception": exception_type, "body": except_body})

                if stmt.finalbody:
                    self._analyze_body(stmt.finalbody, finally_body)

                steps.append(try_block)

            # for loop (simplified representation)
            elif isinstance(stmt, ast.For):
                loop_body: list[dict[str, Any]] = []
                loop = {
                    "type": "loop",
                    "loop_type": "for",
                    "target": self._unparse_safely(stmt.target),
                    "iter": self._unparse_safely(stmt.iter),
                    "body": loop_body,
                }
                self._analyze_body(stmt.body, loop_body)
                steps.append(loop)

            # while loop (simplified representation)
            elif isinstance(stmt, ast.While):
                while_body: list[dict[str, Any]] = []
                loop = {
                    "type": "loop",
                    "loop_type": "while",
                    "test": self._unparse_safely(stmt.test),
                    "body": while_body,
                }
                self._analyze_body(stmt.body, while_body)
                steps.append(loop)

            # match-case statement (Python 3.10+)
            elif isinstance(stmt, ast.Match):
                cases: list[dict[str, Any]] = []
                match_block = {
                    "type": "match",
                    "subject": self._unparse_safely(stmt.subject),
                    "cases": cases,
                }

                for case in stmt.cases:
                    case_body: list[dict[str, Any]] = []
                    self._analyze_body(case.body, case_body)

                    # Extract pattern as string
                    pattern = self._unparse_safely(case.pattern)

                    # Extract guard condition (if clause) if present
                    guard = None
                    if case.guard:
                        guard = self._unparse_safely(case.guard)

                    cases.append(
                        {
                            "pattern": pattern,
                            "guard": guard,
                            "body": case_body,
                        }
                    )

                steps.append(match_block)

    def _extract_call_info(self, call: ast.Call) -> dict[str, Any] | None:
        """
        Extract information from function call.

        Args:
            call: Call AST node

        Returns:
            Dictionary with call information or None
        """
        if isinstance(call.func, ast.Name):
            func_name = call.func.id

            # Detect special Edda functions
            if func_name == "register_compensation":
                # await register_compensation(ctx, compensation_func, **kwargs)
                compensation_func = (
                    self._get_arg_name(call.args[1]) if len(call.args) > 1 else "unknown"
                )
                return {"type": "compensation", "activity_name": compensation_func}

            elif func_name == "wait_event":
                # await wait_event(ctx, event_type="...", ...)
                event_type = self._get_keyword_arg(call, "event_type")
                timeout = self._get_keyword_arg(call, "timeout_seconds")
                return {
                    "type": "wait_event",
                    "event_type": event_type or "unknown",
                    "timeout": timeout,
                }

            else:
                # Regular activity or function call
                return {"type": "activity", "activity_name": func_name}

        return None

    def _get_arg_name(self, arg: ast.expr) -> str:
        """
        Get function name from argument.

        Args:
            arg: Argument expression node

        Returns:
            Function name as string
        """
        if isinstance(arg, ast.Name):
            return arg.id
        return self._unparse_safely(arg)

    def _get_keyword_arg(self, call: ast.Call, key: str) -> str | None:
        """
        Extract keyword argument value from call.

        Args:
            call: Call AST node
            key: Keyword argument name

        Returns:
            Argument value as string or None
        """
        for keyword in call.keywords:
            if keyword.arg == key:
                if isinstance(keyword.value, ast.Constant):
                    return str(keyword.value.value)
                return self._unparse_safely(keyword.value)
        return None

    def _is_elif_chain(self, stmt: ast.If) -> bool:
        """
        Check if an if statement is part of an if-elif chain.

        An if-elif chain is detected when the orelse contains another ast.If node.

        Args:
            stmt: If statement node

        Returns:
            True if this is an if-elif chain
        """
        # Check if orelse contains exactly one statement and it's an ast.If
        return bool(len(stmt.orelse) == 1 and isinstance(stmt.orelse[0], ast.If))

    def _flatten_elif_chain(self, stmt: ast.If) -> dict[str, Any]:
        """
        Flatten an if-elif-else chain into a multi-branch structure.

        Converts nested ast.If nodes in orelse into a flat list of branches,
        similar to match-case structure.

        Args:
            stmt: Root if statement node

        Returns:
            Dictionary with type "multi_condition" and flat list of branches
        """
        branches: list[dict[str, Any]] = []

        # Process the initial if branch
        if_body: list[dict[str, Any]] = []
        self._analyze_body(stmt.body, if_body)
        branches.append({"test": self._unparse_safely(stmt.test), "body": if_body})

        # Recursively process elif/else branches
        current_orelse = stmt.orelse
        while current_orelse:
            # Check if orelse contains an elif (another ast.If)
            if len(current_orelse) == 1 and isinstance(current_orelse[0], ast.If):
                elif_node = current_orelse[0]
                elif_body: list[dict[str, Any]] = []
                self._analyze_body(elif_node.body, elif_body)
                branches.append({"test": self._unparse_safely(elif_node.test), "body": elif_body})
                current_orelse = elif_node.orelse
            else:
                # This is the final else branch (or no else)
                if current_orelse:
                    else_body: list[dict[str, Any]] = []
                    self._analyze_body(current_orelse, else_body)
                    branches.append({"test": None, "body": else_body})
                break

        return {"type": "multi_condition", "branches": branches}

    def _unparse_safely(self, node: ast.AST) -> str:
        """
        Safely unparse AST node to string.

        Args:
            node: AST node

        Returns:
            String representation of the node
        """
        try:
            return ast.unparse(node)
        except Exception:
            return "<unparseable>"
