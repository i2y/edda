"""
Mermaid diagram generator for workflow visualization.

This module generates Mermaid flowchart syntax from workflow structure
extracted by the AST analyzer.
"""

from typing import Any


class MermaidGenerator:
    """
    Generator for Mermaid flowchart diagrams.

    Converts workflow structure dictionaries into Mermaid flowchart syntax.
    """

    def __init__(self) -> None:
        """Initialize the Mermaid generator."""
        self.node_counter = 0
        self.lines: list[str] = []
        self.compensation_nodes: list[tuple[str, str]] = []  # (from, to) pairs

    def generate(self, workflow: dict[str, Any]) -> str:
        """
        Generate Mermaid flowchart from workflow structure.

        Args:
            workflow: Workflow dictionary from WorkflowAnalyzer

        Returns:
            Mermaid flowchart syntax as string
        """
        self.node_counter = 0
        self.lines = ["flowchart TD"]
        self.compensation_nodes = []

        # Start node
        start_id = "Start"
        self.lines.append(f"    {start_id}([{workflow['name']}])")

        # Generate steps
        prev_id = start_id
        prev_id = self._generate_steps(workflow["steps"], prev_id)

        # End node
        end_id = "End"
        self.lines.append(f"    {end_id}([Complete])")
        self.lines.append(f"    {prev_id} --> {end_id}")

        # Add compensation paths (dashed arrows in reverse)
        if self.compensation_nodes:
            self.lines.append("")
            self.lines.append("    %% Compensation paths")
            for from_node, to_node in reversed(self.compensation_nodes):
                self.lines.append(f"    {from_node} -.compensation.-> {to_node}")

        return "\n".join(self.lines)

    def _generate_steps(self, steps: list[dict[str, Any]], prev_id: str) -> str:
        """
        Generate Mermaid nodes for a sequence of steps.

        Args:
            steps: List of step dictionaries
            prev_id: ID of the previous node

        Returns:
            ID of the last node in the sequence
        """
        current_id = prev_id

        for step in steps:
            step_type = step.get("type")

            if step_type == "activity":
                # Regular activity call
                node_id = self._next_node_id()
                self.lines.append(f"    {node_id}[{step['function']}]")
                self.lines.append(f"    {current_id} --> {node_id}")
                current_id = node_id

            elif step_type == "compensation":
                # Compensation registration
                node_id = self._next_node_id()
                func_name = step["function"]
                self.lines.append(f"    {node_id}[register_compensation:<br/>{func_name}]")
                self.lines.append(f"    {current_id} --> {node_id}")
                self.lines.append(f"    style {node_id} fill:#ffe6e6")

                # Track compensation for reverse path
                self.compensation_nodes.append((current_id, node_id))

                current_id = node_id

            elif step_type == "wait_event":
                # Event waiting
                node_id = self._next_node_id()
                event_type = step.get("event_type", "unknown")
                timeout = step.get("timeout")

                label = f"wait_event:<br/>{event_type}"
                if timeout:
                    label += f"<br/>timeout: {timeout}s"

                self.lines.append(f"    {node_id}{{{{{label}}}}}")
                self.lines.append(f"    {current_id} --> {node_id}")
                self.lines.append(f"    style {node_id} fill:#fff4e6")
                current_id = node_id

            elif step_type == "condition":
                # Conditional branch (if/else)
                current_id = self._generate_conditional(step, current_id)

            elif step_type == "try":
                # Try-except block
                current_id = self._generate_try_except(step, current_id)

            elif step_type == "loop":
                # Loop (for/while)
                current_id = self._generate_loop(step, current_id)

            elif step_type == "match":
                # Match-case statement (Python 3.10+)
                current_id = self._generate_match(step, current_id)

        return current_id

    def _generate_conditional(self, condition: dict[str, Any], prev_id: str) -> str:
        """
        Generate conditional branch (if/else).

        Args:
            condition: Condition step dictionary
            prev_id: Previous node ID

        Returns:
            ID of merge node
        """
        # Condition node
        cond_id = self._next_node_id()
        test_expr = condition.get("test", "?")
        self.lines.append(f"    {cond_id}{{{test_expr}?}}")
        self.lines.append(f"    {prev_id} --> {cond_id}")

        # Process if branch
        if_branch = condition.get("if_branch", [])
        else_branch = condition.get("else_branch", [])

        # Create merge node
        merge_id = self._next_node_id()

        if if_branch:
            if_end = self._generate_steps(if_branch, cond_id)
            # Update last connection to show "Yes" label
            if self.lines and "-->" in self.lines[-1]:
                self.lines[-1] = self.lines[-1].replace("-->", "-->|Yes|", 1)
            self.lines.append(f"    {if_end} --> {merge_id}")
        else:
            self.lines.append(f"    {cond_id} -->|Yes| {merge_id}")

        # Process else branch
        if else_branch:
            else_end = self._generate_steps(else_branch, cond_id)
            # Update last connection to show "No" label
            if self.lines and "-->" in self.lines[-1]:
                self.lines[-1] = self.lines[-1].replace("-->", "-->|No|", 1)
            self.lines.append(f"    {else_end} --> {merge_id}")
        else:
            self.lines.append(f"    {cond_id} -->|No| {merge_id}")

        return merge_id

    def _generate_try_except(self, try_block: dict[str, Any], prev_id: str) -> str:
        """
        Generate try-except block.

        Args:
            try_block: Try block dictionary
            prev_id: Previous node ID

        Returns:
            ID of merge node after try-except
        """
        # Try block marker
        try_id = self._next_node_id()
        self.lines.append(f"    {try_id}[try block]")
        self.lines.append(f"    {prev_id} --> {try_id}")
        self.lines.append(f"    style {try_id} fill:#e6f2ff")

        # Process try body
        try_body = try_block.get("try_body", [])
        try_end = self._generate_steps(try_body, try_id)

        # Merge node after try-except
        merge_id = self._next_node_id()

        # Success path
        self.lines.append(f"    {try_end} -->|success| {merge_id}")

        # Exception handlers
        except_handlers = try_block.get("except_handlers", [])
        for handler in except_handlers:
            except_id = self._next_node_id()
            exception = handler.get("exception", "Exception")
            self.lines.append(f"    {except_id}[except {exception}]")
            self.lines.append(f"    {try_end} -.error.-> {except_id}")
            self.lines.append(f"    style {except_id} fill:#ffe6e6")

            # Process except body
            except_body = handler.get("body", [])
            if except_body:
                except_end = self._generate_steps(except_body, except_id)
                self.lines.append(f"    {except_end} --> {merge_id}")
            else:
                self.lines.append(f"    {except_id} --> {merge_id}")

        # Finally block (if exists)
        finally_body = try_block.get("finally_body", [])
        if finally_body:
            finally_id = self._next_node_id()
            self.lines.append(f"    {finally_id}[finally]")
            self.lines.append(f"    style {finally_id} fill:#f0f0f0")
            self.lines.append(f"    {merge_id} --> {finally_id}")
            merge_id = self._generate_steps(finally_body, finally_id)

        return merge_id

    def _generate_loop(self, loop: dict[str, Any], prev_id: str) -> str:
        """
        Generate loop structure (simplified).

        Args:
            loop: Loop dictionary
            prev_id: Previous node ID

        Returns:
            ID of node after loop
        """
        loop_type = loop.get("loop_type", "loop")
        loop_id = self._next_node_id()

        if loop_type == "for":
            target = loop.get("target", "item")
            iter_expr = loop.get("iter", "items")
            label = f"for {target} in {iter_expr}"
        else:  # while
            test = loop.get("test", "condition")
            label = f"while {test}"

        self.lines.append(f"    {loop_id}[{label}]")
        self.lines.append(f"    {prev_id} --> {loop_id}")
        self.lines.append(f"    style {loop_id} fill:#fff0f0")

        # Process loop body (simplified - show as subgraph or single block)
        body = loop.get("body", [])
        if body:
            body_end = self._generate_steps(body, loop_id)
            # Loop back
            self.lines.append(f"    {body_end} -.loop.-> {loop_id}")

        # Exit loop
        exit_id = self._next_node_id()
        self.lines.append(f"    {loop_id} -->|exit| {exit_id}")

        return exit_id

    def _generate_match(self, match: dict[str, Any], prev_id: str) -> str:
        """
        Generate match-case structure (Python 3.10+).

        Args:
            match: Match block dictionary
            prev_id: Previous node ID

        Returns:
            ID of merge node after match
        """
        # Match node (diamond shape for the subject)
        match_id = self._next_node_id()
        subject = match.get("subject", "value")

        # Sanitize subject expression for Mermaid
        subject = (
            subject.replace('"', "'")
            .replace("{", "(")
            .replace("}", ")")
            .replace("[", ".")
            .replace("]", "")
            .replace("'", "")
        )
        if len(subject) > 30:
            subject = subject[:27] + "..."

        self.lines.append(f"    {match_id}{{{{match {subject}}}}}")
        self.lines.append(f"    {prev_id} --> {match_id}")
        self.lines.append(f"    style {match_id} fill:#e8f5e9,stroke:#4caf50,stroke-width:2px")

        # Create merge node
        merge_id = self._next_node_id()

        # Process each case
        cases = match.get("cases", [])
        for _i, case in enumerate(cases):
            pattern = case.get("pattern", "_")
            guard = case.get("guard")
            body = case.get("body", [])

            # Sanitize pattern for Mermaid
            pattern = (
                pattern.replace('"', "'").replace("{", "(").replace("}", ")").replace("|", " or ")
            )
            if len(pattern) > 25:
                pattern = pattern[:22] + "..."

            # Create label for the edge
            if guard:
                # Sanitize guard
                guard_str = guard.replace('"', "'").replace("{", "(").replace("}", ")")
                if len(guard_str) > 15:
                    guard_str = guard_str[:12] + "..."
                edge_label = f"case {pattern} if {guard_str}"
            else:
                edge_label = f"case {pattern}"

            # Process case body
            if body:
                # Create a case-specific start node to ensure proper branching visualization
                case_start_id = self._next_node_id()
                self.lines.append(f"    {case_start_id}(( ))")
                self.lines.append(f"    {match_id} -->|{edge_label}| {case_start_id}")
                self.lines.append(
                    f"    style {case_start_id} fill:#fff,stroke:#999,stroke-width:1px"
                )

                # Generate body steps starting from case_start_id
                case_end = self._generate_steps(body, case_start_id)

                # Connect to merge node
                self.lines.append(f"    {case_end} --> {merge_id}")
            else:
                # Empty case body - direct connection to merge
                self.lines.append(f"    {match_id} -->|{edge_label}| {merge_id}")

        return merge_id

    def _next_node_id(self) -> str:
        """
        Generate next unique node ID.

        Returns:
            Unique node ID string
        """
        self.node_counter += 1
        return f"N{self.node_counter}"
