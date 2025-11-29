"""
UI components for generating interactive Mermaid diagrams.
"""

import json
from typing import Any

from edda.viewer_ui.theme import get_edge_color, get_mermaid_node_style, get_mermaid_style
from edda.visualizer.ast_analyzer import WorkflowAnalyzer
from edda.visualizer.mermaid_generator import MermaidGenerator


def generate_interactive_mermaid(
    instance_id: str, history: list[dict[str, Any]], is_dark: bool = False
) -> str:
    """
    Generate interactive Mermaid diagram from execution history.

    Each node is clickable and emits an "activity_click" event with
    instance_id and activity_id.

    This function now detects:
    - Loops (same activity executed multiple times)

    Args:
        instance_id: Workflow instance ID
        history: List of execution activity dictionaries
        is_dark: Whether dark mode is enabled

    Returns:
        Mermaid flowchart diagram code with embedded click events
    """
    if not history:
        return "flowchart TD\n    Start([No activities])"

    lines = ["flowchart TD"]

    # Extract workflow name from first activity
    workflow_name = history[0].get("workflow_name", "workflow")

    # Start node
    lines.append(f"    Start([{workflow_name}])")

    prev_node = "Start"
    activity_occurrences: dict[str, list[str]] = {}  # Track activity name -> list of activity_ids
    activity_index = 0

    # Status icon mapping
    status_icons = {
        "completed": "✅",
        "running": "⏳",
        "failed": "❌",
        "waiting_for_event": "⏸️",
        "waiting_for_timer": "⏱️",
        "cancelled": "🚫",
        "compensated": "🔄",
        "compensating": "🔄",
        "compensation_failed": "⚠️",
        "event_received": "📨",
    }

    for activity_data in history:
        activity_id = activity_data.get("activity_id")
        if not activity_id:
            activity_index += 1
            activity_id = f"activity_{activity_index}"

        activity = activity_data.get("activity_name", activity_id)
        status = activity_data.get("status", "unknown")

        # Track activity occurrences for loop detection
        if activity not in activity_occurrences:
            activity_occurrences[activity] = []
        activity_occurrences[activity].append(activity_id)

        # Generate unique node ID based on activity_id (sanitized for Mermaid)
        safe_id = activity_id.replace(":", "_").replace("-", "_")
        node_id = f"N_{safe_id}"

        # Detect if this is a repeated activity (loop iteration)
        occurrence_count = len(activity_occurrences[activity])
        label_suffix = f" ({occurrence_count}x)" if occurrence_count >= 2 else ""

        # Node label with status icon
        icon = status_icons.get(status, "")
        label = f"{icon} {activity}{label_suffix}" if icon else f"{activity}{label_suffix}"

        # Get themed style colors
        style_color = get_mermaid_style(status, is_dark)
        if status == "compensated":
            style_color += ",stroke-dasharray:5"

        # Node definition
        lines.append(f'    {node_id}["{label}"]')
        lines.append(f"    style {node_id} {style_color}")

        # Click event - CRITICAL: embed click handler
        # Pass instance_id and activity_id as separate string arguments to avoid JSON escaping issues
        lines.append(
            f'    click {node_id} call emitEvent("activity_click", "{instance_id}", "{activity_id}")'
        )

        # Check if this is a loop iteration
        if occurrence_count > 1:
            # This is a loop iteration - show with different arrow style
            lines.append(f"    {prev_node} -.->|retry/loop| {node_id}")
        else:
            # Normal sequential flow
            lines.append(f"    {prev_node} --> {node_id}")

        prev_node = node_id

    # End node
    lines.append("    End([Complete])")
    lines.append(f"    {prev_node} --> End")

    return "\n".join(lines)


class HybridMermaidGenerator(MermaidGenerator):
    """
    Extended Mermaid generator that combines static workflow analysis with execution history.

    This generator highlights executed paths while showing the complete workflow structure.
    """

    def __init__(
        self,
        instance_id: str,
        executed_activities: set[str],
        compensations: dict[str, dict[str, Any]] | None = None,
        workflow_status: str = "running",
        activity_status_map: dict[str, str] | None = None,
        is_dark: bool = False,
    ):
        """
        Initialize hybrid Mermaid generator.

        Args:
            instance_id: Workflow instance ID for click events
            executed_activities: Set of activity names that were executed
            compensations: Optional mapping of activity_id -> compensation info
            workflow_status: Status of the workflow instance (running, completed, failed, etc.)
            activity_status_map: Optional mapping of activity name to status (completed, failed, etc.)
            is_dark: Whether dark mode is enabled
        """
        super().__init__()
        self.instance_id = instance_id
        self.executed_activities = executed_activities
        self.compensations = compensations or {}
        self.workflow_status = workflow_status
        self.activity_status_map = activity_status_map or {}
        self.is_dark = is_dark
        self.activity_id_map: dict[str, str] = {}  # Map activity name to activity_id for clicks
        self.activity_execution_counts: dict[str, int] = {}  # Map activity name to execution count
        self.edge_counter = 0  # Track edge indices for linkStyle
        self.executed_edges: list[int] = []  # Indices of executed edges for green styling
        self.executed_compensations: list[dict[str, Any]] = []  # Compensation execution history

    def generate(self, workflow: dict[str, Any]) -> str:
        """
        Generate Mermaid flowchart with execution highlighting and linkStyle.

        Args:
            workflow: Workflow dictionary from WorkflowAnalyzer

        Returns:
            Mermaid flowchart syntax as string with styled edges
        """
        # Reset counters
        self.node_counter = 0
        self.edge_counter = 0
        self.executed_edges = []
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

        # Mark this edge as executed if workflow is completed
        if self.workflow_status == "completed":
            self.executed_edges.append(self.edge_counter)

        self.edge_counter += 1  # Count the final edge to End

        # Add compensation execution section if any compensations were executed
        if self.executed_compensations:
            # Compensations are already in chronological order (no need to sort)
            sorted_compensations = self.executed_compensations

            # Add a visual separator (compensation execution section)
            comp_start_id = self._next_node_id()
            self.lines.append(f"    {comp_start_id}[Compensation Execution]")
            comp_header_style = get_mermaid_style("running", self.is_dark)
            self.lines.append(f"    style {comp_start_id} {comp_header_style}")
            self.lines.append(f"    {end_id} -.->|rollback| {comp_start_id}")
            self.edge_counter += 1

            # Render each compensation execution
            prev_comp_id = comp_start_id
            for comp in sorted_compensations:
                comp_name = comp["activity_name"]
                comp_activity_id = comp["activity_id"]

                # Create compensation node with rollback icon
                comp_node_id = self._next_node_id()
                label = f"🔄 {comp_name}"

                self.lines.append(f'    {comp_node_id}["{label}"]')
                comp_node_style = get_mermaid_style("compensating", self.is_dark)
                self.lines.append(f"    style {comp_node_id} {comp_node_style},stroke-width:3px")

                # Add click event for compensation activity
                self.lines.append(
                    f'    click {comp_node_id} call emitEvent("activity_click", "{self.instance_id}", "{comp_activity_id}")'
                )

                # Connect with dashed arrow (compensation flow)
                self.lines.append(f"    {prev_comp_id} -.-> {comp_node_id}")
                self.executed_edges.append(self.edge_counter)
                self.edge_counter += 1

                prev_comp_id = comp_node_id

        # Add linkStyle for executed edges
        if self.executed_edges:
            edge_indices = ",".join(str(i) for i in self.executed_edges)
            edge_color = get_edge_color("executed", self.is_dark)
            self.lines.append(f"    linkStyle {edge_indices} stroke:{edge_color},stroke-width:3px")

        return "\n".join(self.lines)

    def _generate_steps(self, steps: list[dict[str, Any]], prev_id: str) -> str:
        """
        Generate Mermaid nodes for a sequence of steps with execution highlighting.

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
                func_name = step.get("activity_name", step.get("function", "unknown"))
                executed = func_name in self.executed_activities

                # Get execution count and status for this activity
                exec_count = self.activity_execution_counts.get(func_name, 1)
                status = self.activity_status_map.get(func_name)

                # Check if this activity has compensation registered
                has_compensation = False
                if func_name in self.activity_id_map:
                    activity_id = self.activity_id_map[func_name]
                    has_compensation = activity_id in self.compensations

                # Determine label prefix based on status
                if status == "completed":
                    label_prefix = "✅ "
                elif status == "failed":
                    label_prefix = "❌ "
                elif status == "compensated":
                    label_prefix = "🔄 "
                elif status is not None:
                    # Other executed statuses (running, waiting, etc.)
                    label_prefix = "⏳ "
                else:
                    # Not executed
                    label_prefix = ""

                # Build label with execution count badge if >= 2
                if exec_count >= 2:
                    label = f"{label_prefix}{func_name} ({exec_count}x)"
                else:
                    label = f"{label_prefix}{func_name}"

                # Add compensation badge if registered
                if has_compensation:
                    label += " ⚠"

                # Style based on status using theme colors
                if status == "completed":
                    style_color = get_mermaid_style("completed", self.is_dark) + ",stroke-width:3px"
                elif status == "failed":
                    style_color = get_mermaid_style("failed", self.is_dark) + ",stroke-width:3px"
                elif status == "compensated":
                    style_color = (
                        get_mermaid_style("compensating", self.is_dark)
                        + ",stroke-width:3px,stroke-dasharray:5"
                    )
                elif status is not None:
                    # Other executed statuses (running, waiting)
                    style_color = get_mermaid_style("running", self.is_dark) + ",stroke-width:3px"
                else:
                    # Not executed
                    style_color = get_mermaid_style("not_executed", self.is_dark)

                self.lines.append(f'    {node_id}["{label}"]')
                self.lines.append(f"    style {node_id} {style_color}")

                # Add click event only for executed nodes
                if executed and func_name in self.activity_id_map:
                    activity_id = self.activity_id_map[func_name]
                    self.lines.append(
                        f'    click {node_id} call emitEvent("activity_click", "{self.instance_id}", "{activity_id}")'
                    )

                # Edge styling based on execution
                if executed:
                    self.lines.append(f"    {current_id} --> {node_id}")
                    self.executed_edges.append(self.edge_counter)
                else:
                    self.lines.append(f"    {current_id} -.-> {node_id}")

                self.edge_counter += 1
                current_id = node_id

            elif step_type == "compensation":
                # Compensation registration
                node_id = self._next_node_id()
                func_name = step.get("activity_name", step.get("function", "unknown"))
                self.lines.append(f"    {node_id}[register_compensation:<br/>{func_name}]")
                self.lines.append(f"    {current_id} --> {node_id}")
                comp_reg_style = get_mermaid_style("compensating", self.is_dark)
                self.lines.append(f"    style {node_id} {comp_reg_style}")

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
                wait_event_style = get_mermaid_style("waiting_event", self.is_dark)
                self.lines.append(f"    style {node_id} {wait_event_style}")
                current_id = node_id

            elif step_type == "condition":
                # Conditional branch (if/else)
                current_id = self._generate_conditional(step, current_id)

            elif step_type == "multi_condition":
                # Multi-branch conditional (if-elif-else chain)
                current_id = self._generate_multi_conditional(step, current_id)

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

    def _generate_conditional(
        self,
        condition: dict[str, Any],
        prev_id: str,
        edge_label: str = "",
        incoming_executed: bool | None = None,
    ) -> str:
        """
        Generate conditional branch (if/else) with execution highlighting.

        Args:
            condition: Condition step dictionary
            prev_id: Previous node ID
            edge_label: Optional label for edge from prev_id (e.g., "|No|")
            incoming_executed: Whether the incoming edge was executed (for nested conditions)

        Returns:
            ID of merge node
        """
        # Condition node (diamond shape)
        cond_id = self._next_node_id()
        test_expr = condition.get("test", "condition")

        # Sanitize test expression for Mermaid - remove ALL problematic characters
        # Replace dict/list accessors and special chars
        test_expr = (
            test_expr.replace('"', "'")
            .replace("{", "(")
            .replace("}", ")")
            .replace("[", ".")
            .replace("]", "")
            .replace("'", "")
        )
        # Limit length to avoid overflow
        if len(test_expr) > 40:
            test_expr = test_expr[:37] + "..."

        # Use diamond shape for condition - correct Mermaid syntax
        self.lines.append(f"    {cond_id}{{{test_expr}?}}")

        # Process branches first to determine execution status
        if_branch = condition.get("if_branch", [])
        else_branch = condition.get("else_branch", [])

        # Check if branches contain executed activities
        if_has_executed = self._branch_has_executed_activity(if_branch)
        else_has_executed = self._branch_has_executed_activity(else_branch)

        # Draw edge from prev_id to this condition
        # Use incoming_executed if provided (for nested conditions), otherwise check branches
        edge_executed = (
            incoming_executed
            if incoming_executed is not None
            else (if_has_executed or else_has_executed)
        )

        if edge_label:
            self.lines.append(f"    {prev_id} -->{edge_label} {cond_id}")
        else:
            self.lines.append(f"    {prev_id} --> {cond_id}")
        if edge_executed:
            self.executed_edges.append(self.edge_counter)
        self.edge_counter += 1

        condition_style = get_mermaid_node_style("condition", self.is_dark)
        self.lines.append(f"    style {cond_id} {condition_style}")

        # Create merge node with invisible/minimal label
        merge_id = self._next_node_id()
        # Use a minimal circle node for merging (instead of showing "N4")
        self.lines.append(f"    {merge_id}(( ))")  # Small empty circle
        merge_style = get_mermaid_node_style("merge", self.is_dark)
        self.lines.append(f"    style {merge_id} {merge_style}")

        # If branch
        if if_branch:
            # Generate if branch steps
            if_start = cond_id
            for i, step in enumerate(if_branch):
                # Pass branch execution status ONLY for the first edge from condition
                branch_executed = if_has_executed if i == 0 and if_start == cond_id else None
                if_start = self._process_single_step(
                    step, if_start, "|Yes|" if if_start == cond_id else "", branch_executed
                )
            # Connect final node of if branch to merge
            self.lines.append(f"    {if_start} --> {merge_id}")
            if if_has_executed:
                self.executed_edges.append(self.edge_counter)
            self.edge_counter += 1
        else:
            # No if branch - connect condition directly to merge
            self.lines.append(f"    {cond_id} -->|Yes| {merge_id}")
            if if_has_executed:
                self.executed_edges.append(self.edge_counter)
            self.edge_counter += 1

        # Else branch
        if else_branch:
            # Generate else branch steps
            else_start = cond_id
            for i, step in enumerate(else_branch):
                # Pass branch execution status ONLY for the first edge from condition
                branch_executed = else_has_executed if i == 0 and else_start == cond_id else None
                else_start = self._process_single_step(
                    step, else_start, "|No|" if else_start == cond_id else "", branch_executed
                )
            # Connect final node of else branch to merge
            self.lines.append(f"    {else_start} --> {merge_id}")
            if else_has_executed:
                self.executed_edges.append(self.edge_counter)
            self.edge_counter += 1
        else:
            # No else branch - connect condition directly to merge
            self.lines.append(f"    {cond_id} -->|No| {merge_id}")
            if else_has_executed:
                self.executed_edges.append(self.edge_counter)
            self.edge_counter += 1

        return merge_id

    def _process_single_step(
        self,
        step: dict[str, Any],
        prev_id: str,
        edge_label: str = "",
        branch_executed: bool | None = None,
    ) -> str:
        """
        Process a single step and return the new current ID.

        Args:
            step: Step dictionary
            prev_id: Previous node ID
            edge_label: Optional label for the edge (e.g., "|Yes|")
            branch_executed: Optional branch execution status (for conditional branch edges)

        Returns:
            ID of the generated node
        """
        step_type = step.get("type")

        if step_type == "activity":
            node_id = self._next_node_id()
            func_name = step.get("activity_name", step.get("function", "unknown"))
            # Use branch execution status if provided (for conditional edges),
            # otherwise use activity execution status
            if branch_executed is not None:
                executed = branch_executed
            else:
                executed = func_name in self.executed_activities

            # Get execution count and status for this activity
            exec_count = self.activity_execution_counts.get(func_name, 1)
            status = self.activity_status_map.get(func_name)

            # Determine label prefix based on status
            if status == "completed":
                label_prefix = "✅ "
            elif status == "failed":
                label_prefix = "❌ "
            elif status == "compensated":
                label_prefix = "🔄 "
            elif status is not None:
                # Other executed statuses (running, waiting, etc.)
                label_prefix = "⏳ "
            else:
                # Not executed
                label_prefix = ""

            # Build label with execution count badge if >= 2
            if exec_count >= 2:
                label = f"{label_prefix}{func_name} ({exec_count}x)"
            else:
                label = f"{label_prefix}{func_name}"

            # Style based on status using theme colors
            if status == "completed":
                style_color = get_mermaid_style("completed", self.is_dark) + ",stroke-width:3px"
            elif status == "failed":
                style_color = get_mermaid_style("failed", self.is_dark) + ",stroke-width:3px"
            elif status == "compensated":
                style_color = (
                    get_mermaid_style("compensating", self.is_dark)
                    + ",stroke-width:3px,stroke-dasharray:5"
                )
            elif status is not None:
                # Other executed statuses (running, waiting)
                style_color = get_mermaid_style("running", self.is_dark) + ",stroke-width:3px"
            else:
                # Not executed
                style_color = get_mermaid_style("not_executed", self.is_dark)

            self.lines.append(f'    {node_id}["{label}"]')
            self.lines.append(f"    style {node_id} {style_color}")

            # Add click event only for executed nodes
            if executed and func_name in self.activity_id_map:
                activity_id = self.activity_id_map[func_name]
                self.lines.append(
                    f'    click {node_id} call emitEvent("activity_click", "{self.instance_id}", "{activity_id}")'
                )

            # Edge with optional label
            # Correct Mermaid syntax:
            #   Solid: A --> B or A -->|label| B
            #   Dashed: A -.-> B or A -.->|label| B
            if edge_label:
                # Edge from condition node with label (e.g., |Yes| or |No|)
                arrow = "-->" if executed else "-.->"
                self.lines.append(f"    {prev_id} {arrow}{edge_label} {node_id}")
            else:
                # Normal edge without label
                arrow = "-->" if executed else "-.->"
                self.lines.append(f"    {prev_id} {arrow} {node_id}")

            # Track edge index for linkStyle
            if executed:
                self.executed_edges.append(self.edge_counter)
            self.edge_counter += 1

            return node_id

        elif step_type == "condition":
            # Handle nested conditional
            # Recursively generate the nested condition
            # Pass edge_label and branch_executed (as incoming_executed)
            return self._generate_conditional(step, prev_id, edge_label, branch_executed)

        # For other step types, fall back to parent implementation
        return prev_id

    def _branch_has_executed_activity(self, branch: list[dict[str, Any]]) -> bool:
        """
        Check if a branch contains any executed activities.

        For conditional branches (if/elif/else), we only check DIRECT activities
        in the branch, not nested conditions. Nested conditions are evaluated
        separately when _generate_conditional is called recursively.

        Args:
            branch: List of step dictionaries representing a branch

        Returns:
            True if any direct activity in the branch was executed
        """
        for step in branch:
            step_type = step.get("type")

            if step_type == "activity":
                func_name = step.get("activity_name", step.get("function", "unknown"))
                if func_name in self.executed_activities:
                    return True

            elif step_type == "condition":
                # Skip nested conditions - they are handled separately
                # Only check direct activities in this branch
                pass

            elif step_type == "loop":
                # For loops, check recursively since the loop body is part of
                # the same execution path
                body = step.get("body", [])
                if self._branch_has_executed_activity(body):
                    return True

            elif step_type == "match":
                # For match statements, check recursively
                cases = step.get("cases", [])
                for case in cases:
                    case_body = case.get("body", [])
                    if self._branch_has_executed_activity(case_body):
                        return True

        return False

    def _branch_has_unclaimed_executed_activity(
        self, branch: list[dict[str, Any]], claimed_activities: set[str]
    ) -> bool:
        """
        Check if a branch contains executed activities that haven't been claimed yet.

        This method is used to prevent multiple branches with the same activity name
        from all being marked as executed in if-elif-else chains. Only the first
        branch (in order) that contains an unclaimed executed activity will be marked.

        Args:
            branch: List of step dictionaries representing a branch
            claimed_activities: Set of activity names already claimed by previous branches
                               (will be modified to add newly claimed activities)

        Returns:
            True if the branch contains unclaimed executed activities
        """
        found_unclaimed = False

        for step in branch:
            step_type = step.get("type")

            if step_type == "activity":
                func_name = step.get("activity_name", step.get("function", "unknown"))
                if func_name in self.executed_activities and func_name not in claimed_activities:
                    # This branch has an unclaimed executed activity
                    # Claim it so other branches won't use it
                    claimed_activities.add(func_name)
                    found_unclaimed = True

            elif step_type == "condition":
                # Skip nested conditions - they are handled separately
                pass

            elif step_type == "loop":
                # For loops, check recursively
                body = step.get("body", [])
                if self._branch_has_unclaimed_executed_activity(body, claimed_activities):
                    found_unclaimed = True

            elif step_type == "match":
                # For match statements, check recursively
                cases = step.get("cases", [])
                for case in cases:
                    case_body = case.get("body", [])
                    if self._branch_has_unclaimed_executed_activity(case_body, claimed_activities):
                        found_unclaimed = True

        return found_unclaimed

    def _generate_loop(self, loop: dict[str, Any], prev_id: str) -> str:
        """
        Generate loop structure with execution highlighting and minimal exit node.

        Args:
            loop: Loop dictionary
            prev_id: Previous node ID

        Returns:
            ID of exit node after loop
        """
        loop_type = loop.get("loop_type", "loop")
        loop_id = self._next_node_id()

        # Generate loop label
        if loop_type == "for":
            target = loop.get("target", "item")
            iter_expr = loop.get("iter", "items")
            label = f"for {target} in {iter_expr}"
        else:  # while
            test = loop.get("test", "condition")
            label = f"while {test}"

        # Create loop node
        self.lines.append(f'    {loop_id}["{label}"]')
        self.lines.append(f"    {prev_id} --> {loop_id}")
        self.edge_counter += 1  # Edge from prev to loop
        loop_style = get_mermaid_node_style("loop", self.is_dark)
        self.lines.append(f"    style {loop_id} {loop_style}")

        # Check if loop body contains executed activities
        body = loop.get("body", [])
        body_has_executed = self._branch_has_executed_activity(body)

        # Process loop body
        if body:
            body_end = self._generate_steps(body, loop_id)
            # Loop back edge
            self.lines.append(f"    {body_end} -.loop.-> {loop_id}")
            if body_has_executed:
                self.executed_edges.append(self.edge_counter)
            self.edge_counter += 1

        # Create exit node as small empty circle (instead of labeled node)
        exit_id = self._next_node_id()
        self.lines.append(f"    {exit_id}(( ))")  # Small empty circle
        merge_style = get_mermaid_node_style("merge", self.is_dark)
        self.lines.append(f"    style {exit_id} {merge_style}")
        self.lines.append(f"    {loop_id} -->|exit| {exit_id}")
        self.edge_counter += 1

        return exit_id

    def _generate_match(self, match: dict[str, Any], prev_id: str) -> str:
        """
        Generate match-case structure (Python 3.10+) with execution highlighting.

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
        self.edge_counter += 1
        match_style = get_mermaid_node_style("match", self.is_dark)
        self.lines.append(f"    style {match_id} {match_style}")

        # Create merge node
        merge_id = self._next_node_id()
        self.lines.append(f"    {merge_id}(( ))")  # Small empty circle for merge
        merge_style = get_mermaid_node_style("merge", self.is_dark)
        self.lines.append(f"    style {merge_id} {merge_style}")

        # Process each case
        cases = match.get("cases", [])
        for _i, case in enumerate(cases):
            pattern = case.get("pattern", "_")
            guard = case.get("guard")
            body = case.get("body", [])

            # Check if this case contains executed activities
            case_has_executed = self._branch_has_executed_activity(body)

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

                # Edge from match to case start
                arrow = "-->" if case_has_executed else "-.->"
                self.lines.append(f"    {match_id} {arrow}|{edge_label}| {case_start_id}")
                if case_has_executed:
                    self.executed_edges.append(self.edge_counter)
                self.edge_counter += 1

                case_start_style = get_mermaid_node_style("merge", self.is_dark)
                self.lines.append(f"    style {case_start_id} {case_start_style}")

                # Generate body steps starting from case_start_id
                case_end = self._generate_steps(body, case_start_id)

                # Connect final node of case to merge
                self.lines.append(f"    {case_end} --> {merge_id}")
                if case_has_executed:
                    self.executed_edges.append(self.edge_counter)
                self.edge_counter += 1
            else:
                # Empty case body - direct connection to merge
                arrow = "-->" if case_has_executed else "-.->"
                self.lines.append(f"    {match_id} {arrow}|{edge_label}| {merge_id}")
                if case_has_executed:
                    self.executed_edges.append(self.edge_counter)
                self.edge_counter += 1

        return merge_id

    def _generate_multi_conditional(self, multi_cond: dict[str, Any], prev_id: str) -> str:
        """
        Generate multi-branch conditional (if-elif-else chain) with execution highlighting.

        Similar to match-case rendering but for if-elif-else chains.

        Args:
            multi_cond: Multi-condition dictionary with branches
            prev_id: Previous node ID

        Returns:
            ID of merge node after conditional
        """
        # Decision node (diamond shape)
        decision_id = self._next_node_id()

        # Use a generic label for the decision point
        self.lines.append(f"    {decision_id}{{{{if-elif-else}}}}")
        self.lines.append(f"    {prev_id} --> {decision_id}")
        self.edge_counter += 1
        decision_style = get_mermaid_node_style("condition", self.is_dark)
        self.lines.append(f"    style {decision_id} {decision_style}")

        # Create merge node
        merge_id = self._next_node_id()
        self.lines.append(f"    {merge_id}(( ))")  # Small empty circle for merge
        merge_style = get_mermaid_node_style("merge", self.is_dark)
        self.lines.append(f"    style {merge_id} {merge_style}")

        # Process each branch
        branches = multi_cond.get("branches", [])

        # Track which activities have been claimed by previous branches
        # This prevents multiple branches with the same activity name from all being marked as executed
        claimed_activities: set[str] = set()

        # IMPORTANT: Process branches in REVERSE order to determine execution
        # This ensures that later branches (which typically have more specific conditions)
        # claim executed activities first, preventing earlier branches from being
        # incorrectly marked as executed.
        #
        # Example: if both Branch 1 and Branch 4 contain auto_reject_loan:
        #   - Process Branch 4 first → finds auto_reject_loan → claims it → marked as executed
        #   - Process Branch 1 second → auto_reject_loan already claimed → not marked as executed
        #
        # We build a list of (index, branch, execution_status) tuples in reverse order,
        # then render them in normal order for correct diagram layout.
        branch_execution_status: list[bool] = []

        for i in reversed(range(len(branches))):
            branch = branches[i]
            test = branch.get("test")  # None for else branch
            body = branch.get("body", [])

            # Check if this branch contains executed activities that haven't been claimed yet
            branch_has_executed = self._branch_has_unclaimed_executed_activity(
                body, claimed_activities
            )
            branch_execution_status.append(branch_has_executed)

        # Reverse the execution status list to match normal branch order
        branch_execution_status.reverse()

        # Now render branches in normal order (top to bottom)
        for i, branch in enumerate(branches):
            test = branch.get("test")  # None for else branch
            body = branch.get("body", [])

            # Use pre-computed execution status (from reverse-order processing)
            branch_has_executed = branch_execution_status[i]

            # Create edge label
            if test is None:
                # This is the else branch
                edge_label = "else"
            elif i == 0:
                # First branch (if)
                test_str = self._sanitize_condition_expr(test)
                edge_label = f"if {test_str}"
            else:
                # elif branches
                test_str = self._sanitize_condition_expr(test)
                edge_label = f"elif {test_str}"

            # Process branch body
            if body:
                # Create a branch-specific start node to ensure proper branching visualization
                branch_start_id = self._next_node_id()
                self.lines.append(f"    {branch_start_id}(( ))")

                # Edge from decision to branch start
                arrow = "-->" if branch_has_executed else "-.->"
                self.lines.append(f"    {decision_id} {arrow}|{edge_label}| {branch_start_id}")
                if branch_has_executed:
                    self.executed_edges.append(self.edge_counter)
                self.edge_counter += 1

                branch_start_style = get_mermaid_node_style("merge", self.is_dark)
                self.lines.append(f"    style {branch_start_id} {branch_start_style}")

                # Generate body steps starting from branch_start_id
                branch_end = self._generate_steps(body, branch_start_id)

                # Connect final node of branch to merge
                self.lines.append(f"    {branch_end} --> {merge_id}")
                if branch_has_executed:
                    self.executed_edges.append(self.edge_counter)
                self.edge_counter += 1
            else:
                # Empty branch body - direct connection to merge
                arrow = "-->" if branch_has_executed else "-.->"
                self.lines.append(f"    {decision_id} {arrow}|{edge_label}| {merge_id}")
                if branch_has_executed:
                    self.executed_edges.append(self.edge_counter)
                self.edge_counter += 1

        return merge_id

    def _sanitize_condition_expr(self, expr: str) -> str:
        """
        Sanitize condition expression for Mermaid edge labels.

        Args:
            expr: Condition expression string

        Returns:
            Sanitized expression suitable for Mermaid
        """
        # Replace problematic characters for Mermaid
        sanitized = (
            expr.replace('"', "'")
            .replace("{", "(")
            .replace("}", ")")
            .replace("[", ".")
            .replace("]", "")
            .replace("'", "")
        )

        # Limit length to avoid overflow
        if len(sanitized) > 35:
            sanitized = sanitized[:32] + "..."

        return sanitized


def generate_hybrid_mermaid(
    _workflow_name: str,
    instance_id: str,
    history: list[dict[str, Any]],
    source_code: str,
    compensations: dict[str, dict[str, Any]] | None = None,
    workflow_status: str = "running",
    is_dark: bool = False,
) -> str:
    """
    Generate hybrid Mermaid diagram combining static analysis and execution history.

    Args:
        workflow_name: Name of the workflow
        instance_id: Workflow instance ID
        history: List of execution activity dictionaries
        source_code: Source code of the workflow function
        compensations: Optional mapping of activity_id -> compensation info
        workflow_status: Status of the workflow instance (running, completed, failed, etc.)
        is_dark: Whether dark mode is enabled

    Returns:
        Mermaid flowchart diagram code with execution highlighting
    """
    if compensations is None:
        compensations = {}
    try:
        # Analyze workflow structure from source code
        analyzer = WorkflowAnalyzer()
        workflows = analyzer.analyze(source_code)

        if not workflows:
            # Fallback to history-only diagram
            return generate_interactive_mermaid(instance_id, history, is_dark)

        workflow_structure = workflows[0]

        # Extract executed activities from history
        executed_activities = set()
        activity_id_map = {}
        activity_execution_counts: dict[str, int] = {}
        activity_status_map: dict[str, str] = {}  # activity_name -> status
        executed_compensations: list[dict[str, Any]] = []  # Track compensation executions

        for activity_data in history:
            activity_name = activity_data.get("activity_name")
            activity_id = activity_data.get("activity_id")
            status = activity_data.get("status")

            if activity_name and activity_id:
                # Normalize activity name (remove "Compensate: " prefix if present)
                normalized_name = activity_name
                if activity_name.startswith("Compensate: "):
                    normalized_name = activity_name.replace("Compensate: ", "")
                    # Track compensation executions separately
                    executed_compensations.append(
                        {
                            "activity_name": normalized_name,
                            "activity_id": activity_id,
                            "status": status,
                        }
                    )
                else:
                    executed_activities.add(normalized_name)
                    # Map first occurrence for click events
                    if normalized_name not in activity_id_map:
                        activity_id_map[normalized_name] = activity_id
                    # Count execution occurrences
                    activity_execution_counts[normalized_name] = (
                        activity_execution_counts.get(normalized_name, 0) + 1
                    )
                    # Store latest status for each activity
                    if status:
                        activity_status_map[normalized_name] = status

        # Generate hybrid diagram
        generator = HybridMermaidGenerator(
            instance_id,
            executed_activities,
            compensations,
            workflow_status,
            activity_status_map,
            is_dark,
        )
        generator.activity_id_map = activity_id_map
        generator.activity_execution_counts = activity_execution_counts
        generator.executed_compensations = executed_compensations  # Add compensation executions

        return generator.generate(workflow_structure)

    except Exception as e:
        # Fallback to history-only diagram on any error
        print(f"Warning: Hybrid diagram generation failed, falling back to history-only: {e}")
        return generate_interactive_mermaid(instance_id, history, is_dark)


def format_json_for_display(data: Any) -> str:
    """
    Format data as pretty-printed JSON.

    Args:
        data: Data to format

    Returns:
        Formatted JSON string
    """
    return json.dumps(data, indent=2, ensure_ascii=False)
