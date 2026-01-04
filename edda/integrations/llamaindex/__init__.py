"""
LlamaIndex Workflow Integration for Edda.

This module provides integration between LlamaIndex Workflow and Edda's durable
execution framework, making workflow execution crash-recoverable and supporting
durable wait operations.

Example:
    from llama_index.core.workflow import Workflow, step, Event, StartEvent, StopEvent
    from edda import workflow, WorkflowContext
    from edda.integrations.llamaindex import DurableWorkflowRunner, DurableSleepEvent

    # Define events
    class ProcessedEvent(Event):
        data: str

    # Define workflow
    class MyWorkflow(Workflow):
        @step
        async def process(self, ctx: Context, ev: StartEvent) -> ProcessedEvent:
            return ProcessedEvent(data=f"processed: {ev.input}")

        @step
        async def finalize(self, ctx: Context, ev: ProcessedEvent) -> StopEvent:
            return StopEvent(result={"status": "done", "data": ev.data})

    # Create durable runner
    runner = DurableWorkflowRunner(MyWorkflow)

    # Use in Edda workflow
    @workflow
    async def my_workflow(ctx: WorkflowContext, input_data: str) -> dict:
        result = await runner.run(ctx, input=input_data)
        return result

Installation:
    pip install 'edda-framework[llamaindex]'
"""

from .events import DurableSleepEvent, DurableWaitEvent, ResumeEvent
from .exceptions import WorkflowExecutionError, WorkflowReplayError
from .workflow import DurableWorkflowRunner

__all__ = [
    "DurableWorkflowRunner",
    "DurableSleepEvent",
    "DurableWaitEvent",
    "ResumeEvent",
    "WorkflowExecutionError",
    "WorkflowReplayError",
]
