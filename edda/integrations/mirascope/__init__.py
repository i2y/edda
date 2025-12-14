"""
Edda + Mirascope V2 integration for durable LLM calls.

This module provides utilities to make LLM calls durable through
Edda's activity system, enabling automatic caching, retry, and
crash recovery for LLM operations.

Example:
    Using the decorator::

        from edda import workflow, WorkflowContext
        from edda.integrations.mirascope import durable_call

        @durable_call("anthropic/claude-sonnet-4-20250514")
        async def summarize(text: str) -> str:
            return f"Summarize: {text}"

        @workflow
        async def my_workflow(ctx: WorkflowContext, text: str) -> str:
            response = await summarize(ctx, text)
            return response["content"]

    Using the call function::

        from edda import workflow, WorkflowContext
        from edda.integrations.mirascope import call

        @workflow
        async def my_workflow(ctx: WorkflowContext, question: str) -> str:
            response = await call(
                ctx,
                model="anthropic/claude-sonnet-4-20250514",
                prompt=question,
            )
            return response["content"]

    Using DurableAgent for context-aware conversations::

        from dataclasses import dataclass
        from mirascope import llm
        from edda import workflow, WorkflowContext
        from edda.integrations.mirascope import DurableAgent, DurableDeps

        @dataclass
        class MyDeps:
            documents: list[str]

        class MyAgent(DurableAgent[MyDeps]):
            model = "anthropic/claude-sonnet-4-20250514"

            def build_prompt(self, ctx, message):
                docs = "\\n".join(ctx.deps.documents)
                return [
                    llm.messages.system(f"Documents:\\n{docs}"),
                    llm.messages.user(message),
                ]

        @workflow
        async def my_workflow(ctx: WorkflowContext, query: str) -> str:
            deps = MyDeps(documents=["Doc 1", "Doc 2"])
            agent = MyAgent(ctx)
            response = await agent.chat(deps, query)
            return response["content"]
"""

from edda.integrations.mirascope.agent import DurableAgent, DurableDeps
from edda.integrations.mirascope.call import call, call_with_messages
from edda.integrations.mirascope.decorator import durable_call
from edda.integrations.mirascope.types import DurableResponse

__all__ = [
    "durable_call",
    "call",
    "call_with_messages",
    "DurableAgent",
    "DurableDeps",
    "DurableResponse",
]
