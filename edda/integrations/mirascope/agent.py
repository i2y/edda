"""
DurableAgent: llm.Context を活用した durable エージェント.

Mirascope V2 の llm.Context を durable execution と統合:
- llm.Context 経由の dependency injection
- 会話履歴の自動管理
- 各ターンは durable activity として実行

Example:
    Using DurableAgent with context::

        from dataclasses import dataclass
        from mirascope import llm
        from edda import workflow, WorkflowContext
        from edda.integrations.mirascope import DurableAgent, DurableDeps

        @dataclass
        class ResearchDeps:
            documents: list[str]
            search_index: dict[str, str]

        class ResearchAgent(DurableAgent[ResearchDeps]):
            model = "anthropic/claude-sonnet-4-20250514"

            @staticmethod
            @llm.tool()
            def search(ctx: llm.Context[ResearchDeps], query: str) -> str:
                '''Search through documents.'''
                return ctx.deps.search_index.get(query, "No results")

            def get_tools(self) -> list:
                return [self.search]

            def build_prompt(self, ctx: llm.Context[ResearchDeps], message: str) -> list:
                docs = "\\n".join(ctx.deps.documents)
                return [
                    llm.messages.system(f"You are a research assistant.\\nDocs:\\n{docs}"),
                    llm.messages.user(message),
                ]

        @workflow
        async def research_workflow(ctx: WorkflowContext, topic: str) -> str:
            deps = ResearchDeps(
                documents=["Doc 1...", "Doc 2..."],
                search_index={"key1": "value1"},
            )
            agent = ResearchAgent(ctx)
            response = await agent.chat(deps, f"Research: {topic}")
            return response["content"]
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from edda.activity import activity
from edda.context import WorkflowContext

from .types import DurableResponse

if TYPE_CHECKING:
    pass

# Type variable for dependency data
T = TypeVar("T")


def _import_mirascope() -> Any:
    """Import mirascope with helpful error message."""
    try:
        from mirascope import llm

        return llm
    except ImportError as e:
        msg = (
            "Mirascope is not installed. Install with:\n"
            "  pip install 'mirascope[anthropic]'\n"
            "or\n"
            "  pip install 'edda-framework[mirascope]'"
        )
        raise ImportError(msg) from e


@dataclass
class DurableDeps(Generic[T]):
    """
    Serializable dependency container for DurableAgent.

    Bridges llm.Context and Edda's durable activity system.
    Manages both user-defined dependencies and conversation history.

    Attributes:
        data: User-defined dependency data (will be injected into llm.Context)
        history: Conversation history (automatically managed)

    Example:
        >>> @dataclass
        ... class MyDeps:
        ...     api_key: str
        ...     cache: dict[str, str]
        ...
        >>> deps = DurableDeps(data=MyDeps(api_key="xxx", cache={}))
        >>> agent = MyAgent(ctx)
        >>> await agent.chat(deps, "Hello")  # history auto-updated
    """

    data: T
    history: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dictionary for activity caching."""
        import dataclasses

        # Handle dataclass or dict data
        if dataclasses.is_dataclass(self.data) and not isinstance(self.data, type):
            data_dict: dict[str, Any] = dataclasses.asdict(self.data)
        elif hasattr(self.data, "model_dump"):
            # Pydantic model
            data_dict = self.data.model_dump()
        elif isinstance(self.data, dict):
            data_dict = self.data
        else:
            data_dict = {"value": self.data}

        return {
            "data": data_dict,
            "history": self.history,
        }

    def add_user_message(self, content: str) -> None:
        """Add a user message to history."""
        self.history.append({"role": "user", "content": content})

    def add_assistant_message(self, content: str) -> None:
        """Add an assistant message to history."""
        self.history.append({"role": "assistant", "content": content})

    def add_system_message(self, content: str) -> None:
        """Add a system message to history."""
        self.history.append({"role": "system", "content": content})

    def clear_history(self) -> None:
        """Clear conversation history."""
        self.history = []


@activity
async def _chat_activity(
    ctx: WorkflowContext,  # noqa: ARG001
    *,
    model: str,
    messages: list[dict[str, str]],
    tools: list[Any] | None = None,
    response_model: type | None = None,
    deps_dict: dict[str, Any],  # noqa: ARG001 - for logging/debugging
    turn: int,  # noqa: ARG001 - used in activity ID
    **call_params: Any,
) -> dict[str, Any]:
    """Internal: Execute LLM call as durable activity."""
    llm = _import_mirascope()
    provider = model.split("/")[0] if "/" in model else "unknown"

    def convert_messages(msgs: list[dict[str, str]]) -> list[Any]:
        result: list[Any] = []
        for msg in msgs:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "system":
                result.append(llm.messages.system(content))
            elif role == "assistant":
                # Mirascope V2: assistant messages require model_id and provider_id
                result.append(llm.messages.assistant(content, model_id=model, provider_id=provider))
            else:
                result.append(llm.messages.user(content))
        return result

    @llm.call(model, tools=tools, response_model=response_model, **call_params)  # type: ignore[misc]
    async def _call() -> list[Any]:
        return convert_messages(messages)

    response = await _call()

    # Handle structured output (response_model)
    if response_model is not None and hasattr(response, "model_dump"):
        return {
            "content": "",
            "model": model,
            "provider": provider,
            "structured_output": response.model_dump(),
        }

    return DurableResponse.from_mirascope(response, provider).to_dict()


class DurableAgent(Generic[T]):
    """
    Base class for durable agents with llm.Context support.

    Integrates Mirascope V2's llm.Context with Edda's durable execution:
    - Each chat turn is a separate durable activity (cached & replayable)
    - llm.Context provides dependency injection to prompts and tools
    - Conversation history is automatically managed via DurableDeps

    Subclass and override:
    - `model`: The LLM model string (e.g., "anthropic/claude-sonnet-4-20250514")
    - `build_prompt()`: Construct the prompt with access to ctx.deps
    - `get_tools()`: Return list of @llm.tool() decorated functions

    Attributes:
        model: The model string in "provider/model" format
        response_model: Optional Pydantic model for structured output

    Example:
        >>> class MyAgent(DurableAgent[MyDeps]):
        ...     model = "anthropic/claude-sonnet-4-20250514"
        ...
        ...     def build_prompt(self, ctx, message):
        ...         return [
        ...             llm.messages.system(f"Context: {ctx.deps.some_data}"),
        ...             llm.messages.user(message),
        ...         ]
        ...
        >>> @workflow
        ... async def my_workflow(ctx: WorkflowContext, query: str) -> str:
        ...     deps = MyDeps(some_data="value")
        ...     agent = MyAgent(ctx)
        ...     response = await agent.chat(deps, query)
        ...     return response["content"]
    """

    model: str = "anthropic/claude-sonnet-4-20250514"
    response_model: type | None = None

    def __init__(self, workflow_ctx: WorkflowContext) -> None:
        """
        Initialize the agent with a workflow context.

        Args:
            workflow_ctx: The Edda WorkflowContext for durable execution
        """
        self._workflow_ctx = workflow_ctx
        self._turn_count = 0

    def get_tools(self) -> list[Any] | None:
        """
        Override to provide tools for the agent.

        Tools should be decorated with @llm.tool() and can access
        ctx: llm.Context[T] as their first parameter.

        Returns:
            List of tool functions, or None if no tools
        """
        return None

    def build_prompt(self, ctx: Any, message: str) -> list[Any]:
        """
        Override to build the prompt for each turn.

        Access dependencies via ctx.deps. The returned messages
        will be sent to the LLM.

        Args:
            ctx: llm.Context[T] with access to deps
            message: The user message for this turn

        Returns:
            List of llm.messages (system, user, assistant)

        Example:
            >>> def build_prompt(self, ctx, message):
            ...     llm = _import_mirascope()
            ...     return [
            ...         llm.messages.system(f"Data: {ctx.deps.my_data}"),
            ...         llm.messages.user(message),
            ...     ]
        """
        llm = _import_mirascope()
        messages: list[Any] = []

        # Extract provider from model string for assistant messages
        provider = self.model.split("/")[0] if "/" in self.model else "unknown"

        # Include history from DurableDeps if available
        history = getattr(ctx.deps, "history", [])
        for msg in history:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "system":
                messages.append(llm.messages.system(content))
            elif role == "assistant":
                # Mirascope V2: assistant messages require model_id and provider_id
                messages.append(
                    llm.messages.assistant(content, model_id=self.model, provider_id=provider)
                )
            else:
                messages.append(llm.messages.user(content))

        # Add the new user message
        messages.append(llm.messages.user(message))
        return messages

    async def chat(
        self,
        deps: T | DurableDeps[T],
        message: str,
        **call_params: Any,
    ) -> dict[str, Any]:
        """
        Send a message and get a response.

        Each call is a separate durable activity - results are cached
        and replayed on workflow recovery.

        Args:
            deps: Dependency data (raw or wrapped in DurableDeps)
            message: User message to send
            **call_params: Additional LLM call parameters

        Returns:
            DurableResponse as dict with keys:
            - content: Response text
            - model: Model used
            - provider: Provider name
            - tool_calls: List of tool calls (if any)
            - usage: Token usage stats
        """
        self._turn_count += 1
        llm = _import_mirascope()

        # Wrap in DurableDeps if needed
        durable_deps = deps if isinstance(deps, DurableDeps) else DurableDeps(data=deps)

        # Add user message to history
        durable_deps.add_user_message(message)

        # Build llm.Context with the actual data
        llm_ctx = llm.Context(deps=durable_deps.data)

        # Build prompt using the context
        prompt_messages = self.build_prompt(llm_ctx, message)

        # Convert to serializable format for activity
        serializable_messages = self._messages_to_dict(prompt_messages)

        # Execute as durable activity
        # The @activity decorator transforms the function signature, but mypy doesn't understand it
        response: dict[str, Any] = await _chat_activity(  # type: ignore[misc, call-arg]
            self._workflow_ctx,  # type: ignore[arg-type]
            model=self.model,
            messages=serializable_messages,
            tools=self.get_tools(),
            response_model=self.response_model,
            deps_dict=durable_deps.to_dict(),
            turn=self._turn_count,
            **call_params,
        )

        # Add assistant response to history
        assistant_content = response.get("content", "")
        if assistant_content:
            durable_deps.add_assistant_message(assistant_content)

        return response

    def _messages_to_dict(self, messages: list[Any]) -> list[dict[str, str]]:
        """Convert llm.messages to serializable dicts."""
        result = []
        for msg in messages:
            if isinstance(msg, dict):
                result.append(msg)
            elif hasattr(msg, "role") and hasattr(msg, "content"):
                content = self._extract_text_content(msg.content)
                result.append({"role": msg.role, "content": content})
            elif hasattr(msg, "content"):
                content = self._extract_text_content(msg.content)
                result.append({"role": "user", "content": content})
            else:
                result.append({"role": "user", "content": str(msg)})
        return result

    def _extract_text_content(self, content: Any) -> str:
        """
        Extract text from Mirascope V2 content format.

        Mirascope V2 content can be:
        - A plain string
        - A list of Text/ContentBlock objects with .text attribute
        - None

        Args:
            content: The content to extract text from.

        Returns:
            Extracted text as a string.
        """
        if content is None:
            return ""
        if isinstance(content, str):
            return content
        # Handle Mirascope V2's list of Text/ContentBlock objects
        if isinstance(content, list):
            text_parts = []
            for item in content:
                if hasattr(item, "text"):
                    text_parts.append(item.text)
                elif isinstance(item, str):
                    text_parts.append(item)
                else:
                    text_parts.append(str(item))
            return "".join(text_parts)
        return str(content)

    async def chat_with_tool_loop(
        self,
        deps: T | DurableDeps[T],
        message: str,
        tool_executor: Any | None = None,
        max_iterations: int = 10,
        **call_params: Any,
    ) -> dict[str, Any]:
        """
        Chat with automatic tool execution loop.

        Continues calling tools until the model stops requesting them
        or max_iterations is reached.

        Args:
            deps: Dependency data
            message: Initial user message
            tool_executor: Optional callable(tool_name, tool_args) -> result.
                          If None, tools are not executed.
            max_iterations: Maximum tool call iterations
            **call_params: Additional LLM call parameters

        Returns:
            Final response after tool loop completes
        """
        response = await self.chat(deps, message, **call_params)

        iteration = 0
        while response.get("tool_calls") and iteration < max_iterations:
            if tool_executor is None:
                # No executor provided, return with tool_calls
                break

            # Execute tools
            tool_outputs = []
            for tc in response["tool_calls"]:
                tool_name = tc.get("name")
                tool_args = tc.get("args", {})
                try:
                    result = await tool_executor(tool_name, tool_args)
                    tool_outputs.append({"tool": tool_name, "output": str(result)})
                except Exception as e:
                    tool_outputs.append({"tool": tool_name, "error": str(e)})

            # Format tool results and continue conversation
            tool_results_str = "\n".join(
                f"Tool {to['tool']}: {to.get('output', to.get('error', 'Unknown'))}"
                for to in tool_outputs
            )
            response = await self.chat(deps, f"Tool results:\n{tool_results_str}", **call_params)
            iteration += 1

        return response
