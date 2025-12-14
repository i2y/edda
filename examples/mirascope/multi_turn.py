"""
Multi-turn conversation example for Edda + Mirascope V2 integration.

This example demonstrates:
- Using call_with_messages() for multi-turn conversations
- Maintaining conversation history in a durable workflow
- Each turn is a separate activity (durable)

Requirements:
    pip install 'edda-framework[mirascope]'

Environment:
    ANTHROPIC_API_KEY: Your Anthropic API key

Run with:
    uv run python -m examples.mirascope.multi_turn
"""

import asyncio
import os

from edda import EddaApp, WorkflowContext, workflow
from edda.integrations.mirascope import call_with_messages

# Check for API key
if not os.environ.get("ANTHROPIC_API_KEY"):
    print("Warning: ANTHROPIC_API_KEY not set. Set it before running this example.")
    print("  export ANTHROPIC_API_KEY=your_api_key")


@workflow
async def multi_turn_conversation(
    ctx: WorkflowContext,
    initial_question: str,
    follow_up_questions: list[str],
) -> list[dict[str, str]]:
    """
    A multi-turn conversation workflow.

    Each turn is a separate durable activity:
    - If the workflow crashes mid-conversation, it resumes from the last turn
    - Previously completed turns are replayed from cache (no LLM re-calls)
    """
    print("\n[Workflow] Starting multi-turn conversation")

    # Initialize conversation history with system prompt
    messages: list[dict[str, str]] = [
        {
            "role": "system",
            "content": "You are a helpful assistant specializing in explaining technical concepts. "
            "Keep your responses concise but informative.",
        },
    ]

    # Store all exchanges
    exchanges: list[dict[str, str]] = []

    # Turn 1: Initial question
    print(f"\n[Turn 1] User: {initial_question}")
    messages.append({"role": "user", "content": initial_question})

    # V2: Use unified "provider/model" format
    response = await call_with_messages(
        ctx,
        model="anthropic/claude-sonnet-4-20250514",
        messages=messages,
    )

    assistant_response = response["content"]
    print(f"[Turn 1] Assistant: {assistant_response[:200]}...")
    messages.append({"role": "assistant", "content": assistant_response})
    exchanges.append({"user": initial_question, "assistant": assistant_response})

    # Follow-up turns
    for i, question in enumerate(follow_up_questions, start=2):
        print(f"\n[Turn {i}] User: {question}")
        messages.append({"role": "user", "content": question})

        # V2: Use unified "provider/model" format
        response = await call_with_messages(
            ctx,
            model="anthropic/claude-sonnet-4-20250514",
            messages=messages,
        )

        assistant_response = response["content"]
        print(f"[Turn {i}] Assistant: {assistant_response[:200]}...")
        messages.append({"role": "assistant", "content": assistant_response})
        exchanges.append({"user": question, "assistant": assistant_response})

    print("\n[Workflow] Conversation completed!")
    return exchanges


async def main():
    """Main function to demonstrate multi-turn conversations."""
    print("=" * 60)
    print("Edda Framework - Multi-turn Conversation Example")
    print("=" * 60)

    # Create Edda app
    app = EddaApp(
        service_name="multi-turn-example",
        db_url="sqlite:///multi_turn_demo.db",
    )

    # Initialize the app
    await app.initialize()

    try:
        # Start a multi-turn conversation about durable execution
        initial_question = "What is durable execution and why is it useful?"
        follow_up_questions = [
            "How does it handle failures and crashes?",
            "Can you give me a practical example of when I would use it?",
        ]

        print("\n>>> Starting multi-turn conversation workflow")
        # start() runs the workflow to completion
        instance_id = await multi_turn_conversation.start(
            initial_question=initial_question,
            follow_up_questions=follow_up_questions,
        )
        print(f">>> Workflow completed with ID: {instance_id}")

        # Get the result from storage
        instance = await app.storage.get_instance(instance_id)
        output = instance.get("output_data", {}) if instance else {}
        if output.get("result"):
            print("\n>>> Conversation Summary:")
            print("-" * 40)
            for i, exchange in enumerate(output["result"], start=1):
                print(f"Turn {i}:")
                print(f"  User: {exchange['user']}")
                print(f"  Assistant: {exchange['assistant'][:150]}...")
                print()

    except Exception as e:
        print(f"\n>>> Error: {e}")
        print(">>> Make sure ANTHROPIC_API_KEY is set correctly.")

    finally:
        await app.shutdown()
        print("=" * 60)
        print("Example completed!")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
