"""
Simple durable LLM call example for Edda + Mirascope V2 integration.

This example demonstrates:
- Using @durable_call decorator for LLM calls
- Using call() function for ad-hoc LLM calls
- Automatic caching and crash recovery

Requirements:
    pip install 'edda-framework[mirascope]'
    # or
    pip install 'mirascope[anthropic]'

Environment:
    ANTHROPIC_API_KEY: Your Anthropic API key

Run with:
    uv run python -m examples.mirascope.simple_call
"""

import asyncio
import os

from edda import EddaApp, WorkflowContext, workflow
from edda.integrations.mirascope import call, durable_call

# Check for API key
if not os.environ.get("ANTHROPIC_API_KEY"):
    print("Warning: ANTHROPIC_API_KEY not set. Set it before running this example.")
    print("  export ANTHROPIC_API_KEY=your_api_key")


# Method 1: Using @durable_call decorator (recommended for reusable LLM calls)
# V2: Use unified "provider/model" format
@durable_call("anthropic/claude-sonnet-4-20250514")
async def summarize_text(text: str) -> str:
    """Summarize the given text."""
    return f"Please summarize the following text in 2-3 sentences:\n\n{text}"


@durable_call("anthropic/claude-sonnet-4-20250514")
async def translate_to_japanese(text: str) -> str:
    """Translate text to Japanese."""
    return f"Translate the following text to Japanese:\n\n{text}"


# Workflow using @durable_call decorated functions
@workflow
async def summarize_and_translate_workflow(
    ctx: WorkflowContext,
    text: str,
) -> dict[str, str]:
    """
    Workflow that summarizes text and translates the summary.

    Each LLM call is durable:
    - Results are cached in the database
    - If the workflow crashes, it resumes from the last completed call
    - Replaying the workflow doesn't re-call the LLM (saves cost!)
    """
    print("\n[Workflow] Starting summarize and translate workflow")

    # Step 1: Summarize the text
    print("[Workflow] Step 1: Summarizing text...")
    summary_response = await summarize_text(ctx, text)
    summary = summary_response["content"]
    print(f"[Workflow] Summary: {summary[:100]}...")

    # Step 2: Translate the summary to Japanese
    print("[Workflow] Step 2: Translating to Japanese...")
    translation_response = await translate_to_japanese(ctx, summary)
    translation = translation_response["content"]
    print(f"[Workflow] Translation: {translation[:100]}...")

    return {
        "original": text,
        "summary": summary,
        "translation": translation,
    }


# Method 2: Using call() function (for ad-hoc LLM calls)
@workflow
async def adhoc_call_workflow(ctx: WorkflowContext, question: str) -> str:
    """
    Workflow using the call() function for ad-hoc LLM calls.

    Use call() when you don't need a reusable decorated function.
    """
    print(f"\n[Workflow] Answering question: {question}")

    # V2: Use unified "provider/model" format
    response = await call(
        ctx,
        model="anthropic/claude-sonnet-4-20250514",
        prompt=question,
        system="You are a helpful assistant. Provide concise, accurate answers.",
    )

    answer = response["content"]
    print(f"[Workflow] Answer: {answer[:200]}...")

    return answer


async def main():
    """Main function to demonstrate durable LLM calls."""
    print("=" * 60)
    print("Edda Framework - Mirascope Integration Example")
    print("=" * 60)

    # Create Edda app
    app = EddaApp(
        service_name="mirascope-example",
        db_url="sqlite:///mirascope_demo.db",
    )

    # Initialize the app
    await app.initialize()

    try:
        # Example 1: Using @durable_call decorator
        sample_text = """
        The Edda framework provides durable execution for Python applications.
        It ensures that workflows can survive crashes and restarts by persisting
        state to a database. Combined with Mirascope, LLM calls become durable
        activities that are automatically cached and can be replayed efficiently.
        """

        print("\n>>> Example 1: Summarize and Translate Workflow")
        # start() runs the workflow to completion and returns the instance ID
        instance_id = await summarize_and_translate_workflow.start(text=sample_text)
        print(f">>> Workflow completed with ID: {instance_id}")

        # Get result from storage
        instance = await app.storage.get_instance(instance_id)
        output = instance.get("output_data", {}) if instance else {}
        if output.get("result"):
            print("\n>>> Result:")
            result = output["result"]
            print(f"  Summary: {result.get('summary', '')[:100]}...")
            print(f"  Translation: {result.get('translation', '')[:100]}...")

        # Example 2: Using call() function
        print("\n>>> Example 2: Ad-hoc Call Workflow")
        question = "What are the benefits of durable execution in software systems?"
        instance_id2 = await adhoc_call_workflow.start(question=question)
        print(f">>> Workflow completed with ID: {instance_id2}")

        instance2 = await app.storage.get_instance(instance_id2)
        output2 = instance2.get("output_data", {}) if instance2 else {}
        if output2.get("result"):
            print(f"\n>>> Answer: {output2['result'][:200]}...")

    except Exception as e:
        print(f"\n>>> Error: {e}")
        print(">>> Make sure ANTHROPIC_API_KEY is set correctly.")

    finally:
        await app.shutdown()
        print("\n" + "=" * 60)
        print("Example completed!")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
