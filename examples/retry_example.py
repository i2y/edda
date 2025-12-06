"""
Automatic Retry Example

Demonstrates:
- Default retry behavior (5 attempts, exponential backoff)
- Custom retry policies
- TerminalError for non-retryable errors
- RetryExhaustedError handling
- Retry metadata in workflow history

Run:
    python examples/retry_example.py
"""

import asyncio
from datetime import UTC, datetime

from edda import (
    EddaApp,
    RetryExhaustedError,
    RetryPolicy,
    TerminalError,
    WorkflowContext,
    activity,
    workflow,
)

# ============================================================================
# Example 1: Default Retry (5 attempts, exponential backoff)
# ============================================================================

# Simulated external API call counter
api_call_count = 0


@activity  # Uses default retry policy (5 attempts)
async def call_flaky_api(_ctx: WorkflowContext, _url: str) -> dict:
    """
    Simulates a flaky API that fails the first 2 times, then succeeds.

    Default retry schedule:
    - Attempt 1: Immediate
    - Attempt 2: 1 second delay
    - Attempt 3: 2 seconds delay (succeeds here)
    """
    global api_call_count
    api_call_count += 1

    print(f"🔄 API call attempt #{api_call_count} at {datetime.now(UTC).strftime('%H:%M:%S')}")

    if api_call_count < 3:
        # Simulate transient failure (network timeout)
        raise ConnectionError(f"Network timeout (attempt {api_call_count})")

    # Success on 3rd attempt
    print("✅ API call succeeded!")
    return {"data": "API response", "attempts": api_call_count}


@workflow
async def default_retry_workflow(ctx: WorkflowContext, url: str) -> dict:
    """Demonstrates default retry behavior."""
    print("\n=== Example 1: Default Retry ===")
    print("Activity will fail twice, then succeed on 3rd attempt")
    print("Watch for 1s and 2s delays between attempts\n")

    result = await call_flaky_api(ctx, url)
    print(f"\n📊 Result: {result}")
    print(f"Total attempts: {result['attempts']}")

    return {"status": "completed", "result": result}


# ============================================================================
# Example 2: Custom Retry Policy (aggressive retry for critical operations)
# ============================================================================

payment_attempt_count = 0


@activity(
    retry_policy=RetryPolicy(
        max_attempts=10,  # More attempts for critical operations
        initial_interval=0.5,  # Faster initial retry
        backoff_coefficient=1.5,  # Slower exponential growth
        max_interval=10.0,  # Cap at 10 seconds
        max_duration=60.0,  # Stop after 1 minute total
    )
)
async def process_critical_payment(_ctx: WorkflowContext, amount: float) -> dict:
    """
    Critical payment processing with aggressive retry policy.

    Custom retry schedule:
    - Attempt 1: Immediate
    - Attempt 2: 0.5 seconds
    - Attempt 3: 0.75 seconds (0.5 * 1.5)
    - Attempt 4: 1.125 seconds (0.75 * 1.5)
    - ... (up to 10 attempts or 60 seconds total)
    """
    global payment_attempt_count
    payment_attempt_count += 1

    print(f"💳 Payment attempt #{payment_attempt_count} for ${amount}")

    if payment_attempt_count < 4:
        raise ConnectionError(f"Payment gateway timeout (attempt {payment_attempt_count})")

    print("✅ Payment processed successfully!")
    return {"transaction_id": f"TXN-{payment_attempt_count}", "amount": amount}


@workflow
async def custom_retry_workflow(ctx: WorkflowContext, amount: float) -> dict:
    """Demonstrates custom retry policy."""
    print("\n=== Example 2: Custom Retry Policy ===")
    print("Aggressive retry: 10 max attempts, faster initial retry\n")

    result = await process_critical_payment(ctx, amount)
    print(f"\n📊 Payment result: {result}")

    return {"status": "completed", "transaction_id": result["transaction_id"]}


# ============================================================================
# Example 3: TerminalError (non-retryable errors)
# ============================================================================


@activity
async def validate_user(_ctx: WorkflowContext, user_id: str) -> dict:
    """
    Validates user existence.

    Uses TerminalError for permanent failures (no retry).
    """
    print(f"🔍 Validating user: {user_id}")

    # Simulate validation logic
    if user_id == "invalid_user":
        # Don't retry - user doesn't exist (permanent error)
        print("❌ User not found - raising TerminalError (no retry)")
        raise TerminalError(f"User {user_id} not found")

    if user_id == "banned_user":
        # Business rule violation - don't retry
        print("❌ User is banned - raising TerminalError (no retry)")
        raise TerminalError(f"User {user_id} is banned")

    print("✅ User validated successfully")
    return {"user_id": user_id, "valid": True}


@workflow
async def terminal_error_workflow(ctx: WorkflowContext, user_id: str) -> dict:
    """Demonstrates TerminalError usage."""
    print("\n=== Example 3: TerminalError (Non-Retryable) ===")
    print(f"Attempting to validate user: {user_id}\n")

    try:
        result = await validate_user(ctx, user_id)
        return {"status": "completed", "result": result}
    except TerminalError as e:
        print(f"\n⚠️ TerminalError caught: {e}")
        print("No retry attempted - error is permanent")
        return {"status": "failed", "error": str(e)}


# ============================================================================
# Example 4: RetryExhaustedError (all attempts failed)
# ============================================================================

order_attempt_count = 0


@activity(retry_policy=RetryPolicy(max_attempts=3, initial_interval=0.1))
async def place_order(_ctx: WorkflowContext, order_id: str) -> dict:
    """
    Order placement that always fails (to demonstrate RetryExhaustedError).
    """
    global order_attempt_count
    order_attempt_count += 1

    print(f"📦 Order attempt #{order_attempt_count} for {order_id}")

    # Simulate persistent failure
    raise ConnectionError(f"Order service unavailable (attempt {order_attempt_count})")


@activity
async def notify_failure(_ctx: WorkflowContext, order_id: str, error: str) -> dict:
    """Notification sent when order placement fails."""
    print(f"📧 Sending failure notification for order {order_id}")
    print(f"   Error: {error}")
    return {"notified": True, "order_id": order_id}


@workflow
async def retry_exhausted_workflow(ctx: WorkflowContext, order_id: str) -> dict:
    """Demonstrates handling RetryExhaustedError."""
    print("\n=== Example 4: RetryExhaustedError Handling ===")
    print("Activity will fail 3 times and raise RetryExhaustedError\n")

    try:
        # This will fail all 3 attempts
        result = await place_order(ctx, order_id)
        return {"status": "completed", "result": result}

    except RetryExhaustedError as e:
        print("\n⚠️ RetryExhaustedError caught!")
        print(f"   Message: {e}")
        print(f"   Original error: {e.__cause__}")  # Exception chaining

        # Fallback: Notify team and mark as failed
        await notify_failure(ctx, order_id, error=str(e))

        return {
            "status": "failed_after_retry",
            "order_id": order_id,
            "error": str(e),
            "total_attempts": order_attempt_count,
        }


# ============================================================================
# Example 5: Accessing Retry Metadata from History
# ============================================================================


@activity
async def unreliable_service(_ctx: WorkflowContext, service_name: str) -> dict:
    """Service that fails once before succeeding."""
    # Access current step to check if this is a retry
    print(f"🔧 Calling {service_name} service...")

    # Simple counter-based failure (fails first time)
    if not hasattr(unreliable_service, "called"):
        unreliable_service.called = True
        print("   First call - simulating failure")
        raise ConnectionError(f"{service_name} service timeout")

    print("   Second call - success!")
    return {"service": service_name, "status": "operational"}


@workflow
async def retry_metadata_workflow(ctx: WorkflowContext, service_name: str) -> dict:
    """Demonstrates retry metadata in history."""
    print("\n=== Example 5: Retry Metadata ===")
    print("Activity will fail once, then succeed")
    print("Check workflow history for retry metadata\n")

    result = await unreliable_service(ctx, service_name)

    print(f"\n📊 Service result: {result}")
    print(
        f"\n💡 Tip: Check workflow history (instance_id: {ctx.instance_id})"
        f"\n   to see retry_metadata with:"
        f"\n   - total_attempts"
        f"\n   - total_duration_ms"
        f"\n   - last_error details"
        f"\n   - errors array"
    )

    return {"status": "completed", "result": result}


# ============================================================================
# Main Application
# ============================================================================


async def main():
    """Run all retry examples."""
    print("=" * 70)
    print("🔄 Edda Automatic Retry Examples")
    print("=" * 70)

    # Initialize Edda
    app = EddaApp(service_name="retry-examples", db_url="sqlite:///retry_examples.db")
    await app.initialize()

    try:
        # Example 1: Default retry
        global api_call_count
        api_call_count = 0  # Reset counter
        instance_id_1 = await default_retry_workflow.start(url="https://api.example.com/data")
        print(f"Instance ID: {instance_id_1}\n")

        # Wait a bit before next example
        await asyncio.sleep(1)

        # Example 2: Custom retry policy
        global payment_attempt_count
        payment_attempt_count = 0  # Reset counter
        instance_id_2 = await custom_retry_workflow.start(amount=99.99)
        print(f"Instance ID: {instance_id_2}\n")

        await asyncio.sleep(1)

        # Example 3: TerminalError (no retry)
        instance_id_3 = await terminal_error_workflow.start(user_id="invalid_user")
        print(f"Instance ID: {instance_id_3}\n")

        await asyncio.sleep(1)

        # Example 4: RetryExhaustedError
        global order_attempt_count
        order_attempt_count = 0  # Reset counter
        instance_id_4 = await retry_exhausted_workflow.start(order_id="ORDER-123")
        print(f"Instance ID: {instance_id_4}\n")

        await asyncio.sleep(1)

        # Example 5: Retry metadata
        # Reset attribute for demo
        if hasattr(unreliable_service, "called"):
            delattr(unreliable_service, "called")
        instance_id_5 = await retry_metadata_workflow.start(service_name="payment-gateway")
        print(f"Instance ID: {instance_id_5}\n")

        print("\n" + "=" * 70)
        print("✅ All examples completed!")
        print("=" * 70)
        print(
            "\n💡 View workflow history in Viewer UI:"
            "\n   python viewer_app.py"
            "\n   http://localhost:8080"
        )

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
