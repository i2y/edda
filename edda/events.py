"""
Event handling module for Edda framework.

This module provides CloudEvents integration including wait_event and send_event.
"""

import uuid
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, TypeVar, cast

import httpx
from cloudevents.conversion import to_structured
from cloudevents.http import CloudEvent
from pydantic import BaseModel

from edda.pydantic_utils import from_json_dict, is_pydantic_instance, to_json_dict

if TYPE_CHECKING:
    from edda.context import WorkflowContext

T = TypeVar("T", bound=BaseModel)


@dataclass(frozen=True)
class ReceivedEvent:
    """
    Represents a CloudEvent received by a workflow.

    This class provides structured access to both the event payload (data)
    and CloudEvents metadata (type, source, id, time, etc.).

    Attributes:
        data: The event payload (JSON dict or Pydantic model)
        type: CloudEvent type (e.g., "payment.completed")
        source: CloudEvent source (e.g., "payment-service")
        id: Unique event identifier
        time: Event timestamp (ISO 8601 format)
        datacontenttype: Content type of the data (typically "application/json")
        subject: Subject of the event (optional CloudEvents extension)
        extensions: Additional CloudEvents extension attributes

    Example:
        >>> # Without Pydantic model
        >>> event = await wait_event(ctx, "payment.completed")
        >>> amount = event.data["amount"]
        >>> order_id = event.data["order_id"]
        >>>
        >>> # With Pydantic model (type-safe)
        >>> event = await wait_event(ctx, "payment.completed", model=PaymentCompleted)
        >>> amount = event.data.amount  # Type-safe access
        >>> order_id = event.data.order_id  # IDE completion
        >>>
        >>> # Access CloudEvents metadata
        >>> event_source = event.source
        >>> event_time = event.time
        >>> event_id = event.id
    """

    # Event payload (JSON dict or Pydantic model)
    data: dict[str, Any] | BaseModel

    # CloudEvents standard attributes
    type: str
    source: str
    id: str
    time: str | None = None
    datacontenttype: str | None = None
    subject: str | None = None

    # CloudEvents extension attributes
    extensions: dict[str, Any] = field(default_factory=dict)


class WaitForEventException(Exception):
    """
    Exception raised to pause workflow execution while waiting for an event.

    This is an internal exception used to signal that the workflow should be
    paused and resumed when the specified event arrives.
    """

    def __init__(
        self,
        event_type: str,
        timeout_seconds: int | None = None,
        activity_id: str | None = None,
    ):
        self.event_type = event_type
        self.timeout_seconds = timeout_seconds
        self.activity_id = activity_id
        super().__init__(f"Waiting for event: {event_type}")


class WaitForTimerException(Exception):
    """
    Exception raised to pause workflow execution while waiting for a timer.

    This is an internal exception used to signal that the workflow should be
    paused and resumed when the timer expires.
    """

    def __init__(
        self,
        duration_seconds: int,
        expires_at: "datetime",
        timer_id: str,
        activity_id: str | None = None,
    ):
        self.duration_seconds = duration_seconds
        self.expires_at = expires_at
        self.timer_id = timer_id
        self.activity_id = activity_id
        super().__init__(f"Waiting for timer: {timer_id} ({duration_seconds}s)")


class EventTimeoutError(Exception):
    """
    Exception raised when wait_event() times out.

    This exception is raised when an event does not arrive within the
    specified timeout period. The workflow can catch this exception to
    handle timeout scenarios gracefully.

    Example:
        try:
            event = await wait_event(ctx, "payment.completed", timeout_seconds=60)
        except EventTimeoutError:
            # Handle timeout - maybe send reminder or cancel order
            await send_notification("Payment timeout")
    """

    def __init__(self, event_type: str, timeout_seconds: int):
        self.event_type = event_type
        self.timeout_seconds = timeout_seconds
        super().__init__(f"Event '{event_type}' did not arrive within {timeout_seconds} seconds")


async def wait_event(
    ctx: "WorkflowContext",
    event_type: str,
    timeout_seconds: int | None = None,
    model: type[T] | None = None,
    event_id: str | None = None,
) -> ReceivedEvent:
    """
    Wait for a CloudEvent to arrive.

    This function pauses the workflow execution until a matching CloudEvent is received.
    During replay, it returns the cached event data and metadata.

    Args:
        ctx: Workflow context
        event_type: CloudEvent type to wait for (e.g., "payment.completed")
        timeout_seconds: Optional timeout in seconds
        model: Optional Pydantic model class to convert event data to
        event_id: Optional event identifier (auto-generated if not provided)

    Returns:
        ReceivedEvent object containing event data and CloudEvents metadata.
        If model is provided, ReceivedEvent.data will be a Pydantic model instance.

    Note:
        Events are delivered to specific workflow instances based on instance_id.
        No filter function is needed since subscriptions are already instance-specific.

    Raises:
        WaitForEventException: During normal execution to pause the workflow
        TimeoutError: If timeout is reached

    Example:
        >>> # Without Pydantic (dict access)
        >>> @saga
        ... async def order_workflow(ctx: WorkflowContext, order_id: str):
        ...     payment_event = await wait_event(ctx, "payment.completed")
        ...     amount = payment_event.data["amount"]
        ...     order_id = payment_event.data["order_id"]
        ...
        >>> # With Pydantic (type-safe access)
        >>> @saga
        ... async def order_workflow_typed(ctx: WorkflowContext, order_id: str):
        ...     payment_event = await wait_event(
        ...         ctx,
        ...         event_type="payment.completed",
        ...         model=PaymentCompleted
        ...     )
        ...     # Type-safe access with IDE completion
        ...     amount = payment_event.data.amount
        ...     order_id = payment_event.data.order_id
        ...     transaction_id = payment_event.data.transaction_id
        ...
        ...     # Access CloudEvents metadata
        ...     event_source = payment_event.source
        ...     event_time = payment_event.time
        ...
        ...     # Continue with order fulfillment
        ...     await ship_order(ctx, order_id, amount)
        ...
        >>> # Concurrent event waiting (with explicit IDs)
        >>> @saga
        ... async def multi_event_workflow(ctx: WorkflowContext, order_id: str):
        ...     payment, inventory = await asyncio.gather(
        ...         wait_event(ctx, "payment.completed", event_id="payment"),
        ...         wait_event(ctx, "inventory.reserved", event_id="inventory"),
        ...     )
    """
    # Resolve activity ID (explicit or auto-generated)
    if event_id is None:
        # Auto-generate event_id using context's generator
        activity_id = ctx._generate_activity_id(f"wait_event_{event_type}")
    else:
        activity_id = event_id

    # Record activity ID execution
    ctx._record_activity_id(activity_id)

    # During replay, return cached event data
    if ctx.is_replaying:
        found, cached_result = ctx._get_cached_result(activity_id)
        if found:
            # Check if this was an error (timeout)
            if isinstance(cached_result, dict) and cached_result.get("_error"):
                error_type = cached_result.get("error_type", "Exception")
                error_message = cached_result.get("error_message", "Unknown error")
                raise Exception(f"{error_type}: {error_message}")

            # Convert data to Pydantic model if requested
            if model is not None and isinstance(cached_result.data, dict):
                converted_data = from_json_dict(cached_result.data, model)
                cached_result = replace(cached_result, data=converted_data)

            # Return cached event data
            return cached_result  # type: ignore[no-any-return]

    # During normal execution, raise exception to pause workflow execution
    # The ReplayEngine will catch this and atomically:
    # 1. Register event subscription
    # 2. Update activity ID
    # 3. Release lock
    # This ensures distributed coroutines work correctly in multi-Pod environments
    raise WaitForEventException(
        event_type=event_type,
        timeout_seconds=timeout_seconds,
        activity_id=activity_id,
    )


async def wait_timer(
    ctx: "WorkflowContext",
    duration_seconds: int,
    timer_id: str | None = None,
) -> None:
    """
    Wait for a specific duration (timer).

    This function pauses the workflow execution until the specified duration has elapsed.
    During replay, it returns immediately without waiting.

    Args:
        ctx: Workflow context
        duration_seconds: Duration to wait in seconds
        timer_id: Optional timer identifier (auto-generated if not provided)

    Note:
        Timers are checked periodically by a background task. The actual resume time
        may be slightly later than the specified duration depending on the check interval.

        For concurrent timer waiting, provide explicit timer_id:
        >>> results = await asyncio.gather(
        ...     wait_timer(ctx, 60, timer_id="short"),
        ...     wait_timer(ctx, 120, timer_id="long"),
        ... )

    Raises:
        WaitForTimerException: During normal execution to pause the workflow

    Example:
        >>> @saga
        ... async def order_workflow(ctx: WorkflowContext, order_id: str):
        ...     # Create order
        ...     await create_order(ctx, order_id)
        ...
        ...     # Wait 60 seconds for payment
        ...     await wait_timer(ctx, duration_seconds=60)
        ...
        ...     # Check payment status
        ...     await check_payment_status(ctx, order_id)
    """
    # Resolve activity ID (timer_id or auto-generated)
    if timer_id is None:
        # Auto-generate using context's generator
        activity_id = ctx._generate_activity_id("wait_timer")
        timer_id = activity_id  # Use same ID for timer subscription
    else:
        activity_id = timer_id

    # Record activity ID execution
    ctx._record_activity_id(activity_id)

    # During replay, return immediately (timer has already expired)
    if ctx.is_replaying:
        found, cached_result = ctx._get_cached_result(activity_id)
        if found:
            # Check if this was an error
            if isinstance(cached_result, dict) and cached_result.get("_error"):
                error_type = cached_result.get("error_type", "Exception")
                error_message = cached_result.get("error_message", "Unknown error")
                raise Exception(f"{error_type}: {error_message}")

            # Timer has expired, continue execution
            return

    # During normal execution, raise exception to pause workflow execution
    # Calculate absolute expiration time (deterministic for replay)
    # This calculation happens only once (during initial execution)
    # and the absolute time is stored in the exception.
    expires_at = datetime.now(UTC) + timedelta(seconds=duration_seconds)

    # The ReplayEngine will catch this and atomically:
    # 1. Register timer subscription (with expires_at)
    # 2. Update activity ID
    # 3. Release lock
    # This ensures distributed coroutines work correctly in multi-Pod environments
    raise WaitForTimerException(
        duration_seconds=duration_seconds,
        expires_at=expires_at,
        timer_id=timer_id,
        activity_id=activity_id,
    )


async def wait_until(
    ctx: "WorkflowContext",
    until_time: "datetime",
    timer_id: str | None = None,
) -> None:
    """
    Wait until a specific absolute time.

    This function pauses the workflow execution until the specified absolute time is reached.
    During replay, it returns immediately without waiting.

    Args:
        ctx: Workflow context
        until_time: Absolute datetime to wait until (any timezone, will be converted to UTC)
        timer_id: Optional timer identifier (auto-generated if not provided)

    Note:
        Timers are checked periodically by a background task. The actual resume time
        may be slightly later than the specified time depending on the check interval.

    Raises:
        WaitForTimerException: During normal execution to pause the workflow

    Example:
        >>> from datetime import datetime, timedelta, UTC
        >>>
        >>> @saga
        ... async def order_workflow(ctx: WorkflowContext, order_id: str):
        ...     # Create order
        ...     await create_order(ctx, order_id)
        ...
        ...     # Wait until 3 PM today
        ...     deadline = datetime.now(UTC).replace(hour=15, minute=0, second=0)
        ...     await wait_until(ctx, deadline)
        ...
        ...     # Check payment status
        ...     await check_payment_status(ctx, order_id)
    """
    # Resolve activity ID (timer_id or auto-generated)
    if timer_id is None:
        # Auto-generate using context's generator
        activity_id = ctx._generate_activity_id("wait_until")
        timer_id = activity_id  # Use same ID for timer subscription
    else:
        activity_id = timer_id

    # Record activity ID execution
    ctx._record_activity_id(activity_id)

    # During replay, return immediately (timer has already expired)
    if ctx.is_replaying:
        found, cached_result = ctx._get_cached_result(activity_id)
        if found:
            # Check if this was an error
            if isinstance(cached_result, dict) and cached_result.get("_error"):
                error_type = cached_result.get("error_type", "Exception")
                error_message = cached_result.get("error_message", "Unknown error")
                raise Exception(f"{error_type}: {error_message}")

            # Timer has expired, continue execution
            return

    # During normal execution, raise exception to pause workflow execution
    # Convert to UTC if needed
    if until_time.tzinfo is None:
        # Naive datetime - assume UTC
        expires_at_utc = until_time.replace(tzinfo=UTC)
    else:
        # Aware datetime - convert to UTC
        expires_at_utc = until_time.astimezone(UTC)

    # The ReplayEngine will catch this and atomically:
    # 1. Register timer subscription (with expires_at)
    # 2. Update activity ID
    # 3. Release lock
    # This ensures distributed coroutines work correctly in multi-Pod environments
    raise WaitForTimerException(
        duration_seconds=0,  # Not applicable for wait_until
        expires_at=expires_at_utc,
        timer_id=timer_id,
        activity_id=activity_id,
    )


async def send_event(
    event_type: str,
    source: str,
    data: dict[str, Any] | BaseModel,
    broker_url: str = "http://broker-ingress.knative-eventing.svc.cluster.local",
    datacontenttype: str | None = None,
) -> None:
    """
    Send a CloudEvent to Knative Broker.

    Args:
        event_type: CloudEvent type (e.g., "order.created")
        source: CloudEvent source (e.g., "order-service")
        data: Event payload (JSON dict or Pydantic model)
        broker_url: Knative Broker URL
        datacontenttype: Content type (defaults to "application/json")

    Raises:
        httpx.HTTPError: If the HTTP request fails

    Example:
        >>> # With dict
        >>> await send_event("order.created", "order-service", {"order_id": "123"})
        >>>
        >>> # With Pydantic model (automatically converted to JSON)
        >>> order = OrderCreated(order_id="123", amount=99.99)
        >>> await send_event("order.created", "order-service", order)
    """
    # Convert Pydantic model to JSON dict
    data_dict: dict[str, Any]
    data_dict = to_json_dict(data) if is_pydantic_instance(data) else cast(dict[str, Any], data)

    # Create CloudEvent attributes
    attributes = {
        "type": event_type,
        "source": source,
        "id": str(uuid.uuid4()),
    }

    # Set datacontenttype if specified
    if datacontenttype:
        attributes["datacontenttype"] = datacontenttype

    event = CloudEvent(attributes, data_dict)

    # Convert to structured format (HTTP)
    headers, body = to_structured(event)

    # Send to Knative Broker via HTTP POST
    async with httpx.AsyncClient() as client:
        response = await client.post(
            broker_url,
            headers=headers,
            content=body,
            timeout=10.0,
        )
        response.raise_for_status()


async def send_event_from_dict(event_dict: dict[str, Any], broker_url: str | None = None) -> None:
    """
    Send a CloudEvent from a dictionary.

    Convenience function that extracts type, source, and data from a dict.

    Args:
        event_dict: Dictionary with 'type', 'source', and 'data' keys
        broker_url: Optional Knative Broker URL

    Raises:
        ValueError: If required keys are missing
        httpx.HTTPError: If the HTTP request fails
    """
    if "type" not in event_dict:
        raise ValueError("event_dict must contain 'type' key")
    if "source" not in event_dict:
        raise ValueError("event_dict must contain 'source' key")
    if "data" not in event_dict:
        raise ValueError("event_dict must contain 'data' key")

    kwargs = {
        "event_type": event_dict["type"],
        "source": event_dict["source"],
        "data": event_dict["data"],
    }

    if broker_url is not None:
        kwargs["broker_url"] = broker_url

    await send_event(**kwargs)
