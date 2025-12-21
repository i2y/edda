"""
Channel-based Message Queue System for Edda.

This module provides Erlang/Elixir mailbox-style messaging with support for:
- Broadcast mode: All subscribers receive all messages (fan-out pattern)
- Competing mode: Each message is processed by only one subscriber (producer-consumer pattern)

Key concepts:
- Channel: A named message queue with persistent storage
- Message: A data payload published to a channel
- Subscription: A workflow's interest in receiving messages from a channel

The channel system solves the "mailbox problem" where messages sent before
`receive()` is called would be lost. Messages are always queued and persist
until consumed.

Example:
    >>> from edda.channels import subscribe, receive, publish, ChannelMessage
    >>>
    >>> @workflow
    ... async def worker(ctx: WorkflowContext, id: str):
    ...     # Subscribe to a channel
    ...     await subscribe(ctx, "tasks", mode="competing")
    ...
    ...     while True:
    ...         # Receive messages (blocks until message available)
    ...         msg = await receive(ctx, "tasks")
    ...         await process(ctx, msg.data, activity_id=f"process:{msg.id}")
    ...         await ctx.recur()
    >>>
    >>> @workflow
    ... async def producer(ctx: WorkflowContext, task_data: dict):
    ...     await publish(ctx, "tasks", task_data)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, overload

if TYPE_CHECKING:
    from edda.context import WorkflowContext
    from edda.storage.protocol import StorageProtocol


# =============================================================================
# Data Classes
# =============================================================================


@dataclass(frozen=True)
class ChannelMessage:
    """
    A message received from a channel.

    Attributes:
        id: Unique message identifier
        channel: Channel name this message was received on
        data: Message payload (dict or bytes)
        metadata: Optional metadata (source, timestamp, etc.)
        published_at: When the message was published
    """

    id: str
    channel: str
    data: dict[str, Any] | bytes
    metadata: dict[str, Any] = field(default_factory=dict)
    published_at: datetime = field(default_factory=lambda: datetime.now(UTC))


# =============================================================================
# Exceptions
# =============================================================================


class WaitForChannelMessageException(Exception):
    """
    Raised to pause workflow execution until a channel message arrives.

    This exception is caught by the ReplayEngine to:
    1. Register the workflow as waiting for a channel message
    2. Release the workflow lock
    3. Update workflow status to 'waiting_for_message'

    The workflow will be resumed when a message is delivered to the channel.
    """

    def __init__(
        self,
        channel: str,
        timeout_seconds: int | None,
        activity_id: str,
    ) -> None:
        self.channel = channel
        self.timeout_seconds = timeout_seconds
        self.activity_id = activity_id
        # Calculate absolute timeout if specified
        self.timeout_at: datetime | None = None
        if timeout_seconds is not None:
            self.timeout_at = datetime.now(UTC) + timedelta(seconds=timeout_seconds)
        super().__init__(f"Waiting for message on channel: {channel}")


class WaitForTimerException(Exception):
    """
    Raised to pause workflow execution until a timer expires.

    This exception is caught by the ReplayEngine to:
    1. Register a timer subscription in the database
    2. Release the workflow lock
    3. Update workflow status to 'waiting_for_timer'

    The workflow will be resumed when the timer expires.
    """

    def __init__(
        self,
        duration_seconds: int,
        expires_at: datetime,
        timer_id: str,
        activity_id: str,
    ) -> None:
        self.duration_seconds = duration_seconds
        self.expires_at = expires_at
        self.timer_id = timer_id
        self.activity_id = activity_id
        super().__init__(f"Waiting for timer: {timer_id}")


class ChannelModeConflictError(Exception):
    """
    Raised when subscribing with a different mode than the channel's established mode.

    A channel's mode is locked when the first subscription is created. Subsequent
    subscriptions must use the same mode.
    """

    def __init__(self, channel: str, existing_mode: str, requested_mode: str) -> None:
        self.channel = channel
        self.existing_mode = existing_mode
        self.requested_mode = requested_mode
        super().__init__(
            f"Channel '{channel}' is already configured as '{existing_mode}' mode. "
            f"Cannot subscribe with '{requested_mode}' mode."
        )


# =============================================================================
# Subscription Functions
# =============================================================================


async def subscribe(
    ctx: WorkflowContext,
    channel: str,
    mode: str = "broadcast",
) -> None:
    """
    Subscribe to a channel for receiving messages.

    Args:
        ctx: Workflow context
        channel: Channel name to subscribe to
        mode: Subscription mode:
              - "broadcast": All subscribers receive all messages (fan-out pattern)
              - "competing": Each message goes to only one subscriber (work queue pattern)
              - "direct": Receive messages sent via send_to() to this instance

    Raises:
        ChannelModeConflictError: If the channel is already configured with a different mode
        ValueError: If mode is not 'broadcast', 'competing', or 'direct'

    The "direct" mode is syntactic sugar that subscribes to "channel:instance_id" internally,
    allowing simpler code when receiving direct messages:

        # Instead of this:
        direct_channel = f"notifications:{ctx.instance_id}"
        await subscribe(ctx, direct_channel, mode="broadcast")
        msg = await receive(ctx, direct_channel)

        # You can write:
        await subscribe(ctx, "notifications", mode="direct")
        msg = await receive(ctx, "notifications")

    Example:
        >>> @workflow
        ... async def event_handler(ctx: WorkflowContext, id: str):
        ...     # Subscribe to order events (all handlers receive all events)
        ...     await subscribe(ctx, "order.events", mode="broadcast")
        ...
        ...     while True:
        ...         event = await receive(ctx, "order.events")
        ...         await handle_event(ctx, event.data, activity_id=f"handle:{event.id}")
        ...         await ctx.recur()

        >>> @workflow
        ... async def job_worker(ctx: WorkflowContext, worker_id: str):
        ...     # Subscribe to job queue (each job processed by one worker)
        ...     await subscribe(ctx, "jobs", mode="competing")
        ...
        ...     while True:
        ...         job = await receive(ctx, "jobs")
        ...         await execute_job(ctx, job.data, activity_id=f"job:{job.id}")
        ...         await ctx.recur()

        >>> @workflow
        ... async def direct_receiver(ctx: WorkflowContext, id: str):
        ...     # Subscribe to receive direct messages via send_to()
        ...     await subscribe(ctx, "notifications", mode="direct")
        ...
        ...     msg = await receive(ctx, "notifications")
        ...     print(f"Received: {msg.data}")
    """
    actual_channel = channel
    actual_mode = mode

    if mode == "direct":
        # Transform to instance-specific channel
        actual_channel = f"{channel}:{ctx.instance_id}"
        actual_mode = "broadcast"
        ctx._record_direct_subscription(channel)
    elif mode not in ("broadcast", "competing"):
        raise ValueError(
            f"Invalid subscription mode: {mode}. Must be 'broadcast', 'competing', or 'direct'"
        )

    # Check for mode conflict
    existing_mode = await ctx.storage.get_channel_mode(actual_channel)
    if existing_mode is not None and existing_mode != actual_mode:
        raise ChannelModeConflictError(channel, existing_mode, mode)

    await ctx.storage.subscribe_to_channel(ctx.instance_id, actual_channel, actual_mode)


async def unsubscribe(
    ctx: WorkflowContext,
    channel: str,
) -> None:
    """
    Unsubscribe from a channel.

    Note: Workflows are automatically unsubscribed from all channels when they
    complete, fail, or are cancelled. Explicit unsubscribe is usually not necessary.

    For channels subscribed with mode="direct", use the original channel name
    (not the transformed "channel:instance_id" form).

    Args:
        ctx: Workflow context
        channel: Channel name to unsubscribe from
    """
    actual_channel = channel
    if ctx._is_direct_subscription(channel):
        actual_channel = f"{channel}:{ctx.instance_id}"
    await ctx.storage.unsubscribe_from_channel(ctx.instance_id, actual_channel)


# =============================================================================
# Message Receiving
# =============================================================================


async def receive(
    ctx: WorkflowContext,
    channel: str,
    timeout_seconds: int | None = None,
    message_id: str | None = None,
) -> ChannelMessage:
    """
    Receive a message from a channel.

    This function blocks (pauses the workflow) until a message is available
    on the channel. Messages are queued persistently, so messages published
    before this function is called will still be received.

    Args:
        ctx: Workflow context
        channel: Channel name to receive from
        timeout_seconds: Optional timeout in seconds
        message_id: Optional ID for concurrent waiting (auto-generated if not provided)

    Returns:
        ChannelMessage object containing data and metadata

    Raises:
        WaitForChannelMessageException: Raised to pause workflow (caught by ReplayEngine)
        TimeoutError: If timeout expires before message arrives

    Example:
        >>> @workflow
        ... async def consumer(ctx: WorkflowContext, id: str):
        ...     await subscribe(ctx, "tasks", mode="competing")
        ...
        ...     while True:
        ...         msg = await receive(ctx, "tasks")
        ...         await process(ctx, msg.data, activity_id=f"process:{msg.id}")
        ...         await ctx.recur()
    """
    # Transform channel for direct subscriptions
    actual_channel = channel
    if ctx._is_direct_subscription(channel):
        actual_channel = f"{channel}:{ctx.instance_id}"

    # Generate activity ID (use original channel name for deterministic replay)
    if message_id is None:
        activity_id = ctx._generate_activity_id(f"receive_{channel}")
    else:
        activity_id = message_id

    ctx._record_activity_id(activity_id)

    # During replay, return cached message
    if ctx.is_replaying:
        found, cached_result = ctx._get_cached_result(activity_id)
        if found:
            # Check for cached error
            if isinstance(cached_result, dict) and cached_result.get("_error"):
                error_type = cached_result.get("error_type", "Exception")
                error_message = cached_result.get("error_message", "Unknown error")
                if error_type == "TimeoutError":
                    raise TimeoutError(error_message)
                raise Exception(f"{error_type}: {error_message}")
            # Return cached ChannelMessage
            if isinstance(cached_result, ChannelMessage):
                return cached_result
            # Convert dict to ChannelMessage (from history)
            if isinstance(cached_result, dict):
                raw_data = cached_result.get("data", cached_result.get("payload", {}))
                data: dict[str, Any] | bytes = (
                    raw_data if isinstance(raw_data, (dict, bytes)) else {}
                )
                published_at_str = cached_result.get("published_at")
                published_at = (
                    datetime.fromisoformat(published_at_str)
                    if published_at_str
                    else datetime.now(UTC)
                )
                return ChannelMessage(
                    id=cached_result.get("id", "") or "",
                    channel=cached_result.get("channel", channel) or channel,
                    data=data,
                    metadata=cached_result.get("metadata") or {},
                    published_at=published_at,
                )
            raise RuntimeError(f"Unexpected cached result type: {type(cached_result)}")

    # Check for pending messages in the queue
    pending = await ctx.storage.get_pending_channel_messages(ctx.instance_id, actual_channel)
    if pending:
        # Get the first pending message
        msg_dict = pending[0]
        msg_id = msg_dict["message_id"]

        # For competing mode, try to claim the message
        subscription = await _get_subscription(ctx, actual_channel)
        if subscription and subscription.get("mode") == "competing":
            claimed = await ctx.storage.claim_channel_message(msg_id, ctx.instance_id)
            if not claimed:
                # Another worker claimed it, check next message
                # For simplicity, raise exception to retry
                raise WaitForChannelMessageException(
                    channel=actual_channel,
                    timeout_seconds=timeout_seconds,
                    activity_id=activity_id,
                )
            # Delete the message after claiming (competing mode)
            await ctx.storage.delete_channel_message(msg_id)
        else:
            # Broadcast mode - update cursor
            await ctx.storage.update_delivery_cursor(
                actual_channel, ctx.instance_id, msg_dict["id"]
            )

        # Build the message
        raw_data = msg_dict.get("data")
        data = raw_data if isinstance(raw_data, (dict, bytes)) else {}
        published_at_str = msg_dict.get("published_at")
        published_at = (
            datetime.fromisoformat(published_at_str)
            if isinstance(published_at_str, str)
            else (published_at_str if isinstance(published_at_str, datetime) else datetime.now(UTC))
        )

        message = ChannelMessage(
            id=msg_id,
            channel=channel,
            data=data,
            metadata=msg_dict.get("metadata") or {},
            published_at=published_at,
        )

        # Record in history for replay
        await ctx.storage.append_history(
            ctx.instance_id,
            activity_id,
            "ChannelMessageReceived",
            {
                "id": message.id,
                "channel": message.channel,
                "data": message.data,
                "metadata": message.metadata,
                "published_at": message.published_at.isoformat(),
            },
        )

        return message

    # No pending messages, raise exception to pause workflow
    raise WaitForChannelMessageException(
        channel=actual_channel,
        timeout_seconds=timeout_seconds,
        activity_id=activity_id,
    )


async def _get_subscription(ctx: WorkflowContext, channel: str) -> dict[str, Any] | None:
    """Get the subscription info for a channel."""
    return await ctx.storage.get_channel_subscription(ctx.instance_id, channel)


# =============================================================================
# Message Publishing
# =============================================================================


@overload
async def publish(
    ctx_or_storage: WorkflowContext,
    channel: str,
    data: dict[str, Any] | bytes,
    metadata: dict[str, Any] | None = None,
    *,
    target_instance_id: str | None = None,
    worker_id: str | None = None,
) -> str: ...


@overload
async def publish(
    ctx_or_storage: StorageProtocol,
    channel: str,
    data: dict[str, Any] | bytes,
    metadata: dict[str, Any] | None = None,
    *,
    target_instance_id: str | None = None,
    worker_id: str | None = None,
) -> str: ...


async def publish(
    ctx_or_storage: WorkflowContext | StorageProtocol,
    channel: str,
    data: dict[str, Any] | bytes,
    metadata: dict[str, Any] | None = None,
    *,
    target_instance_id: str | None = None,
    worker_id: str | None = None,
) -> str:
    """
    Publish a message to a channel.

    Can be called from within a workflow (with WorkflowContext) or from
    external code (with StorageProtocol directly).

    Args:
        ctx_or_storage: Workflow context or storage backend
        channel: Channel name to publish to
        data: Message payload (dict or bytes)
        metadata: Optional metadata
        target_instance_id: If provided, only deliver to this specific instance
                           (Point-to-Point delivery). If None, deliver to all
                           waiting subscribers (Pub/Sub delivery).
        worker_id: Optional worker ID for Lock-First pattern (required for
                   CloudEvents HTTP handler)

    Returns:
        Message ID of the published message

    Example:
        >>> # From within a workflow
        >>> @workflow
        ... async def order_processor(ctx: WorkflowContext, order_id: str):
        ...     result = await process_order(ctx, order_id, activity_id="process:1")
        ...     await publish(ctx, "order.completed", {"order_id": order_id})
        ...     return result

        >>> # From external code (e.g., HTTP handler)
        >>> async def api_handler(request):
        ...     message_id = await publish(app.storage, "jobs", {"task": "process"})
        ...     return {"message_id": message_id}

        >>> # Point-to-Point delivery (CloudEvents with eddainstanceid)
        >>> await publish(
        ...     storage, "payment.completed", {"amount": 100},
        ...     target_instance_id="order-123", worker_id="worker-1"
        ... )
    """
    # Determine if we have a context or direct storage
    from edda.context import WorkflowContext as WfCtx

    if isinstance(ctx_or_storage, WfCtx):
        storage = ctx_or_storage.storage
        # Add source metadata
        full_metadata = metadata.copy() if metadata else {}
        full_metadata.setdefault("source_instance_id", ctx_or_storage.instance_id)
        full_metadata.setdefault("published_at", datetime.now(UTC).isoformat())
        effective_worker_id = worker_id or ctx_or_storage.worker_id
    else:
        storage = ctx_or_storage
        full_metadata = metadata.copy() if metadata else {}
        full_metadata.setdefault("published_at", datetime.now(UTC).isoformat())
        effective_worker_id = worker_id or f"publisher-{uuid.uuid4()}"

    # Publish to channel
    message_id = await storage.publish_to_channel(channel, data, full_metadata)

    # Wake up waiting subscribers
    # If in a transaction, defer delivery until after commit to ensure atomicity
    if storage.in_transaction():
        # Capture current values for the closure
        _storage = storage
        _channel = channel
        _message_id = message_id
        _data = data
        _metadata = full_metadata
        _target_instance_id = target_instance_id
        _worker_id = effective_worker_id

        async def deferred_wake() -> None:
            await _wake_waiting_subscribers(
                _storage,
                _channel,
                _message_id,
                _data,
                _metadata,
                target_instance_id=_target_instance_id,
                worker_id=_worker_id,
            )

        storage.register_post_commit_callback(deferred_wake)
    else:
        # Not in transaction - deliver immediately
        await _wake_waiting_subscribers(
            storage,
            channel,
            message_id,
            data,
            full_metadata,
            target_instance_id=target_instance_id,
            worker_id=effective_worker_id,
        )

    return message_id


async def _wake_waiting_subscribers(
    storage: StorageProtocol,
    channel: str,
    message_id: str,
    data: dict[str, Any] | bytes,
    metadata: dict[str, Any],
    *,
    target_instance_id: str | None = None,
    worker_id: str,
) -> None:
    """
    Wake up subscribers waiting on a channel.

    Args:
        storage: Storage backend
        channel: Channel name
        message_id: Message ID
        data: Message payload
        metadata: Message metadata
        target_instance_id: If provided, only wake this specific instance
                           (Point-to-Point delivery)
        worker_id: Worker ID for Lock-First pattern
    """
    if target_instance_id:
        # Point-to-Point delivery: deliver only to specific instance
        await storage.deliver_channel_message(
            instance_id=target_instance_id,
            channel=channel,
            message_id=message_id,
            data=data,
            metadata=metadata,
            worker_id=worker_id,
        )
        return

    # Pub/Sub delivery: deliver to all waiting subscribers
    waiting = await storage.get_channel_subscribers_waiting(channel)

    for sub in waiting:
        instance_id = sub["instance_id"]
        mode = sub["mode"]

        if mode == "competing":
            # For competing mode, only wake one subscriber
            # Use Lock-First pattern
            result = await storage.deliver_channel_message(
                instance_id=instance_id,
                channel=channel,
                message_id=message_id,
                data=data,
                metadata=metadata,
                worker_id=worker_id,
            )
            if result:
                # Successfully woke one subscriber, stop
                break
        else:
            # For broadcast mode, wake all subscribers
            await storage.deliver_channel_message(
                instance_id=instance_id,
                channel=channel,
                message_id=message_id,
                data=data,
                metadata=metadata,
                worker_id=worker_id,
            )


# =============================================================================
# Direct Messaging (Instance-to-Instance)
# =============================================================================


async def send_to(
    ctx: WorkflowContext,
    instance_id: str,
    data: dict[str, Any] | bytes,
    channel: str = "__direct__",
    metadata: dict[str, Any] | None = None,
) -> bool:
    """
    Send a message directly to a specific workflow instance.

    This is useful for workflow-to-workflow communication where the target
    instance ID is known.

    Args:
        ctx: Workflow context (source workflow)
        instance_id: Target workflow instance ID
        channel: Channel name (defaults to "__direct__" for direct messages)
        data: Message payload
        metadata: Optional metadata

    Returns:
        True if delivered, False if no workflow waiting

    Example:
        >>> @workflow
        ... async def approver(ctx: WorkflowContext, request_id: str):
        ...     decision = await review(ctx, request_id, activity_id="review:1")
        ...     await send_to(ctx, instance_id=request_id, data={"approved": decision})
    """
    full_metadata = metadata.copy() if metadata else {}
    full_metadata.setdefault("source_instance_id", ctx.instance_id)
    full_metadata.setdefault("sent_at", datetime.now(UTC).isoformat())

    # Publish to a direct channel for the target instance
    direct_channel = f"{channel}:{instance_id}"
    message_id = await ctx.storage.publish_to_channel(direct_channel, data, full_metadata)

    # Try to deliver
    result = await ctx.storage.deliver_channel_message(
        instance_id=instance_id,
        channel=direct_channel,
        message_id=message_id,
        data=data,
        metadata=full_metadata,
        worker_id=ctx.worker_id,
    )

    return result is not None


# =============================================================================
# Timer Functions
# =============================================================================


async def sleep(
    ctx: WorkflowContext,
    seconds: int,
    timer_id: str | None = None,
) -> None:
    """
    Pause workflow execution for a specified duration.

    This is a durable sleep - the workflow will be resumed after the specified
    time even if the worker restarts.

    Args:
        ctx: Workflow context
        seconds: Duration to sleep in seconds
        timer_id: Optional unique ID for this timer (auto-generated if not provided)

    Example:
        >>> @workflow
        ... async def order_workflow(ctx: WorkflowContext, order_id: str):
        ...     await create_order(ctx, order_id, activity_id="create:1")
        ...     await sleep(ctx, 60)  # Wait 60 seconds for payment
        ...     await check_payment(ctx, order_id, activity_id="check:1")
    """
    # Generate activity ID
    if timer_id is None:
        activity_id = ctx._generate_activity_id("sleep")
        timer_id = activity_id
    else:
        activity_id = timer_id

    ctx._record_activity_id(activity_id)

    # During replay, return immediately
    if ctx.is_replaying:
        found, cached_result = ctx._get_cached_result(activity_id)
        if found:
            return

    # Calculate expiry time (deterministic - calculated once)
    expires_at = datetime.now(UTC) + timedelta(seconds=seconds)

    # Raise exception to pause workflow
    raise WaitForTimerException(
        duration_seconds=seconds,
        expires_at=expires_at,
        timer_id=timer_id,
        activity_id=activity_id,
    )


async def sleep_until(
    ctx: WorkflowContext,
    target_time: datetime,
    timer_id: str | None = None,
) -> None:
    """
    Pause workflow execution until a specific time.

    This is a durable sleep - the workflow will be resumed at the specified
    time even if the worker restarts.

    Args:
        ctx: Workflow context
        target_time: Absolute time to wake up (must be timezone-aware)
        timer_id: Optional unique ID for this timer (auto-generated if not provided)

    Example:
        >>> from datetime import datetime, timedelta, UTC
        >>>
        >>> @workflow
        ... async def scheduled_report(ctx: WorkflowContext, report_id: str):
        ...     # Schedule for tomorrow at 9 AM
        ...     tomorrow_9am = datetime.now(UTC).replace(hour=9, minute=0, second=0)
        ...     tomorrow_9am += timedelta(days=1)
        ...     await sleep_until(ctx, tomorrow_9am)
        ...     await generate_report(ctx, report_id, activity_id="generate:1")
    """
    if target_time.tzinfo is None:
        raise ValueError("target_time must be timezone-aware")

    # Generate activity ID
    if timer_id is None:
        activity_id = ctx._generate_activity_id("sleep_until")
        timer_id = activity_id
    else:
        activity_id = timer_id

    ctx._record_activity_id(activity_id)

    # During replay, return immediately
    if ctx.is_replaying:
        found, cached_result = ctx._get_cached_result(activity_id)
        if found:
            return

    # Calculate seconds until target
    now = datetime.now(UTC)
    delta = target_time - now
    seconds = max(0, int(delta.total_seconds()))

    # Raise exception to pause workflow
    raise WaitForTimerException(
        duration_seconds=seconds,
        expires_at=target_time,
        timer_id=timer_id,
        activity_id=activity_id,
    )


# =============================================================================
# CloudEvents Integration
# =============================================================================


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
    data: dict[str, Any] | Any  # Any to support Pydantic models

    # CloudEvents standard attributes
    type: str
    source: str
    id: str
    time: str | None = None
    datacontenttype: str | None = None
    subject: str | None = None

    # CloudEvents extension attributes
    extensions: dict[str, Any] = field(default_factory=dict)


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


def _convert_channel_message_to_received_event(
    msg: ChannelMessage,
    event_type: str,
    model: type[Any] | None = None,
) -> ReceivedEvent:
    """
    Convert a ChannelMessage to a ReceivedEvent.

    CloudEvents metadata is extracted from the message's metadata field
    where it was stored with 'ce_' prefix.

    Args:
        msg: ChannelMessage received from receive()
        event_type: The event type that was waited for
        model: Optional Pydantic model to convert data to

    Returns:
        ReceivedEvent with CloudEvents metadata
    """
    from edda.pydantic_utils import from_json_dict

    data: dict[str, Any] | Any
    if model is not None and isinstance(msg.data, dict):
        data = from_json_dict(msg.data, model)
    elif isinstance(msg.data, dict):
        data = msg.data
    else:
        # bytes data - wrap in dict for ReceivedEvent compatibility
        data = {"_binary": msg.data}

    return ReceivedEvent(
        data=data,
        type=event_type,
        source=msg.metadata.get("ce_source", "unknown"),
        id=msg.metadata.get("ce_id", msg.id),
        time=msg.metadata.get("ce_time"),
        datacontenttype=msg.metadata.get("ce_datacontenttype"),
        subject=msg.metadata.get("ce_subject"),
        extensions=msg.metadata.get("ce_extensions", {}),
    )


async def wait_event(
    ctx: WorkflowContext,
    event_type: str,
    timeout_seconds: int | None = None,
    model: type[Any] | None = None,
    event_id: str | None = None,
) -> ReceivedEvent:
    """
    Wait for a CloudEvent to arrive.

    This function pauses the workflow execution until a matching CloudEvent is received.
    During replay, it returns the cached event data and metadata.

    Internally, this uses the Channel-based Message Queue with event_type as the channel name.
    CloudEvents metadata is preserved in the message metadata.

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
        Events are delivered to workflows that are subscribed to the event_type channel.
        Use subscribe(ctx, event_type) before calling wait_event() or let it auto-subscribe.

    Raises:
        WaitForChannelMessageException: During normal execution to pause the workflow
        EventTimeoutError: If timeout is reached

    Example:
        >>> # Without Pydantic (dict access)
        >>> @workflow
        ... async def order_workflow(ctx: WorkflowContext, order_id: str):
        ...     await subscribe(ctx, "payment.completed", mode="broadcast")
        ...     payment_event = await wait_event(ctx, "payment.completed")
        ...     amount = payment_event.data["amount"]
        ...     order_id = payment_event.data["order_id"]
        ...
        >>> # With Pydantic (type-safe access)
        >>> @workflow
        ... async def order_workflow_typed(ctx: WorkflowContext, order_id: str):
        ...     await subscribe(ctx, "payment.completed", mode="broadcast")
        ...     payment_event = await wait_event(
        ...         ctx,
        ...         event_type="payment.completed",
        ...         model=PaymentCompleted
        ...     )
        ...     # Type-safe access with IDE completion
        ...     amount = payment_event.data.amount
    """
    # Auto-subscribe to the event_type channel if not already subscribed
    subscription = await _get_subscription(ctx, event_type)
    if subscription is None:
        await subscribe(ctx, event_type, mode="broadcast")

    # Use receive() with event_type as channel
    msg = await receive(
        ctx,
        channel=event_type,
        timeout_seconds=timeout_seconds,
        message_id=event_id,
    )

    # Convert ChannelMessage to ReceivedEvent with CloudEvents metadata
    return _convert_channel_message_to_received_event(msg, event_type, model)


# Backward compatibility aliases
wait_timer = sleep
wait_until = sleep_until


async def send_event(
    event_type: str,
    source: str,
    data: dict[str, Any] | Any,
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
    import httpx
    from cloudevents.conversion import to_structured
    from cloudevents.http import CloudEvent

    from edda.pydantic_utils import is_pydantic_instance, to_json_dict

    # Convert Pydantic model to JSON dict
    data_dict: dict[str, Any]
    if is_pydantic_instance(data):
        data_dict = to_json_dict(data)
    elif isinstance(data, dict):
        data_dict = data
    else:
        data_dict = {"_data": data}

    # Create CloudEvent attributes
    attributes: dict[str, Any] = {
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


# =============================================================================
# Utility Functions
# =============================================================================


async def get_channel_stats(
    _storage: StorageProtocol,
    channel: str,
) -> dict[str, Any]:
    """
    Get statistics about a channel.

    Args:
        storage: Storage backend
        channel: Channel name

    Returns:
        Dictionary with channel statistics
    """
    # TODO: Implement actual statistics retrieval using _storage
    # - Query ChannelMessage table for message_count
    # - Query ChannelSubscription table for subscriber_count
    # For now, return placeholder values
    return {
        "channel": channel,
        "message_count": 0,
        "subscriber_count": 0,
    }
