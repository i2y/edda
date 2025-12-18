# Channel-based Message Queue

Edda provides a channel-based message queue system inspired by Erlang/Elixir mailbox patterns. This enables:

- **Broadcast mode**: All subscribers receive all messages (fan-out pattern)
- **Competing mode**: Each message is processed by only one subscriber (producer-consumer pattern)
- **Direct messaging**: Send messages to specific workflow instances
- **Persistent queuing**: Messages are never lost (queued until consumed)

## Overview

The channel system solves the "mailbox problem" where messages sent before `receive()` is called would be lost. Messages are always queued and persist until consumed.

## Key Functions

### Subscription Management

#### `subscribe()`

Subscribe to a channel to receive messages:

```python
from edda import subscribe, receive, WorkflowContext

@workflow
async def job_worker(ctx: WorkflowContext, worker_id: str):
    # Subscribe to job queue (each job processed by one worker)
    await subscribe(ctx, "jobs", mode="competing")

    while True:
        job = await receive(ctx, "jobs")
        await execute_job(ctx, job.data, activity_id=f"job:{job.id}")
        await ctx.recur()

@workflow
async def notification_service(ctx: WorkflowContext, service_id: str):
    # Subscribe to notifications (all handlers receive all messages)
    await subscribe(ctx, "notifications", mode="broadcast")

    while True:
        msg = await receive(ctx, "notifications")
        await send_notification(ctx, msg.data, activity_id=f"notify:{msg.id}")
        await ctx.recur()
```

**Modes**:

- `"broadcast"` (default): All subscribers receive all messages. Use for fan-out patterns like notifications.
- `"competing"`: Each message is processed by only one subscriber. Use for job queues and task distribution.
- `"direct"`: Receive messages sent via `send_to()` to this specific instance. Syntactic sugar for point-to-point messaging.

**Using `mode="direct"`**:

The `"direct"` mode simplifies receiving messages sent via `send_to()`:

```python
@workflow
async def direct_receiver(ctx: WorkflowContext, id: str):
    # Subscribe to receive direct messages
    await subscribe(ctx, "notifications", mode="direct")

    # Wait for a message sent via send_to()
    msg = await receive(ctx, "notifications")
    return msg.data

@workflow
async def sender(ctx: WorkflowContext, receiver_id: str):
    # Send directly to the receiver instance
    await send_to(ctx, instance_id=receiver_id, data={"hello": "world"}, channel="notifications")
```

This is equivalent to manually constructing the channel name:

```python
# Without mode="direct" (manual approach)
direct_channel = f"notifications:{ctx.instance_id}"
await subscribe(ctx, direct_channel, mode="broadcast")
msg = await receive(ctx, direct_channel)

# With mode="direct" (simplified)
await subscribe(ctx, "notifications", mode="direct")
msg = await receive(ctx, "notifications")
```

#### `unsubscribe()`

Unsubscribe from a channel (optional - workflows auto-unsubscribe on completion):

```python
from edda import unsubscribe, WorkflowContext

@workflow
async def temporary_subscriber(ctx: WorkflowContext):
    await subscribe(ctx, "temp_channel", mode="broadcast")

    # Process some messages...
    for i in range(10):
        msg = await receive(ctx, "temp_channel")
        await process(ctx, msg.data, activity_id=f"process:{i+1}")

    # Done subscribing
    await unsubscribe(ctx, "temp_channel")

    # Continue with other work...
```

### Message Receiving

#### `receive()`

Receive a message from a channel:

```python
from edda import receive, WorkflowContext

@workflow
async def consumer(ctx: WorkflowContext, id: str):
    await subscribe(ctx, "tasks", mode="competing")

    while True:
        msg = await receive(ctx, "tasks")
        await process(ctx, msg.data, activity_id=f"process:{msg.id}")
        await ctx.recur()
```

**With timeout**:

```python
@workflow
async def workflow_with_timeout(ctx: WorkflowContext):
    await subscribe(ctx, "approval", mode="broadcast")

    try:
        msg = await receive(
            ctx,
            "approval",
            timeout_seconds=300  # 5 minutes
        )
        await handle_approval(ctx, msg.data, activity_id="handle:1")
    except TimeoutError:
        await handle_timeout(ctx, activity_id="timeout:1")
```

### Message Publishing

#### `publish()`

Publish a message to a channel:

```python
from edda import publish, WorkflowContext

@workflow
async def order_processor(ctx: WorkflowContext, order_id: str):
    result = await process_order(ctx, order_id, activity_id="process:1")

    # Notify all subscribers
    await publish(ctx, "order.completed", {"order_id": order_id, "status": "completed"})

    return result
```

**From external code** (e.g., HTTP handler):

```python
from edda import publish

# From HTTP handler or background task
message_id = await publish(
    app.storage,
    "jobs",
    {"task": "send_report", "user_id": 123},
)
```

### Direct Messaging

#### `send_to()`

Send a message directly to a specific workflow instance:

```python
from edda import send_to, receive, WorkflowContext

@workflow
async def approver(ctx: WorkflowContext, request_id: str):
    decision = await review(ctx, request_id, activity_id="review:1")

    # Send decision to requester
    await send_to(
        ctx,
        instance_id=request_id,
        data={"approved": decision},
    )

@workflow
async def requester(ctx: WorkflowContext, approver_id: str):
    # Send request
    await send_to(
        ctx,
        instance_id=approver_id,
        data={"action": "review", "request_id": ctx.instance_id},
    )

    # Wait for response
    response = await receive(ctx, f"__direct__:{ctx.instance_id}")
    return response.data
```

## Detailed API Reference

### ChannelMessage Class

```python
@dataclass(frozen=True)
class ChannelMessage:
    """A message received from a channel."""
    id: str                           # Unique message ID
    channel: str                      # Channel name
    data: dict[str, Any] | bytes      # Message payload
    metadata: dict[str, Any]          # Optional metadata
    published_at: datetime            # When the message was published
```

### subscribe()

```python
async def subscribe(
    ctx: WorkflowContext,
    channel: str,
    mode: str = "broadcast",
) -> None:
```

**Parameters**:

- `ctx`: Workflow context
- `channel`: Channel name to subscribe to
- `mode`: `"broadcast"` (all subscribers receive), `"competing"` (one subscriber per message), or `"direct"` (receive messages from `send_to()`)

### receive()

```python
async def receive(
    ctx: WorkflowContext,
    channel: str,
    timeout_seconds: int | None = None,
    message_id: str | None = None,
) -> ChannelMessage:
```

**Parameters**:

- `ctx`: Workflow context
- `channel`: Channel name to receive from
- `timeout_seconds`: Optional timeout in seconds
- `message_id`: Optional custom ID for deterministic replay

**Returns**: `ChannelMessage` object

### publish()

```python
async def publish(
    ctx_or_storage: WorkflowContext | StorageProtocol,
    channel: str,
    data: dict[str, Any] | bytes,
    metadata: dict[str, Any] | None = None,
    *,
    target_instance_id: str | None = None,
) -> str:
```

**Parameters**:

- `ctx_or_storage`: Workflow context or storage backend
- `channel`: Channel name to publish to
- `data`: Message payload
- `metadata`: Optional metadata
- `target_instance_id`: If provided, only deliver to this specific instance (Point-to-Point delivery)

**Returns**: Message ID of the published message

### send_to()

```python
async def send_to(
    ctx: WorkflowContext,
    instance_id: str,
    data: dict[str, Any] | bytes,
    channel: str = "__direct__",
    metadata: dict[str, Any] | None = None,
) -> bool:
```

**Parameters**:

- `ctx`: Workflow context (source workflow)
- `instance_id`: Target workflow instance ID
- `data`: Message payload
- `channel`: Channel name (defaults to `"__direct__"` for direct messages)
- `metadata`: Optional metadata

**Returns**: `True` if delivered, `False` if target was not waiting

## Common Patterns

### Request-Response Pattern

```python
@workflow
async def requester_workflow(ctx: WorkflowContext, responder_id: str):
    # Send request
    await send_to(
        ctx,
        instance_id=responder_id,
        data={"action": "process", "request_id": ctx.instance_id},
        channel="request",
    )

    # Wait for response
    response = await receive(
        ctx,
        channel=f"response:{ctx.instance_id}",
        timeout_seconds=60,
    )

    return response.data

@workflow
async def responder_workflow(ctx: WorkflowContext):
    await subscribe(ctx, "request", mode="competing")

    # Wait for requests
    request = await receive(ctx, "request")

    # Process and respond
    result = await process_request(ctx, request.data, activity_id="process:1")

    await send_to(
        ctx,
        instance_id=request.data["request_id"],
        data={"result": result},
        channel=f"response:{request.data['request_id']}",
    )
```

### Fan-Out/Fan-In Pattern

```python
@workflow
async def coordinator_workflow(ctx: WorkflowContext, tasks: list[dict]):
    # Start workers and collect their IDs
    worker_ids = []
    for i, task in enumerate(tasks):
        worker_id = await worker_workflow.start(task={**task, "coordinator_id": ctx.instance_id})
        worker_ids.append(worker_id)

    # Subscribe to results channel
    await subscribe(ctx, f"result:{ctx.instance_id}", mode="broadcast")

    # Wait for all results
    results = []
    for worker_id in worker_ids:
        msg = await receive(
            ctx,
            channel=f"result:{ctx.instance_id}",
            message_id=f"result:{worker_id}",
        )
        results.append(msg.data)

    return {"results": results}

@workflow
async def worker_workflow(ctx: WorkflowContext, task: dict):
    coordinator_id = task["coordinator_id"]

    # Do work
    result = await process_task(ctx, task, activity_id="process:1")

    # Send result back to coordinator
    await send_to(
        ctx,
        instance_id=coordinator_id,
        data=result,
        channel=f"result:{coordinator_id}",
    )
```

### Producer-Consumer (Job Queue) Pattern

```python
# Producer
@workflow
async def job_producer(ctx: WorkflowContext, jobs: list[dict]):
    for job in jobs:
        await publish(ctx, "jobs", job)

# Consumer (competing mode - each job processed by one worker)
@workflow
async def job_consumer(ctx: WorkflowContext, worker_id: str):
    await subscribe(ctx, "jobs", mode="competing")

    while True:
        job = await receive(ctx, "jobs")
        await execute_job(ctx, job.data, activity_id=f"job:{job.id}")
        await ctx.recur()
```

### Broadcast (Pub/Sub) Pattern

```python
# Publisher
@workflow
async def event_publisher(ctx: WorkflowContext):
    while True:
        event = await get_next_event(ctx, activity_id="get:1")

        # Publish to all subscribers
        await publish(ctx, f"events.{event['type']}", event)
        await ctx.recur()

# Subscriber (broadcast mode - all subscribers receive all events)
@workflow
async def event_subscriber(ctx: WorkflowContext, subscriber_id: str):
    await subscribe(ctx, "events.order_created", mode="broadcast")

    while True:
        msg = await receive(ctx, "events.order_created")
        await handle_order_event(ctx, msg.data, activity_id=f"handle:{msg.id}")
        await ctx.recur()
```

## Comparison with CloudEvents

| Feature | Channel-based Messaging | CloudEvents |
|---------|------------------------|-------------|
| **Primary Use** | Workflow-to-workflow | External events |
| **Sender Awareness** | Internal workflows | External systems |
| **Protocol** | Internal database | HTTP + CloudEvents spec |
| **Durability** | Database-backed | Database-backed |
| **Fan-out** | `mode="broadcast"` | Event type matching |
| **Load Balancing** | `mode="competing"` | External load balancer |

### When to Use Each

**Use Channel-based Messaging when:**

- Communicating between workflow instances
- Implementing producer-consumer patterns
- Building internal workflow orchestration
- Need guaranteed message delivery within workflows

**Use CloudEvents when:**

- Receiving events from external systems
- Integrating with Knative, Kafka, etc.
- Need standardized event format
- Building event-driven microservices

## Best Practices

### 1. Use Descriptive Channel Names

```python
# Good: Clear, hierarchical naming
await subscribe(ctx, "orders.approved", mode="broadcast")
await receive(ctx, "payments.completed")

# Avoid: Generic names
await receive(ctx, "message")
await receive(ctx, "data")
```

### 2. Choose the Right Mode

```python
# Broadcast: Notifications, events that everyone needs to see
await subscribe(ctx, "audit_log", mode="broadcast")

# Competing: Job queues, tasks that should be processed once
await subscribe(ctx, "pending_orders", mode="competing")
```

### 3. Include Correlation IDs in Metadata

```python
await publish(
    ctx,
    "order.completed",
    {"order_id": order_id, "status": "completed"},
    metadata={
        "correlation_id": order_id,
        "published_at": datetime.now().isoformat(),
    },
)
```

### 4. Handle Timeouts Gracefully

```python
@workflow
async def robust_workflow(ctx: WorkflowContext):
    await subscribe(ctx, "response", mode="broadcast")

    try:
        msg = await receive(ctx, "response", timeout_seconds=300)
        return {"status": "success", "data": msg.data}
    except TimeoutError:
        return {"status": "timeout", "error": "Response not received in time"}
```

### 5. Clean Up Subscriptions When Done

Workflows automatically unsubscribe on completion/failure. For long-running workflows, explicitly unsubscribe when no longer needed:

```python
@workflow
async def temporary_subscriber(ctx: WorkflowContext):
    await subscribe(ctx, "temp_channel", mode="broadcast")

    # Process some messages...
    for i in range(10):
        msg = await receive(ctx, "temp_channel")
        await process(ctx, msg.data, activity_id=f"process:{i+1}")

    # Done subscribing
    await unsubscribe(ctx, "temp_channel")

    # Continue with other work...
```

## Transactional Message Processing

When using channel-based messaging inside activities, both `publish()` and `receive()` participate in the activity's database transaction.

### Transactional Publish

When `publish()` is called inside an activity, the message is only published **after the transaction commits**:

```python
@activity  # transactional=True by default
async def process_order(ctx: WorkflowContext, order_id: str):
    # Do some work...
    result = await do_processing(order_id)

    # Message is queued for post-commit delivery
    await publish(ctx, "order.completed", {"order_id": order_id})

    return result  # Commit: message is now published
```

**Behavior:**

- If the activity **succeeds**: Message is published after commit
- If the activity **fails**: Message is **NOT** published (rollback)

This ensures that messages are only sent when the associated business logic succeeds.

### Transactional Receive

When `receive()` is called inside an activity, the message claim is part of the transaction:

```python
@activity  # transactional=True by default
async def process_job(ctx: WorkflowContext, channel: str):
    msg = await receive(ctx, channel)  # Claim is part of transaction

    # Process the message...
    result = await do_work(msg.data)

    return result  # Commit: claim is finalized
```

**Behavior:**

- If the activity **succeeds**: Message claim is committed, message is processed
- If the activity **fails**: Message claim is rolled back, message returns to queue

This provides **at-least-once delivery** semantics - if processing fails, the message will be redelivered to another subscriber.

### Recommended Pattern

For reliable message processing, wrap `receive()` calls inside activities:

```python
@workflow
async def job_worker(ctx: WorkflowContext, worker_id: str):
    await subscribe(ctx, "jobs", mode="competing")

    while True:
        # Process job inside activity for transactional guarantees
        await process_job(ctx, "jobs", activity_id="process:1")
        await ctx.recur(worker_id)

@activity
async def process_job(ctx: WorkflowContext, channel: str):
    msg = await receive(ctx, channel)  # Part of activity transaction

    # Do work...
    await execute_task(msg.data)

    # Publish completion notification (also transactional)
    await publish(ctx, "job.completed", {"job_id": msg.id})

    return {"processed": msg.id}
```

## Performance Considerations

### Database-Backed Durability

All messages are routed through the database for durability and crash recovery. This ensures:

- Messages are never lost (even on crash)
- Deterministic replay works correctly
- Distributed safety across multiple workers

### Mode Selection Impact

| Mode | Delivery | Use Case | Performance |
|------|----------|----------|-------------|
| `broadcast` | All subscribers | Notifications | O(n) where n is subscriber count |
| `competing` | One subscriber | Job queues | O(1) with lock-first pattern |

For high-throughput scenarios with many subscribers, consider partitioning into smaller channels.
