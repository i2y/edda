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
        await ctx.recur(worker_id=worker_id)

@workflow
async def notification_service(ctx: WorkflowContext, service_id: str):
    # Subscribe to notifications (all handlers receive all messages)
    await subscribe(ctx, "notifications", mode="broadcast")

    while True:
        msg = await receive(ctx, "notifications")
        await send_notification(ctx, msg.data, activity_id=f"notify:{msg.id}")
        await ctx.recur(service_id=service_id)
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

A direct channel is the instance's mailbox: it keeps what is sent to it. A subscription to it starts at the first message sent to the instance, so the messages sent before the instance subscribed are not lost.

A broadcast subscription keeps its place after `unsubscribe()` and after `ctx.recur()`: subscribing to the channel again continues where the subscription stopped, so the messages published in between are received too.

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
        await process(ctx, msg.data)

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
        await process(ctx, msg.data, activity_id=f"process:msg-{msg.id}")
        await ctx.recur(id=id)
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

Send a message directly to a specific workflow instance. `send_to(ctx, instance_id, data, channel=...)` delivers to the channel `"{channel}:{instance_id}"`. The receiver subscribes with `mode="direct"` and receives on the plain channel name:

```python
from edda import send_to, subscribe, receive, WorkflowContext

@workflow
async def approver(ctx: WorkflowContext, requester_id: str):
    await subscribe(ctx, "review", mode="direct")
    request = await receive(ctx, "review")

    decision = await review(ctx, request.data["request_id"])

    # Send decision to requester
    await send_to(
        ctx,
        instance_id=requester_id,
        data={"approved": decision},
        channel="decision",
    )

@workflow
async def requester(ctx: WorkflowContext, approver_id: str):
    await subscribe(ctx, "decision", mode="direct")

    # Send request
    await send_to(
        ctx,
        instance_id=approver_id,
        data={"request_id": ctx.instance_id},
        channel="review",
    )

    # Wait for response
    response = await receive(ctx, "decision")
    return response.data
```

A direct channel is private to its receiver. Only the instance whose ID is in the channel name can subscribe to it, receive from it or read it (`read_channel_since`, `read_channel_after`). Any other instance gets `ChannelPermissionError`, whatever the seal state. Sending to it with `send_to()` (or a targeted CloudEvent) is allowed.

## Detailed API Reference

### ChannelMessage Class

```python
@dataclass(frozen=True)
class ChannelMessage:
    """A message received from a channel."""
    id: str                           # Unique message ID
    channel: str                      # Channel name
    data: Any                         # Message payload as published: any JSON value, or bytes
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

**Returns**: `ChannelMessage` object. `data` is the published value as it was sent: any JSON value (dict, list, str, int, float, bool, `None`) or `bytes`.

`receive()` belongs in workflow code. Called inside an activity it raises `TerminalError`.

### publish()

```python
async def publish(
    ctx_or_storage: WorkflowContext | StorageProtocol,
    channel: str,
    data: Any,
    metadata: dict[str, Any] | None = None,
    *,
    target_instance_id: str | None = None,
) -> str:
```

**Parameters**:

- `ctx_or_storage`: Workflow context or storage backend
- `channel`: Channel name to publish to
- `data`: Message payload (any JSON value or `bytes`)
- `metadata`: Optional metadata
- `target_instance_id`: If provided, the message is for this instance only. It is stored on the instance's direct channel `"{channel}:{target_instance_id}"`, like a `send_to()`, where `wait_event()` or a direct subscription (`mode="direct"`) in that instance receives it. A plain `receive()` on `channel` does not.

**Returns**: Message ID of the published message

### send_to()

```python
async def send_to(
    ctx: WorkflowContext,
    instance_id: str,
    data: Any,
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

    # Wait for response (direct channel: only this instance can receive it)
    await subscribe(ctx, "response", mode="direct")
    response = await receive(ctx, channel="response", timeout_seconds=60)

    return response.data

@workflow
async def responder_workflow(ctx: WorkflowContext):
    await subscribe(ctx, "request", mode="direct")

    # Wait for requests
    request = await receive(ctx, "request")

    # Process and respond
    result = await process_request(ctx, request.data, activity_id="process:1")

    await send_to(
        ctx,
        instance_id=request.data["request_id"],
        data={"result": result},
        channel="response",
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

    # Subscribe to this instance's direct "result" channel
    await subscribe(ctx, "result", mode="direct")

    # Wait for all results
    results = []
    for worker_id in worker_ids:
        msg = await receive(
            ctx,
            channel="result",
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
        channel="result",
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
        await ctx.recur(worker_id=worker_id)
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
        await ctx.recur(subscriber_id=subscriber_id)
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
        await process(ctx, msg.data)

    # Done subscribing
    await unsubscribe(ctx, "temp_channel")

    # Continue with other work...
```

## Transactional Message Processing

Called inside an activity, `publish()` and `send_to()` participate in the activity's database transaction. `receive()` belongs in workflow code (see below).

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

### Receiving and Processing

Call `receive()` from workflow code, then process the message in an activity:

```python
@workflow
async def job_worker(ctx: WorkflowContext, worker_id: str):
    await subscribe(ctx, "jobs", mode="competing")

    # Waits durably (no worker is held); the message is recorded in history
    msg = await receive(ctx, "jobs")
    await process_job(ctx, msg.data, activity_id=f"process:{msg.id}")
    await ctx.recur(worker_id=worker_id)

@activity
async def process_job(ctx: WorkflowContext, job: dict):
    await execute_task(job)

    # Publish completion notification (published after the activity commits)
    await publish(ctx, "job.completed", {"job_id": job["id"]})

    return {"processed": True}
```

**Behavior:**

- On a competing channel, the claim, the history record and the removal of the message commit in one transaction, so at most one subscriber consumes each message.
- Once received, the message is part of the workflow's history. After a crash the workflow replays with the same message, and the activity runs again under its retry policy. If processing finally fails, the message does not go back to the queue; handle that in the workflow, for example by publishing it to a dead-letter channel.
- Do not call `receive()` inside an activity. An activity cannot suspend the workflow, so `receive()` there raises `TerminalError` (no retry). Receive in the workflow and pass the message to the activity.

## Replay Safety and Sealed Channels

Called from workflow code, `publish()` and `send_to()` are recorded steps. They run once, and a replay returns the recorded result instead of publishing again. A run that lost its lock to another worker publishes nothing (`LockLostError`). Inside an activity they run directly, and the activity is what is recorded.

A channel is open until a `channel_pub` or `channel_sub` capability is issued for it. From then on, publishing (or receiving) needs a valid token, passed as `capability_token=`:

```python
from edda.actors import issue_capability

token, capability_id = await issue_capability(ctx, "channel_pub", "orders")
await publish(ctx, "orders", {"order_id": "A-1"}, capability_token=token)
```

The seal also covers the direct channels under that name (`orders:<instance_id>`), so `send_to(..., channel="orders")` needs the token too. A refused publish raises `ChannelPermissionError`; the refusal is recorded, so a replay takes the same branch.

### Capability Rules

- **Expiry does not unseal.** A channel stays sealed while any grant for it exists, expired or not. An expired token is rejected. The expiry sweep deletes an expired grant only while another grant for the same kind and target remains that is unexpired or newer, so the newest grant of a channel is never removed. Only revoking the last grant re-opens the channel.
- **Expiring capabilities.** `issue_capability(ctx, kind, target, expires_at=...)` issues a capability that expires at the given time.
- **Ownership.** `revoke_capability(ctx, capability_id)` works only for the instance that issued the capability, and `unregister_name(ctx, name)` only for the instance that holds the name. Any other instance gets `NotOwnerError` (recorded like other step errors). An unknown ID or an unbound name is a no-op.
- **Capability key.** With `EddaApp(capability_key=...)` (or the `DURAX_CAPABILITY_KEY` environment variable), every new secret is `HMAC-SHA256(key, capability_id)` and the history records only the ID, kind and target, so no token is ever written to history. Without a key the token is stored in history in plain text. Use the same key on every worker, and do not change it while instances that issued capabilities are alive.
- **CloudEvents.** An event posted to a channel sealed for `channel_pub` needs the token in the `capabilitytoken` extension attribute (see [CloudEvents HTTP Binding](events/cloudevents-http-binding.md)).

### Cursor Reads

`read_channel_since` and `read_channel_after` (both in `edda.tasks`) read a channel without consuming it. A sealed channel needs a `channel_sub` token, as for `receive()`.

```python
from edda.tasks import read_channel_since, read_channel_after

messages = await read_channel_after(ctx, "orders", 0)           # from the start
last = messages[-1]
newer = await read_channel_after(ctx, "orders", last["id"])     # keeps working if `last` is deleted
since = await read_channel_since(ctx, "orders", last["message_id"])
```

`read_channel_since` raises `CursorNotFoundError` when no message with that `message_id` is on the channel any more (it no longer returns the whole channel). `read_channel_after` takes the numeric `id` of the last message you read (`0` reads from the start) and does not depend on that message still existing. Returned messages carry both `id` and `message_id`.

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
