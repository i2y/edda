# Edda

**Edda** - Norse mythology poetic narratives that preserve ancient sagas and legends

> Lightweight durable execution framework - no separate server required

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Documentation](https://img.shields.io/badge/docs-latest-green.svg)](https://i2y.github.io/edda/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/i2y/edda)

## Overview

Edda is a lightweight durable execution framework for Python that runs as a **library** in your application - no separate workflow server required. It provides automatic crash recovery through deterministic replay, allowing **long-running workflows** to survive process restarts and failures without losing progress.

**Perfect for**: Order processing, distributed transactions (Saga pattern), AI agent orchestration, and any workflow that must survive crashes.

For detailed documentation, visit [https://i2y.github.io/edda/](https://i2y.github.io/edda/)

## Key Features

- ✨ **Lightweight Library**: Runs in your application process - no separate server infrastructure
- 🔄 **Durable Execution**: Deterministic replay with workflow history for automatic crash recovery
- 🎯 **Workflow & Activity**: Clear separation between orchestration logic and business logic
- 🔁 **Saga Pattern**: Automatic compensation on failure with `@on_failure` decorator
- 🌐 **Multi-worker Execution**: Run workflows safely across multiple servers or containers
- 🔒 **Pydantic Integration**: Type-safe workflows with automatic validation
- 📦 **Transactional Outbox**: Reliable event publishing with guaranteed delivery
- ☁️ **CloudEvents Support**: Native support for CloudEvents protocol
- ⏱️ **Event & Timer Waiting**: Free up worker resources while waiting for events or timers, resume on any available worker
- 📬 **Channel-based Messaging**: Actor-model style communication with competing (job queue) and broadcast (fan-out) modes
- 🧩 **Actor OS Primitives**: Names, monitors, capabilities and sealed channels, tasks, an actor filesystem and supervision trees, shared with Romancy (Go) and Shikibu (Ruby)
- ⚡ **Instant Notifications**: PostgreSQL LISTEN/NOTIFY for near-instant event delivery (optional)
- 🤖 **MCP Integration**: Expose durable workflows as AI tools via Model Context Protocol
- 🧠 **Mirascope Integration**: Durable LLM calls
- 🦙 **LlamaIndex Integration**: Make LlamaIndex Workflows durable with crash recovery
- 📊 **pydantic-graph Integration**: Durable graph-based workflows (experimental)
- 🌍 **ASGI/WSGI Support**: Deploy with your preferred server (uvicorn, gunicorn, uWSGI)

## Use Cases

Edda excels at orchestrating **long-running workflows** that must survive failures:

- **🏢 Long-Running Jobs**: Order processing, data pipelines, batch jobs - from minutes to days, weeks, or even months
- **🔄 Distributed Transactions**: Coordinate microservices with automatic compensation (Saga pattern)
- **🤖 AI Agent Workflows**: Orchestrate multi-step AI tasks (LLM calls, tool usage, long-running inference)
- **📡 Event-Driven Workflows**: React to external events with guaranteed delivery and automatic retry

### Business Process Automation

Edda's waiting functions make it ideal for time-based and event-driven business processes:

- **📧 User Onboarding**: Send reminders if users haven't completed setup after N days
- **🎁 Campaign Processing**: Evaluate conditions and notify winners after campaign ends
- **💳 Payment Reminders**: Send escalating reminders before payment deadlines
- **📦 Scheduled Notifications**: Shipping updates, subscription renewals, appointment reminders

**Waiting functions**:
- `sleep(seconds)`: Wait for a relative duration
- `sleep_until(target_time)`: Wait until an absolute datetime (e.g., campaign end date)
- `wait_event(event_type)`: Wait for external events (near real-time response)

```python
@workflow
async def onboarding_reminder(ctx: WorkflowContext, user_id: str):
    await sleep(ctx, seconds=3*24*60*60)  # Wait 3 days
    if not await check_completed(ctx, user_id):
        await send_reminder(ctx, user_id)
```

**Key benefit**: Workflows **never lose progress** - crashes and restarts are handled automatically through deterministic replay.

## Architecture

Edda runs as a lightweight library in your applications, with all workflow state stored in a shared database:

```mermaid
%%{init: {'theme':'base', 'themeVariables': {'primaryTextColor':'#1a1a1a', 'secondaryTextColor':'#1a1a1a', 'tertiaryTextColor':'#1a1a1a', 'textColor':'#1a1a1a', 'nodeTextColor':'#1a1a1a'}}}%%
graph TB
    subgraph ext["External Systems"]
        API[REST API<br/>Clients]
        CE[CloudEvents<br/>Producer]
    end

    subgraph cluster["Your Multiple Instances"]
        subgraph pod1["order-service Pod 1"]
            W1[Edda Workflow]
        end
        subgraph pod2["order-service Pod 2"]
            W2[Edda Workflow]
        end
        subgraph pod3["order-service Pod 3"]
            W3[Edda Workflow]
        end
    end

    DB[(Shared Database<br/>PostgreSQL/MySQL<br/>SQLite: single-process only)]

    API -->|"workflow.start()<br/>(Direct Invocation)"| W1
    API -->|"workflow.start()<br/>(Direct Invocation)"| W2
    CE -->|"POST /<br/>(CloudEvents)"| W1
    CE -->|"POST /<br/>(CloudEvents)"| W3

    W1 <-->|Workflow<br/>State| DB
    W2 <-->|Workflow<br/>State| DB
    W3 <-->|Workflow<br/>State| DB

    style DB fill:#e1f5ff
    style W1 fill:#fff4e6
    style W2 fill:#fff4e6
    style W3 fill:#fff4e6
```

**Key Points**:

- Multiple workers can run simultaneously across different pods/servers
- Each workflow instance runs on only one worker at a time (automatic coordination)
- `wait_event()` and `sleep()` free up worker resources while waiting, resume on any worker when event arrives or timer expires
- Automatic crash recovery with stale lock cleanup and workflow auto-resume

## Quick Start

```python
from edda import EddaApp, workflow, activity, WorkflowContext

@activity
async def process_payment(ctx: WorkflowContext, amount: float):
    # Durable execution - automatically recorded in history
    print(f"Processing payment: ${amount}")
    return {"status": "paid", "amount": amount}

@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str, amount: float):
    # Workflow orchestrates activities with automatic retry on crash
    result = await process_payment(ctx, amount)
    return {"order_id": order_id, **result}

# Simplified example - production code needs:
# 1. await app.initialize() before starting workflows
# 2. try-finally with await app.shutdown() for cleanup
# 3. PostgreSQL or MySQL for multi-process/multi-pod deployments
app = EddaApp(db_url="sqlite:///workflow.db")

# Start workflow
instance_id = await order_workflow.start(order_id="ORD-123", amount=99.99)
```

**What happens on crash?**

1. Activities already executed return cached results from history
2. Workflow resumes from the last checkpoint
3. No manual intervention required

## Installation

Install Edda from PyPI using uv:

```bash
# Basic installation (includes SQLite support)
uv add edda-framework

# With PostgreSQL support
uv add edda-framework --extra postgresql

# With MySQL support
uv add edda-framework --extra mysql

# With Viewer UI
uv add edda-framework --extra viewer

# With PostgreSQL instant notifications (LISTEN/NOTIFY)
uv add edda-framework --extra postgres-notify

# With LlamaIndex Workflow integration
uv add edda-framework --extra llamaindex

# With pydantic-graph integration (experimental)
uv add edda-framework --extra graph

# All extras (PostgreSQL, MySQL, Viewer UI)
uv add edda-framework --extra postgresql --extra mysql --extra viewer
```

### Installing from GitHub (Development Versions)

Install the latest development version directly from GitHub:

```bash
# Using uv (latest from main branch)
uv add git+https://github.com/i2y/edda.git

# Using pip
pip install git+https://github.com/i2y/edda.git
```

**Install specific version or branch:**

```bash
# Specific tag/release
uv add git+https://github.com/i2y/edda.git@v0.1.0
pip install git+https://github.com/i2y/edda.git@v0.1.0

# Specific branch
uv add git+https://github.com/i2y/edda.git@feature-branch
pip install git+https://github.com/i2y/edda.git@feature-branch

# With extras (PostgreSQL, Viewer)
uv add "git+https://github.com/i2y/edda.git[postgresql,viewer]"
pip install "git+https://github.com/i2y/edda.git[postgresql,viewer]"
```

**Database Drivers**:
- **SQLite**: Included by default (via `aiosqlite`)
  - Single-process deployments only (supports multiple async workers within one process, not multiple processes/pods)
- **PostgreSQL**: Add `--extra postgresql` for `asyncpg` driver
  - **Recommended for production**
- **MySQL**: Add `--extra mysql` for `aiomysql` driver
  - **Recommended for production**
- **Viewer UI**: Add `--extra viewer` for workflow visualization

### Database Selection Guide

| Database | Use Case | Multi-Pod Support | Production Ready | Notes |
|----------|----------|-------------------|------------------|-------|
| **SQLite** | Development, testing, single-process deployments | ❌ No | ⚠️ Limited | Supports multiple async workers within one process, but not multiple processes/pods (K8s, Docker Compose with multiple replicas) |
| **PostgreSQL** | Production, multi-process/multi-pod systems | ✅ Yes | ✅ Yes | **Recommended for production** - Full support for database-based exclusive control and concurrent workflows |
| **MySQL** | Production with existing MySQL infrastructure | ✅ Yes | ✅ Yes | Suitable for production - Good choice if you already use MySQL |

**Session settings**: Edda sets the session time zone to UTC (PostgreSQL and MySQL) and the isolation level to `READ COMMITTED` (MySQL) on every connection. This also applies to an engine you create yourself and pass to `SQLAlchemyStorage`, including connections it opened before the storage was created. An explicit `isolation_level` on your engine is kept.

**MySQL identifiers**: MySQL compares identifiers (instance IDs, channel names, actor names, paths, capability targets) with the database collation. The default `utf8mb4_0900_ai_ci` ignores case and accents, so `Orders` and `orders` are the same channel. For exact matching create the database with `utf8mb4_0900_bin`. Never change the collation of single columns. Migration `20261208000000_widen_actor_owner_columns` widens the MySQL instance-id columns of the actor tables to `VARCHAR(255)`.

**SQLite**: File-backed SQLite databases run in WAL mode, so `-wal` and `-shm` files appear next to the database (do not use a network filesystem). An activity that hits a SQLite write conflict is restarted in a new transaction; this does not count as a retry.

**Important**: For multi-process or multi-pod deployments (K8s, Docker Compose with multiple replicas, etc.), you **must** use PostgreSQL or MySQL. SQLite supports multiple async workers within a single process, but its table-level locking makes it unsuitable for multi-process/multi-pod scenarios.

> **Tip**: For PostgreSQL, install the `postgres-notify` extra for near-instant event delivery using LISTEN/NOTIFY instead of polling.

### Database Schema Migration

**Automatic Migration (Default)**

Edda automatically applies database migrations at startup. No manual commands needed:

```python
from edda import EddaApp

# Migrations are applied automatically
app = EddaApp(db_url="postgresql://user:pass@localhost/dbname")
```

This is safe in multi-worker environments - Edda handles concurrent startup gracefully.

**Manual Migration with dbmate (Optional)**

For explicit schema control, you can disable auto-migration and use [dbmate](https://github.com/amacneil/dbmate):

```python
# Disable auto-migration
app = EddaApp(
    db_url="postgresql://...",
    auto_migrate=False  # Use dbmate-managed schema
)
```

```bash
# Install dbmate
brew install dbmate  # macOS

# Add schema submodule
git submodule add https://github.com/durax-io/schema.git schema

# Run migration manually
DATABASE_URL="postgresql://user:pass@localhost/dbname" dbmate -d ./schema/db/migrations/postgresql up
```

> **Note**: Edda's auto-migration uses the same SQL files as dbmate, maintaining full compatibility.

### Development Installation

If you want to contribute to Edda or modify the framework itself:

```bash
# Clone repository
git clone https://github.com/i2y/edda.git
cd edda
uv sync --all-extras
```

### Running Tests

Run Edda's test suite:

```bash
# Run tests
uv run pytest

# Run with coverage
uv run pytest --cov=edda
```

## Core Concepts

### Workflows and Activities

**Activity**: A unit of work that performs business logic. Activity results are recorded in history.

**Workflow**: Orchestration logic that coordinates activities. Workflows can be replayed from history after crashes.

```python
from edda import workflow, activity, WorkflowContext

@activity
async def send_email(ctx: WorkflowContext, email: str, message: str):
    # Business logic - this will be recorded
    print(f"Sending email to {email}")
    return {"sent": True}

@workflow
async def user_signup(ctx: WorkflowContext, email: str):
    # Orchestration logic
    await send_email(ctx, email, "Welcome!")
    return {"status": "completed"}
```

**Activity IDs**: Activities are automatically identified with IDs like `"send_email:1"` for deterministic replay. Manual IDs are only needed for concurrent execution (e.g., `asyncio.gather`). An activity called inside another activity's body takes no ID: it runs as part of the outer activity, in its transaction.

### Durable Execution

Edda ensures workflow progress is never lost through **deterministic replay**:

1. **Activity results are recorded** in a history table
2. **On crash recovery**, workflows resume from the last checkpoint
3. **Already-executed activities** return cached results from history
4. **New activities** continue from where the workflow left off

```python
@workflow
async def long_running_workflow(ctx: WorkflowContext, user_id: str):
    # Activity 1: Recorded in history
    result1 = await create_user(ctx, user_id)

    # If process crashes here, activity won't re-execute on restart

    # Activity 2: Continues from history on restart
    result2 = await send_welcome_email(ctx, result1["email"])

    return result2
```

**Key guarantees**:
- Activities execute **exactly once** (results cached in history)
- Workflows can survive **arbitrary crashes**
- No manual checkpoint management required

### Automatic Activity Retry

Activities automatically retry with exponential backoff when errors occur, improving reliability without manual error handling:

```python
from edda import activity, WorkflowContext

@activity
async def call_external_api(ctx: WorkflowContext, url: str):
    # Automatically retries up to 5 times with exponential backoff
    # Delays: 1s, 2s, 4s, 8s, 16s
    async with httpx2.AsyncClient() as client:
        response = await client.get(url, timeout=10)
    return response.json()
```

**Default retry policy**:
- **5 attempts** (including initial)
- **Exponential backoff**: 1s, 2s, 4s, 8s, 16s between attempts
- **Max delay**: 60 seconds
- **Total duration**: 5 minutes maximum

**Custom retry policies** for specific activities:

```python
from edda import activity, RetryPolicy, WorkflowContext

@activity(retry_policy=RetryPolicy(
    max_attempts=3,
    initial_interval=0.5,
    backoff_coefficient=2.0,
    max_interval=10.0,
    max_duration=60.0
))
async def flaky_operation(ctx: WorkflowContext, data: dict):
    # Custom: 3 attempts, delays 0.5s, 1s, 2s
    return await external_service.process(data)
```

**Application-level default policy**:

```python
from edda import EddaApp, RetryPolicy

app = EddaApp(
    db_url="sqlite:///workflow.db",
    default_retry_policy=RetryPolicy(
        max_attempts=10,
        initial_interval=2.0
    )
)
```

**Non-retryable errors** with `TerminalError`:

```python
from edda import activity, TerminalError, WorkflowContext

@activity
async def validate_user(ctx: WorkflowContext, user_id: str):
    user = await get_user(user_id)
    if user is None:
        # Immediately fail without retry (user doesn't exist)
        raise TerminalError(f"User {user_id} not found")
    return user
```

**Retry metadata for observability**:

Retry information is automatically embedded in activity history for monitoring:

```python
{
    "event_type": "ActivityCompleted",
    "event_data": {
        "activity_name": "call_external_api",
        "result": {...},
        "retry_metadata": {
            "total_attempts": 3,
            "total_duration_ms": 7200,
            "last_error": {...},
            "exhausted": False,
            "errors": [...]
        }
    }
}
```

**Policy resolution order**:
1. Activity-level policy (`@activity(retry_policy=...)`)
2. Application-level policy (`EddaApp(default_retry_policy=...)`)
3. Framework default (5 attempts, exponential backoff)

### Compensation (Saga Pattern)

When a workflow fails, Edda automatically executes compensation functions for **already-executed activities in reverse order**. This implements the Saga pattern for distributed transaction rollback.

**Key behavior**:
- Compensation functions run in **reverse order** of activity execution
- Only **already-executed activities** are compensated
- If Activity A and B completed, then C fails → B and A compensations run (in that order)

```python
from edda import activity, on_failure, compensation, workflow, WorkflowContext

@compensation
async def cancel_reservation(ctx: WorkflowContext, item_id: str):
    # Automatically called on workflow failure (reverse order)
    print(f"Cancelled reservation for {item_id}")
    return {"cancelled": True}

@activity
@on_failure(cancel_reservation)
async def reserve_inventory(ctx: WorkflowContext, item_id: str):
    print(f"Reserved {item_id}")
    return {"reserved": True}

@workflow
async def order_workflow(ctx: WorkflowContext, item1: str, item2: str):
    await reserve_inventory(ctx, item1)  # Step 1: Reserve item1
    await reserve_inventory(ctx, item2)  # Step 2: Reserve item2
    await charge_payment(ctx)            # Step 3: If this fails...
    # Compensation runs: cancel item2 → cancel item1 (reverse order)
```

### Multi-worker Execution

Multiple workers can safely process workflows using database-based exclusive control. This means:

- Edda uses database-based locks (not Redis or ZooKeeper)
- Each workflow instance runs on only one worker at a time
- A worker claims only instances of the workflows it has registered, so every worker must register the workflows it should run
- If a worker crashes, another worker automatically resumes
- No additional infrastructure required

```python
# Worker 1 and Worker 2 can run simultaneously
# Only one will acquire the lock for each workflow instance

app = EddaApp(
    db_url="postgresql://localhost/workflows",  # Shared database for coordination
    service_name="order-service",
    # Connection pool settings (optional)
    pool_size=5,        # Concurrent connections
    max_overflow=10,    # Additional burst capacity
    # Batch processing (optional)
    max_workflows_per_batch=10,  # Or "auto" / "auto:cpu" for dynamic scaling
)
```

**Features**:
- Each workflow instance runs on only one worker at a time (automatic coordination)
- Automatic stale lock cleanup (5-minute timeout)
- Crashed workflows automatically resume on any available worker

### Pydantic Integration

Type-safe workflows with automatic validation:

```python
from pydantic import BaseModel, Field
from edda import workflow, WorkflowContext

class OrderItem(BaseModel):
    item_id: str
    quantity: int = Field(..., ge=1)
    price: float = Field(..., gt=0)

@workflow
async def process_order(ctx: WorkflowContext, items: list[OrderItem]) -> dict:
    # Automatic validation before workflow starts
    total = sum(item.price * item.quantity for item in items)
    return {"total": total, "item_count": len(items)}
```

### Transactional Outbox

Activities are automatically transactional by default, ensuring atomicity:

```python
from edda import activity, send_event_transactional, WorkflowContext

@activity  # Automatically transactional
async def create_order(ctx: WorkflowContext, order_id: str):
    # All operations in a single transaction:
    # 1. Activity execution
    # 2. History recording
    # 3. Event publishing (outbox table)

    await send_event_transactional(
        ctx,
        event_type="order.created",
        event_source="order-service",
        event_data={"order_id": order_id}
    )

    return {"order_id": order_id}
```

**Custom Database Operations** - Use `ctx.session` for your database operations:

```python
@activity  # Edda manages the transaction
async def process_payment(ctx: WorkflowContext, order_id: str, amount: float):
    # Access Edda-managed session (same database as Edda)
    session = ctx.session

    # Your business logic
    payment = Payment(order_id=order_id, amount=amount)
    session.add(payment)

    # Edda event (same transaction)
    await send_event_transactional(
        ctx,
        event_type="payment.processed",
        event_source="payment-service",
        event_data={"order_id": order_id, "amount": amount}
    )

    # Edda automatically commits: your data + Edda's outbox (atomic!)
    return {"payment_id": f"PAY-{order_id}"}
```

## Event Integration

Edda provides optional event-driven capabilities for workflows that need to wait for external events.

### CloudEvents Support

Native support for CloudEvents protocol:

```python
from edda import EddaApp

app = EddaApp(
    db_url="sqlite:///workflow.db",
    service_name="order-service",
    outbox_enabled=True  # Enable transactional outbox
)

# Accepts CloudEvents at any HTTP path
```

**CloudEvents handling**:
- All HTTP requests (any path) are accepted as CloudEvents
- Events without matching workflow handlers are silently discarded
- Special endpoint: `POST /cancel/{instance_id}` for workflow cancellation. `running`, `waiting_*` and `recurred` instances can be cancelled; an instance that is running its compensations cannot (the request is refused and the compensations run to the end)
- An event for a channel sealed for `channel_pub` must carry the token in the `capabilitytoken` extension attribute (binary mode: `ce-capabilitytoken` header). Otherwise the endpoint answers 403 `{"error", "error_type": "Forbidden", "retryable": false}` and nothing is published. The attribute is not stored
- Automatic CloudEvents validation and parsing
- Works with CloudEvents-compatible systems (Knative Eventing, CloudEvents SDKs, etc.)

**CloudEvents HTTP Binding compliance**:
- **202 Accepted**: Event stored; its workflows and handlers run after the response (success)
- **400 Bad Request**: CloudEvents parsing/validation error (non-retryable)
- **403 Forbidden**: The event's channel is sealed and the event carries no valid `capabilitytoken` (non-retryable)
- **500 Internal Server Error**: Internal error, such as an event that could not be stored (retryable)
- **503 Service Unavailable**: The app is shutting down (retryable)
- Error responses include `error_type` and `retryable` flags for client retry logic

### Event & Timer Waiting

Workflows can wait for external events or timers without consuming worker resources. While waiting, the workflow state is persisted to the database and can be resumed by **any available worker** when the event arrives or timer expires:

```python
from edda import workflow, wait_event, send_event, WorkflowContext

@workflow
async def payment_workflow(ctx: WorkflowContext, order_id: str):
    # Send payment request event
    await send_event("payment.requested", "payment-service", {"order_id": order_id})

    # Wait for payment completion event (process-releasing)
    payment_event = await wait_event(ctx, "payment.completed")

    return payment_event.data
```

**ReceivedEvent attributes**: The `wait_event()` function returns a `ReceivedEvent` object:

```python
event = await wait_event(ctx, "payment.completed")
amount = event.data["amount"]           # Event payload (dict or bytes)
source = event.metadata.source          # CloudEvents source
event_type = event.metadata.type        # CloudEvents type
extensions = event.extensions           # CloudEvents extensions
```

**Timeout handling with EventTimeoutError**:

```python
from edda import wait_event, EventTimeoutError

try:
    event = await wait_event(ctx, "payment.completed", timeout_seconds=60)
except EventTimeoutError:
    # Handle timeout (e.g., cancel order, send reminder)
    await cancel_order(ctx, order_id)
```

**sleep() for time-based waiting**:

```python
from edda import sleep

@workflow
async def order_with_timeout(ctx: WorkflowContext, order_id: str):
    # Create order
    await create_order(ctx, order_id)

    # Wait 60 seconds for payment
    await sleep(ctx, seconds=60)

    # Check payment status
    return await check_payment(ctx, order_id)
```

**Multi-worker continuation behavior**:
- `wait_event()` releases the workflow lock atomically
- Event delivery acquires the lock and resumes on any available worker
- Safe for multi-pod/multi-container environments (K8s, Docker Compose, etc.)
- No worker is blocked while waiting for events or timers

**For technical details**, see [Multi-Worker Continuations](local-docs/distributed-coroutines.md).

### Channel-based Messaging

Edda provides channel-based messaging for workflow-to-workflow communication with two delivery modes:

```python
from edda import workflow, subscribe, receive, publish, send_to, WorkflowContext

# Job Worker - processes jobs exclusively (competing mode)
@workflow
async def job_worker(ctx: WorkflowContext, worker_id: str):
    # Subscribe with competing mode - each job goes to ONE worker only
    await subscribe(ctx, channel="jobs", mode="competing")

    job = await receive(ctx, channel="jobs")  # Get next job
    await process_job(ctx, job.data)
    await ctx.recur(worker_id=worker_id)  # Continue with fresh history

# Notification Handler - receives ALL messages (broadcast mode)
@workflow
async def notification_handler(ctx: WorkflowContext, handler_id: str):
    # Subscribe with broadcast mode - ALL handlers receive each message
    await subscribe(ctx, channel="notifications", mode="broadcast")

    msg = await receive(ctx, channel="notifications")
    await send_notification(ctx, msg.data)
    await ctx.recur(handler_id=handler_id)

# Publish to channel (all subscribers or one competing subscriber)
await publish(ctx, channel="jobs", data={"task": "send_report"})

# Direct message to specific workflow instance
await send_to(ctx, instance_id="workflow-123", channel="approval", data={"approved": True})
```

**Delivery modes**:
- **`competing`**: Each message goes to exactly ONE subscriber (job queue/task distribution)
- **`broadcast`**: Each message goes to ALL subscribers (notifications/fan-out)

**Key features**:
- **Channel-based messaging**: Messages are delivered to workflows waiting on specific channels
- **Competing vs Broadcast**: Choose semantics per subscription
- **Direct messaging**: `send_to(ctx, instance_id, data, channel=...)` delivers to `"{channel}:{instance_id}"`; the receiver subscribes with `mode="direct"`
- **Payloads**: `receive()` returns the published value as it was sent: any JSON value (dict, list, str, int, float, bool, None) or `bytes`. `publish()` and `send_to()` take `data: Any`
- **Workflow code only**: `receive()` called inside an activity raises `TerminalError`. `publish()` and `send_to()` inside an activity join the activity's transaction and deliver after commit
- **Database-backed**: All messages are persisted for durability
- **Lock-first delivery**: Safe for multi-worker environments
- **Replay-safe**: Called from workflow code, `publish()` and `send_to()` are recorded steps; a replay does not publish again
- **Sealed channels**: Once a `channel_pub` or `channel_sub` capability is issued for a channel, publishing or receiving on it (including its direct channels) requires a token, passed as `capability_token=`
- **Seals outlast expiry**: an expired grant keeps its channel sealed; only revoking the last grant re-opens it. `revoke_capability` works only for the instance that issued the capability and `unregister_name` only for the instance that holds the name (`NotOwnerError` otherwise; an unknown ID or an unbound name is a no-op). A sweep never removes the newest grant of a channel
- **Capability key**: `EddaApp(capability_key=...)` (or `DURAX_CAPABILITY_KEY`) derives every new secret as the HMAC-SHA256 of the capability ID, and history then records no token. Without a key the token is stored in history. Keep the key the same on all workers while the issuing instances are alive
- **CloudEvents ingress**: an event for a channel sealed for `channel_pub` needs a valid token in the `capabilitytoken` extension (`ce-capabilitytoken` header in binary mode), or the endpoint answers 403
- **Direct channels are private**: only the receiving instance can `subscribe`, `receive` or read (`read_channel_since`, `read_channel_after`) its `"{channel}:{instance_id}"`. For another existing instance these raise `ChannelPermissionError` whatever the seal state; `send_to()` and targeted CloudEvents to it stay allowed

### Actor Primitives

Workflows double as actors of a kernel shared with Romancy and Shikibu (same schema, same semantics). The primitives live in their own modules:

- **`edda.actors`**: `register_name`, `unregister_name`, `whereis`, `monitor`, `unmonitor`, `issue_capability` (`expires_at=` issues an expiring capability), `check_capability`, `revoke_capability`
- **`edda.tasks`**: `post_task`, `claim_task`, `complete_task`, `fail_task`, `list_tasks`, `wait_task`, `wait_any_task`, `read_channel_since` (raises `CursorNotFoundError` when the cursor message is not on the channel), `read_channel_after` (numeric `id` cursor, `0` = from the start; keeps working after the cursor message is deleted). Returned messages carry `id` and `message_id`
- **`edda.fs`** (inside activities only): `fs_read`, `fs_write`, `fs_list`, `fs_delete`
- **`edda.blobs`**: `store_payload`, `load_payload`, `stat_payload`
- **`edda.supervisor`**: `define_supervisor`, `ChildSpec`, `Strategy`, `RestartMode`. A child written for Romancy or Shikibu sets `framework="go"` or `framework="ruby"` on its `ChildSpec`: each runtime runs only the instances of its own framework. Without `input`, a Go child gets `null` and a Python or Ruby child `{}`

```python
from edda import workflow, subscribe, receive, WorkflowContext
from edda.actors import whereis, monitor
from edda.supervisor import ChildSpec, RestartMode, Strategy, define_supervisor

@workflow
async def watcher(ctx: WorkflowContext) -> str:
    target = await whereis(ctx, "ingest")
    if target is None:
        return "not running"
    # The DOWN message arrives on this instance's direct channel
    await subscribe(ctx, channel="down", mode="direct")
    await monitor(ctx, target, "down")
    msg = await receive(ctx, channel="down")
    return msg.data["status"]  # "completed", "failed" or "cancelled" (a recurring instance stays alive: no DOWN)

sup = define_supervisor(
    "pipeline-sup",
    strategy=Strategy.ONE_FOR_ONE,
    max_restarts=5,
    within_seconds=60,
    children=[
        ChildSpec(name="ingest", workflow="ingest", restart=RestartMode.PERMANENT),
        # A workflow that Romancy runs
        ChildSpec(name="indexer", workflow="index_repo", framework="go",
                  input={"repo": "/srv/app"}, restart=RestartMode.TRANSIENT),
    ],
)
# instance_id = await sup.start()
```

Called from workflow code, every primitive is a recorded step: a replay returns the recorded outcome, exceptions included, instead of running it again. A primitive with a side effect first proves that the run still holds the instance lock, so a run that lost its lease changes nothing (`LockLostError`). Only kernel errors that carry a code are recorded (`task_conflict`, `task_not_found`, `actor_name_taken`, `fs_conflict`, `sandboxed`, `channel_forbidden`, `not_owner`, `cursor_not_found`, `invalid_argument`).

Any other failure aborts the run: a database error inside a primitive or around an activity attempt (raised as `RunAbortedError`), a lost lock (`LockLostError`), or a shutdown. An aborted run is not failed and nothing is compensated; the instance stays `running` and a later run replays it. `RunAbortedError` derives from `BaseException`, so `except Exception:` does not swallow it. Do not catch either error. Task handlers and supervised children are at-least-once and must be idempotent.

### Sandboxed Actors

Workflows can be started as sandboxed actors when they should receive only explicitly delegated capabilities:

```python
child_id = await untrusted_workflow.start(sandboxed=True, input=value)
```

Sandboxing enforces three rules:

- A sandboxed workflow cannot mint new capabilities with `issue_capability`.
- Children started by a sandboxed workflow inherit the sandbox flag. A `recur()` continues the same instance and keeps its flag.
- Only an unsandboxed parent (or an external caller) may use `sandboxed=True` for a child.

Delegation itself uses the existing capability tokens: pass tokens the child needs in its input. Edda itself enforces these rules. Code in the worker process that uses the storage directly can bypass them, and database-level isolation (for example PostgreSQL row-level security) is not provided.

### Workflow Recurrence

Long-running workflows can use `ctx.recur()` to continue with fresh history. This is essential for workflows that run indefinitely (job workers, notification handlers, etc.):

```python
from edda import workflow, subscribe, receive, WorkflowContext

@workflow
async def job_worker(ctx: WorkflowContext, worker_id: str):
    await subscribe(ctx, channel="jobs", mode="competing")

    # Process one job
    job = await receive(ctx, channel="jobs")
    await process_job(ctx, job.data)

    # Archive history and continue on the same instance id
    # (keyword arguments become the next iteration's input)
    await ctx.recur(worker_id=worker_id)
```

**How it works**:
- **Prevents history growth**: The current history is archived to `workflow_history_archive`, and the next iteration starts fresh
- **Same instance id**: `recur()` archives the history, clears the instance's subscriptions, timers, group memberships and registered compensations, stores the new input and continues on the same instance id. Between iterations the status is `recurred`, which counts as alive: no DOWN message is sent and registered names stay bound. There is no `continued_from` chain
- **Subscribe again**: Subscriptions are cleared, so subscribe again at the start of the workflow (as above). A broadcast channel you subscribe to again continues after the last message delivered to the instance; competing messages wait in the channel
- **Identity survives**: Names, monitors, issued capabilities, tasks, the actor FS (`/actor/{id}/`) and the sandbox flag all stay with the instance. A supervisor keeps its id across recurs, so its children's ids stay stable too
- **Compensations**: Each iteration is a finished unit; a failure in a later iteration does not undo an earlier one

### ASGI Integration

Edda runs as an ASGI application:

```bash
# Run standalone
uvicorn demo_app:application --port 8001
```

**Mounting to existing ASGI apps:**

You can mount EddaApp to any path in existing ASGI frameworks:

```python
from fastapi import FastAPI
from edda import EddaApp

# Create FastAPI app
api = FastAPI()

# Create Edda app
edda_app = EddaApp(db_url="sqlite:///workflow.db")

# Mount Edda at /workflows path
api.mount("/workflows", edda_app)

# Now Edda handles all requests under /workflows/*
# POST /workflows/any-path -> CloudEvents handler
# POST /workflows/cancel/{instance_id} -> Cancellation
```

This works with any ASGI framework (Starlette, FastAPI, Quart, etc.)

### WSGI Integration

For WSGI environments (gunicorn, uWSGI, Flask, Django), use the WSGI adapter:

```python
from edda import EddaApp
from edda.wsgi import create_wsgi_app

# Create Edda app
edda_app = EddaApp(db_url="sqlite:///workflow.db")

# Convert to WSGI
wsgi_application = create_wsgi_app(edda_app)
```

**Running with WSGI servers:**

```bash
# With Gunicorn
gunicorn demo_app:wsgi_application --workers 4

# With uWSGI
uwsgi --http :8000 --wsgi-file demo_app.py --callable wsgi_application
```

**Sync Activities**: For WSGI environments or legacy codebases, you can write synchronous activities:

```python
from edda import activity, WorkflowContext

@activity
def process_payment(ctx: WorkflowContext, amount: float) -> dict:
    # Sync function - automatically executed in thread pool
    # No async/await needed!
    return {"status": "paid", "amount": amount}

@workflow
async def payment_workflow(ctx: WorkflowContext, order_id: str) -> dict:
    # Workflows still use async (for deterministic replay)
    result = await process_payment(ctx, 99.99)
    return result
```

**Performance note**: ASGI servers (uvicorn, hypercorn) are recommended for better performance with Edda's async architecture. WSGI support is provided for compatibility with existing infrastructure and users who prefer synchronous programming.

### Event Loop

Edda runs on the event loop of the server that runs it. uvicorn (with its default `--loop auto`) and tsuno use uvloop, which Edda installs as a dependency; with hypercorn, pass `--worker-class uvloop`. `create_wsgi_app()` runs the application on a uvloop loop, in a background thread of each worker process. In a script, start the loop with `uvloop.run(main())`.

Edda does not set an event loop policy: that API is deprecated since Python 3.14 and goes away in 3.16. Before Edda 0.18, `EddaApp.initialize()` set uvloop's policy. It ran inside the server's loop, so it changed only the loops created after it.

## MCP Integration

Edda integrates with the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/), allowing AI assistants like Claude to interact with your durable workflows as long-running tools.

### Quick Example

```python
from edda.integrations.mcp import EddaMCPServer
from edda import WorkflowContext, activity

# Create MCP server
server = EddaMCPServer(
    name="Order Service",
    db_url="postgresql://user:pass@localhost/orders",
)

@activity
async def process_payment(ctx: WorkflowContext, amount: float):
    return {"status": "paid", "amount": amount}

@server.durable_tool(description="Process customer order")
async def process_order(ctx: WorkflowContext, order_id: str):
    await process_payment(ctx, 99.99)
    return {"status": "completed", "order_id": order_id}

# Deploy with uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(server.asgi_app(), host="0.0.0.0", port=8000)
```

### Auto-Generated Tools

Each `@durable_tool` automatically generates **four MCP tools**:

1. **Main tool** (`process_order`): Starts the workflow, returns instance ID
2. **Status tool** (`process_order_status`): Checks workflow progress with completed activity count and suggested poll interval
3. **Result tool** (`process_order_result`): Gets final result when completed
4. **Cancel tool** (`process_order_cancel`): Cancels workflow if running or waiting, executes compensation handlers (refused while the workflow is already running its compensations)

This enables AI assistants to work with workflows that take minutes, hours, or even days to complete, with full control over the workflow lifecycle.

### MCP Prompts

Define reusable prompt templates that can access workflow state:

```python
from mcp.server.mcpserver.prompts.base import UserMessage
from mcp.types import TextContent

@server.prompt(description="Analyze a workflow execution")
async def analyze_workflow(instance_id: str) -> UserMessage:
    """Generate analysis prompt for a specific workflow."""
    instance = await server.storage.get_instance(instance_id)
    history = await server.storage.get_history(instance_id)

    text = f"""Analyze this workflow:
**Status**: {instance['status']}
**Activities**: {len(history)}
**Result**: {instance.get('output_data')}

Please provide insights and optimization suggestions."""

    return UserMessage(content=TextContent(type="text", text=text))
```

AI clients can use these prompts to generate context-aware analysis of your workflows.

**For detailed documentation**, see [MCP Integration Guide](docs/integrations/mcp.md).

## Observability Hooks

Extend Edda with custom observability without coupling to specific tools:

```python
from edda import EddaApp

class MyHooks:
    async def on_workflow_start(self, instance_id, workflow_name, input_data):
        print(f"Workflow {workflow_name} started: {instance_id}")

    async def on_workflow_complete(self, instance_id, workflow_name, result):
        print(f"Workflow {workflow_name} completed")

    async def on_activity_complete(self, instance_id, activity_id, activity_name, result, cache_hit):
        print(f"Activity {activity_name} completed (cache_hit={cache_hit})")

app = EddaApp(
    db_url="sqlite:///workflow.db",
    service_name="my-service",
    hooks=MyHooks()
)
```

`MyHooks` implements the `WorkflowHooks` Protocol through structural subtyping. See integration examples in the examples directory.

## Serialization

Edda supports both **JSON (dict)** and **binary (bytes)** data for event storage and transport, allowing you to choose based on your needs.

### JSON Data Support

For debugging and human-readable logs, use JSON dict format:

```python
from google.protobuf import json_format
from edda import send_event, wait_event

# Send: Protobuf → JSON dict
msg = OrderCreated(order_id="123", amount=99.99)
await send_event("order.created", "orders", json_format.MessageToDict(msg))

# Receive: JSON dict → Protobuf
event = await wait_event(ctx, "payment.completed")
payment = json_format.ParseDict(event.data, PaymentCompleted())
```

**Benefits**:
- ✅ Human-readable in database and logs
- ✅ Easy debugging and troubleshooting
- ✅ Full Viewer UI compatibility
- ✅ CloudEvents Structured Content Mode compatible

### Binary Data Support

For maximum performance and zero storage overhead, Edda stores binary data directly in database BLOB columns:

```python
from edda import send_event, wait_event

# Send binary data (e.g., Protobuf)
msg = OrderCreated(order_id="123", amount=99.99)
await send_event("order.created", "orders", msg.SerializeToString())  # bytes → BLOB

# Receive binary data
event = await wait_event(ctx, "payment.completed")
payment = PaymentCompleted()
payment.ParseFromString(event.data)  # bytes from BLOB
```

**Benefits**:
- ✅ Zero storage overhead (100 bytes → 100 bytes, not 133 bytes with base64)
- ✅ Maximum performance (no encoding/decoding)
- ✅ Native BLOB storage (SQLite, PostgreSQL, MySQL)
- ✅ CloudEvents Binary Content Mode compatible

### Choosing Between JSON and Binary Mode

Both modes are equally valid for production use:

- **JSON Mode**: Human-readable, excellent observability, Viewer UI support
  - Use when debugging, monitoring, and data inspection are priorities

- **Binary Mode**: Zero serialization overhead, smaller storage
  - Use when payload size or serialization performance are critical
  - Ideal for high-throughput scenarios (>1000 events/sec)

**Both modes are first-class citizens** - choose based on your specific requirements, not environment.

## Next Steps

- **[Getting Started](https://edda-framework.dev/getting-started/installation/)**: Installation and setup guide
- **[Core Concepts](https://edda-framework.dev/getting-started/concepts/)**: Learn about workflows, activities, and durable execution
- **[Examples](https://edda-framework.dev/examples/simple/)**: See Edda in action with real-world examples
- **[FastAPI Integration](https://edda-framework.dev/examples/fastapi-integration/)**: Integrate with FastAPI (direct invocation + CloudEvents)
- **[Transactional Outbox](https://edda-framework.dev/core-features/transactional-outbox/)**: Reliable event publishing with guaranteed delivery
- **[Viewer UI](https://edda-framework.dev/viewer-ui/setup/)**: Visualize and monitor your workflows
- **[Lifecycle Hooks](https://edda-framework.dev/core-features/hooks/)**: Add observability and monitoring with custom hooks
- **[CloudEvents HTTP Binding](https://edda-framework.dev/core-features/events/cloudevents-http-binding/)**: CloudEvents specification compliance and error handling

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- GitHub Issues: https://github.com/i2y/edda/issues
- Documentation: https://github.com/i2y/edda#readme
