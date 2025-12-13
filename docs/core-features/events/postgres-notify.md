# PostgreSQL LISTEN/NOTIFY

Edda supports PostgreSQL's LISTEN/NOTIFY mechanism for near-instant event and message delivery. This optional feature significantly reduces latency compared to polling-based delivery.

## Overview

By default, Edda uses polling to check for new events and messages. With PostgreSQL LISTEN/NOTIFY enabled:

- **Event delivery**: Near-instant (milliseconds) instead of polling interval
- **Message delivery**: Near-instant instead of polling interval
- **Database load**: Reduced polling queries
- **Fallback**: Automatic fallback to polling if notifications are missed

## Installation

Install the `postgres-notify` extra:

```bash
# Using uv
uv add edda-framework --extra postgres-notify

# Using pip
pip install "edda-framework[postgres-notify]"
```

This installs `asyncpg`, which is required for LISTEN/NOTIFY support.

## Configuration

### Basic Usage (Auto-detection)

```python
from edda import EddaApp

# LISTEN/NOTIFY is auto-detected for PostgreSQL
app = EddaApp(
    service_name="demo-service",
    db_url="postgresql://user:password@localhost/workflows",
)
```

When using PostgreSQL, LISTEN/NOTIFY is automatically enabled if `asyncpg` is installed.

### Explicit Configuration

```python
from edda import EddaApp

app = EddaApp(
    service_name="demo-service",
    db_url="postgresql://user:password@localhost/workflows",
    use_listen_notify=True,  # Force enable
    notify_fallback_interval=30,  # Fallback polling every 30 seconds
)
```

### Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `use_listen_notify` | `bool \| None` | `None` | Notification mode: `None` = auto-detect, `True` = force enable, `False` = force disable |
| `notify_fallback_interval` | `int` | `30` | Fallback polling interval in seconds when NOTIFY is enabled |
| `max_workflows_per_batch` | `int \| "auto" \| "auto:cpu"` | `10` | Workflows per resume cycle. `"auto"` scales by queue depth, `"auto:cpu"` by CPU usage |

### Auto-detection Behavior

| Database | `use_listen_notify=None` | `use_listen_notify=True` | `use_listen_notify=False` |
|----------|--------------------------|--------------------------|---------------------------|
| PostgreSQL | Enabled (if asyncpg installed) | Enabled (error if asyncpg missing) | Disabled (polling only) |
| MySQL | Disabled | Error | Disabled |
| SQLite | Disabled | Error | Disabled |

## How It Works

### Architecture

```
┌─────────────────┐     NOTIFY      ┌─────────────────┐
│   Worker Pod 1  │ <────────────── │    PostgreSQL   │
│   (Edda App)    │                 │    Database     │
└─────────────────┘                 └─────────────────┘
         │                                   │
         │         LISTEN                    │
         └───────────────────────────────────┘
```

1. **Dedicated Connection**: Edda maintains a separate asyncpg connection for LISTEN/NOTIFY
2. **Channel Subscription**: Subscribes to notification channels on startup
3. **Instant Wakeup**: When a notification arrives, waiting workflows are immediately resumed
4. **Fallback Polling**: Periodic polling ensures no notifications are missed

### Notification Channels

Edda uses the following PostgreSQL channels:

- `edda_workflow_resume`: Notifies when workflows should be resumed
- `edda_outbox_ready`: Notifies when outbox events are ready for relay

## Performance Comparison

| Metric | Polling Only | With LISTEN/NOTIFY |
|--------|--------------|-------------------|
| Event delivery latency | 0.5-1s (polling interval) | ~10-50ms |
| Message delivery latency | 0.5-1s | ~10-50ms |
| Database queries per idle workflow | Every 1s | Every 30s (fallback only) |
| Connection overhead | 1 per pool | +1 dedicated LISTEN connection |

## Reliability Features

### Automatic Reconnection

The LISTEN connection automatically reconnects on failure with configurable retry settings.

### Fallback Polling

Even with NOTIFY enabled, Edda maintains fallback polling:

- **Default interval**: 30 seconds (`notify_fallback_interval`)
- **Purpose**: Catch any missed notifications
- **Behavior**: Polling runs in parallel with NOTIFY

## Troubleshooting

### asyncpg Not Installed

If you see a warning about asyncpg not being installed:

```
WARNING: asyncpg not installed, falling back to polling-only mode.
Install with: pip install edda-framework[postgres-notify]
```

**Solution**: Install the postgres-notify extra.

### Force Enable on Non-PostgreSQL

If you see an error about LISTEN/NOTIFY requiring PostgreSQL:

```
ValueError: use_listen_notify=True requires PostgreSQL database.
```

**Solution**: Use `use_listen_notify=None` (auto-detect) or `False` for non-PostgreSQL databases.

### Connection Lost

If you see reconnection messages in logs:

```
INFO: Connection lost, attempting reconnection...
```

This is normal behavior. Edda automatically reconnects, and workflows continue with polling fallback during reconnection.

## Best Practices

1. **Use auto-detection**: Set `use_listen_notify=None` (default) for portability across different database backends
2. **Install postgres-notify in production**: For best performance with PostgreSQL
3. **Monitor reconnections**: Frequent reconnection warnings may indicate network issues
4. **Adjust fallback interval**: Lower values provide more reliability but increase database load

## Related Topics

- [Event Waiting](wait-event.md) - How workflows wait for events
- [Channel-based Messaging](../messages.md) - Workflow-to-workflow communication
- [Installation](../../getting-started/installation.md) - Database setup
