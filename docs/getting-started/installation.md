# Installation

This guide will help you install Edda and set up your development environment.

## Prerequisites

- **Python 3.11 or higher**
- **uv** package manager (recommended) or pip

## Installing Edda

### Using uv

Install Edda from PyPI using uv:

```bash
# Basic installation (includes SQLite support)
uv add edda-framework
```

This installs Edda with SQLite support, which is perfect for:

- Local development
- Testing
- Single-process deployments (suitable for development, testing, and low-traffic single-server apps)

**Important**: For multi-process or multi-pod deployments (K8s, Docker Compose with multiple replicas, etc.), you must use PostgreSQL or MySQL. SQLite supports multiple async workers within a single process, but its table-level locking makes it unsuitable for multi-process/multi-pod scenarios.

**With database extras:**

```bash
# With PostgreSQL support (recommended for production)
uv add edda-framework --extra postgresql

# With MySQL support
uv add edda-framework --extra mysql

# With Viewer UI (workflow visualization)
uv add edda-framework --extra viewer

# With PostgreSQL instant notifications (LISTEN/NOTIFY)
uv add edda-framework --extra postgres-notify

# All extras (PostgreSQL + MySQL + Viewer UI)
uv add edda-framework --extra postgresql --extra mysql --extra viewer
```

**What gets installed:**

- **Base**: SQLite support via `aiosqlite` (always included)
- **postgresql**: `asyncpg` driver for PostgreSQL
- **mysql**: `aiomysql` driver for MySQL
- **viewer**: `nicegui` and `httpx` for workflow visualization UI
- **postgres-notify**: `asyncpg` driver for PostgreSQL LISTEN/NOTIFY instant notifications

### Using pip

If you prefer using pip:

```bash
# Basic installation
pip install edda-framework

# With PostgreSQL support
pip install "edda-framework[postgresql]"

# With MySQL support
pip install "edda-framework[mysql]"

# With Viewer UI
pip install "edda-framework[viewer]"

# With PostgreSQL instant notifications
pip install "edda-framework[postgres-notify]"

# All extras
pip install "edda-framework[postgresql,mysql,viewer]"
```

### Installing from GitHub (Development Versions)

For testing the latest development version or a specific branch/commit, you can install directly from GitHub:

#### Latest Development Version

```bash
# Using uv
uv add git+https://github.com/i2y/edda.git

# Using pip
pip install git+https://github.com/i2y/edda.git
```

This installs the latest code from the `main` branch.

#### Specific Version, Branch, or Commit

```bash
# Specific tag (e.g., v0.1.0)
uv add git+https://github.com/i2y/edda.git@v0.1.0
pip install git+https://github.com/i2y/edda.git@v0.1.0

# Specific branch
uv add git+https://github.com/i2y/edda.git@feature-branch
pip install git+https://github.com/i2y/edda.git@feature-branch

# Specific commit
uv add git+https://github.com/i2y/edda.git@abc1234
pip install git+https://github.com/i2y/edda.git@abc1234
```

#### With Database Extras

```bash
# Using uv (PostgreSQL + Viewer)
uv add "git+https://github.com/i2y/edda.git[postgresql,viewer]"

# Using pip
pip install "git+https://github.com/i2y/edda.git[postgresql,viewer]"
```

**When to use GitHub installation:**

- ✅ **Development & Testing**: Try unreleased features or bug fixes
- ✅ **Contributing**: Test your changes before submitting a PR
- ✅ **Specific Version**: Pin to a particular commit or branch
- ❌ **Production**: Use PyPI releases for production deployments

**Note**: GitHub installations require Git to be installed on your system.

## Verifying Installation

Check that Edda is installed correctly:

```python
# test_installation.py
import asyncio
from edda import EddaApp, workflow, activity, WorkflowContext

@activity
async def hello_activity(ctx: WorkflowContext, name: str):
    return f"Hello, {name}!"

@workflow
async def hello_workflow(ctx: WorkflowContext, name: str):
    result = await hello_activity(ctx, name)
    return {"message": result}

async def main():
    # Create app with in-memory SQLite
    app = EddaApp(service_name="demo-service", db_url="sqlite:///:memory:")

    # Initialize the app before starting workflows
    await app.initialize()

    try:
        # Start workflow
        instance_id = await hello_workflow.start(name="World")
        print(f"Workflow started: {instance_id}")

        # Get result
        instance = await app.storage.get_instance(instance_id)
        result = instance['output_data']['result']
        print(f"Result: {result}")

    finally:
        # Cleanup resources
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

Run the test:

```bash
uv run python test_installation.py
```

Expected output:

```
Workflow started: <instance_id>
Result: {'message': 'Hello, World!'}
```

The result is extracted from `instance['output_data']['result']`, which contains the return value of the workflow.

## Database Setup

### Database Selection

| Database | Use Case | Multi-Worker Support | Production Ready |
|----------|----------|-------------------|------------------|
| **SQLite** | Development, testing, single-process apps | ⚠️ Single-process only | ⚠️ Limited |
| **PostgreSQL** | Production, multi-process/multi-pod systems | ✅ Yes | ✅ Yes (Recommended) |
| **MySQL** | Production with existing MySQL infrastructure | ✅ Yes | ✅ Yes |

### SQLite (Default)

No additional setup required! SQLite databases are created automatically:

```python
from edda import EddaApp

# File-based SQLite (single-process only)
app = EddaApp(service_name="demo-service", db_url="sqlite:///workflow.db")

# In-memory (testing only)
app = EddaApp(service_name="demo-service", db_url="sqlite:///:memory:")
```

**SQLite Considerations:**

✅ **Supported scenarios:**

- Single-process deployments (even with multiple async workers within that process)
- Development and testing environments
- Low-traffic single-server applications

❌ **Not supported:**

- Multi-process deployments (Docker Compose with `replicas: 3`, multiple systemd services)
- Multi-pod deployments (Kubernetes with multiple replicas)
- High-concurrency production scenarios

⚠️ **Performance limitations:**

- Table-level locking (not row-level like PostgreSQL/MySQL)
- Concurrent writes are serialized, impacting throughput
- For production with multiple processes/pods, use PostgreSQL or MySQL

### PostgreSQL

1. **Install PostgreSQL** (if not already installed)

2. **Create a database**:

```sql
CREATE DATABASE edda_workflows;
```

3. **Configure connection**:

```python
from edda import EddaApp

app = EddaApp(
    service_name="demo-service",
    db_url="postgresql://user:password@localhost/edda_workflows"
)
```

#### Enabling Instant Notifications (LISTEN/NOTIFY)

For near-instant event and message delivery, enable PostgreSQL LISTEN/NOTIFY:

```bash
# Install the postgres-notify extra
uv add edda-framework --extra postgres-notify
# or
pip install "edda-framework[postgres-notify]"
```

```python
from edda import EddaApp

app = EddaApp(
    service_name="demo-service",
    db_url="postgresql://user:password@localhost/edda_workflows",
    use_listen_notify=True,  # Enable LISTEN/NOTIFY (auto-detected by default)
    notify_fallback_interval=30,  # Fallback polling interval in seconds
)
```

**Configuration options:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `use_listen_notify` | `bool \| None` | `None` | `None` = auto-detect (enabled for PostgreSQL), `True` = force enable, `False` = force disable |
| `notify_fallback_interval` | `int` | `30` | Fallback polling interval in seconds when NOTIFY is enabled |

**Benefits:**

- Near-instant event delivery (milliseconds vs. seconds with polling)
- Reduced database load (fewer polling queries)
- Automatic fallback to polling if NOTIFY fails
- Automatic reconnection on connection loss

See [PostgreSQL LISTEN/NOTIFY](../core-features/events/postgres-notify.md) for detailed documentation.

### MySQL

1. **Install MySQL** (if not already installed)

2. **Create a database**:

```sql
CREATE DATABASE edda_workflows;
```

3. **Configure connection**:

```python
from edda import EddaApp

app = EddaApp(
    service_name="demo-service",
    db_url="mysql://user:password@localhost/edda_workflows"
)
```

### Schema Migration

#### Automatic Migration (Default)

Edda automatically applies database migrations at startup. No manual commands needed:

```python
from edda import EddaApp

# Migrations are applied automatically at startup
app = EddaApp(
    service_name="demo-service",
    db_url="postgresql://user:pass@localhost/dbname"
)
```

**Key features:**

- **Zero configuration**: Works out of the box
- **Multi-worker safe**: Handles concurrent startup gracefully (race condition protected)
- **dbmate compatible**: Uses the same SQL files and `schema_migrations` table
- **Incremental**: Only applies pending migrations

#### Manual Migration with dbmate (Optional)

For explicit schema control, you can disable auto-migration and use [dbmate](https://github.com/amacneil/dbmate):

```python
# Disable auto-migration
app = EddaApp(
    service_name="demo-service",
    db_url="postgresql://...",
    auto_migrate=False  # Use dbmate-managed schema
)
```

```bash
# Install dbmate
brew install dbmate  # macOS
# Linux: curl -fsSL https://github.com/amacneil/dbmate/releases/latest/download/dbmate-linux-amd64 -o /usr/local/bin/dbmate && chmod +x /usr/local/bin/dbmate

# Add schema submodule to your project
git submodule add https://github.com/durax-io/schema.git schema

# Run migration manually
DATABASE_URL="postgresql://user:pass@localhost/dbname" dbmate -d ./schema/db/migrations/postgresql up

# Check status
dbmate -d ./schema/db/migrations/postgresql status
```

> **Note**: Edda's auto-migration uses the same SQL files as dbmate, so you can switch between modes freely.

### Multi-Worker Configuration

When running multiple Edda workers (e.g., in Kubernetes or with multiple processes), Edda automatically coordinates background tasks using **leader election**. Only one worker runs maintenance tasks (timers, message cleanup, etc.) while others focus on workflow execution.

```python
from edda import EddaApp

app = EddaApp(
    service_name="demo-service",
    db_url="postgresql://...",
    # Leader election settings (optional - defaults work well for most cases)
    leader_heartbeat_interval=15,  # How often workers check/renew leadership
    leader_lease_duration=45,      # How long before a failed leader is replaced
)
```

**Configuration options:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `leader_heartbeat_interval` | `int` | `15` | Interval in seconds for leader heartbeat |
| `leader_lease_duration` | `int` | `45` | Duration in seconds before leadership expires |

**Notes:**

- Default values work well for most deployments
- Reduce `leader_lease_duration` for faster failover (minimum: 3x heartbeat interval)
- Leader election uses the database for coordination (no external dependencies)

## Next Steps

- **[Quick Start](quick-start.md)**: Build your first workflow in 5 minutes
- **[Core Concepts](concepts.md)**: Learn about workflows, activities, and durable execution
- **[Your First Workflow](first-workflow.md)**: Step-by-step tutorial
