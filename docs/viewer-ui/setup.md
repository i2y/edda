# Viewer UI Setup

The Edda Viewer UI provides workflow visualization and monitoring capabilities.

## Installation

Install Edda with Viewer UI support:

```bash
# Install Edda with viewer extras
pip install edda-framework[viewer]

# Or using uv
uv add edda-framework --extra viewer

# With all extras (PostgreSQL, MySQL, Viewer)
pip install edda-framework[postgresql,mysql,viewer]

# Or using uv
uv add edda-framework --extra postgresql --extra mysql --extra viewer
```

## Quick Start

The easiest way to start the Viewer is using the command-line tool:

```bash
# Start viewer with default settings (demo.db, port 8080)
edda-viewer

# With custom database and port
edda-viewer --db my_workflows.db --port 9000

# Import workflow modules for visualization
edda-viewer --import-module demo_app

# Multiple modules
edda-viewer -m demo_app -m my_workflows
```

Then open http://localhost:8080 in your browser.

![Workflow List View](images/workflow-list-view.png)

*The Viewer UI shows all workflow instances with status badges and action buttons*

**Pydantic Form Generation:**

![Pydantic Form Generation](images/start-workflow-form-pydantic.png)

*Auto-generated form fields based on Pydantic model type hints (str, float, int)*

![Workflow Selection](images/workflow-selection-dropdown.png)

*Dropdown showing all registered workflows with event_handler=True*

## Three Ways to Run the Viewer

### Method 1: Command Line (Recommended)

The simplest way to start the Viewer:

```bash
# Basic usage
edda-viewer

# Full options
edda-viewer --db demo.db --port 8080 --import-module demo_app
```

**Command Line Options:**

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--db` | `-d` | `demo.db` | Database file path (SQLite) |
| `--port` | `-p` | `8080` | Web server port |
| `--import-module` | `-m` | None | Python module to import (can be used multiple times) |

**Environment Variables:**

- `EDDA_DB_URL`: Database URL (overrides `--db` option)
  - SQLite: `sqlite:///demo.db`
  - PostgreSQL: `postgresql+asyncpg://user:pass@localhost/db`
  - MySQL: `mysql+aiomysql://user:pass@localhost/db`

**Examples:**

```bash
# Development with demo workflows
edda-viewer -m demo_app

# Production with PostgreSQL
export EDDA_DB_URL="postgresql+asyncpg://user:pass@localhost/workflows"
edda-viewer --port 8080

# Multiple workflow modules
edda-viewer -m workflow_module1 -m workflow_module2
```

### Method 2: Python Script

Create a custom viewer script (`my_viewer.py`):

```python
from edda import EddaApp
from edda.viewer_ui import start_viewer

# Create Edda app (for database access only)
edda_app = EddaApp(
    service_name="viewer",
    db_url="sqlite:///demo.db",
)

# Start the viewer
start_viewer(edda_app, port=8080)
```

Run the script:

```bash
uv run python my_viewer.py
```

**start_viewer() Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `edda_app` | `EddaApp` | (required) | EddaApp instance with database connection |
| `port` | `int` | `8080` | Viewer UI port |
| `reload` | `bool` | `False` | Enable auto-reload for development |

### Method 3: Direct Script Execution

Run the included `viewer_app.py` script directly:

```bash
# Basic
uv run python viewer_app.py

# With options
uv run python viewer_app.py --db my.db --port 9000 -m demo_app

# Development mode with auto-reload (using NiceGUI directly)
nicegui viewer_app.py --reload
```

## Running the Viewer with Your Application

### Step 1: Start Your Edda Application

First, start your Edda application (e.g., demo_app.py):

```bash
# Run demo application
uv run tsuno demo_app:application --bind 127.0.0.1:8001

# Or run with uvicorn
uv run uvicorn demo_app:application --port 8001
```

### Step 2: Start the Viewer UI

In a separate terminal, start the Viewer:

```bash
# Option 1: Command line (easiest)
edda-viewer -m demo_app

# Option 2: Python script
uv run python viewer_app.py -m demo_app
```

The Viewer UI will be available at http://localhost:8080

## Production Configuration

### Using Environment Variables

For production deployments, use environment variables:

```bash
# .env file
EDDA_DB_URL=postgresql+asyncpg://user:pass@db.example.com/workflows
```

Start the viewer:

```bash
edda-viewer --port 8080
```

### Custom Python Script

Create a production viewer script:

```python
import os
from edda import EddaApp
from edda.viewer_ui import start_viewer

# Create EddaApp with environment-based configuration
edda_app = EddaApp(
    service_name="viewer",
    db_url=os.getenv("EDDA_DB_URL", "sqlite:///demo.db"),
)

# Start viewer
start_viewer(
    edda_app,
    port=int(os.getenv("VIEWER_PORT", "8080"))
)
```

## Features

The Viewer UI provides:

- ✅ **Workflow List**: View all workflow instances with filtering
- ✅ **Workflow Details**: See execution history and current status
- ✅ **Hybrid Diagram**: Visual workflow graph (AST + execution history)
- ✅ **Start Workflows**: Launch workflows from the UI with auto-generated forms
- ✅ **Cancel Workflows**: Cancel running or waiting workflows
- ✅ **Filter & Search**: Find specific workflows by status, name, ID, or input parameters
- ✅ **Real-time Updates**: Auto-refresh workflow status

### Input Parameter Search

Filter workflow instances by their input data values using the Input Key and Input Value fields:

1. **Input Key**: JSON path to the field (e.g., `order_id` or `input.order_id` for nested data)
2. **Input Value**: Expected value to match (exact match)

**Example:** To find workflows with input `{"input": {"order_id": "ORD-123"}}`:

- Input Key: `input.order_id`
- Input Value: `ORD-123`

## Troubleshooting

### Viewer Dependencies Not Installed

**Error**: `Error: Viewer dependencies are not installed`

**Solution**:

```bash
# Install viewer dependencies with pip
pip install edda-framework[viewer]

# Or with uv
uv sync --extra viewer
# Or
uv add edda-framework --extra viewer
```

### Database Connection Error

**Error**: "Database not found" or "Permission denied"

**Solution**:

1. Verify database path/URL is correct
2. Check file permissions for SQLite database file
3. For PostgreSQL/MySQL, verify credentials and network access
4. Ensure the database was created by your Edda application

### Port Already in Use

**Error**: "Address already in use" on port 8080

**Solution**:

```bash
# Use a different port
edda-viewer --port 8081
```

Or kill the existing process:

```bash
# Find process using port 8080
lsof -ti:8080 | xargs kill -9
```

### Module Import Errors

**Error**: `Warning: Could not import module 'my_module'`

**Solution**:

1. Ensure the module is in your Python path:
   ```bash
   export PYTHONPATH="${PYTHONPATH}:$(pwd)"
   edda-viewer -m my_module
   ```

2. Or run from the directory containing your module:
   ```bash
   cd /path/to/your/project
   edda-viewer -m my_module
   ```

### Workflows Not Appearing

**Problem**: Viewer shows no workflows or diagrams

**Solution**:

1. Import the module containing your workflows:
   ```bash
   edda-viewer -m demo_app
   ```

2. Verify workflows are decorated with `@workflow`

3. Check that the database contains workflow instances

Once workflows appear, you'll see them with color-coded status badges:

![Status Badges Example](images/workflow-list-view.png)

*Workflow instances displayed with status badges (Completed ✅, Running ⏳, Failed ❌, Waiting ⏸️, Cancelled 🚫, etc.)*

## Next Steps

- **[Visualization Guide](visualization.md)**: Learn about workflow diagrams
- **[Examples](../examples/simple.md)**: See Viewer in action
- **[Core Concepts](../getting-started/concepts.md)**: Understanding workflows and activities
