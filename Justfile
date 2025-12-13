# Edda - Development Commands
# Run `just` to see available commands

# Default recipe - show available commands
default:
    @just --list

# Install development dependencies
install:
    uv sync --extra dev

# Install all dependencies including database drivers
install-all:
    uv sync --extra dev --extra postgresql --extra mysql --extra postgres-notify

# Install viewer dependencies
install-viewer:
    uv sync --extra viewer

# Run tests with coverage
test:
    uv run python -m pytest

# Run tests without coverage (faster)
test-fast:
    uv run python -m pytest --no-cov

# Run specific test file
test-file FILE:
    uv run python -m pytest {{FILE}} --no-cov -v

# Format code with black
format:
    @uv sync --extra dev --quiet
    uv run black edda tests

# Check code formatting
format-check:
    @uv sync --extra dev --quiet
    uv run black --check edda tests

# Lint code with ruff
lint:
    @uv sync --extra dev --quiet
    uv run ruff check edda tests

# Type check with mypy
type-check:
    @uv sync --extra dev --quiet
    uv run mypy edda

# Run all checks (format, lint, type-check, test)
check: format-check lint type-check test

# Auto-fix issues (format + lint with auto-fix)
fix:
    @uv sync --extra dev --quiet
    uv run black edda tests
    uv run ruff check --fix edda tests

# Run demo application (installs dependencies if needed)
demo:
    @echo "Stopping existing demo app on port 8001..."
    @lsof -ti :8001 | xargs kill -15 2>/dev/null || true
    @sleep 1
    @lsof -ti :8001 | xargs kill -9 2>/dev/null || true
    @echo "Ensuring server dependencies are installed..."
    @uv sync --extra server --quiet
    uv run tsuno demo_app:application --bind 127.0.0.1:8001

# Run demo application with PostgreSQL (requires local PostgreSQL)
# Usage: just demo-postgresql
# Requires: EDDA_POSTGRES_PASSWORD environment variable
# Uses LISTEN/NOTIFY for instant notifications (auto-detected)
demo-postgresql:
    @echo "Stopping existing demo app on port 8001..."
    @lsof -ti :8001 | xargs kill -15 2>/dev/null || true
    @sleep 1
    @lsof -ti :8001 | xargs kill -9 2>/dev/null || true
    @echo "Ensuring server and PostgreSQL dependencies are installed..."
    @uv sync --extra server --extra postgresql --extra postgres-notify --quiet
    @echo "Starting demo app with PostgreSQL (NOTIFY enabled)..."
    EDDA_DB_URL="postgresql+asyncpg://postgres:{{env_var('EDDA_POSTGRES_PASSWORD')}}@localhost:5432/edda" \
    uv run tsuno demo_app:application --bind 127.0.0.1:8001

# Run demo application with PostgreSQL but WITHOUT NOTIFY (polling only)
# Usage: just demo-postgresql-polling
# Requires: EDDA_POSTGRES_PASSWORD environment variable
# Uses polling-only mode (no LISTEN/NOTIFY) for comparison testing
demo-postgresql-polling:
    @echo "Stopping existing demo app on port 8001..."
    @lsof -ti :8001 | xargs kill -15 2>/dev/null || true
    @sleep 1
    @lsof -ti :8001 | xargs kill -9 2>/dev/null || true
    @echo "Ensuring server and PostgreSQL dependencies are installed..."
    @uv sync --extra server --extra postgresql --quiet
    @echo "Starting demo app with PostgreSQL (polling only, NOTIFY disabled)..."
    EDDA_DB_URL="postgresql+asyncpg://postgres:{{env_var('EDDA_POSTGRES_PASSWORD')}}@localhost:5432/edda" \
    EDDA_USE_NOTIFY=false \
    uv run tsuno demo_app:application --bind 127.0.0.1:8001

# Run viewer with demo_app using PostgreSQL (requires local PostgreSQL)
# Usage: just demo-postgresql-viewer [PORT]
# Requires: EDDA_POSTGRES_PASSWORD environment variable
demo-postgresql-viewer PORT='8080':
    @echo "Stopping existing viewer on port {{PORT}}..."
    @lsof -ti :{{PORT}} | xargs kill -15 2>/dev/null || true
    @sleep 1
    @lsof -ti :{{PORT}} | xargs kill -9 2>/dev/null || true
    @echo "Ensuring viewer and PostgreSQL dependencies are installed..."
    @uv sync --extra viewer --extra postgresql --quiet
    @echo "Starting viewer with PostgreSQL and demo_app..."
    EDDA_DB_URL="postgresql+asyncpg://postgres:{{env_var('EDDA_POSTGRES_PASSWORD')}}@localhost:5432/edda" \
    uv run edda-viewer --port {{PORT}} --import-module demo_app

# Run viewer application (installs dependencies if needed)
# Usage: just viewer [DB] [PORT] [MODULES]
# Examples:
#   just viewer                                  # No modules, demo.db, port 8080
#   just viewer my.db                            # Custom DB, no modules
#   just viewer demo.db 8080 "-m demo_app"       # With demo_app module
#   just viewer demo.db 8080 "-m demo_app -m my_app"  # Multiple modules
viewer DB='demo.db' PORT='8080' MODULES='':
    @echo "Stopping existing viewer on port {{PORT}}..."
    @lsof -ti :{{PORT}} | xargs kill -15 2>/dev/null || true
    @sleep 1
    @lsof -ti :{{PORT}} | xargs kill -9 2>/dev/null || true
    @echo "Ensuring viewer dependencies are installed..."
    @uv sync --extra viewer --quiet
    uv run edda-viewer --db {{DB}} --port {{PORT}} {{MODULES}}

# Run viewer with demo_app (shortcut)
# Usage: just viewer-demo [DB] [PORT]
# Examples:
#   just viewer-demo              # demo.db, port 8080, with demo_app
#   just viewer-demo my.db 9000   # Custom DB and port, with demo_app
viewer-demo DB='demo.db' PORT='8080':
    just viewer {{DB}} {{PORT}} "--import-module demo_app"


# Build documentation (clears cache for fresh API reference)
docs:
    rm -rf .cache site
    uv run zensical build

# Serve documentation locally (clears cache for fresh API reference)
docs-serve:
    @lsof -ti :8000 | xargs kill -15 2>/dev/null || true
    @sleep 1
    rm -rf .cache site
    uv run zensical serve

# Clean build artifacts and caches
clean:
    rm -rf .pytest_cache
    rm -rf htmlcov
    rm -rf .coverage
    rm -rf .mypy_cache
    rm -rf .ruff_cache
    rm -rf .cache
    rm -rf site
    rm -rf dist
    rm -rf build
    rm -rf *.egg-info
    find . -type d -name __pycache__ -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete
    rm -f demo.db

# Helper recipe for point-to-point test (uses bash for command substitution)
_test-point-to-point LOG_FILE:
    #!/usr/bin/env bash
    set -e
    echo "=== Test 3: Point-to-Point Mode (Direct Message) ==="
    echo "Starting receiver..."
    curl -s -X POST http://localhost:8001/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{"specversion":"1.0","type":"direct_message_receiver_workflow","source":"test","id":"receiver-1","data":{"receiver_id":"receiver-1"}}' > /dev/null
    sleep 2
    # Parse instance_id from server log (format: [RECEIVER] Instance ID: workflow-uuid)
    # Strip ANSI color codes before grepping
    INSTANCE_ID=$(sed 's/\x1b\[[0-9;]*m//g' "{{LOG_FILE}}" | grep -o '\[RECEIVER\] Instance ID: [^ ]*' | tail -1 | cut -d' ' -f4)
    echo "Receiver instance_id: $INSTANCE_ID"
    if [ -z "$INSTANCE_ID" ]; then
        echo "ERROR: Could not extract instance_id from server log"
        echo "Log content (stripped):"
        sed 's/\x1b\[[0-9;]*m//g' "{{LOG_FILE}}" | grep -i receiver || echo "(no receiver logs found)"
        exit 1
    fi
    echo "Sending direct message to receiver..."
    curl -s -X POST http://localhost:8001/ \
        -H "Content-Type: application/cloudevents+json" \
        -d "{\"specversion\":\"1.0\",\"type\":\"direct_message_sender_workflow\",\"source\":\"test\",\"id\":\"sender-1\",\"data\":{\"target_instance_id\":\"$INSTANCE_ID\",\"message\":\"Hello from sender!\"}}"
    echo ""
    sleep 5
    # Verify receiver got the message (strip ANSI codes)
    if sed 's/\x1b\[[0-9;]*m//g' "{{LOG_FILE}}" | grep -q "\[RECEIVER\] Received message:"; then
        echo "✓ Point-to-Point message delivered successfully!"
    else
        echo "✗ Point-to-Point message delivery failed"
    fi

# Test message passing (competing, broadcast, and point-to-point modes)
test-messages:
    #!/usr/bin/env bash
    set -e
    LOG_FILE=$(mktemp)
    echo "=== Message Passing Test ==="
    echo "Server log: $LOG_FILE"
    echo "Starting demo app in background..."
    rm -f demo.db
    uv sync --extra server --quiet
    PYTHONUNBUFFERED=1 uv run tsuno demo_app:application --bind 127.0.0.1:8001 --workers 1 > "$LOG_FILE" 2>&1 &
    SERVER_PID=$!
    sleep 2

    echo ""
    echo "=== Test 1: Competing Mode (Job Worker) ==="
    echo "Starting worker..."
    curl -s -X POST http://localhost:8001/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{"specversion":"1.0","type":"job_worker_workflow","source":"test","id":"worker-1","data":{"worker_id":"worker-1"}}'
    echo ""
    sleep 1
    echo "Publishing job..."
    curl -s -X POST http://localhost:8001/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{"specversion":"1.0","type":"job_publisher_workflow","source":"test","id":"job-1","data":{"task":"test-task"}}'
    echo ""
    sleep 2

    echo ""
    echo "=== Test 2: Broadcast Mode (Notification) ==="
    echo "Starting notification service..."
    curl -s -X POST http://localhost:8001/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{"specversion":"1.0","type":"notification_service_workflow","source":"test","id":"service-1","data":{"service_id":"service-1"}}'
    echo ""
    sleep 1
    echo "Publishing notification..."
    curl -s -X POST http://localhost:8001/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{"specversion":"1.0","type":"notification_publisher_workflow","source":"test","id":"notification-1","data":{"message":"Test notification"}}'
    echo ""
    sleep 2

    just _test-point-to-point "$LOG_FILE"

    echo ""
    echo "=== Stopping demo app ==="
    kill -15 $SERVER_PID 2>/dev/null || true
    sleep 1

    echo ""
    echo "=== Server Log ==="
    cat "$LOG_FILE"
    rm -f "$LOG_FILE"
    echo ""
    echo "Done!"
