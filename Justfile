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
    uv sync --extra dev --extra postgresql --extra mysql

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
    uv run black edda tests

# Check code formatting
format-check:
    uv run black --check edda tests

# Lint code with ruff
lint:
    uv run ruff check edda tests

# Type check with mypy
type-check:
    uv run mypy edda

# Run all checks (format, lint, type-check, test)
check: format-check lint type-check test

# Auto-fix issues (format + lint with auto-fix)
fix:
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
demo-postgresql:
    @echo "Stopping existing demo app on port 8001..."
    @lsof -ti :8001 | xargs kill -15 2>/dev/null || true
    @sleep 1
    @lsof -ti :8001 | xargs kill -9 2>/dev/null || true
    @echo "Ensuring server and PostgreSQL dependencies are installed..."
    @uv sync --extra server --extra postgresql --quiet
    @echo "Starting demo app with PostgreSQL..."
    EDDA_DB_URL="postgresql+asyncpg://postgres:{{env_var('EDDA_POSTGRES_PASSWORD')}}@localhost:5432/edda" \
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


# Clean build artifacts and caches
clean:
    rm -rf .pytest_cache
    rm -rf htmlcov
    rm -rf .coverage
    rm -rf .mypy_cache
    rm -rf .ruff_cache
    rm -rf dist
    rm -rf build
    rm -rf *.egg-info
    find . -type d -name __pycache__ -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete
    rm -f demo.db
