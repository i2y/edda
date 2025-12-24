"""
Cross-language channel communication tests between Edda (Python) and Romancy (Go).

These tests verify that channel messages can be published from one framework
and received by the other, ensuring interoperability.

Requirements:
- Go must be installed and available in PATH
- The Romancy repository must be available at ../romancy (relative to this repo)

Test Organization:
==================

Go → Python:
  - TestGoToPython: All modes work correctly
    - Broadcast mode
    - Competing mode
    - Direct mode (SendTo)

Python → Go with Compatible Types:
  - TestPythonToGoCompatible: Works with --message-type=compatible
    - Go uses CompatibleMessage (string timestamp) instead of TestMessage (time.Time)
    - Broadcast mode
    - Direct mode

Python → Go with Strict Types:
  - TestPythonToGoStrict: Works with cross-language format handling in Romancy
    - Go's Receive[T]() handles both Go and Python message formats during replay
    - Broadcast mode
    - Complex data mode
    - Direct mode
"""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from edda import EddaApp, WorkflowContext, workflow
from edda.channels import publish, receive, send_to, subscribe
from edda.storage.migrations import apply_dbmate_migrations
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage

# Path to schema migrations
SCHEMA_DIR = Path(__file__).parent.parent / "schema" / "db" / "migrations"

# Path to Romancy repository (relative to this test file)
ROMANCY_DIR = Path(__file__).parent.parent.parent / "romancy"

# Path to Shikibu repository (relative to this test file)
SHIKIBU_DIR = Path(__file__).parent.parent.parent / "shikibu"


def is_go_available() -> bool:
    """Check if Go is available in PATH."""
    return shutil.which("go") is not None


def is_romancy_available() -> bool:
    """Check if Romancy repository is available."""
    return (ROMANCY_DIR / "cmd" / "crosstest" / "main.go").exists()


def is_ruby_available() -> bool:
    """Check if Ruby is available in PATH."""
    return shutil.which("ruby") is not None


def is_shikibu_available() -> bool:
    """Check if Shikibu crosstest is available."""
    return (SHIKIBU_DIR / "bin" / "crosstest").exists()


# Skip all tests in this module if Go or Romancy is not available
pytestmark = [
    pytest.mark.skipif(not is_go_available(), reason="Go is not installed"),
    pytest.mark.skipif(not is_romancy_available(), reason="Romancy repository not found"),
]


@pytest.fixture(scope="module")
def go_binary(tmp_path_factory) -> Path:
    """Build the Go crosstest binary once per test module."""
    build_dir = tmp_path_factory.mktemp("go_build")
    binary_path = build_dir / "crosstest"

    # Build the Go binary
    result = subprocess.run(
        ["go", "build", "-o", str(binary_path), "./cmd/crosstest"],
        cwd=ROMANCY_DIR,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        pytest.fail(f"Failed to build Go crosstest binary: {result.stderr}")

    return binary_path


@pytest_asyncio.fixture
async def cross_db(tmp_path) -> tuple[Path, SQLAlchemyStorage]:
    """Create a shared SQLite database for cross-language testing."""
    db_path = tmp_path / "cross_test.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"

    engine = create_async_engine(db_url, echo=False)

    # Apply migrations
    await apply_dbmate_migrations(engine, SCHEMA_DIR)

    storage = SQLAlchemyStorage(engine)

    # Register test workflows
    await storage.upsert_workflow_definition(
        workflow_name="crosstest_subscriber",
        source_hash="test",
        source_code="",
    )
    await storage.upsert_workflow_definition(
        workflow_name="crosstest_publisher",
        source_hash="test",
        source_code="",
    )
    await storage.upsert_workflow_definition(
        workflow_name="crosstest_send_to",
        source_hash="test",
        source_code="",
    )

    yield db_path, storage

    await storage.close()


@pytest_asyncio.fixture
async def edda_app(cross_db) -> EddaApp:
    """Create an EddaApp instance for testing."""
    db_path, _ = cross_db
    db_url = f"sqlite:///{db_path}"

    app = EddaApp(
        service_name="crosstest-service",
        db_url=db_url,
    )

    await app.initialize()
    yield app
    await app.shutdown()


def parse_go_json_output(stdout: str) -> dict[str, Any] | None:
    """Parse JSON output from Go crosstest, handling multi-line indented JSON."""
    lines = stdout.strip().split("\n")

    # Find the start of JSON (line starting with '{')
    json_start = -1
    for i, line in enumerate(lines):
        if line.strip() == "{":
            json_start = i
            break

    if json_start == -1:
        return None

    # Collect all lines from JSON start to matching closing brace
    json_lines = []
    brace_depth = 0
    for line in lines[json_start:]:
        json_lines.append(line)
        # Count braces to track nesting depth
        stripped = line.strip()
        if stripped == "{" or stripped.endswith("{"):
            brace_depth += 1
        if stripped == "}" or stripped == "},":
            brace_depth -= 1
        if brace_depth == 0:
            break

    try:
        return json.loads("\n".join(json_lines))
    except json.JSONDecodeError:
        return None


def run_go_crosstest(
    binary_path: Path,
    db_path: Path,
    mode: str,
    channel: str,
    channel_mode: str = "broadcast",
    message_type: str = "strict",
    timeout: int = 10,
    message: str | None = None,
    data: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    target_instance: str | None = None,
) -> dict[str, Any]:
    """Run the Go crosstest CLI and return the result."""
    cmd = [
        str(binary_path),
        f"--db={db_path}",
        f"--channel={channel}",
        f"--mode={mode}",
        f"--channel-mode={channel_mode}",
        f"--message-type={message_type}",
        f"--timeout={timeout}",
    ]

    if message:
        cmd.append(f"--message={message}")
    if data:
        cmd.append(f"--data={json.dumps(data)}")
    if metadata:
        cmd.append(f"--metadata={json.dumps(metadata)}")
    if target_instance:
        cmd.append(f"--target-instance={target_instance}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout + 30,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Go crosstest failed: {result.stderr}")

    # Parse JSON output (may be multi-line indented)
    go_result = parse_go_json_output(result.stdout)
    if go_result is None:
        raise RuntimeError(f"No JSON output from Go crosstest: {result.stdout}")

    return go_result


def run_ruby_crosstest(
    db_path: Path,
    mode: str,
    channel: str,
    channel_mode: str = "broadcast",
    timeout: int = 10,
    message: str | None = None,
    data: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    target_instance: str | None = None,
) -> dict[str, Any]:
    """Run the Ruby crosstest CLI and return the result."""
    cmd = [
        "bundle",
        "exec",
        "ruby",
        str(SHIKIBU_DIR / "bin" / "crosstest"),
        f"--db={db_path}",
        f"--channel={channel}",
        f"--mode={mode}",
        f"--channel-mode={channel_mode}",
        f"--timeout={timeout}",
    ]

    if message:
        cmd.append(f"--message={message}")
    if data:
        cmd.append(f"--data={json.dumps(data)}")
    if metadata:
        cmd.append(f"--metadata={json.dumps(metadata)}")
    if target_instance:
        cmd.append(f"--target-instance={target_instance}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout + 30,
        cwd=SHIKIBU_DIR,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Ruby crosstest failed: {result.stderr}")

    # Ruby outputs clean JSON to stdout (logs go to stderr)
    try:
        return json.loads(result.stdout.strip())
    except json.JSONDecodeError as err:
        raise RuntimeError(f"No JSON output from Ruby crosstest: {result.stdout}") from err


# ============================================================================
# Test workflows (Python side)
# ============================================================================


@workflow
async def py_subscriber(
    ctx: WorkflowContext,
    channel: str,
    timeout_seconds: int,
    channel_mode: str,
) -> dict[str, Any]:
    """Python subscriber workflow for cross-language testing."""
    await subscribe(ctx, channel, mode=channel_mode)

    try:
        msg = await receive(ctx, channel, timeout_seconds=timeout_seconds)
        return {
            "received": True,
            "message": msg.data if isinstance(msg.data, dict) else {"raw": str(msg.data)},
            "metadata": msg.metadata,
            "error": None,
        }
    except TimeoutError:
        return {"received": False, "message": None, "metadata": None, "error": "timeout"}


@workflow
async def py_publisher(
    ctx: WorkflowContext,
    channel: str,
    message_data: dict[str, Any],
    metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    """Python publisher workflow for cross-language testing."""
    try:
        await publish(ctx, channel, message_data, metadata=metadata)
        return {"published": True, "error": None}
    except Exception as e:
        return {"published": False, "error": str(e)}


@workflow
async def py_send_to(
    ctx: WorkflowContext,
    target_instance_id: str,
    channel: str,
    message_data: dict[str, Any],
    metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    """Python send_to workflow for direct messaging."""
    try:
        await send_to(ctx, target_instance_id, message_data, channel=channel, metadata=metadata)
        return {"sent": True, "error": None}
    except Exception as e:
        return {"sent": False, "error": str(e)}


async def wait_for_workflow(
    storage: SQLAlchemyStorage,
    instance_id: str,
    timeout: float = 30.0,
) -> dict[str, Any] | None:
    """Wait for a workflow to complete and return its result."""
    deadline = asyncio.get_event_loop().time() + timeout
    poll_interval = 0.2

    while asyncio.get_event_loop().time() < deadline:
        instance = await storage.get_instance(instance_id)
        if instance:
            if instance.get("status") == "completed":
                output = instance.get("output_data", {})
                return output.get("result")
            if instance.get("status") == "failed":
                return {"error": "workflow failed"}

        await asyncio.sleep(poll_interval)

    return None


# ============================================================================
# Go → Python Tests (WORKING)
# ============================================================================


@pytest.mark.asyncio
class TestGoToPython:
    """
    Test Go publishing and Python receiving.

    These tests verify that messages published from Go (Romancy) can be
    correctly received by Python (Edda) workflows. All modes work correctly.
    """

    async def test_broadcast_mode(self, go_binary, cross_db, edda_app):
        """Test Go publishes, Python receives (broadcast mode)."""
        db_path, storage = cross_db
        channel = "go-to-py-broadcast"

        # Start Python subscriber workflow
        sub_instance_id = await py_subscriber.start(
            channel=channel,
            timeout_seconds=15,
            channel_mode="broadcast",
        )

        # Wait for subscriber to be waiting
        await asyncio.sleep(2)

        # Go publishes a message
        go_result = run_go_crosstest(
            binary_path=go_binary,
            db_path=db_path,
            mode="publisher",
            channel=channel,
            message="Hello from Go!",
            data={"nested": {"value": 123}},
            metadata={"source": "go", "test": "go-to-py"},
        )

        assert go_result.get("published") is True

        # Wait for Python subscriber to complete
        py_result = await wait_for_workflow(storage, sub_instance_id, timeout=20)

        assert py_result is not None
        assert py_result.get("received") is True

        # Verify message content
        msg = py_result.get("message", {})
        assert msg.get("text") == "Hello from Go!"
        assert msg.get("data", {}).get("nested", {}).get("value") == 123

        # Verify metadata
        metadata = py_result.get("metadata", {})
        assert metadata.get("source") == "go"

    async def test_competing_mode(self, go_binary, cross_db, edda_app):
        """Test competing mode: message goes to one subscriber."""
        db_path, storage = cross_db
        channel = "competing-test"

        # Start Python subscriber
        py_instance_id = await py_subscriber.start(
            channel=channel,
            timeout_seconds=15,
            channel_mode="competing",
        )

        # Start Go subscriber in background
        go_proc = subprocess.Popen(
            [
                str(go_binary),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=competing",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            # Wait for both subscribers to be ready
            await asyncio.sleep(3)

            # Publish a single message from Python
            pub_instance_id = await py_publisher.start(
                channel=channel,
                message_data={"text": "Competing message", "id": 1},
                metadata=None,
            )

            pub_result = await wait_for_workflow(storage, pub_instance_id)
            assert pub_result is not None
            assert pub_result.get("published") is True

            # Wait for results
            await asyncio.sleep(5)

            # Get Python result
            py_result = await wait_for_workflow(storage, py_instance_id, timeout=10)

            # Get Go result
            go_proc.wait(timeout=20)
            stdout = go_proc.stdout.read()
            go_result = parse_go_json_output(stdout)

            # Exactly one should have received the message
            py_received = py_result and py_result.get("received") is True
            go_received = go_result and go_result.get("received") is True

            # At least one should receive (competing mode delivers to one)
            assert py_received or go_received, "No subscriber received the message"

        finally:
            go_proc.kill()
            go_proc.wait()

    async def test_direct_mode(self, go_binary, cross_db, edda_app):
        """Test Go sends direct message to Python subscriber."""
        db_path, storage = cross_db
        channel = "direct-go-to-py"

        # Start Python subscriber in direct mode
        sub_instance_id = await py_subscriber.start(
            channel=channel,
            timeout_seconds=15,
            channel_mode="direct",
        )

        # Wait for subscriber to be waiting
        await asyncio.sleep(2)

        # Go sends a direct message to the Python subscriber
        go_result = run_go_crosstest(
            binary_path=go_binary,
            db_path=db_path,
            mode="send-to",
            channel=channel,
            message="Direct message from Go!",
            data={"direct": True, "value": 999},
            metadata={"source": "go", "type": "direct"},
            target_instance=sub_instance_id,
        )

        assert go_result.get("sent") is True, f"Go send-to failed: {go_result}"

        # Wait for Python subscriber to complete
        py_result = await wait_for_workflow(storage, sub_instance_id, timeout=20)

        assert py_result is not None, "Python subscriber did not complete"
        assert py_result.get("received") is True, f"Python did not receive: {py_result}"

        # Verify message content
        msg = py_result.get("message", {})
        assert msg.get("text") == "Direct message from Go!"
        assert msg.get("data", {}).get("direct") is True
        assert msg.get("data", {}).get("value") == 999

        # Verify metadata
        metadata = py_result.get("metadata", {})
        assert metadata.get("source") == "go"
        assert metadata.get("type") == "direct"


# ============================================================================
# Python → Go Tests (Compatible Message Type)
# ============================================================================


@pytest.mark.asyncio
class TestPythonToGoCompatible:
    """
    Test Python publishing and Go receiving with compatible message types.

    Cross-language format handling is implemented in Romancy's Receive[T]() function
    to support both Go and Python message formats during replay.
    """

    async def test_broadcast_mode(self, go_binary, cross_db, edda_app):
        """Test Python publishes, Go receives (broadcast, compatible type)."""
        db_path, storage = cross_db
        channel = "py-to-go-compat-broadcast"

        # Start Go subscriber with compatible message type
        go_proc = subprocess.Popen(
            [
                str(go_binary),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=broadcast",
                "--message-type=compatible",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            await asyncio.sleep(2)

            # Python publishes a message
            instance_id = await py_publisher.start(
                channel=channel,
                message_data={
                    "text": "Hello from Python!",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "data": {"nested": {"value": 42}},
                },
                metadata={"source": "python", "test": "py-to-go-compat"},
            )

            result = await wait_for_workflow(storage, instance_id)
            assert result is not None
            assert result.get("published") is True

            stdout, stderr = go_proc.communicate(timeout=20)

            go_result = parse_go_json_output(stdout)
            assert go_result is not None, f"No JSON output from Go: {stdout}"
            assert go_result.get("received") is True, f"Go did not receive: {go_result}"

            # Verify message content
            msg = go_result.get("message", {})
            assert msg.get("text") == "Hello from Python!"
            assert msg.get("data", {}).get("nested", {}).get("value") == 42

        finally:
            go_proc.kill()
            go_proc.wait()

    async def test_direct_mode(self, go_binary, cross_db, edda_app):
        """Test Python sends direct message to Go (compatible type)."""
        db_path, storage = cross_db
        channel = "py-to-go-compat-direct"

        # Start Go subscriber in direct mode with compatible message type
        go_proc = subprocess.Popen(
            [
                str(go_binary),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=direct",
                "--message-type=compatible",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            await asyncio.sleep(2)

            # Find Go subscriber's instance ID (direct SQL since list_instances filters by Python)
            from sqlalchemy import text

            async with storage.engine.begin() as conn:
                result = await conn.execute(
                    text(
                        """
                    SELECT instance_id FROM workflow_instances
                    WHERE workflow_name = 'crosstest_subscriber'
                    AND status = 'waiting_for_message'
                    AND framework = 'go'
                    ORDER BY started_at DESC
                    LIMIT 1
                """
                    )
                )
                row = result.fetchone()
            assert row is not None, "No Go subscriber instance found"
            go_instance_id = row[0]

            # Python sends direct message
            send_instance_id = await py_send_to.start(
                target_instance_id=go_instance_id,
                channel=channel,
                message_data={
                    "text": "Direct from Python!",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "data": {"direct": True},
                },
                metadata={"source": "python"},
            )

            send_result = await wait_for_workflow(storage, send_instance_id)
            assert send_result is not None
            assert send_result.get("sent") is True

            stdout, stderr = go_proc.communicate(timeout=20)

            go_result = parse_go_json_output(stdout)
            assert go_result is not None, f"No JSON output from Go: {stdout}"
            assert go_result.get("received") is True, f"Go did not receive: {go_result}"

            msg = go_result.get("message", {})
            assert msg.get("text") == "Direct from Python!"

        finally:
            go_proc.kill()
            go_proc.wait()


# ============================================================================
# Python → Go Tests (Strict Message Type - XFAIL)
# ============================================================================


@pytest.mark.asyncio
class TestPythonToGoStrict:
    """
    Test Python publishing and Go receiving with strict Go types.

    Cross-language format handling in Romancy's Receive[T]() supports parsing
    Python's ISO string timestamps into Go's time.Time during replay.
    """

    async def test_broadcast_mode(self, go_binary, cross_db, edda_app):
        """Test Python publishes, Go receives (broadcast mode)."""
        db_path, storage = cross_db
        channel = "py-to-go-broadcast"

        # Start Go subscriber in background
        go_proc = subprocess.Popen(
            [
                str(go_binary),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=broadcast",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            # Wait for Go subscriber to start
            await asyncio.sleep(2)

            # Python publishes a message
            instance_id = await py_publisher.start(
                channel=channel,
                message_data={
                    "text": "Hello from Python!",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "data": {"nested": {"value": 42}},
                },
                metadata={"source": "python", "test": "py-to-go"},
            )

            # Wait for publisher to complete
            result = await wait_for_workflow(storage, instance_id)
            assert result is not None
            assert result.get("published") is True

            # Wait for Go subscriber to complete
            stdout, stderr = go_proc.communicate(timeout=20)

            # Parse Go result
            go_result = parse_go_json_output(stdout)
            assert go_result is not None, f"No JSON output from Go: {stdout}"
            assert go_result.get("received") is True
            assert go_result.get("message") is not None

            # Verify message content
            msg = go_result["message"]
            assert msg.get("text") == "Hello from Python!"
            assert msg.get("data", {}).get("nested", {}).get("value") == 42

            # Verify metadata
            metadata = go_result.get("metadata", {})
            assert metadata.get("source") == "python"

        finally:
            go_proc.kill()
            go_proc.wait()

    async def test_complex_data(self, go_binary, cross_db, edda_app):
        """Test deeply nested JSON data from Python to Go."""
        db_path, storage = cross_db
        channel = "complex-data"

        complex_data = {
            "text": "Complex message",
            "timestamp": "2025-01-01T12:00:00Z",
            "data": {
                "level1": {
                    "level2": {
                        "level3": {
                            "value": 42,
                            "list": [1, 2, 3],
                            "nested_list": [{"a": 1}, {"b": 2}],
                        }
                    }
                },
                "array": ["x", "y", "z"],
                "boolean": True,
                "null_value": None,
            },
        }

        complex_metadata = {
            "source_instance_id": "test-instance-123",
            "custom_field": "custom_value",
            "numeric": 999,
        }

        # Start Go subscriber
        go_proc = subprocess.Popen(
            [
                str(go_binary),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=broadcast",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            await asyncio.sleep(2)

            # Python publishes complex data
            instance_id = await py_publisher.start(
                channel=channel,
                message_data=complex_data,
                metadata=complex_metadata,
            )

            result = await wait_for_workflow(storage, instance_id)
            assert result is not None
            assert result.get("published") is True

            # Get Go result
            stdout, _ = go_proc.communicate(timeout=20)
            go_result = parse_go_json_output(stdout)
            assert go_result is not None, f"No JSON output from Go: {stdout}"
            assert go_result.get("received") is True

            # Verify nested data is preserved
            msg = go_result.get("message", {})
            assert (
                msg.get("data", {})
                .get("level1", {})
                .get("level2", {})
                .get("level3", {})
                .get("value")
                == 42
            )
            assert msg.get("data", {}).get("array") == ["x", "y", "z"]
            assert msg.get("data", {}).get("boolean") is True

            # Verify metadata is preserved
            metadata = go_result.get("metadata", {})
            assert metadata.get("source_instance_id") == "test-instance-123"
            assert metadata.get("custom_field") == "custom_value"

        finally:
            go_proc.kill()
            go_proc.wait()

    async def test_direct_mode(self, go_binary, cross_db, edda_app):
        """Test Python sends direct message to Go subscriber."""
        db_path, storage = cross_db
        channel = "direct-py-to-go"

        # Start Go subscriber in direct mode (in background)
        go_proc = subprocess.Popen(
            [
                str(go_binary),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=direct",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            # Wait for Go subscriber to start and get its instance ID
            await asyncio.sleep(2)

            # We need to find the Go subscriber's instance ID from the database
            # The Go workflow should be running and waiting for a message
            # Note: list_instances() filters by framework="python", so we use direct SQL
            async with storage.engine.begin() as conn:
                result = await conn.execute(
                    text(
                        """
                    SELECT instance_id FROM workflow_instances
                    WHERE workflow_name = 'crosstest_subscriber'
                    AND status = 'waiting_for_message'
                    AND framework = 'go'
                    ORDER BY started_at DESC
                    LIMIT 1
                """
                    )
                )
                row = result.fetchone()

            assert row is not None, "No Go subscriber instance found"
            go_instance_id = row[0]

            # Python sends a direct message to the Go subscriber
            send_instance_id = await py_send_to.start(
                target_instance_id=go_instance_id,
                channel=channel,
                message_data={
                    "text": "Direct message from Python!",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "data": {"direct": True, "value": 888},
                },
                metadata={"source": "python", "type": "direct"},
            )

            # Wait for send_to to complete
            send_result = await wait_for_workflow(storage, send_instance_id)
            assert send_result is not None
            assert send_result.get("sent") is True, f"Python send_to failed: {send_result}"

            # Wait for Go subscriber to complete
            stdout, stderr = go_proc.communicate(timeout=20)

            # Parse Go result
            go_result = parse_go_json_output(stdout)
            assert go_result is not None, f"No JSON output from Go: {stdout}"
            assert go_result.get("received") is True, f"Go did not receive: {go_result}"

            # Verify message content
            msg = go_result["message"]
            assert msg.get("text") == "Direct message from Python!"
            assert msg.get("data", {}).get("direct") is True
            assert msg.get("data", {}).get("value") == 888

            # Verify metadata
            metadata = go_result.get("metadata", {})
            assert metadata.get("source") == "python"
            assert metadata.get("type") == "direct"

        finally:
            go_proc.kill()
            go_proc.wait()


# ============================================================================
# Ruby → Python Tests
# ============================================================================


# Skip conditions for Ruby tests
ruby_skip_conditions = [
    pytest.mark.skipif(not is_ruby_available(), reason="Ruby is not installed"),
    pytest.mark.skipif(not is_shikibu_available(), reason="Shikibu crosstest not found"),
]


@pytest.mark.asyncio
class TestRubyToPython:
    """
    Test Ruby (Shikibu) publishing and Python (Edda) receiving.

    These tests verify that messages published from Ruby can be
    correctly received by Python workflows.
    """

    pytestmark = ruby_skip_conditions

    async def test_broadcast_mode(self, cross_db, edda_app):
        """Test Ruby publishes, Python receives (broadcast mode)."""
        db_path, storage = cross_db
        channel = "ruby-to-py-broadcast"

        # Start Python subscriber workflow
        sub_instance_id = await py_subscriber.start(
            channel=channel,
            timeout_seconds=15,
            channel_mode="broadcast",
        )

        # Wait for subscriber to be waiting
        await asyncio.sleep(2)

        # Ruby publishes a message
        ruby_result = run_ruby_crosstest(
            db_path=db_path,
            mode="publisher",
            channel=channel,
            message="Hello from Ruby!",
            data={"nested": {"value": 456}},
            metadata={"source": "ruby", "test": "ruby-to-py"},
        )

        assert ruby_result.get("published") is True

        # Wait for Python subscriber to complete
        py_result = await wait_for_workflow(storage, sub_instance_id, timeout=20)

        assert py_result is not None
        assert py_result.get("received") is True

        # Verify message content
        msg = py_result.get("message", {})
        assert msg.get("text") == "Hello from Ruby!"
        assert msg.get("data", {}).get("nested", {}).get("value") == 456

        # Verify metadata
        metadata = py_result.get("metadata", {})
        assert metadata.get("source") == "ruby"

    async def test_competing_mode(self, cross_db, edda_app):
        """Test competing mode with Ruby publisher."""
        db_path, storage = cross_db
        channel = "ruby-to-py-competing"

        # Start Python subscriber
        py_instance_id = await py_subscriber.start(
            channel=channel,
            timeout_seconds=15,
            channel_mode="competing",
        )

        # Start another Python subscriber
        py_instance_id_2 = await py_subscriber.start(
            channel=channel,
            timeout_seconds=15,
            channel_mode="competing",
        )

        # Wait for subscribers to be ready
        await asyncio.sleep(2)

        # Ruby publishes a single message
        ruby_result = run_ruby_crosstest(
            db_path=db_path,
            mode="publisher",
            channel=channel,
            message="Competing message from Ruby",
            data={"id": 1},
        )

        assert ruby_result.get("published") is True

        # Wait for results
        await asyncio.sleep(3)

        # Get both Python results
        py_result_1 = await wait_for_workflow(storage, py_instance_id, timeout=10)
        py_result_2 = await wait_for_workflow(storage, py_instance_id_2, timeout=10)

        # At least one should receive (competing mode delivers to one)
        py1_received = py_result_1 and py_result_1.get("received") is True
        py2_received = py_result_2 and py_result_2.get("received") is True

        assert py1_received or py2_received, "No subscriber received the message"

    async def test_direct_mode(self, cross_db, edda_app):
        """Test Ruby sends direct message to Python subscriber."""
        db_path, storage = cross_db
        channel = "direct-ruby-to-py"

        # Start Python subscriber in direct mode
        sub_instance_id = await py_subscriber.start(
            channel=channel,
            timeout_seconds=15,
            channel_mode="direct",
        )

        # Wait for subscriber to be waiting
        await asyncio.sleep(2)

        # Ruby sends a direct message to the Python subscriber
        ruby_result = run_ruby_crosstest(
            db_path=db_path,
            mode="send-to",
            channel=channel,
            message="Direct message from Ruby!",
            data={"direct": True, "value": 777},
            metadata={"source": "ruby", "type": "direct"},
            target_instance=sub_instance_id,
        )

        assert ruby_result.get("sent") is True, f"Ruby send-to failed: {ruby_result}"

        # Wait for Python subscriber to complete
        py_result = await wait_for_workflow(storage, sub_instance_id, timeout=20)

        assert py_result is not None, "Python subscriber did not complete"
        assert py_result.get("received") is True, f"Python did not receive: {py_result}"

        # Verify message content
        msg = py_result.get("message", {})
        assert msg.get("text") == "Direct message from Ruby!"
        assert msg.get("data", {}).get("direct") is True
        assert msg.get("data", {}).get("value") == 777

        # Verify metadata
        metadata = py_result.get("metadata", {})
        assert metadata.get("source") == "ruby"
        assert metadata.get("type") == "direct"


# ============================================================================
# Python → Ruby Tests
# ============================================================================


@pytest.mark.asyncio
class TestPythonToRuby:
    """
    Test Python (Edda) publishing and Ruby (Shikibu) receiving.
    """

    pytestmark = ruby_skip_conditions

    async def test_broadcast_mode(self, cross_db, edda_app):
        """Test Python publishes, Ruby receives (broadcast mode)."""
        db_path, storage = cross_db
        channel = "py-to-ruby-broadcast"

        # Start Ruby subscriber in background
        ruby_proc = subprocess.Popen(
            [
                "bundle",
                "exec",
                "ruby",
                str(SHIKIBU_DIR / "bin" / "crosstest"),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=broadcast",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=SHIKIBU_DIR,
        )

        try:
            await asyncio.sleep(2)

            # Python publishes
            instance_id = await py_publisher.start(
                channel=channel,
                message_data={
                    "text": "Hello from Python to Ruby!",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "data": {"nested": {"value": 789}},
                },
                metadata={"source": "python", "test": "py-to-ruby"},
            )

            result = await wait_for_workflow(storage, instance_id)
            assert result is not None
            assert result.get("published") is True

            stdout, stderr = ruby_proc.communicate(timeout=20)
            ruby_result = json.loads(stdout.strip())

            assert ruby_result.get("received") is True, f"Ruby did not receive: {ruby_result}"

            msg = ruby_result.get("message", {})
            assert msg.get("text") == "Hello from Python to Ruby!"
            assert msg.get("data", {}).get("nested", {}).get("value") == 789

        finally:
            ruby_proc.kill()
            ruby_proc.wait()

    async def test_direct_mode(self, cross_db, edda_app):
        """Test Python sends direct message to Ruby subscriber."""
        db_path, storage = cross_db
        channel = "py-to-ruby-direct"

        # Start Ruby subscriber in direct mode
        ruby_proc = subprocess.Popen(
            [
                "bundle",
                "exec",
                "ruby",
                str(SHIKIBU_DIR / "bin" / "crosstest"),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=direct",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=SHIKIBU_DIR,
        )

        try:
            await asyncio.sleep(2)

            # Find Ruby subscriber's instance ID
            async with storage.engine.begin() as conn:
                result = await conn.execute(
                    text(
                        """
                    SELECT instance_id FROM workflow_instances
                    WHERE workflow_name = 'crosstest_subscriber'
                    AND status = 'waiting_for_message'
                    AND framework = 'ruby'
                    ORDER BY started_at DESC
                    LIMIT 1
                """
                    )
                )
                row = result.fetchone()
            assert row is not None, "No Ruby subscriber instance found"
            ruby_instance_id = row[0]

            # Python sends direct message
            send_instance_id = await py_send_to.start(
                target_instance_id=ruby_instance_id,
                channel=channel,
                message_data={
                    "text": "Direct from Python to Ruby!",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "data": {"direct": True},
                },
                metadata={"source": "python"},
            )

            send_result = await wait_for_workflow(storage, send_instance_id)
            assert send_result is not None
            assert send_result.get("sent") is True

            stdout, stderr = ruby_proc.communicate(timeout=20)
            ruby_result = json.loads(stdout.strip())

            assert ruby_result.get("received") is True, f"Ruby did not receive: {ruby_result}"

            msg = ruby_result.get("message", {})
            assert msg.get("text") == "Direct from Python to Ruby!"

        finally:
            ruby_proc.kill()
            ruby_proc.wait()


# ============================================================================
# Ruby → Go Tests
# ============================================================================


@pytest.mark.asyncio
class TestRubyToGo:
    """
    Test Ruby (Shikibu) publishing and Go (Romancy) receiving.
    """

    pytestmark = ruby_skip_conditions

    async def test_broadcast_mode(self, go_binary, cross_db, edda_app):
        """Test Ruby publishes, Go receives (broadcast mode)."""
        db_path, storage = cross_db
        channel = "ruby-to-go-broadcast"

        # Start Go subscriber with compatible message type
        go_proc = subprocess.Popen(
            [
                str(go_binary),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=broadcast",
                "--message-type=compatible",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            await asyncio.sleep(2)

            # Ruby publishes
            ruby_result = run_ruby_crosstest(
                db_path=db_path,
                mode="publisher",
                channel=channel,
                message="Hello from Ruby to Go!",
                data={"cross": True, "value": 111},
                metadata={"source": "ruby"},
            )

            assert ruby_result.get("published") is True

            stdout, stderr = go_proc.communicate(timeout=20)
            go_result = parse_go_json_output(stdout)

            assert go_result is not None, f"No JSON output from Go: {stdout}"
            assert go_result.get("received") is True, f"Go did not receive: {go_result}"

            msg = go_result.get("message", {})
            assert msg.get("text") == "Hello from Ruby to Go!"
            assert msg.get("data", {}).get("cross") is True
            assert msg.get("data", {}).get("value") == 111

        finally:
            go_proc.kill()
            go_proc.wait()

    async def test_direct_mode(self, go_binary, cross_db, edda_app):
        """Test Ruby sends direct message to Go subscriber."""
        db_path, storage = cross_db
        channel = "ruby-to-go-direct"

        # Start Go subscriber in direct mode
        go_proc = subprocess.Popen(
            [
                str(go_binary),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=direct",
                "--message-type=compatible",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            await asyncio.sleep(2)

            # Find Go subscriber's instance ID
            async with storage.engine.begin() as conn:
                result = await conn.execute(
                    text(
                        """
                    SELECT instance_id FROM workflow_instances
                    WHERE workflow_name = 'crosstest_subscriber'
                    AND status = 'waiting_for_message'
                    AND framework = 'go'
                    ORDER BY started_at DESC
                    LIMIT 1
                """
                    )
                )
                row = result.fetchone()
            assert row is not None, "No Go subscriber instance found"
            go_instance_id = row[0]

            # Ruby sends direct message
            ruby_result = run_ruby_crosstest(
                db_path=db_path,
                mode="send-to",
                channel=channel,
                message="Direct from Ruby to Go!",
                data={"direct": True},
                metadata={"source": "ruby"},
                target_instance=go_instance_id,
            )

            assert ruby_result.get("sent") is True

            stdout, stderr = go_proc.communicate(timeout=20)
            go_result = parse_go_json_output(stdout)

            assert go_result is not None, f"No JSON output from Go: {stdout}"
            assert go_result.get("received") is True, f"Go did not receive: {go_result}"

            msg = go_result.get("message", {})
            assert msg.get("text") == "Direct from Ruby to Go!"

        finally:
            go_proc.kill()
            go_proc.wait()


# ============================================================================
# Go → Ruby Tests
# ============================================================================


@pytest.mark.asyncio
class TestGoToRuby:
    """
    Test Go (Romancy) publishing and Ruby (Shikibu) receiving.
    """

    pytestmark = ruby_skip_conditions

    async def test_broadcast_mode(self, go_binary, cross_db, edda_app):
        """Test Go publishes, Ruby receives (broadcast mode)."""
        db_path, storage = cross_db
        channel = "go-to-ruby-broadcast"

        # Start Ruby subscriber in background
        ruby_proc = subprocess.Popen(
            [
                "bundle",
                "exec",
                "ruby",
                str(SHIKIBU_DIR / "bin" / "crosstest"),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=broadcast",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=SHIKIBU_DIR,
        )

        try:
            await asyncio.sleep(2)

            # Go publishes
            go_result = run_go_crosstest(
                binary_path=go_binary,
                db_path=db_path,
                mode="publisher",
                channel=channel,
                message="Hello from Go to Ruby!",
                data={"cross": True, "value": 222},
                metadata={"source": "go"},
            )

            assert go_result.get("published") is True

            stdout, stderr = ruby_proc.communicate(timeout=20)
            ruby_result = json.loads(stdout.strip())

            assert ruby_result.get("received") is True, f"Ruby did not receive: {ruby_result}"

            msg = ruby_result.get("message", {})
            assert msg.get("text") == "Hello from Go to Ruby!"
            assert msg.get("data", {}).get("cross") is True
            assert msg.get("data", {}).get("value") == 222

        finally:
            ruby_proc.kill()
            ruby_proc.wait()

    async def test_direct_mode(self, go_binary, cross_db, edda_app):
        """Test Go sends direct message to Ruby subscriber."""
        db_path, storage = cross_db
        channel = "go-to-ruby-direct"

        # Start Ruby subscriber in direct mode
        ruby_proc = subprocess.Popen(
            [
                "bundle",
                "exec",
                "ruby",
                str(SHIKIBU_DIR / "bin" / "crosstest"),
                f"--db={db_path}",
                f"--channel={channel}",
                "--mode=subscriber",
                "--channel-mode=direct",
                "--timeout=15",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=SHIKIBU_DIR,
        )

        try:
            await asyncio.sleep(2)

            # Find Ruby subscriber's instance ID
            async with storage.engine.begin() as conn:
                result = await conn.execute(
                    text(
                        """
                    SELECT instance_id FROM workflow_instances
                    WHERE workflow_name = 'crosstest_subscriber'
                    AND status = 'waiting_for_message'
                    AND framework = 'ruby'
                    ORDER BY started_at DESC
                    LIMIT 1
                """
                    )
                )
                row = result.fetchone()
            assert row is not None, "No Ruby subscriber instance found"
            ruby_instance_id = row[0]

            # Go sends direct message
            go_result = run_go_crosstest(
                binary_path=go_binary,
                db_path=db_path,
                mode="send-to",
                channel=channel,
                message="Direct from Go to Ruby!",
                data={"direct": True, "value": 333},
                metadata={"source": "go"},
                target_instance=ruby_instance_id,
            )

            assert go_result.get("sent") is True

            stdout, stderr = ruby_proc.communicate(timeout=20)
            ruby_result = json.loads(stdout.strip())

            assert ruby_result.get("received") is True, f"Ruby did not receive: {ruby_result}"

            msg = ruby_result.get("message", {})
            assert msg.get("text") == "Direct from Go to Ruby!"
            assert msg.get("data", {}).get("direct") is True
            assert msg.get("data", {}).get("value") == 333

        finally:
            ruby_proc.kill()
            ruby_proc.wait()
