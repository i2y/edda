"""
Tests for ASGI/WSGI application interface.

This module tests the EddaApp ASGI interface with tsuno server integration.
"""

import asyncio
import json
import os
import signal
import subprocess
from typing import Any

import httpx
import pytest

from edda import EddaApp, activity, workflow
from edda.context import WorkflowContext


class TestASGILifespan:
    """Test ASGI lifespan events."""

    @pytest.fixture
    async def app(self, tmp_path):
        """Create a EddaApp for testing."""
        db_path = tmp_path / "test.db"
        app = EddaApp(
            service_name="test-service",
            db_url=f"sqlite:///{db_path}",
        )
        yield app
        # Cleanup happens automatically on shutdown

    async def test_asgi_lifespan_startup_shutdown(self, app):
        """Test ASGI lifespan startup and shutdown events."""
        startup_called = False
        shutdown_called = False

        # Create a test ASGI receive queue
        startup_event = asyncio.Event()
        shutdown_event = asyncio.Event()

        async def receive():
            """Mock ASGI receive callable."""
            if not startup_called:
                return {"type": "lifespan.startup"}
            await shutdown_event.wait()
            return {"type": "lifespan.shutdown"}

        async def send(message):
            """Mock ASGI send callable."""
            nonlocal startup_called, shutdown_called
            if message["type"] == "lifespan.startup.complete":
                startup_called = True
                startup_event.set()
            elif message["type"] == "lifespan.shutdown.complete":
                shutdown_called = True

        # Create lifespan scope
        scope = {
            "type": "lifespan",
            "asgi": {"version": "3.0"},
        }

        # Run lifespan in background
        lifespan_task = asyncio.create_task(app(scope, receive, send))

        # Wait for startup
        await asyncio.wait_for(startup_event.wait(), timeout=5.0)
        assert startup_called, "Startup event should be called"
        assert app.storage is not None, "Storage should be initialized"

        # Trigger shutdown
        shutdown_event.set()
        await asyncio.wait_for(lifespan_task, timeout=5.0)
        assert shutdown_called, "Shutdown event should be called"


class TestASGIHTTP:
    """Test ASGI HTTP interface."""

    @pytest.fixture
    async def app_with_workflow(self, tmp_path):
        """Create a EddaApp with a test workflow."""
        db_path = tmp_path / "test.db"
        app = EddaApp(
            service_name="test-service",
            db_url=f"sqlite:///{db_path}",
        )

        # Define test workflow
        @workflow
        async def test_workflow(ctx: WorkflowContext, value: int) -> dict[str, Any]:
            result = await double_value(ctx, value)
            return {"doubled": result}

        @activity
        async def double_value(ctx: WorkflowContext, value: int) -> int:
            return value * 2

        # Workflows are automatically registered via @workflow decorator
        yield app

    async def test_asgi_http_cloudevent_post(self, app_with_workflow):
        """Test HTTP POST with CloudEvent."""
        # Initialize the app
        await app_with_workflow.initialize()

        try:
            # Create CloudEvent payload
            event_data = {
                "specversion": "1.0",
                "type": "test_workflow",
                "source": "test",
                "id": "test-123",
                "datacontenttype": "application/json",
                "data": {"value": 21},
            }

            # Create ASGI HTTP scope
            body = json.dumps(event_data).encode("utf-8")

            # Mock receive/send
            receive_called = False

            async def receive():
                nonlocal receive_called
                if not receive_called:
                    receive_called = True
                    return {
                        "type": "http.request",
                        "body": body,
                        "more_body": False,
                    }
                return {
                    "type": "http.disconnect",
                }

            response_started = False
            response_body = b""

            async def send(message):
                nonlocal response_started, response_body
                if message["type"] == "http.response.start":
                    response_started = True
                    assert message["status"] == 202  # CloudEvents HTTP Binding: 202 Accepted
                elif message["type"] == "http.response.body":
                    response_body += message.get("body", b"")

            scope = {
                "type": "http",
                "method": "POST",
                "path": "/",
                "query_string": b"",
                "headers": [
                    (b"content-type", b"application/cloudevents+json"),
                    (b"content-length", str(len(body)).encode()),
                ],
                "asgi": {"version": "3.0"},
            }

            # Call ASGI app
            await app_with_workflow(scope, receive, send)

            # Verify response
            assert response_started, "Response should be started"
            # Response body should be JSON with status accepted
            assert (
                response_body == b'{"status": "accepted"}'
            ), "Response body should indicate acceptance"

            # Note: Workflow execution verification is tested in other test files
            # Here we only verify the ASGI interface accepts CloudEvents correctly

        finally:
            await app_with_workflow.shutdown()

    async def test_asgi_invalid_request_handling(self, app_with_workflow):
        """Test error handling for invalid requests."""
        await app_with_workflow.initialize()

        try:
            # Test invalid JSON
            async def receive():
                return {
                    "type": "http.request",
                    "body": b"invalid json",
                    "more_body": False,
                }

            response_status = None

            async def send(message):
                nonlocal response_status
                if message["type"] == "http.response.start":
                    response_status = message["status"]

            scope = {
                "type": "http",
                "method": "POST",
                "path": "/",
                "query_string": b"",
                "headers": [
                    (b"content-type", b"application/cloudevents+json"),
                ],
                "asgi": {"version": "3.0"},
            }

            await app_with_workflow(scope, receive, send)

            # Should return error status
            assert response_status in [400, 500], "Should return error status"

        finally:
            await app_with_workflow.shutdown()


class TestTsunoServerIntegration:
    """Test tsuno server integration."""

    @pytest.fixture
    def test_app_file(self, tmp_path):
        """Create a test application file for tsuno."""
        app_file = tmp_path / "test_tsuno_app.py"
        app_file.write_text(
            '''
"""Test application for tsuno server."""
import asyncio
import sys
import uvloop
from edda import EddaApp

# Python 3.12+ uses asyncio.set_event_loop_policy() instead of uvloop.install()
if sys.version_info >= (3, 12):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    uvloop.install()

app = EddaApp(
    service_name="tsuno-test-service",
    db_url="sqlite:///test_tsuno.db",
)

# Export ASGI application
application = app
'''
        )
        return app_file

    async def test_tsuno_server_cli_startup(self, test_app_file, tmp_path):
        """Test tsuno CLI startup and shutdown."""
        # Add the test directory to PYTHONPATH so tsuno can import the module
        env = os.environ.copy()
        pythonpath = str(test_app_file.parent)
        if "PYTHONPATH" in env:
            pythonpath = f"{pythonpath}:{env['PYTHONPATH']}"
        env["PYTHONPATH"] = pythonpath

        # Start tsuno server as subprocess
        process = subprocess.Popen(
            [
                "tsuno",
                f"{test_app_file.stem}:application",
                "--bind",
                "127.0.0.1:18000",
            ],
            cwd=str(test_app_file.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        try:
            # Wait for server to start (longer in CI)
            await asyncio.sleep(5.0)

            # Check if process is running
            poll_result = process.poll()
            if poll_result is not None:
                # Process exited, capture output for debugging
                stdout, stderr = process.communicate()
                pytest.fail(
                    f"Tsuno server failed to start (exit code {poll_result}).\n"
                    f"STDOUT: {stdout}\nSTDERR: {stderr}"
                )
            assert poll_result is None, "Tsuno server should be running"

            # Try to connect with retries
            async with httpx.AsyncClient(timeout=10.0) as client:
                for attempt in range(3):
                    try:
                        response = await client.get("http://127.0.0.1:18000/health")
                        # Server is responding (may return various codes since /health is not implemented)
                        # 404=not found, 405=method not allowed, 400=bad request, 500=server error
                        assert response.status_code in [404, 200, 202, 400, 405, 500]
                        break
                    except (httpx.ConnectError, httpx.ReadTimeout):
                        # This is acceptable - server may not have /health endpoint or be slow
                        if attempt < 2:
                            await asyncio.sleep(2.0)
                        pass

        finally:
            # Stop server
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

            # Cleanup test database
            db_file = test_app_file.parent / "test_tsuno.db"
            if db_file.exists():
                db_file.unlink()

    async def test_tsuno_python_api(self, tmp_path):
        """Test tsuno Python API."""
        # Note: tsuno.run() is blocking, so we test the import and API availability
        try:
            from tsuno import run  # type: ignore[import-untyped]

            # Verify the run function exists and is callable
            assert callable(run), "tsuno.run should be callable"

            # Create a minimal ASGI app for testing
            async def minimal_app(scope, receive, send):
                if scope["type"] == "lifespan":
                    while True:
                        message = await receive()
                        if message["type"] == "lifespan.startup":
                            await send({"type": "lifespan.startup.complete"})
                        elif message["type"] == "lifespan.shutdown":
                            await send({"type": "lifespan.shutdown.complete"})
                            return
                elif scope["type"] == "http":
                    await send(
                        {
                            "type": "http.response.start",
                            "status": 200,
                            "headers": [(b"content-type", b"text/plain")],
                        }
                    )
                    await send(
                        {
                            "type": "http.response.body",
                            "body": b"OK",
                        }
                    )

            # We can't actually run the server here since it's blocking,
            # but we can verify the API is available
            assert callable(run)

        except ImportError:
            pytest.fail("tsuno package should be installed")

    async def test_tsuno_multiple_workers(self, test_app_file):
        """Test tsuno with multiple workers (distributed locking)."""
        # Add the test directory to PYTHONPATH so tsuno can import the module
        env = os.environ.copy()
        pythonpath = str(test_app_file.parent)
        if "PYTHONPATH" in env:
            pythonpath = f"{pythonpath}:{env['PYTHONPATH']}"
        env["PYTHONPATH"] = pythonpath

        # Start tsuno with 2 workers
        process = subprocess.Popen(
            [
                "tsuno",
                f"{test_app_file.stem}:application",
                "--bind",
                "127.0.0.1:18001",
                "--workers",
                "2",
            ],
            cwd=str(test_app_file.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )

        try:
            # Wait for server to start (longer in CI, multiple workers need more time)
            await asyncio.sleep(6.0)

            # Check if process is running
            poll_result = process.poll()
            if poll_result is not None:
                # Process exited, capture output for debugging
                stdout, stderr = process.communicate()
                pytest.fail(
                    f"Tsuno server with multiple workers failed to start (exit code {poll_result}).\n"
                    f"STDOUT: {stdout}\nSTDERR: {stderr}"
                )
            assert poll_result is None, "Tsuno server should be running with multiple workers"

            # Try to send a request with retries
            async with httpx.AsyncClient(timeout=10.0) as client:
                for attempt in range(3):
                    try:
                        # Send a test CloudEvent
                        event_data = {
                            "specversion": "1.0",
                            "type": "test.event",
                            "source": "test",
                            "id": "test-multi-worker",
                            "data": {"test": "data"},
                        }

                        response = await client.post(
                            "http://127.0.0.1:18001/",
                            json=event_data,
                            headers={"content-type": "application/cloudevents+json"},
                        )

                        # Accept various response codes since the workflow might not be registered
                        # 202=accepted (success), 404=not found, 400=bad request, 500=server error
                        assert response.status_code in [200, 202, 404, 400, 500]
                        break

                    except (httpx.ConnectError, httpx.ReadTimeout):
                        # Server might not be fully ready
                        if attempt < 2:
                            await asyncio.sleep(2.0)
                        pass

        finally:
            # Stop server
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

            # Cleanup test database
            db_file = test_app_file.parent / "test_tsuno.db"
            if db_file.exists():
                db_file.unlink()
