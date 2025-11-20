"""
Tests for CloudEvents HTTP Binding compliance.

This module tests that Edda properly implements the CloudEvents HTTP Binding
specification, particularly regarding HTTP status codes and error responses.

CloudEvents HTTP Binding specification:
- Success (async processing): 202 Accepted
- Client errors (non-retryable): 400 Bad Request, 415 Unsupported Media Type
- Server errors (retryable): 500 Internal Server Error, 503 Service Unavailable
"""

import json
from typing import Any

import pytest

from edda import EddaApp, WorkflowContext, activity, workflow


class TestCloudEventsHTTPBinding:
    """Test CloudEvents HTTP Binding compliance."""

    @pytest.fixture
    async def app_with_workflow(self):
        """Create EddaApp with test workflow."""
        app = EddaApp(
            service_name="test-service",
            db_url="sqlite+aiosqlite:///:memory:",
        )

        # Define test workflow
        @workflow(event_handler=True)
        async def test_workflow(ctx: WorkflowContext, value: int) -> dict[str, Any]:
            result = await double_value(ctx, value)
            return {"doubled": result}

        @activity
        async def double_value(ctx: WorkflowContext, value: int) -> int:
            return value * 2

        yield app

    async def test_success_returns_202_accepted(self, app_with_workflow):
        """Test that successful CloudEvent processing returns 202 Accepted."""
        await app_with_workflow.initialize()

        try:
            # Create valid CloudEvent payload
            event_data = {
                "specversion": "1.0",
                "type": "test_workflow",
                "source": "test",
                "id": "test-202",
                "datacontenttype": "application/json",
                "data": {"value": 21},
            }

            body = json.dumps(event_data).encode("utf-8")

            # Mock ASGI receive/send
            async def receive():
                return {
                    "type": "http.request",
                    "body": body,
                    "more_body": False,
                }

            response_status = None
            response_body = b""

            async def send(message):
                nonlocal response_status, response_body
                if message["type"] == "http.response.start":
                    response_status = message["status"]
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
            }

            # Call ASGI app
            await app_with_workflow(scope, receive, send)

            # Verify 202 Accepted (async processing)
            assert response_status == 202
            response_json = json.loads(response_body)
            assert response_json["status"] == "accepted"

        finally:
            await app_with_workflow.shutdown()

    async def test_invalid_cloudevent_returns_400_bad_request(self, app_with_workflow):
        """Test that invalid CloudEvent returns 400 Bad Request with retryable=false."""
        await app_with_workflow.initialize()

        try:
            # Create invalid CloudEvent payload (missing required fields)
            invalid_event = {
                "type": "test_workflow",
                # Missing: specversion, source, id
            }

            body = json.dumps(invalid_event).encode("utf-8")

            # Mock ASGI receive/send
            async def receive():
                return {
                    "type": "http.request",
                    "body": body,
                    "more_body": False,
                }

            response_status = None
            response_body = b""

            async def send(message):
                nonlocal response_status, response_body
                if message["type"] == "http.response.start":
                    response_status = message["status"]
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
            }

            # Call ASGI app
            await app_with_workflow(scope, receive, send)

            # Verify 400 Bad Request (client error, non-retryable)
            assert response_status == 400
            response_json = json.loads(response_body)
            assert "error" in response_json
            assert response_json["retryable"] is False
            assert "error_type" in response_json
            # Should be CloudEvents exception or standard Python exception
            assert response_json["error_type"] in [
                "ValueError",
                "KeyError",
                "TypeError",
                "GenericException",  # CloudEvents exception
                "MissingRequiredFields",  # CloudEvents specific exception
            ]

        finally:
            await app_with_workflow.shutdown()

    async def test_malformed_json_returns_400_bad_request(self, app_with_workflow):
        """Test that malformed JSON returns 400 Bad Request."""
        await app_with_workflow.initialize()

        try:
            # Malformed JSON body
            body = b"{invalid json"

            # Mock ASGI receive/send
            async def receive():
                return {
                    "type": "http.request",
                    "body": body,
                    "more_body": False,
                }

            response_status = None
            response_body = b""

            async def send(message):
                nonlocal response_status, response_body
                if message["type"] == "http.response.start":
                    response_status = message["status"]
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
            }

            # Call ASGI app
            await app_with_workflow(scope, receive, send)

            # Verify 400 Bad Request
            assert response_status == 400
            response_json = json.loads(response_body)
            assert response_json["retryable"] is False

        finally:
            await app_with_workflow.shutdown()

    async def test_response_body_includes_error_type(self, app_with_workflow):
        """Test that error responses include error_type field."""
        await app_with_workflow.initialize()

        try:
            # Invalid CloudEvent (missing required fields)
            event_data = {"type": "test"}

            body = json.dumps(event_data).encode("utf-8")

            # Mock ASGI receive/send
            async def receive():
                return {
                    "type": "http.request",
                    "body": body,
                    "more_body": False,
                }

            response_body = b""

            async def send(message):
                nonlocal response_body
                if message["type"] == "http.response.body":
                    response_body += message.get("body", b"")

            scope = {
                "type": "http",
                "method": "POST",
                "path": "/",
                "query_string": b"",
                "headers": [
                    (b"content-type", b"application/cloudevents+json"),
                ],
            }

            # Call ASGI app
            await app_with_workflow(scope, receive, send)

            # Verify error response structure
            response_json = json.loads(response_body)
            assert "error" in response_json
            assert "error_type" in response_json
            assert "retryable" in response_json
            assert isinstance(response_json["retryable"], bool)

        finally:
            await app_with_workflow.shutdown()

    async def test_success_response_structure(self, app_with_workflow):
        """Test that success response has correct structure."""
        await app_with_workflow.initialize()

        try:
            # Valid CloudEvent
            event_data = {
                "specversion": "1.0",
                "type": "test_workflow",
                "source": "test",
                "id": "test-structure",
                "data": {"value": 10},
            }

            body = json.dumps(event_data).encode("utf-8")

            # Mock ASGI receive/send
            async def receive():
                return {
                    "type": "http.request",
                    "body": body,
                    "more_body": False,
                }

            response_body = b""

            async def send(message):
                nonlocal response_body
                if message["type"] == "http.response.body":
                    response_body += message.get("body", b"")

            scope = {
                "type": "http",
                "method": "POST",
                "path": "/",
                "query_string": b"",
                "headers": [
                    (b"content-type", b"application/cloudevents+json"),
                ],
            }

            # Call ASGI app
            await app_with_workflow(scope, receive, send)

            # Verify success response structure
            response_json = json.loads(response_body)
            assert "status" in response_json
            assert response_json["status"] == "accepted"
            # Success responses should not have error fields
            assert "error" not in response_json
            assert "error_type" not in response_json
            assert "retryable" not in response_json

        finally:
            await app_with_workflow.shutdown()
