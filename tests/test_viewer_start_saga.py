"""
Tests for Viewer's workflow start functionality.

Tests the ability to start workflows from the Viewer UI by sending CloudEvents.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from edda.storage.sqlalchemy_storage import SQLAlchemyStorage
from edda.viewer_ui.data_service import WorkflowDataService
from edda.workflow import workflow


@pytest.fixture
async def storage():
    """Create in-memory storage for testing."""
    storage = SQLAlchemyStorage(create_async_engine("sqlite+aiosqlite:///:memory:", echo=False))
    await storage.initialize()
    yield storage
    await storage.close()


@pytest.fixture
def data_service(storage):
    """Create DataService for testing."""
    return WorkflowDataService(storage)


@pytest.fixture
def sample_workflow():
    """Create a sample workflow for testing."""

    @workflow
    async def test_workflow(ctx, user_id: int):
        """Test workflow."""
        return f"Hello {user_id}"

    return test_workflow


class TestGetAllSagas:
    """Test get_all_workflows() method."""

    def test_get_all_workflows_returns_registry(self, data_service, sample_workflow):
        """Test that get_all_workflows() returns the workflow registry."""
        all_workflows = data_service.get_all_workflows()

        assert isinstance(all_workflows, dict)
        assert "test_workflow" in all_workflows
        assert all_workflows["test_workflow"].func == sample_workflow.func

    def test_get_all_workflows_empty_when_no_sagas(self, data_service):
        """Test that get_all_workflows() returns empty dict when no workflows registered."""
        # Clear the registry
        from edda.workflow import _workflow_registry

        original_registry = _workflow_registry.copy()
        _workflow_registry.clear()

        try:
            all_workflows = data_service.get_all_workflows()
            assert all_workflows == {}
        finally:
            # Restore registry
            _workflow_registry.update(original_registry)


class TestStartSaga:
    """Test start_workflow() method."""

    @pytest.mark.asyncio
    async def test_start_saga_sends_cloudevent(self, data_service, sample_workflow):
        """Test that start_workflow() sends a CloudEvent to EddaApp."""
        workflow_name = "test_workflow"
        params = {"user_id": 123}
        edda_app_url = "http://localhost:8001"

        # Mock httpx.AsyncClient
        with patch("httpx.AsyncClient") as mock_client_class:
            # Create mock response
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = '{"status": "accepted"}'

            # Create mock client
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None

            mock_client_class.return_value = mock_client

            # Call start_workflow
            success, message, instance_id = await data_service.start_workflow(
                workflow_name, params, edda_app_url
            )

            # Verify result
            assert success is True
            assert "started successfully" in message
            assert instance_id is None  # CloudEvent returns immediately

            # Verify httpx.post was called
            mock_client.post.assert_called_once()
            call_args = mock_client.post.call_args

            # Verify URL
            assert call_args[0][0] == edda_app_url

            # Verify CloudEvent format
            headers = call_args[1]["headers"]
            content = call_args[1]["content"]

            # Parse content as JSON
            event_data = json.loads(content)

            # Verify CloudEvent structure
            assert event_data["type"] == workflow_name
            assert event_data["source"] == "edda.viewer"
            assert "id" in event_data
            assert event_data["data"] == params

            # Verify headers
            assert headers["content-type"] == "application/cloudevents+json"

    @pytest.mark.asyncio
    async def test_start_saga_with_empty_params(self, data_service, sample_workflow):
        """Test start_workflow() with empty parameters."""
        workflow_name = "test_workflow"
        params = {}
        edda_app_url = "http://localhost:8001"

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = '{"status": "accepted"}'

            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None

            mock_client_class.return_value = mock_client

            success, message, _ = await data_service.start_workflow(
                workflow_name, params, edda_app_url
            )

            assert success is True
            assert "started successfully" in message

    @pytest.mark.asyncio
    async def test_start_saga_not_found(self, data_service):
        """Test start_workflow() with non-existent workflow."""
        workflow_name = "non_existent_workflow"
        params = {}
        edda_app_url = "http://localhost:8001"

        success, message, instance_id = await data_service.start_workflow(
            workflow_name, params, edda_app_url
        )

        assert success is False
        assert "not found in registry" in message
        assert instance_id is None

    @pytest.mark.asyncio
    async def test_start_saga_connection_error(self, data_service, sample_workflow):
        """Test start_workflow() when EddaApp is not reachable."""
        workflow_name = "test_workflow"
        params = {"user_id": 123}
        edda_app_url = "http://localhost:9999"  # Non-existent server

        with patch("httpx.AsyncClient") as mock_client_class:
            from httpx import ConnectError

            mock_client = AsyncMock()
            mock_client.post.side_effect = ConnectError("Connection refused")
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None

            mock_client_class.return_value = mock_client

            success, message, instance_id = await data_service.start_workflow(
                workflow_name, params, edda_app_url
            )

            assert success is False
            assert "Cannot connect" in message
            assert instance_id is None

    @pytest.mark.asyncio
    async def test_start_saga_timeout(self, data_service, sample_workflow):
        """Test start_workflow() when request times out."""
        workflow_name = "test_workflow"
        params = {"user_id": 123}
        edda_app_url = "http://localhost:8001"

        with patch("httpx.AsyncClient") as mock_client_class:
            from httpx import TimeoutException

            mock_client = AsyncMock()
            mock_client.post.side_effect = TimeoutException("Request timed out")
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None

            mock_client_class.return_value = mock_client

            success, message, instance_id = await data_service.start_workflow(
                workflow_name, params, edda_app_url
            )

            assert success is False
            assert "timed out" in message
            assert instance_id is None

    @pytest.mark.asyncio
    async def test_start_saga_server_error(self, data_service, sample_workflow):
        """Test start_workflow() when server returns error."""
        workflow_name = "test_workflow"
        params = {"user_id": 123}
        edda_app_url = "http://localhost:8001"

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.text = '{"error": "Internal server error"}'

            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None

            mock_client_class.return_value = mock_client

            success, message, instance_id = await data_service.start_workflow(
                workflow_name, params, edda_app_url
            )

            assert success is False
            assert "Server error" in message
            assert instance_id is None


class TestEndToEnd:
    """End-to-end tests with real EddaApp."""

    @pytest.mark.asyncio
    async def test_start_saga_end_to_end(self, data_service, sample_workflow):
        """Test starting a workflow end-to-end (mock ASGI app)."""
        from edda import EddaApp
        from edda.activity import activity

        # Create a simple test activity
        @activity
        async def test_activity(x: int) -> int:
            return x * 2

        # Create test saga
        @workflow
        async def e2e_test_workflow(ctx, value: int):
            result = await ctx.run(test_activity, x=value)
            return result

        # Create EddaApp
        app = EddaApp(
            service_name="test_service",
            db_url="sqlite:///:memory:",
        )
        await app.initialize()

        # Create DataService with app's storage
        service = WorkflowDataService(app.storage)

        try:
            # Get workflow registry
            all_workflows = service.get_all_workflows()
            assert "e2e_test_workflow" in all_workflows

            # Mock HTTP client to send CloudEvent
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.text = '{"status": "accepted"}'

                mock_client = AsyncMock()
                mock_client.post.return_value = mock_response
                mock_client.__aenter__.return_value = mock_client
                mock_client.__aexit__.return_value = None

                mock_client_class.return_value = mock_client

                # Start workflow via DataService
                success, message, _ = await service.start_workflow(
                    "e2e_test_workflow", {"value": 42}, "http://localhost:8001"
                )

                assert success is True
                assert "started successfully" in message

                # Verify CloudEvent was sent with correct data
                call_args = mock_client.post.call_args
                content = call_args[1]["content"]
                event_data = json.loads(content)

                assert event_data["type"] == "e2e_test_workflow"
                assert event_data["data"]["value"] == 42

        finally:
            await app.shutdown()
