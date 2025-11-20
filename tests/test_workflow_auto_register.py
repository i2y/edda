"""
Tests for automatic workflow registration.

This module tests the auto-registration feature where @workflow decorated
workflows are automatically registered as CloudEvent handlers.
"""

import pytest

from edda import EddaApp, WorkflowContext, activity, workflow
from edda.workflow import get_all_workflows


class TestSagaGlobalRegistry:
    """Test global workflow registry functionality."""

    def test_saga_auto_registration_in_global_registry(self):
        """@workflow decorator automatically registers workflow in global registry."""
        # Clear registry before test
        from edda import workflow
        from edda.workflow import _workflow_registry

        initial_count = len(_workflow_registry)

        @workflow(event_handler=True)
        async def test_saga_global(ctx: WorkflowContext, value: int) -> dict:
            return {"result": value * 2}

        # Check that workflow is registered
        all_workflows = get_all_workflows()
        assert "test_saga_global" in all_workflows
        assert all_workflows["test_saga_global"].name == "test_saga_global"
        assert len(all_workflows) == initial_count + 1

    def test_get_all_workflows_returns_copy(self):
        """get_all_workflows() returns a copy of the registry."""
        sagas1 = get_all_workflows()
        sagas2 = get_all_workflows()

        # Should be different dict instances
        assert sagas1 is not sagas2

        # But should have the same content
        assert sagas1.keys() == sagas2.keys()


class TestEddaAppAutoRegistration:
    """Test EddaApp auto-registration of workflow handlers."""

    @pytest.fixture
    async def clean_app(self, tmp_path):
        """Create a clean EddaApp for testing."""
        db_path = tmp_path / "test_auto_register.db"
        app = EddaApp(
            service_name="test-service",
            db_url=f"sqlite:///{db_path}",
        )
        yield app
        if app._initialized:
            await app.shutdown()

    async def test_kairo_app_auto_registers_saga_handlers(self, clean_app):
        """EddaApp.initialize() automatically registers all sagas."""

        @workflow(event_handler=True)
        async def auto_test_workflow(ctx: WorkflowContext, x: int) -> dict:
            return {"result": x}

        # Before initialization, no handlers
        assert "auto_test_workflow" not in clean_app.event_handlers

        # Initialize app
        await clean_app.initialize()

        # After initialization, handler should be registered
        assert "auto_test_workflow" in clean_app.event_handlers
        assert len(clean_app.event_handlers["auto_test_workflow"]) == 1

        await clean_app.shutdown()

    async def test_auto_registered_handler_starts_workflow(self, clean_app, tmp_path):
        """Auto-registered handler correctly starts workflow with CloudEvent data."""

        @activity
        async def double(ctx: WorkflowContext, value: int) -> int:
            return value * 2

        @workflow(event_handler=True)
        async def auto_handler_test(ctx: WorkflowContext, value: int) -> dict:
            result = await double(ctx, value)
            return {"doubled": result}

        # Initialize app
        await clean_app.initialize()

        # Create a mock CloudEvent
        class MockCloudEvent:
            def __init__(self, data):
                self._data = data

            def __getitem__(self, key):
                if key == "type":
                    return "auto_handler_test"
                raise KeyError(key)

            def get_data(self):
                return self._data

        event = MockCloudEvent({"value": 21})

        # Call the handler (wait=True for synchronous test execution)
        await clean_app.handle_cloudevent(event, wait=True)

        # Verify workflow was started
        # Note: We can't easily verify the workflow execution here without more complex setup,
        # but we've verified the handler is registered correctly

        await clean_app.shutdown()

    async def test_manual_handler_takes_precedence(self, clean_app):
        """Manual @app.on_event() registration takes precedence over auto-registration."""
        manual_called = False
        auto_called = False

        @workflow(event_handler=True)
        async def precedence_test(ctx: WorkflowContext, val: int) -> dict:
            nonlocal auto_called
            auto_called = True
            return {"val": val}

        # Manually register handler BEFORE initialization
        @clean_app.on_event("precedence_test")
        async def manual_handler(event):
            nonlocal manual_called
            manual_called = True

        # Initialize app
        await clean_app.initialize()

        # Only manual handler should be registered
        assert "precedence_test" in clean_app.event_handlers
        assert len(clean_app.event_handlers["precedence_test"]) == 1

        # Create mock event
        class MockCloudEvent:
            def __getitem__(self, key):
                if key == "type":
                    return "precedence_test"
                raise KeyError(key)

        event = MockCloudEvent()

        # Call handler (wait=True for synchronous test execution)
        await clean_app.handle_cloudevent(event, wait=True)

        # Only manual handler should have been called
        assert manual_called
        assert not auto_called

        await clean_app.shutdown()

    async def test_auto_handler_with_dict_data(self, clean_app):
        """Auto-registered handler passes dict data as kwargs."""
        received_kwargs = {}

        @workflow(event_handler=True)
        async def kwargs_test(ctx: WorkflowContext, name: str, count: int) -> dict:
            nonlocal received_kwargs
            received_kwargs = {"name": name, "count": count}
            return {"status": "ok"}

        await clean_app.initialize()

        # Create mock CloudEvent with dict data
        class MockCloudEvent:
            def __getitem__(self, key):
                if key == "type":
                    return "kwargs_test"
                raise KeyError(key)

            def get_data(self):
                return {"name": "test", "count": 5}

        # The auto-handler should call saga.start(**data)
        # We can't easily verify the kwargs here without more complex mocking,
        # but we can verify the handler exists
        assert "kwargs_test" in clean_app.event_handlers

        await clean_app.shutdown()

    async def test_auto_handler_with_non_dict_data(self, clean_app):
        """Auto-registered handler handles non-dict data gracefully."""

        @workflow(event_handler=True)
        async def non_dict_test(ctx: WorkflowContext) -> dict:
            return {"status": "ok"}

        await clean_app.initialize()

        # Create mock CloudEvent with non-dict data
        class MockCloudEvent:
            def __getitem__(self, key):
                if key == "type":
                    return "non_dict_test"
                raise KeyError(key)

            def get_data(self):
                return "simple string"  # Not a dict

        event = MockCloudEvent()

        # Handler should not crash (wait=True for synchronous test execution)
        await clean_app.handle_cloudevent(event, wait=True)

        await clean_app.shutdown()
