"""Tests for detached workflow start.

``start_detached`` returns the instance ID immediately and runs the workflow
body on a background task, so a caller is never blocked for the duration of a
workflow that has no suspension point (wait_event/sleep/channel). Execution
reuses the normal start path, so activities, history, and failure handling all
behave exactly as with ``start``.
"""

import asyncio
import sys

import pytest

from edda.activity import activity
from edda.context import WorkflowContext
from edda.exceptions import TerminalError
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine, workflow


@pytest.mark.asyncio
class TestStartDetached:
    """Test suite for ReplayEngine.start_workflow_detached / Workflow.start_detached."""

    @pytest.fixture
    def replay_engine(self, sqlite_storage):
        return ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-detached-001",
        )

    async def test_returns_before_body_completes(self, replay_engine, sqlite_storage):
        """The caller gets the instance ID while the workflow is still mid-body."""
        started = asyncio.Event()
        gate = asyncio.Event()

        async def gated_workflow(ctx: WorkflowContext) -> dict:
            started.set()
            await gate.wait()
            return {"ok": True}

        instance_id = await replay_engine.start_workflow_detached(
            workflow_name="gated_workflow",
            workflow_func=gated_workflow,
            input_data={},
        )
        assert instance_id.startswith("gated_workflow-")

        # Grab the background task (white-box) so completion is deterministic.
        assert len(replay_engine._detached_tasks) == 1
        task = next(iter(replay_engine._detached_tasks))

        # Let the workflow run up to its parking point, then prove it is parked
        # mid-body: the ID was handed back without running to completion.
        await asyncio.wait_for(started.wait(), timeout=2)
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "running"

        # Release it; the background task finishes on its own.
        gate.set()
        await asyncio.wait_for(task, timeout=2)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        assert instance["output_data"] == {"result": {"ok": True}}
        assert replay_engine._detached_tasks == set()  # cleaned up by the done-callback

    async def test_runs_activities_on_the_normal_path(self, replay_engine, sqlite_storage):
        """A detached workflow executes activities and records history like start()."""

        @activity
        async def greet(ctx: WorkflowContext, name: str) -> dict:
            return {"greeting": f"Hello, {name}"}

        async def wf(ctx: WorkflowContext, name: str) -> dict:
            return await greet(ctx, name)

        instance_id = await replay_engine.start_workflow_detached(
            workflow_name="wf",
            workflow_func=wf,
            input_data={"name": "Bob"},
        )
        task = next(iter(replay_engine._detached_tasks))
        await asyncio.wait_for(task, timeout=5)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        assert instance["output_data"] == {"result": {"greeting": "Hello, Bob"}}

        history = await sqlite_storage.get_history(instance_id)
        assert [h["event_type"] for h in history] == ["ActivityCompleted"]
        assert history[0]["event_data"]["activity_name"] == "greet"

    async def test_failure_is_backgrounded_not_raised_to_caller(
        self, replay_engine, sqlite_storage
    ):
        """A workflow that raises fails in the background; the caller keeps its ID."""

        async def boom(ctx: WorkflowContext) -> dict:
            raise TerminalError("kaboom")

        instance_id = await replay_engine.start_workflow_detached(
            workflow_name="boom",
            workflow_func=boom,
            input_data={},
        )
        # start_detached itself did not raise — the caller already holds an ID.
        assert instance_id.startswith("boom-")

        task = next(iter(replay_engine._detached_tasks))
        # start_workflow re-raises after marking the instance failed; drain the
        # task so the exception is observed.
        results = await asyncio.gather(task, return_exceptions=True)
        assert isinstance(results[0], TerminalError)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "failed"
        assert replay_engine._detached_tasks == set()

    async def test_workflow_start_detached_public_api(self, sqlite_storage):
        """Workflow.start_detached() drives the global replay engine end to end."""
        engine = ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-detached-002",
        )
        # edda/__init__ rebinds the name ``edda.workflow`` to the decorator, so
        # reach the module (which owns the global engine) via sys.modules.
        wf_mod = sys.modules["edda.workflow"]
        previous = wf_mod._replay_engine
        set_replay_engine(engine)
        try:

            @workflow
            async def public_wf(ctx: WorkflowContext, n: int) -> dict:
                return {"n": n}

            instance_id = await public_wf.start_detached(n=7)
            assert instance_id.startswith("public_wf-")

            task = next(iter(engine._detached_tasks))
            await asyncio.wait_for(task, timeout=5)

            instance = await sqlite_storage.get_instance(instance_id)
            assert instance["status"] == "completed"
            assert instance["output_data"] == {"result": {"n": 7}}
        finally:
            set_replay_engine(previous)
