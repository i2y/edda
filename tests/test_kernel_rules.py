"""Kernel semantics: same-id recur, run aborts, and cancellation."""

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from edda import actors, tasks
from edda.activity import activity
from edda.channels import publish, receive, sleep, subscribe
from edda.context import WorkflowContext
from edda.exceptions import ChannelPermissionError, InvalidArgumentError, TaskConflictError
from edda.locking import LockLostError
from edda.replay import ReplayEngine
from edda.storage.sqlalchemy_storage import (
    ChannelDeliveryCursor,
    WorkflowTimerSubscription,
)

pytestmark = pytest.mark.multidb


def _engine(storage):
    return ReplayEngine(storage=storage, service_name="phase-a", worker_id="phase-a-worker")


async def _fire_timer_and_resume(storage, engine, instance_id, workflow_func):
    async with storage.engine.begin() as conn:
        timer_id, activity_id = (
            await conn.execute(
                text(
                    "SELECT timer_id, activity_id FROM workflow_timer_subscriptions "
                    "WHERE instance_id = :i"
                ),
                {"i": instance_id},
            )
        ).one()
    await storage.append_history(
        instance_id,
        activity_id=activity_id,
        event_type="TimerExpired",
        event_data={"result": None, "timer_id": timer_id},
    )
    await storage.remove_timer_subscription(instance_id, timer_id)
    await storage.update_instance_status(instance_id, "running")
    await engine.resume_workflow(instance_id, workflow_func)


async def test_recur_keeps_instance_id_uses_new_input_and_archives_history(
    sqlite_storage,
):
    engine = _engine(sqlite_storage)

    @activity
    async def first_iteration_activity(ctx: WorkflowContext, iteration: int) -> dict:
        return {"iteration": iteration}

    async def recurring(ctx: WorkflowContext, iteration: int = 0) -> dict:
        if iteration == 0:
            await first_iteration_activity(ctx, iteration)
            await ctx.recur(iteration=1)
        return {"iteration": iteration}

    instance_id = await engine.start_workflow("phase_a_recur_basic", recurring, {"iteration": 0})

    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    if instance["status"] == "recurred":
        await engine.resume_workflow(instance_id, recurring)
        instance = await sqlite_storage.get_instance(instance_id)

    assert instance is not None
    assert instance["status"] == "completed"
    assert instance["input_data"] == {"iteration": 1}
    assert instance["output_data"]["result"] == {"iteration": 1}

    async with sqlite_storage.engine.begin() as conn:
        rows = (
            await conn.execute(
                text("SELECT instance_id, status FROM workflow_instances WHERE workflow_name = :w"),
                {"w": "phase_a_recur_basic"},
            )
        ).all()
        archived = (
            await conn.execute(
                text(
                    "SELECT instance_id, activity_id FROM workflow_history_archive "
                    "WHERE activity_id = 'first_iteration_activity:1'"
                )
            )
        ).all()

    assert [r[0] for r in rows] == [instance_id]
    assert [r[0] for r in archived] == [instance_id]
    assert rows[0][1] == "completed"


async def test_recur_preserves_instance_scoped_state_and_emits_no_down(
    sqlite_storage, create_test_instance
):
    engine = _engine(sqlite_storage)
    target_id = await create_test_instance("phase-a-monitor-target", "wf", {})

    async def stateful_recur(ctx: WorkflowContext, target: str, iteration: int = 0) -> dict:
        if iteration == 0:
            await actors.register_name(ctx, "phase-a-stable-name")
            await actors.monitor(ctx, target, "phase-a-down")
            await actors.issue_capability(ctx, "actor", "phase-a-target")
            await ctx.recur(target=target, iteration=1)
        # Keep the next iteration live so instance-scoped state can be checked
        # before the legitimate terminal-state cleanup runs.
        await sleep(ctx, 3600)
        return {"done": True}

    instance_id = await engine.start_workflow(
        "phase_a_recur_state", stateful_recur, {"target": target_id, "iteration": 0}
    )
    instance = await sqlite_storage.get_instance(instance_id)
    if instance is not None and instance["status"] == "recurred":
        await engine.resume_workflow(instance_id, stateful_recur)

    assert await sqlite_storage.whereis_actor("phase-a-stable-name") == instance_id
    async with sqlite_storage.engine.begin() as conn:
        monitors = (
            await conn.execute(
                text(
                    "SELECT watcher_instance_id, target_instance_id FROM actor_monitors "
                    "WHERE watcher_instance_id = :i"
                ),
                {"i": instance_id},
            )
        ).all()
        capabilities = (
            await conn.execute(
                text("SELECT granted_by FROM actor_capabilities WHERE target = 'phase-a-target'")
            )
        ).all()

    assert monitors == [(instance_id, target_id)]
    assert capabilities == [(instance_id,)]

    # A recur is not a death: the DOWN sweep neither emits nor consumes the monitor.
    await sqlite_storage.update_instance_status(target_id, "recurred")
    assert await sqlite_storage.emit_actor_down_for_terminated() == 0
    assert await sqlite_storage.whereis_actor("phase-a-stable-name") == instance_id


async def test_recur_reset_is_one_fenced_transaction_and_preserves_delivery_cursors(
    sqlite_storage, create_test_instance
):
    instance_id = await create_test_instance("phase-a-reset", "wf", {"old": True})
    await sqlite_storage.append_history(
        instance_id, "seed:1", "ActivityCompleted", {"result": "old"}
    )
    await sqlite_storage.subscribe_to_channel(instance_id, "phase-a-channel", "broadcast")
    await sqlite_storage.join_group(instance_id, "phase-a-group")
    await sqlite_storage.push_compensation(
        instance_id, "seed:1", "undo", {"args": [], "kwargs": {}}
    )
    async with AsyncSession(sqlite_storage.engine) as session:
        session.add(
            WorkflowTimerSubscription(
                instance_id=instance_id,
                timer_id="timer:1",
                expires_at=datetime.now(UTC) + timedelta(hours=1),
                activity_id="timer:1",
            )
        )
        session.add(
            ChannelDeliveryCursor(
                channel="phase-a-channel", instance_id=instance_id, last_delivered_id=7
            )
        )
        await session.commit()

    lock_id = "phase-a-lock"
    assert await sqlite_storage.try_acquire_lock(instance_id, lock_id)
    await sqlite_storage.reset_instance_for_recur(instance_id, {"new": True}, lock_id)
    await sqlite_storage.release_lock(instance_id, lock_id)

    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "recurred"
    assert instance["input_data"] == {"new": True}
    assert await sqlite_storage.get_history(instance_id) == []
    assert await sqlite_storage.get_compensations(instance_id) == []
    assert await sqlite_storage.get_group_members("phase-a-group") == []
    assert await sqlite_storage.find_waiting_instances_by_channel("phase-a-channel") == []

    async with sqlite_storage.engine.begin() as conn:
        archived = (
            await conn.execute(
                text("SELECT COUNT(*) FROM workflow_history_archive WHERE instance_id = :i"),
                {"i": instance_id},
            )
        ).scalar_one()
        timers = (
            await conn.execute(
                text("SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id = :i"),
                {"i": instance_id},
            )
        ).scalar_one()
        subscriptions = (
            await conn.execute(
                text("SELECT COUNT(*) FROM channel_subscriptions WHERE instance_id = :i"),
                {"i": instance_id},
            )
        ).scalar_one()
        cursor = (
            await conn.execute(
                text(
                    "SELECT last_delivered_id FROM channel_delivery_cursors "
                    "WHERE instance_id = :i AND channel = 'phase-a-channel'"
                ),
                {"i": instance_id},
            )
        ).scalar_one()
    assert archived == 1
    assert timers == 0
    assert subscriptions == 0
    assert cursor == 7


async def test_recur_reset_with_a_lost_lock_changes_nothing(sqlite_storage, create_test_instance):
    instance_id = await create_test_instance("phase-a-reset-lost", "wf", {"old": True})
    await sqlite_storage.append_history(
        instance_id, "seed:1", "ActivityCompleted", {"result": "old"}
    )
    await sqlite_storage.subscribe_to_channel(instance_id, "phase-a-lost", "broadcast")
    await sqlite_storage.push_compensation(
        instance_id, "seed:1", "undo", {"args": [], "kwargs": {}}
    )
    lock_id = "phase-a-lost-lock"
    assert await sqlite_storage.try_acquire_lock(instance_id, lock_id)
    await sqlite_storage.release_lock(instance_id, lock_id)

    with pytest.raises(LockLostError):
        await sqlite_storage.reset_instance_for_recur(instance_id, {"new": True}, lock_id)

    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "running"
    assert instance["input_data"] == {"old": True}
    assert len(await sqlite_storage.get_history(instance_id)) == 1
    assert len(await sqlite_storage.get_compensations(instance_id)) == 1
    async with sqlite_storage.engine.begin() as conn:
        archived = (
            await conn.execute(
                text("SELECT COUNT(*) FROM workflow_history_archive WHERE instance_id = :i"),
                {"i": instance_id},
            )
        ).scalar_one()
        subscriptions = (
            await conn.execute(
                text("SELECT COUNT(*) FROM channel_subscriptions WHERE instance_id = :i"),
                {"i": instance_id},
            )
        ).scalar_one()
    assert archived == 0
    assert subscriptions == 1


async def test_recurred_instance_is_claimed_set_running_and_resumed(
    sqlite_storage, create_test_instance
):
    instance_id = await create_test_instance("phase-a-left-recurred", "wf", {"iteration": 1})
    await sqlite_storage.update_instance_status(instance_id, "recurred")
    engine = _engine(sqlite_storage)

    async def recurring(ctx: WorkflowContext, iteration: int) -> dict:
        return {"iteration": iteration}

    claimed = await sqlite_storage.claim_resumable_workflows("sweep-lock", limit=10)
    assert instance_id in {row["instance_id"] for row in claimed}
    try:
        await engine.resume_workflow(
            instance_id, recurring, already_locked=True, lock_id="sweep-lock"
        )
    finally:
        await sqlite_storage.release_lock(instance_id, "sweep-lock")

    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed"
    assert instance["output_data"]["result"] == {"iteration": 1}


async def test_transient_storage_error_in_a_step_aborts_and_next_resume_reexecutes(
    sqlite_storage, monkeypatch
):
    engine = _engine(sqlite_storage)
    calls = 0
    original = sqlite_storage.whereis_actor

    async def flaky_whereis(name):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OperationalError("SELECT 1", {}, Exception("transient"))
        return await original(name)

    monkeypatch.setattr(sqlite_storage, "whereis_actor", flaky_whereis)

    async def lookup_workflow(ctx: WorkflowContext) -> dict:
        return {"found": await actors.whereis(ctx, "nobody")}

    instance_id = await engine.start_workflow("phase_a_abort_step", lookup_workflow, {})
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "running"
    assert await sqlite_storage.get_history(instance_id) == []
    assert calls == 1

    await engine.resume_workflow(instance_id, lookup_workflow)
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed"
    assert instance["output_data"]["result"] == {"found": None}
    assert calls == 2
    assert all(
        h["event_type"] != "ActivityFailed" for h in await sqlite_storage.get_history(instance_id)
    )


async def test_transient_storage_error_in_an_effect_step_aborts_and_is_reexecuted(
    sqlite_storage, monkeypatch
):
    engine = _engine(sqlite_storage)
    calls = 0
    original = sqlite_storage.post_task

    async def flaky_post_task(task_id, channel, data):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OperationalError("INSERT", {}, Exception("transient"))
        return await original(task_id, channel, data)

    monkeypatch.setattr(sqlite_storage, "post_task", flaky_post_task)

    async def posting_workflow(ctx: WorkflowContext) -> dict:
        task_id = await tasks.post_task(ctx, "jobs:effect-abort", {"x": 1})
        return {"task_id": task_id}

    instance_id = await engine.start_workflow("phase_a_abort_effect", posting_workflow, {})
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "running"
    assert calls == 1
    assert await sqlite_storage.get_history(instance_id) == []

    await engine.resume_workflow(instance_id, posting_workflow)
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed"
    assert calls == 2
    assert all(
        h["event_type"] != "ActivityFailed" for h in await sqlite_storage.get_history(instance_id)
    )


async def test_a_swallowed_run_abort_still_aborts_the_run(sqlite_storage, monkeypatch):
    engine = _engine(sqlite_storage)
    original = sqlite_storage.whereis_actor
    seen = []

    async def alwaysbroken(name):
        raise OperationalError("SELECT 1", {}, Exception("transient"))

    monkeypatch.setattr(sqlite_storage, "whereis_actor", alwaysbroken)

    async def swallowing_workflow(ctx: WorkflowContext) -> dict:
        try:
            await actors.whereis(ctx, "nobody")
        except BaseException as first:
            seen.append(first)
        try:
            await actors.whereis(ctx, "nobody-again")
        except BaseException as second:
            seen.append(second)
        return {"completed": True}

    instance_id = await engine.start_workflow("phase_a_swallow_abort", swallowing_workflow, {})
    monkeypatch.setattr(sqlite_storage, "whereis_actor", original)

    assert len(seen) == 2
    assert seen[0] is seen[1]
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "running"
    assert instance["output_data"] is None


async def test_kernel_step_errors_are_recorded_with_code_and_replayed_as_their_class(
    sqlite_storage,
):
    engine = _engine(sqlite_storage)
    await sqlite_storage.post_task("phase-a-task", "jobs:phase-a", "{}")
    assert (await sqlite_storage.claim_task("jobs:phase-a", "someone-else")) is not None

    async def conflict_workflow(ctx: WorkflowContext) -> dict:
        try:
            await tasks.complete_task(ctx, "phase-a-task", {"ok": True})
            outcome = "completed"
        except TaskConflictError:
            outcome = "conflict"
        await sleep(ctx, 1)
        return {"outcome": outcome}

    instance_id = await engine.start_workflow("phase_a_kernel_error", conflict_workflow, {})
    history = await sqlite_storage.get_history(instance_id)
    failure = next(h for h in history if h["event_type"] == "ActivityFailed")
    assert failure["event_data"]["error_type"] == "TaskConflictError"
    assert failure["event_data"]["code"] == "task_conflict"

    # Revoke the conflicting ownership fact: replay must still raise the recorded error.
    async with sqlite_storage.engine.begin() as conn:
        await conn.execute(text("DELETE FROM actor_tasks WHERE id = 'phase-a-task'"))
    await _fire_timer_and_resume(sqlite_storage, engine, instance_id, conflict_workflow)

    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed"
    assert instance["output_data"]["result"] == {"outcome": "conflict"}


async def test_actor_name_taken_is_recorded_as_the_kernel_error_class(sqlite_storage):
    engine = _engine(sqlite_storage)

    async def owner_workflow(ctx: WorkflowContext) -> dict:
        await actors.register_name(ctx, "phase-a-name")
        return {}

    async def thief_workflow(ctx: WorkflowContext) -> dict:
        try:
            await actors.register_name(ctx, "phase-a-name")
            taken = False
        except ValueError as error:
            taken = type(error).__name__ == "ActorNameTakenError"
        return {"taken": taken}

    await engine.start_workflow("phase_a_name_owner", owner_workflow, {})
    thief_id = await engine.start_workflow("phase_a_name_thief", thief_workflow, {})
    instance = await sqlite_storage.get_instance(thief_id)
    assert instance is not None
    assert instance["output_data"]["result"] == {"taken": True}
    failure = next(
        h for h in await sqlite_storage.get_history(thief_id) if h["event_type"] == "ActivityFailed"
    )
    assert failure["event_data"]["error_type"] == "ActorNameTakenError"
    assert failure["event_data"]["code"] == "actor_name_taken"


async def test_invalid_primitive_arguments_are_recorded_as_kernel_errors(sqlite_storage):
    engine = _engine(sqlite_storage)

    async def invalid_workflow(ctx: WorkflowContext) -> dict:
        try:
            await tasks.post_task(ctx, "jobs:invalid", {"bad": object()})
            invalid = False
        except Exception as error:
            invalid = type(error).__name__ == "InvalidArgumentError"
        return {"invalid": invalid}

    instance_id = await engine.start_workflow("phase_a_invalid_argument", invalid_workflow, {})
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["output_data"]["result"] == {"invalid": True}
    failure = next(
        h
        for h in await sqlite_storage.get_history(instance_id)
        if h["event_type"] == "ActivityFailed"
    )
    assert failure["event_data"]["error_type"] == "InvalidArgumentError"
    assert failure["event_data"]["code"] == "invalid_argument"


async def test_a_value_error_from_storage_in_a_step_is_not_an_outcome(sqlite_storage, monkeypatch):
    # aiosqlite raises ValueError("Connection closed") on a closed connection:
    # only errors a primitive detects itself are recorded as invalid_argument.
    engine = _engine(sqlite_storage)
    calls = 0
    original = sqlite_storage.whereis_actor

    async def closed_whereis(name):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("Connection closed")
        return await original(name)

    monkeypatch.setattr(sqlite_storage, "whereis_actor", closed_whereis)

    async def lookup_workflow(ctx: WorkflowContext) -> dict:
        return {"found": await actors.whereis(ctx, "nobody")}

    instance_id = await engine.start_workflow("phase_a_value_error_step", lookup_workflow, {})
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "running"
    assert await sqlite_storage.get_history(instance_id) == []

    await engine.resume_workflow(instance_id, lookup_workflow)
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed"
    assert calls == 2


async def test_an_unserializable_message_is_recorded_as_an_invalid_argument(sqlite_storage):
    engine = _engine(sqlite_storage)

    async def publishing_workflow(ctx: WorkflowContext) -> dict:
        try:
            await publish(ctx, "phase-a-bad-payload", {"bad": object()})
        except InvalidArgumentError:
            return {"invalid": True}
        return {"invalid": False}

    instance_id = await engine.start_workflow("phase_a_bad_payload", publishing_workflow, {})
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["output_data"]["result"] == {"invalid": True}
    failure = next(
        h
        for h in await sqlite_storage.get_history(instance_id)
        if h["event_type"] == "ActivityFailed"
    )
    assert failure["event_data"]["code"] == "invalid_argument"


async def test_sealed_receive_refusal_is_recorded_and_replay_does_not_recheck(
    sqlite_storage,
):
    engine = _engine(sqlite_storage)
    channel = "phase-a-sealed"
    await sqlite_storage.grant_capability(
        "phase-a-cap", "channel_sub", channel, "not-a-real-secret-hash"
    )

    async def forbidden_receiver(ctx: WorkflowContext) -> dict:
        await subscribe(ctx, channel, mode="broadcast")
        try:
            await receive(ctx, channel)
            outcome = "received"
        except ChannelPermissionError:
            outcome = "forbidden"
        await sleep(ctx, 1)
        return {"outcome": outcome}

    instance_id = await engine.start_workflow("phase_a_sealed_receive", forbidden_receiver, {})
    history = await sqlite_storage.get_history(instance_id)
    refusal = next(h for h in history if h["event_type"] == "ActivityFailed")
    assert refusal["activity_id"].startswith("receive_phase-a-sealed")
    assert refusal["event_data"]["error_type"] == "ChannelPermissionError"
    assert refusal["event_data"]["code"] == "channel_forbidden"

    # Once the grant is gone a fresh check would allow the receive; replay must not re-check.
    await sqlite_storage.revoke_capability("phase-a-cap")
    await _fire_timer_and_resume(sqlite_storage, engine, instance_id, forbidden_receiver)
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed"
    assert instance["output_data"]["result"] == {"outcome": "forbidden"}


async def test_compensations_never_run_after_lock_loss(sqlite_storage, monkeypatch):
    engine = _engine(sqlite_storage)
    compensation_calls = []

    async def spy_compensations(ctx):
        compensation_calls.append(ctx.instance_id)

    monkeypatch.setattr("edda.replay.execute_compensations", spy_compensations)
    lose_once = True

    async def losing_workflow(ctx: WorkflowContext) -> dict:
        nonlocal lose_once
        if lose_once:
            lose_once = False
            await ctx.storage.release_lock(ctx.instance_id, ctx.lock_id)
            raise LockLostError("lease stolen")
        return {"ok": True}

    instance_id = await engine.start_workflow("phase_a_lock_loss", losing_workflow, {})
    assert compensation_calls == []
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "running"
    assert all(
        h["event_type"] != "WorkflowFailed" for h in await sqlite_storage.get_history(instance_id)
    )

    await engine.resume_workflow(instance_id, losing_workflow)
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed"


async def test_a_storage_failure_while_recording_an_activity_aborts_without_activity_failed(
    sqlite_storage, monkeypatch
):
    engine = _engine(sqlite_storage)
    original_append = sqlite_storage.append_history
    body_calls = 0
    failed_once = False

    @activity
    async def simple_activity(ctx: WorkflowContext) -> dict:
        nonlocal body_calls
        body_calls += 1
        return {"ok": True}

    async def flaky_append(instance_id, activity_id, event_type, event_data, lock_id=None):
        nonlocal failed_once
        if (
            event_type == "ActivityCompleted"
            and activity_id == "simple_activity:1"
            and not failed_once
        ):
            failed_once = True
            raise OperationalError("INSERT", {}, Exception("transient"))
        return await original_append(
            instance_id, activity_id, event_type, event_data, lock_id=lock_id
        )

    monkeypatch.setattr(sqlite_storage, "append_history", flaky_append)

    async def activity_workflow(ctx: WorkflowContext) -> dict:
        return await simple_activity(ctx)

    instance_id = await engine.start_workflow(
        "phase_a_activity_record_abort", activity_workflow, {}
    )
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "running"
    assert body_calls == 1
    assert all(
        h["event_type"] != "ActivityFailed" for h in await sqlite_storage.get_history(instance_id)
    )

    await engine.resume_workflow(instance_id, activity_workflow)
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed"
    assert body_calls == 2


async def test_lock_loss_or_cancellation_during_an_activity_is_not_recorded(
    sqlite_storage,
):
    engine = _engine(sqlite_storage)

    @activity
    async def loses_lock(ctx: WorkflowContext) -> dict:
        await ctx.storage.release_lock(ctx.instance_id, ctx.lock_id)
        raise LockLostError("lease stolen")

    async def lock_workflow(ctx: WorkflowContext) -> dict:
        await loses_lock(ctx)
        return {}

    instance_id = await engine.start_workflow("phase_a_activity_lock_loss", lock_workflow, {})
    assert all(
        h["event_type"] != "ActivityFailed" for h in await sqlite_storage.get_history(instance_id)
    )

    @activity
    async def never_finishes(ctx: WorkflowContext) -> dict:
        await asyncio.sleep(60)
        return {}

    async def cancelling_workflow(ctx: WorkflowContext) -> dict:
        await never_finishes(ctx)
        return {}

    run = asyncio.create_task(
        engine.start_workflow("phase_a_activity_cancelled", cancelling_workflow, {})
    )
    await asyncio.sleep(0.1)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    async with sqlite_storage.engine.begin() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT event_type FROM workflow_history WHERE instance_id IN "
                    "(SELECT instance_id FROM workflow_instances WHERE workflow_name = :w)"
                ),
                {"w": "phase_a_activity_cancelled"},
            )
        ).all()
    assert "ActivityFailed" not in {r[0] for r in rows}


async def test_a_swallowed_task_cancellation_still_ends_the_run_without_completion(
    sqlite_storage,
):
    engine = _engine(sqlite_storage)

    @activity
    async def never_finishes_swallowed(ctx: WorkflowContext) -> dict:
        await asyncio.sleep(60)
        return {}

    async def swallow_cancel_workflow(ctx: WorkflowContext) -> dict:
        with suppress(BaseException):
            await never_finishes_swallowed(ctx)
        return {"completed": True}

    run = asyncio.create_task(
        engine.start_workflow("phase_a_swallow_cancel", swallow_cancel_workflow, {})
    )
    await asyncio.sleep(0.1)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    async with sqlite_storage.engine.begin() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT instance_id, status, output_data FROM workflow_instances "
                    "WHERE workflow_name = 'phase_a_swallow_cancel'"
                )
            )
        ).one()
    assert row[1] == "running"
    assert row[2] is None
    assert all(
        h["event_type"] != "ActivityFailed" for h in await sqlite_storage.get_history(row[0])
    )


async def test_compensating_instances_are_not_cancellable_but_recurred_instances_are(
    sqlite_storage, create_test_instance
):
    compensating = await create_test_instance("phase-a-compensating", "wf", {})
    await sqlite_storage.update_instance_status(compensating, "compensating")
    assert not await sqlite_storage.cancel_instance(compensating, "test")
    assert not await sqlite_storage.cancel_instance(compensating, "test", allow_compensating=True)
    assert (await sqlite_storage.get_instance(compensating))["status"] == "compensating"  # type: ignore[index]

    recurred = await create_test_instance("phase-a-recurred-cancel", "wf", {})
    await sqlite_storage.update_instance_status(recurred, "recurred")
    engine = _engine(sqlite_storage)
    assert await engine.cancel_workflow(recurred, "test")
    assert (await sqlite_storage.get_instance(recurred))["status"] == "cancelled"  # type: ignore[index]
