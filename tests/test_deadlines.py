"""S13: no timer and no receive timeout fires before its deadline."""

import asyncio
import time
from datetime import UTC, datetime, timedelta

import pytest

from edda.channels import receive, sleep, sleep_until, subscribe
from edda.context import WorkflowContext
from edda.replay import ReplayEngine

pytestmark = pytest.mark.multidb

DEADLINE = 0.3


async def _align_to_early_second_half() -> None:
    """Start in the first half of a second: a deadline then truncates to the
    second it was set in, which is how an early expiry shows up."""
    while time.time() % 1 > 0.5:
        await asyncio.sleep(0.01)


async def _first_seen(find, instance_id: str) -> float:
    for _ in range(400):
        if any(row["instance_id"] == instance_id for row in await find()):
            return time.time()
        await asyncio.sleep(0.01)
    raise AssertionError("deadline never expired")


async def test_sleep_never_fires_early(sqlite_storage):
    engine = ReplayEngine(storage=sqlite_storage, service_name="s13", worker_id="s13-worker")

    async def workflow(ctx: WorkflowContext) -> None:
        await sleep(ctx, DEADLINE)

    await _align_to_early_second_half()
    slept_at = time.time()
    instance_id = await engine.start_workflow("s13_sleep", workflow, {})
    fired_at = await _first_seen(sqlite_storage.find_expired_timers, instance_id)
    assert fired_at - slept_at >= DEADLINE


async def test_receive_timeout_never_fires_early(sqlite_storage):
    engine = ReplayEngine(storage=sqlite_storage, service_name="s13", worker_id="s13-worker")

    async def workflow(ctx: WorkflowContext) -> None:
        await subscribe(ctx, "s13", mode="broadcast")
        await receive(ctx, "s13", timeout_seconds=DEADLINE)

    await _align_to_early_second_half()
    waiting_since = time.time()
    instance_id = await engine.start_workflow("s13_receive", workflow, {})
    timed_out_at = await _first_seen(sqlite_storage.find_expired_message_subscriptions, instance_id)
    assert timed_out_at - waiting_since >= DEADLINE


async def test_sleep_until_rounds_up_but_a_past_target_fires_at_once(sqlite_storage):
    engine = ReplayEngine(storage=sqlite_storage, service_name="s13", worker_id="s13-worker")
    await _align_to_early_second_half()
    target = datetime.now(UTC) + timedelta(seconds=DEADLINE)

    async def future(ctx: WorkflowContext) -> None:
        await sleep_until(ctx, target)

    async def past(ctx: WorkflowContext) -> None:
        await sleep_until(ctx, datetime.now(UTC) - timedelta(seconds=DEADLINE))

    future_id = await engine.start_workflow("s13_future", future, {})
    past_id = await engine.start_workflow("s13_past", past, {})
    assert await _first_seen(sqlite_storage.find_expired_timers, future_id) >= target.timestamp()
    expired = {row["instance_id"] for row in await sqlite_storage.find_expired_timers()}
    assert past_id in expired
