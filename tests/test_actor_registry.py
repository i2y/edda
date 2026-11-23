"""Actor registry tests: register/whereis, monitors, terminal DOWN sweep."""

import pytest


@pytest.mark.asyncio
async def test_register_whereis_unregister(sqlite_storage, create_test_instance):
    iid = await create_test_instance("wf-actor-1", "wf", {})
    other = await create_test_instance("wf-actor-2", "wf", {})

    await sqlite_storage.register_actor_name("order-actor", iid)
    assert await sqlite_storage.whereis_actor("order-actor") == iid
    assert await sqlite_storage.whereis_actor("nope") is None

    # duplicate name rejected
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        await sqlite_storage.register_actor_name("order-actor", other)

    await sqlite_storage.unregister_actor_name("order-actor")
    await sqlite_storage.register_actor_name("order-actor", other)
    assert await sqlite_storage.whereis_actor("order-actor") == other


@pytest.mark.asyncio
async def test_monitor_down_delivery(sqlite_storage, create_test_instance):
    watcher = await create_test_instance("wf-watcher", "wf", {})
    target = await create_test_instance("wf-target", "wf", {})

    await sqlite_storage.monitor_actor(watcher, target, "mon:wf-watcher")
    # idempotent re-monitor
    await sqlite_storage.monitor_actor(watcher, target, "mon:wf-watcher")

    # not terminal yet — nothing emitted
    assert await sqlite_storage.emit_actor_down_for_terminated() == 0

    await sqlite_storage.update_instance_status(target, "completed")
    assert await sqlite_storage.emit_actor_down_for_terminated() == 1

    # DOWN message delivered to the monitor channel
    from sqlalchemy import text

    async with sqlite_storage.engine.begin() as conn:
        rows = (
            await conn.execute(
                text("SELECT data FROM channel_messages WHERE channel = 'mon:wf-watcher'")
            )
        ).all()
    assert len(rows) == 1
    import json

    payload = json.loads(rows[0][0])
    assert payload == {"type": "DOWN", "actor": target, "status": "completed"}

    # monitor row consumed — re-emit is a no-op
    assert await sqlite_storage.emit_actor_down_for_terminated() == 0

    await sqlite_storage.monitor_actor(watcher, target, "mon:wf-watcher")
    await sqlite_storage.unmonitor_actor(watcher, target)
    assert await sqlite_storage.emit_actor_down_for_terminated() == 0


@pytest.mark.asyncio
async def test_name_freed_on_terminal(sqlite_storage, create_test_instance):
    watcher = await create_test_instance("wf-w", "wf", {})
    target = await create_test_instance("wf-t", "wf", {})

    await sqlite_storage.register_actor_name("named", target)
    await sqlite_storage.monitor_actor(watcher, target, "mon:wf-w")
    await sqlite_storage.update_instance_status(target, "failed")
    await sqlite_storage.emit_actor_down_for_terminated()

    assert await sqlite_storage.whereis_actor("named") is None
