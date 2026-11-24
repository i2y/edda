"""Task primitive + cursor-based channel reads (Vercel Board-style)."""

import pytest

from edda.exceptions import TaskConflictError


@pytest.mark.asyncio
async def test_task_claim_and_complete(sqlite_storage, create_test_instance):
    w1 = await create_test_instance("wf-w1", "wf", {})
    w2 = await create_test_instance("wf-w2", "wf", {})

    await sqlite_storage.post_task("t1", "tasks:x", '{"n":1}')
    await sqlite_storage.post_task("t2", "tasks:x", '{"n":2}')
    await sqlite_storage.post_task("t3", "tasks:y", '{"n":3}')

    task = await sqlite_storage.claim_task("tasks:x", w1)
    assert task is not None
    assert task["id"] == "t1"
    assert task["state"] == "claimed"
    assert task["claimed_by"] == w1

    task2 = await sqlite_storage.claim_task("tasks:x", w2)
    assert task2 is not None and task2["id"] == "t2"

    # none left on tasks:x
    assert await sqlite_storage.claim_task("tasks:x", "wf-w3") is None

    # only claimant may complete
    with pytest.raises(TaskConflictError):
        await sqlite_storage.complete_task("t1", w2, '{"ok":true}', failed=False)
    await sqlite_storage.complete_task("t1", w1, '{"ok":true}', failed=False)
    await sqlite_storage.complete_task("t2", w2, '{"err":"x"}', failed=True)

    tasks = await sqlite_storage.list_tasks("tasks:x")
    assert [t["state"] for t in tasks] == ["done", "failed"]
    assert await sqlite_storage.list_tasks("tasks:x", "open") == []
    assert len(await sqlite_storage.list_tasks("tasks:y", "open")) == 1


@pytest.mark.asyncio
async def test_channel_messages_since(sqlite_storage, create_test_instance):
    await create_test_instance("wf-cur", "wf", {})

    ids = [await sqlite_storage.publish_to_channel("cur", {"i": n}) for n in (1, 2, 3)]
    all_msgs = await sqlite_storage.channel_messages_since("cur", None)
    assert len(all_msgs) == 3
    assert all_msgs[0]["message_id"] == ids[0]

    rest = await sqlite_storage.channel_messages_since("cur", ids[0])
    assert len(rest) == 2
    assert rest[0]["message_id"] == ids[1]

    end = await sqlite_storage.channel_messages_since("cur", ids[2])
    assert end == []

    # unknown cursor behaves like empty (reads all)
    unk = await sqlite_storage.channel_messages_since("cur", "no-such-id")
    assert len(unk) == 3


@pytest.mark.asyncio
async def test_task_event_stream_and_get_task(sqlite_storage, create_test_instance):
    w1 = await create_test_instance("wf-w1", "wf", {})

    await sqlite_storage.post_task("t1", "tasks:x", '{"n":1}')
    msgs = await sqlite_storage.channel_messages_since("tasks:x:tasks", None)
    assert len(msgs) == 1
    import json as _json

    assert _json.loads(msgs[0]["data"]) == {
        "type": "task_posted",
        "task_id": "t1",
        "channel": "tasks:x",
    }

    got = await sqlite_storage.get_task("t1")
    assert got is not None and got["state"] == "open"
    assert await sqlite_storage.get_task("nope") is None

    task = await sqlite_storage.claim_task("tasks:x", w1)
    assert task["id"] == "t1"
    await sqlite_storage.complete_task("t1", w1, '{"ok":true}', failed=False)

    msgs = await sqlite_storage.channel_messages_since("tasks:x:tasks", msgs[0]["message_id"])
    assert len(msgs) == 2
    assert _json.loads(msgs[0]["data"])["type"] == "task_claimed"
    assert _json.loads(msgs[1]["data"]) == {
        "type": "task_state",
        "task_id": "t1",
        "state": "done",
    }


@pytest.mark.asyncio
async def test_reclaim_stale_tasks(sqlite_storage, create_test_instance):
    await create_test_instance("wf-r1", "wf", {})
    await create_test_instance("wf-r2", "wf", {})

    await sqlite_storage.post_task("tr1", "tasks:r", '{}')
    await sqlite_storage.post_task("tr2", "tasks:r", '{}')
    assert (await sqlite_storage.claim_task("tasks:r", "wf-r1"))["id"] == "tr1"
    assert (await sqlite_storage.claim_task("tasks:r", "wf-r2"))["id"] == "tr2"

    # attempts incremented on claim
    assert (await sqlite_storage.get_task("tr1"))["attempts"] == 1

    # fresh claims are not stale
    assert await sqlite_storage.reclaim_stale_tasks(3600) == 0

    # backdate tr1's claimed_at to simulate an orphaned claim
    from sqlalchemy import text

    session = sqlite_storage._get_session_for_operation()
    async with sqlite_storage._session_scope(session) as session:
        await session.execute(
            text("UPDATE actor_tasks SET claimed_at = :c WHERE id = 'tr1'"),
            {"c": "2000-01-01 00:00:00"},
        )
        await session.commit()

    assert await sqlite_storage.reclaim_stale_tasks(3600) == 1
    t = await sqlite_storage.get_task("tr1")
    assert t["state"] == "open" and t["claimed_by"] is None
    # fresh claim untouched
    t2 = await sqlite_storage.get_task("tr2")
    assert t2["state"] == "claimed" and t2["claimed_by"] == "wf-r2"
