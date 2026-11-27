"""Settling tasks: unknown IDs, and task notices only after the commit."""

import pytest

from edda.exceptions import TaskConflictError, TaskNotFoundError
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage

pytestmark = [pytest.mark.asyncio, pytest.mark.multidb]


async def test_settling_an_unknown_task_is_not_found(sqlite_storage, create_test_instance):
    owner = await create_test_instance("wf-settle", "wf", {})
    with pytest.raises(TaskNotFoundError):
        await sqlite_storage.complete_task("no-such-task", owner, None, failed=False)
    with pytest.raises(TaskNotFoundError):
        await sqlite_storage.complete_task("no-such-task", owner, None, failed=True)

    await sqlite_storage.post_task("t-open", "settle", "{}")
    with pytest.raises(TaskConflictError):  # exists, but nobody claimed it
        await sqlite_storage.complete_task("t-open", owner, None, failed=False)


async def test_task_notices_wait_for_the_commit(sqlite_storage, monkeypatch):
    in_transaction_at_call: list[bool] = []
    send = sqlite_storage._send_notify

    async def spy(channel, payload):
        in_transaction_at_call.append(sqlite_storage.in_transaction())
        await send(channel, payload)

    monkeypatch.setattr(SQLAlchemyStorage, "_notify_enabled", property(lambda self: True))
    monkeypatch.setattr(sqlite_storage, "_send_notify", spy)

    await sqlite_storage.begin_transaction()
    await sqlite_storage.post_task("t-commit", "notice", "{}")
    assert in_transaction_at_call == [True]  # deferred
    await sqlite_storage.commit_transaction()
    assert in_transaction_at_call == [True, False]  # sent once the task is visible

    in_transaction_at_call.clear()
    await sqlite_storage.begin_transaction()
    await sqlite_storage.post_task("t-rollback", "notice", "{}")
    await sqlite_storage.rollback_transaction()
    assert in_transaction_at_call == [True]  # never sent
