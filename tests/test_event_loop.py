"""Edda runs on the event loop the server chose; the WSGI adapter uses uvloop."""

import asyncio
import time
import warnings
from collections.abc import Callable
from unittest.mock import patch
from wsgiref.util import setup_testing_defaults

import uvloop

from edda import EddaApp
from edda.wsgi import create_wsgi_app


def make_app(tmp_path, name: str) -> EddaApp:
    return EddaApp(service_name=name, db_url=f"sqlite:///{tmp_path / f'{name}.db'}")


def wait_until(condition: Callable[[], bool], what: str) -> None:
    deadline = time.monotonic() + 5
    while not condition():
        assert time.monotonic() < deadline, what
        time.sleep(0.01)


async def test_initialize_does_not_set_the_event_loop_policy(tmp_path):
    """initialize() runs on the loop it is awaited on, so a policy set there
    changed only loops created later in the process."""
    app = make_app(tmp_path, "loop-policy")
    with patch.object(asyncio, "set_event_loop_policy", create=True) as set_policy:
        await app.initialize()
    try:
        set_policy.assert_not_called()
    finally:
        await app.shutdown()


async def test_initialize_emits_no_event_loop_policy_deprecation(tmp_path):
    """The event loop policy API is deprecated since Python 3.14."""
    app = make_app(tmp_path, "loop-policy-warning")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        await app.initialize()
    await app.shutdown()
    messages = [str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)]
    assert [m for m in messages if "EventLoopPolicy" in m or "event_loop_policy" in m] == []


def test_create_wsgi_app_serves_requests_on_a_uvloop_loop(tmp_path):
    """a2wsgi runs the app on a loop in a background thread: create_wsgi_app
    gives it a uvloop loop instead of one made from the event loop policy."""
    app = make_app(tmp_path, "wsgi-loop")
    wsgi_app = create_wsgi_app(app)
    loop = wsgi_app.loop
    try:
        assert isinstance(loop, uvloop.Loop)
        wait_until(loop.is_running, "the loop thread did not start")

        environ: dict[str, object] = {}
        setup_testing_defaults(environ)
        statuses: list[str] = []
        body = b"".join(
            wsgi_app(environ, lambda status, headers, exc_info=None: statuses.append(status))
        )
        # An empty GET is not a CloudEvent: the request went through the loop
        # and came back as 400.
        assert len(statuses) == 1 and statuses[0].startswith("400 "), (statuses, body)
    finally:
        asyncio.run_coroutine_threadsafe(app.shutdown(), loop).result(timeout=30)
        loop.call_soon_threadsafe(loop.stop)
        wait_until(lambda: not loop.is_running(), "the loop did not stop")
        loop.close()
