"""Demo to manually verify issue #7 fix in the Edda viewer.

Defines a workflow that uses an f-string event_type in wait_event — this is
the exact pattern that broke Mermaid rendering before the fix in
edda/visualizer/mermaid_generator.py.

Two modes of use:

1. Bootstrap the DB with one paused instance (run once):
       uv run python fstring_demo.py

2. Run as an ASGI server so the viewer's Cancel button can reach it:
       uv run tsuno fstring_demo:application --bind 127.0.0.1:8001

Then in another terminal launch the viewer:
       just viewer fstring_demo.db 8080 "--import-module fstring_demo"
"""

import asyncio
from typing import Any

from edda import EddaApp, WorkflowContext, activity, wait_event, workflow

app = EddaApp(
    service_name="fstring-demo",
    db_url="sqlite:///fstring_demo.db",
)


@activity
async def init_session(_ctx: WorkflowContext, user_id: str) -> dict[str, Any]:
    return {"user_id": user_id, "session_token": f"tok-{user_id}"}


@workflow
async def fstring_session_workflow(ctx: WorkflowContext, user_id: str) -> dict[str, Any]:
    """Workflow waiting for a per-user continuation event.

    The event_type is built dynamically from user_id, so the AST analyzer
    extracts the literal f-string source ``f'workflow.continue:{user_id}'``
    which contains ``{`` / ``}``. Without the fix in this branch, the
    viewer's Mermaid renderer chokes on those braces inside the hexagon
    node syntax ``id{{label}}``.
    """
    session = await init_session(ctx, user_id)

    event = await wait_event(
        ctx,
        event_type=f"workflow.continue:{user_id}",
        timeout_seconds=86400,  # 1 day — long enough to interact with the viewer manually
    )
    return {"session": session, "resumed": True, "data": event.data}


# ASGI entry point. `tsuno fstring_demo:application` exposes /cancel/{id}
# (and CloudEvent intake), which is what the viewer's Cancel button hits.
application = app


async def _bootstrap() -> None:
    await app.initialize()
    instance_id = await fstring_session_workflow.start(user_id="alice")

    print()
    print("=" * 70)
    print(f"  Started workflow instance: {instance_id}")
    print("  Status: waiting at wait_event(event_type=f'workflow.continue:alice')")
    print("=" * 70)
    print()
    print("Next steps:")
    print("  1. Start the ASGI server (so Cancel button works):")
    print("       uv run tsuno fstring_demo:application --bind 127.0.0.1:8001")
    print("  2. Start the viewer in another terminal:")
    print('       just viewer fstring_demo.db 8080 "--import-module fstring_demo"')
    print("  3. Open http://localhost:8080 and inspect the instance.")
    print()

    await app.shutdown()


if __name__ == "__main__":
    asyncio.run(_bootstrap())
