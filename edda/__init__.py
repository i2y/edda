"""
Edda Framework - CloudEvents-native Durable Execution framework.

Example:
    >>> import uvloop
    >>> from edda import EddaApp, workflow, activity, wait_event, wait_timer
    >>>
    >>> uvloop.install()
    >>>
    >>> app = EddaApp(
    ...     service_name="order-service",
    ...     db_url="sqlite:///workflow.db",
    ...     outbox_enabled=True
    ... )
"""

from edda.activity import activity
from edda.app import EddaApp
from edda.compensation import compensation, on_failure, register_compensation
from edda.context import WorkflowContext
from edda.events import ReceivedEvent, send_event, wait_event, wait_timer, wait_until
from edda.hooks import HooksBase, WorkflowHooks
from edda.outbox import OutboxRelayer, send_event_transactional
from edda.workflow import workflow

__version__ = "0.1.0"

__all__ = [
    "EddaApp",
    "workflow",
    "activity",
    "WorkflowContext",
    "ReceivedEvent",
    "wait_event",
    "wait_timer",
    "wait_until",
    "send_event",
    "compensation",
    "register_compensation",
    "on_failure",  # Already exported, just confirming it's in __all__
    "OutboxRelayer",
    "send_event_transactional",
    "WorkflowHooks",
    "HooksBase",
]
