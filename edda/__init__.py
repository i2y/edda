"""
Edda Framework - CloudEvents-native Durable Execution framework.

Example:
    >>> from edda import EddaApp, workflow, activity, wait_event, sleep
    >>>
    >>> app = EddaApp(
    ...     service_name="order-service",
    ...     db_url="sqlite:///workflow.db",
    ...     outbox_enabled=True
    ... )
"""

import importlib.metadata

from edda.activity import activity
from edda.app import EddaApp
from edda.channels import (
    # Channel-based messaging (new unified API)
    ChannelMessage,
    EventTimeoutError,
    ReceivedEvent,
    publish,
    receive,
    send_event,
    send_to,
    sleep,
    sleep_until,
    subscribe,
    unsubscribe,
    wait_event,
    wait_timer,
    wait_until,
)
from edda.compensation import compensation, on_failure, register_compensation
from edda.context import WorkflowContext
from edda.exceptions import (
    ActorNameTakenError,
    CursorNotFoundError,
    InvalidArgumentError,
    NotOwnerError,
    RetryExhaustedError,
    RunAbortedError,
    TerminalError,
)
from edda.hooks import HooksBase, WorkflowHooks
from edda.outbox import OutboxRelayer, send_event_transactional
from edda.retry import RetryPolicy
from edda.workflow import workflow
from edda.wsgi import create_wsgi_app

try:
    __version__ = importlib.metadata.version("edda-framework")
except importlib.metadata.PackageNotFoundError:  # imported from a source tree that isn't installed
    __version__ = "0.0.0"

__all__ = [
    # Core
    "EddaApp",
    "workflow",
    "activity",
    "WorkflowContext",
    # Channel-based Messaging (Erlang mailbox-style)
    "ChannelMessage",
    "subscribe",
    "unsubscribe",
    "receive",
    "publish",
    "send_to",
    # CloudEvents
    "ReceivedEvent",
    "wait_event",
    "send_event",
    "EventTimeoutError",
    # Timer Functions
    "sleep",
    "sleep_until",
    "wait_timer",  # Backward compatibility alias for sleep
    "wait_until",  # Backward compatibility alias for sleep_until
    # Compensation
    "compensation",
    "register_compensation",
    "on_failure",
    # Outbox
    "OutboxRelayer",
    "send_event_transactional",
    # Hooks
    "WorkflowHooks",
    "HooksBase",
    # Retry
    "RetryPolicy",
    "RetryExhaustedError",
    "TerminalError",
    "ActorNameTakenError",
    "NotOwnerError",
    "CursorNotFoundError",
    "InvalidArgumentError",
    "RunAbortedError",
    # WSGI
    "create_wsgi_app",
]
