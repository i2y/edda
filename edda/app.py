"""
Main application module for Edda framework.

This module provides the EddaApp class, which is an ASGI/WSGI compatible
application for handling CloudEvents and executing workflows.
"""

import asyncio
import json
import logging
import random
import sys
from collections.abc import Callable
from typing import Any

import uvloop
from cloudevents.exceptions import GenericException as CloudEventsException
from cloudevents.http import from_http
from sqlalchemy.ext.asyncio import create_async_engine

from edda import workflow
from edda.hooks import WorkflowHooks
from edda.locking import auto_resume_stale_workflows_periodically, generate_worker_id
from edda.outbox.relayer import OutboxRelayer
from edda.replay import ReplayEngine
from edda.retry import RetryPolicy
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage

logger = logging.getLogger(__name__)


class EddaApp:
    """
    ASGI/WSGI compatible workflow application with distributed execution support.

    This is the main entry point for the Edda framework. It handles:
    - CloudEvents HTTP endpoint
    - Event routing and workflow triggering
    - Distributed locking and coordination
    - Storage management
    """

    def __init__(
        self,
        service_name: str,
        db_url: str,
        outbox_enabled: bool = False,
        broker_url: str | None = None,
        hooks: WorkflowHooks | None = None,
        default_retry_policy: "RetryPolicy | None" = None,
        message_retention_days: int = 7,
        # Connection pool settings (ignored for SQLite)
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        pool_pre_ping: bool = True,
    ):
        """
        Initialize Edda application.

        Args:
            service_name: Service name for distributed execution (e.g., "order-service")
            db_url: Database URL (e.g., "sqlite:///workflow.db")
            outbox_enabled: Enable transactional outbox pattern
            broker_url: Broker URL for outbox publishing. Required if outbox_enabled=True.
            hooks: Optional WorkflowHooks implementation for observability
            default_retry_policy: Default retry policy for all activities.
                                 If None, uses DEFAULT_RETRY_POLICY (5 attempts, exponential backoff).
                                 Can be overridden per-activity using @activity(retry_policy=...).
            message_retention_days: Number of days to retain channel messages before automatic cleanup.
                                   Defaults to 7 days. Messages older than this will be deleted
                                   by a background task running every hour.
            pool_size: Number of connections to keep open in the pool (default: 5).
                      Ignored for SQLite. For production, consider 20+.
            max_overflow: Maximum number of connections to create above pool_size (default: 10).
                         Ignored for SQLite. For production, consider 40+.
            pool_timeout: Seconds to wait for a connection from the pool (default: 30).
                         Ignored for SQLite.
            pool_recycle: Seconds before a connection is recycled (default: 3600).
                         Helps prevent stale connections. Ignored for SQLite.
            pool_pre_ping: If True, test connections before use (default: True).
                          Helps detect disconnected connections. Ignored for SQLite.
        """
        self.db_url = db_url
        self.service_name = service_name
        self.outbox_enabled = outbox_enabled
        self.broker_url = broker_url
        if self.outbox_enabled and not self.broker_url:
            raise ValueError("broker_url is required when outbox_enabled=True")
        self.hooks = hooks
        self.default_retry_policy = default_retry_policy
        self._message_retention_days = message_retention_days

        # Connection pool settings
        self._pool_size = pool_size
        self._max_overflow = max_overflow
        self._pool_timeout = pool_timeout
        self._pool_recycle = pool_recycle
        self._pool_pre_ping = pool_pre_ping

        # Generate unique worker ID for this process
        self.worker_id = generate_worker_id(service_name)

        # Initialize storage
        self.storage = self._create_storage(db_url)

        # Event handlers registry
        self.event_handlers: dict[str, list[Callable[..., Any]]] = {}

        # Replay engine (will be initialized in initialize())
        self.replay_engine: ReplayEngine | None = None

        # Outbox relayer (will be initialized if outbox_enabled)
        self.outbox_relayer: OutboxRelayer | None = None

        # Background tasks
        self._background_tasks: list[asyncio.Task[Any]] = []
        self._initialized = False

    def _create_storage(self, db_url: str) -> SQLAlchemyStorage:
        """
        Create storage backend from database URL.

        Supports SQLite, PostgreSQL, and MySQL via SQLAlchemy.

        Args:
            db_url: Database URL in SQLAlchemy format
                Examples:
                - SQLite: "sqlite:///saga.db" or "sqlite+aiosqlite:///saga.db"
                - PostgreSQL: "postgresql+asyncpg://user:pass@localhost/dbname"
                - MySQL: "mysql+aiomysql://user:pass@localhost/dbname"

        Returns:
            SQLAlchemyStorage instance
        """
        # Check if using SQLite (connection pool settings not applicable)
        is_sqlite = db_url.startswith("sqlite")

        # Convert plain sqlite:// URLs to use aiosqlite driver
        if db_url.startswith("sqlite:///"):
            db_url = db_url.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
        elif db_url == "sqlite:///:memory:" or db_url.startswith("sqlite:///:memory:"):
            db_url = "sqlite+aiosqlite:///:memory:"

        # Build engine kwargs
        engine_kwargs: dict[str, Any] = {
            "echo": False,  # Set to True for SQL logging
            "future": True,
        }

        # Add connection pool settings for non-SQLite databases
        if not is_sqlite:
            engine_kwargs.update(
                {
                    "pool_size": self._pool_size,
                    "max_overflow": self._max_overflow,
                    "pool_timeout": self._pool_timeout,
                    "pool_recycle": self._pool_recycle,
                    "pool_pre_ping": self._pool_pre_ping,
                }
            )

        # Create async engine
        engine = create_async_engine(db_url, **engine_kwargs)

        return SQLAlchemyStorage(engine)

    async def initialize(self) -> None:
        """
        Initialize the application.

        This should be called before the app starts receiving requests.
        """
        if self._initialized:
            return

        # Install uvloop for better performance
        # Python 3.12+ uses asyncio.set_event_loop_policy() instead of uvloop.install()
        if sys.version_info >= (3, 12):
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        else:
            uvloop.install()

        # Initialize storage
        await self.storage.initialize()

        # Initialize replay engine
        self.replay_engine = ReplayEngine(
            storage=self.storage,
            service_name=self.service_name,
            worker_id=self.worker_id,
            hooks=self.hooks,
            default_retry_policy=self.default_retry_policy,
        )

        # Set global replay engine for workflow decorator
        workflow.set_replay_engine(self.replay_engine)

        # Initialize outbox relayer if enabled
        if self.outbox_enabled:
            assert self.broker_url is not None  # Validated in __init__
            self.outbox_relayer = OutboxRelayer(
                storage=self.storage,
                broker_url=self.broker_url,
                poll_interval=1.0,
                max_retries=3,
                batch_size=10,
            )
            await self.outbox_relayer.start()

        # Auto-register all @workflow decorated workflows
        self._auto_register_workflows()

        # Start background tasks
        self._start_background_tasks()

        self._initialized = True

    async def shutdown(self) -> None:
        """
        Shutdown the application and cleanup resources.

        This should be called when the app is shutting down.
        """
        # Stop outbox relayer if enabled
        if self.outbox_relayer:
            await self.outbox_relayer.stop()

        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*self._background_tasks, return_exceptions=True)

        # Close storage
        await self.storage.close()

        self._initialized = False

    def _start_background_tasks(self) -> None:
        """Start background maintenance tasks."""
        # Task to cleanup stale locks and auto-resume workflows
        auto_resume_task = asyncio.create_task(
            auto_resume_stale_workflows_periodically(
                self.storage,
                self.replay_engine,
                self.worker_id,
                interval=60,  # Check every 60 seconds
            )
        )
        self._background_tasks.append(auto_resume_task)

        # Task to check expired timers and resume workflows
        timer_check_task = asyncio.create_task(
            self._check_expired_timers_periodically(interval=10)  # Check every 10 seconds
        )
        self._background_tasks.append(timer_check_task)

        # Task to check expired message subscriptions and fail workflows
        # Note: CloudEvents timeouts are also handled here since wait_event() uses wait_message()
        message_timeout_task = asyncio.create_task(
            self._check_expired_message_subscriptions_periodically(
                interval=10
            )  # Check every 10 seconds
        )
        self._background_tasks.append(message_timeout_task)

        # Task to resume workflows after message delivery (fast resumption)
        message_resume_task = asyncio.create_task(
            self._resume_running_workflows_periodically(interval=1)  # Check every 1 second
        )
        self._background_tasks.append(message_resume_task)

        # Task to cleanup old channel messages (orphaned messages)
        message_cleanup_task = asyncio.create_task(
            self._cleanup_old_messages_periodically(
                interval=3600,  # Check every 1 hour
                retention_days=self._message_retention_days,
            )
        )
        self._background_tasks.append(message_cleanup_task)

    def _auto_register_workflows(self) -> None:
        """
        Auto-register workflows with event_handler=True as CloudEvent handlers.

        Only workflows explicitly marked with @workflow(event_handler=True) will be
        auto-registered. For each eligible workflow, a default handler is registered that:
        1. Extracts data from CloudEvent
        2. Starts the workflow with data as kwargs

        Manual @app.on_event() registrations take precedence.
        """
        from edda.workflow import get_all_workflows

        for workflow_name, workflow_instance in get_all_workflows().items():
            # Only register if event_handler=True
            if not workflow_instance.event_handler:
                continue

            # Skip if already manually registered (manual takes precedence)
            if workflow_name not in self.event_handlers:
                self._register_default_workflow_handler(workflow_name, workflow_instance)

    def _register_default_workflow_handler(self, event_type: str, wf: Any) -> None:
        """
        Register a default CloudEvent handler for a workflow.

        The default handler extracts the CloudEvent data and passes it
        as kwargs to workflow.start(). If the CloudEvent contains
        traceparent/tracestate extension attributes (for distributed tracing),
        they are automatically injected into _trace_context.

        Args:
            event_type: CloudEvent type (same as workflow name)
            wf: Workflow instance to start when event is received
        """

        async def default_handler(event: Any) -> None:
            """Default handler that starts workflow with CloudEvent data."""
            # Extract data from CloudEvent
            data = event.get_data()

            # Extract trace context from CloudEvent extension attributes
            # (W3C Trace Context: traceparent, tracestate)
            trace_context: dict[str, str] = {}
            attrs = event.get_attributes()
            if "traceparent" in attrs:
                trace_context["traceparent"] = str(attrs["traceparent"])
            if "tracestate" in attrs:
                trace_context["tracestate"] = str(attrs["tracestate"])

            # Start workflow with data as kwargs
            if isinstance(data, dict):
                # Inject trace context if present
                if trace_context:
                    data = {**data, "_trace_context": trace_context}
                await wf.start(**data)
            else:
                # If data is not a dict, start without arguments
                # (trace context cannot be injected)
                await wf.start()

        # Register the handler
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        self.event_handlers[event_type].append(default_handler)

    def on_event(
        self, event_type: str, proto_type: type[Any] | None = None
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Decorator to register an event handler.

        Example:
            >>> @app.on_event("order.created")
            ... async def handle_order_created(event):
            ...     await order_workflow.start(...)

        Args:
            event_type: CloudEvent type to handle
            proto_type: Optional protobuf message type

        Returns:
            Decorator function
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            if event_type not in self.event_handlers:
                self.event_handlers[event_type] = []
            self.event_handlers[event_type].append(func)

            # Store proto_type if provided
            if proto_type is not None:
                func._proto_type = proto_type  # type: ignore[attr-defined]

            return func

        return decorator

    async def handle_cloudevent(self, event: Any, wait: bool = False) -> None:
        """
        Handle incoming CloudEvent.

        This will route the event to registered handlers and deliver events
        to waiting workflows.

        By default, handlers are executed as background tasks to avoid blocking
        the HTTP response. Set wait=True for synchronous execution (useful for testing).

        Args:
            event: CloudEvent instance
            wait: If True, wait for handlers to complete before returning.
                  If False (default), execute handlers as background tasks.
        """
        import asyncio

        event_type = event["type"]

        # Find handlers for this event type
        handlers = self.event_handlers.get(event_type, [])

        if wait:
            # Synchronous execution (for tests)
            for handler in handlers:
                await self._run_handler(handler, event, event_type)
            await self._deliver_event_to_waiting_workflows_safe(event)
        else:
            # Background execution (for production)
            for handler in handlers:
                asyncio.create_task(self._run_handler(handler, event, event_type))
            asyncio.create_task(self._deliver_event_to_waiting_workflows_safe(event))

    async def _run_handler(self, handler: Callable[..., Any], event: Any, event_type: str) -> None:
        """
        Run a CloudEvent handler with error handling.

        Args:
            handler: Event handler function
            event: CloudEvent instance
            event_type: Event type for logging
        """
        try:
            await handler(event)
        except Exception as e:
            logger.error("Error handling event %s: %s", event_type, e, exc_info=True)

    async def _deliver_event_to_waiting_workflows_safe(self, event: Any) -> None:
        """
        Deliver event to waiting workflows with error handling.

        Args:
            event: CloudEvent instance
        """
        try:
            await self._deliver_event_to_waiting_workflows(event)
        except Exception as e:
            logger.error("Error delivering event to waiting workflows: %s", e, exc_info=True)

    async def _deliver_event_to_waiting_workflows(self, event: Any) -> None:
        """
        Deliver CloudEvent to workflows waiting for this event type.

        This method supports two delivery patterns based on the 'eddainstanceid' extension:

        1. **Point-to-Point** (when 'eddainstanceid' is present):
           Delivers to a specific workflow instance only.

        2. **Pub/Sub** (when 'eddainstanceid' is absent):
           Delivers to ALL workflows waiting for this event type.

        Both patterns use the Channel-based Message Queue system for delivery:
        - Lock acquisition (Lock-First pattern)
        - History recording (ChannelMessageReceived)
        - Subscription cursor update (broadcast) or message deletion (competing)
        - Status update to 'running'
        - Lock release

        Workflow resumption is handled by background task (_resume_running_workflows_periodically).

        Args:
            event: CloudEvent instance
        """
        from edda.channels import publish

        event_type = event["type"]
        event_data = event.get_data()

        # Extract CloudEvents metadata with ce_ prefix
        # This allows ReceivedEvent to reconstruct CloudEvents attributes
        metadata = {
            "ce_type": event["type"],
            "ce_source": event["source"],
            "ce_id": event["id"],
            "ce_time": event.get("time"),
            "ce_datacontenttype": event.get("datacontenttype"),
            "ce_subject": event.get("subject"),
        }

        # Extract extension attributes (any attributes not in the standard set)
        standard_attrs = {
            "type",
            "source",
            "id",
            "time",
            "datacontenttype",
            "subject",
            "specversion",
            "data",
            "data_base64",
        }
        extensions = {k: v for k, v in event.get_attributes().items() if k not in standard_attrs}
        if extensions:
            metadata["ce_extensions"] = extensions

        # Check for eddainstanceid extension attribute for Point-to-Point delivery
        target_instance_id = extensions.get("eddainstanceid")

        if target_instance_id:
            # Point-to-Point: Deliver to specific instance only
            logger.debug(
                "Point-to-Point: Delivering '%s' to instance %s",
                event_type,
                target_instance_id,
            )

            try:
                await publish(
                    self.storage,
                    channel=event_type,
                    data=event_data,
                    metadata=metadata,
                    target_instance_id=target_instance_id,
                    worker_id=self.worker_id,
                )
                logger.debug(
                    "Published '%s' to channel (target: %s)",
                    event_type,
                    target_instance_id,
                )

            except Exception as e:
                logger.error(
                    "Error delivering to workflow %s: %s",
                    target_instance_id,
                    e,
                    exc_info=True,
                )

        else:
            # Pub/Sub: Deliver to ALL waiting instances
            logger.debug("Pub/Sub: Publishing '%s' to channel", event_type)

            try:
                message_id = await publish(
                    self.storage,
                    channel=event_type,
                    data=event_data,
                    metadata=metadata,
                    worker_id=self.worker_id,
                )
                logger.debug(
                    "Published '%s' to channel (message_id: %s)",
                    event_type,
                    message_id,
                )

            except Exception as e:
                logger.error(
                    "Error publishing to channel '%s': %s",
                    event_type,
                    e,
                    exc_info=True,
                )

    async def _check_expired_timers(self) -> None:
        """
        Check for expired timers and resume waiting workflows.

        This method:
        1. Finds timers that have expired
        2. Records timer expiration to workflow history
        3. Removes timer subscription
        4. Resumes the workflow

        Note:
            This is called periodically by a background task.
            Timer expiration is recorded to history to enable deterministic replay.
            During replay, wait_timer() will find this history entry and skip the wait.
        """
        # Find expired timers
        expired_timers = await self.storage.find_expired_timers()

        if not expired_timers:
            return  # No expired timers

        logger.debug("Found %d expired timer(s)", len(expired_timers))

        for timer in expired_timers:
            instance_id = timer["instance_id"]
            timer_id = timer["timer_id"]
            workflow_name = timer["workflow_name"]
            activity_id = timer.get("activity_id")

            if not activity_id:
                logger.warning("No activity_id in timer for %s, skipping", instance_id)
                continue

            # Note: find_expired_timers() already filters by status='waiting_for_timer'
            # and JOINs with workflow_instances, so no need for additional get_instance() call.
            # The lock mechanism below handles race conditions.

            # Distributed Coroutines: Acquire lock FIRST to prevent race conditions
            # This ensures only ONE pod processes this timer, even if multiple pods
            # check timers simultaneously
            lock_acquired = await self.storage.try_acquire_lock(
                instance_id, self.worker_id, timeout_seconds=300
            )

            if not lock_acquired:
                logger.debug(
                    "Another worker is processing %s, skipping (lock already held)",
                    instance_id,
                )
                continue

            try:
                logger.debug(
                    "Timer '%s' expired for workflow %s (activity_id: %s)",
                    timer_id,
                    instance_id,
                    activity_id,
                )

                # 1. Record timer expiration to history (allows deterministic replay)
                # During replay, wait_timer() will find this entry and skip the wait
                try:
                    await self.storage.append_history(
                        instance_id,
                        activity_id=activity_id,
                        event_type="TimerExpired",
                        event_data={
                            "result": None,
                            "timer_id": timer_id,
                            "expires_at": timer["expires_at"],
                        },
                    )
                except Exception as history_error:
                    # If history entry already exists (UNIQUE constraint), this timer was already
                    # processed by another worker in a multi-process environment.
                    # Skip workflow resumption to prevent duplicate processing.
                    logger.debug(
                        "History already exists for activity_id %s: %s",
                        activity_id,
                        history_error,
                    )
                    logger.debug(
                        "Timer '%s' was already processed by another worker, skipping",
                        timer_id,
                    )
                    continue

                # 2. Remove timer subscription
                await self.storage.remove_timer_subscription(instance_id, timer_id)

                # 3. Resume workflow (lock already held by this worker - distributed coroutine pattern)
                if self.replay_engine is None:
                    logger.error("Replay engine not initialized")
                    continue

                await self.replay_engine.resume_by_name(
                    instance_id, workflow_name, already_locked=True
                )

                logger.debug(
                    "Resumed workflow %s after timer '%s' expired",
                    instance_id,
                    timer_id,
                )

            except Exception as e:
                logger.error("Error resuming workflow %s: %s", instance_id, e, exc_info=True)

            finally:
                # Always release the lock, even if an error occurred
                await self.storage.release_lock(instance_id, self.worker_id)

    async def _check_expired_timers_periodically(self, interval: int = 10) -> None:
        """
        Background task to periodically check for expired timers.

        Args:
            interval: Check interval in seconds (default: 10)

        Note:
            This runs indefinitely until the application is shut down.
            The actual resume time may be slightly later than the specified
            duration depending on the check interval.
        """
        while True:
            try:
                await asyncio.sleep(interval)
                await self._check_expired_timers()
            except Exception as e:
                logger.error("Error in periodic timer check: %s", e, exc_info=True)

    async def _check_expired_message_subscriptions(self) -> None:
        """
        Check for message subscriptions that have timed out and fail those workflows.

        This method:
        1. Finds all message subscriptions where timeout_at <= now
        2. For each timeout, acquires workflow lock (Lock-First pattern)
        3. Records MessageTimeout to history
        4. Removes message subscription
        5. Fails the workflow with TimeoutError
        """
        # Find all expired message subscriptions
        expired = await self.storage.find_expired_message_subscriptions()

        if not expired:
            return

        logger.debug("Found %d expired message subscriptions", len(expired))

        for subscription in expired:
            instance_id = subscription["instance_id"]
            channel = subscription["channel"]
            timeout_at = subscription["timeout_at"]
            created_at = subscription["created_at"]

            # Lock-First pattern: Try to acquire the lock before processing
            # If we can't get the lock, another worker is processing this workflow
            lock_acquired = await self.storage.try_acquire_lock(instance_id, self.worker_id)
            if not lock_acquired:
                logger.debug(
                    "Could not acquire lock for workflow %s, skipping (another worker is processing)",
                    instance_id,
                )
                continue

            try:
                logger.debug(
                    "Message on channel '%s' timed out for workflow %s",
                    channel,
                    instance_id,
                )

                # Note: find_expired_message_subscriptions() JOINs with workflow_instances,
                # so we know the instance exists. No need for separate get_instance() call.

                # Get activity_id from the subscription (stored when wait_message was called)
                activity_id = subscription.get("activity_id")
                if not activity_id:
                    logger.warning(
                        "No activity_id in subscription for %s, skipping",
                        instance_id,
                    )
                    continue

                # 1. Record message timeout to history
                # This allows the workflow to see what happened during replay
                # Convert datetime to ISO string for JSON serialization
                from datetime import datetime as dt_type

                timeout_at_str = (
                    timeout_at.isoformat() if isinstance(timeout_at, dt_type) else str(timeout_at)
                )
                try:
                    await self.storage.append_history(
                        instance_id,
                        activity_id=activity_id,
                        event_type="MessageTimeout",
                        event_data={
                            "_error": True,
                            "error_type": "TimeoutError",
                            "error_message": f"Message on channel '{channel}' did not arrive within timeout",
                            "channel": channel,
                            "timeout_at": timeout_at_str,
                        },
                    )
                except Exception as history_error:
                    # If history entry already exists, this timeout was already processed
                    logger.debug(
                        "History already exists for activity_id %s: %s",
                        activity_id,
                        history_error,
                    )
                    logger.debug(
                        "Timeout for channel '%s' was already processed, skipping",
                        channel,
                    )
                    continue

                # 2. Remove message subscription
                await self.storage.remove_message_subscription(instance_id, channel)

                # 3. Fail the workflow with TimeoutError
                import traceback

                # Get timeout_seconds from timeout_at and created_at
                # Handle both datetime objects and ISO strings
                try:
                    timeout_dt = (
                        timeout_at
                        if isinstance(timeout_at, dt_type)
                        else dt_type.fromisoformat(str(timeout_at))
                    )
                    created_dt = (
                        created_at
                        if isinstance(created_at, dt_type)
                        else dt_type.fromisoformat(str(created_at))
                    )
                    # Calculate the original timeout duration (timeout_at - created_at)
                    timeout_seconds = int((timeout_dt - created_dt).total_seconds())
                except Exception:
                    timeout_seconds = 0  # Fallback

                error = TimeoutError(
                    f"Message on channel '{channel}' did not arrive within {timeout_seconds} seconds"
                )
                stack_trace = "".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                )

                # Update workflow status to failed with error details
                await self.storage.update_instance_status(
                    instance_id,
                    "failed",
                    {
                        "error_message": str(error),
                        "error_type": "TimeoutError",
                        "stack_trace": stack_trace,
                    },
                )

                logger.debug(
                    "Marked workflow %s as failed due to message timeout",
                    instance_id,
                )

            except Exception as e:
                logger.error("Error processing timeout for %s: %s", instance_id, e, exc_info=True)

            finally:
                # Always release the lock
                await self.storage.release_lock(instance_id, self.worker_id)

    async def _check_expired_message_subscriptions_periodically(self, interval: int = 10) -> None:
        """
        Background task to periodically check for expired message subscriptions.

        Args:
            interval: Check interval in seconds (default: 10)

        Note:
            This runs indefinitely until the application is shut down.
        """
        while True:
            try:
                await asyncio.sleep(interval)
                await self._check_expired_message_subscriptions()
            except Exception as e:
                logger.error("Error in periodic timeout check: %s", e, exc_info=True)

    async def _resume_running_workflows_periodically(self, interval: int = 1) -> None:
        """
        Background task to resume workflows that are ready to run.

        This provides fast resumption after message delivery. When deliver_message()
        sets a workflow's status to 'running' and releases the lock, this task
        will pick it up within 1 second and resume it.

        Uses adaptive backoff to reduce DB load when no workflows are ready:
        - When workflows are processed, uses base interval
        - When no workflows found, exponentially backs off up to 60 seconds
        - Always adds jitter to prevent thundering herd in multi-pod deployments

        Args:
            interval: Check interval in seconds (default: 1)
        """
        consecutive_empty = 0  # Track empty results for adaptive backoff
        while True:
            try:
                # Adaptive backoff: longer sleep when no work available
                jitter = random.uniform(0, interval * 0.3)
                if consecutive_empty > 0:
                    # Exponential backoff: 2s, 4s, 8s, 16s, 32s, max 60s
                    backoff = min(interval * (2 ** min(consecutive_empty, 5)), 60)
                else:
                    backoff = interval
                await asyncio.sleep(backoff + jitter)

                count = await self._resume_running_workflows()
                if count == 0:
                    consecutive_empty += 1
                else:
                    consecutive_empty = 0
            except Exception as e:
                consecutive_empty = 0  # Reset on error
                logger.error("Error in periodic resume check: %s", e, exc_info=True)

    async def _resume_running_workflows(self) -> int:
        """
        Find and resume workflows that are ready to run.

        Finds workflows with status='running' that don't have a lock,
        acquires a lock, and resumes them.

        Returns:
            Number of workflows successfully processed (lock acquired and resumed).
        """
        resumable = await self.storage.find_resumable_workflows()
        processed_count = 0

        for workflow_info in resumable:
            instance_id = workflow_info["instance_id"]
            workflow_name = workflow_info["workflow_name"]

            try:
                # Try to acquire lock (Lock-First pattern)
                lock_acquired = await self.storage.try_acquire_lock(instance_id, self.worker_id)
                if not lock_acquired:
                    # Another worker got it first, skip
                    continue

                try:
                    # Resume the workflow
                    if self.replay_engine is None:
                        logger.error("ReplayEngine not initialized, skipping %s", instance_id)
                        continue
                    await self.replay_engine.resume_by_name(
                        instance_id, workflow_name, already_locked=True
                    )
                    processed_count += 1
                finally:
                    # Always release lock
                    await self.storage.release_lock(instance_id, self.worker_id)

            except Exception as e:
                logger.error("Error resuming %s: %s", instance_id, e, exc_info=True)

        return processed_count

    async def _cleanup_old_messages_periodically(
        self, interval: int = 3600, retention_days: int = 7
    ) -> None:
        """
        Background task to periodically cleanup old channel messages.

        Messages older than `retention_days` are deleted to prevent the database
        from growing indefinitely with orphaned messages (messages that were
        published but never received by any subscriber).

        Uses system-level locking to ensure only one pod executes cleanup at a time.

        Args:
            interval: Cleanup interval in seconds (default: 3600 = 1 hour)
            retention_days: Number of days to retain messages (default: 7)

        Note:
            This runs indefinitely until the application is shut down.
        """
        while True:
            try:
                # Add jitter to prevent thundering herd in multi-pod deployments
                jitter = random.uniform(0, interval * 0.3)
                await asyncio.sleep(interval + jitter)

                # Try to acquire global lock for this task
                lock_acquired = await self.storage.try_acquire_system_lock(
                    lock_name="cleanup_old_messages",
                    worker_id=self.worker_id,
                    timeout_seconds=interval,
                )

                if not lock_acquired:
                    # Another pod is handling this task
                    continue

                try:
                    deleted_count = await self.storage.cleanup_old_channel_messages(retention_days)
                    if deleted_count > 0:
                        logger.info("Cleaned up %d old channel messages", deleted_count)
                finally:
                    await self.storage.release_system_lock("cleanup_old_messages", self.worker_id)
            except Exception as e:
                logger.error("Error cleaning up old messages: %s", e, exc_info=True)

    # -------------------------------------------------------------------------
    # ASGI Interface
    # -------------------------------------------------------------------------

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Any],
        send: Callable[[dict[str, Any]], Any],
    ) -> None:
        """
        ASGI interface.

        Args:
            scope: ASGI scope dictionary
            receive: Async function to receive messages
            send: Async function to send messages
        """
        # Initialize if not already done
        if not self._initialized:
            await self.initialize()

        if scope["type"] == "lifespan":
            await self._handle_lifespan(scope, receive, send)
        elif scope["type"] == "http":
            await self._handle_http(scope, receive, send)
        else:
            raise NotImplementedError(f"Unsupported scope type: {scope['type']}")

    async def _handle_lifespan(
        self,
        _scope: dict[str, Any],
        receive: Callable[[], Any],
        send: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Handle ASGI lifespan events."""
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await self.initialize()
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await self.shutdown()
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def _handle_http(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Any],
        send: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Handle HTTP request (CloudEvents and API endpoints)."""
        # Get request path and method
        path = scope.get("path", "/")
        method = scope.get("method", "GET")

        # Route to appropriate handler
        if path.startswith("/cancel/") and method == "POST":
            await self._handle_cancel_request(scope, receive, send)
        else:
            # Default: CloudEvents handler
            await self._handle_cloudevent_request(scope, receive, send)

    async def _handle_cloudevent_request(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Any],
        send: Callable[[dict[str, Any]], Any],
    ) -> None:
        """
        Handle CloudEvent HTTP request.

        CloudEvents HTTP Binding compliant responses:
        - 202 Accepted: Event accepted for async processing
        - 400 Bad Request: CloudEvents parsing/validation error (non-retryable)
        - 500 Internal Server Error: Internal error (retryable)
        """
        # Read request body
        body = b""
        while True:
            message = await receive()
            if message["type"] == "http.request":
                body += message.get("body", b"")
                if not message.get("more_body", False):
                    break

        # Parse and handle CloudEvent
        try:
            headers = {k.decode("latin1"): v.decode("latin1") for k, v in scope.get("headers", [])}

            # Create CloudEvent from HTTP request
            event = from_http(headers, body)

            # Handle the event (background task execution)
            await self.handle_cloudevent(event)

            # Success: 202 Accepted (async processing)
            status = 202
            response_body: dict[str, Any] = {"status": "accepted"}

        except (ValueError, TypeError, KeyError, CloudEventsException) as e:
            # CloudEvents parsing/validation error: 400 Bad Request (non-retryable)
            status = 400
            response_body = {
                "error": str(e),
                "error_type": type(e).__name__,
                "retryable": False,
            }

        except Exception as e:
            # Internal error: 500 Internal Server Error (retryable)
            status = 500
            response_body = {
                "error": str(e),
                "error_type": type(e).__name__,
                "retryable": True,
            }

        # Send response (only once, at the end)
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [[b"content-type", b"application/json"]],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": json.dumps(response_body).encode("utf-8"),
            }
        )

    async def _handle_cancel_request(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Any],
        send: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Handle workflow cancellation request."""
        # Extract instance_id from path: /cancel/{instance_id}
        path = scope.get("path", "")
        instance_id = path.split("/cancel/")[-1]

        # Determine response (default: error)
        status = 500
        response_body: dict[str, Any] = {"error": "Unknown error"}

        if not instance_id:
            status = 400
            response_body = {"error": "Missing instance_id"}
        else:
            # Consume request body (even if we don't use it)
            while True:
                message = await receive()
                if message["type"] == "http.request" and not message.get("more_body", False):
                    break

            # Try to cancel the workflow
            try:
                if self.replay_engine is None:
                    raise RuntimeError("Replay engine not initialized")

                success = await self.replay_engine.cancel_workflow(
                    instance_id=instance_id, cancelled_by="api_user"
                )

                if success:
                    # Successfully cancelled
                    status = 200
                    response_body = {"status": "cancelled", "instance_id": instance_id}
                else:
                    # Could not cancel (not found or already completed/failed)
                    status = 400
                    response_body = {
                        "error": "Cannot cancel workflow (not found or already completed/failed/cancelled)"
                    }

            except Exception as e:
                # Internal error - log detailed traceback
                logger.error("Error cancelling workflow %s: %s", instance_id, e, exc_info=True)

                status = 500
                response_body = {"error": str(e), "type": type(e).__name__}

        # Send response (only once, at the end)
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [[b"content-type", b"application/json"]],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": json.dumps(response_body).encode("utf-8"),
            }
        )
