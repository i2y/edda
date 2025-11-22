"""
Main application module for Edda framework.

This module provides the EddaApp class, which is an ASGI/WSGI compatible
application for handling CloudEvents and executing workflows.
"""

import asyncio
import json
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
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


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
        broker_url: str = "http://broker-ingress.knative-eventing.svc.cluster.local/default/default",
        hooks: WorkflowHooks | None = None,
        default_retry_policy: "RetryPolicy | None" = None,
    ):
        """
        Initialize Edda application.

        Args:
            service_name: Service name for distributed execution (e.g., "order-service")
            db_url: Database URL (e.g., "sqlite:///workflow.db")
            outbox_enabled: Enable transactional outbox pattern
            broker_url: Knative Broker URL for outbox publishing
            hooks: Optional WorkflowHooks implementation for observability
            default_retry_policy: Default retry policy for all activities.
                                 If None, uses DEFAULT_RETRY_POLICY (5 attempts, exponential backoff).
                                 Can be overridden per-activity using @activity(retry_policy=...).
        """
        self.db_url = db_url
        self.service_name = service_name
        self.outbox_enabled = outbox_enabled
        self.broker_url = broker_url
        self.hooks = hooks
        self.default_retry_policy = default_retry_policy

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
        # Convert plain sqlite:// URLs to use aiosqlite driver
        if db_url.startswith("sqlite:///"):
            db_url = db_url.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
        elif db_url == "sqlite:///:memory:" or db_url.startswith("sqlite:///:memory:"):
            db_url = "sqlite+aiosqlite:///:memory:"

        # Create async engine
        engine = create_async_engine(
            db_url,
            echo=False,  # Set to True for SQL logging
            future=True,
        )

        return SQLAlchemyStorage(engine)

    async def initialize(self) -> None:
        """
        Initialize the application.

        This should be called before the app starts receiving requests.
        """
        if self._initialized:
            return

        # Install uvloop for better performance
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
                interval=60,  # Check every 60 seconds
                timeout_seconds=300,  # Clean locks older than 5 minutes
            )
        )
        self._background_tasks.append(auto_resume_task)

        # Task to check expired timers and resume workflows
        timer_check_task = asyncio.create_task(
            self._check_expired_timers_periodically(interval=10)  # Check every 10 seconds
        )
        self._background_tasks.append(timer_check_task)

        # Task to check expired event timeouts and fail workflows
        event_timeout_task = asyncio.create_task(
            self._check_expired_event_timeouts_periodically(interval=10)  # Check every 10 seconds
        )
        self._background_tasks.append(event_timeout_task)

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
        as kwargs to workflow.start().

        Args:
            event_type: CloudEvent type (same as workflow name)
            wf: Workflow instance to start when event is received
        """

        async def default_handler(event: Any) -> None:
            """Default handler that starts workflow with CloudEvent data."""
            # Extract data from CloudEvent
            data = event.get_data()

            # Start workflow with data as kwargs
            if isinstance(data, dict):
                await wf.start(**data)
            else:
                # If data is not a dict, start without arguments
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
            # Log error (in a real implementation, use proper logging)
            print(f"Error handling event {event_type}: {e}")
            import traceback

            traceback.print_exc()

    async def _deliver_event_to_waiting_workflows_safe(self, event: Any) -> None:
        """
        Deliver event to waiting workflows with error handling.

        Args:
            event: CloudEvent instance
        """
        try:
            await self._deliver_event_to_waiting_workflows(event)
        except Exception as e:
            print(f"Error delivering event to waiting workflows: {e}")
            import traceback

            traceback.print_exc()

    async def _deliver_event_to_waiting_workflows(self, event: Any) -> None:
        """
        Deliver event to workflows waiting for this event type.

        This method:
        1. Finds workflows waiting for the event type
        2. Records event data to workflow history
        3. Removes event subscription
        4. Resumes the workflow

        Args:
            event: CloudEvent instance
        """
        event_type = event["type"]
        event_data = event.get_data()

        # Extract CloudEvents metadata
        event_metadata = {
            "type": event["type"],
            "source": event["source"],
            "id": event["id"],
            "time": event.get("time"),
            "datacontenttype": event.get("datacontenttype"),
            "subject": event.get("subject"),
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

        # Find workflows waiting for this event type
        waiting_instances = await self.storage.find_waiting_instances(event_type)

        if not waiting_instances:
            return  # No workflows waiting for this event

        print(
            f"[EventDelivery] Found {len(waiting_instances)} workflow(s) waiting for '{event_type}'"
        )

        for subscription in waiting_instances:
            instance_id = subscription["instance_id"]

            # Get workflow instance
            instance = await self.storage.get_instance(instance_id)
            if not instance:
                print(f"[EventDelivery] Warning: Instance {instance_id} not found, skipping")
                continue

            # Check if instance is still waiting
            if instance.get("status") != "waiting_for_event":
                print(
                    f"[EventDelivery] Warning: Instance {instance_id} "
                    f"status is '{instance.get('status')}', expected 'waiting_for_event', skipping"
                )
                continue

            # Get activity_id from the subscription (stored when wait_event was called)
            activity_id = subscription.get("activity_id")
            if not activity_id:
                print(f"[EventDelivery] Warning: No activity_id in subscription for {instance_id}, skipping")
                continue

            workflow_name = instance["workflow_name"]

            # Distributed Coroutines: Acquire lock FIRST to prevent race conditions
            # This ensures only ONE pod processes this event, even if multiple pods
            # receive the event simultaneously
            lock_acquired = await self.storage.try_acquire_lock(
                instance_id, self.worker_id, timeout_seconds=300
            )

            if not lock_acquired:
                print(
                    f"[EventDelivery] Another worker is processing {instance_id}, skipping "
                    "(distributed coroutine - lock already held)"
                )
                continue

            try:
                print(
                    f"[EventDelivery] Delivering event to workflow {instance_id} (activity_id: {activity_id})"
                )

                # 1. Record event data and metadata to history
                try:
                    await self.storage.append_history(
                        instance_id,
                        activity_id=activity_id,
                        event_type="EventReceived",
                        event_data={
                            "payload": event_data,
                            "metadata": event_metadata,
                            "extensions": extensions,
                        },
                    )
                except Exception as history_error:
                    # If history entry already exists (UNIQUE constraint), this event was already
                    # delivered by another worker in a multi-process environment.
                    # Skip workflow resumption to prevent duplicate processing.
                    print(
                        f"[EventDelivery] History already exists for activity_id {activity_id}: {history_error}"
                    )
                    print(
                        f"[EventDelivery] Event '{event_type}' was already delivered by another worker, skipping"
                    )
                    continue

                # 2. Remove event subscription
                await self.storage.remove_event_subscription(instance_id, event_type)

                # 3. Resume workflow (lock already held by this worker - distributed coroutine pattern)
                if self.replay_engine is None:
                    print("[EventDelivery] Error: Replay engine not initialized")
                    continue

                await self.replay_engine.resume_by_name(
                    instance_id, workflow_name, already_locked=True
                )

                print(
                    f"[EventDelivery] ✅ Resumed workflow {instance_id} after receiving '{event_type}'"
                )

            except Exception as e:
                print(f"[EventDelivery] ❌ Error resuming workflow {instance_id}: {e}")
                import traceback

                traceback.print_exc()

            finally:
                # Always release the lock, even if an error occurred
                await self.storage.release_lock(instance_id, self.worker_id)

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

        print(f"[TimerCheck] Found {len(expired_timers)} expired timer(s)")

        for timer in expired_timers:
            instance_id = timer["instance_id"]
            timer_id = timer["timer_id"]
            workflow_name = timer["workflow_name"]
            activity_id = timer.get("activity_id")

            if not activity_id:
                print(f"[TimerCheck] Warning: No activity_id in timer for {instance_id}, skipping")
                continue

            # Get workflow instance
            instance = await self.storage.get_instance(instance_id)
            if not instance:
                print(f"[TimerCheck] Warning: Instance {instance_id} not found, skipping")
                continue

            # Check if instance is still waiting for timer
            if instance.get("status") != "waiting_for_timer":
                print(
                    f"[TimerCheck] Warning: Instance {instance_id} "
                    f"status is '{instance.get('status')}', expected 'waiting_for_timer', skipping"
                )
                continue

            # Distributed Coroutines: Acquire lock FIRST to prevent race conditions
            # This ensures only ONE pod processes this timer, even if multiple pods
            # check timers simultaneously
            lock_acquired = await self.storage.try_acquire_lock(
                instance_id, self.worker_id, timeout_seconds=300
            )

            if not lock_acquired:
                print(
                    f"[TimerCheck] Another worker is processing {instance_id}, skipping "
                    "(distributed coroutine - lock already held)"
                )
                continue

            try:
                print(
                    f"[TimerCheck] Timer '{timer_id}' expired for workflow {instance_id} (activity_id: {activity_id})"
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
                    print(
                        f"[TimerCheck] History already exists for activity_id {activity_id}: {history_error}"
                    )
                    print(
                        f"[TimerCheck] Timer '{timer_id}' was already processed by another worker, skipping"
                    )
                    continue

                # 2. Remove timer subscription
                await self.storage.remove_timer_subscription(instance_id, timer_id)

                # 3. Resume workflow (lock already held by this worker - distributed coroutine pattern)
                if self.replay_engine is None:
                    print("[TimerCheck] Error: Replay engine not initialized")
                    continue

                await self.replay_engine.resume_by_name(
                    instance_id, workflow_name, already_locked=True
                )

                print(
                    f"[TimerCheck] ✅ Resumed workflow {instance_id} after timer '{timer_id}' expired"
                )

            except Exception as e:
                print(f"[TimerCheck] ❌ Error resuming workflow {instance_id}: {e}")
                import traceback

                traceback.print_exc()

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
                print(f"[TimerCheck] Error in periodic timer check: {e}")
                import traceback

                traceback.print_exc()

    async def _check_expired_event_timeouts(self) -> None:
        """
        Check for event subscriptions that have timed out and fail those workflows.

        This method:
        1. Finds all event subscriptions where timeout_at <= now
        2. For each timeout, acquires workflow lock (Lock-First pattern)
        3. Records EventTimeout to history
        4. Removes event subscription
        5. Fails the workflow with EventTimeoutError
        """
        # Find all expired event subscriptions
        expired = await self.storage.find_expired_event_subscriptions()

        if not expired:
            return

        print(f"[EventTimeoutCheck] Found {len(expired)} expired event subscriptions")

        for subscription in expired:
            instance_id = subscription["instance_id"]
            event_type = subscription["event_type"]
            timeout_at = subscription["timeout_at"]
            created_at = subscription["created_at"]

            # Lock-First pattern: Try to acquire the lock before processing
            # If we can't get the lock, another worker is processing this workflow
            lock_acquired = await self.storage.try_acquire_lock(instance_id, self.worker_id)
            if not lock_acquired:
                print(
                    f"[EventTimeoutCheck] Could not acquire lock for workflow {instance_id}, skipping (another worker is processing)"
                )
                continue

            try:
                print(
                    f"[EventTimeoutCheck] Event '{event_type}' timed out for workflow {instance_id}"
                )

                # Get workflow instance
                instance = await self.storage.get_instance(instance_id)
                if not instance:
                    print(f"[EventTimeoutCheck] Workflow {instance_id} not found")
                    continue

                # Get activity_id from the subscription (stored when wait_event was called)
                activity_id = subscription.get("activity_id")
                if not activity_id:
                    print(f"[EventTimeoutCheck] Warning: No activity_id in subscription for {instance_id}, skipping")
                    continue

                # 1. Record event timeout to history
                # This allows the workflow to see what happened during replay
                try:
                    await self.storage.append_history(
                        instance_id,
                        activity_id=activity_id,
                        event_type="EventTimeout",
                        event_data={
                            "event_type": event_type,
                            "timeout_at": timeout_at,
                            "error_message": f"Event '{event_type}' did not arrive within timeout",
                        },
                    )
                except Exception as history_error:
                    # If history entry already exists, this timeout was already processed
                    print(
                        f"[EventTimeoutCheck] History already exists for activity_id {activity_id}: {history_error}"
                    )
                    print(
                        f"[EventTimeoutCheck] Timeout for '{event_type}' was already processed, skipping"
                    )
                    continue

                # 2. Remove event subscription
                await self.storage.remove_event_subscription(instance_id, event_type)

                # 3. Fail the workflow with EventTimeoutError
                # Create error details similar to workflow failure
                import traceback

                # Get timeout_seconds from timeout_at and created_at
                from datetime import datetime

                from edda.events import EventTimeoutError

                try:
                    timeout_dt = datetime.fromisoformat(timeout_at)
                    created_dt = datetime.fromisoformat(created_at)
                    # Calculate the original timeout duration (timeout_at - created_at)
                    timeout_seconds = int((timeout_dt - created_dt).total_seconds())
                except Exception:
                    timeout_seconds = 0  # Fallback

                error = EventTimeoutError(event_type, timeout_seconds)
                stack_trace = "".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                )

                # Update workflow status to failed with error details
                await self.storage.update_instance_status(
                    instance_id,
                    "failed",
                    {
                        "error_message": str(error),
                        "error_type": "EventTimeoutError",
                        "stack_trace": stack_trace,
                    },
                )

                print(
                    f"[EventTimeoutCheck] ✅ Marked workflow {instance_id} as failed due to event timeout"
                )

            except Exception as e:
                print(f"[EventTimeoutCheck] ❌ Error processing timeout for {instance_id}: {e}")
                import traceback

                traceback.print_exc()

            finally:
                # Always release the lock
                await self.storage.release_lock(instance_id, self.worker_id)

    async def _check_expired_event_timeouts_periodically(self, interval: int = 10) -> None:
        """
        Background task to periodically check for expired event timeouts.

        Args:
            interval: Check interval in seconds (default: 10)

        Note:
            This runs indefinitely until the application is shut down.
        """
        while True:
            try:
                await asyncio.sleep(interval)
                await self._check_expired_event_timeouts()
            except Exception as e:
                print(f"[EventTimeoutCheck] Error in periodic timeout check: {e}")
                import traceback

                traceback.print_exc()

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
                print(f"[Cancel] Error cancelling workflow {instance_id}: {e}")
                import traceback

                traceback.print_exc()

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
