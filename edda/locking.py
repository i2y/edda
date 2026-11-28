"""
Distributed locking utilities for Edda framework.

This module provides helper functions and context managers for working with
distributed locks in multi-pod deployments.
"""

import asyncio
import logging
import os
import random
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from typing import Any, TypeVar

from edda.storage.protocol import StorageProtocol

logger = logging.getLogger(__name__)

T = TypeVar("T")


def new_lock_identity(worker_id: str) -> str:
    """
    Generate an epoch-bearing lock-holder identity.

    Every lock acquisition epoch gets a fresh identity ``worker_id#<random>``,
    so a lease lost and re-acquired by the *same* worker is a different
    holder. All lock writes compare ``locked_by`` for exact equality, which
    makes them fenced writes with no schema change.

    Args:
        worker_id: Process-level worker identifier

    Returns:
        Epoch-scoped lock identity (``worker_id#<hex>``)
    """
    return f"{worker_id}#{uuid.uuid4().hex[:16]}"


def generate_worker_id(service_name: str) -> str:
    """
    Generate a unique worker ID for this process.

    The worker ID combines the service name, process ID, and a random UUID
    to ensure uniqueness across pods and restarts.

    Args:
        service_name: Name of the service (e.g., "order-service")

    Returns:
        Unique worker ID (e.g., "order-service-12345-a1b2c3d4")
    """
    pid = os.getpid()
    unique_id = uuid.uuid4().hex[:8]
    return f"{service_name}-{pid}-{unique_id}"


async def acquire_lock_with_retry(
    storage: StorageProtocol,
    instance_id: str,
    worker_id: str,
    max_retries: int = 3,
    retry_delay: float = 0.1,
    timeout_seconds: int = 30,
) -> bool:
    """
    Try to acquire lock with retries.

    This is useful in high-contention scenarios where multiple workers
    are trying to acquire the same lock simultaneously.

    Args:
        storage: Storage backend
        instance_id: Workflow instance to lock
        worker_id: Unique worker identifier
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        timeout_seconds: Lock timeout in seconds

    Returns:
        True if lock was acquired, False otherwise
    """
    for attempt in range(max_retries):
        if await storage.try_acquire_lock(instance_id, worker_id, timeout_seconds):
            return True

        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay)

    return False


async def ensure_lock_held(
    storage: StorageProtocol,
    instance_id: str,
    worker_id: str,
) -> None:
    """
    Verify that we still hold the lock for a workflow instance.

    Raises RuntimeError if the lock is not held by this worker.

    Args:
        storage: Storage backend
        instance_id: Workflow instance
        worker_id: Unique worker identifier

    Raises:
        RuntimeError: If lock is not held by this worker
    """
    instance = await storage.get_instance(instance_id)
    if instance is None:
        raise RuntimeError(f"Workflow instance {instance_id} not found")

    if instance.get("locked_by") != worker_id:
        raise RuntimeError(
            f"Lock lost for instance {instance_id}. "
            f"Current lock holder: {instance.get('locked_by')}"
        )


@asynccontextmanager
async def workflow_lock(
    storage: StorageProtocol,
    instance_id: str,
    worker_id: str,
    timeout_seconds: int = 300,
    refresh_interval: float | None = None,
) -> AsyncIterator[None]:
    """
    Context manager for acquiring and releasing workflow locks.

    Automatically refreshes the lock periodically if refresh_interval is provided.

    Example:
        >>> async with workflow_lock(storage, instance_id, worker_id):
        ...     # Execute workflow
        ...     pass

    Args:
        storage: Storage backend
        instance_id: Workflow instance to lock
        worker_id: Unique worker identifier
        timeout_seconds: Lock timeout in seconds
        refresh_interval: Optional interval for lock refresh (seconds)

    Yields:
        None (lock is held during context)

    Raises:
        LockNotAcquiredError: If another holder has the lock (a RuntimeError)
    """
    # Try to acquire lock
    acquired = await storage.try_acquire_lock(instance_id, worker_id, timeout_seconds)
    if not acquired:
        raise LockNotAcquiredError(f"Failed to acquire lock for instance {instance_id}")

    refresh_task: asyncio.Task[Any] | None = None
    stop_refresh = asyncio.Event()
    owner_task = asyncio.current_task()
    # Cancellation requests the task already carries; the heartbeat's own
    # request comes on top of them (see below).
    cancelling = owner_task.cancelling() if owner_task is not None else 0
    lost = asyncio.Event()

    try:
        # Start lock refresh task if requested. If the lock is lost the
        # heartbeat task cancels the workflow execution task so a stale
        # runner cannot keep working without the lease.
        if refresh_interval is not None:
            refresh_task = asyncio.create_task(
                _refresh_lock_periodically(
                    storage,
                    instance_id,
                    worker_id,
                    refresh_interval,
                    owner_task,
                    lost,
                    stop_refresh,
                )
            )

        try:
            yield
        except asyncio.CancelledError:
            # The heartbeat's cancellation ends this block only, as
            # LockLostError (the way asyncio.timeout ends its block with
            # TimeoutError). The task running the block may be a caller's --
            # a parent's activity starting a child inline, a loop resuming
            # workflows -- and must not stay cancelled. A cancellation
            # requested on top of the heartbeat's still propagates.
            if lost.is_set() and owner_task is not None and owner_task.uncancel() <= cancelling:
                raise LockLostError(f"Lock lost for instance {instance_id}") from None
            raise
        if lost.is_set():
            # The block swallowed the heartbeat's cancellation; the run still
            # lost its lease.
            if owner_task is not None:
                owner_task.uncancel()
            raise LockLostError(f"Lock lost for instance {instance_id}")

    finally:
        # Stop the refresh task (one that saw the lock lost ended with an error)
        if refresh_task is not None:
            await _stop_heartbeat(refresh_task, stop_refresh)

        # Release lock
        await storage.release_lock(instance_id, worker_id)


async def run_with_lock_heartbeat(
    storage: StorageProtocol,
    instance_id: str,
    lock_id: str,
    run: Callable[[], Awaitable[T]],
    refresh_interval: float = 60.0,
) -> T:
    """
    Run ``run()`` under a lock the caller already holds, keeping the lease alive.

    The lease is refreshed once up front, so a claim that already lapsed (and
    may belong to another worker now) raises LockLostError before anything
    runs, and then every refresh_interval seconds. On lock loss only the
    run's own task is cancelled -- never the caller's -- and LockLostError is
    raised here.

    Args:
        storage: Storage backend
        instance_id: Workflow instance whose lock is held
        lock_id: Epoch lock identity the caller holds
        run: Zero-argument coroutine function to execute
        refresh_interval: Heartbeat interval in seconds

    Raises:
        LockLostError: If the lock is not (or no longer) held by lock_id
    """
    if not await storage.refresh_lock(instance_id, lock_id):
        raise LockLostError(f"Lock lost for instance {instance_id}")
    task = asyncio.ensure_future(run())
    stop = asyncio.Event()
    heartbeat = asyncio.create_task(
        _refresh_lock_periodically(storage, instance_id, lock_id, refresh_interval, task, stop=stop)
    )
    try:
        return await task
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if task.cancelled() and (current is None or current.cancelling() == 0):
            # Cancelled by the heartbeat, not by our caller.
            raise LockLostError(f"Lock lost for instance {instance_id}") from None
        raise
    finally:
        await _stop_heartbeat(heartbeat, stop)


# How long stopping a heartbeat waits for a refresh it is running.
_HEARTBEAT_STOP_GRACE = 5.0


async def _stop_heartbeat(heartbeat: asyncio.Task[Any], stop: asyncio.Event) -> None:
    """Stop a heartbeat without cutting off a refresh it is running.

    Cancelling a refresh mid-statement makes SQLAlchemy invalidate the
    connection (with an in-memory SQLite database on a StaticPool, the whole
    database goes with it). The heartbeat stops at its next wait instead; one
    still running after the grace period is cancelled.
    """
    stop.set()
    try:
        if not heartbeat.done():
            await asyncio.wait({heartbeat}, timeout=_HEARTBEAT_STOP_GRACE)
    finally:
        if not heartbeat.done():
            heartbeat.cancel()
        with suppress(asyncio.CancelledError, RuntimeError):
            await heartbeat


async def _refresh_lock_periodically(
    storage: StorageProtocol,
    instance_id: str,
    worker_id: str,
    interval: float,
    owner_task: asyncio.Task[Any] | None = None,
    lost: asyncio.Event | None = None,
    stop: asyncio.Event | None = None,
) -> None:
    """
    Periodically refresh a lock to prevent timeout.

    This is a background task that runs while a workflow is executing.
    On lock loss it cancels owner_task (the workflow run) when provided.

    Args:
        storage: Storage backend
        instance_id: Workflow instance
        worker_id: Unique worker identifier
        interval: Refresh interval in seconds
        owner_task: Task to cancel if the lock is lost
        lost: Set before owner_task is cancelled, so that the owner can tell
            this cancellation from others
        stop: Once set, the task ends at its next wait (see _stop_heartbeat)
    """
    with suppress(asyncio.CancelledError):
        while True:
            if stop is None:
                await asyncio.sleep(interval)
            else:
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                if stop.is_set():
                    return

            # Refresh the lock
            success = await storage.refresh_lock(instance_id, worker_id)
            if not success:
                # Lock was lost - cancel the workflow run so a stale runner
                # cannot keep executing without the lease.
                if lost is not None:
                    lost.set()
                if owner_task is not None:
                    owner_task.cancel()
                raise RuntimeError(
                    f"Lost lock for instance {instance_id} during refresh. "
                    "Workflow execution may be compromised."
                )


async def cleanup_stale_locks_periodically(
    storage: StorageProtocol,
    interval: int = 60,
) -> None:
    """
    Background task to periodically clean up stale locks.

    This should be run as a background task in your application to ensure
    that locks from crashed workers don't block workflows indefinitely.

    Note: This function only cleans up locks without resuming workflows.
    For automatic workflow resumption, use auto_resume_stale_workflows_periodically().

    Important: This function should only be run by a single worker (e.g., via leader
    election). It does not perform its own distributed coordination.

    Example:
        >>> asyncio.create_task(
        ...     cleanup_stale_locks_periodically(storage, interval=60)
        ... )

    Args:
        storage: Storage backend
        interval: Cleanup interval in seconds (default: 60)
    """
    with suppress(asyncio.CancelledError):
        while True:
            # Add jitter to prevent thundering herd
            jitter = random.uniform(0, interval * 0.3)
            await asyncio.sleep(interval + jitter)

            try:
                # Clean up stale locks
                workflows = await storage.cleanup_stale_locks()

                if len(workflows) > 0:
                    logger.info("Cleaned up %d stale locks", len(workflows))
            except Exception as e:
                logger.error("Failed to cleanup stale locks: %s", e, exc_info=True)


async def auto_resume_stale_workflows_periodically(
    storage: StorageProtocol,
    replay_engine: Any,
    interval: int = 60,
) -> None:
    """
    Background task to periodically clean up stale locks and auto-resume workflows.

    This combines lock cleanup with automatic workflow resumption, ensuring
    that workflows interrupted by worker crashes are automatically recovered.

    Important: This function should only be run by a single worker (e.g., via leader
    election). It does not perform its own distributed coordination.

    Example:
        >>> asyncio.create_task(
        ...     auto_resume_stale_workflows_periodically(
        ...         storage, replay_engine, interval=60
        ...     )
        ... )

    Args:
        storage: Storage backend
        replay_engine: ReplayEngine instance for resuming workflows
        interval: Cleanup interval in seconds (default: 60)
    """
    with suppress(asyncio.CancelledError):
        while True:
            # Add jitter to prevent thundering herd
            jitter = random.uniform(0, interval * 0.3)
            await asyncio.sleep(interval + jitter)

            try:
                # Backstop: requeue channel waits stranded by the residual
                # missed-wakeup race (publisher scan ran before the wait
                # registration committed).
                try:
                    requeued = await storage.requeue_channel_waits_with_pending()
                    if requeued:
                        logger.info("Requeued %d channel wait(s) with pending messages", requeued)
                except Exception as e:
                    logger.error("Failed to requeue channel waits: %s", e, exc_info=True)

                # Deliver DOWN notifications for terminated actors and free
                # their registered names.
                try:
                    expired = await storage.delete_expired_capabilities()
                    if expired:
                        logger.debug("deleted %d expired capabilities", expired)
                    downs = await storage.emit_actor_down_for_terminated()
                    if downs:
                        logger.info("Emitted %d actor DOWN notification(s)", downs)
                except Exception as e:
                    logger.error("Failed to emit actor DOWN notifications: %s", e, exc_info=True)

                # Orphan-task reclaim: 'claimed' rows older than the lock
                # lease were abandoned by a crashed claimant — reopen them.
                try:
                    reclaimed = await storage.reclaim_stale_tasks(300)
                    if reclaimed:
                        logger.info("Reclaimed %d stale task(s)", reclaimed)
                except Exception as e:
                    logger.error("Failed to reclaim stale tasks: %s", e, exc_info=True)

                # Clean up stale locks and get workflows to resume
                workflows_to_resume = await storage.cleanup_stale_locks()

                if len(workflows_to_resume) > 0:
                    logger.info("Cleaned up %d stale locks", len(workflows_to_resume))

                    # Auto-resume workflows
                    for workflow in workflows_to_resume:
                        instance_id = workflow["instance_id"]
                        workflow_name = workflow["workflow_name"]
                        source_hash = workflow["source_hash"]
                        status = workflow.get("status", "running")

                        try:
                            # Special handling for workflows in compensating state
                            if status == "compensating":
                                # Workflow crashed during compensation execution
                                # Only re-execute compensations, don't run workflow function
                                logger.info(
                                    "Auto-resuming compensating workflow: %s "
                                    "(compensation recovery only, no workflow execution)",
                                    instance_id,
                                )
                                success = await replay_engine.resume_compensating_workflow(
                                    instance_id
                                )
                                if success:
                                    logger.info(
                                        "Successfully completed compensations for: %s",
                                        instance_id,
                                    )
                                else:
                                    logger.warning(
                                        "Failed to complete compensations for: %s", instance_id
                                    )
                                continue

                            # Normal workflow resumption (status='running')
                            # Check if workflow definition matches current Saga registry
                            # This prevents resuming workflows with outdated/incompatible code
                            current_definition = await storage.get_current_workflow_definition(
                                workflow_name
                            )

                            if current_definition is None:
                                logger.warning(
                                    "Skipping auto-resume for %s: "
                                    "workflow '%s' not found in registry",
                                    instance_id,
                                    workflow_name,
                                )
                                continue

                            if current_definition["source_hash"] != source_hash:
                                logger.warning(
                                    "Skipping auto-resume for %s: "
                                    "workflow definition has changed "
                                    "(old hash: %s..., new hash: %s...)",
                                    instance_id,
                                    source_hash[:8],
                                    current_definition["source_hash"][:8],
                                )
                                continue

                            # Hash matches - safe to resume
                            logger.info(
                                "Auto-resuming workflow: %s (instance: %s)",
                                workflow_name,
                                instance_id,
                            )
                            await replay_engine.resume_by_name(instance_id, workflow_name)
                            logger.info("Successfully resumed workflow: %s", instance_id)
                        except Exception as e:
                            # Log error but continue with other workflows
                            logger.error(
                                "Failed to auto-resume workflow %s: %s",
                                instance_id,
                                e,
                                exc_info=True,
                            )
            except Exception as e:
                logger.error("Failed to cleanup stale locks: %s", e, exc_info=True)


class LockNotAcquiredError(RuntimeError):
    """Raised when a lock cannot be acquired because another holder has it."""

    pass


class LockLostError(Exception):
    """Raised when a lock is unexpectedly lost during execution."""

    pass


class InstanceTerminatedError(LockLostError):
    """Raised by a fenced write when the instance already reached a terminal
    status (completed, failed, cancelled).

    Terminal statuses are absorbing: a cancel that landed while a run held
    the lock must not be overwritten. It is a LockLostError, so every path
    that tears a run down on lock loss stops this run the same way.
    """
