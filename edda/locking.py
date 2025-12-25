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
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any

from edda.storage.protocol import StorageProtocol

logger = logging.getLogger(__name__)


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
        RuntimeError: If lock cannot be acquired
    """
    # Try to acquire lock
    acquired = await storage.try_acquire_lock(instance_id, worker_id, timeout_seconds)
    if not acquired:
        raise RuntimeError(f"Failed to acquire lock for instance {instance_id}")

    refresh_task: asyncio.Task[Any] | None = None

    try:
        # Start lock refresh task if requested
        if refresh_interval is not None:
            refresh_task = asyncio.create_task(
                _refresh_lock_periodically(storage, instance_id, worker_id, refresh_interval)
            )

        yield

    finally:
        # Cancel refresh task
        if refresh_task is not None:
            refresh_task.cancel()
            with suppress(asyncio.CancelledError):
                await refresh_task

        # Release lock
        await storage.release_lock(instance_id, worker_id)


async def _refresh_lock_periodically(
    storage: StorageProtocol,
    instance_id: str,
    worker_id: str,
    interval: float,
) -> None:
    """
    Periodically refresh a lock to prevent timeout.

    This is a background task that runs while a workflow is executing.

    Args:
        storage: Storage backend
        instance_id: Workflow instance
        worker_id: Unique worker identifier
        interval: Refresh interval in seconds
    """
    with suppress(asyncio.CancelledError):
        while True:
            await asyncio.sleep(interval)

            # Refresh the lock
            success = await storage.refresh_lock(instance_id, worker_id)
            if not success:
                # Lock was lost - this shouldn't happen in normal operation
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


class LockNotAcquiredError(Exception):
    """Raised when a lock cannot be acquired."""

    pass


class LockLostError(Exception):
    """Raised when a lock is unexpectedly lost during execution."""

    pass
