"""
Transactional outbox helpers for reliable event publishing.

This module provides utilities for sending events using the transactional
outbox pattern. Events are written to the database within the same transaction
as other business logic, ensuring atomicity.
"""

import logging
import uuid
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel

from edda.pydantic_utils import is_pydantic_instance, to_json_dict

if TYPE_CHECKING:
    from edda.context import WorkflowContext

logger = logging.getLogger(__name__)


async def send_event_transactional(
    ctx: "WorkflowContext",
    event_type: str,
    event_source: str,
    event_data: dict[str, Any] | BaseModel,
    content_type: str = "application/json",
) -> str:
    """
    Send an event using the transactional outbox pattern.

    This function writes an event to the outbox table instead of sending it
    directly. The event will be asynchronously published by the Outbox Relayer.

    This ensures that event publishing is atomic with the workflow execution:
    - If the workflow fails, the event is not published
    - If the workflow succeeds, the event is guaranteed to be published

    Example:
        >>> # With dict
        >>> @activity
        ... async def reserve_inventory(ctx: WorkflowContext, order_id: str) -> dict:
        ...     reservation_id = str(uuid.uuid4())
        ...     await send_event_transactional(
        ...         ctx,
        ...         event_type="inventory.reserved",
        ...         event_source="order-service",
        ...         event_data={
        ...             "order_id": order_id,
        ...             "reservation_id": reservation_id,
        ...         }
        ...     )
        ...     return {"reservation_id": reservation_id}
        >>>
        >>> # With Pydantic model (automatically converted to JSON)
        >>> @activity
        ... async def reserve_inventory_typed(ctx: WorkflowContext, order_id: str) -> dict:
        ...     reservation_id = str(uuid.uuid4())
        ...     event = InventoryReserved(
        ...         order_id=order_id,
        ...         reservation_id=reservation_id,
        ...     )
        ...     await send_event_transactional(
        ...         ctx,
        ...         event_type="inventory.reserved",
        ...         event_source="order-service",
        ...         event_data=event,
        ...     )
        ...     return {"reservation_id": reservation_id}

    Args:
        ctx: Workflow context
        event_type: CloudEvent type (e.g., "order.created")
        event_source: CloudEvent source (e.g., "order-service")
        event_data: Event payload (JSON dict or Pydantic model)
        content_type: Content type (defaults to "application/json")

    Returns:
        Event ID (UUID)

    Raises:
        Exception: If writing to outbox fails
    """
    # Check if in transaction
    if not ctx.in_transaction():
        logger.warning(
            "send_event_transactional() called outside of a transaction. "
            "Event will still be sent, but atomicity with other operations is not guaranteed. "
            "Consider using @activity (with default transactional=True) or wrapping in ctx.transaction()."
        )

    # Convert Pydantic model to JSON dict
    event_data_dict: dict[str, Any]
    if is_pydantic_instance(event_data):
        event_data_dict = to_json_dict(event_data)
    else:
        event_data_dict = cast(dict[str, Any], event_data)

    # Generate event ID
    event_id = str(uuid.uuid4())

    # Write to outbox table
    await ctx.storage.add_outbox_event(
        event_id=event_id,
        event_type=event_type,
        event_source=event_source,
        event_data=event_data_dict,
        content_type=content_type,
    )

    return event_id
