"""
Transactional Outbox Pattern Implementation.

This module provides reliable event publishing using the transactional outbox pattern.
Events are first written to the database, then asynchronously published by a background
relayer process.
"""

from edda.outbox.relayer import OutboxRelayer
from edda.outbox.transactional import send_event_transactional

__all__ = [
    "OutboxRelayer",
    "send_event_transactional",
]
