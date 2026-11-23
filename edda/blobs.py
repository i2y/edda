"""Content-addressed blob store with payload references.

Large payloads are stored once by content hash and referenced as
``"blob:sha256:<hex>"`` inside channel messages, event data, etc. Writes
are idempotent — identical payloads share a single blob.
"""

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from edda.context import WorkflowContext

BLOB_REF_PREFIX = "blob:sha256:"


async def store_payload(ctx: "WorkflowContext", data: bytes) -> str:
    """
    Store data in the blob store and return its reference
    ``"blob:sha256:<hash>"``.
    """
    h = hashlib.sha256(data).hexdigest()
    await ctx.storage.put_blob(h, data, created_by=ctx.instance_id)
    return BLOB_REF_PREFIX + h


async def load_payload(ctx: "WorkflowContext", ref: str) -> bytes:
    """
    Resolve a blob reference to its data. Accepts a bare hash or the full
    ``"blob:sha256:<hash>"`` reference.

    Raises:
        KeyError: if no blob exists for the reference
    """
    h = ref.removeprefix(BLOB_REF_PREFIX)
    data = await ctx.storage.get_blob(h)
    if data is None:
        raise KeyError(f"blob not found: {ref}")
    return data
