# CloudEvents HTTP Binding

Edda fully supports the [CloudEvents HTTP Protocol Binding specification](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/http-protocol-binding.md), ensuring reliable event delivery and proper error handling.

## CloudEvents Content Modes

Edda supports both CloudEvents content modes:

**Structured Mode (Recommended)**:
- All CloudEvents attributes in JSON body
- `Content-Type: application/cloudevents+json`
- No CE-* headers required
- Examples in this document use Structured Mode

**Binary Mode (Alternative)**:

```bash
curl -X POST http://localhost:8001/ \
  -H "Content-Type: application/json" \
  -H "CE-SpecVersion: 1.0" \
  -H "CE-Type: payment.completed" \
  -H "CE-Source: payment-service" \
  -H "CE-ID: event-123" \
  -d '{"amount": 99.99}'
```

Both modes are fully supported by Edda's CloudEvents implementation.

## HTTP Response Status Codes

Edda returns appropriate HTTP status codes according to the CloudEvents specification:

### Success (202 Accepted)

Edda answers 202 once the event is stored: it is published to its channel, and each workflow registered for its type with `@workflow(event_handler=True)` has a new instance. From then on the event is kept even if the process stops: a running worker resumes a first run or a delivery that did not finish. Handlers registered with `@app.on_event()` run after the response, and are not run again.

The channel is the event's `type`. An event with the `eddainstanceid` extension attribute is for that instance only, and is published to its direct channel `type:<instance ID>`; `wait_event` in the instance receives it, even when it arrived before the wait (see [Sending to One Instance](wait-event.md#sending-to-one-instance)).

```bash
curl -X POST http://localhost:8001/ \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "payment.completed",
    "source": "payment-service",
    "id": "event-123",
    "data": {"amount": 99.99}
  }'
```

**Response:**
```http
HTTP/1.1 202 Accepted
Content-Type: application/json

{
  "status": "accepted"
}
```

**When returned:**

- ✅ The event was parsed and stored
- ✅ Its workflows and `on_event` handlers run, and the workflows waiting for it (`wait_event`) are woken, after the response
- ✅ Final processing outcome is not yet known

### Client Error (400 Bad Request)

When the CloudEvent is malformed or fails validation (**non-retryable**):

```bash
# Missing required field: specversion
curl -X POST http://localhost:8001/ \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "type": "payment.completed",
    "source": "payment-service",
    "id": "event-123"
  }'
```

**Response:**
```http
HTTP/1.1 400 Bad Request
Content-Type: application/json

{
  "error": "Failed to find specversion in HTTP request",
  "error_type": "GenericException",
  "retryable": false
}
```

**When returned:**

- ❌ Missing required CloudEvents fields (`specversion`, `type`, `source`, `id`)
- ❌ Invalid JSON format
- ❌ CloudEvents validation errors

**Client action:**

- 🚫 **DO NOT retry** - Fix the event structure and resend

### Client Error (403 Forbidden)

An event posted to a channel that is sealed for `channel_pub` must carry a valid capability token in the `capabilitytoken` extension attribute. In binary mode the token goes in the `ce-capabilitytoken` header. The target channel is the event `type`, or `type:{instance_id}` for an event addressed to one instance (`eddainstanceid`).

```bash
curl -X POST http://localhost:8001/ \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "orders",
    "source": "order-service",
    "id": "event-124",
    "capabilitytoken": "cap:channel_pub:orders:<secret>",
    "data": {"order_id": "A-1"}
  }'
```

Without a valid token Edda publishes nothing and answers:

```http
HTTP/1.1 403 Forbidden
Content-Type: application/json

{
  "error": "...",
  "error_type": "Forbidden",
  "retryable": false
}
```

The `capabilitytoken` attribute is not stored with the message. An expired token is rejected, and an expired grant still keeps the channel sealed. See [Replay Safety and Sealed Channels](../messages.md#replay-safety-and-sealed-channels).

### Server Error (500 Internal Server Error)

When an internal error occurs (**retryable**):

**Response:**
```http
HTTP/1.1 500 Internal Server Error
Content-Type: application/json

{
  "error": "Database connection failed",
  "error_type": "DatabaseError",
  "retryable": true
}
```

**When returned:**

- ⚠️ The event could not be stored (for example, the database cannot be reached)
- ⚠️ Internal server errors
- ⚠️ Unexpected exceptions

**Client action:**

- 🔄 **Retry with exponential backoff**

### Service Unavailable (503 Service Unavailable)

When the app is shutting down (**retryable**). Nothing is published and no workflow starts.

**Response:**
```http
HTTP/1.1 503 Service Unavailable
Content-Type: application/json

{
  "error": "app is not running",
  "error_type": "ServiceUnavailable",
  "retryable": true
}
```

**Client action:**

- 🔄 **Retry with exponential backoff**: another worker, or this one once it runs again, accepts the event

`shutdown()` waits for the events it accepted before: the first runs of their workflows, their `on_event` handlers and their delivery to waiting workflows get 30 seconds to finish. What still runs then is cancelled, and a running worker resumes a cancelled first run or delivery.

### Delivery Guarantee

Events are delivered at least once: a sender that retries after a 500, a 503 or a lost response can store the same event twice. A workflow that must not handle an event twice deduplicates on an id it receives. A workflow that waits for the event gets its `id` (`ReceivedEvent.id`); a workflow that the event starts (`event_handler=True`) gets only its `data`, so the sender puts an id there.

## Error Response Structure

All error responses include structured information to help clients decide whether to retry:

```json
{
  "error": "Human-readable error message",
  "error_type": "PythonExceptionClassName",
  "retryable": true | false
}
```

### Fields

- **`error`** (string): Human-readable error message
- **`error_type`** (string): Python exception class name for debugging
- **`retryable`** (boolean): Whether the client should retry
  - `false`: Client error (400, 403) - Fix the request before retrying
  - `true`: Server error (500, 503) - Retry with exponential backoff

## Client Retry Logic

Example retry implementation:

```python
import httpx2
import asyncio

async def send_cloudevent_with_retry(event_data: dict, max_retries: int = 3):
    """Send CloudEvent with automatic retry on server errors."""

    for attempt in range(max_retries):
        try:
            async with httpx2.AsyncClient() as client:
                response = await client.post(
                    "http://localhost:8001/",
                    json=event_data,
                    headers={"content-type": "application/cloudevents+json"},
                )

                if response.status_code == 202:
                    # Success
                    print("✅ Event accepted")
                    return response.json()

                elif response.status_code == 400:
                    # Client error - DO NOT retry
                    error = response.json()
                    print(f"❌ Client error: {error['error']}")
                    raise ValueError(f"Non-retryable error: {error['error']}")

                elif response.status_code in (500, 503):
                    # Server error - Retry with exponential backoff
                    error = response.json()
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        print(f"⚠️ Server error, retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise Exception(f"Max retries exceeded: {error['error']}")

        except httpx2.ConnectError:
            # Connection error - Retry
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"⚠️ Connection error, retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                raise
```

## CloudEvents Specification Compliance

Edda complies with the following CloudEvents specifications:

### HTTP Protocol Binding v1.0.2

✅ **Success Responses:**

- `202 Accepted` - Event accepted for async processing (recommended for async systems)
- `200 OK` - Event processed synchronously (not used by Edda)

✅ **Client Error Responses (Non-Retryable):**

- `400 Bad Request` - Malformed CloudEvent
- `403 Forbidden` - The event's channel is sealed and the event carries no valid `capabilitytoken`
- `415 Unsupported Media Type` - (Reserved for future use)

✅ **Server Error Responses (Retryable):**

- `500 Internal Server Error` - Internal error
- `503 Service Unavailable` - The app is shutting down

❌ **Prohibited:**

- 3xx redirect codes - Not allowed by CloudEvents spec

### Error Response Extensions

Edda extends the CloudEvents specification with additional error metadata:

```json
{
  "error": "Error message",
  "error_type": "ExceptionClassName",
  "retryable": boolean
}
```

This extension helps clients make intelligent retry decisions without parsing error messages.

## Integration Examples

### With CloudEvents SDK

Using the official CloudEvents Python SDK:

```python
from cloudevents.http import CloudEvent, to_structured
import httpx2

async def send_event():
    """Send CloudEvent using official SDK."""

    # Create CloudEvent
    attributes = {
        "type": "payment.completed",
        "source": "payment-service",
    }
    data = {"order_id": "ORD-123", "amount": 99.99}

    event = CloudEvent(attributes, data)

    # Convert to structured format
    headers, body = to_structured(event)

    # Send to Edda
    async with httpx2.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8001/",
            headers=headers,
            content=body,
        )

        if response.status_code == 202:
            print("✅ Event accepted")
        elif response.status_code == 400:
            error = response.json()
            print(f"❌ Client error: {error}")
        elif response.status_code in (500, 503):
            error = response.json()
            print(f"⚠️ Server error (retryable): {error}")
```

## Best Practices

### 1. Always Check Response Status

```python
# ❌ Bad: Ignoring response status
await client.post(url, json=event_data)

# ✅ Good: Checking response status
response = await client.post(url, json=event_data)
if response.status_code != 202:
    handle_error(response)
```

### 2. Implement Retry Logic

```python
# ✅ Retry on 500 and 503, don't retry on 400
if response.status_code in (500, 503):
    error = response.json()
    if error["retryable"]:
        retry_with_backoff()
```

### 3. Use Structured Logging

```python
import structlog

logger = structlog.get_logger()

response = await client.post(url, json=event_data)
logger.info(
    "cloudevent_sent",
    status_code=response.status_code,
    event_type=event_data["type"],
    retryable=response.json().get("retryable"),
)
```

## Related Documentation

- **[Event Waiting Example](../../examples/events.md)**: Complete event-driven workflow examples
- **[CloudEvents Specification](https://github.com/cloudevents/spec)**: Official CloudEvents spec
- **[HTTP Protocol Binding](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/http-protocol-binding.md)**: HTTP binding specification
