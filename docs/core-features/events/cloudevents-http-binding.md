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

When an event is successfully accepted for asynchronous processing:

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

**When to use:**

- ✅ Event was successfully parsed and accepted
- ✅ Event handler is executing in the background
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

- ⚠️ Database connection failures
- ⚠️ Internal server errors
- ⚠️ Unexpected exceptions

**Client action:**

- 🔄 **Retry with exponential backoff**

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
  - `false`: Client error (400) - Fix the request before retrying
  - `true`: Server error (500) - Retry with exponential backoff

## Client Retry Logic

Example retry implementation:

```python
import httpx
import asyncio

async def send_cloudevent_with_retry(event_data: dict, max_retries: int = 3):
    """Send CloudEvent with automatic retry on server errors."""

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient() as client:
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

                elif response.status_code == 500:
                    # Server error - Retry with exponential backoff
                    error = response.json()
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        print(f"⚠️ Server error, retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise Exception(f"Max retries exceeded: {error['error']}")

        except httpx.ConnectError:
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
- `415 Unsupported Media Type` - (Reserved for future use)

✅ **Server Error Responses (Retryable):**

- `500 Internal Server Error` - Internal error
- `503 Service Unavailable` - (Reserved for future use)

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
import httpx

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
    async with httpx.AsyncClient() as client:
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
        elif response.status_code == 500:
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
# ✅ Retry on 500, don't retry on 400
if response.status_code == 500:
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
