"""Demo Edda ASGI application for tsuno."""

import asyncio
import logging
import os
import sys
from datetime import datetime
from enum import Enum
from typing import Any, cast

import uvloop

# Configure logging to see Edda startup messages (must be before edda import)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

from pydantic import BaseModel, Field, field_validator  # noqa: E402

from edda import (  # noqa: E402
    EddaApp,
    RetryPolicy,
    WorkflowContext,
    activity,
    compensation,
    on_failure,
    publish,
    receive,
    send_to,
    subscribe,
    wait_until,
    workflow,
)
from edda.wsgi import create_wsgi_app  # noqa: E402

# Python 3.12+ uses asyncio.set_event_loop_policy() instead of uvloop.install()
if sys.version_info >= (3, 12):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    uvloop.install()


# ========== Enum Definitions for Advanced Workflow ==========

class OrderStatus(Enum):
    """Order status enum."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class Priority(Enum):
    """Priority level enum."""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    URGENT = 4


# ========== Pydantic Models for Type-Safe Workflows ==========

class OrderItem(BaseModel):
    """Order item with validation."""
    item_id: str
    name: str = Field(..., min_length=1)
    price: float = Field(..., gt=0)
    quantity: int = Field(..., ge=1)


class ShippingAddress(BaseModel):
    """Shipping address with validation."""
    street: str = Field(..., min_length=1)
    city: str = Field(..., min_length=1)
    state: str = Field(..., pattern=r"^[A-Z]{2}$")
    zip_code: str = Field(..., pattern=r"^\d{5}$")
    country: str = "US"


class OrderValidation(BaseModel):
    """Order validation result."""
    order_id: str
    total_amount: float
    validated: bool
    validation_time: datetime


class InventoryReservation(BaseModel):
    """Inventory reservation result."""
    reservation_id: str
    item_id: str
    quantity: int


class OrderInventory(BaseModel):
    """Order inventory status."""
    order_id: str
    reservations: list[InventoryReservation]
    status: str


class PaymentResult(BaseModel):
    """Payment processing result."""
    order_id: str
    transaction_id: str
    amount: float
    status: str
    payment_method: str


class ShipmentResult(BaseModel):
    """Shipment creation result."""
    order_id: str
    shipment_id: str
    tracking_number: str
    address: ShippingAddress
    estimated_delivery: str


class ConfirmationResult(BaseModel):
    """Email confirmation result."""
    order_id: str
    email: str
    tracking_number: str
    sent: bool
    message_id: str


class UserData(BaseModel):
    """User data with validation."""
    name: str = Field(..., min_length=2)
    email: str = Field(..., pattern=r"^[^@]+@[^@]+\.[^@]+$")
    age: int = Field(..., ge=18, le=120)

    @field_validator('email')
    @classmethod
    def validate_email_domain(cls, v: str) -> str:
        """Validate email has proper domain structure."""
        if '@' not in v:
            raise ValueError("Invalid email format: missing @ symbol")
        local, domain = v.split('@', 1)
        if not local:
            raise ValueError("Invalid email format: empty local part")
        if not domain or '.' not in domain:
            raise ValueError(f"Invalid email format: invalid domain '{domain}'")
        return v


class UserCreationResult(BaseModel):
    """User creation result."""
    user_id: str
    name: str
    email: str
    status: str


class PaymentAPIResponse(BaseModel):
    """External payment API response."""
    transaction_id: str
    status: str
    payment_gateway_response: str


class PaymentProcessingResult(BaseModel):
    """Payment processing result with API response."""
    user_id: str
    amount: float
    transaction_id: str
    api_response: PaymentAPIResponse
    status: str


# ========== Workflow Input/Output Models ==========


class DemoWorkflowInput(BaseModel):
    """Input for demo_workflow."""
    name: str = Field(..., min_length=1)


class DemoWorkflowResult(BaseModel):
    """Result of demo_workflow."""
    message: str
    status: str


class OrderProcessingInput(BaseModel):
    """Input for order_processing_workflow."""
    order_id: str = Field(..., pattern=r"^ORD-")
    customer_email: str = Field(..., pattern=r"^[^@]+@[^@]+\.[^@]+$")
    items: list[OrderItem]
    shipping_address: ShippingAddress


class OrderProcessingResult(BaseModel):
    """Result of order_processing_workflow."""
    order_id: str
    status: str
    total_amount: float
    tracking_number: str
    confirmation_message_id: str


class LoanApprovalInput(BaseModel):
    """Input for loan_approval_workflow."""
    applicant_id: str = Field(..., min_length=1)
    loan_amount: float = Field(..., gt=0)
    stated_income: float = Field(..., gt=0)


class LoanApprovalResult(BaseModel):
    """Result of loan_approval_workflow."""
    applicant_id: str
    loan_amount: float
    credit_check: dict[str, Any]
    income_check: dict[str, Any]
    decision: dict[str, Any]


class ApiIntegrationInput(BaseModel):
    """Input for api_integration_with_retry_workflow."""
    endpoint: str = Field(..., min_length=1)
    max_retries: int = Field(default=5, ge=1, le=10)


class ApiIntegrationResult(BaseModel):
    """Result of api_integration_with_retry_workflow."""
    endpoint: str
    status: str  # "success" or "failed"
    attempts: int
    result: dict[str, Any] | None = None
    error: str | None = None


class BookingInput(BaseModel):
    """Input for cancellable_booking workflow."""
    booking_id: str = Field(..., pattern=r"^BOOK-")
    destination: str = Field(..., min_length=1)
    amount: float = Field(..., gt=0)


class BookingResult(BaseModel):
    """Result of cancellable_booking workflow."""
    booking_id: str
    hotel: str
    flight: str
    payment: str
    status: str


class PaymentWorkflowInput(BaseModel):
    """Input for payment_workflow."""
    order_id: str = Field(..., min_length=1)
    amount: float = Field(..., gt=0)


class PaymentWorkflowResult(BaseModel):
    """Result of payment_workflow."""
    order_id: str
    status: str
    payment_confirmed: bool


class MatchCaseInput(BaseModel):
    """Input for match_case_workflow."""
    order_id: str = Field(..., min_length=1)
    order_status: str = Field(..., min_length=1)
    reason: str | None = None


class MatchCaseResult(BaseModel):
    """Result of match_case_workflow."""
    order_id: str
    original_status: str
    result: dict[str, Any]
    workflow_type: str


class AdvancedTypesInput(BaseModel):
    """Input for advanced_types_demo_workflow."""
    order_id: str = Field(..., min_length=1)
    status: OrderStatus
    items: list[dict[str, Any]]
    metadata: dict[str, str]
    tags: list[str]
    priority: Priority = Priority.MEDIUM


class AdvancedTypesResult(BaseModel):
    """Result of advanced_types_demo_workflow."""
    order_id: str
    status: str
    priority: int
    result: dict[str, Any]
    workflow_type: str


class OrderWithAutoCancelInput(BaseModel):
    """Input for order_with_auto_cancel_workflow."""
    order_id: str = Field(..., min_length=1)
    amount: float = Field(..., gt=0)
    timeout_seconds: int = Field(default=30, ge=1)


class OrderWithAutoCancelResult(BaseModel):
    """Result of order_with_auto_cancel_workflow."""
    order_id: str
    amount: float
    timeout_seconds: int
    payment_status: dict[str, Any]
    final_result: dict[str, Any]
    final_status: str


class ErrorHandlingInput(BaseModel):
    """Input for error_handling_demo_workflow."""
    user_name: str = Field(..., min_length=2)
    user_email: str = Field(..., pattern=r"^[^@]+@[^@]+\.[^@]+$")
    user_age: int = Field(..., ge=18, le=120)
    payment_amount: float = Field(..., gt=0)
    transaction_id: str = Field(..., min_length=1)


class ErrorHandlingResult(BaseModel):
    """Result of error_handling_demo_workflow."""
    user_id: str
    user_name: str
    user_email: str
    payment_amount: float
    transaction_id: str
    user_result: UserCreationResult
    payment_result: PaymentProcessingResult
    email_result: dict[str, Any]
    status: str


# ========== Channel-based Message Queue Models ==========


class JobWorkerInput(BaseModel):
    """Input for job_worker_workflow."""
    worker_id: str = Field(..., min_length=1)


class JobWorkerResult(BaseModel):
    """Result of job_worker_workflow."""
    worker_id: str
    job_id: str
    job_data: dict[str, Any]
    status: str


class NotificationServiceInput(BaseModel):
    """Input for notification_service_workflow."""
    service_id: str = Field(..., min_length=1)


class NotificationServiceResult(BaseModel):
    """Result of notification_service_workflow."""
    service_id: str
    notification_id: str
    notification_data: dict[str, Any]
    status: str


class JobPublisherInput(BaseModel):
    """Input for job_publisher_workflow."""
    task: str = Field(..., min_length=1)


class NotificationPublisherInput(BaseModel):
    """Input for notification_publisher_workflow."""
    message: str = Field(..., min_length=1)


class DirectMessageReceiverInput(BaseModel):
    """Input for direct_message_receiver_workflow."""
    receiver_id: str = Field(..., min_length=1)


class DirectMessageReceiverResult(BaseModel):
    """Result of direct_message_receiver_workflow."""
    receiver_id: str
    received_message: dict[str, Any]


class DirectMessageSenderInput(BaseModel):
    """Input for direct_message_sender_workflow."""
    target_instance_id: str = Field(..., min_length=1)
    message: str = Field(..., min_length=1)


class DirectMessageSenderResult(BaseModel):
    """Result of direct_message_sender_workflow."""
    sent: bool
    target_instance_id: str
    message: str


class ScheduledShipmentInput(BaseModel):
    """Input for scheduled_order_shipment_workflow."""
    order_id: str = Field(..., min_length=1)
    shipment_datetime: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class ScheduledShipmentResult(BaseModel):
    """Result of scheduled_order_shipment_workflow."""
    order_id: str
    shipment_datetime: str
    tracking_number: str
    carrier: str
    status: str
    message: str


# Create Edda application
# Use environment variable EDDA_DB_URL if available, otherwise default to sqlite:///demo.db
# Use EDDA_USE_NOTIFY to control LISTEN/NOTIFY (true/false/auto, default: auto)
_use_notify_env = os.getenv("EDDA_USE_NOTIFY", "auto").lower()
_use_listen_notify: bool | None = None  # Auto-detect
if _use_notify_env == "true":
    _use_listen_notify = True
elif _use_notify_env == "false":
    _use_listen_notify = False

app = EddaApp(
    service_name="demo-service",
    db_url=os.getenv("EDDA_DB_URL", "sqlite:///demo.db"),
    use_listen_notify=_use_listen_notify,
)


# Simple workflow definition
@activity
async def greet(ctx: WorkflowContext, name: str) -> str:  # noqa: ARG001
    print(f"[Activity] Greeting: {name}")
    return f"Hello, {name}!"


# @workflow(event_handler=True)でCloudEvent "demo_workflow"のハンドラーを自動登録
@workflow(event_handler=True)
async def demo_workflow(ctx: WorkflowContext, input: DemoWorkflowInput) -> DemoWorkflowResult:
    """
    Demo workflow that greets a user.

    This saga is automatically triggered by CloudEvents with type="demo_workflow".
    The CloudEvent data field is automatically passed as kwargs.
    """
    print(f"[Workflow] Starting demo_workflow for {input.name}")
    greeting = await greet(ctx, input.name)  # Auto-generated: "greet:1"
    print(f"[Workflow] Completed: {greeting}")
    return DemoWorkflowResult(message=greeting, status="completed")


# ========== Complex Workflow Definition (E-commerce Order Processing) ==========
# Pydantic Integration Demo: OrderItem, ShippingAddress, Pydantic Response Models


@activity
async def validate_order(ctx: WorkflowContext, order_id: str, items: list[OrderItem]) -> OrderValidation:  # noqa: ARG001
    """Validate order (Pydantic integration)"""
    print(f"[Activity] Validating order: {order_id}")
    total_amount = sum(item.price * item.quantity for item in items)
    return OrderValidation(
        order_id=order_id,
        total_amount=total_amount,
        validated=True,
        validation_time=datetime.now(),
    )


@activity
async def reserve_inventory(
    ctx: WorkflowContext, order_id: str, items: list[OrderItem]  # noqa: ARG001
) -> OrderInventory:
    """Reserve inventory (Pydantic integration)"""
    print(f"[Activity] Reserving inventory for order: {order_id}")
    reservations = [
        InventoryReservation(
            reservation_id=f"RES-{order_id}-{item.item_id}",
            item_id=item.item_id,
            quantity=item.quantity,
        )
        for item in items
    ]
    return OrderInventory(
        order_id=order_id,
        reservations=reservations,
        status="reserved",
    )


@activity(
    retry_policy=RetryPolicy(
        max_attempts=10,  # More attempts for critical payment operations
        initial_interval=0.5,  # Faster initial retry (0.5 seconds)
        backoff_coefficient=1.5,  # Slower exponential growth
        max_interval=30.0,  # Cap at 30 seconds (instead of default 60)
        max_duration=120.0,  # Stop after 2 minutes total
    )
)
async def process_payment(ctx: WorkflowContext, order_id: str, amount: float) -> PaymentResult:  # noqa: ARG001
    """
    Process payment with aggressive retry policy.

    Custom retry configuration for critical payment operations:
    - 10 max attempts (vs default 5) for higher resilience
    - 0.5s initial interval (vs default 1s) for faster recovery
    - 1.5 backoff coefficient (vs default 2.0) for more frequent retries
    - 30s max interval to avoid long waits
    - 2 minute total duration to prevent indefinite retry

    This ensures payment gateway timeouts are handled gracefully while
    preventing revenue loss from transient failures.
    """
    print(f"[Activity] Processing payment for order: {order_id}, amount: ${amount}")
    return PaymentResult(
        order_id=order_id,
        transaction_id=f"TXN-{order_id}",
        amount=amount,
        status="paid",
        payment_method="credit_card",
    )


@activity
async def create_shipment(ctx: WorkflowContext, order_id: str, address: ShippingAddress) -> ShipmentResult:  # noqa: ARG001
    """Create shipment (Pydantic integration)"""
    print(f"[Activity] Creating shipment for order: {order_id}")
    return ShipmentResult(
        order_id=order_id,
        shipment_id=f"SHIP-{order_id}",
        tracking_number=f"TRACK-{order_id}-1234567890",
        address=address,
        estimated_delivery="2025-10-30",
    )


@activity
async def send_confirmation(
    ctx: WorkflowContext, order_id: str, email: str, tracking_number: str  # noqa: ARG001
) -> ConfirmationResult:
    """Send confirmation email (Pydantic integration)"""
    print(f"[Activity] Sending confirmation email to: {email}")
    return ConfirmationResult(
        order_id=order_id,
        email=email,
        tracking_number=tracking_number,
        sent=True,
        message_id=f"MSG-{order_id}",
    )


@workflow(event_handler=True)
async def order_processing_workflow(
    ctx: WorkflowContext,
    input: OrderProcessingInput,
) -> OrderProcessingResult:
    """
    E-commerce order processing workflow with Pydantic integration.

    This demonstrates:
    - Pydantic models for input validation (OrderItem, ShippingAddress)
    - Type-safe activity parameters and return values
    - Automatic validation of nested models
    - IDE completion and type checking

    CloudEvent type: "order_processing_workflow"

    Example CloudEvent data:
    {
        "order_id": "ORD-12345",
        "customer_email": "customer@example.com",
        "items": [
            {"item_id": "ITEM-1", "name": "Product A", "price": 29.99, "quantity": 2},
            {"item_id": "ITEM-2", "name": "Product B", "price": 49.99, "quantity": 1}
        ],
        "shipping_address": {
            "street": "221B Baker Street",
            "city": "London",
            "state": "Greater London",
            "zip_code": "NW1 6XE"
        }
    }

    Try from Viewer:
    1. Click "Start New Workflow"
    2. Select "order_processing_workflow"
    3. Fill in the auto-generated form with Pydantic validation
    4. Observe real-time validation errors for invalid inputs
    """
    print(f"[Workflow] Starting order processing for: {input.order_id}")
    print(f"[Workflow] Items: {[item.name for item in input.items]}")
    print(f"[Workflow] Shipping to: {input.shipping_address.city}, {input.shipping_address.state}")

    # Step 1: Validate order (Pydantic return type)
    validation = await validate_order(ctx, input.order_id, input.items)
    print(f"[Workflow] Order validated: total=${validation.total_amount}")

    # Step 2: Reserve inventory (Pydantic return type)
    inventory = await reserve_inventory(ctx, input.order_id, input.items)
    print(f"[Workflow] Inventory reserved: {len(inventory.reservations)} items")

    # Step 3: Process payment (Pydantic return type)
    payment = await process_payment(ctx, input.order_id, validation.total_amount)
    print(f"[Workflow] Payment processed: {payment.transaction_id}")

    # Step 4: Create shipment (Pydantic return type)
    shipment = await create_shipment(ctx, input.order_id, input.shipping_address)
    print(f"[Workflow] Shipment created: {shipment.tracking_number}")

    # Step 5: Send confirmation email (Pydantic return type)
    confirmation = await send_confirmation(
        ctx, input.order_id, input.customer_email, shipment.tracking_number
    )
    print(f"[Workflow] Confirmation sent: {confirmation.message_id}")

    print(f"[Workflow] Order processing completed: {input.order_id}")

    # Return Pydantic model
    return OrderProcessingResult(
        order_id=input.order_id,
        status="completed",
        total_amount=validation.total_amount,
        tracking_number=shipment.tracking_number,
        confirmation_message_id=confirmation.message_id,
    )


# ========== Conditional Branching Workflow (Loan Approval Process) ==========


@activity
async def check_credit_score(ctx: WorkflowContext, applicant_id: str) -> dict[str, Any]:  # noqa: ARG001
    """Check credit score"""
    print(f"[Activity] Checking credit score for: {applicant_id}")
    # Simulate different credit scores based on applicant_id
    score = hash(applicant_id) % 850 + 300  # 300-1150 range, then cap at 850
    score = min(score, 850)
    print(f"[Activity] Credit score: {score}")
    return {
        "applicant_id": applicant_id,
        "credit_score": score,
        "checked_at": "2025-10-26T12:00:00Z",
    }


@activity
async def verify_income(ctx: WorkflowContext, applicant_id: str, stated_income: float) -> dict[str, Any]:  # noqa: ARG001
    """Verify income"""
    print(f"[Activity] Verifying income for: {applicant_id}")
    # Simulate income verification
    verified = hash(applicant_id) % 10 > 2  # 80% pass rate
    return {
        "applicant_id": applicant_id,
        "stated_income": stated_income,
        "verified": verified,
        "verified_at": "2025-10-26T12:00:00Z",
    }


@activity
async def auto_approve_loan(ctx: WorkflowContext, applicant_id: str, amount: float) -> dict[str, Any]:  # noqa: ARG001
    """Auto-approve loan"""
    print(f"[Activity] Auto-approving loan for: {applicant_id}, amount: ${amount}")
    return {
        "applicant_id": applicant_id,
        "amount": amount,
        "status": "approved",
        "approval_type": "automatic",
        "interest_rate": 3.5,
        "loan_id": f"LOAN-{applicant_id}-AUTO",
    }


@activity
async def auto_reject_loan(ctx: WorkflowContext, applicant_id: str, reason: str) -> dict[str, Any]:  # noqa: ARG001
    """Auto-reject loan"""
    print(f"[Activity] Auto-rejecting loan for: {applicant_id}, reason: {reason}")
    return {
        "applicant_id": applicant_id,
        "status": "rejected",
        "rejection_type": "automatic",
        "reason": reason,
    }


@activity
async def request_manual_review(ctx: WorkflowContext, applicant_id: str, amount: float, credit_score: int) -> dict[str, Any]:  # noqa: ARG001
    """Request manual review"""
    print(f"[Activity] Requesting manual review for: {applicant_id}")
    return {
        "applicant_id": applicant_id,
        "amount": amount,
        "credit_score": credit_score,
        "status": "pending_review",
        "review_id": f"REVIEW-{applicant_id}",
        "assigned_to": "senior_underwriter",
    }


@workflow(event_handler=True)
async def loan_approval_workflow(
    ctx: WorkflowContext,
    input: LoanApprovalInput,
) -> LoanApprovalResult:
    """
    Loan approval workflow with conditional branching.

    This demonstrates:
    - Credit score check
    - Income verification
    - Conditional branching based on credit score and amount
    - Auto-approval for good credit
    - Auto-rejection for poor credit
    - Manual review for borderline cases

    CloudEvent type: "loan_approval_workflow"
    """
    print(f"[Workflow] Starting loan approval for: {input.applicant_id}, amount: ${input.loan_amount}")

    # Step 1: Check credit score
    credit_check = await check_credit_score(ctx, input.applicant_id)
    credit_score = credit_check["credit_score"]
    print(f"[Workflow] Credit score: {credit_score}")

    # Step 2: Verify income
    income_check = await verify_income(ctx, input.applicant_id, input.stated_income)
    print(f"[Workflow] Income verified: {income_check['verified']}")

    # Conditional branching based on credit score and income
    if not income_check["verified"]:
        # Income verification failed - auto reject
        result = await auto_reject_loan(ctx, input.applicant_id, "Income verification failed")
        print("[Workflow] Loan rejected: Income verification failed")
    elif credit_score >= 750:
        # Excellent credit - auto approve
        result = await auto_approve_loan(ctx, input.applicant_id, input.loan_amount)
        print("[Workflow] Loan auto-approved: Excellent credit")
    elif credit_score >= 650 and input.loan_amount <= 50000:
        # Good credit + reasonable amount - auto approve
        result = await auto_approve_loan(ctx, input.applicant_id, input.loan_amount)
        print("[Workflow] Loan auto-approved: Good credit and reasonable amount")
    elif credit_score < 600:
        # Poor credit - auto reject
        result = await auto_reject_loan(ctx, input.applicant_id, f"Credit score too low: {credit_score}")
        print("[Workflow] Loan rejected: Poor credit")
    else:
        # Borderline case - manual review required
        result = await request_manual_review(ctx, input.applicant_id, input.loan_amount, credit_score)
        print("[Workflow] Manual review requested: Borderline case")

    print(f"[Workflow] Loan approval completed: {result['status']}")
    return LoanApprovalResult(
        applicant_id=input.applicant_id,
        loan_amount=input.loan_amount,
        credit_check=credit_check,
        income_check=income_check,
        decision=result,
    )


# ========== Loop/Retry Workflow (API Integration with Retry) ==========


@activity
async def fetch_external_data(ctx: WorkflowContext, endpoint: str, attempt: int) -> dict[str, Any]:  # noqa: ARG001
    """Fetch data from external API (with retry)"""
    print(f"[Activity] Fetching data from: {endpoint} (attempt {attempt})")
    # Simulate API success based on attempt number
    success = attempt >= 3 or hash(endpoint) % 5 == 0  # Succeed on 3rd attempt or 20% chance
    if success:
        return {
            "endpoint": endpoint,
            "data": {"user_id": "12345", "status": "active", "balance": 1500.50},
            "success": True,
            "attempt": attempt,
        }
    else:
        return {
            "endpoint": endpoint,
            "success": False,
            "attempt": attempt,
            "error": f"Connection timeout on attempt {attempt}",
        }


@activity
async def process_fetched_data(ctx: WorkflowContext, data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG001
    """Process fetched data"""
    print(f"[Activity] Processing data: {data}")
    return {
        "processed": True,
        "user_id": data.get("user_id"),
        "enriched_data": {**data, "processed_at": "2025-10-26T12:00:00Z"},
    }


@activity
async def log_retry_attempt(ctx: WorkflowContext, attempt: int, error: str) -> dict[str, Any]:  # noqa: ARG001
    """Log retry attempt"""
    print(f"[Activity] Logging retry attempt {attempt}: {error}")
    return {
        "attempt": attempt,
        "error": error,
        "logged_at": "2025-10-26T12:00:00Z",
    }


@workflow(event_handler=True)
async def api_integration_with_retry_workflow(
    ctx: WorkflowContext,
    input: ApiIntegrationInput,
) -> ApiIntegrationResult:
    """
    API integration workflow with retry loop.

    This demonstrates:
    - Loop-based retry logic
    - Conditional continuation based on success/failure
    - Logging each retry attempt
    - Processing data after successful fetch

    CloudEvent type: "api_integration_with_retry_workflow"
    """
    print(f"[Workflow] Starting API integration for: {input.endpoint}")

    result = None
    attempt = 1

    # Retry loop (auto-generated IDs work correctly for sequential loops)
    while attempt <= input.max_retries:
        print(f"[Workflow] Attempt {attempt}/{input.max_retries}")

        fetch_result = await fetch_external_data(ctx, input.endpoint, attempt)

        if fetch_result["success"]:
            print(f"[Workflow] Data fetched successfully on attempt {attempt}")
            # Process the fetched data
            result = await process_fetched_data(ctx, fetch_result["data"])
            break
        else:
            print(f"[Workflow] Fetch failed on attempt {attempt}: {fetch_result['error']}")
            # Log the retry attempt
            await log_retry_attempt(ctx, attempt, fetch_result["error"])
            attempt += 1

    if result is None:
        print(f"[Workflow] All {input.max_retries} attempts failed")
        return ApiIntegrationResult(
            endpoint=input.endpoint,
            status="failed",
            attempts=input.max_retries,
            error="Max retries exceeded",
        )

    print(f"[Workflow] API integration completed successfully after {attempt} attempts")
    return ApiIntegrationResult(
        endpoint=input.endpoint,
        status="success",
        attempts=attempt,
        result=result,
    )


# ========== Cancellable Workflow (for testing) ==========

# Simulated external reservations
_external_reservations: dict[str, dict[str, str]] = {}


# ========== Compensation Functions (defined first) ==========

@compensation
async def cancel_hotel_room(ctx: WorkflowContext, hotel_reservation_id: str) -> None:  # noqa: ARG001
    """Compensation: Cancel hotel reservation"""
    print(f"[Hotel Compensation] Cancelling: {hotel_reservation_id}")
    if hotel_reservation_id in _external_reservations:
        del _external_reservations[hotel_reservation_id]
        print(f"[Hotel Compensation] Cancelled: {hotel_reservation_id}")


@compensation
async def cancel_flight_ticket(ctx: WorkflowContext, flight_reservation_id: str) -> None:  # noqa: ARG001
    """Compensation: Cancel flight reservation"""
    print(f"[Flight Compensation] Cancelling: {flight_reservation_id}")
    if flight_reservation_id in _external_reservations:
        del _external_reservations[flight_reservation_id]
        print(f"[Flight Compensation] Cancelled: {flight_reservation_id}")


@compensation
async def refund_payment(ctx: WorkflowContext, payment_id: str, amount: float) -> None:  # noqa: ARG001
    """Compensation: Refund payment"""
    print(f"[Payment Compensation] Refunding payment: {payment_id}, amount: ${amount}")
    # In real system, call refund API
    print(f"[Payment Compensation] Refunded: {payment_id}")


# ========== Activities (auto-register compensation with @on_failure) ==========

@activity
@on_failure(cancel_hotel_room)
async def reserve_hotel_room(_ctx: WorkflowContext, booking_id: str) -> dict[str, str]:
    """Reserve hotel room"""
    print(f"[Hotel] Reserving hotel for booking {booking_id}")
    hotel_reservation_id = f"HOTEL-{booking_id}"
    _external_reservations[hotel_reservation_id] = {"type": "hotel", "booking_id": booking_id}
    print(f"[Hotel] Reserved: {hotel_reservation_id}")
    return {"hotel_reservation_id": hotel_reservation_id}


@activity
@on_failure(cancel_flight_ticket)
async def reserve_flight_ticket(_ctx: WorkflowContext, booking_id: str) -> dict[str, str]:
    """Reserve flight ticket (with delay)"""
    print(f"[Flight] Reserving flight for booking {booking_id}")
    import asyncio
    await asyncio.sleep(2)
    flight_reservation_id = f"FLIGHT-{booking_id}"
    _external_reservations[flight_reservation_id] = {"type": "flight", "booking_id": booking_id}
    print(f"[Flight] Reserved: {flight_reservation_id}")
    return {"flight_reservation_id": flight_reservation_id}


@activity
@on_failure(refund_payment)
async def charge_customer(ctx: WorkflowContext, booking_id: str, amount: float) -> dict[str, Any]:  # noqa: ARG001
    """Charge customer (slow process, can be cancelled)"""
    print(f"[Payment] Charging customer for {booking_id}: ${amount}")
    import asyncio
    for i in range(10):
        await asyncio.sleep(1)
        print(f"[Payment] Processing... {i+1}/10")
    payment_id = f"PAY-{booking_id}"
    print(f"[Payment] Charge completed: {payment_id}")
    return {"payment_id": payment_id, "amount": amount}


@workflow(event_handler=True)
async def cancellable_booking(
    ctx: WorkflowContext, input: BookingInput
) -> BookingResult:
    """
    Cancellable travel booking workflow

    This workflow can be cancelled from Viewer.
    When cancelled, compensation transactions are executed in reverse order.

    CloudEvent type: "cancellable_booking"
    Data: {"booking_id": "BOOK-XXX", "destination": "Tokyo", "amount": 1500.0}
    """
    print(f"\n{'='*60}")
    print(f"🎫 Starting cancellable booking: {input.booking_id}")
    print(f"📍 Destination: {input.destination}, 💰 Amount: ${input.amount}")
    print(f"{'='*60}\n")

    # Step 1: Reserve hotel
    hotel_result = await reserve_hotel_room(ctx, input.booking_id)
    print(f"✓ Hotel: {hotel_result}")

    # Step 2: Reserve flight
    flight_result = await reserve_flight_ticket(ctx, input.booking_id)
    print(f"✓ Flight: {flight_result}")

    # Step 3: Payment processing (takes 10 seconds - can be cancelled during this time)
    print("\n⏰ Payment processing (10 seconds)...")
    print("💡 You can cancel from Viewer: http://localhost:8080\n")
    payment_result = await charge_customer(ctx, input.booking_id, input.amount)
    print(f"✓ Payment: {payment_result}")

    print(f"\n{'='*60}")
    print("✅ Booking completed successfully!")
    print(f"{'='*60}\n")

    return BookingResult(
        booking_id=input.booking_id,
        hotel=hotel_result["hotel_reservation_id"],
        flight=flight_result["flight_reservation_id"],
        payment=payment_result["payment_id"],
        status="completed",
    )


# ========== Event Waiting Workflow (wait_event example) ==========

@activity
async def start_payment_processing(_ctx: WorkflowContext, order_id: str) -> dict[str, Any]:
    """Start payment processing"""
    print(f"[Activity] Starting payment processing for order: {order_id}")
    payment_id = f"payment-{order_id}"
    return {"payment_id": payment_id, "status": "pending"}


@activity
async def complete_order(_ctx: WorkflowContext, order_id: str, payment_data: dict[str, Any]) -> dict[str, Any]:
    """Complete order after payment confirmation"""
    print(f"[Activity] Completing order: {order_id}")
    print(f"[Activity] Payment data: {payment_data}")
    return {
        "order_id": order_id,
        "status": "completed",
        "payment_confirmed": True,
    }


@workflow(event_handler=True)
async def payment_workflow(ctx: WorkflowContext, input: PaymentWorkflowInput) -> PaymentWorkflowResult:  # noqa: ARG001
    """
    Workflow waiting for payment confirmation

    CloudEvent type: "payment_workflow"
    Data: {"order_id": "ORDER-12345", "amount": 99.99}

    This workflow:
    1. Starts payment processing
    2. Waits for external event (payment.completed)
    3. Completes order after receiving event

    Test with curl:
    # Start workflow
    curl -X POST http://localhost:8001/events \
      -H "Content-Type: application/json" \
      -H "CE-Type: payment_workflow" \
      -H "CE-Source: example" \
      -H "CE-ID: $(uuidgen)" \
      -H "CE-SpecVersion: 1.0" \
      -d '{"order_id": "ORDER-TEST", "amount": 199.99}'

    # Send payment.completed event
    curl -X POST http://localhost:8001/events \
      -H "Content-Type: application/json" \
      -H "CE-Type: payment.completed" \
      -H "CE-Source: payment-service" \
      -H "CE-ID: $(uuidgen)" \
      -H "CE-SpecVersion: 1.0" \
      -d '{"order_id": "ORDER-TEST", "payment_id": "PAY-123", "status": "success", "amount": 199.99}'
    """
    from edda import wait_event

    print(f"\n{'='*60}")
    print(f"[Workflow] Starting payment workflow for order: {input.order_id}")
    print(f"[Workflow] Amount: ${input.amount}")
    print(f"[Workflow] Instance ID: {ctx.instance_id}")
    print(f"{'='*60}\n")

    # Step 1: Start payment processing
    payment_result = await start_payment_processing(ctx, input.order_id)
    print(f"[Workflow] Payment initiated: {payment_result}")

    # Step 2: Wait for payment.completed event
    print("\n[Workflow] ⏸️  Waiting for payment confirmation event...")
    print("[Workflow] Expected event type: 'payment.completed'")
    print(f"[Workflow] Expected data: order_id = '{input.order_id}'")
    print("\n💡 To complete this workflow, send payment.completed event:")
    print("curl -X POST http://localhost:8001/ \\")
    print('  -H "Content-Type: application/json" \\')
    print('  -H "CE-Type: payment.completed" \\')
    print('  -H "CE-Source: payment-service" \\')
    print('  -H "CE-ID: $(uuidgen)" \\')
    print('  -H "CE-SpecVersion: 1.0" \\')
    print(f'  -H "CE-Eddainstanceid: {ctx.instance_id}" \\')
    print(f"  -d '{{\"order_id\": \"{input.order_id}\", \"payment_id\": \"PAY-123\", \"status\": \"success\", \"amount\": {input.amount}}}'\n")

    payment_event = await wait_event(
        ctx,
        event_type="payment.completed",
        timeout_seconds=300,  # 5 minute timeout
    )
    print(f"\n[Workflow] ✅ Payment event received from {payment_event.source}: {payment_event.data}")
    print(f"[Workflow] Event ID: {payment_event.id}, Time: {payment_event.time}")

    # Step 3: Complete order
    final_result = await complete_order(ctx, input.order_id, payment_event.data)
    print(f"[Workflow] ✅ Order completed: {final_result}")

    print(f"\n{'='*60}")
    print("[Workflow] Payment workflow completed successfully!")
    print(f"{'='*60}\n")

    return PaymentWorkflowResult(
        order_id=final_result["order_id"],
        status=final_result["status"],
        payment_confirmed=final_result["payment_confirmed"],
    )


# ========== Match-Case Workflow (Python 3.10+) ==========

@activity
async def notify_pending(_ctx: WorkflowContext, order_id: str) -> dict[str, str]:
    """Send notification (pending)"""
    print(f"[Activity] Sending pending notification for: {order_id}")
    return {"status": "notified", "order_id": order_id}


@activity
async def process_approved(_ctx: WorkflowContext, order_id: str) -> dict[str, str]:
    """Process approved order"""
    print(f"[Activity] Processing approved order: {order_id}")
    return {"status": "processed", "order_id": order_id}


@activity
async def handle_rejected(_ctx: WorkflowContext, order_id: str, reason: str) -> dict[str, str]:
    """Handle rejected order"""
    print(f"[Activity] Handling rejected order: {order_id}, reason: {reason}")
    return {"status": "rejected_handled", "order_id": order_id, "reason": reason}


@activity
async def escalate_unknown(_ctx: WorkflowContext, order_id: str, status: str) -> dict[str, str]:
    """Escalate unknown status"""
    print(f"[Activity] Escalating unknown status: {order_id}, status: {status}")
    return {"status": "escalated", "order_id": order_id, "original_status": status}


@workflow(event_handler=True)
async def match_case_workflow(
    ctx: WorkflowContext, input: MatchCaseInput
) -> MatchCaseResult:
    """
    Order processing workflow using match-case statement (Python 3.10+)

    This workflow uses structural pattern matching (match-case) to perform
    different operations based on order status.

    CloudEvent type: "match_case_workflow"
    Data: {
        "order_id": "ORDER-12345",
        "order_status": "pending" | "approved" | "accepted" | "rejected" | "cancelled" | other,
        "reason": "optional reason string"
    }

    Example (test with curl):
    curl -X POST http://localhost:8001 \\
      -H "Content-Type: application/cloudevents+json" \\
      -d '{
        "specversion": "1.0",
        "type": "match_case_workflow",
        "source": "demo",
        "id": "test-1",
        "data": {
          "order_id": "ORDER-001",
          "order_status": "pending"
        }
      }'
    """
    print(f"\n{'='*60}")
    print(f"[Workflow] Match-case workflow for order: {input.order_id}")
    print(f"[Workflow] Status: {input.order_status}")
    print(f"{'='*60}\n")

    # Process according to status using match-case statement
    match input.order_status:
        case "pending":
            # Pending order - send notification
            result = await notify_pending(ctx, input.order_id)
            print(f"[Workflow] Pending order notification sent: {result}")

        case "approved" | "accepted":
            # Approved order (multiple patterns) - proceed with processing
            result = await process_approved(ctx, input.order_id)
            print(f"[Workflow] Approved order processed: {result}")

        case "rejected" | "cancelled" if input.reason:
            # Rejected/cancelled with reason - with guard
            result = await handle_rejected(ctx, input.order_id, input.reason)
            print(f"[Workflow] Rejected order handled: {result}")

        case _:
            # Other status - escalate
            result = await escalate_unknown(ctx, input.order_id, input.order_status)
            print(f"[Workflow] Unknown status escalated: {result}")

    print(f"\n{'='*60}")
    print("[Workflow] Match-case workflow completed!")
    print(f"{'='*60}\n")

    return MatchCaseResult(
        order_id=input.order_id,
        original_status=input.order_status,
        result=result,
        workflow_type="match_case"
    )


# ========== Advanced Types Demo Workflow (Enum + list + dict) ==========

@activity
async def process_advanced_order(
    _ctx: WorkflowContext,
    order_id: str,
    status: OrderStatus,
    priority: Priority,
    items: list[dict[str, Any]],
    metadata: dict[str, str],
    tags: list[str],
) -> dict[str, Any]:
    """Process an advanced order with complex types."""
    print(f"[Activity] Processing advanced order: {order_id}")
    print(f"  Status: {status.value}, Priority: {priority.value}")
    print(f"  Items: {items}")
    print(f"  Metadata: {metadata}")
    print(f"  Tags: {tags}")

    return {
        "order_id": order_id,
        "status": status.value,
        "priority": priority.value,
        "processed": True,
        "item_count": len(items),
        "tags_count": len(tags),
    }


@workflow(event_handler=True)
async def advanced_types_demo_workflow(
    ctx: WorkflowContext,
    input: AdvancedTypesInput,
) -> AdvancedTypesResult:
    """
    Advanced types demonstration workflow.

    This workflow showcases the Viewer's auto-form generation with:
    - Enum type (status, priority) - Dropdown UI
    - list[dict] type (items) - Dynamic table/list UI
    - dict[str, str] type (metadata) - Dynamic key-value form
    - list[str] type (tags) - Dynamic list UI

    CloudEvent type: "advanced_types_demo_workflow"

    Example data:
    {
        "order_id": "ORDER-ADV-001",
        "status": "processing",
        "items": [
            {"item_id": "ITEM-1", "name": "Product A", "price": 29.99, "quantity": 2},
            {"item_id": "ITEM-2", "name": "Product B", "price": 49.99, "quantity": 1}
        ],
        "metadata": {
            "customer_id": "CUST-123",
            "payment_method": "credit_card",
            "shipping_method": "express"
        },
        "tags": ["urgent", "vip", "gift_wrap"],
        "priority": 3
    }

    Try it in Viewer:
    1. Click "Start New Workflow"
    2. Select "advanced_types_demo_workflow"
    3. Fill in the auto-generated form fields:
       - order_id: text input
       - status: dropdown (PENDING/PROCESSING/COMPLETED/CANCELLED)
       - items: dynamic list with +/- buttons
       - metadata: dynamic key-value pairs with +/- buttons
       - tags: dynamic list with +/- buttons
       - priority: dropdown (LOW/MEDIUM/HIGH/URGENT)
    4. Click "Start"
    """
    print(f"\n{'='*60}")
    print("[Workflow] Advanced Types Demo Workflow")
    print(f"[Workflow] Order ID: {input.order_id}")
    print(f"[Workflow] Status: {input.status.name} ({input.status.value})")
    print(f"[Workflow] Priority: {input.priority.name} ({input.priority.value})")
    print(f"{'='*60}\n")

    # Process the order
    result = await process_advanced_order(
        ctx, input.order_id, input.status, input.priority, input.items, input.metadata, input.tags
    )
    print(f"[Workflow] Order processed: {result}")

    print(f"\n{'='*60}")
    print("[Workflow] Advanced Types Demo Workflow completed!")
    print(f"{'='*60}\n")

    return AdvancedTypesResult(
        order_id=input.order_id,
        status=input.status.value,
        priority=input.priority.value,
        result=result,
        workflow_type="advanced_types_demo"
    )


# ========== Timer Waiting Workflow (wait_timer example) ==========

@activity
async def create_order_with_timeout(_ctx: WorkflowContext, order_id: str, amount: float) -> dict[str, Any]:
    """Create order with timeout"""
    print(f"[Activity] Creating order: {order_id}, amount: ${amount}")
    return {
        "order_id": order_id,
        "amount": amount,
        "status": "pending_payment",
        "created_at": "2025-10-29T00:00:00Z",
    }


@activity
async def check_payment_status(_ctx: WorkflowContext, order_id: str) -> dict[str, Any]:
    """Check payment status"""
    print(f"[Activity] Checking payment status for order: {order_id}")
    # Simulate payment status check (hash-based for determinism)
    is_paid = hash(order_id) % 3 == 0  # 33% paid, 67% unpaid
    return {
        "order_id": order_id,
        "is_paid": is_paid,
        "payment_method": "credit_card" if is_paid else None,
        "checked_at": "2025-10-29T00:00:30Z",
    }


@activity
async def auto_cancel_order(_ctx: WorkflowContext, order_id: str) -> dict[str, Any]:
    """Auto-cancel order"""
    print(f"[Activity] Auto-cancelling order: {order_id}")
    return {
        "order_id": order_id,
        "status": "auto_cancelled",
        "reason": "Payment timeout",
        "cancelled_at": "2025-10-29T00:00:30Z",
    }


@activity
async def confirm_order(_ctx: WorkflowContext, order_id: str) -> dict[str, Any]:
    """Confirm order"""
    print(f"[Activity] Confirming order: {order_id}")
    return {
        "order_id": order_id,
        "status": "confirmed",
        "confirmed_at": "2025-10-29T00:00:30Z",
    }


@workflow(event_handler=True)
async def order_with_auto_cancel_workflow(
    ctx: WorkflowContext,
    input: OrderWithAutoCancelInput,
) -> OrderWithAutoCancelResult:
    """
    Order processing workflow with timeout

    This workflow demonstrates wait_timer() functionality:
    1. Create order
    2. Wait for specified time (default 30 seconds) using wait_timer (payment grace period)
    3. Check payment status after timeout
    4. Auto-cancel if unpaid, confirm if paid

    CloudEvent type: "order_with_auto_cancel_workflow"
    Data: {"order_id": "ORDER-TIMEOUT-001", "amount": 99.99, "timeout_seconds": 30}

    Test with Viewer:
    1. Click "Start New Workflow"
    2. Select "order_with_auto_cancel_workflow"
    3. Enter parameters:
       - order_id: "ORDER-TEST-001"
       - amount: 99.99
       - timeout_seconds: 30 (optional, default 30 seconds)
    4. After starting, confirm status becomes "waiting_for_timer"
    5. Confirm workflow automatically resumes after 30 seconds

    Test with curl:
    # Start workflow
    curl -X POST http://localhost:8001/events \\
      -H "Content-Type: application/json" \\
      -H "CE-Type: order_with_auto_cancel_workflow" \\
      -H "CE-Source: demo" \\
      -H "CE-ID: $(uuidgen)" \\
      -H "CE-SpecVersion: 1.0" \\
      -d '{"order_id": "ORDER-TIMEOUT-001", "amount": 99.99, "timeout_seconds": 30}'

    Or CloudEvents JSON format:
    curl -X POST http://localhost:8001 \\
      -H "Content-Type: application/cloudevents+json" \\
      -d '{
        "specversion": "1.0",
        "type": "order_with_auto_cancel_workflow",
        "source": "demo",
        "id": "test-1",
        "data": {
          "order_id": "ORDER-TIMEOUT-001",
          "amount": 99.99,
          "timeout_seconds": 30
        }
      }'
    """
    from edda import wait_timer

    print(f"\n{'='*60}")
    print("[Workflow] Order with Auto-Cancel Workflow")
    print(f"[Workflow] Order ID: {input.order_id}")
    print(f"[Workflow] Amount: ${input.amount}")
    print(f"[Workflow] Payment timeout: {input.timeout_seconds} seconds")
    print(f"[Workflow] Instance ID: {ctx.instance_id}")
    print(f"{'='*60}\n")

    # Step 1: Create order
    order_result = await create_order_with_timeout(ctx, input.order_id, input.amount)
    print(f"[Workflow] Order created: {order_result}")

    # Step 2: Wait for payment grace period using wait_timer
    print(f"\n[Workflow] ⏱️  Waiting for {input.timeout_seconds} seconds (payment grace period)...")
    print("[Workflow] During this time, customer should complete payment")
    print("[Workflow] Status will show 'waiting_for_timer' in Viewer")
    print("[Workflow] Watch at: http://localhost:8080\n")

    await wait_timer(ctx, duration_seconds=input.timeout_seconds)

    print("\n[Workflow] ⏰ Timer expired! Checking payment status...")

    # Step 3: Check payment status
    payment_status = await check_payment_status(ctx, input.order_id)
    print(f"[Workflow] Payment status: {payment_status}")

    # Step 4: Process according to payment status
    if payment_status["is_paid"]:
        # Paid - confirm order
        result = await confirm_order(ctx, input.order_id)
        print(f"\n[Workflow] ✅ Order confirmed: {result}")
        final_status = "confirmed"
    else:
        # Unpaid - auto-cancel
        result = await auto_cancel_order(ctx, input.order_id)
        print(f"\n[Workflow] ❌ Order auto-cancelled: {result}")
        final_status = "auto_cancelled"

    print(f"\n{'='*60}")
    print("[Workflow] Order workflow completed!")
    print(f"[Workflow] Final status: {final_status}")
    print(f"{'='*60}\n")

    return OrderWithAutoCancelResult(
        order_id=input.order_id,
        amount=input.amount,
        timeout_seconds=input.timeout_seconds,
        payment_status=payment_status,
        final_result=result,
        final_status=final_status,
    )


# ========== Error Handling Demo Workflow ==========
# Pydantic Integration Demo: Stack trace display for validation errors


class ExternalAPIError(RuntimeError):
    """External API call error"""
    pass


@activity
async def validate_and_create_user(
    ctx: WorkflowContext,  # noqa: ARG001
    user_data: UserData  # Pydantic model with built-in validation
) -> UserCreationResult:
    """
    Validate and create user data (Pydantic integration)

    Automatic validation by Pydantic:
    - name: 2 characters or more
    - email: Valid email format (@ and domain required)
    - age: 18 years or older and 120 years or younger

    Validation errors are raised as Pydantic ValidationError,
    and displayed as beautiful stack traces in Viewer.
    """
    print(f"[Activity] Validating user data: {user_data}")
    print("[Activity] User data passed Pydantic validation automatically!")

    # Since it's a Pydantic model, all validation is already complete at this point
    user_id = f"USER-{hash(user_data.email)}"
    print(f"[Activity] User created successfully: {user_id}")
    return UserCreationResult(
        user_id=user_id,
        name=user_data.name,
        email=user_data.email,
        status="created",
    )


def call_external_payment_api(transaction_id: str, amount: float) -> PaymentAPIResponse:
    """Call external payment API (simulation)"""
    # Raise errors under specific conditions
    if amount > 10000:
        raise ExternalAPIError(
            f"Payment API error: Amount ${amount} exceeds transaction limit of $10,000 "
            f"(transaction_id: {transaction_id})"
        )

    if transaction_id.startswith("FAIL-"):
        raise ExternalAPIError(
            f"Payment API error: Transaction declined by payment gateway "
            f"(transaction_id: {transaction_id}, error_code: DECLINED_001)"
        )

    return PaymentAPIResponse(
        transaction_id=transaction_id,
        status="success",
        payment_gateway_response="APPROVED",
    )


@activity
async def process_payment_with_api(
    ctx: WorkflowContext,  # noqa: ARG001
    user_id: str,
    amount: float,
    transaction_id: str,
) -> PaymentProcessingResult:
    """
    Process payment via external payment API (Pydantic integration)

    This Activity simulates external API call errors.
    """
    print(f"[Activity] Processing payment via API: user={user_id}, amount=${amount}")

    # External API call (Pydantic return type)
    api_response = call_external_payment_api(transaction_id, amount)

    print(f"[Activity] Payment processed: {api_response}")
    return PaymentProcessingResult(
        user_id=user_id,
        amount=amount,
        transaction_id=transaction_id,
        api_response=api_response,
        status="completed",
    )


@activity
async def send_confirmation_email(
    ctx: WorkflowContext,  # noqa: ARG001
    user_email: str,
    order_details: dict[str, Any],
) -> dict[str, Any]:
    """Send confirmation email"""
    print(f"[Activity] Sending confirmation email to: {user_email}")
    return {
        "email": user_email,
        "order_details": order_details,
        "sent": True,
        "sent_at": "2025-10-30T12:00:00Z",
    }


@workflow(event_handler=True)
async def error_handling_demo_workflow(
    ctx: WorkflowContext,
    input: ErrorHandlingInput,
) -> ErrorHandlingResult:
    """
    Error Handling Demo Workflow

    This workflow demonstrates the following error patterns:
    1. Data validation errors (nested function calls)
    2. External API call errors
    3. Detailed stack trace display

    CloudEvent type: "error_handling_demo_workflow"

    Test Cases:

    # Success case:
    curl -X POST http://localhost:8001 \\
      -H "Content-Type: application/cloudevents+json" \\
      -d '{
        "specversion": "1.0",
        "type": "error_handling_demo_workflow",
        "source": "demo-client",
        "id": "error-demo-001",
        "datacontenttype": "application/json",
        "data": {
          "user_name": "Alice Smith",
          "user_email": "alice@example.com",
          "user_age": 25,
          "payment_amount": 99.99,
          "transaction_id": "TXN-SUCCESS-001"
        }
      }'

    # Error case 1: Invalid email address (check stack trace)
    curl -X POST http://localhost:8001 \\
      -H "Content-Type: application/cloudevents+json" \\
      -d '{
        "specversion": "1.0",
        "type": "error_handling_demo_workflow",
        "source": "demo-client",
        "id": "error-demo-002",
        "datacontenttype": "application/json",
        "data": {
          "user_name": "Bob",
          "user_email": "invalid-email",
          "user_age": 30,
          "payment_amount": 50.00,
          "transaction_id": "TXN-002"
        }
      }'

    # Error case 2: Age restriction error
    curl -X POST http://localhost:8001 \\
      -H "Content-Type: application/cloudevents+json" \\
      -d '{
        "specversion": "1.0",
        "type": "error_handling_demo_workflow",
        "source": "demo-client",
        "id": "error-demo-003",
        "datacontenttype": "application/json",
        "data": {
          "user_name": "Charlie",
          "user_email": "charlie@example.com",
          "user_age": 16,
          "payment_amount": 75.00,
          "transaction_id": "TXN-003"
        }
      }'

    # Error case 3: Payment amount exceeded error (external API)
    curl -X POST http://localhost:8001 \\
      -H "Content-Type: application/cloudevents+json" \\
      -d '{
        "specversion": "1.0",
        "type": "error_handling_demo_workflow",
        "source": "demo-client",
        "id": "error-demo-004",
        "datacontenttype": "application/json",
        "data": {
          "user_name": "Diana",
          "user_email": "diana@example.com",
          "user_age": 35,
          "payment_amount": 15000.00,
          "transaction_id": "TXN-004"
        }
      }'

    # Error case 4: Payment gateway rejection error
    curl -X POST http://localhost:8001 \\
      -H "Content-Type: application/cloudevents+json" \\
      -d '{
        "specversion": "1.0",
        "type": "error_handling_demo_workflow",
        "source": "demo-client",
        "id": "error-demo-005",
        "datacontenttype": "application/json",
        "data": {
          "user_name": "Eve",
          "user_email": "eve@example.com",
          "user_age": 28,
          "payment_amount": 120.00,
          "transaction_id": "FAIL-TXN-005"
        }
      }'

    Check in Viewer:
    - Display workflow list at http://localhost:8080
    - Click on the failed workflow
    - Expand stack trace in the error section
    """
    print(f"\n{'='*60}")
    print("[Workflow] Error Handling Demo Workflow")
    print(f"[Workflow] User: {input.user_name} ({input.user_email})")
    print(f"[Workflow] Payment: ${input.payment_amount}")
    print(f"[Workflow] Instance ID: {ctx.instance_id}")
    print(f"{'='*60}\n")

    # Step 1: Validate and create user data (Pydantic automatic validation)
    print("[Workflow] Step 1: Validating and creating user...")
    user_data = UserData(
        name=input.user_name,
        email=input.user_email,
        age=input.user_age,
    )
    user_result = await validate_and_create_user(ctx, user_data)
    user_id = user_result.user_id  # Pydantic model attribute access
    print(f"[Workflow] ✅ User created: {user_id}")

    # Step 2: Process payment (external API call, error may occur)
    print("\n[Workflow] Step 2: Processing payment via external API...")
    payment_result = await process_payment_with_api(
        ctx,
        user_id=user_id,
        amount=input.payment_amount,
        transaction_id=input.transaction_id,
    )
    print(f"[Workflow] ✅ Payment processed: {payment_result.transaction_id}")  # Pydantic model attribute access

    # Step 3: Send confirmation email
    print("\n[Workflow] Step 3: Sending confirmation email...")
    email_result = await send_confirmation_email(
        ctx,
        user_email=input.user_email,
        order_details={
            "user_id": user_id,
            "amount": input.payment_amount,
            "transaction_id": input.transaction_id,
        },
    )
    print(f"[Workflow] ✅ Confirmation email sent to: {input.user_email}")

    print(f"\n{'='*60}")
    print("[Workflow] Workflow completed successfully!")
    print(f"[Workflow] User ID: {user_id}")
    print(f"[Workflow] Transaction ID: {input.transaction_id}")
    print(f"{'='*60}\n")

    return ErrorHandlingResult(
        user_id=user_id,
        user_name=input.user_name,
        user_email=input.user_email,
        payment_amount=input.payment_amount,
        transaction_id=input.transaction_id,
        user_result=user_result,
        payment_result=payment_result,
        email_result=email_result,
        status="completed",
    )


# =============================================================================
# Example 11: Scheduled Order Shipment Workflow (wait_until)
# =============================================================================


@activity
async def create_scheduled_order(_ctx: WorkflowContext, order_id: str, shipment_time: str) -> dict:
    """
    Create order and set scheduled shipment time.

    Args:
        ctx: Workflow context
        order_id: Order ID
        shipment_time: ISO 8601 format datetime string

    Returns:
        Order details with shipment schedule
    """
    print(f"\n{'='*60}")
    print(f"[CREATE ORDER] Order ID: {order_id}")
    print(f"[CREATE ORDER] Scheduled shipment: {shipment_time}")
    print(f"{'='*60}\n")

    return {
        "order_id": order_id,
        "shipment_time": shipment_time,
        "status": "scheduled",
        "created_at": "2025-10-31T10:00:00Z",
    }


@activity
async def prepare_shipment(_ctx: WorkflowContext, order_id: str) -> dict:
    """
    Prepare shipment at scheduled time.

    Args:
        ctx: Workflow context
        order_id: Order ID

    Returns:
        Shipment preparation details
    """
    print(f"\n{'='*60}")
    print(f"[PREPARE SHIPMENT] Order ID: {order_id}")
    print("[PREPARE SHIPMENT] Picking items from warehouse...")
    print("[PREPARE SHIPMENT] Packing order...")
    print(f"{'='*60}\n")

    return {
        "order_id": order_id,
        "status": "prepared",
        "tracking_number": f"TRACK-{order_id[:8].upper()}",
    }


@activity
async def dispatch_shipment(_ctx: WorkflowContext, order_id: str, tracking_number: str) -> dict:
    """
    Dispatch shipment to carrier.

    Args:
        ctx: Workflow context
        order_id: Order ID
        tracking_number: Tracking number

    Returns:
        Dispatch confirmation
    """
    print(f"\n{'='*60}")
    print(f"[DISPATCH SHIPMENT] Order ID: {order_id}")
    print(f"[DISPATCH SHIPMENT] Tracking: {tracking_number}")
    print("[DISPATCH SHIPMENT] Handed over to carrier")
    print(f"{'='*60}\n")

    return {
        "order_id": order_id,
        "tracking_number": tracking_number,
        "status": "dispatched",
        "carrier": "FastShip Express",
        "dispatched_at": "2025-11-01T09:00:00Z",
    }


@workflow(event_handler=True)
async def scheduled_order_shipment_workflow(
    ctx: WorkflowContext,
    input: ScheduledShipmentInput,
) -> ScheduledShipmentResult:
    """
    Scheduled Order Shipment Workflow (wait_until demo).

    This workflow demonstrates using wait_until() to wait until a specific
    absolute time before executing shipment operations. Unlike wait_timer()
    which uses relative durations, wait_until() accepts an absolute datetime.

    Demonstrates:
    - wait_until() for absolute time waiting
    - ISO 8601 datetime parsing
    - Timezone handling (UTC)
    - Scheduled task execution at specific time

    CloudEvent type: "scheduled_order_shipment_workflow"
    Data: {
        "order_id": "ORD-12345",
        "shipment_datetime": "2025-11-01T09:00:00Z"  # ISO 8601 UTC time
    }

    How to trigger this workflow:
    1. From Viewer UI:
       - Click "Start New Workflow"
       - Select "scheduled_order_shipment_workflow"
       - Enter order_id: "ORD-12345"
       - Enter shipment_datetime: "2025-11-01T09:00:00Z" (future time in UTC)
       - Click "Start Workflow"

    2. Using curl (CloudEvents):
       curl -X POST http://localhost:8001/events \\
         -H "Content-Type: application/cloudevents+json" \\
         -d '{
           "specversion": "1.0",
           "type": "scheduled_order_shipment_workflow",
           "source": "demo-client",
           "id": "demo-scheduled-shipment-1",
           "datacontenttype": "application/json",
           "data": {
             "order_id": "ORD-12345",
             "shipment_datetime": "2025-11-01T09:00:00Z"
           }
         }'

    Note: For quick testing, use a datetime a few seconds in the future:
      shipment_datetime: "2025-10-31T10:35:00Z"  # Replace with current UTC time + 30 seconds

    Args:
        ctx: Workflow context
        order_id: Order ID (e.g., "ORD-12345")
        shipment_datetime: ISO 8601 datetime string in UTC (e.g., "2025-11-01T09:00:00Z")

    Returns:
        Shipment completion status with tracking information
    """
    from datetime import datetime

    # Step 1: Create order and set shipment schedule
    _ = await create_scheduled_order(ctx, input.order_id, input.shipment_datetime)

    # Parse ISO 8601 datetime string to datetime object
    shipment_time = datetime.fromisoformat(input.shipment_datetime.replace("Z", "+00:00"))

    print(f"\n{'='*60}")
    print(f"[WORKFLOW] Waiting until: {shipment_time.isoformat()}")
    print(f"[WORKFLOW] Current time: {datetime.now().isoformat()}")
    print("[WORKFLOW] This workflow will resume at the scheduled time...")
    print(f"{'='*60}\n")

    # Step 2: Wait until scheduled shipment time (absolute time)
    # This is the key difference from wait_timer() which uses relative duration
    await wait_until(ctx, until_time=shipment_time)

    print(f"\n{'='*60}")
    print("[WORKFLOW] Scheduled time reached! Proceeding with shipment...")
    print(f"{'='*60}\n")

    # Step 3: Prepare shipment at scheduled time
    shipment_prep = await prepare_shipment(ctx, input.order_id)

    # Step 4: Dispatch shipment
    dispatch_result = await dispatch_shipment(ctx, input.order_id, shipment_prep["tracking_number"])

    return ScheduledShipmentResult(
        order_id=input.order_id,
        shipment_datetime=input.shipment_datetime,
        tracking_number=dispatch_result["tracking_number"],
        carrier=dispatch_result["carrier"],
        status="completed",
        message=f"Order {input.order_id} dispatched successfully at scheduled time",
    )


# ========== Channel-based Message Queue Workflows ==========
# Demonstrates Erlang/Elixir mailbox pattern with competing and broadcast modes


@activity
async def execute_job(ctx: WorkflowContext, job_id: str, job_data: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG001
    """
    Execute a job from the competing queue.

    This simulates processing a job (e.g., sending emails, generating reports).
    """
    print(f"[Activity] Executing job: {job_id}")
    print(f"[Activity] Job data: {job_data}")
    # Simulate some work
    return {
        "job_id": job_id,
        "processed": True,
        "result": f"Job {job_id} completed successfully",
    }


@activity
async def send_notification(
    ctx: WorkflowContext, notification_id: str, notification_data: dict[str, Any]  # noqa: ARG001
) -> dict[str, Any]:
    """
    Send a notification (e.g., push notification, SMS, email).

    This is used in broadcast mode where all subscribers receive all messages.
    """
    print(f"[Activity] Sending notification: {notification_id}")
    print(f"[Activity] Notification data: {notification_data}")
    return {
        "notification_id": notification_id,
        "sent": True,
        "channel": notification_data.get("channel", "default"),
    }


@workflow(event_handler=True)
async def job_worker_workflow(
    ctx: WorkflowContext,
    input: JobWorkerInput,
) -> JobWorkerResult:
    """
    Job Worker Workflow (Competing Mode Demo).

    This workflow demonstrates the competing consumer pattern where each message
    (job) is processed by only ONE subscriber. This is useful for:
    - Job queues / task distribution
    - Work stealing patterns
    - Load balancing across workers

    Multiple instances of this workflow can run concurrently, each competing
    for jobs from the "jobs" channel. When a job is published, only one
    worker will receive and process it.

    Demonstrates:
    - subscribe() with mode="competing"
    - receive() to get the next available job
    - Message claiming (only one worker gets each message)

    CloudEvent type: "job_worker_workflow"
    Data: {"worker_id": "worker-1"}

    How to use:
    1. Start multiple worker instances:
       - Start this workflow multiple times with different worker_id values
       - Each worker will subscribe to the "jobs" channel in competing mode

    2. Publish jobs using the job_publisher_workflow:
       curl -X POST http://localhost:8001/ \\
         -H "Content-Type: application/cloudevents+json" \\
         -d '{
           "specversion": "1.0",
           "type": "job_publisher_workflow",
           "source": "demo-client",
           "id": "job-1",
           "datacontenttype": "application/json",
           "data": {
             "task": "send_report"
           }
         }'

    3. Only one worker will receive and process each job

    Args:
        ctx: Workflow context
        input: Worker configuration (worker_id)

    Returns:
        JobWorkerResult with processed job information
    """
    print(f"\n{'='*60}")
    print(f"[WORKFLOW] Job Worker {input.worker_id} starting...")
    print("[WORKFLOW] Subscribing to 'jobs' channel in COMPETING mode")
    print(f"{'='*60}\n")

    # Subscribe to the jobs channel in competing mode
    # Each job will be processed by only ONE worker
    await subscribe(ctx, "jobs", mode="competing")

    print(f"[WORKFLOW] Worker {input.worker_id} waiting for jobs...")

    # Receive a job from the queue
    # This will block until a job is available, then return immediately
    job_message = await receive(ctx, channel="jobs")

    print(f"\n{'='*60}")
    print(f"[WORKFLOW] Worker {input.worker_id} received job!")
    print(f"[WORKFLOW] Job ID: {job_message.id}")
    print(f"[WORKFLOW] Job data: {job_message.data}")
    print(f"{'='*60}\n")

    # Cast data to dict (we know it's a dict in this demo, not bytes)
    job_data = cast(dict[str, Any], job_message.data)

    # Process the job (activity_id auto-generated: "execute_job:1")
    await execute_job(ctx, job_message.id, job_data)

    return JobWorkerResult(
        worker_id=input.worker_id,
        job_id=job_message.id,
        job_data=job_data,
        status="completed",
    )


@workflow(event_handler=True)
async def notification_service_workflow(
    ctx: WorkflowContext,
    input: NotificationServiceInput,
) -> NotificationServiceResult:
    """
    Notification Service Workflow (Broadcast Mode Demo).

    This workflow demonstrates the broadcast (fan-out) pattern where ALL
    subscribers receive ALL messages. This is useful for:
    - Event notifications
    - Audit logging
    - Real-time updates to multiple services

    Multiple instances of this workflow can run concurrently, and when a
    notification is published, ALL instances will receive it.

    Demonstrates:
    - subscribe() with mode="broadcast" (default)
    - receive() to get notifications
    - Fan-out pattern (all subscribers get all messages)

    CloudEvent type: "notification_service_workflow"
    Data: {"service_id": "notification-handler-1"}

    How to use:
    1. Start multiple notification service instances:
       - Start this workflow multiple times with different service_id values
       - Each instance will subscribe to the "notifications" channel

    2. Publish a notification using the notification_publisher_workflow:
       curl -X POST http://localhost:8001/ \\
         -H "Content-Type: application/cloudevents+json" \\
         -d '{
           "specversion": "1.0",
           "type": "notification_publisher_workflow",
           "source": "demo-client",
           "id": "notification-1",
           "datacontenttype": "application/json",
           "data": {
             "message": "System maintenance scheduled"
           }
         }'

    3. ALL notification service instances will receive the message

    Args:
        ctx: Workflow context
        input: Service configuration (service_id)

    Returns:
        NotificationServiceResult with notification handling information
    """
    print(f"\n{'='*60}")
    print(f"[WORKFLOW] Notification Service {input.service_id} starting...")
    print("[WORKFLOW] Subscribing to 'notifications' channel in BROADCAST mode")
    print(f"{'='*60}\n")

    # Subscribe to the notifications channel in broadcast mode (default)
    # All subscribers will receive all messages
    await subscribe(ctx, "notifications", mode="broadcast")

    print(f"[WORKFLOW] Service {input.service_id} waiting for notifications...")

    # Receive a notification
    # All subscribers will receive this notification
    notification = await receive(ctx, channel="notifications")

    print(f"\n{'='*60}")
    print(f"[WORKFLOW] Service {input.service_id} received notification!")
    print(f"[WORKFLOW] Notification ID: {notification.id}")
    print(f"[WORKFLOW] Notification data: {notification.data}")
    print(f"{'='*60}\n")

    # Cast data to dict (we know it's a dict in this demo, not bytes)
    notification_data = cast(dict[str, Any], notification.data)

    # Process the notification (activity_id auto-generated: "send_notification:1")
    await send_notification(ctx, notification.id, notification_data)

    return NotificationServiceResult(
        service_id=input.service_id,
        notification_id=notification.id,
        notification_data=notification_data,
        status="sent",
    )


@workflow(event_handler=True)
async def job_publisher_workflow(
    ctx: WorkflowContext,
    input: JobPublisherInput,
) -> dict[str, Any]:
    """
    Job Publisher Workflow.

    Publishes a job to the "jobs" channel. Used with job_worker_workflow
    to test the competing consumer pattern.

    CloudEvent type: "job_publisher_workflow"
    Data: {"task": "my-task-name"}
    """
    print(f"\n[PUBLISHER] Publishing job: {input.task}")
    await publish(ctx, "jobs", {"task": input.task})
    print("[PUBLISHER] Job published to 'jobs' channel")
    return {"published": True, "channel": "jobs", "task": input.task}


@workflow(event_handler=True)
async def notification_publisher_workflow(
    ctx: WorkflowContext,
    input: NotificationPublisherInput,
) -> dict[str, Any]:
    """
    Notification Publisher Workflow.

    Publishes a notification to the "notifications" channel. Used with
    notification_service_workflow to test the broadcast pattern.

    CloudEvent type: "notification_publisher_workflow"
    Data: {"message": "my-notification-message"}
    """
    print(f"\n[PUBLISHER] Publishing notification: {input.message}")
    await publish(ctx, "notifications", {"message": input.message})
    print("[PUBLISHER] Notification published to 'notifications' channel")
    return {"published": True, "channel": "notifications", "message": input.message}


# ========== Point-to-Point Messaging Workflows ==========


@workflow(event_handler=True)
async def direct_message_receiver_workflow(
    ctx: WorkflowContext,
    input: DirectMessageReceiverInput,
) -> DirectMessageReceiverResult:
    """
    Point-to-Point Receiver Workflow.

    Waits for a direct message from another workflow. Other workflows can send
    messages using send_to(ctx, instance_id, data).

    CloudEvent type: "direct_message_receiver_workflow"
    Data: {"receiver_id": "my-receiver-id"}
    """
    print(f"\n[RECEIVER] Starting receiver: {input.receiver_id}")
    print(f"[RECEIVER] Instance ID: {ctx.instance_id}")

    # Subscribe to direct channel for this instance
    # send_to() publishes to __direct__:{target_instance_id}
    direct_channel = f"__direct__:{ctx.instance_id}"
    await subscribe(ctx, direct_channel, mode="broadcast")

    print(f"[RECEIVER] Waiting for direct message on {direct_channel}...")

    # Wait for direct message
    message = await receive(ctx, channel=direct_channel)

    print(f"[RECEIVER] Received message: {message.data}")
    return DirectMessageReceiverResult(
        receiver_id=input.receiver_id,
        received_message=cast(dict[str, Any], message.data),
    )


@workflow(event_handler=True)
async def direct_message_sender_workflow(
    ctx: WorkflowContext,
    input: DirectMessageSenderInput,
) -> DirectMessageSenderResult:
    """
    Point-to-Point Sender Workflow.

    Sends a message directly to a specific workflow instance using send_to().

    CloudEvent type: "direct_message_sender_workflow"
    Data: {"target_instance_id": "...", "message": "Hello!"}
    """
    print(f"\n[SENDER] Sending message to: {input.target_instance_id}")
    print(f"[SENDER] Message: {input.message}")

    await send_to(ctx, input.target_instance_id, {"message": input.message})

    print("[SENDER] Message sent!")
    return DirectMessageSenderResult(
        sent=True,
        target_instance_id=input.target_instance_id,
        message=input.message,
    )


# Export as ASGI application
# No need to manually register event handlers!
application = app

# Export as WSGI application (for gunicorn, uWSGI, etc.)
# Usage: gunicorn demo_app:wsgi_application --workers 4
wsgi_application = create_wsgi_app(app)
