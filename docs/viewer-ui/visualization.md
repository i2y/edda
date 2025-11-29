# Workflow Visualization

The Edda Viewer UI provides workflow visualization and monitoring.

## Diagram Elements

### Nodes

**Nodes in Diagram**:

- **Start node**: Rounded rectangle with workflow name
- **Activity nodes**: Rectangles with status-specific emoji icons (✅, ⏳, ❌, ⏸️, ⏱️, 🚫, 🔄)
- **End node**: Rounded rectangle labeled "Complete"

**Status Colors**:

- ✅ **Green** - Completed successfully
- ⏳ **Yellow** - Currently executing
- ❌ **Red** - Failed
- ⏸️ **Blue** - Waiting for event
- ⏱️ **Cyan** - Waiting for timer
- 🚫 **Orange** - Cancelled
- 🔄 **Purple** - Compensating

### Edges

**Arrows in Diagram**:

- **Solid arrows**: Normal control flow between activities (green for executed paths)
- **Labeled arrows**: Conditional branches (e.g., "sufficient", "insufficient")
- **Dashed arrows**: Compensation flow (reverse order, orange)

### Conditional Branching Example

Workflows with conditional logic (if/else, match/case) are visualized with labeled branches:

![Workflow Detail Page](images/detail-page-loan-approval.png)

*Example: `loan_approval_workflow` detail page. Note the **Execution Flow diagram** (bottom left) showing conditional branches with "sufficient" vs "insufficient" paths based on credit score.*

The diagram clearly shows:

- Decision points (diamond shapes in some diagrams)
- Branch labels indicating conditions
- Different execution paths based on runtime data

**Match/Case Pattern:**

![Match Case Workflow](images/detail-page-match-case.png)

*Example: `match_case_workflow` showing multiple case branches with different handlers*

## Viewing Workflows

### Workflow List

The main page shows all workflow instances:

| Column | Description |
|--------|-------------|
| **Instance ID** | Unique workflow identifier |
| **Workflow Name** | Name of the workflow function |
| **Status** | Current status (running, completed, failed, etc.) |
| **Started At** | Workflow start timestamp |
| **Updated At** | Last update timestamp |
| **Actions** | View details, cancel, etc. |

### Status Badges

- ✅ **Completed**: Workflow finished successfully (green)
- ⏳ **Running**: Workflow currently executing (yellow)
- ❌ **Failed**: Workflow failed (red)
- ⏸️ **Waiting (Event)**: Waiting for external event (blue)
- ⏱️ **Waiting (Timer)**: Waiting for timer (cyan)
- 🚫 **Cancelled**: Manually cancelled (orange)
- 🔄 **Compensating**: Executing compensations (purple)

![Status Badges Example](images/workflow-list-view.png)

### Workflow List View

The main page displays workflow instances as interactive cards with search and filter capabilities:

![Workflow List View](images/workflow-list-view.png)

*Workflow list with search filter bar, status filter, date range picker, and pagination controls*

### Workflow Detail Page

Click on a workflow instance to see the detail page:

![Workflow Detail Page](images/detail-page-loan-approval.png)

*Workflow detail page showing the Overview Panel (top), Execution Flow diagram (bottom left), and Activity Details panel (bottom right).*

1. **Overview Panel** (top):

      - Instance ID, status, timestamps
      - Input parameters (JSON)
      - Output result (if completed)

2. **Execution Flow** (bottom left):

      - Visual graph of workflow structure
      - Color-coded execution status
      - Compensation flow (if applicable)

3. **Activity Details** (bottom right):

      - Step-by-step execution log
      - Activity results
      - Event data (for wait_event)
      - Error messages (if failed)

4. **Actions**:

      - **Cancel**: Stop running workflow

## Starting Workflows from Viewer

The Viewer UI can start workflows with automatic form generation:

### Automatic Form Generation

For Pydantic-based workflows, the Viewer generates input forms automatically.

![Start Workflow Dialog](images/workflow-selection-dropdown.png)

*Example: Workflow selection dropdown showing all registered workflows*

The form generator supports various field types based on Pydantic model definitions:

![Nested Pydantic Form](images/nested-pydantic-form.png)

*Auto-generated form with nested Pydantic models (items list, shipping_address) showing text inputs, number inputs, and nested model containers*

### Starting Workflow

1. Click "Start New Workflow" button
2. Select workflow from dropdown
3. Fill in parameters (auto-generated form)
4. Click "Start"
5. Workflow begins, redirects to detail page

## Viewing Real-time Updates

The Viewer UI displays the latest workflow state. To see updates:

- ↻ Refresh the browser page
- ↻ Navigate between pages
- ↻ Click workflow cards to see current status

Note: The Viewer does not currently implement automatic polling. Manual refresh is required to see the latest changes.

## Cancelling Workflows

Click the "Cancel" button on a running workflow:

1. Workflow status → `cancelled`
2. Compensation functions execute (if applicable)
3. Worker process released
4. Workflow cannot be resumed

## Compensation Visualization

When a workflow is cancelled or fails, and compensations run, the diagram shows:

- 🔄 Purple nodes: Compensation activities
- 🚫 Orange nodes: Cancelled activities
- Dashed arrows: Compensation flow (reverse order)

![Compensation Execution](images/compensation-execution.png)

*Saga compensation flow showing rollback activities (cancel_flight_ticket, cancel_hotel_room)*

## Event Waiting Visualization

Workflows can wait for external events using `wait_event()`. The Viewer displays waiting state:

![Event Waiting Visualization](images/wait-event-visualization.png)

*Workflow with wait_event showing event type, timeout, and activity details panel*

The diagram shows:
- Hexagon node: Event wait point
- Event type being waited for
- Timeout duration (if specified)
- ⏸️ Waiting (Event) status badge

## CLI Integration

Edda workflows can be triggered via CloudEvents using standard HTTP clients like `curl`:

![CloudEvents CLI Trigger](images/cloudevents-cli-trigger.png)

*Example: Triggering `payment_workflow` via curl with CloudEvents headers*

**CloudEvents HTTP Binding:**

```bash
# Start payment_workflow via CloudEvents
curl -X POST http://localhost:8001/ \
  -H "Content-Type: application/json" \
  -H "CE-Type: payment_workflow" \
  -H "CE-Source: example" \
  -H "CE-ID: $(uuidgen)" \
  -H "CE-SpecVersion: 1.0" \
  -d '{"order_id": "ORD-12345", "amount": 1000.0}'
```

**CloudEvents Headers:**

| Header | Description | Example |
|--------|-------------|---------|
| `CE-Type` | Event type (workflow name for auto-start) | `payment_workflow` |
| `CE-Source` | Event source identifier | `example` or `payment-service` |
| `CE-ID` | Unique event ID | `$(uuidgen)` generates UUID |
| `CE-SpecVersion` | CloudEvents spec version | `1.0` |
| `Content-Type` | Payload content type | `application/json` |

**Workflow Requirements:**

For CloudEvents auto-start, the workflow must have `event_handler=True`:

```python
@workflow(event_handler=True)  # ← Required for CloudEvents auto-start
async def payment_workflow(ctx: WorkflowContext, input: PaymentWorkflowInput):
    # ...
```

**Sending Events to Waiting Workflows:**

```bash
# Send payment.completed event to resume waiting workflow
curl -X POST http://localhost:8001/ \
  -H "Content-Type: application/json" \
  -H "CE-Type: payment.completed" \
  -H "CE-Source: payment-service" \
  -H "CE-ID: $(uuidgen)" \
  -H "CE-SpecVersion: 1.0" \
  -d '{"order_id": "ORD-12345", "payment_id": "PAY-123", "status": "success", "amount": 1000.0}'
```

This flexibility allows:

- ✅ Automated testing with shell scripts
- ✅ Integration with CI/CD pipelines
- ✅ External system integration
- ✅ Manual workflow triggering for debugging

## Next Steps

- **[Setup Guide](setup.md)**: Install and configure Viewer UI
- **[Examples](../examples/simple.md)**: See Viewer in action with examples
