"""
Tests for Pydantic model integration in Viewer UI form generation.

Tests cover:
- Pydantic model parameter detection
- JSON Schema generation from Pydantic models
- Nested Pydantic models
- Mixed Pydantic and primitive parameters
- List of Pydantic models
"""

from datetime import datetime

from pydantic import BaseModel, Field

from edda import workflow
from edda.context import WorkflowContext
from edda.viewer_ui.data_service import WorkflowDataService


# Test Pydantic models
class Address(BaseModel):
    """Address model for testing."""

    street: str
    city: str
    state: str = Field(..., pattern=r"^[A-Z]{2}$")
    zip_code: str = Field(..., pattern=r"^\d{5}$")


class Customer(BaseModel):
    """Customer model for testing."""

    customer_id: str = Field(..., pattern=r"^CUST-\d+$")
    name: str = Field(..., min_length=1, max_length=100)
    email: str
    age: int = Field(..., ge=18, le=120)
    address: Address


class OrderItem(BaseModel):
    """Order item model."""

    product_id: str
    quantity: int = Field(..., ge=1)
    unit_price: float = Field(..., ge=0.01)


class TestPydanticParameterDetection:
    """Test suite for detecting Pydantic model parameters."""

    def test_detect_simple_pydantic_parameter(self, sqlite_storage):
        """Test detection of simple Pydantic model parameter (multiple params, no expansion)."""

        @workflow
        async def customer_workflow(
            ctx: WorkflowContext, customer: Customer, order_id: str
        ) -> dict:
            """Workflow with Pydantic model parameter and primitive parameter."""
            return {"name": customer.name}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("customer_workflow")

        assert "customer" in params
        param_info = params["customer"]
        assert param_info["type"] == "pydantic"
        assert param_info["model_name"] == "Customer"
        assert param_info["required"] is True
        assert "json_schema" in param_info

    def test_detect_nested_pydantic_model(self, sqlite_storage):
        """Test detection of nested Pydantic model (multiple params, no expansion)."""

        @workflow
        async def customer_workflow(
            ctx: WorkflowContext, customer: Customer, order_id: str
        ) -> dict:
            """Workflow with nested Pydantic model and primitive parameter."""
            return {"name": customer.name}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("customer_workflow")

        param_info = params["customer"]
        schema = param_info["json_schema"]

        # Verify schema contains nested Address definition
        assert "properties" in schema
        assert "address" in schema["properties"]

    def test_mixed_pydantic_and_primitive_parameters(self, sqlite_storage):
        """Test workflow with both Pydantic and primitive parameters."""

        @workflow
        async def mixed_workflow(
            ctx: WorkflowContext, customer: Customer, order_id: str, amount: float
        ) -> dict:
            """Workflow with mixed parameters."""
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("mixed_workflow")

        # Verify Pydantic parameter
        assert params["customer"]["type"] == "pydantic"
        assert params["customer"]["model_name"] == "Customer"

        # Verify primitive parameters
        assert params["order_id"]["type"] == "str"
        assert params["amount"]["type"] == "float"

    def test_optional_pydantic_parameter(self, sqlite_storage):
        """Test optional Pydantic model parameter (multiple params, no expansion)."""

        @workflow
        async def optional_workflow(
            ctx: WorkflowContext, customer: Customer | None = None, order_id: str = "default"
        ) -> dict:
            """Workflow with optional Pydantic parameter and primitive parameter."""
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("optional_workflow")

        assert "customer" in params
        param_info = params["customer"]
        assert param_info["type"] == "pydantic"
        assert param_info["required"] is False
        assert param_info["default"] is None

    def test_list_of_pydantic_models(self, sqlite_storage):
        """Test list of Pydantic models parameter."""

        @workflow
        async def bulk_workflow(ctx: WorkflowContext, items: list[OrderItem]) -> dict:
            """Workflow with list of Pydantic models."""
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("bulk_workflow")

        assert "items" in params
        param_info = params["items"]
        assert param_info["type"] == "list"
        assert param_info["item_type"] == "pydantic"


class TestJSONSchemaGeneration:
    """Test suite for JSON Schema generation from Pydantic models."""

    def test_json_schema_contains_field_definitions(self, sqlite_storage):
        """Test that JSON Schema contains proper field definitions."""

        @workflow
        async def customer_workflow(
            ctx: WorkflowContext, customer: Customer, order_id: str
        ) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("customer_workflow")

        schema = params["customer"]["json_schema"]

        # Verify basic structure
        assert "type" in schema
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "required" in schema

        # Verify fields
        properties = schema["properties"]
        assert "customer_id" in properties
        assert "name" in properties
        assert "email" in properties
        assert "age" in properties
        assert "address" in properties

    def test_json_schema_contains_validation_rules(self, sqlite_storage):
        """Test that JSON Schema contains Pydantic validation rules."""

        @workflow
        async def customer_workflow(
            ctx: WorkflowContext, customer: Customer, order_id: str
        ) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("customer_workflow")

        schema = params["customer"]["json_schema"]
        properties = schema["properties"]

        # Verify field constraints are preserved
        # name has min/max length
        if "minLength" in properties["name"]:
            assert properties["name"]["minLength"] == 1
        if "maxLength" in properties["name"]:
            assert properties["name"]["maxLength"] == 100

        # age has min/max value
        if "minimum" in properties["age"]:
            assert properties["age"]["minimum"] == 18
        if "maximum" in properties["age"]:
            assert properties["age"]["maximum"] == 120

    def test_json_schema_with_nested_models(self, sqlite_storage):
        """Test JSON Schema generation with nested Pydantic models."""

        @workflow
        async def customer_workflow(
            ctx: WorkflowContext, customer: Customer, order_id: str
        ) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("customer_workflow")

        schema = params["customer"]["json_schema"]

        # Verify nested Address model is included
        # Pydantic v2 may use $defs or definitions for nested schemas
        if "$defs" in schema:
            assert "Address" in schema["$defs"]
        elif "definitions" in schema:
            assert "Address" in schema["definitions"]

    def test_json_schema_with_datetime_field(self, sqlite_storage):
        """Test JSON Schema generation with datetime field."""

        class Event(BaseModel):
            event_id: str
            timestamp: datetime

        @workflow
        async def event_workflow(ctx: WorkflowContext, event: Event, order_id: str) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("event_workflow")

        schema = params["event"]["json_schema"]
        properties = schema["properties"]

        # Verify datetime field is present
        assert "timestamp" in properties
        # Pydantic v2 uses "format": "date-time" for datetime fields
        if "format" in properties["timestamp"]:
            assert properties["timestamp"]["format"] == "date-time"


class TestBackwardCompatibility:
    """Test suite for backward compatibility with non-Pydantic parameters."""

    def test_primitive_types_still_work(self, sqlite_storage):
        """Test that primitive type detection still works."""

        @workflow
        async def primitive_workflow(
            ctx: WorkflowContext, name: str, age: int, price: float, active: bool
        ) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("primitive_workflow")

        assert params["name"]["type"] == "str"
        assert params["age"]["type"] == "int"
        assert params["price"]["type"] == "float"
        assert params["active"]["type"] == "bool"

    def test_dict_parameters_still_work(self, sqlite_storage):
        """Test that dict parameter detection still works."""

        @workflow
        async def dict_workflow(ctx: WorkflowContext, metadata: dict) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("dict_workflow")

        # dict without type parameters should fallback to "json"
        assert params["metadata"]["type"] in ("dict", "json")


class TestNestedModelsAndLists:
    """Test suite for nested models and lists with JSON textarea."""

    def test_nested_pydantic_model_as_subform(self, sqlite_storage):
        """Test that nested Pydantic model fields are expanded as sub-form."""

        class ShippingAddress(BaseModel):
            """Nested shipping address model."""

            street: str
            city: str
            zip_code: str

        class OrderInput(BaseModel):
            """Input with nested Pydantic model."""

            order_id: str = Field(..., pattern=r"^ORD-")
            customer_email: str
            shipping_address: ShippingAddress

        @workflow
        async def order_workflow(ctx: WorkflowContext, input: OrderInput) -> dict:
            """Workflow with nested Pydantic model."""
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("order_workflow")

        # Should expand flat fields
        assert "order_id" in params
        assert params["order_id"]["type"] == "str"

        assert "customer_email" in params
        assert params["customer_email"]["type"] == "str"

        # Nested model should be expanded as sub-form
        assert "shipping_address.street" in params
        assert params["shipping_address.street"]["type"] == "str"
        assert params["shipping_address.street"]["_parent_field"] == "shipping_address"

        assert "shipping_address.city" in params
        assert "shipping_address.zip_code" in params

        # All fields should have _pydantic_model_name for reconstruction
        assert params["order_id"]["_pydantic_model_name"] == "input"
        assert params["shipping_address.street"]["_pydantic_model_name"] == "input"

    def test_list_of_pydantic_models_as_dynamic_list(self, sqlite_storage):
        """Test that list[PydanticModel] fields are expanded as dynamic list."""

        class OrderItem(BaseModel):
            """Order item model."""

            item_id: str
            quantity: int = Field(..., ge=1)
            price: float = Field(..., gt=0)

        class OrderInput(BaseModel):
            """Input with list of Pydantic models."""

            order_id: str
            items: list[OrderItem]

        @workflow
        async def order_workflow(ctx: WorkflowContext, input: OrderInput) -> dict:
            """Workflow with list of Pydantic models."""
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("order_workflow")

        # Flat field should expand
        assert "order_id" in params
        assert params["order_id"]["type"] == "str"

        # list[OrderItem] should be list_of_pydantic
        assert "items" in params
        assert params["items"]["type"] == "list_of_pydantic"
        assert "item_fields" in params["items"]
        assert "item_schema" in params["items"]

        # Verify item_fields are expanded
        item_fields = params["items"]["item_fields"]
        assert "item_id" in item_fields
        assert "quantity" in item_fields
        assert "price" in item_fields

        # All fields should have _pydantic_model_name for reconstruction
        assert params["order_id"]["_pydantic_model_name"] == "input"
        assert params["items"]["_pydantic_model_name"] == "input"

    def test_list_of_primitives_as_json_textarea(self, sqlite_storage):
        """Test that list[str], list[int] etc. are expanded as JSON textarea."""

        class InputWithLists(BaseModel):
            """Input with lists of primitives."""

            name: str
            tags: list[str]
            scores: list[int]

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: InputWithLists) -> dict:
            """Workflow with list of primitives."""
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Flat field should expand
        assert "name" in params
        assert params["name"]["type"] == "str"

        # list[str] should be JSON textarea
        assert "tags" in params
        assert params["tags"]["type"] == "json"
        assert params["tags"]["json_schema"]["type"] == "array"
        assert params["tags"]["json_schema"]["items"]["type"] == "string"

        # list[int] should be JSON textarea
        assert "scores" in params
        assert params["scores"]["type"] == "json"
        assert params["scores"]["json_schema"]["type"] == "array"
        assert params["scores"]["json_schema"]["items"]["type"] == "integer"

        # All fields should have _pydantic_model_name for reconstruction
        assert params["name"]["_pydantic_model_name"] == "input"
        assert params["tags"]["_pydantic_model_name"] == "input"
        assert params["scores"]["_pydantic_model_name"] == "input"

    def test_complex_nested_structure(self, sqlite_storage):
        """Test complex structure with nested models, lists, and flat fields."""

        class OrderItem(BaseModel):
            item_id: str
            quantity: int

        class ShippingAddress(BaseModel):
            street: str
            city: str

        class ComplexOrderInput(BaseModel):
            """Complex input with multiple nested structures."""

            order_id: str = Field(..., pattern=r"^ORD-")
            customer_email: str
            items: list[OrderItem]
            shipping_address: ShippingAddress
            tags: list[str]
            priority: int = Field(..., ge=1, le=5)

        @workflow
        async def complex_workflow(ctx: WorkflowContext, input: ComplexOrderInput) -> dict:
            """Workflow with complex nested structure."""
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("complex_workflow")

        # Flat fields should expand
        assert params["order_id"]["type"] == "str"
        assert params["customer_email"]["type"] == "str"
        assert params["priority"]["type"] == "int"

        # list[OrderItem] should be list_of_pydantic
        assert params["items"]["type"] == "list_of_pydantic"
        assert "item_fields" in params["items"]

        # list[str] should still be JSON textarea
        assert params["tags"]["type"] == "json"

        # Nested model (ShippingAddress) should expand as sub-form
        assert "shipping_address.street" in params
        assert params["shipping_address.street"]["type"] == "str"
        assert params["shipping_address.street"]["_parent_field"] == "shipping_address"
        assert params["shipping_address.street"]["_nested_level"] == 1

        assert "shipping_address.city" in params
        assert params["shipping_address.city"]["type"] == "str"
        assert params["shipping_address.city"]["_parent_field"] == "shipping_address"

        # Parent field should not be in params (it's been expanded)
        assert "shipping_address" not in params

        # Root fields should have _pydantic_model_name
        for field_name in ["order_id", "items", "tags", "priority"]:
            assert params[field_name]["_pydantic_model_name"] == "input"

        # Nested fields should also have _pydantic_model_name from their parent
        assert params["shipping_address.street"]["_pydantic_model_name"] == "input"


class TestSinglePydanticModelParameter:
    """Test suite for single Pydantic model parameter field expansion."""

    def test_single_pydantic_model_field_expansion(self, sqlite_storage):
        """Test that single Pydantic model parameter expands to individual fields."""
        from enum import Enum

        class Priority(str, Enum):
            LOW = "low"
            HIGH = "high"

        class WorkflowInput(BaseModel):
            """Simple input model with flat fields."""

            name: str = Field(..., min_length=1)
            age: int = Field(..., ge=18)
            price: float = Field(..., gt=0)
            active: bool = True
            priority: Priority = Priority.LOW

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: WorkflowInput) -> dict:
            """Workflow with single Pydantic model parameter."""
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Should expand to individual fields, not "input" with type="pydantic"
        assert "input" not in params
        assert "name" in params
        assert "age" in params
        assert "price" in params
        assert "active" in params
        assert "priority" in params

        # Verify field types
        assert params["name"]["type"] == "str"
        assert params["age"]["type"] == "int"
        assert params["price"]["type"] == "float"
        assert params["active"]["type"] == "bool"
        assert params["priority"]["type"] == "enum"

    def test_expanded_fields_have_pydantic_model_name(self, sqlite_storage):
        """Test that expanded fields contain original model name for reconstruction."""

        class SimpleInput(BaseModel):
            name: str
            age: int

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: SimpleInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # All expanded fields should have _pydantic_model_name
        for _field_name, field_info in params.items():
            assert "_pydantic_model_name" in field_info
            assert field_info["_pydantic_model_name"] == "input"

    def test_nested_models_expanded_as_subform(self, sqlite_storage):
        """Test that nested Pydantic models are expanded as sub-form."""

        class NestedInput(BaseModel):
            name: str
            address: Address  # Nested model

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: NestedInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Root field should expand
        assert "name" in params
        assert params["name"]["type"] == "str"

        # Nested model (Address) should expand as sub-form
        assert "address.street" in params
        assert params["address.street"]["type"] == "str"
        assert params["address.street"]["_parent_field"] == "address"
        assert params["address.street"]["_nested_level"] == 1

        assert "address.city" in params
        assert params["address.city"]["type"] == "str"
        assert params["address.city"]["_parent_field"] == "address"

        assert "address.state" in params
        assert params["address.state"]["type"] == "str"
        assert params["address.state"]["_parent_field"] == "address"

        # Parent field should not be in params (it's been expanded)
        assert "address" not in params

    def test_list_fields_expanded_as_json_textarea(self, sqlite_storage):
        """Test that list fields are expanded as JSON textarea."""

        class ListInput(BaseModel):
            name: str
            items: list[str]  # List field

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: ListInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Both "name" and "items" should be expanded
        assert "name" in params
        assert params["name"]["type"] == "str"

        # List fields are now expanded as JSON textarea
        assert "items" in params
        assert params["items"]["type"] == "json"
        assert params["items"]["json_schema"]["type"] == "array"

    def test_multiple_parameters_not_expanded(self, sqlite_storage):
        """Test that multiple parameters are NOT expanded."""

        class Input1(BaseModel):
            field1: str

        @workflow
        async def test_workflow(ctx: WorkflowContext, input1: Input1, input2: str) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Should NOT expand because there are multiple parameters
        assert "input1" in params
        assert params["input1"]["type"] == "pydantic"
        assert "input2" in params
        assert params["input2"]["type"] == "str"

    def test_empty_pydantic_model_no_expansion(self, sqlite_storage):
        """Test that empty Pydantic model (no fields) does not expand."""

        class EmptyInput(BaseModel):
            pass

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: EmptyInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        service.get_workflow_parameters("test_workflow")

        # Should return empty dict or original pydantic type
        # (Empty model has no fields to expand)


class TestNestedModelSubforms:
    """Test suite for sub-forms for 1-level nested Pydantic models."""

    def test_nested_model_expansion(self, sqlite_storage):
        """Test that 1-level nested Pydantic model expands to individual fields."""

        class ShippingAddress(BaseModel):
            """Nested shipping address model."""

            street: str
            city: str
            state: str = Field(..., pattern=r"^[A-Z]{2}$")
            zip_code: str

        class OrderInput(BaseModel):
            """Input with nested Pydantic model."""

            order_id: str = Field(..., pattern=r"^ORD-")
            customer_email: str
            shipping_address: ShippingAddress

        @workflow
        async def order_workflow(ctx: WorkflowContext, input: OrderInput) -> dict:
            """Workflow with nested Pydantic model."""
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("order_workflow")

        # Should expand flat fields
        assert "order_id" in params
        assert params["order_id"]["type"] == "str"

        assert "customer_email" in params
        assert params["customer_email"]["type"] == "str"

        # Nested model fields should be expanded with qualified names
        assert "shipping_address.street" in params
        assert params["shipping_address.street"]["type"] == "str"
        assert params["shipping_address.street"]["_parent_field"] == "shipping_address"
        assert params["shipping_address.street"]["_nested_level"] == 1

        assert "shipping_address.city" in params
        assert params["shipping_address.city"]["type"] == "str"

        assert "shipping_address.state" in params
        assert "shipping_address.zip_code" in params

        # Parent field itself should NOT be in params (it's been expanded)
        assert "shipping_address" not in params

    def test_nested_model_with_defaults(self, sqlite_storage):
        """Test that nested model default values are preserved."""

        class Address(BaseModel):
            street: str = "123 Main St"
            city: str = "Springfield"
            country: str = "US"

        class UserInput(BaseModel):
            name: str
            address: Address = Field(default_factory=Address)

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: UserInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Nested fields should have defaults
        assert params["address.street"]["default"] == "123 Main St"
        assert params["address.city"]["default"] == "Springfield"
        assert params["address.country"]["default"] == "US"

    def test_deeply_nested_model_fallback_to_json(self, sqlite_storage):
        """Test that 2+ levels of nesting fall back to JSON textarea."""

        class BillingAddress(BaseModel):
            street: str
            city: str

        class Address(BaseModel):
            shipping: BillingAddress
            billing: BillingAddress

        class OrderInput(BaseModel):
            order_id: str
            address: Address

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: OrderInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Level 0 → 1: address should be expanded
        assert "address.shipping" in params or "address.billing" in params

        # But shipping/billing should be JSON textarea (too deep)
        if "address.shipping" in params:
            assert params["address.shipping"]["type"] == "json"
        if "address.billing" in params:
            assert params["address.billing"]["type"] == "json"

    def test_multiple_nested_models(self, sqlite_storage):
        """Test multiple nested models in same input."""

        class ShippingAddress(BaseModel):
            street: str
            city: str

        class BillingAddress(BaseModel):
            street: str
            city: str

        class OrderInput(BaseModel):
            order_id: str
            shipping_address: ShippingAddress
            billing_address: BillingAddress

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: OrderInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Both nested models should be expanded
        assert "shipping_address.street" in params
        assert "shipping_address.city" in params
        assert params["shipping_address.street"]["_parent_field"] == "shipping_address"

        assert "billing_address.street" in params
        assert "billing_address.city" in params
        assert params["billing_address.street"]["_parent_field"] == "billing_address"

        # Flat field should still work
        assert "order_id" in params
        assert "_parent_field" not in params["order_id"]

    def test_ref_resolution(self, sqlite_storage):
        """Test that $ref references are correctly resolved."""

        class ShippingAddress(BaseModel):
            street: str
            city: str

        class OrderInput(BaseModel):
            order_id: str
            shipping_address: ShippingAddress

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: OrderInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # ShippingAddress is typically represented with $ref in JSON Schema
        # Should still be expanded correctly
        assert "shipping_address.street" in params
        assert "shipping_address.city" in params

    def test_qualified_field_names(self, sqlite_storage):
        """Test that qualified field names (parent.child) are used correctly."""

        class Address(BaseModel):
            street: str
            city: str

        class OrderInput(BaseModel):
            order_id: str
            address: Address

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: OrderInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Qualified names should be present
        assert "address.street" in params
        assert "address.city" in params

        # Simple "street" or "city" should NOT be present (they're qualified)
        assert "street" not in params
        assert "city" not in params

    def test_parent_field_metadata(self, sqlite_storage):
        """Test that _parent_field and _nested_level metadata are correctly set."""

        class Address(BaseModel):
            street: str
            city: str
            zip_code: str

        class OrderInput(BaseModel):
            order_id: str
            address: Address

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: OrderInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # All nested fields should have metadata
        for field_name in ["address.street", "address.city", "address.zip_code"]:
            assert field_name in params
            assert params[field_name]["_parent_field"] == "address"
            assert params[field_name]["_nested_level"] == 1
            assert params[field_name]["_pydantic_model_name"] == "input"

        # Root field should NOT have _parent_field
        assert "order_id" in params
        assert "_parent_field" not in params["order_id"]
        assert params["order_id"]["_pydantic_model_name"] == "input"


class TestDynamicLists:
    """Test suite for dynamic lists for list[PydanticModel]."""

    def test_list_of_pydantic_model_detection(self, sqlite_storage):
        """Test that list[PydanticModel] is detected and expanded as dynamic list."""

        class OrderItem(BaseModel):
            name: str
            quantity: int
            price: float

        class OrderInput(BaseModel):
            order_id: str
            items: list[OrderItem]

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: OrderInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # list[PydanticModel] should be type "list_of_pydantic"
        assert "items" in params
        assert params["items"]["type"] == "list_of_pydantic"
        assert "item_fields" in params["items"]
        assert "item_schema" in params["items"]

    def test_list_of_pydantic_model_item_fields(self, sqlite_storage):
        """Test that item_fields are correctly expanded for list[PydanticModel]."""

        class OrderItem(BaseModel):
            name: str
            quantity: int = Field(..., ge=1)
            price: float = Field(..., gt=0)

        class OrderInput(BaseModel):
            items: list[OrderItem]

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: OrderInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Verify item_fields are expanded
        item_fields = params["items"]["item_fields"]
        assert "name" in item_fields
        assert item_fields["name"]["type"] == "str"

        assert "quantity" in item_fields
        assert item_fields["quantity"]["type"] == "int"

        assert "price" in item_fields
        assert item_fields["price"]["type"] == "float"

    def test_list_of_pydantic_model_with_ref(self, sqlite_storage):
        """Test that $ref in list[PydanticModel] is correctly resolved."""

        class Product(BaseModel):
            product_id: str = Field(..., pattern=r"^PROD-")
            name: str

        class CartInput(BaseModel):
            user_id: str
            products: list[Product]

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: CartInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # list[Product] should be expanded with $ref resolved
        assert params["products"]["type"] == "list_of_pydantic"
        item_fields = params["products"]["item_fields"]
        assert "product_id" in item_fields
        assert "name" in item_fields

    def test_list_of_primitives_fallback_to_json(self, sqlite_storage):
        """Test that list[str], list[int], etc. fall back to JSON textarea."""

        class TagsInput(BaseModel):
            tags: list[str]
            scores: list[int]

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: TagsInput) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # list[str] and list[int] should be JSON textarea
        assert params["tags"]["type"] == "json"
        assert params["scores"]["type"] == "json"

    def test_empty_list_of_pydantic_model(self, sqlite_storage):
        """Test handling of empty list[PydanticModel]."""

        class Item(BaseModel):
            item_id: str

        class Input(BaseModel):
            items: list[Item] = Field(default_factory=list)

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: Input) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Should still be detected as list_of_pydantic
        assert params["items"]["type"] == "list_of_pydantic"
        assert params["items"]["required"] is False  # Has default

    def test_list_of_pydantic_with_defaults(self, sqlite_storage):
        """Test list[PydanticModel] with default values in nested model."""

        class Item(BaseModel):
            name: str
            quantity: int = 1
            price: float = 0.0

        class Input(BaseModel):
            items: list[Item]

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: Input) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Verify defaults are preserved in item_fields
        item_fields = params["items"]["item_fields"]
        assert item_fields["quantity"]["default"] == 1
        assert item_fields["price"]["default"] == 0.0

    def test_list_of_complex_pydantic_model(self, sqlite_storage):
        """Test list[PydanticModel] with complex nested model (many fields)."""

        class ComplexItem(BaseModel):
            item_id: str = Field(..., pattern=r"^ITEM-")
            name: str
            quantity: int = Field(..., ge=1, le=100)
            price: float = Field(..., gt=0)
            category: str
            in_stock: bool = True

        class Input(BaseModel):
            items: list[ComplexItem]

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: Input) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # All fields should be expanded
        item_fields = params["items"]["item_fields"]
        assert len(item_fields) == 6
        assert "item_id" in item_fields
        assert "name" in item_fields
        assert "quantity" in item_fields
        assert "price" in item_fields
        assert "category" in item_fields
        assert "in_stock" in item_fields

    def test_multiple_list_of_pydantic_fields(self, sqlite_storage):
        """Test multiple list[PydanticModel] fields in same input."""

        class Item(BaseModel):
            name: str
            price: float

        class Discount(BaseModel):
            code: str
            percentage: float

        class Input(BaseModel):
            items: list[Item]
            discounts: list[Discount]

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: Input) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Both should be list_of_pydantic
        assert params["items"]["type"] == "list_of_pydantic"
        assert "name" in params["items"]["item_fields"]
        assert "price" in params["items"]["item_fields"]

        assert params["discounts"]["type"] == "list_of_pydantic"
        assert "code" in params["discounts"]["item_fields"]
        assert "percentage" in params["discounts"]["item_fields"]

    def test_nested_list_of_pydantic_fallback(self, sqlite_storage):
        """Test that list[PydanticModel] inside nested model falls back to JSON."""

        class Item(BaseModel):
            name: str

        class NestedModel(BaseModel):
            items: list[Item]

        class Input(BaseModel):
            nested: NestedModel

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: Input) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Nested model should be expanded as sub-form
        assert "nested.items" in params
        # But nested.items (list[Item] at level 1) should be list_of_pydantic or JSON
        # Since nested level is reset for list items, it should be list_of_pydantic
        assert params["nested.items"]["type"] in ["list_of_pydantic", "json"]

    def test_list_of_pydantic_item_field_types(self, sqlite_storage):
        """Test that item_fields have correct type information."""

        class Item(BaseModel):
            name: str
            quantity: int
            price: float
            active: bool

        class Input(BaseModel):
            items: list[Item]

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: Input) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Verify each field has correct type
        item_fields = params["items"]["item_fields"]
        assert item_fields["name"]["type"] == "str"
        assert item_fields["quantity"]["type"] == "int"
        assert item_fields["price"]["type"] == "float"
        assert item_fields["active"]["type"] == "bool"


class TestPolishAndEdgeCases:
    """Test suite for polish and edge cases."""

    def test_required_field_metadata(self, sqlite_storage):
        """Test that required field metadata is correctly set."""

        class Input(BaseModel):
            required_field: str
            optional_field: str = "default"
            optional_with_none: str | None = None

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: Input) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Verify required metadata
        assert params["required_field"]["required"] is True
        assert params["optional_field"]["required"] is False
        assert params["optional_with_none"]["required"] is False

    def test_default_value_metadata(self, sqlite_storage):
        """Test that default values are correctly preserved."""

        class Input(BaseModel):
            str_with_default: str = "hello"
            int_with_default: int = 42
            float_with_default: float = 3.14
            bool_with_default: bool = True
            no_default: str

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: Input) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Verify defaults
        assert params["str_with_default"]["default"] == "hello"
        assert params["int_with_default"]["default"] == 42
        assert params["float_with_default"]["default"] == 3.14
        assert params["bool_with_default"]["default"] is True
        assert params["no_default"]["default"] is None

    def test_deeply_nested_fallback_to_json(self, sqlite_storage):
        """Test that 3+ level nesting falls back to JSON textarea."""

        class Level3(BaseModel):
            value: str

        class Level2(BaseModel):
            level3: Level3

        class Level1(BaseModel):
            level2: Level2

        class Input(BaseModel):
            level1: Level1

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: Input) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Level 1 should expand as sub-form
        assert "level1.level2" in params

        # Level 2 (nested at level 1) should be JSON textarea (too deep)
        assert params["level1.level2"]["type"] == "json"

    def test_empty_list_default(self, sqlite_storage):
        """Test handling of empty list with default_factory."""

        class Item(BaseModel):
            name: str

        class Input(BaseModel):
            items: list[Item] = Field(default_factory=list)
            tags: list[str] = Field(default_factory=list)

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: Input) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Both should be detected
        assert "items" in params
        assert "tags" in params

        # Items should be list_of_pydantic
        assert params["items"]["type"] == "list_of_pydantic"

        # Tags should be JSON
        assert params["tags"]["type"] == "json"

        # Both should be optional
        assert params["items"]["required"] is False
        assert params["tags"]["required"] is False

    def test_field_metadata_in_nested_model(self, sqlite_storage):
        """Test that nested model fields preserve required/default metadata."""

        class Address(BaseModel):
            street: str
            city: str
            zip_code: str = "00000"
            country: str = "USA"

        class Input(BaseModel):
            address: Address

        @workflow
        async def test_workflow(ctx: WorkflowContext, input: Input) -> dict:
            return {}

        service = WorkflowDataService(sqlite_storage)
        params = service.get_workflow_parameters("test_workflow")

        # Verify required metadata for nested fields
        assert params["address.street"]["required"] is True
        assert params["address.city"]["required"] is True
        assert params["address.zip_code"]["required"] is False
        assert params["address.country"]["required"] is False

        # Verify defaults
        assert params["address.zip_code"]["default"] == "00000"
        assert params["address.country"]["default"] == "USA"
