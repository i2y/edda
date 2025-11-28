"""
Tests for Viewer UI pagination and filtering functionality.
"""

from datetime import datetime

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


@pytest.fixture
async def storage():
    """Create a test storage instance."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    storage = SQLAlchemyStorage(engine)
    await storage.initialize()
    yield storage
    await storage.close()


@pytest.fixture
async def storage_with_instances(storage):
    """Create storage with multiple workflow instances for pagination testing."""
    # First, create all workflow definitions
    for name_char in "ABCDE":
        workflow_name = f"workflow_{name_char}"
        await storage.upsert_workflow_definition(
            workflow_name=workflow_name,
            source_hash=f"hash_{workflow_name}",
            source_code=f"async def {workflow_name}(): pass",
        )

    for i in range(25):
        instance_id = f"inst_{i:03d}"
        workflow_name = f"workflow_{chr(65 + (i % 5))}"  # workflow_A to workflow_E
        status = ["running", "completed", "failed", "waiting_for_event", "cancelled"][i % 5]

        await storage.create_instance(
            instance_id=instance_id,
            workflow_name=workflow_name,
            source_hash=f"hash_{workflow_name}",
            owner_service="test_service",
            input_data={"index": i},
        )

        # Update status for non-running instances
        if status != "running":
            await storage.update_instance_status(
                instance_id=instance_id,
                status=status,
                output_data={"result": f"result_{i}"} if status == "completed" else None,
            )

    return storage


class TestCursorBasedPagination:
    """Test cursor-based pagination functionality."""

    async def test_list_instances_returns_dict_format(self, storage_with_instances):
        """Verify list_instances returns the new dict format."""
        result = await storage_with_instances.list_instances(limit=10)

        assert isinstance(result, dict)
        assert "instances" in result
        assert "next_page_token" in result
        assert "has_more" in result
        assert isinstance(result["instances"], list)
        assert isinstance(result["has_more"], bool)

    async def test_pagination_first_page(self, storage_with_instances):
        """Test first page of pagination."""
        result = await storage_with_instances.list_instances(limit=10)

        assert len(result["instances"]) == 10
        assert result["has_more"] is True
        assert result["next_page_token"] is not None

    async def test_pagination_next_page(self, storage_with_instances):
        """Test navigating to next page using page_token."""
        # Get first page
        first_page = await storage_with_instances.list_instances(limit=10)
        first_page_ids = {inst["instance_id"] for inst in first_page["instances"]}

        # Get second page
        second_page = await storage_with_instances.list_instances(
            limit=10, page_token=first_page["next_page_token"]
        )
        second_page_ids = {inst["instance_id"] for inst in second_page["instances"]}

        # Verify no overlap
        assert len(first_page_ids & second_page_ids) == 0
        assert len(second_page["instances"]) == 10
        assert second_page["has_more"] is True

    async def test_pagination_last_page(self, storage_with_instances):
        """Test last page has no more pages."""
        # Get first two pages
        page1 = await storage_with_instances.list_instances(limit=10)
        page2 = await storage_with_instances.list_instances(
            limit=10, page_token=page1["next_page_token"]
        )

        # Get third page (should be the last with 5 items)
        page3 = await storage_with_instances.list_instances(
            limit=10, page_token=page2["next_page_token"]
        )

        assert len(page3["instances"]) == 5
        assert page3["has_more"] is False
        assert page3["next_page_token"] is None

    async def test_pagination_ordering_desc(self, storage_with_instances):
        """Test instances are ordered by started_at DESC."""
        result = await storage_with_instances.list_instances(limit=25)

        instances = result["instances"]
        for i in range(len(instances) - 1):
            # Each instance should have started_at >= next instance
            current_time = instances[i]["started_at"]
            next_time = instances[i + 1]["started_at"]
            assert current_time >= next_time, f"Instance {i} should be newer than {i+1}"


class TestStatusFilter:
    """Test status filter functionality."""

    async def test_filter_by_running(self, storage_with_instances):
        """Test filtering by 'running' status."""
        result = await storage_with_instances.list_instances(limit=50, status_filter="running")

        assert all(inst["status"] == "running" for inst in result["instances"])
        assert len(result["instances"]) == 5  # 25 instances / 5 statuses

    async def test_filter_by_completed(self, storage_with_instances):
        """Test filtering by 'completed' status."""
        result = await storage_with_instances.list_instances(limit=50, status_filter="completed")

        assert all(inst["status"] == "completed" for inst in result["instances"])
        assert len(result["instances"]) == 5

    async def test_filter_by_failed(self, storage_with_instances):
        """Test filtering by 'failed' status."""
        result = await storage_with_instances.list_instances(limit=50, status_filter="failed")

        assert all(inst["status"] == "failed" for inst in result["instances"])

    async def test_filter_with_pagination(self, storage_with_instances):
        """Test status filter works with pagination."""
        result = await storage_with_instances.list_instances(limit=3, status_filter="completed")

        assert len(result["instances"]) == 3
        assert result["has_more"] is True
        assert all(inst["status"] == "completed" for inst in result["instances"])


class TestSearchFilter:
    """Test search filter functionality (workflow name and instance ID)."""

    async def test_search_by_workflow_name(self, storage_with_instances):
        """Test searching by workflow name."""
        result = await storage_with_instances.list_instances(
            limit=50, workflow_name_filter="workflow_A"
        )

        assert all("workflow_A" in inst["workflow_name"] for inst in result["instances"])
        assert len(result["instances"]) == 5  # 25 instances / 5 workflow names

    async def test_search_by_instance_id(self, storage_with_instances):
        """Test searching by instance ID."""
        result = await storage_with_instances.list_instances(limit=50, instance_id_filter="inst_01")

        # Should match inst_01, inst_010, inst_011, etc.
        assert all("inst_01" in inst["instance_id"] for inst in result["instances"])

    async def test_unified_search_or_logic(self, storage_with_instances):
        """Test unified search (same value for both filters) uses OR logic."""
        # Search for a term that only exists in workflow names
        result = await storage_with_instances.list_instances(
            limit=50,
            workflow_name_filter="workflow_B",
            instance_id_filter="workflow_B",  # Same value triggers OR logic
        )

        # Should find instances with workflow_B in name OR id (just name in this case)
        assert len(result["instances"]) == 5

    async def test_case_insensitive_search(self, storage_with_instances):
        """Test search is case-insensitive."""
        result = await storage_with_instances.list_instances(
            limit=50, workflow_name_filter="WORKFLOW_C"  # Uppercase
        )

        # Should still find workflow_C instances
        assert len(result["instances"]) == 5


class TestDateRangeFilter:
    """Test date range filter functionality."""

    async def test_filter_started_after(self, storage_with_instances):
        """Test filtering by started_after."""
        # Get all instances first
        all_instances = await storage_with_instances.list_instances(limit=50)

        # Use the started_at of the 10th instance as cutoff
        cutoff_time = datetime.fromisoformat(
            all_instances["instances"][10]["started_at"].replace("Z", "+00:00")
        )

        result = await storage_with_instances.list_instances(limit=50, started_after=cutoff_time)

        # Should only include instances started after cutoff
        for inst in result["instances"]:
            inst_time = datetime.fromisoformat(inst["started_at"].replace("Z", "+00:00"))
            assert inst_time >= cutoff_time

    async def test_filter_started_before(self, storage_with_instances):
        """Test filtering by started_before."""
        all_instances = await storage_with_instances.list_instances(limit=50)

        # Use the started_at of the 10th instance as cutoff
        cutoff_time = datetime.fromisoformat(
            all_instances["instances"][10]["started_at"].replace("Z", "+00:00")
        )

        result = await storage_with_instances.list_instances(limit=50, started_before=cutoff_time)

        # Should only include instances started before or at cutoff
        for inst in result["instances"]:
            inst_time = datetime.fromisoformat(inst["started_at"].replace("Z", "+00:00"))
            assert inst_time <= cutoff_time

    async def test_date_range_combination(self, storage_with_instances):
        """Test combining started_after and started_before."""
        all_instances = await storage_with_instances.list_instances(limit=50)

        start_cutoff = datetime.fromisoformat(
            all_instances["instances"][15]["started_at"].replace("Z", "+00:00")
        )
        end_cutoff = datetime.fromisoformat(
            all_instances["instances"][5]["started_at"].replace("Z", "+00:00")
        )

        result = await storage_with_instances.list_instances(
            limit=50, started_after=start_cutoff, started_before=end_cutoff
        )

        for inst in result["instances"]:
            inst_time = datetime.fromisoformat(inst["started_at"].replace("Z", "+00:00"))
            assert start_cutoff <= inst_time <= end_cutoff


class TestCombinedFilters:
    """Test combining multiple filters."""

    async def test_status_and_search(self, storage_with_instances):
        """Test combining status filter with search."""
        result = await storage_with_instances.list_instances(
            limit=50, status_filter="completed", workflow_name_filter="workflow_A"
        )

        for inst in result["instances"]:
            assert inst["status"] == "completed"
            assert "workflow_A" in inst["workflow_name"]

    async def test_all_filters_combined(self, storage_with_instances):
        """Test combining all filter types."""
        all_instances = await storage_with_instances.list_instances(limit=50)

        cutoff = datetime.fromisoformat(
            all_instances["instances"][12]["started_at"].replace("Z", "+00:00")
        )

        result = await storage_with_instances.list_instances(
            limit=50,
            status_filter="running",
            workflow_name_filter="workflow_A",
            started_after=cutoff,
        )

        for inst in result["instances"]:
            assert inst["status"] == "running"
            assert "workflow_A" in inst["workflow_name"]
            inst_time = datetime.fromisoformat(inst["started_at"].replace("Z", "+00:00"))
            assert inst_time >= cutoff


class TestEmptyResults:
    """Test behavior with empty or no matching results."""

    async def test_no_instances(self, storage):
        """Test with no instances in database."""
        result = await storage.list_instances(limit=10)

        assert result["instances"] == []
        assert result["has_more"] is False
        assert result["next_page_token"] is None

    async def test_no_matching_status(self, storage_with_instances):
        """Test with non-matching status filter."""
        result = await storage_with_instances.list_instances(
            limit=50, status_filter="nonexistent_status"
        )

        assert result["instances"] == []
        assert result["has_more"] is False

    async def test_no_matching_search(self, storage_with_instances):
        """Test with non-matching search term."""
        result = await storage_with_instances.list_instances(
            limit=50, workflow_name_filter="nonexistent_workflow"
        )

        assert result["instances"] == []
        assert result["has_more"] is False
