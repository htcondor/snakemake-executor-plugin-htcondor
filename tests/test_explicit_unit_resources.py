"""
Tests for explicit unit resource parameters (htcondor_request_mem_mb, etc.)

These resources are designed for use with grouped jobs where Snakemake can aggregate
numeric values (taking max for sequential jobs), avoiding the "string resources must
be identical" limitation.
"""

import unittest
from unittest.mock import Mock
from snakemake_executor_plugin_htcondor import Executor


class TestExplicitUnitResources(unittest.TestCase):
    """Tests for htcondor_request_mem_mb, htcondor_request_disk_mb, htcondor_gpus_min_mem_mb."""

    def setUp(self):
        """Set up mock executor for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()

        # Bind the actual methods to the mock
        self.executor._format_size_mb = Executor._format_size_mb.__get__(
            self.executor, Executor
        )
        self.executor._handle_explicit_unit_resources = (
            Executor._handle_explicit_unit_resources.__get__(self.executor, Executor)
        )
        self.executor._log_resource_requests = Executor._log_resource_requests.__get__(
            self.executor, Executor
        )

        # Set up mock job
        self.job = Mock()
        self.job.name = "test_job"

    def _create_resource_getter(self, resource_dict):
        """Create a resource getter function from a dictionary."""

        def get(key, default=None):
            return resource_dict.get(key, default)

        return get


class TestFormatSizeMB(TestExplicitUnitResources):
    """Tests for _format_size_mb helper method."""

    def test_format_size_mb_exact_gb(self):
        """Test formatting size values that are exact GB multiples."""
        assert self.executor._format_size_mb(1024) == "1GB"
        assert self.executor._format_size_mb(2048) == "2GB"
        assert self.executor._format_size_mb(4096) == "4GB"
        assert self.executor._format_size_mb(10240) == "10GB"

    def test_format_size_mb_non_exact_gb(self):
        """Test formatting size values that are not exact GB multiples."""
        assert self.executor._format_size_mb(512) == "512MB"
        assert self.executor._format_size_mb(1536) == "1536MB"
        assert self.executor._format_size_mb(2500) == "2500MB"

    def test_format_size_mb_small_values(self):
        """Test formatting small size values."""
        assert self.executor._format_size_mb(128) == "128MB"
        assert self.executor._format_size_mb(256) == "256MB"

    def test_format_size_mb_zero(self):
        """Test formatting zero value."""
        assert self.executor._format_size_mb(0) == "0MB"

    def test_format_size_mb_negative_raises_error(self):
        """Test that negative values raise WorkflowError."""
        from snakemake_interface_common.exceptions import WorkflowError

        with self.assertRaises(WorkflowError) as context:
            self.executor._format_size_mb(-1024)
        self.assertIn("must be non-negative", str(context.exception))


class TestHandleExplicitUnitResources(TestExplicitUnitResources):
    """Tests for _handle_explicit_unit_resources method."""

    def test_htcondor_request_mem_mb_sets_request_memory(self):
        """Test that htcondor_request_mem_mb sets request_memory in submit dict."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter(
            {"htcondor_request_mem_mb": 2048}
        )

        submit_dict = {}
        self.executor._handle_explicit_unit_resources(self.job, submit_dict)

        assert submit_dict["request_memory"] == "2GB"

    def test_htcondor_request_mem_mb_takes_precedence(self):
        """Test that htcondor_request_mem_mb takes precedence over request_memory."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter(
            {"htcondor_request_mem_mb": 4096, "request_memory": "1GB"}
        )

        submit_dict = {"request_memory": "1GB"}  # Pre-set by regular handler
        self.executor._handle_explicit_unit_resources(self.job, submit_dict)

        assert submit_dict["request_memory"] == "4GB"
        self.executor.logger.warning.assert_called()

    def test_htcondor_request_disk_mb_sets_request_disk(self):
        """Test that htcondor_request_disk_mb sets request_disk in KB."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter(
            {"htcondor_request_disk_mb": 1024}
        )

        submit_dict = {}
        self.executor._handle_explicit_unit_resources(self.job, submit_dict)

        # 1024 MB = 1,048,576 KB
        assert submit_dict["request_disk"] == 1024 * 1024

    def test_htcondor_request_disk_mb_takes_precedence(self):
        """Test that htcondor_request_disk_mb takes precedence over request_disk."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter(
            {"htcondor_request_disk_mb": 2048, "request_disk": "500MB"}
        )

        submit_dict = {"request_disk": "500MB"}
        self.executor._handle_explicit_unit_resources(self.job, submit_dict)

        assert submit_dict["request_disk"] == 2048 * 1024
        self.executor.logger.warning.assert_called()

    def test_htcondor_gpus_min_mem_mb_sets_gpus_minimum_memory(self):
        """Test that htcondor_gpus_min_mem_mb sets gpus_minimum_memory."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter(
            {"htcondor_gpus_min_mem_mb": 8192}
        )

        submit_dict = {}
        self.executor._handle_explicit_unit_resources(self.job, submit_dict)

        assert submit_dict["gpus_minimum_memory"] == 8192

    def test_htcondor_gpus_min_mem_mb_takes_precedence(self):
        """Test that htcondor_gpus_min_mem_mb takes precedence over gpus_minimum_memory."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter(
            {"htcondor_gpus_min_mem_mb": 16384, "gpus_minimum_memory": "4GB"}
        )

        submit_dict = {"gpus_minimum_memory": "4GB"}
        self.executor._handle_explicit_unit_resources(self.job, submit_dict)

        assert submit_dict["gpus_minimum_memory"] == 16384
        self.executor.logger.warning.assert_called()

    def test_no_explicit_resources_leaves_submit_dict_unchanged(self):
        """Test that submit_dict is unchanged when no explicit unit resources are set."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter({})

        submit_dict = {"request_memory": "1GB", "request_disk": 1000}
        original = submit_dict.copy()
        self.executor._handle_explicit_unit_resources(self.job, submit_dict)

        assert submit_dict == original

    def test_combined_resources(self):
        """Test setting multiple explicit unit resources at once."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter(
            {
                "htcondor_request_mem_mb": 8192,
                "htcondor_request_disk_mb": 20480,
                "htcondor_gpus_min_mem_mb": 4096,
            }
        )

        submit_dict = {}
        self.executor._handle_explicit_unit_resources(self.job, submit_dict)

        assert submit_dict["request_memory"] == "8GB"
        assert submit_dict["request_disk"] == 20480 * 1024
        assert submit_dict["gpus_minimum_memory"] == 4096


class TestGroupedJobResourceAggregation(TestExplicitUnitResources):
    """Tests demonstrating the grouped job use case.

    When Snakemake groups jobs, it aggregates numeric resources by:
    1. Summing resources for jobs that run in parallel (same layer)
    2. Taking the max across sequential layers

    For a linear chain (A → B → C), this is simply max(A, B, C).
    For parallel jobs within a layer, resources are summed first.

    See: https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#resources-and-group-jobs
    """

    def test_aggregated_memory_from_grouped_jobs(self):
        """Test that aggregated memory values from grouped jobs are handled correctly.

        For a linear chain of grouped jobs (running in series), Snakemake takes
        the max of numeric resources. For example:
        - Rule A: htcondor_request_mem_mb=1024
        - Rule B: htcondor_request_mem_mb=2048
        - Grouped job receives: htcondor_request_mem_mb=2048 (max)

        The executor should format this as a readable HTCondor value.
        """
        # Simulate what Snakemake does: take the max of the values
        grouped_mem = max(1024, 2048)  # = 2048 MB = 2GB

        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter(
            {"htcondor_request_mem_mb": grouped_mem}
        )

        submit_dict = {}
        self.executor._handle_explicit_unit_resources(self.job, submit_dict)

        assert submit_dict["request_memory"] == "2GB"

    def test_aggregated_disk_from_grouped_jobs(self):
        """Test that aggregated disk values from grouped jobs are handled correctly."""
        # Simulate: Rule A = 4096 MB, Rule B = 8192 MB → Grouped = 8192 MB (max) = 8GB
        grouped_disk = max(4096, 8192)

        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter(
            {"htcondor_request_disk_mb": grouped_disk}
        )

        submit_dict = {}
        self.executor._handle_explicit_unit_resources(self.job, submit_dict)

        # 8192 MB = 8 GB, converted to KB for HTCondor
        assert submit_dict["request_disk"] == 8192 * 1024


class TestLogResourceRequests(TestExplicitUnitResources):
    """Tests for _log_resource_requests method that logs final resource values."""

    def test_log_memory_request_formatted_string(self):
        """Test logging memory when value is already formatted (e.g., '4GB')."""
        submit_dict = {"request_memory": "4GB"}
        self.executor._log_resource_requests(submit_dict)

        # Should log the formatted string as-is
        self.executor.logger.info.assert_any_call("Requesting memory: 4GB")

    def test_log_memory_request_numeric_mb(self):
        """Test logging memory when value is numeric in MB."""
        submit_dict = {"request_memory": 2048}
        self.executor._log_resource_requests(submit_dict)

        # Should format 2048 MB as 2GB
        self.executor.logger.info.assert_any_call("Requesting memory: 2GB")

    def test_log_memory_request_numeric_non_exact_gb(self):
        """Test logging memory when value is numeric but not exact GB."""
        submit_dict = {"request_memory": 1536}
        self.executor._log_resource_requests(submit_dict)

        # Should format as MB since not exact GB
        self.executor.logger.info.assert_any_call("Requesting memory: 1536MB")

    def test_log_disk_request_numeric_kb(self):
        """Test logging disk when value is numeric in KB (HTCondor's unit)."""
        submit_dict = {"request_disk": 1024 * 1024}  # 1GB in KB
        self.executor._log_resource_requests(submit_dict)

        # Should convert KB to MB and format
        self.executor.logger.info.assert_any_call(
            f"Requesting disk: 1GB ({1024 * 1024} KB)"
        )

    def test_log_disk_request_string(self):
        """Test logging disk when value is a string."""
        submit_dict = {"request_disk": "2GB"}
        self.executor._log_resource_requests(submit_dict)

        self.executor.logger.info.assert_any_call("Requesting disk: 2GB")

    def test_log_gpu_memory_numeric(self):
        """Test logging GPU memory when value is numeric in MB."""
        submit_dict = {"gpus_minimum_memory": 8192}
        self.executor._log_resource_requests(submit_dict)

        # Should format 8192 MB as 8GB
        self.executor.logger.info.assert_any_call("Requesting GPU minimum memory: 8GB")

    def test_log_gpu_memory_string(self):
        """Test logging GPU memory when value is a string."""
        submit_dict = {"gpus_minimum_memory": "16GB"}
        self.executor._log_resource_requests(submit_dict)

        self.executor.logger.info.assert_any_call("Requesting GPU minimum memory: 16GB")

    def test_log_all_resources_together(self):
        """Test logging all resource types in one call."""
        submit_dict = {
            "request_memory": 4096,  # 4GB
            "request_disk": 10240 * 1024,  # 10GB in KB
            "gpus_minimum_memory": 16384,  # 16GB
        }
        self.executor._log_resource_requests(submit_dict)

        # All three should be logged
        self.executor.logger.info.assert_any_call("Requesting memory: 4GB")
        self.executor.logger.info.assert_any_call(
            f"Requesting disk: 10GB ({10240 * 1024} KB)"
        )
        self.executor.logger.info.assert_any_call("Requesting GPU minimum memory: 16GB")
        assert self.executor.logger.info.call_count == 3

    def test_log_no_resources(self):
        """Test that nothing is logged when no resource values are present."""
        submit_dict = {}
        self.executor._log_resource_requests(submit_dict)

        # No logging should occur
        self.executor.logger.info.assert_not_called()

    def test_log_only_present_resources(self):
        """Test that only resources that are present get logged."""
        submit_dict = {"request_memory": "2GB"}  # Only memory, no disk or GPU
        self.executor._log_resource_requests(submit_dict)

        # Only memory should be logged
        self.executor.logger.info.assert_called_once_with("Requesting memory: 2GB")

    def test_integration_with_handle_explicit_unit_resources(self):
        """Test that values set by _handle_explicit_unit_resources are logged correctly."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter(
            {
                "htcondor_request_mem_mb": 8192,
                "htcondor_request_disk_mb": 20480,
                "htcondor_gpus_min_mem_mb": 4096,
            }
        )

        submit_dict = {}
        self.executor._handle_explicit_unit_resources(self.job, submit_dict)

        # Reset the mock to clear any previous calls
        self.executor.logger.reset_mock()

        # Now log the resources
        self.executor._log_resource_requests(submit_dict)

        # Verify correct logging
        self.executor.logger.info.assert_any_call("Requesting memory: 8GB")
        self.executor.logger.info.assert_any_call(
            f"Requesting disk: 20GB ({20480 * 1024} KB)"
        )
        self.executor.logger.info.assert_any_call("Requesting GPU minimum memory: 4GB")

class TestSetResources(TestExplicitUnitResources):
    """Tests for _set_resources helper method."""

    def setUp(self):
        """Set up mock executor for testing."""
        super().setUp()
        # Bind the _set_resources method
        self.executor._set_resources = Executor._set_resources.__get__(
            self.executor, Executor
        )

    def test_set_resources_with_value(self):
        """When resource has a value, it should be set properly."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter({"request_memory": "4GB"})

        # initialize submit_dict
        submit_dict = {}

        # call the _set_resources and set a value
        self.executor._set_resources(submit_dict, self.job, "request_memory")

        # verify result
        assert submit_dict["request_memory"] == "4GB"

    def test_set_resources_with_falsy_value_zero(self):
        """When corresponding resource has a value 0, it should be set properly."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter({"max_retries": 0})

        # initialize submit_dict
        submit_dict = {}

        # call the _set_resources and set a value
        self.executor._set_resources(submit_dict, self.job, "max_retries")

        # verify result
        assert submit_dict["max_retries"] == 0

    def test_set_resources_with_falsy_value_false(self):
        """When corresponding resource has a value False, it should be set properly."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter(
            {"preserve_relative_paths": False}
        )

        # initialize submit_dict
        submit_dict = {}

        # call the _set_resources and set a value
        self.executor._set_resources(submit_dict, self.job, "preserve_relative_paths")

        # verify result
        assert not submit_dict["preserve_relative_paths"]

    def test_set_resources_with_default(self):
        """When corresponding resource has a default value, it should be set properly."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter({})

        # initialize submit_dict
        submit_dict = {}

        # call the _set_resources and set a value
        self.executor._set_resources(submit_dict, self.job, "getenv", default=False)

        # verify result
        assert not submit_dict["getenv"]

    def test_set_resources_with_no_default(self):
        """When corresponding resource has no default value, it should not exist in submit_dict."""
        self.job.resources = Mock()
        self.job.resources.get = self._create_resource_getter({})

        # initialize submit_dict
        submit_dict = {}

        # call the _set_resources and set a value
        self.executor._set_resources(submit_dict, self.job, "max_retries")

        # verify result
        assert "max_retries" not in submit_dict

if __name__ == "__main__":
    unittest.main()
