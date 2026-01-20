"""
Unit tests for job wrapper and job argument handling in HTCondor executor.

These tests verify that:
1. Job wrapper scripts in subdirectories preserve their relative paths
2. Job arguments are correctly extracted and sanitized
3. The executable/arguments split works correctly with and without wrappers

BACKGROUND - Issue #8:
======================
A user reported that they couldn't use a job wrapper nested in a subdirectory
(e.g., workflow/wrapper.sh) when using the HTCondor executor. The bug was that
basename() was being used on the wrapper path, stripping the directory.
"""

from unittest.mock import Mock
from snakemake_executor_plugin_htcondor import Executor


class TestGetBaseExecAndArgs:
    """Test the _get_base_exec_and_args helper method."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()

        # Bind the actual methods to our mock
        self.executor._get_base_exec_and_args = (
            Executor._get_base_exec_and_args.__get__(self.executor, Executor)
        )
        self.executor._sanitize_job_args = Executor._sanitize_job_args.__get__(
            self.executor, Executor
        )

        # Default mock for format_job_exec
        self.executor.format_job_exec = Mock(
            return_value="python -m snakemake --snakefile Snakefile --cores 1"
        )
        self.executor.get_python_executable = Mock(return_value="python")

    def test_wrapper_in_subdirectory_preserves_path(self):
        """
        Test that wrapper in subdirectory preserves the full relative path.

        This is the critical test for issue #8. The wrapper path must NOT
        be stripped to basename.
        """
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(return_value="workflow/wrapper.sh")

        job_exec, _ = self.executor._get_base_exec_and_args(job)

        # The executable MUST be the full relative path
        assert job_exec == "workflow/wrapper.sh"
        # Should NOT be just the basename
        assert job_exec != "wrapper.sh"

    def test_wrapper_deeply_nested_preserves_path(self):
        """Test deeply nested wrapper path is preserved."""
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(
            return_value="workflow/scripts/wrappers/htcondor_wrapper.sh"
        )

        job_exec, _ = self.executor._get_base_exec_and_args(job)

        assert job_exec == "workflow/scripts/wrappers/htcondor_wrapper.sh"

    def test_wrapper_at_root_level(self):
        """Test wrapper at root level works correctly."""
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(return_value="wrapper.sh")

        job_exec, _ = self.executor._get_base_exec_and_args(job)

        assert job_exec == "wrapper.sh"

    def test_no_wrapper_uses_python_executable(self):
        """Test that when no wrapper is specified, Python is used as executable."""
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(return_value=None)  # No wrapper

        job_exec, _ = self.executor._get_base_exec_and_args(job)

        # Should use sys.executable (full path to Python)
        import sys

        assert job_exec == sys.executable
        # Note: get_python_executable() is only called as a fallback if other
        # prefix matches fail, so we don't assert it was called

    def test_wrapper_strips_snakemake_prefix_from_args(self):
        """Test that with wrapper, 'python -m snakemake ' prefix is stripped."""
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(return_value="wrapper.sh")
        self.executor.format_job_exec = Mock(
            return_value="python -m snakemake --snakefile Snakefile --cores 1 --target all"
        )

        _, job_args = self.executor._get_base_exec_and_args(job)

        assert job_args == "--snakefile Snakefile --cores 1 --target all"
        assert "python -m snakemake" not in job_args

    def test_no_wrapper_strips_python_prefix_from_args(self):
        """Test that without wrapper, python executable prefix is stripped."""
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(return_value=None)
        self.executor.get_python_executable = Mock(return_value="/usr/bin/python3")

        # Use sys.executable in the mock return value to match reality
        import sys

        self.executor.format_job_exec = Mock(
            return_value=f"{sys.executable} -m snakemake --snakefile Snakefile"
        )

        job_exec, job_args = self.executor._get_base_exec_and_args(job)

        # Should use sys.executable, not get_python_executable()
        assert job_exec == sys.executable
        assert job_args == "-m snakemake --snakefile Snakefile"


class TestSanitizeJobArgs:
    """Test the _sanitize_job_args helper method."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()

        # Bind the actual method to our mock
        self.executor._sanitize_job_args = Executor._sanitize_job_args.__get__(
            self.executor, Executor
        )

    def test_removes_single_quotes(self):
        """Test that single quotes are removed from arguments."""
        job_args = "--config key='value' --other 'quoted'"

        result = self.executor._sanitize_job_args(job_args)

        assert "'" not in result
        assert result == "--config key=value --other quoted"

    def test_no_warning_when_no_quotes(self):
        """Test that no warning is logged when there are no single quotes."""
        job_args = "--config key=value --other arg"

        result = self.executor._sanitize_job_args(job_args)

        assert result == job_args
        self.executor.logger.warning.assert_not_called()

    def test_preserves_double_quotes(self):
        """Test that double quotes are preserved (only single quotes removed)."""
        job_args = '--config key="value" --other "quoted"'

        result = self.executor._sanitize_job_args(job_args)

        assert result == job_args
        assert '"' in result

    def test_empty_string(self):
        """Test handling of empty string."""
        result = self.executor._sanitize_job_args("")

        assert result == ""
        self.executor.logger.warning.assert_not_called()


class TestJobWrapperIntegration:
    """
    Integration tests that verify the complete flow of job wrapper handling.

    These tests use more realistic mock setups to verify the interaction
    between _get_base_exec_and_args and the rest of the executor.
    """

    def setup_method(self):
        """Setup mock executor with more complete configuration."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()

        # Bind actual methods
        self.executor._get_base_exec_and_args = (
            Executor._get_base_exec_and_args.__get__(self.executor, Executor)
        )
        self.executor._sanitize_job_args = Executor._sanitize_job_args.__get__(
            self.executor, Executor
        )

    def test_wrapper_with_snakefile_in_same_directory(self):
        """
        Test scenario from issue #8: wrapper and Snakefile in workflow/ directory.

        User has:
        - workflow/Snakefile
        - workflow/wrapper.sh

        The wrapper must be 'workflow/wrapper.sh', not 'wrapper.sh'.
        """
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(return_value="workflow/wrapper.sh")
        self.executor.format_job_exec = Mock(
            return_value="python -m snakemake --snakefile workflow/Snakefile --cores 1"
        )

        job_exec, job_args = self.executor._get_base_exec_and_args(job)

        # Critical: wrapper path must be preserved
        assert job_exec == "workflow/wrapper.sh"
        # Args should have snakefile path
        assert "--snakefile workflow/Snakefile" in job_args

    def test_args_are_sanitized_automatically(self):
        """Test that _get_base_exec_and_args returns sanitized arguments."""
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(return_value="workflow/wrapper.sh")
        self.executor.format_job_exec = Mock(
            return_value="python -m snakemake --config name='test' --cores 1"
        )

        # Get executable and args - args should already be sanitized
        job_exec, job_args = self.executor._get_base_exec_and_args(job)

        assert job_exec == "workflow/wrapper.sh"
        # Single quotes should already be removed
        assert "'" not in job_args
        assert "--config name=test" in job_args


class TestJobWrapperEdgeCases:
    """Test edge cases for job wrapper handling."""

    def setup_method(self):
        """Setup mock executor."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()
        self.executor._get_base_exec_and_args = (
            Executor._get_base_exec_and_args.__get__(self.executor, Executor)
        )
        self.executor._sanitize_job_args = Executor._sanitize_job_args.__get__(
            self.executor, Executor
        )
        self.executor.get_python_executable = Mock(return_value="python")
        self.executor.format_job_exec = Mock(
            return_value="python -m snakemake --cores 1"
        )

    def test_wrapper_with_spaces_in_path(self):
        """Test wrapper path with spaces (edge case)."""
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(return_value="my workflow/wrapper script.sh")

        job_exec, _ = self.executor._get_base_exec_and_args(job)

        assert job_exec == "my workflow/wrapper script.sh"

    def test_wrapper_with_dots_in_path(self):
        """Test wrapper path with dots (e.g., hidden directories)."""
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(return_value=".hidden/scripts/wrapper.sh")

        job_exec, _ = self.executor._get_base_exec_and_args(job)

        assert job_exec == ".hidden/scripts/wrapper.sh"

    def test_wrapper_absolute_path(self):
        """Test that absolute wrapper paths are preserved."""
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(return_value="/home/user/workflow/wrapper.sh")

        job_exec, _ = self.executor._get_base_exec_and_args(job)

        assert job_exec == "/home/user/workflow/wrapper.sh"

    def test_wrapper_empty_string_treated_as_no_wrapper(self):
        """Test that empty string wrapper falls back to Python."""
        job = Mock()
        job.resources = Mock()
        job.resources.get = Mock(return_value="")  # Empty string is falsy

        job_exec, _ = self.executor._get_base_exec_and_args(job)

        # Empty string is falsy, so should use sys.executable
        import sys

        assert job_exec == sys.executable
