"""
Unit tests for config file path handling in HTCondor executor.

These tests verify the behavior of _prepare_config_files_for_transfer() which handles:
1. Converting absolute paths back to relative paths for non-shared FS config files
2. Preserving absolute paths for config files on shared filesystems
3. Correctly populating transfer_input_files and --configfiles arguments

Test scenarios cover --shared-fs-usage none with various combinations of
shared filesystem prefixes and config file locations.
"""

from unittest.mock import Mock, MagicMock
from snakemake_executor_plugin_htcondor import Executor
import sys


class TestPrepareConfigFilesForTransfer:
    """Test the _prepare_config_files_for_transfer method with shared FS handling."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.workflow = Mock()
        self.executor.logger = Mock()
        # Default workdir_init - the directory where snakemake was invoked
        self.executor.workflow.workdir_init = "/home/user/project"

        # Bind the actual method to our mock
        self.executor._prepare_config_files_for_transfer = (
            Executor._prepare_config_files_for_transfer.__get__(self.executor, Executor)
        )

    # =========================================================================
    # Basic functionality tests
    # =========================================================================

    def test_no_config_files_returns_unchanged_args(self):
        """Test when no config files are present."""
        self.executor.workflow.configfiles = None
        job_args = "--target-jobs test --cores 1"

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        assert modified_args == job_args
        assert config_paths == []

    def test_empty_config_files_returns_unchanged_args(self):
        """Test when configfiles list is empty."""
        self.executor.workflow.configfiles = []
        job_args = "--target-jobs test --cores 1"

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        assert modified_args == job_args
        assert config_paths == []

    # =========================================================================
    # Non-shared filesystem tests (files must be transferred)
    # =========================================================================

    def test_single_config_file_no_shared_fs_prefixes(self):
        """Test single config file with no shared filesystem - must be transferred."""
        self.executor.workflow.configfiles = ["/home/user/project/workflow/config.yaml"]
        job_args = "--target-jobs test --configfiles /home/user/project/workflow/config.yaml --cores 1"

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        # Arguments should use relative path
        assert "--configfiles workflow/config.yaml" in modified_args
        assert "/home/user/project/workflow/config.yaml" not in modified_args

        # Transfer list should include relative path
        assert config_paths == ["workflow/config.yaml"]

    def test_multiple_config_files_no_shared_fs_prefixes(self):
        """Test multiple config files with no shared filesystem - all must be transferred."""
        self.executor.workflow.configfiles = [
            "/home/user/project/workflow/config.yaml",
            "/home/user/project/modules/text_utils/config_text_utils.yaml",
        ]
        job_args = (
            "--target-jobs test "
            "--configfiles /home/user/project/workflow/config.yaml /home/user/project/modules/text_utils/config_text_utils.yaml "
            "--cores 1"
        )

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        # Arguments should use relative paths
        assert "workflow/config.yaml" in modified_args
        assert "modules/text_utils/config_text_utils.yaml" in modified_args
        assert "/home/user/project" not in modified_args

        # Transfer list should include both relative paths
        assert "workflow/config.yaml" in config_paths
        assert "modules/text_utils/config_text_utils.yaml" in config_paths
        assert len(config_paths) == 2

    def test_nested_config_path_preserved(self):
        """Test that nested directory structure is preserved in relative paths."""
        self.executor.workflow.configfiles = [
            "/home/user/project/deep/nested/path/config.yaml"
        ]
        job_args = (
            "--configfiles /home/user/project/deep/nested/path/config.yaml --cores 1"
        )

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        # Full relative path should be preserved
        assert "--configfiles deep/nested/path/config.yaml" in modified_args
        assert config_paths == ["deep/nested/path/config.yaml"]

    # =========================================================================
    # Shared filesystem tests (files should NOT be transferred)
    # =========================================================================

    def test_config_on_shared_fs_not_transferred(self):
        """Test config file on shared filesystem - should NOT be transferred."""
        self.executor.workflow.configfiles = ["/staging/project/config.yaml"]
        job_args = (
            "--target-jobs test --configfiles /staging/project/config.yaml --cores 1"
        )

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=["/staging/"]
        )

        # Arguments should keep absolute path for shared FS file
        assert "--configfiles /staging/project/config.yaml" in modified_args

        # Transfer list should be empty (file accessed directly on shared FS)
        assert config_paths == []

    def test_config_on_shared_fs_absolute_path_preserved(self):
        """Test that absolute path is preserved in args for shared FS config."""
        self.executor.workflow.configfiles = ["/shared/configs/workflow.yaml"]
        job_args = "--configfiles /shared/configs/workflow.yaml --cores 1"

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=["/shared"]
        )

        # Absolute path must be preserved so Snakemake can access it on the EP
        assert "/shared/configs/workflow.yaml" in modified_args
        assert config_paths == []

    # =========================================================================
    # Mixed filesystem tests (some shared, some not)
    # =========================================================================

    def test_mixed_shared_and_local_config_files(self):
        """Test mix of shared FS and local config files."""
        self.executor.workflow.configfiles = [
            "/home/user/project/workflow/config.yaml",  # Local - must transfer
            "/staging/shared/global_config.yaml",  # Shared - don't transfer
        ]
        job_args = (
            "--configfiles /home/user/project/workflow/config.yaml /staging/shared/global_config.yaml "
            "--cores 1"
        )

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=["/staging/"]
        )

        # Local file should use relative path
        assert "workflow/config.yaml" in modified_args
        assert "/home/user/project/workflow/config.yaml" not in modified_args
        # Shared file should keep absolute path
        assert "/staging/shared/global_config.yaml" in modified_args

        # Only local file should be in transfer list
        assert config_paths == ["workflow/config.yaml"]

    def test_multiple_shared_fs_prefixes(self):
        """Test with multiple shared filesystem prefixes."""
        self.executor.workflow.configfiles = [
            "/home/user/project/local_config.yaml",  # Local
            "/staging/config1.yaml",  # Shared (staging)
            "/shared/config2.yaml",  # Shared (shared)
            "/home/user/project/another_local.yaml",  # Local
        ]
        job_args = (
            "--configfiles /home/user/project/local_config.yaml /staging/config1.yaml "
            "/shared/config2.yaml /home/user/project/another_local.yaml --cores 1"
        )

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=["/staging/", "/shared/"]
        )

        # Check arguments
        assert "local_config.yaml" in modified_args  # Relative
        assert "/staging/config1.yaml" in modified_args  # Absolute (shared)
        assert "/shared/config2.yaml" in modified_args  # Absolute (shared)
        assert "another_local.yaml" in modified_args  # Relative
        assert "/home/user/project" not in modified_args  # No absolute paths for locals

        # Only local files should be transferred
        assert "local_config.yaml" in config_paths
        assert "another_local.yaml" in config_paths
        assert "/home/user/project" not in config_paths
        assert len(config_paths) == 2

    # =========================================================================
    # Edge cases
    # =========================================================================

    def test_config_file_already_relative(self):
        """Test handling of config file that's already a relative path."""
        # This is an edge case - normally Snakemake converts to absolute
        self.executor.workflow.configfiles = ["workflow/config.yaml"]
        job_args = "--configfiles workflow/config.yaml --cores 1"

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        # Should keep relative path as-is
        assert "--configfiles workflow/config.yaml" in modified_args
        assert config_paths == ["workflow/config.yaml"]

    def test_config_at_workdir_root(self):
        """Test config file at the workdir root (no subdirectory)."""
        self.executor.workflow.configfiles = ["/home/user/project/config.yaml"]
        job_args = "--configfiles /home/user/project/config.yaml --cores 1"

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        # Should be just the filename (relative to workdir)
        assert "--configfiles config.yaml" in modified_args
        assert config_paths == ["config.yaml"]

    def test_configfiles_at_end_of_args(self):
        """Test --configfiles appearing at the end of arguments string."""
        self.executor.workflow.configfiles = ["/home/user/project/config.yaml"]
        job_args = (
            "--target-jobs test --cores 1 --configfiles /home/user/project/config.yaml"
        )

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        assert "--configfiles config.yaml" in modified_args
        assert "--target-jobs test" in modified_args
        assert "--cores 1" in modified_args

    def test_no_configfiles_in_args(self):
        """Test when --configfiles not present in args but configfiles exist."""
        self.executor.workflow.configfiles = ["/home/user/project/config.yaml"]
        job_args = "--target-jobs test --cores 1"  # No --configfiles

        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        # Args unchanged (regex won't match), but config_paths still populated
        assert modified_args == job_args
        assert config_paths == ["config.yaml"]

    def test_shared_fs_prefix_partial_match_rejected(self):
        """Test that partial prefix matches don't count as shared FS."""
        import pytest
        from snakemake.exceptions import WorkflowError

        # /staging2/ should NOT match /staging/ prefix
        self.executor.workflow.configfiles = ["/staging2/config.yaml"]
        job_args = "--configfiles /staging2/config.yaml --cores 1"

        # This should raise an error because /staging2/ is not on shared FS
        # and is outside the working directory
        with pytest.raises(WorkflowError) as exc_info:
            self.executor._prepare_config_files_for_transfer(
                job_args, shared_fs_prefixes=["/staging/"]
            )

        error_msg = str(exc_info.value)
        assert "/staging2/config.yaml" in error_msg
        assert "outside the working directory" in error_msg

    def test_shared_fs_config_can_be_outside_workdir(self):
        """Shared FS config files can be outside workdir since they use absolute paths."""
        # Config on shared FS, but outside working directory - this is OK!
        self.executor.workflow.configfiles = ["/staging/shared_config.yaml"]
        job_args = "--configfiles /staging/shared_config.yaml --cores 1"

        # Should succeed without error
        modified_args, config_paths = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=["/staging/"]
        )

        # Shared FS file keeps absolute path and is NOT transferred
        assert config_paths == []
        assert "--configfiles /staging/shared_config.yaml" in modified_args


class TestConfigFilePathsIntegration:
    """Integration-style tests for config file path handling in run_job context."""

    def setup_method(self):
        """Setup mock executor with full context."""
        self.executor = Mock(spec=Executor)
        self.executor.workflow = Mock()
        self.executor.workflow.workdir_init = "/home/user/project"
        self.executor.workflow.storage_settings = Mock()
        self.executor.workflow.storage_settings.shared_fs_usage = []  # "none"
        self.executor.shared_fs_prefixes = []
        self.executor.logger = Mock()

        # Bind methods
        self.executor._prepare_config_files_for_transfer = (
            Executor._prepare_config_files_for_transfer.__get__(self.executor, Executor)
        )

    def test_shared_fs_usage_none_local_config(self):
        """
        Integration test: --shared-fs-usage none with local config file.

        Expected behavior:
        - Config file IS added to transfer_input_files (as relative path)
        - --configfiles argument uses relative path
        """
        self.executor.workflow.configfiles = ["/home/user/project/workflow/config.yaml"]
        job_args = "--configfiles /home/user/project/workflow/config.yaml --cores all"

        modified_args, transfer_configs = (
            self.executor._prepare_config_files_for_transfer(
                job_args, shared_fs_prefixes=[]
            )
        )

        # Verify transfer list
        assert transfer_configs == ["workflow/config.yaml"]

        # Verify arguments
        assert "--configfiles workflow/config.yaml" in modified_args

    def test_shared_fs_usage_none_with_shared_prefix_config_on_shared(self):
        """
        Integration test: --shared-fs-usage none with --htcondor-shared-fs-prefixes,
        config file IS on the shared filesystem.

        Expected behavior:
        - Config file is NOT added to transfer_input_files
        - --configfiles argument keeps absolute path
        """
        self.executor.workflow.configfiles = ["/staging/project/config.yaml"]
        self.executor.shared_fs_prefixes = ["/staging/"]
        job_args = "--configfiles /staging/project/config.yaml --cores all"

        modified_args, transfer_configs = (
            self.executor._prepare_config_files_for_transfer(
                job_args, shared_fs_prefixes=["/staging/"]
            )
        )

        # Verify transfer list is empty
        assert transfer_configs == []

        # Verify absolute path preserved in arguments
        assert "--configfiles /staging/project/config.yaml" in modified_args

    def test_shared_fs_usage_none_with_shared_prefix_config_not_on_shared(self):
        """
        Integration test: --shared-fs-usage none with --htcondor-shared-fs-prefixes,
        config file is NOT on the shared filesystem.

        Expected behavior:
        - Config file IS added to transfer_input_files (as relative path)
        - --configfiles argument uses relative path
        """
        self.executor.workflow.configfiles = ["/home/user/project/workflow/config.yaml"]
        job_args = "--configfiles /home/user/project/workflow/config.yaml --cores all"

        modified_args, transfer_configs = (
            self.executor._prepare_config_files_for_transfer(
                job_args,
                shared_fs_prefixes=["/staging/"],  # Config not on this FS
            )
        )

        # Verify transfer list includes config
        assert transfer_configs == ["workflow/config.yaml"]

        # Verify relative path in arguments
        assert "--configfiles workflow/config.yaml" in modified_args

    def test_shared_fs_usage_none_mixed_configs(self):
        """
        Integration test: --shared-fs-usage none with mixed config file locations.

        Expected behavior:
        - Local config IS added to transfer_input_files (relative path)
        - Shared FS config is NOT added to transfer_input_files
        - --configfiles has relative path for local, absolute for shared
        """
        self.executor.workflow.configfiles = [
            "/home/user/project/workflow/config.yaml",  # Local
            "/staging/shared_config.yaml",  # Shared
        ]
        job_args = (
            "--configfiles /home/user/project/workflow/config.yaml /staging/shared_config.yaml "
            "--cores all"
        )

        modified_args, transfer_configs = (
            self.executor._prepare_config_files_for_transfer(
                job_args, shared_fs_prefixes=["/staging/"]
            )
        )

        # Only local config should be transferred
        assert transfer_configs == ["workflow/config.yaml"]

        # Arguments should have relative for local, absolute for shared
        assert "workflow/config.yaml" in modified_args
        assert "/staging/shared_config.yaml" in modified_args


class TestFilesystemModes:
    """
    Test the three filesystem sharing modes in _get_exec_args_and_transfer_files.

    Mode 1: Full shared FS (needs_transfer=False)
        - AP and EP share a filesystem
        - Absolute paths work everywhere
        - No files need to be transferred
        - Arguments remain unchanged

    Mode 2: No shared FS (needs_transfer=True, shared_fs_prefixes=[])
        - AP and EP share nothing
        - All files must be transferred
        - Paths converted to relative for HTCondor's preserve_relative_paths

    Mode 3: Partial shared FS (needs_transfer=True, shared_fs_prefixes=['/staging'])
        - AP and EP share specific paths only
        - Files on shared FS: absolute paths, no transfer
        - Files not on shared FS: relative paths, transferred
    """

    def setup_method(self):
        """Setup mock executor with all required methods."""
        self.executor = Mock(spec=Executor)
        self.executor.workflow = Mock()
        self.executor.workflow.workdir_init = "/home/user/project"
        self.executor.workflow.configfiles = ["/home/user/project/config.yaml"]
        self.executor.shared_fs_prefixes = []
        self.executor.logger = Mock()
        self.executor.get_snakefile = Mock(return_value="/home/user/project/Snakefile")
        self.executor.get_python_executable = Mock(return_value="python")

        # Create mock job
        self.job = Mock()
        self.job.is_group = Mock(return_value=False)
        self.job.input = []
        self.job.output = []
        self.job.threads = 1
        self.job.resources = Mock()
        self.job.resources.get = Mock(return_value=None)

        # Setup job_exec_prefix mock - returns the snakemake command
        self.executor.format_job_exec = Mock(
            return_value="python -m snakemake --configfiles /home/user/project/config.yaml --cores 1"
        )

        # Bind the actual methods to our mock
        self.executor._get_exec_args_and_transfer_files = (
            Executor._get_exec_args_and_transfer_files.__get__(self.executor, Executor)
        )
        self.executor._get_base_exec_and_args = (
            Executor._get_base_exec_and_args.__get__(self.executor, Executor)
        )
        self.executor._get_files_for_transfer = (
            Executor._get_files_for_transfer.__get__(self.executor, Executor)
        )
        self.executor._prepare_config_files_for_transfer = (
            Executor._prepare_config_files_for_transfer.__get__(self.executor, Executor)
        )
        self.executor._sanitize_job_args = Executor._sanitize_job_args.__get__(
            self.executor, Executor
        )

    def test_mode1_full_shared_fs_no_transfer(self):
        """
        Mode 1: Full shared filesystem - no file transfer needed.

        When AP and EP share a filesystem (shared_fs_usage is set),
        needs_transfer=False and:
        - Arguments should remain unchanged (absolute paths are fine)
        - Transfer lists should be empty
        """
        job_exec, job_args, transfer_in, transfer_out = (
            self.executor._get_exec_args_and_transfer_files(
                self.job, needs_transfer=False
            )
        )

        # Arguments should NOT be modified, will contain abs path for the config file
        assert "/home/user/project/config.yaml" in job_args
        assert (
            "-m snakemake --configfiles /home/user/project/config.yaml --cores 1"
            in job_args
        )
        assert job_exec == sys.executable

        # No files should be transferred
        assert transfer_in == []
        assert transfer_out == []

    def test_mode2_no_shared_fs_all_transferred(self):
        """
        Mode 2: No shared filesystem - all files must be transferred.

        When AP and EP share nothing (shared_fs_usage=none, no shared_fs_prefixes):
        - Config file paths converted to relative
        - All files added to transfer lists
        """
        self.executor.shared_fs_prefixes = []
        self.executor.workflow.configfiles = [
            "/home/user/project/nested/config.yaml",
        ]

        job_exec, job_args, transfer_in, transfer_out = (
            self.executor._get_exec_args_and_transfer_files(
                self.job, needs_transfer=True
            )
        )

        # Arguments should use relative path for config
        assert "--configfiles nested/config.yaml" in job_args
        assert "/home/user/project/nested/config.yaml" not in job_args

        # Config file should be in transfer list (as relative path)
        assert "nested/config.yaml" in transfer_in

    def test_mode3_partial_shared_fs_mixed_handling(self):
        """
        Mode 3: Partial shared filesystem - mixed file handling.

        When AP and EP share some paths (e.g., /staging):
        - Files on shared FS keep absolute paths and are NOT transferred
        - Files not on shared FS use relative paths and ARE transferred
        """
        # Setup: one local config, one on shared FS
        self.executor.workflow.configfiles = [
            "/home/user/project/local_config.yaml",
            "/staging/shared_config.yaml",
        ]
        self.executor.shared_fs_prefixes = ["/staging/"]
        self.executor.format_job_exec = Mock(
            return_value="python -m snakemake --configfiles /home/user/project/local_config.yaml /staging/shared_config.yaml --cores 1"
        )

        job_exec, job_args, transfer_in, transfer_out = (
            self.executor._get_exec_args_and_transfer_files(
                self.job, needs_transfer=True
            )
        )

        # Local config should use relative path
        assert "local_config.yaml" in job_args
        # Shared config should keep absolute path
        assert "/staging/shared_config.yaml" in job_args

        # Only local config should be transferred
        assert "local_config.yaml" in transfer_in
        assert "/staging/shared_config.yaml" not in transfer_in

    def test_mode1_skips_expensive_operations(self):
        """
        Mode 1 should skip file transfer preparation entirely.

        This is an efficiency test - when needs_transfer=False,
        we should not call the methods that compute transfer lists.
        """
        # Wrap the methods to track if they're called
        original_get_files = self.executor._get_files_for_transfer
        original_prepare_config = self.executor._prepare_config_files_for_transfer

        get_files_called = []
        prepare_config_called = []

        def track_get_files(*args, **kwargs):
            get_files_called.append(True)
            return original_get_files(*args, **kwargs)

        def track_prepare_config(*args, **kwargs):
            prepare_config_called.append(True)
            return original_prepare_config(*args, **kwargs)

        self.executor._get_files_for_transfer = track_get_files
        self.executor._prepare_config_files_for_transfer = track_prepare_config

        # Call with needs_transfer=False
        self.executor._get_exec_args_and_transfer_files = (
            Executor._get_exec_args_and_transfer_files.__get__(self.executor, Executor)
        )
        self.executor._get_exec_args_and_transfer_files(self.job, needs_transfer=False)

        # Neither method should have been called
        assert (
            len(get_files_called) == 0
        ), "_get_files_for_transfer should not be called"
        assert (
            len(prepare_config_called) == 0
        ), "_prepare_config_files_for_transfer should not be called"


class TestConfigFilesOutsideWorkdir:
    """Test handling of config files outside the working directory."""

    def setup_method(self):
        """Create a mock executor for each test."""
        self.executor = MagicMock(spec=Executor)
        self.executor.workflow = MagicMock()
        self.executor.workflow.workdir_init = "/home/user/project"
        self.executor.logger = MagicMock()

        # Bind the method to test
        self.executor._prepare_config_files_for_transfer = (
            Executor._prepare_config_files_for_transfer.__get__(self.executor, Executor)
        )

    def test_config_outside_workdir_raises_error(self):
        """
        When a config file is outside workdir_init, an error should be raised.

        This addresses the concern that relpath() will generate paths with '../'
        components, which cannot be transferred correctly with HTCondor.
        """
        import pytest
        from snakemake.exceptions import WorkflowError

        # Config file is outside the working directory
        self.executor.workflow.configfiles = ["/home/user/other_project/config.yaml"]
        job_args = "--configfiles /home/user/other_project/config.yaml --cores all"

        # Should raise WorkflowError
        with pytest.raises(WorkflowError) as exc_info:
            self.executor._prepare_config_files_for_transfer(
                job_args,
                shared_fs_prefixes=[],  # No shared FS
            )

        error_msg = str(exc_info.value)
        assert "/home/user/other_project/config.yaml" in error_msg
        assert "outside the working directory" in error_msg
        assert "directory that is above all config files" in error_msg

    def test_config_inside_workdir_no_error(self):
        """
        When a config file is inside workdir_init, no error should be raised.
        """
        self.executor.workflow.configfiles = ["/home/user/project/workflow/config.yaml"]
        job_args = "--configfiles /home/user/project/workflow/config.yaml --cores all"

        # Should succeed without error
        modified_args, transfer_configs = (
            self.executor._prepare_config_files_for_transfer(
                job_args,
                shared_fs_prefixes=[],  # No shared FS
            )
        )

        # Verify normal relative path (no ../)
        assert transfer_configs == ["workflow/config.yaml"]
        assert "--configfiles workflow/config.yaml" in modified_args

    def test_multiple_configs_some_outside_workdir(self):
        """
        When multiple config files exist, error should be raised for first one outside workdir.
        """
        import pytest
        from snakemake.exceptions import WorkflowError

        self.executor.workflow.configfiles = [
            "/home/user/project/workflow/config.yaml",  # Inside
            "/home/user/other/external.yaml",  # Outside - will trigger error
            "/home/user/project/params.yaml",  # Inside
        ]
        job_args = (
            "--configfiles /home/user/project/workflow/config.yaml "
            "/home/user/other/external.yaml /home/user/project/params.yaml --cores all"
        )

        # Should raise error on first file outside workdir
        with pytest.raises(WorkflowError) as exc_info:
            self.executor._prepare_config_files_for_transfer(
                job_args,
                shared_fs_prefixes=[],  # No shared FS
            )

        error_msg = str(exc_info.value)
        assert "/home/user/other/external.yaml" in error_msg

    def test_already_relative_path_with_dotdot_raises_error(self):
        """
        When a config file is already relative and starts with '..', error should be raised.

        This catches cases where users provide relative paths like '../config.yaml'
        that would escape the working directory.
        """
        import pytest
        from snakemake.exceptions import WorkflowError

        # Config file is already a relative path starting with ..
        self.executor.workflow.configfiles = ["../external/config.yaml"]
        job_args = "--configfiles ../external/config.yaml --cores all"

        # Should raise WorkflowError
        with pytest.raises(WorkflowError) as exc_info:
            self.executor._prepare_config_files_for_transfer(
                job_args,
                shared_fs_prefixes=[],  # No shared FS
            )

        error_msg = str(exc_info.value)
        assert "../external/config.yaml" in error_msg
        assert "outside the working directory" in error_msg
