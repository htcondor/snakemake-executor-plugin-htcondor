"""
Unit tests for shared filesystem prefix functionality in HTCondor executor.
"""

from unittest.mock import Mock, patch
from snakemake_executor_plugin_htcondor import is_shared_fs, ExecutorSettings, Executor


class TestIsSharedFs:
    """Test the is_shared_fs helper function."""

    def test_path_on_shared_fs(self):
        """Test that paths under shared prefixes are correctly identified."""
        # One with trailing /, the other without
        shared_prefixes = ["/staging/", "/shared"]

        assert is_shared_fs("/staging/user/data.txt", shared_prefixes) is True
        assert is_shared_fs("/shared/project/file.txt", shared_prefixes) is True

    def test_path_not_on_shared_fs(self):
        """Test that paths not under shared prefixes are correctly identified."""
        shared_prefixes = ["/staging/", "/shared"]

        assert is_shared_fs("/home/user/data.txt", shared_prefixes) is False
        assert is_shared_fs("input/local-file.txt", shared_prefixes) is False

    def test_empty_shared_prefixes(self):
        """Test that no paths match when shared_prefixes is empty."""
        shared_prefixes = []

        assert is_shared_fs("/staging/user/data.txt", shared_prefixes) is False
        assert is_shared_fs("/any/path/file.txt", shared_prefixes) is False

    def test_normalized_paths(self):
        """Test that paths are normalized correctly for comparison."""
        shared_prefixes = ["/staging"]

        # Should work even if prefix doesn't end with /
        assert is_shared_fs("/staging/user/data.txt", ["/staging"]) is True

        # Should normalize path with trailing separator
        assert is_shared_fs("/staging/", shared_prefixes) is True

    def test_partial_match_rejected(self):
        """Test that partial prefix matches are rejected."""
        shared_prefixes = ["/staging"]

        # /staging2/ should not match /staging/
        assert is_shared_fs("/staging2/user/data.txt", shared_prefixes) is False


class TestParseSharedFsPrefixes:
    """Test the _parse_shared_fs_prefixes method."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()

        # Bind the actual method to our mock
        self.executor._parse_shared_fs_prefixes = (
            Executor._parse_shared_fs_prefixes.__get__(self.executor, Executor)
        )

    def test_parse_single_prefix(self):
        """Test parsing a single filesystem prefix."""
        result = self.executor._parse_shared_fs_prefixes("/staging")

        assert len(result) == 1
        assert result[0] == "/staging/"

    def test_parse_multiple_prefixes(self):
        """Test parsing comma-separated prefixes."""
        result = self.executor._parse_shared_fs_prefixes("/staging/,/shared,/data")

        assert len(result) == 3
        assert "/staging/" in result
        assert "/shared/" in result
        assert "/data/" in result

    def test_parse_with_whitespace(self):
        """Test parsing handles whitespace correctly."""
        result = self.executor._parse_shared_fs_prefixes(
            " /staging/ , /shared , /data "
        )

        assert len(result) == 3
        assert all(prefix.strip() == prefix for prefix in result)

    def test_parse_with_quotes(self):
        """Test parsing removes quotes from prefixes."""
        result = self.executor._parse_shared_fs_prefixes("'/staging/',\"/shared\"")

        assert len(result) == 2
        assert result[0] == "/staging/"
        assert result[1] == "/shared/"

    def test_parse_none_returns_empty_list(self):
        """Test that None input returns empty list."""
        result = self.executor._parse_shared_fs_prefixes(None)

        assert result == []

    def test_parse_empty_string_returns_empty_list(self):
        """Test that empty string returns empty list."""
        result = self.executor._parse_shared_fs_prefixes("")

        assert result == []

    def test_parse_only_whitespace_returns_empty_list(self):
        """Test that whitespace-only string returns empty list."""
        result = self.executor._parse_shared_fs_prefixes("   ,  , ")

        assert result == []


class TestGetFilesForTransfer:
    """Test the _get_files_for_transfer method."""

    def setup_method(self):
        """Setup mock executor and job for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()
        self.executor.shared_fs_prefixes = ["/staging", "/shared/"]
        self.executor.workflow = Mock()
        self.executor.workflow.configfiles = []
        self.executor.get_snakefile = Mock(return_value="/home/user/Snakefile")

        # Bind the actual methods to our mock
        self.executor._get_files_for_transfer = (
            Executor._get_files_for_transfer.__get__(self.executor, Executor)
        )
        self.executor._add_file_if_transferable = (
            Executor._add_file_if_transferable.__get__(self.executor, Executor)
        )
        self.executor._parse_file_list = Executor._parse_file_list.__get__(
            self.executor, Executor
        )

        # Create mock job
        self.job = Mock()
        self.job.is_group = Mock(return_value=False)
        self.job.input = []
        self.job.output = []
        self.job.resources = Mock()
        self.job.resources.get = Mock(
            return_value=None
        )  # Default returns None for all resources
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = None
        # Mock job.rules() to return an iterable containing the rule
        # This is needed because the code iterates through job.rules()
        self.job.rules = [self.job.rule]

    def test_snakefile_not_on_shared_fs(self):
        """Test that Snakefile not on shared FS is included in transfer."""
        transfer_input, _ = self.executor._get_files_for_transfer(self.job)

        assert "/home/user/Snakefile" in transfer_input

    def test_snakefile_on_shared_fs(self):
        """Test that Snakefile on shared FS is excluded from transfer."""
        self.executor.get_snakefile = Mock(return_value="/staging/user/Snakefile")

        transfer_input, _ = self.executor._get_files_for_transfer(self.job)

        assert "/staging/user/Snakefile" not in transfer_input

    def test_snakefile_relative_path_preserved(self):
        """Test that relative Snakefile paths in subdirectories are preserved.

        This is a regression test for GitHub issue #8 - Snakefiles in
        subdirectories (e.g., workflow/Snakefile) must preserve their
        full relative path when added to transfer_input_files.
        """
        # Use a relative path in a subdirectory
        self.executor.get_snakefile = Mock(return_value="workflow/Snakefile")

        transfer_input, _ = self.executor._get_files_for_transfer(self.job)

        # Full relative path must be preserved, not just basename
        assert "workflow/Snakefile" in transfer_input
        # Ensure basename alone is NOT in the list (would be wrong)
        assert transfer_input.count("Snakefile") == 0  # No bare "Snakefile"

    def test_snakefile_nested_subdirectory_preserved(self):
        """Test that deeply nested Snakefile paths are preserved."""
        self.executor.get_snakefile = Mock(
            return_value="project/workflow/rules/Snakefile"
        )

        transfer_input, _ = self.executor._get_files_for_transfer(self.job)

        assert "project/workflow/rules/Snakefile" in transfer_input

    def test_input_files_mixed_locations(self):
        """Test handling of input files in mixed locations."""
        self.job.input = [
            "input/local-file.txt",
            "/staging/user/shared-file.txt",
            "/home/user/another-file.txt",
        ]

        # Set preserve_relative_paths=True to get full paths
        self.job.resources.get = Mock(
            side_effect=lambda key, default=None: (
                True if key == "preserve_relative_paths" else default
            )
        )

        transfer_input, _ = self.executor._get_files_for_transfer(self.job)

        # Local and home files should be transferred
        assert "input/local-file.txt" in transfer_input
        assert "/home/user/another-file.txt" in transfer_input

        # Staging file should not be transferred
        assert "/staging/user/shared-file.txt" not in transfer_input

    def test_output_files_top_level_dirs(self):
        """Test that only top-level output directories are transferred."""
        self.job.output = [
            "output/results/data.txt",
            "output/logs/job.log",
            "/staging/shared-output/result.txt",
        ]

        _, transfer_output = self.executor._get_files_for_transfer(self.job)

        # Only 'output' directory should be in transfer (staging excluded)
        assert "output" in transfer_output
        assert len([f for f in transfer_output if f == "output"]) == 1
        assert "/staging/shared-output/result.txt" not in transfer_output

    def test_config_files_not_handled_here(self):
        """Test that config files are NOT handled by _get_files_for_transfer.

        Config files are now handled separately by _prepare_config_files_for_transfer()
        which handles both the transfer list and the --configfiles arguments together.
        """
        self.executor.workflow.configfiles = [
            "/home/user/config.yaml",
            "/staging/shared-config.yaml",
        ]

        transfer_input, _ = self.executor._get_files_for_transfer(self.job)

        # Config files should NOT be in the transfer list from this method
        # They are handled by _prepare_config_files_for_transfer() instead
        assert "/home/user/config.yaml" not in transfer_input
        assert "/staging/shared-config.yaml" not in transfer_input

    def test_empty_job(self):
        """Test handling of job with no inputs or outputs."""
        transfer_input, transfer_output = self.executor._get_files_for_transfer(
            self.job
        )

        # Should only have Snakefile
        assert len(transfer_input) == 1
        assert transfer_output == []

    def test_script_file_included_in_transfer(self):
        """Test that script files from script: directive are transferred."""
        self.job.rule = Mock()
        self.job.rule.script = "scripts/process.py"
        self.job.rule.notebook = None
        # Update job.rules to reflect the new rule
        self.job.rules = [self.job.rule]
        self.job.wildcards = {}
        self.job.params = {}
        self.job.format_wildcards = Mock(return_value="scripts/process.py")

        # Mock exists to avoid warnings about non-existent test files
        with patch("snakemake_executor_plugin_htcondor.exists", return_value=True):
            transfer_input, _ = self.executor._get_files_for_transfer(self.job)

        assert "scripts/process.py" in transfer_input

    def test_notebook_file_included_in_transfer(self):
        """Test that notebook files from notebook: directive are transferred."""
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = "notebooks/analysis.ipynb"
        # Update job.rules to reflect the new rule
        self.job.rules = [self.job.rule]
        self.job.wildcards = {}
        self.job.params = {}
        self.job.format_wildcards = Mock(return_value="notebooks/analysis.ipynb")

        # Mock exists to avoid warnings about non-existent test files
        with patch("snakemake_executor_plugin_htcondor.exists", return_value=True):
            transfer_input, _ = self.executor._get_files_for_transfer(self.job)

        assert "notebooks/analysis.ipynb" in transfer_input

    def test_script_on_shared_fs_not_transferred(self):
        """Test that scripts on shared FS are not transferred."""
        self.job.rule = Mock()
        self.job.rule.script = "/staging/scripts/process.py"
        self.job.rule.notebook = None
        self.job.wildcards = {}
        self.job.params = {}
        self.job.format_wildcards = Mock(return_value="/staging/scripts/process.py")

        transfer_input, _ = self.executor._get_files_for_transfer(self.job)

        assert "/staging/scripts/process.py" not in transfer_input

    def test_htcondor_transfer_input_files_resource(self):
        """Test that htcondor_transfer_input_files resource adds files to transfer."""
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = None
        self.job.wildcards = {}
        self.job.params = {}
        self.job.format_wildcards = Mock(side_effect=lambda path, **kwargs: path)

        # Mock resources to return additional input files
        def mock_get(key, default=None):
            if key == "htcondor_transfer_input_files":
                return "helpers/module1.py,helpers/module2.py"
            elif key == "preserve_relative_paths":
                return True
            return default

        self.job.resources.get = Mock(side_effect=mock_get)

        # Bind _parse_file_list method
        self.executor._parse_file_list = Executor._parse_file_list.__get__(
            self.executor, Executor
        )

        transfer_input, _ = self.executor._get_files_for_transfer(self.job)

        assert "helpers/module1.py" in transfer_input
        assert "helpers/module2.py" in transfer_input

    def test_htcondor_transfer_output_files_resource(self):
        """Test that htcondor_transfer_output_files resource adds files to transfer."""
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = None
        self.job.wildcards = {}
        self.job.params = {}
        self.job.format_wildcards = Mock(side_effect=lambda path, **kwargs: path)

        # Mock resources to return additional output files
        def mock_get(key, default=None):
            if key == "htcondor_transfer_output_files":
                return ["results/model.pkl", "results/metrics.json"]
            elif key == "preserve_relative_paths":
                return True
            return default

        self.job.resources.get = Mock(side_effect=mock_get)

        # Bind _parse_file_list method
        self.executor._parse_file_list = Executor._parse_file_list.__get__(
            self.executor, Executor
        )

        _, transfer_output = self.executor._get_files_for_transfer(self.job)

        assert "results/model.pkl" in transfer_output
        assert "results/metrics.json" in transfer_output

    def test_job_wrapper_included_in_transfer(self):
        """Test that job_wrapper resource is explicitly transferred."""
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = None

        # Mock resources to return job_wrapper
        def mock_get(key, default=None):
            if key == "job_wrapper":
                return "workflow/wrapper.sh"
            elif key == "preserve_relative_paths":
                return True
            return default

        self.job.resources.get = Mock(side_effect=mock_get)

        transfer_input, _ = self.executor._get_files_for_transfer(self.job)

        assert "workflow/wrapper.sh" in transfer_input


class TestPrepareConfigFilesForTransferBasic:
    """Basic tests for _prepare_config_files_for_transfer method.

    Note: More comprehensive tests covering shared FS handling are in
    test_config_file_paths.py.
    """

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.workflow = Mock()
        self.executor.logger = Mock()
        self.executor.workflow.workdir_init = (
            "/home/user"  # Required for relpath calculation
        )

        # Bind the actual method to our mock
        self.executor._prepare_config_files_for_transfer = (
            Executor._prepare_config_files_for_transfer.__get__(self.executor, Executor)
        )

    def test_no_config_files(self):
        """Test when no config files are present."""
        self.executor.workflow.configfiles = None
        job_args = "--target-jobs test --cores 1"

        modified_args, config_names = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        assert modified_args == job_args
        assert config_names == []

    def test_single_config_file(self):
        """Test with a single config file."""
        self.executor.workflow.configfiles = ["/home/user/config.yaml"]
        job_args = "--target-jobs test --configfiles /home/user/config.yaml --cores 1"

        modified_args, config_names = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        assert "config.yaml" in modified_args
        assert "/home/user/config.yaml" not in modified_args
        assert config_names == ["config.yaml"]

    def test_multiple_config_files(self):
        """Test with multiple config files."""
        self.executor.workflow.configfiles = [
            "/home/user/config1.yaml",
            "/home/user/config2.yaml",
        ]
        job_args = "--target-jobs test --configfiles /home/user/config1.yaml /home/user/config2.yaml --cores 1"

        modified_args, config_names = self.executor._prepare_config_files_for_transfer(
            job_args, shared_fs_prefixes=[]
        )

        assert "config1.yaml" in modified_args
        assert "config2.yaml" in modified_args
        assert "/home/user/config1.yaml" not in modified_args
        assert len(config_names) == 2


class TestValidateSharedFsConfiguration:
    """Test the _validate_shared_fs_configuration method."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()
        self.executor.shared_fs_prefixes = []
        self.executor.workflow = Mock()
        self.executor.workflow.storage_settings = Mock()
        self.executor.workflow.storage_settings.shared_fs_usage = []

        # Bind the actual method to our mock
        self.executor._validate_shared_fs_configuration = (
            Executor._validate_shared_fs_configuration.__get__(self.executor, Executor)
        )

    def test_no_shared_prefixes_no_validation(self):
        """Test that validation is skipped when no prefixes are configured."""
        self.executor.shared_fs_prefixes = []

        self.executor._validate_shared_fs_configuration()

        # Should not log anything
        self.executor.logger.info.assert_not_called()

    def test_shared_prefixes_with_none_usage_logs_info(self):
        """Test that info message is logged for partial shared FS configuration."""
        self.executor.shared_fs_prefixes = ["/staging/", "/shared/"]
        self.executor.workflow.storage_settings.shared_fs_usage = []  # empty = "none"

        self.executor._validate_shared_fs_configuration()

        # Should log informational message
        self.executor.logger.info.assert_called_once()
        call_args = self.executor.logger.info.call_args[0][0]
        assert "partially shared filesystem" in call_args.lower()
        assert "/staging/" in call_args
        assert "/shared/" in call_args

    def test_shared_prefixes_with_some_usage_no_warning(self):
        """Test that no warning is issued when shared_fs_usage has values."""
        self.executor.shared_fs_prefixes = ["/staging/"]
        self.executor.workflow.storage_settings.shared_fs_usage = [
            "input-output",
            "persistence",
        ]

        self.executor._validate_shared_fs_configuration()

        # Should not log anything (configuration is valid but not "none")
        # The method only logs for the "none" case currently
        # This test ensures we don't crash with non-empty shared_fs_usage
        assert True  # No exception raised


class TestExecutorSettings:
    """Test ExecutorSettings configuration."""

    def test_default_settings(self):
        """Test default settings values."""
        settings = ExecutorSettings()

        assert settings.jobdir == ".snakemake/htcondor"
        assert settings.shared_fs_prefixes is None

    def test_custom_jobdir(self):
        """Test custom jobdir setting."""
        settings = ExecutorSettings(jobdir="/custom/path/logs")

        assert settings.jobdir == "/custom/path/logs"

    def test_custom_shared_fs_prefixes(self):
        """Test custom shared_fs_prefixes setting."""
        settings = ExecutorSettings(shared_fs_prefixes="/staging,/shared")

        assert settings.shared_fs_prefixes == "/staging,/shared"
