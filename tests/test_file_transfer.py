"""
Unit tests for file transfer functionality in HTCondor executor.
"""

import tempfile
import os
from unittest.mock import Mock
from snakemake_executor_plugin_htcondor import Executor


class TestAddFileIfTransferable:
    """Test the _add_file_if_transferable helper method."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()
        self.executor.shared_fs_prefixes = ["/staging/", "/shared/"]

        # Bind the actual method to our mock
        self.executor._add_file_if_transferable = (
            Executor._add_file_if_transferable.__get__(self.executor, Executor)
        )

    def test_adds_file_not_on_shared_fs(self):
        """Test that files not on shared FS are added to transfer list."""
        transfer_list = []

        self.executor._add_file_if_transferable("/home/user/data.txt", transfer_list)

        assert "/home/user/data.txt" in transfer_list
        assert len(transfer_list) == 1

    def test_skips_file_on_shared_fs(self):
        """Test that files on shared FS are not added to transfer list."""
        transfer_list = []

        self.executor._add_file_if_transferable("/staging/user/data.txt", transfer_list)

        assert "/staging/user/data.txt" not in transfer_list
        assert len(transfer_list) == 0
        self.executor.logger.debug.assert_called()

    def test_skips_empty_filepath(self):
        """Test that empty filepaths are skipped."""
        transfer_list = []

        self.executor._add_file_if_transferable("", transfer_list)

        assert len(transfer_list) == 0
        # Should log that it's skipping
        self.executor.logger.debug.assert_called()

    def test_skips_none_filepath(self):
        """Test that None filepaths are skipped."""
        transfer_list = []

        self.executor._add_file_if_transferable(None, transfer_list)

        assert len(transfer_list) == 0

    def test_skips_whitespace_only_filepath(self):
        """Test that whitespace-only filepaths are skipped."""
        transfer_list = []

        self.executor._add_file_if_transferable("   ", transfer_list)

        assert len(transfer_list) == 0

    def test_normalizes_path(self):
        """Test that paths are normalized before adding."""
        transfer_list = []

        self.executor._add_file_if_transferable(
            "/home/user/./data/../data.txt", transfer_list
        )

        # Should be normalized to /home/user/data.txt
        assert "/home/user/data.txt" in transfer_list

    def test_deduplicates_files(self):
        """Test that duplicate files are not added multiple times."""
        transfer_list = []

        self.executor._add_file_if_transferable("/home/user/data.txt", transfer_list)
        self.executor._add_file_if_transferable("/home/user/data.txt", transfer_list)

        assert len(transfer_list) == 1

    def test_deduplicates_normalized_paths(self):
        """Test that normalized paths are deduplicated."""
        transfer_list = []

        self.executor._add_file_if_transferable("/home/user/data.txt", transfer_list)
        # Same file but with ./ in path
        self.executor._add_file_if_transferable("/home/user/./data.txt", transfer_list)

        assert len(transfer_list) == 1

    def test_expands_wildcards_when_requested(self):
        """Test that wildcards are expanded when expand_wildcards=True."""
        transfer_list = []
        job = Mock()
        job.wildcards = {"sample": "sample1"}
        job.params = {}
        job.format_wildcards = Mock(return_value="/home/user/sample1.txt")

        self.executor._add_file_if_transferable(
            "/home/user/{sample}.txt", transfer_list, expand_wildcards=True, job=job
        )

        job.format_wildcards.assert_called_once()
        assert "/home/user/sample1.txt" in transfer_list

    def test_validates_file_existence_by_default(self):
        """Test that file existence is validated by default."""
        transfer_list = []

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # File exists
            self.executor._add_file_if_transferable(
                tmp_path, transfer_list, validate_exists=True
            )

            # Should not log warning
            self.executor.logger.warning.assert_not_called()
            assert tmp_path in transfer_list
        finally:
            os.unlink(tmp_path)

    def test_warns_on_nonexistent_file(self):
        """Test that warning is logged for nonexistent files."""
        transfer_list = []
        nonexistent = "/home/user/doesnotexist.txt"

        self.executor._add_file_if_transferable(
            nonexistent, transfer_list, validate_exists=True
        )

        # Should log warning and mention the filepath
        self.executor.logger.warning.assert_called_once()
        warning_msg = self.executor.logger.warning.call_args[0][0]
        assert nonexistent in warning_msg

    def test_can_disable_existence_validation(self):
        """Test that file existence validation can be disabled."""
        transfer_list = []

        self.executor._add_file_if_transferable(
            "/home/user/doesnotexist.txt", transfer_list, validate_exists=False
        )

        # Should not log warning when validation is disabled
        self.executor.logger.warning.assert_not_called()

    def test_logs_debug_messages(self):
        """Test that appropriate debug messages are logged."""
        transfer_list = []

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name

        try:
            self.executor._add_file_if_transferable(tmp_path, transfer_list)

            # Should log debug messages
            self.executor.logger.debug.assert_called()
        finally:
            os.unlink(tmp_path)


class TestParseFileList:
    """Test the _parse_file_list helper method."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = Mock(spec=Executor)

        # Bind the actual method to our mock
        self.executor._parse_file_list = Executor._parse_file_list.__get__(
            self.executor, Executor
        )

    def test_parse_comma_separated_string(self):
        """Test parsing comma-separated string of files."""
        result = self.executor._parse_file_list("file1.txt,file2.txt,file3.txt")

        assert result == ["file1.txt", "file2.txt", "file3.txt"]

    def test_parse_string_with_whitespace(self):
        """Test parsing handles whitespace correctly."""
        result = self.executor._parse_file_list(" file1.txt , file2.txt , file3.txt ")

        assert result == ["file1.txt", "file2.txt", "file3.txt"]

    def test_parse_list_input(self):
        """Test that list input is returned as list."""
        input_list = ["file1.txt", "file2.txt", "file3.txt"]
        result = self.executor._parse_file_list(input_list)

        assert result == input_list

    def test_parse_empty_string(self):
        """Test that empty string returns empty list."""
        result = self.executor._parse_file_list("")

        assert result == []

    def test_parse_whitespace_only_string(self):
        """Test that whitespace-only string returns empty list."""
        result = self.executor._parse_file_list("  ,  , ")

        assert result == []

    def test_parse_single_file(self):
        """Test parsing single file (no commas)."""
        result = self.executor._parse_file_list("file1.txt")

        assert result == ["file1.txt"]

    def test_parse_empty_list(self):
        """Test that empty list input returns empty list."""
        result = self.executor._parse_file_list([])

        assert result == []


class TestScriptTransfer:
    """Test that script files from the script: directive are transferred."""

    def setup_method(self):
        """Setup mock executor and job for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()
        self.executor.shared_fs_prefixes = []
        self.executor.workflow = Mock()
        self.executor.workflow.configfiles = []
        self.executor.get_snakefile = Mock(return_value="Snakefile")

        # Bind the actual methods to our mock
        self.executor._get_files_for_transfer = (
            Executor._get_files_for_transfer.__get__(self.executor, Executor)
        )
        self.executor._add_file_if_transferable = (
            Executor._add_file_if_transferable.__get__(self.executor, Executor)
        )

        # Create mock job
        self.job = Mock()
        self.job.input = []
        self.job.output = []
        self.job.resources = Mock()
        self.job.resources.get = Mock(return_value=None)
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = None

    def test_script_file_transferred(self):
        """Test that script files are included in transfer list."""
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as tmp:
            script_path = tmp.name

        try:
            self.job.rule.script = script_path
            self.job.wildcards = {}
            self.job.params = {}
            self.job.format_wildcards = Mock(return_value=script_path)

            transfer_input, _ = self.executor._get_files_for_transfer(self.job)

            assert script_path in transfer_input
        finally:
            os.unlink(script_path)

    def test_script_with_wildcards_expanded(self):
        """Test that script paths with wildcards are expanded."""
        self.job.rule.script = "scripts/process_{sample}.py"
        self.job.wildcards = {"sample": "sample1"}
        self.job.params = {}
        self.job.format_wildcards = Mock(return_value="scripts/process_sample1.py")

        # Create the actual file so validation passes
        os.makedirs("scripts", exist_ok=True)
        with open("scripts/process_sample1.py", "w") as f:
            f.write("# test script")

        try:
            transfer_input, _ = self.executor._get_files_for_transfer(self.job)

            self.job.format_wildcards.assert_called()
            assert "scripts/process_sample1.py" in transfer_input
        finally:
            if os.path.exists("scripts/process_sample1.py"):
                os.unlink("scripts/process_sample1.py")
            if os.path.exists("scripts") and not os.listdir("scripts"):
                os.rmdir("scripts")

    def test_script_in_subdirectory_preserves_path(self):
        """Test that scripts in subdirectories preserve their relative path."""
        os.makedirs("workflow/scripts", exist_ok=True)
        script_path = "workflow/scripts/process.py"

        with open(script_path, "w") as f:
            f.write("# test script")

        try:
            self.job.rule.script = script_path
            self.job.wildcards = {}
            self.job.params = {}
            self.job.format_wildcards = Mock(return_value=script_path)

            transfer_input, _ = self.executor._get_files_for_transfer(self.job)

            # Full relative path must be preserved
            assert script_path in transfer_input
        finally:
            os.unlink(script_path)
            os.rmdir("workflow/scripts")
            os.rmdir("workflow")


class TestNotebookTransfer:
    """Test that notebook files from the notebook: directive are transferred."""

    def setup_method(self):
        """Setup mock executor and job for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()
        self.executor.shared_fs_prefixes = []
        self.executor.workflow = Mock()
        self.executor.workflow.configfiles = []
        self.executor.get_snakefile = Mock(return_value="Snakefile")

        # Bind the actual methods
        self.executor._get_files_for_transfer = (
            Executor._get_files_for_transfer.__get__(self.executor, Executor)
        )
        self.executor._add_file_if_transferable = (
            Executor._add_file_if_transferable.__get__(self.executor, Executor)
        )

        # Create mock job
        self.job = Mock()
        self.job.input = []
        self.job.output = []
        self.job.resources = Mock()
        self.job.resources.get = Mock(return_value=None)
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = None

    def test_notebook_file_transferred(self):
        """Test that notebook files are included in transfer list."""
        with tempfile.NamedTemporaryFile(suffix=".ipynb", delete=False) as tmp:
            notebook_path = tmp.name

        try:
            self.job.rule.notebook = notebook_path
            self.job.wildcards = {}
            self.job.params = {}
            self.job.format_wildcards = Mock(return_value=notebook_path)

            transfer_input, _ = self.executor._get_files_for_transfer(self.job)

            assert notebook_path in transfer_input
        finally:
            os.unlink(notebook_path)


class TestJobWrapperTransfer:
    """Test that job_wrapper resource is explicitly transferred."""

    def setup_method(self):
        """Setup mock executor and job for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()
        self.executor.shared_fs_prefixes = []
        self.executor.workflow = Mock()
        self.executor.workflow.configfiles = []
        self.executor.get_snakefile = Mock(return_value="Snakefile")

        # Bind the actual methods
        self.executor._get_files_for_transfer = (
            Executor._get_files_for_transfer.__get__(self.executor, Executor)
        )
        self.executor._add_file_if_transferable = (
            Executor._add_file_if_transferable.__get__(self.executor, Executor)
        )

        # Create mock job
        self.job = Mock()
        self.job.input = []
        self.job.output = []
        self.job.resources = Mock()
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = None

    def test_job_wrapper_transferred(self):
        """Test that job_wrapper is included in transfer list."""
        with tempfile.NamedTemporaryFile(suffix=".sh", delete=False) as tmp:
            wrapper_path = tmp.name

        try:
            self.job.resources.get = Mock(
                side_effect=lambda key, default=None: (
                    wrapper_path if key == "job_wrapper" else default
                )
            )

            transfer_input, _ = self.executor._get_files_for_transfer(self.job)

            assert wrapper_path in transfer_input
        finally:
            os.unlink(wrapper_path)

    def test_job_wrapper_in_subdirectory(self):
        """Test that job_wrapper in subdirectory preserves full path."""
        os.makedirs("workflow/wrappers", exist_ok=True)
        wrapper_path = "workflow/wrappers/htcondor.sh"

        with open(wrapper_path, "w") as f:
            f.write("#!/bin/bash\nsnakemake $@")

        try:
            self.job.resources.get = Mock(
                side_effect=lambda key, default=None: (
                    wrapper_path if key == "job_wrapper" else default
                )
            )

            transfer_input, _ = self.executor._get_files_for_transfer(self.job)

            assert wrapper_path in transfer_input
        finally:
            os.unlink(wrapper_path)
            os.rmdir("workflow/wrappers")
            os.rmdir("workflow")

    def test_job_wrapper_logs_debug(self):
        """Test that job_wrapper detection is logged."""
        with tempfile.NamedTemporaryFile(suffix=".sh", delete=False) as tmp:
            wrapper_path = tmp.name

        try:
            self.job.resources.get = Mock(
                side_effect=lambda key, default=None: (
                    wrapper_path if key == "job_wrapper" else default
                )
            )

            self.executor._get_files_for_transfer(self.job)

            # Should log debug messages
            self.executor.logger.debug.assert_called()
        finally:
            os.unlink(wrapper_path)


class TestCustomTransferResources:
    """Test htcondor_transfer_input_files and htcondor_transfer_output_files resources."""

    def setup_method(self):
        """Setup mock executor and job for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()
        self.executor.shared_fs_prefixes = []
        self.executor.workflow = Mock()
        self.executor.workflow.configfiles = []
        self.executor.get_snakefile = Mock(return_value="Snakefile")

        # Bind the actual methods
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
        self.job.input = []
        self.job.output = []
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = None

    def test_htcondor_transfer_input_files_string(self):
        """Test htcondor_transfer_input_files with comma-separated string."""
        # Create temporary files
        with tempfile.NamedTemporaryFile(
            delete=False
        ) as tmp1, tempfile.NamedTemporaryFile(delete=False) as tmp2:
            file1 = tmp1.name
            file2 = tmp2.name

        try:
            self.job.resources = Mock()
            self.job.resources.get = Mock(
                side_effect=lambda key, default=None: (
                    f"{file1},{file2}"
                    if key == "htcondor_transfer_input_files"
                    else default
                )
            )
            self.job.wildcards = {}
            self.job.params = {}
            self.job.format_wildcards = Mock(side_effect=lambda path, **kwargs: path)

            transfer_input, _ = self.executor._get_files_for_transfer(self.job)

            assert file1 in transfer_input
            assert file2 in transfer_input
        finally:
            os.unlink(file1)
            os.unlink(file2)

    def test_htcondor_transfer_input_files_list(self):
        """Test htcondor_transfer_input_files with list."""
        # Create temporary files
        with tempfile.NamedTemporaryFile(
            delete=False
        ) as tmp1, tempfile.NamedTemporaryFile(delete=False) as tmp2:
            file1 = tmp1.name
            file2 = tmp2.name

        try:
            self.job.resources = Mock()
            self.job.resources.get = Mock(
                side_effect=lambda key, default=None: (
                    [file1, file2]
                    if key == "htcondor_transfer_input_files"
                    else default
                )
            )
            self.job.wildcards = {}
            self.job.params = {}
            self.job.format_wildcards = Mock(side_effect=lambda path, **kwargs: path)

            transfer_input, _ = self.executor._get_files_for_transfer(self.job)

            assert file1 in transfer_input
            assert file2 in transfer_input
        finally:
            os.unlink(file1)
            os.unlink(file2)

    def test_htcondor_transfer_input_files_with_wildcards(self):
        """Test htcondor_transfer_input_files expands wildcards."""
        self.job.wildcards = {"module": "stats_helpers"}
        self.job.params = {}
        self.job.format_wildcards = Mock(return_value="scripts/stats_helpers.py")

        # Create the file
        os.makedirs("scripts", exist_ok=True)
        with open("scripts/stats_helpers.py", "w") as f:
            f.write("# helper module")

        try:
            self.job.resources = Mock()
            self.job.resources.get = Mock(
                side_effect=lambda key, default=None: (
                    "scripts/{module}.py"
                    if key == "htcondor_transfer_input_files"
                    else default
                )
            )

            transfer_input, _ = self.executor._get_files_for_transfer(self.job)

            self.job.format_wildcards.assert_called()
            assert "scripts/stats_helpers.py" in transfer_input
        finally:
            os.unlink("scripts/stats_helpers.py")
            os.rmdir("scripts")

    def test_htcondor_transfer_output_files_string(self):
        """Test htcondor_transfer_output_files with comma-separated string."""
        self.job.resources = Mock()
        self.job.resources.get = Mock(
            side_effect=lambda key, default=None: (
                "results/model.pkl,results/metrics.json"
                if key == "htcondor_transfer_output_files"
                else default
            )
        )
        self.job.wildcards = {}
        self.job.params = {}
        self.job.format_wildcards = Mock(side_effect=lambda path, **kwargs: path)

        _, transfer_output = self.executor._get_files_for_transfer(self.job)

        assert "results/model.pkl" in transfer_output
        assert "results/metrics.json" in transfer_output

    def test_htcondor_transfer_output_files_list(self):
        """Test htcondor_transfer_output_files with list."""
        self.job.resources = Mock()
        self.job.resources.get = Mock(
            side_effect=lambda key, default=None: (
                ["results/model.pkl", "results/metrics.json"]
                if key == "htcondor_transfer_output_files"
                else default
            )
        )
        self.job.wildcards = {}
        self.job.params = {}
        self.job.format_wildcards = Mock(side_effect=lambda path, **kwargs: path)

        _, transfer_output = self.executor._get_files_for_transfer(self.job)

        assert "results/model.pkl" in transfer_output
        assert "results/metrics.json" in transfer_output


class TestFileTransferLogging:
    """Test that file transfer operations log appropriately."""

    def setup_method(self):
        """Setup mock executor and job for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()
        self.executor.shared_fs_prefixes = []
        self.executor.workflow = Mock()
        self.executor.workflow.configfiles = []
        self.executor.get_snakefile = Mock(return_value="Snakefile")

        # Bind the actual method
        self.executor._get_files_for_transfer = (
            Executor._get_files_for_transfer.__get__(self.executor, Executor)
        )
        self.executor._add_file_if_transferable = (
            Executor._add_file_if_transferable.__get__(self.executor, Executor)
        )

        # Create mock job
        self.job = Mock()
        self.job.input = []
        self.job.output = []
        self.job.resources = Mock()
        self.job.resources.get = Mock(return_value=None)
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = None

    def test_logs_transfer_summary(self):
        """Test that summary of transfer files is logged."""
        _ = self.executor._get_files_for_transfer(self.job)

        # Should log at debug level
        self.executor.logger.debug.assert_called()

    def test_logs_job_identifier(self):
        """Test that job identifier is logged at start."""
        self.executor._get_files_for_transfer(self.job)

        # Should log debug messages
        self.executor.logger.debug.assert_called()
