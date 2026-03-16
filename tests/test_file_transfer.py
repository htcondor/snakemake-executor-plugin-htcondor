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
        self.executor.workflow.workdir_init = "/test/workdir"
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
        self.job.is_group = Mock(return_value=False)
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

            transfer_input, *_ = self.executor._get_files_for_transfer(self.job)

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
            transfer_input, *_ = self.executor._get_files_for_transfer(self.job)

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

            transfer_input, *_ = self.executor._get_files_for_transfer(self.job)

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
        self.executor.workflow.workdir_init = "/test/workdir"
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
        self.job.is_group = Mock(return_value=False)
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

            transfer_input, *_ = self.executor._get_files_for_transfer(self.job)

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
        self.executor.workflow.workdir_init = "/test/workdir"
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
        self.job.is_group = Mock(return_value=False)
        self.job.input = []
        self.job.output = []
        self.job.resources = Mock()
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = None
        self.job.rules = [self.job.rule]

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
            self.job.rules = [self.job.rule]

            transfer_input, *_ = self.executor._get_files_for_transfer(self.job)

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
            self.job.rules = [self.job.rule]

            transfer_input, *_ = self.executor._get_files_for_transfer(self.job)

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
            self.job.rules = [self.job.rule]

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
        self.executor.workflow.workdir_init = "/test/workdir"
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
        self.job.is_group = Mock(return_value=False)
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

            transfer_input, *_ = self.executor._get_files_for_transfer(self.job)

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

            transfer_input, *_ = self.executor._get_files_for_transfer(self.job)

            assert file1 in transfer_input
            assert file2 in transfer_input
        finally:
            os.unlink(file1)
            os.unlink(file2)

    def test_htcondor_transfer_input_files_with_wildcards(self):
        """Test htcondor_transfer_input_files expands wildcards for individual jobs."""
        self.job.wildcards = {"module": "stats_helpers"}
        self.job.params = {}
        self.job.format_wildcards = Mock(return_value="scripts/stats_helpers.py")

        # Create the file that wildcards will expand to
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

            transfer_input, *_ = self.executor._get_files_for_transfer(self.job)

            # Wildcard expansion SHOULD be called for individual jobs
            self.job.format_wildcards.assert_called()
            # The expanded path should be in the transfer list
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

        _, transfer_output, transfer_remaps = self.executor._get_files_for_transfer(
            self.job
        )

        assert "results/model.pkl" in transfer_output
        assert "results/metrics.json" in transfer_output
        # A remap must exist for every transferred output
        assert len(transfer_remaps) == len(transfer_output)

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

        _, transfer_output, transfer_remaps = self.executor._get_files_for_transfer(
            self.job
        )

        assert "results/model.pkl" in transfer_output
        assert "results/metrics.json" in transfer_output
        # A remap must exist for every transferred output
        assert len(transfer_remaps) == len(transfer_output)


class TestFileTransferLogging:
    """Test that file transfer operations log appropriately."""

    def setup_method(self):
        """Setup mock executor and job for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()
        self.executor.shared_fs_prefixes = []
        self.executor.workflow = Mock()
        self.executor.workflow.configfiles = []
        self.executor.workflow.workdir_init = "/test/workdir"
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
        self.job.is_group = Mock(return_value=False)
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


class TestModuleSnakefileDetection:
    """Test the _add_module_snakefiles method for detecting module Snakefiles."""

    def setup_method(self):
        """Setup mock executor and temporary files for testing."""
        self.executor = Mock(spec=Executor)
        self.executor.logger = Mock()
        self.executor.shared_fs_prefixes = []

        # Bind the actual methods to our mock
        self.executor._add_module_snakefiles = Executor._add_module_snakefiles.__get__(
            self.executor, Executor
        )
        self.executor._add_file_if_transferable = (
            Executor._add_file_if_transferable.__get__(self.executor, Executor)
        )

        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_detects_single_module_with_double_quotes(self):
        """Test detection of module declaration with double quotes."""
        snakefile_content = """
rule all:
    input: "output.txt"

module quality_check:
    snakefile: "modules/qc/Snakefile"

use rule * from quality_check as qc_*
"""
        main_snakefile = os.path.join(self.temp_dir, "Snakefile")
        with open(main_snakefile, "w") as f:
            f.write(snakefile_content)

        # Create the module Snakefile so it exists
        module_dir = os.path.join(self.temp_dir, "modules", "qc")
        os.makedirs(module_dir, exist_ok=True)
        module_snakefile = os.path.join(module_dir, "Snakefile")
        with open(module_snakefile, "w") as f:
            f.write("rule test:\n    output: 'test.txt'\n")

        transfer_list = []
        self.executor._add_module_snakefiles(main_snakefile, transfer_list)

        assert len(transfer_list) == 1
        assert module_snakefile in transfer_list

    def test_detects_single_module_with_single_quotes(self):
        """Test detection of module declaration with single quotes."""
        snakefile_content = """
module quality_check:
    snakefile: 'modules/qc/Snakefile'
"""
        main_snakefile = os.path.join(self.temp_dir, "Snakefile")
        with open(main_snakefile, "w") as f:
            f.write(snakefile_content)

        module_dir = os.path.join(self.temp_dir, "modules", "qc")
        os.makedirs(module_dir, exist_ok=True)
        module_snakefile = os.path.join(module_dir, "Snakefile")
        with open(module_snakefile, "w") as f:
            f.write("rule test:\n    output: 'test.txt'\n")

        transfer_list = []
        self.executor._add_module_snakefiles(main_snakefile, transfer_list)

        assert len(transfer_list) == 1
        assert module_snakefile in transfer_list

    def test_detects_multiple_modules(self):
        """Test detection of multiple module declarations."""
        snakefile_content = """
module quality_check:
    snakefile: "modules/qc/Snakefile"

module preprocessing:
    snakefile: "modules/prep/Snakefile"

module analysis:
    snakefile: "modules/analysis/Snakefile"
"""
        main_snakefile = os.path.join(self.temp_dir, "Snakefile")
        with open(main_snakefile, "w") as f:
            f.write(snakefile_content)

        # Create all module Snakefiles
        for module_name in ["qc", "prep", "analysis"]:
            module_dir = os.path.join(self.temp_dir, "modules", module_name)
            os.makedirs(module_dir, exist_ok=True)
            module_snakefile = os.path.join(module_dir, "Snakefile")
            with open(module_snakefile, "w") as f:
                f.write("rule test:\n    output: 'test.txt'\n")

        transfer_list = []
        self.executor._add_module_snakefiles(main_snakefile, transfer_list)

        assert len(transfer_list) == 3

    def test_handles_no_modules(self):
        """Test that workflows without modules work correctly."""
        snakefile_content = """
rule all:
    input: "output.txt"

rule process:
    output: "output.txt"
    shell: "touch {output}"
"""
        main_snakefile = os.path.join(self.temp_dir, "Snakefile")
        with open(main_snakefile, "w") as f:
            f.write(snakefile_content)

        transfer_list = []
        self.executor._add_module_snakefiles(main_snakefile, transfer_list)

        assert len(transfer_list) == 0

    def test_handles_nonexistent_snakefile(self):
        """Test that nonexistent Snakefile doesn't cause errors."""
        transfer_list = []
        self.executor._add_module_snakefiles("/nonexistent/Snakefile", transfer_list)

        assert len(transfer_list) == 0
        # Should not raise an exception

    def test_resolves_relative_module_paths(self):
        """Test that module paths relative to main Snakefile are resolved."""
        # Create main Snakefile in subdirectory
        workflow_dir = os.path.join(self.temp_dir, "workflow")
        os.makedirs(workflow_dir, exist_ok=True)

        snakefile_content = """
module quality_check:
    snakefile: "rules/qc.smk"
"""
        main_snakefile = os.path.join(workflow_dir, "Snakefile")
        with open(main_snakefile, "w") as f:
            f.write(snakefile_content)

        # Create module Snakefile relative to workflow directory
        rules_dir = os.path.join(workflow_dir, "rules")
        os.makedirs(rules_dir, exist_ok=True)
        module_snakefile = os.path.join(rules_dir, "qc.smk")
        with open(module_snakefile, "w") as f:
            f.write("rule test:\n    output: 'test.txt'\n")

        transfer_list = []
        self.executor._add_module_snakefiles(main_snakefile, transfer_list)

        assert len(transfer_list) == 1
        assert module_snakefile in transfer_list

    def test_skips_modules_on_shared_filesystem(self):
        """Test that module Snakefiles on shared FS are not transferred."""
        self.executor.shared_fs_prefixes = ["/staging/"]

        snakefile_content = """
module quality_check:
    snakefile: "/staging/modules/qc/Snakefile"
"""
        main_snakefile = os.path.join(self.temp_dir, "Snakefile")
        with open(main_snakefile, "w") as f:
            f.write(snakefile_content)

        # Create the module Snakefile on "shared" filesystem
        # (we won't actually create it since it's on shared FS)

        transfer_list = []
        self.executor._add_module_snakefiles(main_snakefile, transfer_list)

        # Should be empty because module is on shared FS
        assert len(transfer_list) == 0

    def test_handles_malformed_module_declarations(self):
        """Test that malformed module declarations don't cause crashes."""
        snakefile_content = """
module quality_check:
    # Missing snakefile declaration

module preprocessing
    snakefile: "broken syntax

rule all:
    input: "test.txt"
"""
        main_snakefile = os.path.join(self.temp_dir, "Snakefile")
        with open(main_snakefile, "w") as f:
            f.write(snakefile_content)

        transfer_list = []
        # Should not raise an exception
        self.executor._add_module_snakefiles(main_snakefile, transfer_list)

        # Malformed declarations should be skipped
        assert len(transfer_list) == 0

    def test_logs_found_module_snakefiles(self):
        """Test that found module Snakefiles are logged."""
        snakefile_content = """
module quality_check:
    snakefile: "modules/qc/Snakefile"
"""
        main_snakefile = os.path.join(self.temp_dir, "Snakefile")
        with open(main_snakefile, "w") as f:
            f.write(snakefile_content)

        module_dir = os.path.join(self.temp_dir, "modules", "qc")
        os.makedirs(module_dir, exist_ok=True)
        module_snakefile = os.path.join(module_dir, "Snakefile")
        with open(module_snakefile, "w") as f:
            f.write("rule test:\n    output: 'test.txt'\n")

        transfer_list = []
        self.executor._add_module_snakefiles(main_snakefile, transfer_list)

        # Should log debug message about finding the module
        self.executor.logger.debug.assert_called()
        debug_calls = [str(call) for call in self.executor.logger.debug.call_args_list]
        assert any("Found module Snakefile" in call for call in debug_calls)

    def test_detects_nested_modules_recursively(self):
        """Test that modules within modules are detected recursively."""
        # Main Snakefile uses quality_check module
        main_snakefile_content = """
module quality_check:
    snakefile: "modules/qc/Snakefile"

use rule * from quality_check as qc_*
"""
        main_snakefile = os.path.join(self.temp_dir, "Snakefile")
        with open(main_snakefile, "w") as f:
            f.write(main_snakefile_content)

        # quality_check module uses validation module (nested)
        qc_module_content = """
module validation:
    snakefile: "validation/Snakefile"

use rule * from validation as val_*

rule quality_check:
    output: "qc.txt"
    shell: "touch {output}"
"""
        qc_dir = os.path.join(self.temp_dir, "modules", "qc")
        os.makedirs(qc_dir, exist_ok=True)
        qc_snakefile = os.path.join(qc_dir, "Snakefile")
        with open(qc_snakefile, "w") as f:
            f.write(qc_module_content)

        # validation module (nested within qc module)
        val_dir = os.path.join(qc_dir, "validation")
        os.makedirs(val_dir, exist_ok=True)
        val_snakefile = os.path.join(val_dir, "Snakefile")
        with open(val_snakefile, "w") as f:
            f.write(
                "rule validate:\n    output: 'validated.txt'\n    shell: 'touch {output}'"
            )

        transfer_list = []
        self.executor._add_module_snakefiles(main_snakefile, transfer_list)

        # Should find both the QC module and the nested validation module
        assert len(transfer_list) == 2
        assert qc_snakefile in transfer_list
        assert val_snakefile in transfer_list

    def test_handles_circular_module_references(self):
        """Test that circular module references don't cause infinite loops."""
        # Main Snakefile references module A
        main_snakefile = os.path.join(self.temp_dir, "Snakefile")
        with open(main_snakefile, "w") as f:
            f.write('module a:\n    snakefile: "modules/a/Snakefile"\n')

        # Module A references module B
        a_dir = os.path.join(self.temp_dir, "modules", "a")
        os.makedirs(a_dir, exist_ok=True)
        a_snakefile = os.path.join(a_dir, "Snakefile")
        with open(a_snakefile, "w") as f:
            f.write('module b:\n    snakefile: "../b/Snakefile"\n')

        # Module B references module A (circular!)
        b_dir = os.path.join(self.temp_dir, "modules", "b")
        os.makedirs(b_dir, exist_ok=True)
        b_snakefile = os.path.join(b_dir, "Snakefile")
        with open(b_snakefile, "w") as f:
            f.write('module a:\n    snakefile: "../a/Snakefile"\n')

        transfer_list = []
        # Should not cause infinite loop
        self.executor._add_module_snakefiles(main_snakefile, transfer_list)

        # Should only add each module once
        assert len(transfer_list) == 2
        assert a_snakefile in transfer_list
        assert b_snakefile in transfer_list

    def test_deeply_nested_modules(self):
        """Test detection of modules nested multiple levels deep."""
        # Main -> level1 -> level2 -> level3
        main_snakefile = os.path.join(self.temp_dir, "Snakefile")
        with open(main_snakefile, "w") as f:
            f.write('module level1:\n    snakefile: "level1/Snakefile"\n')

        level1_dir = os.path.join(self.temp_dir, "level1")
        os.makedirs(level1_dir, exist_ok=True)
        level1_snakefile = os.path.join(level1_dir, "Snakefile")
        with open(level1_snakefile, "w") as f:
            f.write('module level2:\n    snakefile: "level2/Snakefile"\n')

        level2_dir = os.path.join(level1_dir, "level2")
        os.makedirs(level2_dir, exist_ok=True)
        level2_snakefile = os.path.join(level2_dir, "Snakefile")
        with open(level2_snakefile, "w") as f:
            f.write('module level3:\n    snakefile: "level3/Snakefile"\n')

        level3_dir = os.path.join(level2_dir, "level3")
        os.makedirs(level3_dir, exist_ok=True)
        level3_snakefile = os.path.join(level3_dir, "Snakefile")
        with open(level3_snakefile, "w") as f:
            f.write('rule test:\n    output: "test.txt"\n')

        transfer_list = []
        self.executor._add_module_snakefiles(main_snakefile, transfer_list)

        # Should find all 3 nested modules
        assert len(transfer_list) == 3
        assert level1_snakefile in transfer_list
        assert level2_snakefile in transfer_list
        assert level3_snakefile in transfer_list


class TestAbsolutePathWarning:
    """Test warning when absolute paths are used with preserve_relative_paths is True"""

    def setup_method(self):
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
        self.job.is_group = Mock(return_value=False)
        self.job.input = []
        self.job.output = []
        self.job.resources = Mock()
        self.job.resources.get = Mock(return_value=None)
        self.job.rule = Mock()
        self.job.rule.script = None
        self.job.rule.notebook = None

    def test_warns_on_absolute_paths(self):
        # Create a temporary file with an absolute path
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            abs_path = tmp.name  # This is an absolute path like /tmp/xxx

        try:
            # Set preserve_relative_paths = True using lambda function
            self.job.resources.get = Mock(
                side_effect=lambda key, default=None: (
                    True if key == "preserve_relative_paths" else default
                )
            )
            # Add the absolute path to transfer
            self.job.input = [abs_path]

            # Call _get_files_for_transfer
            self.executor._get_files_for_transfer(self.job)

            # Check for warning about absolute path / flattening
            self.executor.logger.warning.assert_called()
            warning_msg = self.executor.logger.warning.call_args[0][0]
            assert "absolute" in warning_msg.lower() or "flatten" in warning_msg.lower()
        finally:
            os.unlink(abs_path)  # delete
