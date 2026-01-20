"""
Unit tests for grouped job file transfer functionality in HTCondor executor.
"""

import tempfile
import os
from conftest import (
    create_mock_executor,
    create_mock_individual_job,
    create_mock_group_job,
)


class TestGroupedJobScriptTransfer:
    """Test that script files from grouped jobs are transferred correctly."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = create_mock_executor()

    def test_single_job_with_script_in_group(self):
        """Test that a script from a single job in a group is transferred."""
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as tmp:
            script_path = tmp.name

        try:
            # Create individual job with script
            individual_job = create_mock_individual_job(script=script_path)

            # Create group containing this job
            group_job = create_mock_group_job([individual_job])

            transfer_input, _ = self.executor._get_files_for_transfer(group_job)

            assert script_path in transfer_input
        finally:
            os.unlink(script_path)

    def test_multiple_jobs_with_scripts_in_group(self):
        """Test that scripts from multiple jobs in a group are all transferred."""
        script_files = []
        try:
            # Create 3 jobs each with their own script
            for i in range(3):
                tmp = tempfile.NamedTemporaryFile(suffix=f"_job{i}.py", delete=False)
                script_files.append(tmp.name)
                tmp.close()

            individual_jobs = [
                create_mock_individual_job(script=script_files[0]),
                create_mock_individual_job(script=script_files[1]),
                create_mock_individual_job(script=script_files[2]),
            ]

            group_job = create_mock_group_job(individual_jobs)

            transfer_input, _ = self.executor._get_files_for_transfer(group_job)

            # All three scripts should be in the transfer list
            for script in script_files:
                assert script in transfer_input
        finally:
            for script in script_files:
                if os.path.exists(script):
                    os.unlink(script)

    def test_mixed_jobs_with_and_without_scripts(self):
        """Test group with some jobs having scripts and some not."""
        with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as tmp:
            script_path = tmp.name

        try:
            individual_jobs = [
                create_mock_individual_job(script=script_path),  # Has script
                create_mock_individual_job(script=None),  # No script
                create_mock_individual_job(script=None),  # No script
            ]

            group_job = create_mock_group_job(individual_jobs)

            transfer_input, _ = self.executor._get_files_for_transfer(group_job)

            # Only the one script should be transferred
            assert script_path in transfer_input
            assert len([f for f in transfer_input if f.endswith(".py")]) == 1
        finally:
            os.unlink(script_path)

    def test_grouped_job_with_wildcard_script_paths(self):
        """Test that wildcard expansion works for scripts in grouped jobs."""
        # Create actual script files with wildcards in names
        script_files = []
        try:
            for sample in ["sample1", "sample2"]:
                tmp = tempfile.NamedTemporaryFile(
                    suffix=f"_process_{sample}.py", delete=False
                )
                script_files.append(tmp.name)
                tmp.close()

            individual_jobs = [
                create_mock_individual_job(
                    script="scripts/process_{wildcards.sample}.py",
                    wildcards={"sample": "sample1"},
                ),
                create_mock_individual_job(
                    script="scripts/process_{wildcards.sample}.py",
                    wildcards={"sample": "sample2"},
                ),
            ]

            # Mock the format_wildcards for each job to expand properly
            individual_jobs[0].format_wildcards = (
                lambda s, **kw: "scripts/process_sample1.py"
            )
            individual_jobs[1].format_wildcards = (
                lambda s, **kw: "scripts/process_sample2.py"
            )

            group_job = create_mock_group_job(individual_jobs)

            transfer_input, _ = self.executor._get_files_for_transfer(group_job)

            # Both expanded script paths should be in transfer list
            assert "scripts/process_sample1.py" in transfer_input
            assert "scripts/process_sample2.py" in transfer_input
        finally:
            for script in script_files:
                if os.path.exists(script):
                    os.unlink(script)


class TestGroupedJobNotebookTransfer:
    """Test that notebook files from grouped jobs are transferred correctly."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = create_mock_executor()

    def test_single_job_with_notebook_in_group(self):
        """Test that a notebook from a single job in a group is transferred."""
        with tempfile.NamedTemporaryFile(suffix=".ipynb", delete=False) as tmp:
            notebook_path = tmp.name

        try:
            individual_job = create_mock_individual_job(notebook=notebook_path)
            group_job = create_mock_group_job([individual_job])

            transfer_input, _ = self.executor._get_files_for_transfer(group_job)

            assert notebook_path in transfer_input
        finally:
            os.unlink(notebook_path)

    def test_multiple_jobs_with_notebooks_in_group(self):
        """Test that notebooks from multiple jobs in a group are all transferred."""
        notebook_files = []
        try:
            for i in range(2):
                tmp = tempfile.NamedTemporaryFile(
                    suffix=f"_analysis{i}.ipynb", delete=False
                )
                notebook_files.append(tmp.name)
                tmp.close()

            individual_jobs = [
                create_mock_individual_job(notebook=notebook_files[0]),
                create_mock_individual_job(notebook=notebook_files[1]),
            ]

            group_job = create_mock_group_job(individual_jobs)

            transfer_input, _ = self.executor._get_files_for_transfer(group_job)

            for notebook in notebook_files:
                assert notebook in transfer_input
        finally:
            for notebook in notebook_files:
                if os.path.exists(notebook):
                    os.unlink(notebook)

    def test_grouped_job_with_both_scripts_and_notebooks(self):
        """Test group with jobs having both scripts and notebooks."""
        script_path = None
        notebook_path = None

        try:
            with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as tmp:
                script_path = tmp.name
            with tempfile.NamedTemporaryFile(suffix=".ipynb", delete=False) as tmp:
                notebook_path = tmp.name

            individual_jobs = [
                create_mock_individual_job(script=script_path),
                create_mock_individual_job(notebook=notebook_path),
            ]

            group_job = create_mock_group_job(individual_jobs)

            transfer_input, _ = self.executor._get_files_for_transfer(group_job)

            # Both script and notebook should be transferred
            assert script_path in transfer_input
            assert notebook_path in transfer_input
        finally:
            if script_path and os.path.exists(script_path):
                os.unlink(script_path)
            if notebook_path and os.path.exists(notebook_path):
                os.unlink(notebook_path)


class TestGroupedJobInputOutputTransfer:
    """Test that input/output files from grouped jobs are transferred correctly."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = create_mock_executor()

    def test_combined_inputs_from_grouped_jobs(self):
        """Test that inputs from all jobs in a group are collected."""
        input1 = tempfile.NamedTemporaryFile(delete=False)
        input2 = tempfile.NamedTemporaryFile(delete=False)

        try:
            individual_jobs = [
                create_mock_individual_job(input_files=[input1.name]),
                create_mock_individual_job(input_files=[input2.name]),
            ]

            group_job = create_mock_group_job(individual_jobs)

            transfer_input, _ = self.executor._get_files_for_transfer(group_job)

            assert input1.name in transfer_input
            assert input2.name in transfer_input
        finally:
            input1.close()
            input2.close()
            os.unlink(input1.name)
            os.unlink(input2.name)

    def test_combined_outputs_from_grouped_jobs(self):
        """Test that output directories from all jobs in a group are collected."""
        individual_jobs = [
            create_mock_individual_job(output_files=["output/job1_result.txt"]),
            create_mock_individual_job(output_files=["results/job2_data.csv"]),
        ]

        group_job = create_mock_group_job(individual_jobs)

        _, transfer_output = self.executor._get_files_for_transfer(group_job)

        # Top-level output directories should be transferred
        assert "output" in transfer_output
        assert "results" in transfer_output


class TestGroupedJobCustomTransferResources:
    """Test that custom transfer resources work with grouped jobs."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = create_mock_executor()

    def test_htcondor_transfer_input_files_on_group(self):
        """Test that htcondor_transfer_input_files resource works on group jobs."""
        helper_file = tempfile.NamedTemporaryFile(suffix=".py", delete=False)

        try:
            individual_jobs = [
                create_mock_individual_job(),
                create_mock_individual_job(),
            ]

            # Add custom transfer files at the group level
            group_job = create_mock_group_job(
                individual_jobs, htcondor_transfer_input_files=helper_file.name
            )

            transfer_input, _ = self.executor._get_files_for_transfer(group_job)

            assert helper_file.name in transfer_input
        finally:
            helper_file.close()
            os.unlink(helper_file.name)

    def test_htcondor_transfer_output_files_on_group(self):
        """Test that htcondor_transfer_output_files resource works on group jobs."""
        individual_jobs = [
            create_mock_individual_job(),
            create_mock_individual_job(),
        ]

        group_job = create_mock_group_job(
            individual_jobs, htcondor_transfer_output_files="logs/,debug/"
        )

        _, transfer_output = self.executor._get_files_for_transfer(group_job)

        # Note: _parse_file_list strips trailing slashes
        assert "logs" in transfer_output
        assert "debug" in transfer_output


class TestGroupedJobComplexScenarios:
    """Test complex scenarios combining multiple features for grouped jobs."""

    def setup_method(self):
        """Setup mock executor for testing."""
        self.executor = create_mock_executor()

    def test_full_pipeline_grouped_job(self):
        """Test a realistic grouped job with scripts, inputs, outputs, and custom transfers."""
        script1 = tempfile.NamedTemporaryFile(suffix="_step1.py", delete=False)
        script2 = tempfile.NamedTemporaryFile(suffix="_step2.py", delete=False)
        input_file = tempfile.NamedTemporaryFile(suffix="_input.txt", delete=False)
        helper = tempfile.NamedTemporaryFile(suffix="_helpers.py", delete=False)

        try:
            individual_jobs = [
                create_mock_individual_job(
                    script=script1.name,
                    input_files=[input_file.name],
                    output_files=["intermediate/step1_result.txt"],
                ),
                create_mock_individual_job(
                    script=script2.name,
                    input_files=["intermediate/step1_result.txt"],
                    output_files=["output/final_result.txt"],
                ),
            ]

            group_job = create_mock_group_job(
                individual_jobs, htcondor_transfer_input_files=helper.name
            )

            transfer_input, transfer_output = self.executor._get_files_for_transfer(
                group_job
            )

            # Verify all expected files are in transfer lists
            assert script1.name in transfer_input
            assert script2.name in transfer_input
            assert input_file.name in transfer_input
            assert helper.name in transfer_input
            assert "intermediate" in transfer_output
            assert "output" in transfer_output
        finally:
            for f in [script1, script2, input_file, helper]:
                f.close()
                os.unlink(f.name)

    def test_empty_group_job(self):
        """Test that an empty group job doesn't cause errors."""
        group_job = create_mock_group_job([])

        transfer_input, transfer_output = self.executor._get_files_for_transfer(
            group_job
        )

        # Should complete without errors, returning empty or minimal transfers
        assert isinstance(transfer_input, list)
        assert isinstance(transfer_output, list)
