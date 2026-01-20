"""
Shared test fixtures and utilities for HTCondor executor tests.
"""

from unittest.mock import Mock
from snakemake_executor_plugin_htcondor import Executor


def create_mock_executor(shared_fs_prefixes=None):
    """
    Create a mock executor with common setup.

    Args:
        shared_fs_prefixes: List of shared filesystem prefixes. Defaults to empty list.

    Returns:
        Mock executor with bound methods.
    """
    executor = Mock(spec=Executor)
    executor.logger = Mock()
    executor.shared_fs_prefixes = shared_fs_prefixes or []
    executor.workflow = Mock()
    executor.workflow.configfiles = []
    executor.get_snakefile = Mock(return_value="Snakefile")

    # Bind the actual methods to the mock
    executor._get_files_for_transfer = Executor._get_files_for_transfer.__get__(
        executor, Executor
    )
    executor._add_file_if_transferable = Executor._add_file_if_transferable.__get__(
        executor, Executor
    )
    executor._parse_file_list = Executor._parse_file_list.__get__(executor, Executor)

    return executor


def create_mock_individual_job(
    script=None,
    notebook=None,
    input_files=None,
    output_files=None,
    htcondor_transfer_input_files=None,
    htcondor_transfer_output_files=None,
    job_wrapper=None,
    wildcards=None,
    params=None,
):
    """
    Create a mock individual (non-grouped) job with common setup.

    Args:
        script: Path to script file or None
        notebook: Path to notebook file or None
        input_files: List of input files
        output_files: List of output files
        htcondor_transfer_input_files: Additional input files from resource
        htcondor_transfer_output_files: Additional output files from resource
        job_wrapper: Path to job wrapper script or None
        wildcards: Dict of wildcards for the job
        params: Dict of params for the job

    Returns:
        Mock job configured as individual job.
    """
    job = Mock()
    job.is_group = Mock(return_value=False)
    job.name = "test_individual_job"
    job.input = input_files or []
    job.output = output_files or []
    job.wildcards = wildcards or {}
    job.params = params or {}

    # Setup rule with script/notebook
    job.rule = Mock()
    job.rule.script = script
    job.rule.notebook = notebook

    # Setup resources
    def resource_get(key, default=None):
        resource_map = {
            "htcondor_transfer_input_files": htcondor_transfer_input_files,
            "htcondor_transfer_output_files": htcondor_transfer_output_files,
            "job_wrapper": job_wrapper,
            "preserve_relative_paths": True,
        }
        return resource_map.get(key, default)

    job.resources = Mock()
    job.resources.get = Mock(side_effect=resource_get)

    # Setup wildcard formatting
    def format_wildcards_func(string, **variables):
        result = string
        # Merge provided wildcards/params with any defaults
        all_vars = {}
        if "wildcards" in variables:
            all_vars.update(variables["wildcards"])
        if "params" in variables:
            all_vars.update(variables["params"])

        for key, value in all_vars.items():
            result = result.replace(f"{{wildcards.{key}}}", str(value))
            result = result.replace(f"{{params.{key}}}", str(value))
            result = result.replace(f"{{{key}}}", str(value))
        return result

    job.format_wildcards = Mock(side_effect=format_wildcards_func)

    return job


def create_mock_group_job(
    individual_jobs,
    htcondor_transfer_input_files=None,
    htcondor_transfer_output_files=None,
):
    """
    Create a mock grouped job containing multiple individual jobs.

    Args:
        individual_jobs: List of mock individual jobs to include in the group
        htcondor_transfer_input_files: Additional input files from resource
        htcondor_transfer_output_files: Additional output files from resource

    Returns:
        Mock job configured as a group job.
    """
    # Use spec_set to limit what attributes the mock has
    job = Mock(spec=["is_group", "jobs", "input", "output", "resources", "name"])
    job.is_group = Mock(return_value=True)
    job.jobs = individual_jobs
    job.name = "test_group_job"

    # Combine input/output from all jobs
    job.input = []
    job.output = []
    for individual_job in individual_jobs:
        job.input.extend(individual_job.input)
        job.output.extend(individual_job.output)

    # Setup resources
    def resource_get(key, default=None):
        resource_map = {
            "htcondor_transfer_input_files": htcondor_transfer_input_files,
            "htcondor_transfer_output_files": htcondor_transfer_output_files,
            "preserve_relative_paths": True,
        }
        return resource_map.get(key, default)

    job.resources = Mock()
    job.resources.get = Mock(side_effect=resource_get)

    return job
