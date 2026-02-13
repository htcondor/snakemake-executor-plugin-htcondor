from dataclasses import dataclass, field
from enum import Enum
from typing import List, AsyncGenerator, Optional, Dict
import time
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins.executors.remote import RemoteExecutor
from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError  # noqa

import htcondor2 as htcondor
from htcondor2 import JobEventLog, JobEventType
import traceback
from os.path import join, isabs, relpath, normpath, exists
from os import makedirs, sep
import re
import sys


class JobStatus(Enum):
    """Enumeration of possible job statuses.

    This enum provides type-safe status values and human-readable display names.
    """

    PENDING = "pending"
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    HELD = "held"
    REMOVED = "removed"
    TRANSFERRING = "transferring"
    SUSPENDED = "suspended"

    @property
    def display_name(self) -> str:
        """Return a human-readable display name for the status."""
        return self.value.capitalize()

    def is_terminal(self) -> bool:
        """Return True if this is a terminal state (job won't change further)."""
        return self in (JobStatus.COMPLETED, JobStatus.REMOVED)


@dataclass
class JobState:
    """Represents the current state of an HTCondor job.

    This dataclass provides type-safe access to job state fields,
    replacing raw dictionaries to prevent silent bugs from key typos.
    """

    status: JobStatus = JobStatus.PENDING
    exit_code: Optional[int] = None
    exit_by_signal: bool = False
    exit_signal: Optional[int] = None
    hold_reason: Optional[str] = None
    held_since: Optional[float] = None


def is_shared_fs(in_path, shared_prefixes) -> bool:
    """
    Check if the job is on a shared filesystem, as configured using the
    `shared_fs_prefixes` config for the executor
    """
    normalized_prefixes = [join(prefix, "") for prefix in shared_prefixes]
    normalized_path = join(in_path, "")

    return any(normalized_path.startswith(prefix) for prefix in normalized_prefixes)


# Optional:
# Default timeout in seconds for held jobs (4 hours).
DEFAULT_HELD_TIMEOUT_SECONDS = 4 * 60 * 60


# Define additional settings for your executor.
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
# Make sure that all defined fields are Optional and specify a default value
# of None or anything else that makes sense in your case.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    jobdir: Optional[str] = field(
        default=".snakemake/htcondor",
        metadata={
            "help": "Directory where the job will create a directory to store log, "
            "output and error files.",
            "required": True,
        },
    )
    shared_fs_prefixes: Optional[str] = field(
        default=None,
        metadata={
            "help": "Comma-separated path prefixes shared between AP and EPs "
            "(e.g., '/staging,/shared'). Files under these paths are accessed directly "
            "without HTCondor transfer. Use with --shared-fs-usage none when only specific "
            "paths (not all I/O) are shared.",
            "required": False,
        },
    )
    held_timeout: Optional[int] = field(
        default=DEFAULT_HELD_TIMEOUT_SECONDS,
        metadata={
            "help": "Timeout in seconds for held jobs before the Executor will treat "
            "them as failed. This allows time for manual intervention "
            "(e.g., condor_release) before the workflow fails. "
            "Default is 4 hours (14400 seconds). Set to 0 to "
            "fail immediately when a job is held.",
            "required": False,
        },
    )


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Whether the executor implies to not have a shared file system
    implies_no_shared_fs=False,
    # whether to deploy workflow sources to default storage provider before execution
    job_deploy_sources=True,
    # whether arguments for setting the storage provider shall be passed to jobs
    pass_default_storage_provider_args=True,
    # whether arguments for setting default resources shall be passed to jobs
    pass_default_resources_args=True,
    # whether environment variables shall be passed to jobs (if False, use
    # self.envvars() to obtain a dict of environment variables and their values
    # and pass them e.g. as secrets to the execution backend)
    pass_envvar_declarations_to_cmd=True,
    # whether the default storage provider shall be deployed before the job is run on
    # the remote node. Usually set to True if the executor does not assume a shared fs
    auto_deploy_default_storage_provider=True,
    # specify initial amount of seconds to sleep before checking for job status
    init_seconds_before_status_checks=0,
    # indicate that the HTCondor executor can transfer its own files to the remote node
    can_transfer_local_files=True,
)


# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        # access workflow
        self.workflow
        # access executor specific settings
        self.workflow.executor_settings

        # jobDir: Directory where the job will store log, output and error files.
        self.jobDir = self.workflow.executor_settings.jobdir

        # Create the job directory immediately so it exists even if no jobs are submitted
        # This ensures logs/errors can be checked in CI/CD regardless of workflow success
        makedirs(self.jobDir, exist_ok=True)

        # Parse shared filesystem prefixes
        self.shared_fs_prefixes = self._parse_shared_fs_prefixes(
            self.workflow.executor_settings.shared_fs_prefixes
        )

        # Validate configuration and provide helpful guidance
        if self.shared_fs_prefixes:
            self._validate_shared_fs_configuration()

        # Dictionary to track JobEventLog readers for each submitted job.
        # Key: external_jobid (ClusterId), Value: JobEventLog reader
        # This approach avoids expensive schedd queries by reading local log files,
        # similar to how DAGMan and condor_watch_q operate.
        self._job_event_logs: Dict[int, JobEventLog] = {}

        # Dictionary to track the latest known state for each job.
        # Key: external_jobid (ClusterId), Value: JobState dataclass
        # This is essential because JobEventLog readers maintain their position -
        # once all events are read, subsequent reads return nothing. We track
        # the last known state so we can return it when there are no new events.
        self._job_current_states: Dict[int, JobState] = {}

        # Held job timeout - allows time for manual intervention before failing.
        # Normalize None to the default value so downstream comparisons don't
        # raise TypeError (held_timeout is Optional[int]).
        held_timeout = self.workflow.executor_settings.held_timeout
        if held_timeout is None:
            held_timeout = DEFAULT_HELD_TIMEOUT_SECONDS
        self._held_timeout = held_timeout
        self._validate_held_timeout()

        # Track when the workflow started for efficient condor_history queries.
        # We use this with the -since option to avoid scanning the entire history.
        self._workflow_start_time = int(time.time())

        # Track jobs that have missing log files and need schedd/history fallback.
        # Key: cluster_id, Value: number of consecutive log-missing checks
        self._log_missing_counts: Dict[int, int] = {}

        # Number of consecutive missing log checks before using fallback
        self._log_missing_threshold = 3

    def _validate_held_timeout(self):
        """Validate the held job timeout configuration.

        Raises:
            WorkflowError: If the held_timeout value is negative.
        """
        if self._held_timeout < 0:
            raise WorkflowError(
                f"Invalid held_timeout value {self._held_timeout}: must be >= 0. "
                "Use 0 to fail immediately on held jobs, or a positive value "
                "for the number of seconds to wait before failing."
            )

        if self._held_timeout > 0:
            self.logger.debug(
                f"Held job timeout set to {self._held_timeout} seconds "
                f"({self._held_timeout / 3600:.1f} hours)"
            )

    def _validate_shared_fs_configuration(self):
        """Validate and warn about potentially confusing shared FS configuration."""
        if not self.shared_fs_prefixes:
            return  # No shared FS configured, nothing to validate

        storage_settings = self.workflow.storage_settings.shared_fs_usage

        # Check if user specified "none" but also configured shared prefixes
        if not storage_settings:  # empty list or None means "none"
            self.logger.info(
                f"Detected partially shared filesystem: {', '.join(self.shared_fs_prefixes)}. "
                "Files under these paths will be accessed directly at the Execution Point without HTCondor transfer."
            )

    def _parse_shared_fs_prefixes(self, prefixes_str: Optional[str]) -> List[str]:
        """
        Parse the comma-separated shared filesystem prefixes string.

        Args:
            prefixes_str: Comma-separated string of filesystem prefixes

        Returns:
            List of normalized filesystem prefix paths
        """
        if not prefixes_str:
            return []

        # Split by comma and strip whitespace/quotes
        prefixes = [
            item.strip().strip("'\"")
            for item in prefixes_str.split(",")
            if item.strip()
        ]

        # Normalize paths to ensure they end with a separator for proper prefix matching
        normalized = [join(prefix, "") for prefix in prefixes]

        self.logger.debug(f"Parsed shared filesystem prefixes: {normalized}")
        return normalized

    def _add_file_if_transferable(
        self,
        file_path: str,
        transfer_list: List[str],
        expand_wildcards: bool = False,
        job: Optional[JobExecutorInterface] = None,
        validate_exists: bool = True,
    ) -> None:
        """
        Add a file to the transfer list if it's not on a shared filesystem.

        Args:
            file_path: Path to potentially transfer
            transfer_list: List to add the file to
            expand_wildcards: Whether to expand wildcards/params in the path
            job: Job object (required if expand_wildcards is True)
            validate_exists: Whether to validate file existence (default True)
        """
        # Explicitly handle empty/None filepaths
        if file_path is None or str(file_path).strip() == "":
            self.logger.debug("Skipping empty or None filepath")
            return

        file_path = str(file_path).strip()

        if expand_wildcards and job:
            # GroupJob objects don't have wildcards/params attributes
            # Only expand if the job has these attributes (individual jobs)
            if hasattr(job, "wildcards") and hasattr(job, "params"):
                self.logger.debug(f"Expanding wildcards in: {file_path}")
                file_path = job.format_wildcards(
                    file_path, wildcards=job.wildcards, params=job.params
                )
                self.logger.debug(f"After wildcard expansion: {file_path}")
            else:
                self.logger.debug(
                    f"Job does not have wildcards/params attributes, skipping expansion for: {file_path}"
                )

        # Normalize path to handle './', '../', '//' etc.
        file_path = normpath(file_path)

        # Check if already in transfer list (after normalization)
        if file_path in transfer_list:
            self.logger.debug(f"File already in transfer list: {file_path}")
            return

        # Check if on shared filesystem
        if is_shared_fs(file_path, self.shared_fs_prefixes):
            self.logger.debug(
                f"File on shared filesystem, skipping transfer: {file_path}"
            )
            return

        # Validate file existence if requested
        if validate_exists and not exists(file_path):
            self.logger.warning(
                f"File marked for transfer does not exist: {file_path}. "
                "This will likely cause job failure."
            )

        self.logger.debug(f"Adding file to transfer list: {file_path}")
        transfer_list.append(file_path)

    def _add_module_snakefiles(
        self,
        snakefile_path: str,
        transfer_list: List[str],
        visited: Optional[set] = None,
    ):
        """
        Recursively parse Snakefile to find module declarations and add their Snakefiles.
        We're using this approach because it wasn't obvious how to access module Snakefiles
        via the `job` object.

        Module declarations in Snakemake look like:
            module name:
                snakefile: "path/to/Snakefile"

        This function works recursively - if a module Snakefile itself contains module
        declarations, those will also be detected and transferred.

        Args:
            snakefile_path: Path to the Snakefile to parse
            transfer_list: List to append module Snakefiles to
            visited: Set of already-visited Snakefiles to prevent infinite loops
        """
        if not exists(snakefile_path):
            return

        # Initialize visited set on first call to prevent infinite recursion
        if visited is None:
            visited = set()

        # Normalize the path for comparison
        normalized_path = normpath(snakefile_path)
        if normalized_path in visited:
            return
        visited.add(normalized_path)

        try:
            with open(snakefile_path, "r") as f:
                content = f.read()

            # Pattern to match: module name:\n    snakefile: "path" or snakefile: 'path'
            # Handles both single and double quotes
            pattern = r'module\s+\w+:\s*\n\s+snakefile:\s*["\']([^"\']+)["\']'
            matches = re.findall(pattern, content, re.MULTILINE)

            for module_snakefile in matches:
                # Module Snakefile paths are relative to the current Snakefile directory
                if not isabs(module_snakefile):
                    snakefile_dir = normpath(join(snakefile_path, ".."))
                    module_snakefile = join(snakefile_dir, module_snakefile)

                self.logger.debug(f"Found module Snakefile: {module_snakefile}")
                self._add_file_if_transferable(module_snakefile, transfer_list)

                # Recursively check if this module Snakefile has its own modules
                self._add_module_snakefiles(module_snakefile, transfer_list, visited)
        except Exception as e:
            self.logger.warning(f"Error parsing Snakefile for modules: {e}")

    def _parse_file_list(self, files_spec) -> List[str]:
        """
        Parse a file specification that can be either a string or list.

        Args:
            files_spec: String (comma-separated) or list of file paths

        Returns:
            List of file paths
        """
        if isinstance(files_spec, str):
            return [f.strip() for f in files_spec.split(",") if f.strip()]
        else:
            return list(files_spec)

    def _format_size_mb(self, mb_value: int) -> str:
        """
        Format a size value in MB to a human-readable string for HTCondor.

        Args:
            mb_value: Size value in megabytes (must be non-negative)

        Returns:
            Formatted string like "1024MB" or "4GB"
        """
        if mb_value < 0:
            raise WorkflowError(
                f"Invalid resource value {mb_value}MB: value must be non-negative."
            )
        if mb_value >= 1024 and mb_value % 1024 == 0:
            return f"{mb_value // 1024}GB"
        return f"{mb_value}MB"

    def _handle_explicit_unit_resources(
        self, job: JobExecutorInterface, submit_dict: dict
    ) -> None:
        """
        Handle HTCondor-specific resource parameters with explicit MB units.

        These parameters are designed for use with grouped jobs where Snakemake
        aggregates numeric resources by summing values for jobs that run in parallel
        within each execution layer and then taking the maximum total across all
        sequential layers. They take precedence over the standard HTCondor parameters
        (request_memory, request_disk, gpus_minimum_memory) when both are specified.

        Args:
            job: The job being prepared for submission
            submit_dict: The submit description dictionary to update
        """
        # htcondor_request_mem_mb -> request_memory (in MB, which is HTCondor's default)
        if htcondor_mem_mb := job.resources.get("htcondor_request_mem_mb"):
            if job.resources.get("request_memory"):
                self.logger.warning(
                    f"Both 'htcondor_request_mem_mb' ({htcondor_mem_mb}) and "
                    f"'request_memory' ({job.resources.get('request_memory')}) are set. "
                    f"Using htcondor_request_mem_mb value."
                )
            formatted_mem = self._format_size_mb(int(htcondor_mem_mb))
            submit_dict["request_memory"] = formatted_mem

        # htcondor_request_disk_mb -> request_disk (convert MB to KB for HTCondor)
        if htcondor_disk_mb := job.resources.get("htcondor_request_disk_mb"):
            if job.resources.get("request_disk"):
                self.logger.warning(
                    f"Both 'htcondor_request_disk_mb' ({htcondor_disk_mb}) and "
                    f"'request_disk' ({job.resources.get('request_disk')}) are set. "
                    f"Using htcondor_request_disk_mb value."
                )
            # HTCondor's request_disk expects KB, so convert from MB
            disk_kb = int(htcondor_disk_mb) * 1024
            submit_dict["request_disk"] = disk_kb

        # htcondor_gpus_min_mem_mb -> gpus_minimum_memory (in MB, HTCondor's default)
        if htcondor_gpu_mem_mb := job.resources.get("htcondor_gpus_min_mem_mb"):
            if job.resources.get("gpus_minimum_memory"):
                self.logger.warning(
                    f"Both 'htcondor_gpus_min_mem_mb' ({htcondor_gpu_mem_mb}) and "
                    f"'gpus_minimum_memory' ({job.resources.get('gpus_minimum_memory')}) "
                    f"are set. Using htcondor_gpus_min_mem_mb value."
                )
            submit_dict["gpus_minimum_memory"] = int(htcondor_gpu_mem_mb)

    def _log_resource_requests(self, submit_dict: dict) -> None:
        """
        Log the final resource requests with human-readable units.

        This is called after all resources have been set in submit_dict,
        so it logs the actual values that will be submitted to HTCondor
        regardless of whether they came from htcondor_* or standard parameters.

        Args:
            submit_dict: The submit description dictionary with final values
        """
        # Log memory request (HTCondor accepts values in MB by default)
        if request_memory := submit_dict.get("request_memory"):
            # Value might already be formatted (e.g., "4GB") or numeric
            if isinstance(request_memory, (int, float)):
                formatted_mem = self._format_size_mb(int(request_memory))
                self.logger.info(f"Requesting memory: {formatted_mem}")
            else:
                self.logger.info(f"Requesting memory: {request_memory}")

        # Log disk request (HTCondor expects KB, we convert to human-readable)
        if request_disk := submit_dict.get("request_disk"):
            if isinstance(request_disk, (int, float)):
                # Value is in KB, convert to MB for formatting
                disk_mb = int(request_disk) // 1024
                formatted_disk = self._format_size_mb(disk_mb)
                self.logger.info(
                    f"Requesting disk: {formatted_disk} ({int(request_disk)} KB)"
                )
            else:
                self.logger.info(f"Requesting disk: {request_disk}")

        # Log GPU memory request (HTCondor accepts values in MB)
        if gpus_minimum_memory := submit_dict.get("gpus_minimum_memory"):
            if isinstance(gpus_minimum_memory, (int, float)):
                formatted_gpu_mem = self._format_size_mb(int(gpus_minimum_memory))
                self.logger.info(f"Requesting GPU minimum memory: {formatted_gpu_mem}")
            else:
                self.logger.info(
                    f"Requesting GPU minimum memory: {gpus_minimum_memory}"
                )

    def _get_files_for_transfer(
        self, job: JobExecutorInterface
    ) -> tuple[List[str], List[str]]:
        """
        Determine which input and output files need to be transferred by HTCondor.

        Files on shared filesystem prefixes are excluded from transfer.

        Args:
            job: The job being prepared for submission

        Returns:
            Tuple of (transfer_input_files, transfer_output_files) lists
        """
        self.logger.debug(f"Determining files for transfer for job: {job}")
        transfer_input_files = []
        transfer_output_files = []

        # Add the main snakefile to the transfer list if not on shared filesystem
        main_snakefile = self.get_snakefile()
        self._add_file_if_transferable(main_snakefile, transfer_input_files)

        # Add module Snakefiles by parsing the main Snakefile
        # Module declarations look like: module name:\n    snakefile: "path/to/Snakefile"
        self._add_module_snakefiles(main_snakefile, transfer_input_files)

        # Process input files
        if job.input:
            inputs = [input for input in job.input]
            self.logger.debug(f"Job {job.name} input files: {inputs}")
            # IMPORTANT: Preserve relative paths by default to maintain directory structure
            # at the execution point. HTCondor will recreate the relative path structure.
            # Set preserve_relative_paths=False to transfer only top-level directories.
            if not job.resources.get("preserve_relative_paths", True):
                inputs = {path.split(sep)[0] for path in inputs}

            for path in inputs:
                self._add_file_if_transferable(path, transfer_input_files)

        # NOTE: Config files are handled separately by _prepare_config_files_for_transfer()
        # which returns relative paths for both the job arguments and the transfer list.

        # Process output files (only transfer top-most output directories)
        # NOTE: For outputs, we only transfer top-level directories to avoid
        # transferring deeply nested structures. HTCondor will bring back the
        # entire directory tree from the execution point.
        if job.output:
            top_most_output_directories = {path.split(sep)[0] for path in job.output}
            for path in top_most_output_directories:
                self._add_file_if_transferable(
                    path, transfer_output_files, validate_exists=False
                )

        # Process script and notebook files from all rules in the job
        # For grouped jobs, we need to iterate over job.jobs to access each individual
        # job's rule which contains the script/notebook attributes.
        # For individual jobs, job.rule directly has these attributes.
        if job.is_group():
            # GroupJob: iterate over individual jobs within the group
            individual_jobs = job.jobs if hasattr(job, "jobs") else []
            for individual_job in individual_jobs:
                if hasattr(individual_job, "rule"):
                    rule = individual_job.rule
                    if hasattr(rule, "script") and rule.script:
                        self.logger.debug(
                            f"Processing script from grouped job: {rule.script}"
                        )
                        self._add_file_if_transferable(
                            rule.script,
                            transfer_input_files,
                            expand_wildcards=True,
                            job=individual_job,
                        )
                    if hasattr(rule, "notebook") and rule.notebook:
                        self.logger.debug(
                            f"Processing notebook from grouped job: {rule.notebook}"
                        )
                        self._add_file_if_transferable(
                            rule.notebook,
                            transfer_input_files,
                            expand_wildcards=True,
                            job=individual_job,
                        )
        else:
            # Individual job: access rule directly
            if hasattr(job, "rule"):
                rule = job.rule
                if hasattr(rule, "script") and rule.script:
                    self.logger.debug(f"Processing script: {rule.script}")
                    self._add_file_if_transferable(
                        rule.script,
                        transfer_input_files,
                        expand_wildcards=True,
                        job=job,
                    )
                if hasattr(rule, "notebook") and rule.notebook:
                    self.logger.debug(f"Processing notebook: {rule.notebook}")
                    self._add_file_if_transferable(
                        rule.notebook,
                        transfer_input_files,
                        expand_wildcards=True,
                        job=job,
                    )

        # Process additional input files from htcondor_transfer_input_files resource
        # Note: Wildcard expansion is only supported for individual jobs. For grouped jobs,
        # wildcards cannot be reliably expanded since these resources are defined at the
        # group level, not per individual job.
        if additional_files := job.resources.get("htcondor_transfer_input_files"):
            expand_wildcards = not job.is_group()
            for file_path in self._parse_file_list(additional_files):
                self._add_file_if_transferable(
                    file_path,
                    transfer_input_files,
                    expand_wildcards=expand_wildcards,
                    job=job,
                )

        # Process additional output files from htcondor_transfer_output_files resource
        # Note: Wildcard expansion is only supported for individual jobs. For grouped jobs,
        # wildcards cannot be reliably expanded since these resources are defined at the
        # group level, not per individual job.
        if additional_outputs := job.resources.get("htcondor_transfer_output_files"):
            expand_wildcards = not job.is_group()
            for file_path in self._parse_file_list(additional_outputs):
                self._add_file_if_transferable(
                    file_path,
                    transfer_output_files,
                    expand_wildcards=expand_wildcards,
                    job=job,
                    validate_exists=False,
                )

        # Explicitly handle job_wrapper if specified
        # The job_wrapper is used as the executable, so it must be transferred
        if job_wrapper := job.resources.get("job_wrapper"):
            self.logger.debug(f"Detected job_wrapper resource: {job_wrapper}")
            self._add_file_if_transferable(
                job_wrapper,
                transfer_input_files,
                expand_wildcards=False,  # job_wrapper paths typically don't use wildcards
                job=job,
            )

        self.logger.debug(
            f"Transfer input files: {transfer_input_files}\n"
            f"Transfer output files: {transfer_output_files}"
        )
        return transfer_input_files, transfer_output_files

    def _prepare_config_files_for_transfer(
        self, job_args: str, shared_fs_prefixes: List[str]
    ) -> tuple[str, List[str]]:
        """
        Prepare config files for transfer and adjust job arguments.

        From the Snakemake documentation:
        "The given path is interpreted relative to the working directory,
        not relative to the location of the snakefile that contains the statement."

        Snakemake internally converts relative config paths to absolute paths.
        We convert them back to relative paths from the working directory so that:
        1. HTCondor preserves the directory structure with preserve_relative_paths=True
        2. The --configfiles argument matches the transferred file locations

        This function is the single source of truth for config file path handling.
        The returned paths should be used for BOTH the job arguments AND the
        transfer_input_files list.

        Shared filesystem checking is done using the ORIGINAL absolute path
        (before conversion to relative), since shared FS prefixes are absolute
        paths like '/staging/' or '/shared/'.

        Args:
            job_args: Original job arguments string
            shared_fs_prefixes: List of shared filesystem path prefixes

        Returns:
            Tuple of (modified_job_args, config_file_paths_for_transfer)
            - modified_job_args: Job arguments with updated --configfiles paths
            - config_file_paths_for_transfer: Relative paths to add to transfer_input_files
              (excludes files on shared filesystems)
        """
        assert isinstance(shared_fs_prefixes, list), "shared_fs_prefixes must be a list"

        if not self.workflow.configfiles:
            self.logger.debug("No config files to process")
            return job_args, []

        # Get the working directory where Snakemake was invoked.
        # Snakemake converts relative config paths to absolute paths internally,
        # so we need to convert them back to relative paths for HTCondor transfer.
        workdir = self.workflow.workdir_init
        self.logger.debug(f"Processing config files with workdir_init: {workdir}")

        config_paths_for_args = []  # All config paths (for --configfiles argument)
        config_paths_for_transfer = (
            []
        )  # Only non-shared-FS paths (for transfer_input_files)

        for fpath in self.workflow.configfiles:
            fpath_str = str(fpath)

            # Check if this config file is on a shared filesystem
            on_shared_fs = is_shared_fs(fpath_str, shared_fs_prefixes)

            if on_shared_fs:
                # Shared FS: Keep the absolute path in args (Snakemake accesses it directly)
                # and do NOT add to transfer list
                self.logger.debug(
                    f"Config file on shared FS (keeping absolute path): {fpath_str}"
                )
                config_paths_for_args.append(fpath_str)
            else:
                # Non-shared FS: Convert to relative path for args (matches where HTCondor
                # will place the file) and add to transfer list
                if isabs(fpath_str):
                    # Snakemake converts relative paths to absolute. Convert back to
                    # relative from the working directory so HTCondor preserves the
                    # directory structure with preserve_relative_paths=True.
                    relative_path = relpath(fpath_str, workdir)
                    self.logger.debug(
                        f"Config file converted to relative path: {fpath_str} -> {relative_path}"
                    )
                else:
                    # Already relative - keep as-is
                    relative_path = fpath_str
                    self.logger.debug(
                        f"Config file already relative (keeping as-is): {relative_path}"
                    )

                # Error if config file path escapes the working directory (uses ../ paths)
                # This applies to both converted and already-relative paths.
                # This is an error because we can't permit a relative path that would "escape"
                # the job's scratch directory on the EP. We require that all paths are under
                # the working/submit directory.
                if relative_path.startswith(".."):
                    raise WorkflowError(
                        f"Config file '{fpath_str}' is outside the working directory "
                        f"'{workdir}'. HTCondor cannot transfer files using relative paths "
                        f"that navigate above the submit directory (paths starting with '../'). "
                        f"Please submit your workflow from a directory that is above all config files, "
                        f"or place the config file on a shared filesystem accessible to both the Access Point "
                        f"and Execution Points."
                    )

                config_paths_for_args.append(relative_path)
                config_paths_for_transfer.append(relative_path)

        # Modify job args to use the appropriate paths
        if config_paths_for_args:
            config_arg = " ".join(config_paths_for_args)
            configfiles_pattern = r"--configfiles .*?(?=( --|$))"
            job_args = re.sub(
                configfiles_pattern, f"--configfiles {config_arg}", job_args
            )
            self.logger.debug(
                f"Updated --configfiles argument to: --configfiles {config_arg}"
            )

        self.logger.debug(f"Config files to transfer: {config_paths_for_transfer}")
        return job_args, config_paths_for_transfer

    def _get_base_exec_and_args(self, job: JobExecutorInterface) -> tuple[str, str]:
        """
        Get the base executable and arguments for the HTCondor job.

        This is an internal helper that returns the initial executable and arguments.
        The arguments may be further modified by _get_exec_args_and_transfer_files()
        to handle config file paths for HTCondor transfer.

        When a job wrapper is specified, it becomes the executable and the
        Snakemake arguments (minus the 'python -m snakemake' prefix) become
        the arguments. The wrapper path is preserved as-is to support wrappers
        in subdirectories (e.g., 'workflow/wrapper.sh').

        When no wrapper is specified, Python is the executable and the full
        Snakemake command (minus the python executable prefix) is used.

        The returned arguments are sanitized for HTCondor compatibility.

        Args:
            job: The Snakemake job being prepared for submission

        Returns:
            Tuple of (executable_path, sanitized_job_arguments)
        """
        if job_wrapper := job.resources.get("job_wrapper"):
            # Use the wrapper script as the executable.
            # IMPORTANT: Preserve the full relative path (e.g., 'workflow/wrapper.sh')
            # to support wrappers in subdirectories.
            job_exec = job_wrapper
            # The wrapper script will take as input all snakemake arguments,
            # so we assume it contains something like `snakemake $@`
            job_args = self.format_job_exec(job).removeprefix("python -m snakemake ")
        else:
            # Use sys.executable to get the full path to Python interpreter
            # (not just "python" which won't work for HTCondor transfers)
            job_exec = sys.executable
            # Get the command that would normally be run
            full_cmd = self.format_job_exec(job)
            # Remove the python executable prefix (which might be just "python" or a path)
            for prefix in [
                sys.executable + " ",
                "python ",
                self.get_python_executable() + " ",
            ]:
                if full_cmd.startswith(prefix):
                    job_args = full_cmd.removeprefix(prefix)
                    break
            else:
                # Fallback: no known prefix matched
                # This is unexpected and might indicate a problem with command formatting
                self.logger.warning(
                    f"Could not strip Python executable prefix from command: '{full_cmd}'. "
                    f"Expected to start with one of: {sys.executable}, python, or {self.get_python_executable()}. "
                    f"Falling back to naive space-based splitting."
                )
                job_args = full_cmd.split(" ", 1)[1] if " " in full_cmd else ""

        # Sanitize arguments before returning
        job_args = self._sanitize_job_args(job_args)

        return job_exec, job_args

    def _sanitize_job_args(self, job_args: str) -> str:
        """
        Sanitize job arguments for HTCondor compatibility.

        HTCondor cannot handle single quotes in arguments, so they are removed.

        Args:
            job_args: The job arguments string

        Returns:
            Sanitized job arguments string
        """
        if "'" in job_args:
            job_args = job_args.replace("'", "")
        return job_args

    def _get_exec_args_and_transfer_files(
        self, job: JobExecutorInterface, needs_transfer: bool
    ) -> tuple[str, str, List[str], List[str]]:
        """
        Get the executable, arguments, and transfer file lists for a job.

        This method handles three filesystem sharing modes:

        1. Full shared FS (needs_transfer=False):
           AP and EP share a filesystem. Absolute paths work everywhere,
           no files need to be transferred. Arguments are returned unchanged.

        2. No shared FS (needs_transfer=True, shared_fs_prefixes=[]):
           AP and EP share nothing. All files must be transferred from AP to EP.
           Paths are converted to relative so HTCondor preserves directory structure.

        3. Partial shared FS (needs_transfer=True, shared_fs_prefixes=['/staging', ...]):
           AP and EP share specific paths (e.g., /staging). Files under shared
           prefixes keep absolute paths and are NOT transferred. Other files use
           relative paths and ARE transferred.

        Args:
            job: The Snakemake job being prepared for submission
            needs_transfer: Whether file transfer is needed (False for full shared FS)

        Returns:
            Tuple of (executable, final_arguments, transfer_input_files, transfer_output_files)
        """
        # Get base executable and arguments
        job_exec, job_args = self._get_base_exec_and_args(job)
        self.logger.debug(f"Base executable: {job_exec}")
        self.logger.debug(f"Base arguments: {job_args}")

        # Mode 1: Full shared filesystem - no transfer preparation needed
        if not needs_transfer:
            self.logger.debug(
                "Full shared filesystem mode - skipping transfer preparation"
            )
            return job_exec, job_args, [], []

        # Modes 2 & 3: Need to prepare files for transfer
        if self.shared_fs_prefixes:
            self.logger.debug(
                f"Partial shared filesystem mode - shared prefixes: {self.shared_fs_prefixes}"
            )
        else:
            self.logger.debug(
                "No shared filesystem mode - all files will be transferred"
            )

        # Get files that need to be transferred (excludes config files)
        transfer_input_files, transfer_output_files = self._get_files_for_transfer(job)

        # Prepare config files - this modifies the arguments to use correct paths
        # and returns the list of config files to transfer
        job_args, config_paths = self._prepare_config_files_for_transfer(
            job_args, self.shared_fs_prefixes
        )

        # Add config files to the transfer input list
        transfer_input_files.extend(config_paths)

        self.logger.debug(f"Final arguments: {job_args}")
        self.logger.debug(
            f"Final transfer_input_files ({len(transfer_input_files)}): {transfer_input_files}"
        )
        self.logger.debug(
            f"Final transfer_output_files ({len(transfer_output_files)}): {transfer_output_files}"
        )

        return job_exec, job_args, transfer_input_files, transfer_output_files

    def run_job(self, job: JobExecutorInterface):
        # Submitting job to HTCondor

        # Determine if file transfer is needed based on shared filesystem configuration
        # When shared_fs_usage is set (truthy), AP and EP share a filesystem
        # When shared_fs_usage is empty/none, files must be transferred
        needs_transfer = not self.workflow.storage_settings.shared_fs_usage

        # Get executable, arguments (with adjusted config paths), and transfer file lists
        job_exec, job_args, transfer_input_files, transfer_output_files = (
            self._get_exec_args_and_transfer_files(job, needs_transfer)
        )

        # Creating submit dictionary which is passed to htcondor.Submit
        submit_dict = {
            "executable": job_exec,
            "arguments": job_args,
            "log": join(self.jobDir, "$(ClusterId).log"),
            "output": join(self.jobDir, "$(ClusterId).out"),
            "error": join(self.jobDir, "$(ClusterId).err"),
            "request_cpus": str(job.threads),
        }

        # Supported universes for HTCondor
        supported_universes = [
            "vanilla",
            "docker",
            "container",
            "scheduler",
            "local",
            "parallel",
            "grid",
            "java",
            "vm",
        ]
        if universe := job.resources.get("universe"):
            if universe not in supported_universes:
                raise WorkflowError(
                    f"The universe {universe} is not supported by HTCondor.",
                    "See the HTCondor reference manual for a list of supported universes.",  # noqa
                )

            submit_dict["universe"] = universe

            # Check for container image requirement
            container_image = job.resources.get("container_image")
            if universe in ["docker", "container"] and not container_image:
                raise WorkflowError(
                    "A container image must be specified when using the docker or container universe."  # noqa
                )
            elif container_image:
                submit_dict["container_image"] = container_image

        # If we're not using a shared filesystem, we need to setup transfers
        # for any job wrapper, config files, input files, etc
        if not self.workflow.storage_settings.shared_fs_usage:
            submit_dict["should_transfer_files"] = "YES"
            submit_dict["when_to_transfer_output"] = "ON_EXIT"

            if transfer_input_files:
                self.logger.debug(f"Transfer input files: {transfer_input_files}")
                submit_dict["transfer_input_files"] = ", ".join(
                    sorted(transfer_input_files)
                )

            if transfer_output_files:
                self.logger.debug(f"Transfer output files: {transfer_output_files}")
                submit_dict["transfer_output_files"] = ", ".join(
                    sorted(transfer_output_files)
                )

        # Basic commands
        if job.resources.get("getenv"):
            submit_dict["getenv"] = job.resources.get("getenv")
        else:
            submit_dict["getenv"] = False

        if job.resources.get("preserve_relative_paths", True):
            submit_dict["preserve_relative_paths"] = True

        for key in ["environment", "input", "max_materialize", "max_idle"]:
            if job.resources.get(key):
                submit_dict[key] = job.resources.get(key)

        # Commands for matchmaking
        for key in [
            "rank",
            "request_disk",
            "request_memory",
            "requirements",
        ]:
            if job.resources.get(key):
                submit_dict[key] = job.resources.get(key)

        # HTCondor-specific resource parameters with explicit units (MB)
        # These take precedence over the standard HTCondor parameters when both are set.
        # They're designed for use with grouped jobs where Snakemake can aggregate numeric values
        # (summing resources across jobs running in parallel within each layer, then taking the
        # maximum across all sequential layers in the group).
        self._handle_explicit_unit_resources(job, submit_dict)

        # Commands for matchmaking (GPU)
        for key in [
            "request_gpus",
            "require_gpus",
            "gpus_minimum_capability",
            "gpus_minimum_memory",
            "gpus_minimum_runtime",
            "cuda_version",
        ]:
            if job.resources.get(key):
                submit_dict[key] = job.resources.get(key)

        # Policy commands
        max_retries = job.resources.get("max_retries")
        if max_retries is not None:
            submit_dict["max_retries"] = max_retries
        else:
            submit_dict["max_retries"] = 5

        for key in ["allowed_execute_duration", "allowed_job_duration", "retry_until"]:
            if job.resources.get(key):
                submit_dict[key] = job.resources.get(key)

        # Name the jobs in the queue something that tells us what the job is
        submit_dict["batch_name"] = f"{job.name}-{job.jobid}"

        # Check any custom classads
        for key in job.resources.keys():
            if key.startswith("classad_"):
                classad_key = "+" + key.removeprefix("classad_")
                value = job.resources.get(key)
                # If the value is a string, HTCondor requires it to be quoted.
                if isinstance(value, str):
                    submit_dict[classad_key] = f'"{value}"'
                else:
                    submit_dict[classad_key] = value

        # Log resource requests with human-readable units
        self._log_resource_requests(submit_dict)

        # HTCondor submit description
        self.logger.debug(f"HTCondor submit subscription: {submit_dict}")
        submit_description = htcondor.Submit(submit_dict)

        # Client for HTCondor Schedduler
        schedd = htcondor.Schedd()

        # Submitting job to HTCondor
        try:
            submit_description.issue_credentials()
            submit_result = schedd.submit(submit_description)
        except Exception as e:
            traceback.print_exc()
            raise WorkflowError(f"Failed to submit HTCondor job: {e}")

        self.logger.info(
            f"Job {job.jobid} submitted to "
            f"HTCondor Cluster ID {submit_result.cluster()}\n"
            f"The logs of the HTCondor job are stored "
            f"in {self.jobDir}/{submit_result.cluster()}.log"
        )

        # Initialize the JobEventLog reader for this job.
        # We do this at submission time so the reader is ready when we start checking.
        cluster_id = submit_result.cluster()
        log_path = join(self.jobDir, f"{cluster_id}.log")
        try:
            self._job_event_logs[cluster_id] = JobEventLog(log_path)
            self.logger.debug(
                f"Initialized JobEventLog reader for cluster {cluster_id}"
            )
        except Exception as e:
            self.logger.warning(
                f"Could not initialize JobEventLog for cluster {cluster_id}: {e}. "
                "Will retry on first status check."
            )

        self.report_job_submission(
            SubmittedJobInfo(job=job, external_jobid=submit_result.cluster())
        )

    def _get_job_event_log(self, cluster_id: int) -> Optional[JobEventLog]:
        """
        Get or create a JobEventLog reader for the given cluster ID.

        This method lazily initializes log readers if they weren't created at
        submission time (e.g., if the log file wasn't ready yet).

        Args:
            cluster_id: The HTCondor ClusterId for the job

        Returns:
            JobEventLog reader, or None if the log file doesn't exist yet
        """
        if cluster_id in self._job_event_logs:
            return self._job_event_logs[cluster_id]

        # Try to create the reader now
        log_path = join(self.jobDir, f"{cluster_id}.log")
        if exists(log_path):
            try:
                self._job_event_logs[cluster_id] = JobEventLog(log_path)
                self.logger.debug(
                    f"Lazily initialized JobEventLog reader for cluster {cluster_id}"
                )
                return self._job_event_logs[cluster_id]
            except Exception as e:
                self.logger.warning(
                    f"Could not initialize JobEventLog for cluster {cluster_id}: {e}"
                )
                return None
        else:
            self.logger.debug(f"Log file not yet available for cluster {cluster_id}")
            return None

    def _cleanup_job_tracking(self, cluster_id: int) -> None:
        """
        Clean up tracking data structures for a completed/failed job.

        This prevents memory growth over long-running workflows by removing
        references to jobs that are no longer active.

        Args:
            cluster_id: The HTCondor ClusterId for the job
        """
        # Remove the JobEventLog reader (closes file handle)
        if cluster_id in self._job_event_logs:
            del self._job_event_logs[cluster_id]

        # Remove cached job state
        if cluster_id in self._job_current_states:
            del self._job_current_states[cluster_id]

        # Remove from log-missing tracking
        if cluster_id in self._log_missing_counts:
            del self._log_missing_counts[cluster_id]

    def _htcondor_status_to_job_status(self, job_status: int) -> JobStatus:
        """
        Convert HTCondor's numeric JobStatus to our JobStatus enum.

        https://htcondor.readthedocs.io/en/latest/codes-other-values/job-status-codes.html#job-status-codes
        HTCondor JobStatus values:
        1 = Idle, 2 = Running, 3 = Removed, 4 = Completed, 5 = Held,
        6 = Transferring Output, 7 = Suspended

        Args:
            job_status: HTCondor's numeric JobStatus value

        Returns:
            Corresponding JobStatus enum value
        """
        status_map = {
            1: JobStatus.IDLE,
            2: JobStatus.RUNNING,
            3: JobStatus.REMOVED,
            4: JobStatus.COMPLETED,
            5: JobStatus.HELD,
            6: JobStatus.TRANSFERRING,
            7: JobStatus.SUSPENDED,
        }
        return status_map.get(job_status, JobStatus.PENDING)

    def _batch_query_schedd(self, cluster_ids: List[int]) -> Dict[int, JobState]:
        """
        Query the schedd for multiple jobs' current status in a single call.

        This is a fallback method used when job log files are not available.
        By batching multiple job queries into one schedd call, we avoid the
        sequential blocking problem where each job's query must complete
        before the next can start.

        Args:
            cluster_ids: List of HTCondor ClusterIds to query

        Returns:
            Dict mapping cluster_id to job state info for jobs found in schedd.
            Jobs not in schedd will not have entries in the result.
        """
        if not cluster_ids:
            return {}

        results = {}
        try:
            schedd = htcondor.Schedd()
            # Build a constraint that matches any of the cluster IDs
            # e.g., "ClusterId == 123 || ClusterId == 456 || ClusterId == 789"
            constraint_parts = [f"ClusterId == {cid}" for cid in cluster_ids]
            constraint = " || ".join(constraint_parts)
            projection = [
                "ClusterId",
                "JobStatus",
                "HoldReason",
                "ExitCode",
                "ExitBySignal",
                "ExitSignal",
                "EnteredCurrentStatus",
            ]

            self.logger.debug(
                f"Batch schedd query for {len(cluster_ids)} jobs: {cluster_ids}"
            )

            jobs = list(schedd.query(constraint=constraint, projection=projection))

            for job in jobs:
                cluster_id = job.get("ClusterId")
                if cluster_id is None:
                    continue

                htcondor_status = job.get("JobStatus", 1)
                status = self._htcondor_status_to_job_status(htcondor_status)

                # Use EnteredCurrentStatus for accurate held_since timestamp.
                # This is the actual time the job entered the held state,
                # not the time we queried it.
                held_since = None
                if status == JobStatus.HELD:
                    entered = job.get("EnteredCurrentStatus")
                    held_since = float(entered) if entered is not None else time.time()

                results[cluster_id] = JobState(
                    status=status,
                    exit_code=job.get("ExitCode"),
                    exit_by_signal=bool(job.get("ExitBySignal", False)),
                    exit_signal=job.get("ExitSignal"),
                    hold_reason=job.get("HoldReason"),
                    held_since=held_since,
                )

            self.logger.debug(
                f"Batch schedd query found {len(results)} of {len(cluster_ids)} jobs"
            )

        except Exception as e:
            self.logger.warning(f"Error in batch schedd query: {e}")

        return results

    def _batch_query_history(self, cluster_ids: List[int]) -> Dict[int, JobState]:
        """
        Query condor_history for multiple completed jobs in a single call.

        This is a fallback method used when jobs are no longer in the schedd
        (already completed) and log files are not available. Uses the -since
        option to limit the scan to jobs completed after the workflow started.

        Args:
            cluster_ids: List of HTCondor ClusterIds to query

        Returns:
            Dict mapping cluster_id to JobState for jobs found in history.
            Jobs not in history will not have entries in the result.
        """
        if not cluster_ids:
            return {}

        results = {}
        try:
            schedd = htcondor.Schedd()
            # Build a constraint that matches any of the cluster IDs
            constraint_parts = [f"ClusterId == {cid}" for cid in cluster_ids]
            constraint = " || ".join(constraint_parts)
            projection = [
                "ClusterId",
                "JobStatus",
                "ExitCode",
                "ExitBySignal",
                "ExitSignal",
                "HoldReason",
            ]

            self.logger.debug(
                f"Batch history query for {len(cluster_ids)} jobs: {cluster_ids}"
            )

            # Use since= to limit the history scan to jobs that completed
            # after the workflow started. This avoids scanning the entire history.
            # Note: match= limits total results, so we set it high enough
            # to potentially find all the jobs we're looking for.
            jobs = list(
                schedd.history(
                    constraint=constraint,
                    projection=projection,
                    since=self._workflow_start_time,
                    match=len(cluster_ids),
                )
            )

            for job in jobs:
                cluster_id = job.get("ClusterId")
                if cluster_id is None:
                    continue

                htcondor_status = job.get("JobStatus", 4)  # Default to Completed
                status = self._htcondor_status_to_job_status(htcondor_status)

                results[cluster_id] = JobState(
                    status=status,
                    exit_code=job.get("ExitCode"),
                    exit_by_signal=bool(job.get("ExitBySignal", False)),
                    exit_signal=job.get("ExitSignal"),
                    hold_reason=job.get("HoldReason"),
                    held_since=None,
                )

            self.logger.debug(
                f"Batch history query found {len(results)} of {len(cluster_ids)} jobs"
            )

            # Log warnings for jobs not found anywhere
            not_found = set(cluster_ids) - set(results.keys())
            for cid in not_found:
                self.logger.warning(
                    f"Job {cid} not found in schedd or recent history. "
                    "This may indicate the job was removed or the log file was lost."
                )

        except Exception as e:
            self.logger.warning(f"Error in batch history query: {e}")

        return results

    def _try_read_job_log(self, cluster_id: int) -> Optional[JobState]:
        """
        Attempt to read job status from the event log.

        This is the preferred method for getting job status as it doesn't
        require any schedd queries. If the log file is available and readable,
        returns the job state. Otherwise returns None.

        Args:
            cluster_id: The HTCondor ClusterId for the job

        Returns:
            A JobState with job state info, or None if log couldn't be read
        """
        job_state = self._read_job_events(cluster_id)

        if job_state is not None:
            # Log file worked - reset missing counter
            if cluster_id in self._log_missing_counts:
                del self._log_missing_counts[cluster_id]
            return job_state

        # Log file not available - track consecutive failures
        self._log_missing_counts[cluster_id] = (
            self._log_missing_counts.get(cluster_id, 0) + 1
        )

        return None

    def _needs_fallback(self, cluster_id: int) -> bool:
        """
        Check if a job has exceeded the log missing threshold and needs fallback.

        Args:
            cluster_id: The HTCondor ClusterId for the job

        Returns:
            True if the job needs fallback queries, False otherwise
        """
        return (
            self._log_missing_counts.get(cluster_id, 0) >= self._log_missing_threshold
        )

    def _read_job_events(self, cluster_id: int) -> Optional[JobState]:
        """
        Read all available events from a job's log file and determine its current state.

        This method reads events non-blockingly (stop_after=0) and updates the
        tracked state for this job. Since JobEventLog readers maintain their position,
        subsequent calls only read NEW events and we must track state across calls.

        The key insight is that HTCondor log files are append-only and contain
        the complete history of a job. We read all new events since our last
        read and update the tracked state.

        Args:
            cluster_id: The HTCondor ClusterId for the job

        Returns:
            A JobState with current job state info, or None if we couldn't read the log.
        """
        # If job already reached a true terminal state, return cached result
        # immediately — no log access needed.
        # Note: 'held' is NOT terminal - job may be released and continue
        cached = self._job_current_states.get(cluster_id)
        if cached is not None and cached.status.is_terminal():
            return cached

        event_log = self._get_job_event_log(cluster_id)
        if event_log is None:
            # Can't read log - return None so _try_read_job_log knows the log
            # is still unavailable and won't reset the missing counter.
            # The cached state in _job_current_states is preserved and will be
            # available when the log becomes readable again.
            return None

        # Get or initialize the current state for this job
        # We persist state across calls because JobEventLog readers maintain position
        if cluster_id not in self._job_current_states:
            self._job_current_states[cluster_id] = JobState()

        current_state = self._job_current_states[cluster_id]

        try:
            # Read all available NEW events without blocking (stop_after=0)
            # This returns immediately with events since our last read
            events_read = 0
            for event in event_log.events(stop_after=0):
                events_read += 1
                event_type = event.type

                if event_type == JobEventType.SUBMIT:
                    current_state.status = JobStatus.IDLE

                elif event_type == JobEventType.EXECUTE:
                    current_state.status = JobStatus.RUNNING

                elif event_type == JobEventType.JOB_EVICTED:
                    # Job was evicted but may be rescheduled
                    current_state.status = JobStatus.IDLE

                elif event_type == JobEventType.JOB_SUSPENDED:
                    current_state.status = JobStatus.SUSPENDED

                elif event_type == JobEventType.JOB_UNSUSPENDED:
                    current_state.status = JobStatus.RUNNING

                elif event_type == JobEventType.IMAGE_SIZE:
                    # Just an update, job is still running - don't change status
                    pass

                elif event_type == JobEventType.FILE_TRANSFER:
                    # File transfer in progress - could be input (before run) or output (after)
                    # Don't override 'running' status if already running
                    if current_state.status != JobStatus.RUNNING:
                        current_state.status = JobStatus.TRANSFERRING

                elif event_type == JobEventType.JOB_HELD:
                    current_state.status = JobStatus.HELD
                    # Try to get hold reason from event
                    current_state.hold_reason = event.get(
                        "HoldReason", "Unknown reason"
                    )
                    # Record when the job was held for timeout tracking
                    current_state.held_since = time.time()

                elif event_type == JobEventType.JOB_RELEASED:
                    # Job was released from hold, back to idle
                    current_state.status = JobStatus.IDLE
                    current_state.hold_reason = None
                    current_state.held_since = None

                elif event_type == JobEventType.JOB_TERMINATED:
                    current_state.status = JobStatus.COMPLETED
                    # Extract exit information from the event
                    current_state.exit_code = event.get("ReturnValue", None)
                    # Check if terminated by signal - look for TerminatedNormally=False
                    terminated_normally = event.get("TerminatedNormally", True)
                    current_state.exit_by_signal = not terminated_normally
                    if current_state.exit_by_signal:
                        current_state.exit_signal = event.get(
                            "TerminatedBySignal", None
                        )

                elif event_type == JobEventType.JOB_ABORTED:
                    current_state.status = JobStatus.REMOVED

                elif event_type == JobEventType.JOB_RECONNECT_FAILED:
                    # Shadow couldn't reconnect, job will be rescheduled
                    current_state.status = JobStatus.IDLE

                elif event_type == JobEventType.SHADOW_EXCEPTION:
                    # Shadow had an exception, job may be rescheduled
                    current_state.status = JobStatus.IDLE

                elif event_type == JobEventType.JOB_DISCONNECTED:
                    # Starter and shadow are disconnected, may reconnect
                    pass  # Keep current status

                elif event_type == JobEventType.JOB_RECONNECTED:
                    # Reconnected after disconnect
                    current_state.status = JobStatus.RUNNING

                # Note: Other event types (ATTRIBUTE_UPDATE, CHECKPOINTED, etc.)
                # don't change the fundamental job state, so we ignore them.

            self.logger.debug(
                f"Read {events_read} new events for cluster {cluster_id}, "
                f"status: {current_state.status.display_name}"
            )

        except Exception as e:
            self.logger.warning(
                f"Error reading job events for cluster {cluster_id}: {e}"
            )
            # Return None so the fallback mechanism can kick in.
            # The last known state is preserved in self._job_current_states
            # and will be available if the log becomes readable again.
            return None

        return current_state

    def _report_and_resolve_job_state(
        self, current_job: SubmittedJobInfo, job_state: JobState
    ) -> Optional[SubmittedJobInfo]:
        """
        Evaluate a job's state, report results to Snakemake, and resolve whether
        the job should remain in the active tracking list.

        For terminal states (completed, removed), this reports success/error to
        the Snakemake framework and cleans up tracking data. For active states,
        it returns the job so it continues to be monitored.

        Args:
            current_job: The SubmittedJobInfo for this job
            job_state: The JobState with current status and metadata

        Returns:
            The current_job if it should remain active, None if it's terminal
        """
        cluster_id = current_job.external_jobid
        status = job_state.status

        self.logger.debug(
            f"Job {current_job.job.jobid} with HTCondor Cluster ID "
            f"{cluster_id} has status: {status.display_name}"
        )

        # Handle different states
        if status in (
            JobStatus.PENDING,
            JobStatus.IDLE,
            JobStatus.RUNNING,
            JobStatus.TRANSFERRING,
        ):
            # Job is still active
            return current_job

        elif status == JobStatus.SUSPENDED:
            self.logger.warning(
                f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                f"{cluster_id} is suspended."
            )
            return current_job

        elif status == JobStatus.COMPLETED:
            exit_code = job_state.exit_code
            exit_by_signal = job_state.exit_by_signal

            if exit_by_signal:
                signal = (
                    job_state.exit_signal
                    if job_state.exit_signal is not None
                    else "unknown"
                )
                self.logger.debug(
                    f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                    f"{cluster_id} terminated by signal {signal}"
                )
                self.report_job_error(
                    current_job,
                    msg=f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                    f"{cluster_id} was terminated by signal {signal}.",
                )
            elif exit_code == 0:
                self.logger.debug(
                    f"Report Job {current_job.job.jobid} with HTCondor Cluster ID "
                    f"{cluster_id} success"
                )
                self.logger.info(
                    f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                    f"{cluster_id} was successful."
                )
                self.report_job_success(current_job)
            else:
                self.logger.debug(
                    f"Report Job {current_job.job.jobid} with HTCondor Cluster ID "
                    f"{cluster_id} error"
                )
                self.report_job_error(
                    current_job,
                    msg=f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                    f"{cluster_id} completed but failed with ExitCode {exit_code}.",
                )

            # Clean up tracking for this job
            self._cleanup_job_tracking(cluster_id)
            return None

        elif status == JobStatus.HELD:
            hold_reason = job_state.hold_reason or "Unknown reason"
            held_since = job_state.held_since

            # Check if hold timeout has been exceeded
            if self._held_timeout == 0:
                # Timeout of 0 means fail immediately
                self.report_job_error(
                    current_job,
                    msg=f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                    f"{cluster_id} is held: {hold_reason}",
                )
                self._cleanup_job_tracking(cluster_id)
                return None
            elif held_since is not None:
                elapsed = time.time() - held_since
                if elapsed >= self._held_timeout:
                    # Timeout exceeded - treat as terminal failure
                    timeout_hours = self._held_timeout / 3600
                    self.report_job_error(
                        current_job,
                        msg=f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                        f"{cluster_id} has been held for {elapsed / 3600:.1f} hours "
                        f"(timeout: {timeout_hours:.1f} hours). Hold reason: {hold_reason}",
                    )
                    self._cleanup_job_tracking(cluster_id)
                    return None
                else:
                    # Still within timeout - keep monitoring
                    remaining = self._held_timeout - elapsed
                    self.logger.info(
                        f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                        f"{cluster_id} is held: {hold_reason}. "
                        f"Will fail in {remaining / 60:.0f} minutes if not released."
                    )
                    return current_job
            else:
                # No held_since timestamp (shouldn't happen) - keep monitoring
                self.logger.warning(
                    f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                    f"{cluster_id} is held but missing timestamp: {hold_reason}"
                )
                return current_job

        elif status == JobStatus.REMOVED:
            self.report_job_error(
                current_job,
                msg=f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                f"{cluster_id} was removed/aborted.",
            )
            # Clean up tracking for this job
            self._cleanup_job_tracking(cluster_id)
            return None

        else:
            # Unknown status - keep monitoring
            self.logger.warning(
                f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                f"{cluster_id} has unexpected status: {status}"
            )
            return current_job

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> AsyncGenerator[SubmittedJobInfo, None]:
        """
        Check the status of active jobs by reading their HTCondor log files.

        This implementation uses a two-pass approach to avoid blocking:

        Pass 1 (fast, non-blocking):
          - Read job event logs for all jobs (local file I/O, microseconds each)
          - Process jobs with known states immediately
          - Collect jobs needing fallback into a batch

        Pass 2 (batched, single blocking call if needed):
          - Query schedd once for all jobs needing fallback
          - Query history once for jobs not found in schedd
          - Process remaining jobs

        This approach ensures that slow history queries for one job don't block
        status checks for other jobs. The common case (log files available) is
        very fast, and the fallback case batches queries efficiently.

        The method reads all new events from each job's log file and determines
        the current state. Terminal states (completed, removed) are cached
        to avoid re-reading finished jobs. Held jobs are monitored with a timeout.
        """
        # Maps cluster_id -> SubmittedJobInfo for jobs needing fallback
        jobs_needing_fallback: Dict[int, SubmittedJobInfo] = {}

        # ========== PASS 1: Try log files for all jobs (fast) ==========
        for current_job in active_jobs:
            cluster_id = current_job.external_jobid

            try:
                # Try to read from the job event log (fast, non-blocking)
                job_state = self._try_read_job_log(cluster_id)

                if job_state is not None:
                    # Got state from log - process it immediately
                    result = self._report_and_resolve_job_state(current_job, job_state)
                    if result is not None:
                        yield result
                else:
                    # Log not available - check if we need fallback
                    if self._needs_fallback(cluster_id):
                        # Collect for batch fallback query
                        jobs_needing_fallback[cluster_id] = current_job
                    else:
                        # Still waiting for log to appear - assume active
                        self.logger.debug(
                            f"Log file for cluster {cluster_id} not yet available "
                            f"(attempt {self._log_missing_counts.get(cluster_id, 0)}/"
                            f"{self._log_missing_threshold})"
                        )
                        yield current_job

            except Exception as e:
                self.logger.warning(
                    f"Error checking job {current_job.job.jobid} with "
                    f"HTCondor Cluster ID {cluster_id}: {e}"
                )
                # Assume job is still running and retry next time
                yield current_job

        # ========== PASS 2: Batch fallback for remaining jobs ==========
        if not jobs_needing_fallback:
            return

        self.logger.info(
            f"Using batch fallback for {len(jobs_needing_fallback)} jobs "
            f"with unavailable log files: {list(jobs_needing_fallback.keys())}"
        )

        # Step 2a: Batch query schedd for all jobs needing fallback
        cluster_ids_to_query = list(jobs_needing_fallback.keys())
        schedd_results = self._batch_query_schedd(cluster_ids_to_query)

        # Process jobs found in schedd
        jobs_not_in_schedd: Dict[int, SubmittedJobInfo] = {}
        for cluster_id, current_job in jobs_needing_fallback.items():
            if cluster_id in schedd_results:
                job_state = schedd_results[cluster_id]
                # Update cached state
                self._job_current_states[cluster_id] = job_state
                result = self._report_and_resolve_job_state(current_job, job_state)
                if result is not None:
                    yield result
            else:
                # Not in schedd - need to check history
                jobs_not_in_schedd[cluster_id] = current_job

        # Step 2b: Batch query history for jobs not in schedd
        if not jobs_not_in_schedd:
            return

        cluster_ids_for_history = list(jobs_not_in_schedd.keys())
        history_results = self._batch_query_history(cluster_ids_for_history)

        # Process jobs found in history
        for cluster_id, current_job in jobs_not_in_schedd.items():
            if cluster_id in history_results:
                job_state = history_results[cluster_id]
                # Update cached state
                self._job_current_states[cluster_id] = job_state
                result = self._report_and_resolve_job_state(current_job, job_state)
                if result is not None:
                    yield result
            else:
                # Job not found anywhere - keep monitoring.
                # This could be a transient error (e.g., schedd was briefly
                # unreachable), so we don't report the job as lost yet.
                self.logger.warning(
                    f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                    f"{cluster_id} could not be found in log files, schedd, or "
                    "history. Keeping job active in case this is transient."
                )
                yield current_job

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.

        if active_jobs:
            schedd = htcondor.Schedd()
            # schedd.act() with a list expects job specs as "clusterID.procID" strings
            job_ids = [f"{current_job.external_jobid}.0" for current_job in active_jobs]
            self.logger.debug(f"Cancelling HTCondor jobs: {job_ids}")
            try:
                schedd.act(htcondor.JobAction.Remove, job_ids)
            except Exception as e:
                self.logger.warning(f"Failed to cancel HTCondor jobs: {e}")
