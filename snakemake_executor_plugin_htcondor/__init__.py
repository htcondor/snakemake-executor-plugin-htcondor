from dataclasses import dataclass, field
from typing import List, Generator, Optional
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
import traceback
from os.path import join, isabs, relpath, normpath, exists
from os import makedirs, sep
import re
import sys


def is_shared_fs(in_path, shared_prefixes) -> bool:
    """
    Check if the job is on a shared filesystem, as configured using the
    `shared_fs_prefixes` config for the executor
    """
    normalized_prefixes = [join(prefix, "") for prefix in shared_prefixes]
    normalized_path = join(in_path, "")

    return any(normalized_path.startswith(prefix) for prefix in normalized_prefixes)


# Optional:
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
                self._add_file_if_transferable(path, transfer_output_files)

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

        # Commands for matchmaking (GPU)
        for key in [
            "request_gpus",
            "require_gpus",
            "gpus_minimum_capability",
            "gpus_minimum_memory ",
            "gpus_minimum_runtime",
            "cuda_version",
        ]:
            if job.resources.get(key):
                submit_dict[key] = job.resources.get(key)

        # Policy commands
        if job.resources.get("max_retries"):
            submit_dict["max_retries"] = job.resources.get("max_retries")
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

        self.report_job_submission(
            SubmittedJobInfo(job=job, external_jobid=submit_result.cluster())
        )

    async def check_active_jobs(
        self, active_jobs: List[SubmittedJobInfo]
    ) -> Generator[SubmittedJobInfo, None, None]:
        # Check the status of active jobs.

        for current_job in active_jobs:
            async with self.status_rate_limiter:
                # Get the status of the job from HTCondor
                try:
                    schedd = htcondor.Schedd()
                    job_status = schedd.query(
                        constraint=f"ClusterId == {current_job.external_jobid}",
                        projection=[
                            "ExitBySignal",
                            "ExitCode",
                            "ExitSignal",
                            "JobStatus",
                        ],
                    )
                    # Job is not running anymore, look
                    if not job_status:
                        job_status = schedd.history(
                            constraint=f"ClusterId == {current_job.external_jobid}",
                            projection=[
                                "ExitBySignal",
                                "ExitCode",
                                "ExitSignal",
                                "JobStatus",
                            ],
                        )
                        #  Storing the one event from history list
                        if job_status and len(job_status) >= 1:
                            job_status = [job_status[0]]
                        else:
                            raise ValueError(
                                f"No job status found in history for HTCondor job with Cluster ID {current_job.external_jobid}."  # noqa
                            )
                except Exception as e:
                    self.logger.warning(f"Failed to retrieve HTCondor job status: {e}")
                    # Assuming the job is still running and retry next time
                    yield current_job
                self.logger.debug(
                    f"Job {current_job.job.jobid} with HTCondor Cluster ID "
                    f"{current_job.external_jobid} has status: {job_status}"
                )

                # Overview of HTCondor job status:
                status_dict = {
                    "1": "Idle",
                    "2": "Running",
                    "3": "Removed",
                    "4": "Completed",
                    "5": "Held",
                    "6": "Transferring Output",
                    "7": "Suspended",
                }

                # Running/idle jobs
                if job_status[0]["JobStatus"] in [1, 2, 6, 7]:
                    if job_status[0]["JobStatus"] in [7]:
                        self.logger.warning(
                            f"Job {current_job.job.jobid} with "
                            "HTCondor Cluster ID "
                            f"{current_job.external_jobid} is suspended."
                        )
                    yield current_job
                # Completed jobs
                elif job_status[0]["JobStatus"] in [4]:
                    self.logger.debug(
                        f"Check whether Job {current_job.job.jobid} with "
                        "HTCondor Cluster ID "
                        f"{current_job.external_jobid} was successful."
                    )
                    # Check ExitCode
                    if job_status[0]["ExitCode"] == 0:
                        # Job was successful
                        self.logger.debug(
                            f"Report Job {current_job.job.jobid} with "
                            "HTCondor Cluster ID "
                            f"{current_job.external_jobid} success"
                        )
                        self.logger.info(
                            f"Job {current_job.job.jobid} with "
                            "HTCondor Cluster ID "
                            f"{current_job.external_jobid} was successful."
                        )
                        self.report_job_success(current_job)
                    else:
                        self.logger.debug(
                            f"Report Job {current_job.job.jobid} with "
                            "HTCondor Cluster ID "
                            f"{current_job.external_jobid} error"
                        )
                        self.report_job_error(
                            current_job,
                            msg=f"Job {current_job.job.jobid} with "
                            "HTCondor Cluster ID "
                            f"{current_job.external_jobid} has "
                            f" status {status_dict[str(job_status[0]['JobStatus'])]}, "
                            "but failed with "
                            f"ExitCode {job_status[0]['ExitCode']}.",
                        )
                # Errored jobs
                elif job_status[0]["JobStatus"] in [3, 5]:
                    self.report_job_error(
                        current_job,
                        msg=f"Job {current_job.job.jobid} with "
                        "HTCondor Cluster ID "
                        f"{current_job.external_jobid} has "
                        f"status {status_dict[str(job_status[0]['JobStatus'])]}.",
                    )
                else:
                    raise WorkflowError(
                        f"Job {current_job.job.jobid} with "
                        "HTCondor Cluster ID "
                        f"{current_job.external_jobid} has "
                        f"unknown HTCondor job status: {job_status[0]['JobStatus']}"
                    )

    def cancel_jobs(self, active_jobs: List[SubmittedJobInfo]):
        # Cancel all active jobs.
        # This method is called when Snakemake is interrupted.

        if active_jobs:
            schedd = htcondor.Schedd()
            job_ids = [current_job.external_jobid for current_job in active_jobs]
            # For some reason HTCondor requires not the BATCH_NAME but the full JOB_IDS
            job_ids = [f"ClusterId == {x}.0" for x in job_ids]
            self.logger.debug(f"Cancelling HTCondor jobs: {job_ids}")
            try:
                schedd.act(htcondor.JobAction.Remove, job_ids)
            except Exception as e:
                self.logger.warning(f"Failed to cancel HTCondor jobs: {e}")
