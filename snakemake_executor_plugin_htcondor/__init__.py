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

import htcondor
import traceback
from os.path import join, basename, abspath, dirname
from os import makedirs, sep
import re
import sys

"""
Given a list of shared prefixes, determine whether a path is on a shared filesystem
"""
def is_shared_fs(in_path, shared_prefixes) -> bool:
    """
    Check if the job is on a shared filesystem, as configured using the
    `shared_fs_prefixes` config for the executor
    """
    normalized_prefixes = [join(prefix, '') for prefix in shared_prefixes]
    normalized_path = join(in_path, '')

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

def get_creds() -> bool:
    """
    Get credentials to avoid job going on hold due to lack of credentials
    """
    # thanks @tlmiller
    local_provider_name = htcondor.param.get("LOCAL_CREDMON_PROVIDER_NAME")
    if local_provider_name is None:
        return False
    magic = ("LOCAL:%s" % local_provider_name).encode()
    credd = htcondor.Credd()
    credd.add_user_cred(htcondor.CredTypes.Kerberos, magic)
    return True


class CredsError(Exception):
    pass

# Required:
# Implementation of your executor
class Executor(RemoteExecutor):
    def __post_init__(self):
        # access workflow
        self.workflow
        # access executor specific settings
        self.workflow.executor_settings

        # jobDir: Directory where the job will tore log, output and error files.
        self.jobDir = self.workflow.executor_settings.jobdir


    def run_job(self, job: JobExecutorInterface):
        # Submitting job to HTCondor

        # Creating directory to store log, output and error files
        makedirs(self.jobDir, exist_ok=True)

        if (jobWrapper := job.resources.get("job_wrapper")):
            job_exec = jobWrapper
            # The wrapper script will take as input all snakemake arguments, so we assume
            # it contains something like `snakemake $@`
            job_args = self.format_job_exec(job).removeprefix("python -m snakemake ")
        else:
            job_exec = self.get_python_executable()
            job_args = self.format_job_exec(job).removeprefix(job_exec + " ")

        # HTCondor cannot handle single quotes
        if "'" in job_args:
            job_args = job_args.replace("'", "")
            self.logger.warning(
                "The job argument contains a single quote. "
                "Removing it to avoid issues with HTCondor."
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
        supported_universes = ["vanilla", "docker", "container", "scheduler", "local", "parallel", "grid", "java", "vm"]
        if (universe := job.resources.get("universe")):
            if universe not in supported_universes:
                raise WorkflowError(
                    f"The universe {universe} is not supported by HTCondor.",
                    "See the HTCondor reference manual for a list of supported universes."
                )

            submit_dict["universe"] = universe

            # Check for container image requirement
            container_image = job.resources.get("container_image")
            if universe in ["docker", "container"] and not container_image:
                raise WorkflowError("A container image must be specified when using the docker or container universe.")
            elif container_image:
                submit_dict["container_image"] = container_image

        # If we're not using a shared filesystem, we need to setup transfers
        # for any job wrapper, config files, input files, etc
        if not self.workflow.storage_settings.shared_fs_usage:
            submit_dict["should_transfer_files"] = "YES"
            submit_dict["when_to_transfer_output"] = "ON_EXIT"

            # Grab our list of shared filesystem prefixes. We'll use these to determine
            # which files HTCondor has to transfer to the EP, and which might already be accessible
            shared_fs_prefixes = job.resources.get("shared_fs_prefixes", "")
            shared_fs_prefixes = [item.strip() for item in shared_fs_prefixes.split(',') if item.strip()]
            self.logger.debug(f"Shared filesystem prefixes: {shared_fs_prefixes}")
            # Initialize the transfer input files list
            transfer_input_files = []

            # Add the snakefile to the transfer list
            snakefile = self.get_snakefile()
            if not is_shared_fs(snakefile, shared_fs_prefixes):
                transfer_input_files.append(str(snakefile))

            if job.input:
                inputs = [input for input in job.input]

                # When snakemake passes its input args to the executable, it does so using the path relative
                # to the specified input directory, e.g. `input/foo/bar`. Therefore, we either need to transfer
                # the inputs in a way that preserves paths relative to the input, or we need to transfer the entire
                # input directory. By default, we'll preserve relative paths, but if the user specifies otherwise,
                # we'll transfer the entire input directory.
                if not job.resources.get("preserve_relative_paths", True):
                    inputs = {path.split(sep)[0] for path in inputs}

                for path in inputs:
                    if path and not is_shared_fs(path, shared_fs_prefixes):
                        transfer_input_files.append(str(path))

            if self.workflow.configfiles:
                # Note that when we transfer the config file(s), we'll pass Condor an absolute path, but we need to
                # modify the job args to use only the file name(s) when execution begins, because the configfile(s)
                # will be accessed from the job's scratch dir.
                config_fnames = []
                for fpath in self.workflow.configfiles:
                    fname = basename(fpath)
                    config_fnames.append(fname)
                    if fpath and not is_shared_fs(fpath, shared_fs_prefixes):
                        transfer_input_files.append(str(fpath))
                config_arg = " ".join(config_fnames)

                configfiles_pattern = r"--configfiles .*?(?=( --|$))"
                submit_dict["arguments"] = re.sub(configfiles_pattern, f"--configfiles {config_arg}", submit_dict["arguments"])

            if transfer_input_files:
                self.logger.debug(f"Transfer input files: {transfer_input_files}")
                submit_dict["transfer_input_files"] = ", ".join(sorted(transfer_input_files))

            if job.output:
                transfer_output_files = []
                # For outputs, we only care about the parent directory, which we'll tell
                # HTCondor to transfer back to the AP.
                top_most_output_directories = {path.split(sep)[0] for path in job.output}
                for path in top_most_output_directories:
                    if path and not is_shared_fs(path, shared_fs_prefixes):
                        transfer_output_files.append(str(path))
                
                if transfer_output_files:
                    self.logger.debug(f"Transfer output files: {transfer_output_files}")
                    submit_dict["transfer_output_files"] = ", ".join(sorted(transfer_output_files))

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
                submit_dict[classad_key] = job.resources.get(key)

        # HTCondor submit description
        self.logger.debug(f"HTCondor submit subscription: {submit_dict}")
        submit_description = htcondor.Submit(submit_dict)

        # Client for HTCondor Schedduler
        schedd = htcondor.Schedd()

        # Submitting job to HTCondor
        try:
            have_creds = get_creds()
            if not have_creds:
                raise CredsError("Credentials not found for this workflow")
            submit_result = schedd.submit(submit_description)
        except CredsError as ce:
            traceback.print_exc()
            print(f"CredsError occurred: {ce}")
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
                        #  Storing the one event from HistoryIterator to list
                        job_status = [next(job_status)]
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
