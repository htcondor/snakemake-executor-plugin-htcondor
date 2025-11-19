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
from os.path import join, basename, abspath, dirname
from os import makedirs, sep
import re


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
            job_exec = basename(jobWrapper)
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

            # Add the snakefile to the transfer list
            submit_dict["transfer_input_files"] = abspath(self.get_snakefile())

            if job.input:
                # When snakemake passes its input args to the executable, it does so using the path relative
                # to the specified input directory, e.g. `input/foo/bar`, so we need to transfer the top-most
                # input directories the will contain any subdirectories/files needed by the job.
                top_most_input_directories = {path.split(sep)[0] for path in job.input}
                submit_dict["transfer_input_files"] += ", " + ", ".join(sorted(top_most_input_directories))

            if self.workflow.configfiles:
                # Note that when we transfer the config file(s), we'll pass Condor an absolute path, but we need to
                # modify the job args to use only the file name(s) when execution begins, because the configfile(s)
                # will be accessed from the job's scratch dir.
                config_fnames = []
                for fpath in self.workflow.configfiles:
                    fname = basename(fpath)
                    config_fnames.append(fname)
                config_arg = " ".join(config_fnames)

                configfiles_pattern = r"--configfiles .*?(?=( --|$))"
                submit_dict["arguments"] = re.sub(configfiles_pattern, f"--configfiles {config_arg}", submit_dict["arguments"])

                submit_dict["transfer_input_files"] += ", " + ", ".join(str(path) for path in self.workflow.configfiles)

            if job.output:
                # For outputs, we only care about the parent directory, which we'll tell
                # HTCondor to transfer back to the AP.
                top_most_output_directories = {path.split(sep)[0] for path in job.output}
                submit_dict["transfer_output_files"] = ", ".join(sorted(top_most_output_directories))

        # Basic commands
        if job.resources.get("getenv"):
            submit_dict["getenv"] = job.resources.get("getenv")
        else:
            submit_dict["getenv"] = False

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
                classad_key =  "+" + key.removeprefix("classad_")
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
                            raise ValueError(f"No job status found in history for HTCondor job with Cluster ID {current_job.external_jobid}.")
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
