# Plugin Configuration

## Basic

- This plugin currently supports job submission with a shared file system, with experimental support for pools without shared filesystems (such as the OSPool).
- Error messages, the output of stdout and log files are written to `htcondor-jobdir` (see in the usage section above).
- The job directive `threads` is used to set `request_cpu` command for HTCondor.
- For the job status, this plugin reports the values of the [job ClassAd Attribute](https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html) `JobStatus`.
- To determine whether a job was successful, this plugin relies on `htcondor.Schedd.history` (see [API reference](https://htcondor.readthedocs.io/en/latest/apis/python-bindings/api/htcondor.html)) and checks the values of the [job ClassAd Attribute](https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html) `ExitCode`.


The following [submit description file commands](https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html) are supported (add them as user-defined resources):
| Basic             | Matchmaking      | Matchmaking (GPU)         | Policy                     |
| ----------------- | ---------------- | ------------------------- | -------------------------- |
| `getenv`          | `rank`           | `request_gpus`            | `max_retries`              |
| `environment`     | `request_disk`   | `require_gpus`            | `allowed_execute_duration` |
| `input`           | `request_memory` | `gpus_minimum_capability` | `allowed_job_duration`     |
| `max_materialize` | `requirements`   | `gpus_minimum_memory`     | `retry_until`              |
| `max_idle`        | `classad_<foo>`**| `gpus_minimum_runtime`    |                            |
| `job_wrapper`*    |                  | `cuda_version`            |                            |
| `universe`        |                  |                           |                            |


\* A custom-defined `job_wrapper` resource will be used as the HTCondor executable for the job. It can be used for environment setup, but must pass all arguments
  to snakemake on the EP. For example, the following is a valid bash script wrapper:
```bash
#!/bin/bash

# Fail early if there's an issue
set -e

# When .cache files are created, they need to know where HOME is to write there.
# In this case, that should be the HTCondor scratch dir the job is executing in.
export HOME=$(pwd)

# Pass any arguments to Snakemake
snakemake "$@"
```

\*\* Custom ClassAds can be defined using the `classad_` prefix as a custom job resource. For example, to define the ClassAd `+MyClassAd`, define `classad_MyClassAd` in
the job's resources.

## Jobs Without Shared Filesystems

Support for jobs without a shared filesystem is preliminary and experimental.

As such, it currently imposes limitations on the structure of your data on the Access Point (AP), as well as the use of a job wrapper (you can use the previous example).
It is also highly recommended that you use containers to bring a runtime execution environment along with the job, which at a minimum must contain Python and Snakemake.

To run a workflow across Execution Points (EPs) that don't share a filesystem, modify the snakemake invocation with `--shared-fs-usage none`:
```bash
snakemake --executor htcondor --shared-fs-usage none
```
Doing so will invoke the HTCondor file transfer mechanism to move files from the AP to the EPs responsible for running each job.

There is currently a limitation that files being transferred (e.g. Snakefile, config files, input data) must have the same scope on both the AP/EP, and in
any Snakefile/Config file declarations. That is, if your configuration yaml file specifies an input directory called `my_data/`, the directory must be at
the same location the job is submitted from, and it must arrive at the EP as `my_data/`. Because of this, a configured input directory like `../../my_data/`
cannot work, because Snakemake at the EP will attempt to find `../../my_data` on its own filesystem where the directory will have been flattened to
`my_data/`.

### Example of Non Shared Filesystem Usage

Given a directory structure on the AP such as:
```
.
└── MyHTCondorWorkflow/
    ├── Snakefile
    ├── my_config.yaml
    ├── runtime_container.sif
    ├── wrapper.sh
    ├── my_input/
    │   ├── file1.txt
    │   └── file2.txt
    ├── my_profile/
    │   └── config.yaml
    └── logs/
```

with `wrapper.sh` as:
```bash
#!/bin/bash
set -e
export HOME=$(pwd)
snakemake "$@"
```
and where `runtime_container.sif` is an apptainer image containing Snakemake and any additional software needed by your job, you can setup
`profile/config.yaml` with something like:
```yaml
# Run at most 30 concurrent jobs
jobs: 30
executor: htcondor
configfile: my_config.yaml
shared-fs-usage: none
htcondor-job-dir: /path/to/MyHTCondorWorkflow/logs
default-resources:
  job_wrapper: "wrapper.sh"
  container_image: "runtime_container.sif"
  universe: "container"
  request_disk: "16GB"
  request_memory: "8GB"
```

Now, if `my_config.yaml` declares `my_input/` as its input data, then the following snakemake command should start the workflow from the AP,
sending each job to a remote EP:
```bash
snakemake --profile my_profile
```
with HTCondor job logs being placed in `MyHTCondorWorkflow/logs/`

Note that exiting the terminal running the Snakemake workflow will currently abort all jobs.
