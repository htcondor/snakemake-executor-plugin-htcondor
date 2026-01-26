# Snakemake executor plugin: HTCondor

A Snakemake executor plugin for submitting jobs with HTCondor.

The [HTCondor Software Suite (HTCSS)](https://htcondor.org/) is a software system that creates a High-Throughput Computing (HTC) environment. This environment might be a single cluster, a set of related clusters on a campus, cloud resources, or national or international federations of computers.

For the official documentation, visit [Snakemake plugin catalog](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/htcondor.html).

# Plugin Configuration

## Basic

- This plugin currently supports job submission with a shared file system, with experimental support for pools without shared filesystems (such as the OSPool).
- Error messages, the output of stdout and log files are written to `htcondor-jobdir` (see in the usage section above).
- The job directive `threads` is used to set `request_cpu` command for HTCondor.
- For the job status, this plugin reports the values of the [job ClassAd Attribute](https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html) `JobStatus`.
- To determine whether a job was successful, this plugin relies on `htcondor.Schedd.history` (see [API reference](https://htcondor.readthedocs.io/en/latest/apis/python-bindings/api/htcondor.html)) and checks the values of the [job ClassAd Attribute](https://htcondor.readthedocs.io/en/latest/classad-attributes/job-classad-attributes.html) `ExitCode`.

The following [submit description file commands](https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html) are supported (add them as user-defined resources):
| Basic                              | Matchmaking      | Matchmaking (GPU)         | Policy                     |
| ---------------------------------- | ---------------- | ------------------------- | -------------------------- |
| `getenv`                           | `rank`           | `request_gpus`            | `max_retries`              |
| `environment`                      | `request_disk`   | `require_gpus`            | `allowed_execute_duration` |
| `input`                            | `request_memory` | `gpus_minimum_capability` | `allowed_job_duration`     |
| `max_materialize`                  | `requirements`   | `gpus_minimum_memory`     | `retry_until`              |
| `max_idle`                         | `classad_<foo>`**| `gpus_minimum_runtime`    |                            |
| `job_wrapper`*                     |                  | `cuda_version`            |                            |
| `universe`                         |                  |                           |                            |
| `htcondor_transfer_input_files`*** |                  |                           |                            |
| `htcondor_transfer_output_files`***|                  |                           |                            |

Additionally, the following **executor-specific resources** are available with explicit units (see [Resources with Explicit Units](#resources-with-explicit-units) below):
| Resource                      | Description                           | Equivalent HTCondor Command |
| ----------------------------- | ------------------------------------- | --------------------------- |
| `htcondor_request_mem_mb`     | Request memory in MB                  | `request_memory`            |
| `htcondor_request_disk_mb`    | Request disk in MB                    | `request_disk`              |
| `htcondor_gpus_min_mem_mb`    | GPU minimum memory in MB              | `gpus_minimum_memory`       |

\* A custom-defined `job_wrapper` resource will be used as the HTCondor executable for the job. It can be used for environment setup, but must pass all arguments to snakemake on the EP. For example, the following is a valid bash script wrapper:
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

\*\*\* Additional input or output files for transfer can be specified using `htcondor_transfer_input_files` and `htcondor_transfer_output_files` resources.
These are useful for transferring files that aren't part of the rule's `input:`/`output:` directives (e.g., helper scripts, configuration files, logs, intermediate results).
Supports both string (comma-separated) and list formats. Wildcards (e.g., `{sample}`) are expanded for individual jobs, but **not** for grouped jobs since the resources are defined at the group level.
Files on shared filesystem prefixes are automatically excluded from transfer.

Example usage:
```python
rule process:
    input: "data/{sample}.txt"
    output: "results/{sample}.out"
    resources:
        htcondor_transfer_input_files="scripts/helpers.py,config/params.yaml",
        htcondor_transfer_output_files="logs/{sample}.log"
    script: "scripts/process.py"
```

## Resources with Explicit Units

Snakemake's [rule grouping](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#group-jobs) feature allows multiple rules to be bundled and submitted as a single job.
However, when rules are grouped, Snakemake requires that **string resources have identical values** across all grouped rules, while **numeric resources can be aggregated**.

This creates a problem with HTCondor's `request_memory` and `request_disk` parameters, which are typically specified as strings like `"8GB"` or `"16GB"`.
If you try to group rules with different memory or disk requirements, Snakemake will raise an error because the string values don't match.

The executor-specific resources solve this by accepting **numeric values in MB** that can be aggregated for grouped jobs:

| Resource                      | Input Unit | Output to HTCondor | Notes                                   |
| ----------------------------- | ---------- | ------------------ | --------------------------------------- |
| `htcondor_request_mem_mb`     | MB         | `request_memory`   | Formatted as "8GB" or "512MB"           |
| `htcondor_request_disk_mb`    | MB         | `request_disk`     | Converted to KB for HTCondor's default  |
| `htcondor_gpus_min_mem_mb`    | MB         | `gpus_minimum_memory` | Formatted as "8GB" or "512MB"        |

### How Snakemake Aggregates Grouped Resources

When Snakemake calculates resources for a group job, it:
1. Determines which jobs run in **parallel** (same layer) vs in **series** (different layers)
2. **Sums** resources across jobs that run in parallel within each layer
3. Takes the **max** across all sequential layers

For a **linear chain** (A → B → C, all in series), this is simply `max(A, B, C)`.
For a **fan-out** pattern where jobs run in parallel, those parallel resources are summed first.

See the [Snakemake documentation on Resources and Group Jobs](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#resources-and-group-jobs) for more details.

### Linear Chain Example (Series Execution)

In a **linear chain**, rules depend on each other's outputs, forcing them to run sequentially:

```python
# Linear chain: step_one → step_two (dependency creates serial execution)
rule step_one:
    input: "data/{sample}.txt"
    output: "intermediate/{sample}.tmp"  # This output becomes step_two's input ← KEY!
    group: "my_group"
    resources:
        htcondor_request_mem_mb=4096,    # 4GB
        htcondor_request_disk_mb=8192    # 8GB
    shell: "process_step1 {input} > {output}"

rule step_two:
    input: "intermediate/{sample}.tmp"   # Depends on step_one's output ← KEY!
    output: "results/{sample}.out"
    group: "my_group"
    resources:
        htcondor_request_mem_mb=8192,    # 8GB
        htcondor_request_disk_mb=4096    # 4GB
    shell: "process_step2 {input} > {output}"
```

**Why this is a linear chain:** `step_two` requires `step_one`'s output, so they **must** run one after another (in series). Snakemake detects this dependency automatically from the input/output declarations.

When these rules are grouped as a linear chain (running in series), Snakemake takes the **max**:
- Memory: max(4096, 8192) = 8192 MB → HTCondor receives `request_memory = "8GB"`
- Disk: max(8192, 4096) = 8192 MB → HTCondor receives `request_disk = "8388608"` (in KB)

### Fan-Out Example (Parallel Jobs)

For workflows with **fan-out patterns** where multiple rules share the same input but produce different outputs, those rules can run **in parallel**:

```python
# Layer 1: Single job
rule prepare:
    input: "data/raw.txt"
    output: "data/prepared.txt"  # This output is shared by all analyze_* rules ← KEY!
    group: "my_group"
    resources:
        htcondor_request_mem_mb=2048,    # 2GB
        htcondor_request_disk_mb=4096    # 4GB
    shell: "prepare_data {input} > {output}"

# Layer 2: Three jobs run in PARALLEL
rule analyze_part_a:
    input: "data/prepared.txt"   # Same input ← KEY!
    output: "results/part_a.txt"  # Different output ← KEY!
    group: "my_group"
    resources:
        htcondor_request_mem_mb=4096,    # 4GB each
        htcondor_request_disk_mb=2048    # 2GB each
    shell: "analyze_a {input} > {output}"

rule analyze_part_b:
    input: "data/prepared.txt"   # Same input ← KEY!
    output: "results/part_b.txt"  # Different output ← KEY!
    group: "my_group"
    resources:
        htcondor_request_mem_mb=4096,    # 4GB each
        htcondor_request_disk_mb=2048    # 2GB each
    shell: "analyze_b {input} > {output}"

rule analyze_part_c:
    input: "data/prepared.txt"   # Same input ← KEY!
    output: "results/part_c.txt"  # Different output ← KEY!
    group: "my_group"
    resources:
        htcondor_request_mem_mb=4096,    # 4GB each
        htcondor_request_disk_mb=2048    # 2GB each
    shell: "analyze_c {input} > {output}"

# Layer 3: Combine results
rule combine:
    input: "results/part_a.txt", "results/part_b.txt", "results/part_c.txt"
    output: "final_results.txt"
    group: "my_group"
    resources:
        htcondor_request_mem_mb=2048,    # 2GB
        htcondor_request_disk_mb=4096    # 4GB
    shell: "combine_results {input} > {output}"
```

**Why this creates parallel execution:** All three `analyze_*` rules take the **same input** (`data/prepared.txt`) but produce **different outputs**. Since they don't depend on each other (no rule needs another's output), Snakemake can run them simultaneously in parallel.

**The critical difference from the linear chain:**
- **Linear chain**: Rule A's output → Rule B's input (dependency = **series**)
- **Fan-out**: Multiple rules with same input, different outputs (no interdependency = **parallel**)

For this fan-out workflow, Snakemake calculates resources by layer:

- **Layer 1** (`prepare`): 2048 MB memory, 4096 MB disk
- **Layer 2** (`analyze_part_a/b/c` in parallel): **sum** the three parallel jobs = 12288 MB memory, 6144 MB disk
- **Layer 3** (`combine`): 2048 MB memory, 4096 MB disk

Then takes **max** across all layers:
- Memory: max(2048, 12288, 2048) = **12288 MB → `request_memory = "12GB"`**
- Disk: max(4096, 6144, 4096) = **6144 MB → `request_disk = "6291456"` (in KB)**

**Key Insight:** The parallel layer determines the total resource needs, not the individual jobs. If any parallel layer needs significant resources, the entire grouped job must request enough to handle that layer running simultaneously.

### Precedence

If both the explicit unit resource and the standard HTCondor resource are specified for the same job, the explicit unit resource takes precedence:

```python
rule example:
    resources:
        request_memory="4GB",              # This will be ignored
        htcondor_request_mem_mb=8192       # This takes precedence (8GB)
```

When this happens, the executor logs a warning to alert you that both resources are set and which one is being used.

## Jobs Without Shared Filesystems

Support for jobs without a shared filesystem is preliminary and experimental.

As such, it currently imposes limitations on the structure of your data on the Access Point (AP), as well as the use of a job wrapper (you can use the previous example).
It is also highly recommended that you use containers to bring a runtime execution environment along with the job, which at a minimum must contain Python and Snakemake.

To run a workflow across Execution Points (EPs) that don't share a filesystem, modify the snakemake invocation with `--shared-fs-usage none`:
```bash
snakemake --executor htcondor --shared-fs-usage none
```
Doing so will invoke the HTCondor file transfer mechanism to move files from the AP to the EPs responsible for running each job.

It is highly recommended that you use containers to bring a runtime execution environment along with the job, which at a minimum must contain Python and Snakemake.

There is currently a limitation that files being transferred (e.g. Snakefile, config files, input data) must have the same scope on both the AP/EP, and in
any Snakefile/Config file declarations. That is, if your configuration yaml file specifies an input directory called `my_data/`, the directory must be at
the same location the job is submitted from, and it must arrive at the EP as `my_data/`. Because of this, a configured input directory like `../../my_data/`
cannot work, because Snakemake at the EP will attempt to find `../../my_data` on its own filesystem where the directory will have been flattened to
`my_data/`.

### Partially Shared Filesystems

In some computing environments, the Access Point and Execution Points may share certain filesystem paths (e.g., `/staging`) while other paths are local to each machine.
The executor can be configured to recognize these shared paths and avoid transferring files that are already accessible on both the AP and EPs.

To configure shared filesystem prefixes, use the `--htcondor-shared-fs-prefixes` command-line option or the `htcondor-shared-fs-prefixes` setting in your executor profile:

```bash
snakemake --executor htcondor --shared-fs-usage none --htcondor-shared-fs-prefixes "/staging,/shared"
```

Or in your profile configuration:

```yaml
executor: htcondor
shared-fs-usage: none
htcondor-shared-fs-prefixes: "/staging,/shared"
```

When prefixes are provided to `--htcondor-shared-fs-prefixes`, any input/output files under those paths will **not** be transferred by HTCondor, because the executor assumes these paths are accessible at both the AP and the EP.
All files not found under these paths will be transferred by HTCondor as usual.

**Example use case:**
If your computing environment has `/staging` mounted on both AP and EPs, but your Snakefile and local input files are in `/home/user/workflow`:
- Set `--htcondor-shared-fs-prefixes "/staging"`
- Files in `/staging/data/` won't be transferred (already accessible)
- Files in `input/` or `/home/user/` will be transferred to EPs

**Important notes:**
- Shared filesystem prefixes are only relevant when using `--shared-fs-usage none`
- Multiple prefixes can be specified as a comma-separated list
- Prefixes should be absolute paths

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
htcondor-jobdir: /path/to/MyHTCondorWorkflow/logs
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

### Example with Partially Shared Filesystem

If your HTCondor cluster/environment has a partially shared filesystem mounted between the AP and EPs, but your workflow directory is local to the AP:

**Directory structure:**
```
/home/user/MyHTCondorWorkflow/    # Local to AP
    ├── Snakefile
    ├── my_config.yaml
    ├── runtime_container.sif
    ├── wrapper.sh
    ├── local_input/
    │   └── input-file1.txt
    └── my_profile/
        └── config.yaml

/staging/shared_data/             # Shared between AP and EPs
    └── input-file2.txt
```

Configure `my_profile/config.yaml` as:

```yaml
jobs: 30
executor: htcondor
configfile: my_config.yaml
shared-fs-usage: none
htcondor-jobdir: logs
htcondor-shared-fs-prefixes: "/staging"
default-resources:
  job_wrapper: "wrapper.sh"
  container_image: "runtime_container.sif"
  universe: "container"
  request_disk: "16GB"
  request_memory: "8GB"
```

In this setup:
- Files in `local_input/` (e.g., `input-file1.txt`) will be transferred by HTCondor
- Files in `/staging/shared_data/` (e.g., `input-file2.txt`) will be accessed directly without transfer
- The Snakefile and config files will be transferred
- Output files written to local paths (e.g., `output/`) will be transferred back
- Output files written to `/staging/results/` will be written directly without transfer back
