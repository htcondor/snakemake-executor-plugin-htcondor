# Basic Workflow and Resources

A simple example demonstrating basic HTCondor job submission with resource requests.

Find out more about plugin's resources and configuration under ["Basic"](https://github.com/htcondor/snakemake-executor-plugin-htcondor/blob/main/README.md) from the official executor's GitHub repository.

## How This Example Works

This example runs a simple Snakemake's rule called `process` that transforms the input contents (i.e., `sample1.txt` and `sample2.txt`) to outputs by replacing the string `input` in each `.txt` with the string `output`.

### Snakefile's Structure

- Line 1: `SAMPLES = ["sample1", "sample2"]` defines the set of sample names the workflow should run on.
- Line 3: `rule all` specifies target rule, which is what our final goals (output files) are for running the entire workflow
- Line 7: `rule process` does the actual processing of our data, specifying the input files, output files, and the shell command to generate the outputs from the inputs. This is the standard specifications for a rule in Snakemake.

### Log files

**HTCondor**

In `.snakemake`, you can find `htcondor` directory that contains `.err`, `.log`, and `.out` files. Each rule in a Snakefile generally produces a set of files (`.err`, `.log`, `.out`), with some exceptions such as that of grouped job.

- `.log` records significant events that occur during the lifetime of all jobs within a cluster. This is important to help us understand what happened to a job and to diagnose if any issues occurred while the job was running/trying to run.
- `.out` is the standard output, which records "normal" messages.
- `.err` is the standard error. which records error messages or additional information.

All the outputs that would normally have been printed in the console are instead redirected to the appropriate `.out` or `.err` file.

**Snakemake**

In addition to the HTCondor log, we also have the Snakemake's log file which can be found in `log` directory under `.snakemake`. The contents in the file is also printed to `Terminal`. This file contains the setups, job execution details and progress, warnings/errors, final status, and more. These are useful for understanding, troubleshooting, and inspecting the workflow that is being run.

### Plugin's Resources Used

Resources are set as **default-resources** in `htcondor_profile/config.yaml`

- `request_memory` — memory requested from HTCondor (default: 1GB)
- `request_disk` — disk space requested from HTCondor (default: 4GB)
- `threads` — maps to HTCondor's `request_cpus` (default: 1)

For HTCondor resource fields that accept size units (for example, `request_memory` and `request_disk`), values may include suffixes such as K, M, G, or T (optionally followed by B). These units are based on powers of 1024, so each step is 1024 times larger than the previous one (e.g., 1K = 1024, 1M = 1024 x 1024).

These defaults apply to every rule in the workflow. For jobs with different
requirements, you can override them per-rule using `resources:`

### How to Run

```bash
snakemake --profile htcondor_profile
```

### Expected Output

```
results/sample1.txt
results/sample2.txt
```
