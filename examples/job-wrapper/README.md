# Job Wrapper

A simple example demonstrating how Job Wrappers are used with HTCondor executor.

## Why Job Wrappers Are Needed

HTCondor executes jobs in sandboxed environments on remote compute nodes. These nodes don't have access to the user's home directory, which breaks Snakemake's default behavior of writing cache files to `$HOME`.

The job wrapper solves this by:

1. Setting up the proper environment (`HOME=$(pwd)`)
1. Forwarding Snakemake arguments from the access point to the execution point
1. Ensuring all job artifacts stay within the HTCondor scratch directory

### Some common scenarios where job wrapper script is needed:

- The execution point (EP) does not have the right environment to execute and needs modules to be loaded.
- You need to activate a conda environment before anything else runs
- `$HOME` is not set or is pointed to somewhere broken

### Some common scenarios where job wrapper script is not needed:

- Your workflow uses containers
- You use a shared file-system where the execution point (EP) has the same environment as the access point (AP)
- All dependencies are transferred via HTCondor's file transferring system

### Illustration

```text
[HTCondor Worker Node]
        │
        ▼
┌─────────────────────────┐
│   job_wrapper.sh        │  ← YOU write this (when needed)
│   - module load conda   │    Sets up the environment
│   - source activate env │
│   - export HOME=$(pwd)  │
│         │               │
│         ▼               │
│  [snakemake_job.sh]     │  ← Snakemake always generates this
│  - run rule `foo`       │    Runs the actual rule
│  - with input X         │
│  - producing output Y   │
└─────────────────────────┘
```

**Notes:**
Snakemake automatically generates a job script for each rule execution. The job wrapper is not a replacement for this. Instead, it is to ensure that the worker node environment is correctly configured before Snakemake's auto-generated script runs.

## How This Example Works

This example runs a simple two-step pipeline that processes two samples (`sample1.txt`, `sample2.txt`) through a series of rules:

1. **`make_intermediary`** — Processes each input file, appending `"foo"`
   to produce `results/intermediary_{sample}.txt`
1. **`make_output`** — Processes each intermediary file, appending `"bar"`
   to produce the final `results/output_{sample}.txt`

The pipeline exists purely to demonstrate the job wrapper mechanic — the
rules themselves do minimal work so the focus stays on how `wrapper.sh`
sets up the environment before each rule executes on the worker node.

Refer to `wrapper.sh` to see how a simple wrapper script is setup.

### How to Run

```bash
snakemake --profile profile
```

### Expected Outputs

```
results/output_sample1.txt
results/output_sample2.txt
```
