# Snakemake-HTCondor Executor Plugin: Uses and Features

This folder contains step-by-step examples demonstrating how Snakemake and
HTCondor interact across different computing environments — from basic job execution to non-shared and partially shared filesystem configurations. The executor plugin acts as the bridge between the two systems, translating
Snakemake's workflow logic into HTCondor job submissions.

For more relevant information, visit:

- [Snakemake plugin catalog](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/htcondor.html)
- [Snakemake plugin GitHub repository](https://github.com/htcondor/snakemake-executor-plugin-htcondor)

## Prerequisites

- Access to an HTCondor Access Point (AP) with `condor_submit`
- Python 3.11+
- Conda or Mamba for environment management

## Environment Setup

Create and activate the conda environment:

```bash
# Using mamba (recommended)
mamba env create -f env.yaml
mamba activate snakemake-env

# Or using conda
conda env create -f env.yaml
conda activae snakemake-env
```

## How to Run

Each example contains a profile/ directory with HTCondor executor settings. Navigate to any example folder and run:

```bash
cd examples/<example-name> snakemake --profile profile
```

The example on the `partially-shared filesystem` is not runnable. However, the README.md in the folder has useful information about this system and how you can set up an example to see how it works.

## Examples

| Folder | Description |
| ------ | ----------- |
| [basic-workflow](basic-workflow/) | Simple job submission with basic HTCondor resources |
| [grouped-jobs](grouped-jobs/) | Job grouping with explicit unit resources |
| [job-wrapper](job-wrapper/) | Custom job wrapper for environment setup |
| [non-shared-filesystem](non-shared-filesystem/) | Running on pools without shared filesystems |
| [partial-shared-filesystem](partial-shared-filesystem/) | Using shared filesystem prefixes |

**Notes:** Detailed profile descriptions are available in `basic-workflow`
