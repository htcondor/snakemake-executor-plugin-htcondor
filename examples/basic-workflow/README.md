# Basic Workflow and Resources

A simple example demonstrating basic HTCondor job submission with resource requests.

The following [submit description file commands](https://htcondor.readthedocs.io/en/latest/man-pages/condor_submit.html) are supported (add them as user-defined resources):
| Basic | Matchmaking | Matchmaking (GPU) | Policy |
| ---------------------------------- | ---------------- | ------------------------- | -------------------------- |
| `getenv` | `rank` | `request_gpus` | `max_retries` |
| `environment` | `request_disk` | `require_gpus` | `allowed_execute_duration` |
| `input` | `request_memory` | `gpus_minimum_capability` | `allowed_job_duration` |
| `max_materialize` | `requirements` | `gpus_minimum_memory` | `retry_until` |
| `max_idle` | `classad_<foo>`**| `gpus_minimum_runtime` | |
| `job_wrapper`\* | | `cuda_version` | |
| `universe` | | | |
| `htcondor_transfer_input_files`**\* | | | |
| `htcondor_transfer_output_files`\*\*\*| | | |

Additionally, the following **executor-specific resources** are available with explicit units (see [Resources with Explicit Units](#resources-with-explicit-units) below):
| Resource | Description | Equivalent HTCondor Command |
| ----------------------------- | ------------------------------------- | --------------------------- |
| `htcondor_request_mem_mb` | Request memory in MB | `request_memory` |
| `htcondor_request_disk_mb` | Request disk in MB | `request_disk` |
| `htcondor_gpus_min_mem_mb` | GPU minimum memory in MB | `gpus_minimum_memory` |

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

## How This Example Works

Resources are set as **default-resources** in `profile/config.yaml`

- `request_memory` — memory requested from HTCondor (default: 1GB)
- `request_disk` — disk space requested from HTCondor (default: 4GB)
- `threads` — maps to HTCondor's `request_cpus` (default: 1)

These defaults apply to every rule in the workflow. For jobs with different
requirements, you can override them per-rule using `resources:`

### How to Run

```bash
snakemake --profile profile
```

### Expected Output

```
results/sample1.txt
results/sample2.txt
```
