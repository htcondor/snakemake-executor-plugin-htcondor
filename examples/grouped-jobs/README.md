# Grouped Resources

A simple example demonstrating how Snakemake aggregates grouped resources in series (linear chain) and parallel (fan-out).

## How Grouped Resources Work

Snakemake rules can be declared in groups that, when paired with an executor like HTCondor, indicate the rules should be run together as a single executor job. For HTCondor, this means that grouped rules/steps are not submitted as individual EP jobs, but as one set of jobs to be run together on a single EP. This is a useful tuning mechanism for smaller jobs, where scheduling overhead may limit throughput.

When Snakemake calculates resources for a group job, it:

1. Determines which jobs run in **parallel** (same layer) vs in **series** (different layers)
1. **Sums** resources across jobs that run in parallel within each layer
1. Takes the **max** across all sequential layers

For a **linear chain** (A → B → C, all in series), this is simply `max(A, B, C)`.

For a **fan-out** pattern where jobs run in parallel, those parallel resources are summed first.

When rules are grouped, Snakemake requires that **string resources have identical values** across all grouped rules, while **numeric resources can be aggregated**.

This creates a problem with HTCondor's `request_memory` and `request_disk` parameters, which are typically specified as strings like `"8GB"` or `"16GB"`.
If you try to group rules with different memory or disk requirements, Snakemake will raise an error because the string values don't match.

The executor-specific resources solve this by accepting **numeric values in MB** that can be aggregated for grouped jobs:

| Resource | Input Unit | Output to HTCondor | Notes |
| ----------------------------- | ---------- | ------------------ | --------------------------------------- |
| `htcondor_request_mem_mb` | MB | `request_memory` | Formatted as "8GB" or "512MB" |
| `htcondor_request_disk_mb` | MB | `request_disk` | Converted to KB for HTCondor's default |
| `htcondor_gpus_min_mem_mb` | MB | `gpus_minimum_memory` | Formatted as "8GB" or "512MB" |

**Note:** For HTCondor resource fields that accept size units (for example, `request_memory` and `request_disk`), values may include suffixes such as K, M, G, or T (optionally followed by B). These units are based on powers of 1024, so each step is 1024 times larger than the previous one (e.g., 1K = 1024, 1M = 1024 x 1024).

See the [Snakemake documentation on Resources and Group Jobs](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#resources-and-group-jobs) for more details.

## How These Examples Work

Since all the rules are labelled as a group `my_group`, all rules will be submitted as one HTCondor's job.

### Fan-Out

For workflows with **fan-out patterns** where multiple rules share the same input but produce different outputs, those rules can run **in parallel**:

From the Snakefile in this folder, our workflow structure is:

```
                    Layer 1: Prepare (Single Job)
                 ┌─────────────────────────────┐
                 │ rule prepare                │
                 │ Input:  data/raw.txt        │
                 │ Output: results/prepared.txt   │
                 │ Mem: 2GB, Disk: 4GB         │
                 └──────────────┬──────────────┘
                                │
                                ▼
                    Layer 2: Analyze (3 PARALLEL Jobs)
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│ ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐ │
│ │ analyze_part_a   │  │ analyze_part_b   │  │ analyze_part_c   │ │
│ │ Input: prepared  │  │ Input: prepared  │  │ Input: prepared  │ │
│ │ Output: part_a   │  │ Output: part_b   │  │ Output: part_c   │ │
│ │ Mem: 4GB each    │  │ Mem: 4GB each    │  │ Mem: 4GB each    │ │
│ └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘ │
│          │                     │                     │           │
└──────────┼─────────────────────┼─────────────────────┼───────────┘
           │                     │                     │
           └─────────────────────┬────────-────────────┘
                                 │
                                 ▼
                    Layer 3: Combine (Single Job)
        ┌─────────────────────────────────────────────────────┐
        │ rule combine                                        │
        │ Input: part_a, part_b, part_c                       │
        │ Output: final_results.txt                           │
        │ Mem: 2GB, Disk: 4GB                                 │
        └─────────────────────────────────────────────────────┘
```

**Parallel execution:** All three `analyze_*` rules take the **same input** (`data/prepared.txt`) but produce **different outputs**. Since they don't depend on each other (no rule needs another's output), Snakemake can run them simultaneously in parallel.

**Important:** In this grouped example, "parallel" means parallel execution within a single grouped HTCondor submission (one cluster job), typically using multiple cores/threads inside that one job allocation. If you want parallelism across multiple HTCondor jobs, split work into multiple groups that can run in the same DAG layer (for example, by assigning different group names) instead of placing everything in one group.

For this fan-out workflow, Snakemake calculates resources by layer:

- **Layer 1** (`prepare`): 2048 MB memory, 4096 MB disk
- **Layer 2** (`analyze_part_a/b/c` in parallel): **sum** the three parallel jobs = 12288 MB memory, 6144 MB disk
- **Layer 3** (`combine`): 2048 MB memory, 4096 MB disk

### Linear Chain

**Linear chain execution:** `Layer 1`, `Layer 2`, and `Layer 3` must be run one after another. The rule `analyze` requires `prepare`'s output and `finalize` requires `analyze`'s outputs, so they **must** run one after another (in series). Snakemake detects this dependency automatically from the input/output declarations.

```
                    Layer 1: Prepare (Single Job)
                 ┌─────────────────────────────┐
                 │ rule prepare                │
                 │ Input:  data/raw.txt        │
                 │ Output: results/prepared.txt│
                 │ Mem: 2GB, Disk: 8GB         │
                 └──────────────┬──────────────┘
                                │
                                ▼
                    Layer 2: Analyze (Single Job)
                 ┌─────────────────────────────┐
                 │ rule analyze                │
                 │ Input:  data/prepared.txt   │
                 │ Output: results/analyzed.txt│
                 │ Mem: 8GB, Disk: 2GB         │
                 └──────────────┬──────────────┘
                                │
                                ▼
                    Layer 3: Finalize (Single Job)
        ┌─────────────────────────────────────────────────────┐
        │ rule finalize                                       │
        │ Input: results/analyzed.txt.                        │
        │ Output: results/linear/final_results.txt            │
        │ Mem: 2GB, Disk: 4GB                                 │
        └─────────────────────────────────────────────────────┘
```

When these rules are grouped as a linear chain (running in series), Snakemake takes the **max**:

- Memory: max(2048, 8192, 2048) = **8192 MB → `request_memory = "8GB"`**
- Disk: max(8192, 4096, 4096) = **8192 MB → `request_disk = "8388608"` (in KB)**

## The critical difference:

- **Linear chain**: Rule A's output → Rule B's input (dependency = **series**)
- **Fan-out**: Multiple rules with same input, different outputs (no interdependency = **parallel**)

**Key Insight:** The parallel layer determines the total resource needs, not the individual jobs. If any parallel layer needs significant resources, the entire grouped job must request enough to handle that layer running simultaneously.

**Precedence:**
If both the explicit unit resource and the standard HTCondor resource are specified for the same job, the explicit unit resource takes precedence:

```
rule example:
    resources:
        request_memory="4GB",              # This will be ignored
        htcondor_request_mem_mb=8192       # This takes precedence (8GB)
```

When this happens, the executor logs a warning to alert you that both resources are set and which one is being used.

### How to Run

```bash
# Run the fanout group example
snakemake --snakefile Snakefile.fanout --profile htcondor_profile

# Run the linear group example
snakemake --snakefile Snakefile.linear --profile htcondor_profile
```

### Expected Outputs

```
# Fan-out example
results/prepared.txt
results/part_a.txt
results/part_b.txt
results/part_c.txt
results/fan-out/final_results.txt

# Linear example
results/prepared.txt
results/analyzed.txt
results/linear/final_results.txt
```
