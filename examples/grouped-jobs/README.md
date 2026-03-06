# Grouped Resources

A simple example demonstrating how Snakemake aggregates grouped resources in series (linear chain) and parallel (fan-out).

## How Grouped Resources Work

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

See the [Snakemake documentation on Resources and Group Jobs](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#resources-and-group-jobs) for more details.

## How This Example Works

Since all the rules are labelled as a group `my_group`, all rules will be submitted as one HTCondor's job.

### Linear and Fan-Out Example

For workflows with **fan-out patterns** where multiple rules share the same input but produce different outputs, those rules can run **in parallel**:

From the Snakefile in this folder, our workflow structure is:

```
                    Layer 1: Prepare (Single Job)
                 ┌─────────────────────────────┐
                 │ rule prepare                │
                 │ Input:  data/raw.txt        │
                 │ Output: data/prepared.txt   │
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

For this fan-out workflow, Snakemake calculates resources by layer:

- **Layer 1** (`prepare`): 2048 MB memory, 4096 MB disk
- **Layer 2** (`analyze_part_a/b/c` in parallel): **sum** the three parallel jobs = 12288 MB memory, 6144 MB disk
- **Layer 3** (`combine`): 2048 MB memory, 4096 MB disk

**Linear chain execution:** `Layer 1`, `Layer 2`, and `Layer 3` must be run one after another. All three `analyze_*` requires `prepare`'s output and `combine` requires all `analyze_*`'s outputs, so they **must** run one after another (in series). Snakemake detects this dependency automatically from the input/output declarations.

When these rules are grouped as a linear chain (running in series), Snakemake takes the **max**:

- Memory: max(2048, 12288, 2048) = **12288 MB → `request_memory = "12GB"`**
- Disk: max(4096, 6144, 4096) = **6144 MB → `request_disk = "6291456"` (in KB)**

**The critical difference:**

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
snakemake --profile profile
```

### Expected Outputs

```
results/prepared.txt
results/part_a.txt
results/part_b.txt
results/part_c.txt
results/final_results.txt
```
