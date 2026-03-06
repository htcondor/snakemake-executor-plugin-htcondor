# Partially Shared Filesystem

An illustrative example demonstrating how to configure the HTCondor executor for a partially shared filesystem environment.

**Note:** Unlike other examples, this example is not directly runnable. A partially-shared filesystem requires real cluster infrastructure - shared mount points like `/staging` must be set up by your cluster administrator and actually exist on both AP and EPs. Substitute the paths shown here with real shared paths on your cluster.

## What Does Partially Shared Filesystem Mean?

In some computing environments, the Access Point and Execution Points may share certain filesystem paths (e.g., `/staging`) while other paths are local to each machine. The executor can be configured to recognize these shared paths and avoid transferring files that are already accessible on both the AP and EPs.

This is different from:

- **Fully shared filesystem:** all paths are accessible everywhere, no file transfer needed
- **Non-shared filesystem:** nothing is shared, HTCondor must transfer everything

## How to Configure

Use the `--shared-fs-usage none` alongside `--htcondor-shared-fs-prefixes` to specify which paths are shared:

```bash
snakemake --executor htcondor --shared-fs-usage none --htcondor-shared-fs-prefixes "/staging,/shared"
```

Or in your profile configuration:

```yaml
executor: htcondor
shared-fs-usage: none
htcondor-shared-fs-prefixes: "/staging,/shared"
```

When prefixes are provided to `--htcondor-shared-fs-prefixes`, any input/output files under those paths will not be transferred by HTCondor, because the executor assumes these paths are accessible at both the AP and the EP. All files not found under these paths will be transferred by HTCondor as usual.

Example use case: If your computing environment has `/staging` mounted on both AP and EPs, but your Snakefile and local input files are in `/home/user/workflow`:

Set `--htcondor-shared-fs-prefixes "/staging"`

- Files in `/staging/data/` won't be transferred (already accessible)
- Files in input/ or `/home/user/` will be transferred to EPs

Important notes:

- Shared filesystem prefixes are only relevant when using `--shared-fs-usage none`
- Multiple prefixes can be specified as a comma-separated list
- Prefixes should be absolute paths

## Example Setup

Consider a cluster where `/staging` is mounted on both the AP and all EPs,
but your workflow directory is local to the AP:

```
/home/user/workflow/          # Local to AP only
    ├── Snakefile
    ├── wrapper.sh
    ├── inputs/
    │   └── local_input.txt   # Must be transferred by HTCondor
    └── profile/
        └── config.yaml

/staging/shared_data/         # Mounted on both AP and EPs
    └── shared_input.txt      # Already accessible, no transfer needed
```

With this profile:

```yaml
jobs: 2
executor: htcondor
shared-fs-usage: none
htcondor-shared-fs-prefixes: "/staging"
default-resources:
  job_wrapper: "wrapper.sh"
  preserve_relative_paths: true
  request_disk: "4GB"
  request_memory: "1GB"
  threads: 1
```

In this setup:

- Files in `inputs/` (e.g., `local_input.txt`) will be transferred by HTCondor
- Files in `/staging/shared_data/` (e.g., `share_input.txt`) will be accessed directly without transfer
- The Snakefile and config files will be transferred
- Output files written to local paths (e.g., `output/`) will be transferred back
- Output files written to `/staging/results/` will be written directly without transfer back

### What to Substitute

When adapting this to your cluster, you need to know:

- What shared mount points exist on your cluster
- What paths are shared between AP and EPs
- Whether those paths are consistent between AP and EPs (they must be)
