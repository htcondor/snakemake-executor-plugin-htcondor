# Non-Shared Filesystem

A simple example demonstrating how running Snakemake jobs on an HTCondor cluster without a shared filesystem works.

## What Is a Shared Filesystem, and What Changes Without One?

### Snakemake vs HTCondor

Snakemake assumes a shared filesystem. This means that when jobs are run, all required files and paths are directly accessible on both the AP and EP.

Some HTCondor clusters lack a shared filesystem, so each EP is effectively an isolated machine with only its own local disk. It cannot directly access files from the AP and does not share the AP's software environment.

### Why the Flag Is Needed

Without telling Snakemake that the execution environment does not share a filesystem with the submission environment, the executor won't trigger HTCondor's file transfer mechanism because it assumes file paths that exist on the AP also exist on the EP.

The flag `--shared-fs-usage none` tells Snakemake to explicitly inform HTCondor to transfer files so that it can have access to on the EP.

Or under default-resources in the htcondor_profile configuration:

```yaml
executor: htcondor
shared-fs-usage: none
```

### Path Limitations

With the flag, Snakemake now tells HTCondor to transfer the files, which solves one layer of our problem.

HTCondor transfers files relative to the submission directory.
By default, it strips all directory structure and dumps files into a flat directory on the EP. So `inputs/sample1.txt` would arrive as just `sample1.txt`, but Snakemake still looks for `inputs/sample1.txt` and fails.

To address this, the executor uses HTCondor's `preserve_relative_paths` submit description behavior so transferred files keep their relative directory structure.
That means `inputs/sample1.txt` is transferred as `inputs/sample1.txt` on the EP instead of being flattened to `sample1.txt`.

This is also why your entire workflow must live inside your submission directory.
`preserve_relative_paths` can only preserve paths relative to where you submitted from.
A path like `../../my_data/` has no equivalent structure to preserve on the EP.

[Read more about HTCondor's file transferring mechanism here](https://htcondor.readthedocs.io/en/lts/users-manual/file-transfer.html).

### What Do You Need If You Are Running Without a Shared Filesystem?

- **Job Wrapper script:** set `$HOME` and the right shell environment before anything can run.
- **A container (highly-recommend):** EP needs to at least have Python and Snakemake to execute the job(s). A container bundles Python + Snakemake + tools your rules use + your dependencies into a single portable image that HTCondor can bring along with the job.
- **Correct directory structure:** Your entire workflow must live inside your submission directory and be referenced with downward relative paths. If anything lives above the submission directory, HTCondor can't transfer it and Snakemake will look for it in the wrong place on the EP.

## How This Example Works

This example runs the same two-step pipeline from the job wrapper example.
It processes two pre-created input files through two rules:

- **`make_intermediary`** — Copies each input file and appends `"foo"` to
  produce `results/intermediary_{sample}.txt`
- **`make_output`** — Copies each intermediary file and appends `"bar"` to produce the final `results/output_{sample}.txt`

The htcondor_profile sets `shared-fs-usage: none` to activate HTCondor's file transfer
mechanism, and `preserve_relative_paths: True` to ensure directory structure is preserved when files arrive on the EP.

### How To Run

```bash
snakemake --profile htcondor_profile
```

### Expected Outputs

```
results/output_sample1.txt
results/output_sample2.txt
```
