# Non Shared Filesystem

A simple example demonstrating how non-shared filesystem works.

## What Does Non Shared Filesystem Mean

### Snakemake vs HTCondor

Snakemake assumes a shared filesystem. This means that when jobs are run, all the resources and files can be read directly because the path exists everywhere, both on the AP and EP.

Some HTCondor's clusters have non-shared filesystem. This means that each EP is essentially a blank machine with only its own local disk. It has no access to files on the AP, and no shared software environment.

### Why the Flag is needed

If you are working on a non-shared setup and do not tell Snakemake, the job will land on an EP and break since it cannot find required files.

The flag `--shared-fs-usage none` tells Snakemake to explicitly inform HTCondor to transfer files so that it can have access to on the EP.

Or under default-resources in the profile configuration:

```yaml
executor: htcondor
shared-fs-usage: none
```

### Path Limitations

With the flag, Snakemake now tells HTCondor to transfer the files, which solves one layer of our problem.

HTCondor transfers files relative to the submission directory. By default,
it strips all directory structure and dumps files into a flat directory on
the EP. So `inputs/sample1.txt` would arrive as just `sample1.txt`, but Snakemake still looks for `inputs/sample1.txt` and fails.

To fix this, use `preserve_relative_paths: True` in your profile. This tells HTCondor to keep
the directory structure intact when transferring files, so `inputs/sample1.txt` arrives at exactly `inputs/sample1.txt` on the EP.

This is also why your entire workflow must live inside your submission
directory. `preserve_relative_paths` can only preserve paths relative to where you submitted from.
A path like `../../my_data/` has no equivalent structure to preserve on the EP.
[Read more about HTCondor's file transferring mechanism here](https://htcondor.readthedocs.io/en/lts/users-manual/file-transfer.html).

### What Do You Need If You Are Using a Non-Shared Filesystem

- **Job Wrapper script:** set `$HOME` and the right shell environment before anything can run.
- **A container (highly-recommend):** EP needs to at least have Python and Snakemake to execute the job(s). A container bundles Python + Snakemake + tools your rules use + your dependencies into a single portable image that HTCondor can bring along with the job.
- **Correct directory structure:** Due to path flattening limitation, your entire workflow must live inside your submission directory and be referenced with downward relative paths. If anything lives above the submission directory, HTCondor can't transfer it and Snakemake will look for it in the wrong place on the EP.

## How This Example Works

This example runs the same two-step pipeline from the job wrapper example, but configured for a non-shared filesystem. It processes two pre-created input files through two rules:

- **`make_intermediary`** — Copies each input file and appends `"foo"` to
  produce `results/intermediary_{sample}.txt`
- **`make_output`** — Copies each intermediary file and appends `"bar"` to produce the final `results/output_{sample}.txt`

Unlike the job wrapper example, input files (`inputs/sample1.txt`,
`inputs/sample2.txt`) are pre-created and committed alongside the example.
This is required because HTCondor must transfer input files from the AP to
each EP before the job runs — they cannot be created as part of the workflow
itself.

The profile sets `shared-fs-usage: none` to activate HTCondor's file transfer
mechanism, and `preserve_relative_paths: True` to ensure directory structure
is preserved when files arrive on the EP.

### How To Run

```bash
snakemake --profile profile
```

### Expected Outputs

```
results/output_sample1.txt
results/output_sample2.txt
```
