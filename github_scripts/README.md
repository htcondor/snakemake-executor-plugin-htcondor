# CI torture test

This directory contains a CI-runnable version of the HTCondor executor
["torture test"](https://github.com/jhiemstrawisc/snakemake-htcondor-executor-torture-test)
and the Docker pool it runs against. It is driven by
[`.github/workflows/torture-test.yml`](../.github/workflows/torture-test.yml)
on every push to `main`, every pull request, and on demand
(`workflow_dispatch`). See [issue #53](https://github.com/htcondor/snakemake-executor-plugin-htcondor/issues/53).

## Why a multi-container pool

The torture test exercises the executor's hardest code paths: file transfer,
`transfer_output_remaps`, grouped jobs, `temp()` exclusion, env-var injection,
AP-side mtime correctness (issue #48 regression), and **partial shared
filesystem** handling via `--htcondor-shared-fs-prefixes`.

Faithfully testing the last two requires a *true two-machine topology*: an
Access Point (AP, schedd) and a separate Execution Point (EP, startd) that
share **only one path** and have genuinely separate working directories, so
HTCondor file transfer is actually invoked. A single `htcondor/mini` node (as
used by [`ci.yml`](../.github/workflows/ci.yml)) cannot do this — every path is
the same disk, so it cannot distinguish "the executor correctly skipped
transferring a `/staging` file" from "the EP read it directly because it is the
same disk anyway," and it cannot separate AP-side from EP-side mtimes.

## Topology

`pool/docker-compose.yml` stands up the canonical HTCondor split-role pool:

```
            docker network: pool
   ┌───────────┐     ┌────────────────────────┐     ┌────────────────────────┐
   │    cm     │     │   ap  (htcondor/submit) │     │   ep  (htcondor/execute)│
   │ collector │◄────┤ schedd + snakemake +    │     │ startd + python +       │
   │ negotiator│     │ plugin (runs run.sh)    │     │ snakemake (runs rules)  │
   └───────────┘     └───────────┬─────────────┘     └───────────┬─────────────┘
        ▲ CONDOR_HOST=cm         │   volume: staging:/staging     │  volume: staging:/staging
        └────────────────────────┴────────── ONLY shared path ────┘
```

- The **`staging` volume mounted at `/staging` on `ap` and `ep` only** is the
  single shared path. The snakemake working directory lives in the `ap`
  container (`~submituser/torture-test`, baked into the image) and is *not*
  mounted on `ep`, so all non-`/staging` files go through HTCondor file transfer.
- Auth is HTCondor **pool password**: `pool/make-pool-password.sh` writes a
  random `pool/pool_password` (git-ignored) that is mounted into all three
  containers; `USE_POOL_PASSWORD=yes` enables PASSWORD auth and `CONDOR_HOST=cm`
  points the AP/EP at the central manager.
- `pool/pre-exec.sh` runs in each container at startup and `chmod 1777 /staging`
  so both the AP's `submituser` and the EP's per-slot job accounts (`slot1_N`)
  can read/write it.
- `--shared-fs-usage none` tells the executor to use file transfer; the executor
  then skips `/staging` paths because of `--htcondor-shared-fs-prefixes /staging`.

## Differences from the upstream (manual) torture test

The CI copy is intentionally adapted in two ways — see the header comment in
[`Snakefile`](Snakefile):

1. **`universe="vanilla"`** instead of `universe="container"` (the
   `container_image="docker://..."` resources are dropped). Running
   container-universe jobs inside a containerized EP would require nested
   containers (apptainer/DinD) — slow and flaky in CI. Vanilla still exercises
   every *executor* code path; it just does not exercise HTCondor's container
   universe itself, which remains covered by the manual CHTC run.
1. **`/staging/torture-test`** instead of the CHTC-specific
   `/staging/jhiemstra/torture-test`. The CI pool shares a single `/staging`
   volume.

The EP images therefore bake `python` + `snakemake` in system-wide so the
`slot1_N` accounts can run each rule; the AP image additionally installs the
executor plugin from the checked-out source.

Switching to vanilla universe + the multi-slot CI EP required three small,
clearly-commented adjustments to the harness (none touch the executor):

- **`wrapper.sh` exports a `PATH`.** Vanilla-universe jobs start with a
  near-empty environment; without `PATH` in `os.environ`, Snakemake's scheduler
  crashes with `KeyError: 'PATH'`. (Container universe gets `PATH` from the image.)
- **`run.sh` pre-creates `/staging/torture-test` world-writable (`1777`).** The
  EP runs each job as a distinct per-slot account (`slot1_N`); without this, the
  first sample's job would own the directory and the second sample's job
  couldn't write into it. A real single-user staging area doesn't hit this.
- **The idempotency dry-run is *local*** (no `--executor htcondor` /
  `--shared-fs-usage none`). The issue-#48 mtime invariant is an AP-side
  filesystem property, and Snakemake 9.x refuses a non-shared-fs dry-run without
  a `--default-storage-provider`.

## What the test asserts

`run.sh` runs the workflow, then verifies:

1. **Re-run idempotency** — `snakemake --dryrun --rerun-triggers mtime` reports
   "Nothing to be done" (catches mtime-stomping / issue #48).
1. **`htcondor_transfer_output_files`** — the sidecar files
   (`output/<sample>_extra_sidecar.txt`) were transferred back to the AP.
1. **Partial shared FS** — `/staging/torture-test/<sample>_shared_data.txt`
   exists on the AP (written directly by the EP, never transferred).

The `mtime_check` rule (a localrule on the AP) additionally fails the run if the
shared-directory mtime ordering is wrong.

The workflow also covers (Tier 1 additions):

4. **Custom ClassAds / raw submit attributes / requirements** — `attribute_check`
   reads its own `.job.ad` (HTCondor writes it into the scratch dir) and asserts
   that `classad_TortureTag`, `htcondor_submit_MY__TortureRaw`, and a
   `requirements="IsContainer == true"` clause all reached the submit description.
1. **`getenv=True`** — `getenv_check` verifies a var exported on the AP but *not*
   passed via `--envvars` still reaches the EP (a distinct env path).
1. **Env-var escaping** — `env_special_check` passes a value with embedded double
   quotes through `--envvars` and asserts it round-trips intact (exercises the
   executor's HTCondor new-syntax `environment` serialization).
1. **Retry on failure** — `flaky_retry` fails its first attempt and succeeds on
   the second (sentinel on `/staging` persists across attempts), exercising
   Snakemake's retry path and the executor's failure reporting.
1. **Negative test** — `run.sh` runs a deliberately-failing rule (`always_fail`,
   not in `rule all`) as a separate targeted invocation and asserts the workflow
   exits non-zero and surfaces the error (catches "hangs/swallows failures").

> Note: `env_special_check` (#6) originally surfaced a real executor bug — env
> values were delivered wrapped in literal double quotes because
> `_format_htcondor_environment()` emitted new-syntax escaping without the
> required outer-quote framing. That was fixed in the executor
> (`_format_htcondor_environment` + the `environment` assembly in `run_job`); this
> test now guards against regressions.

## Run it locally

Requires Docker + Docker Compose v2. From this `pool/` directory:

```bash
cd pool
./make-pool-password.sh
docker compose build
docker compose up -d

# Wait until the EP registers with the CM:
until docker compose exec -T ap condor_status -af Name 2>/dev/null | grep -q .; do sleep 5; done
docker compose exec -T ap condor_status

# Run the torture test as the submit user:
docker compose exec -T -u submituser ap bash -lc 'cd ~/torture-test && ./run.sh'

# Tear down (removes the staging volume too):
docker compose down -v
```

## Files

| Path | Purpose |
|------|---------|
| `Snakefile` | The CI workflow (vanilla universe, `/staging`). |
| `wrapper.sh` | `job_wrapper` — re-invokes `snakemake` on the EP per rule. |
| `run.sh` | Runs the workflow + the three post-run checks. |
| `cleanup.sh` | Removes generated outputs and `/staging/torture-test`. |
| `scripts/` | `process_sample{1,2}.py` (wildcard-selected) + `stats_helpers.py`. |
| `modules/quality_check/` | A module + nested `validation/` module. |
| `pool/docker-compose.yml` | The cm/ap/ep pool definition. |
| `pool/Dockerfile.ap` | AP image: schedd + snakemake + plugin. |
| `pool/Dockerfile.ep` | EP image: startd + python + snakemake. |
| `pool/pre-exec.sh` | Makes `/staging` world-writable at container start. |
| `pool/make-pool-password.sh` | Generates the shared pool-password secret. |
