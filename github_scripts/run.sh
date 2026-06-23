#!/bin/bash
set -e

# =============================================================================
# HTCondor Executor Torture Test Runner (CI variant)
# =============================================================================
# Runs the full integration test workflow and performs post-run verification.
#
# This is a CI-adapted copy of the manual torture test runner. The only change
# from the upstream copy is the shared-FS prefix: the CI pool shares a single
# "/staging" volume between the AP and EP (the manual test used
# "/staging/jhiemstra").
#
# What this script does:
#   1. Exports TORTURE_TEST_VAR for the env var injection test
#   2. Runs the workflow with --envvars to pass the variable to EPs
#   3. Verifies re-run idempotency (--dryrun should find nothing to do)
#   4. Verifies htcondor_transfer_output_files sidecar files exist
#   5. Verifies shared-FS output files exist on the AP
# =============================================================================

# Shared filesystem prefix — files under this path are accessible on both AP
# and EP without HTCondor file transfer.  In the CI pool this is the single
# "/staging" volume mounted on both the AP and EP containers.
SHARED_FS_PREFIX="/staging"
SHARED_FS_TEST_DIR="${SHARED_FS_PREFIX}/torture-test"

# Export the test env var.  The env_var_check rule on the EP will verify this
# arrives via HTCondor's native `environment` key (not inline in arguments).
export TORTURE_TEST_VAR="hello_from_ap"

# Env var with embedded double quotes — exercises the executor's
# _format_htcondor_environment() escaping (quotes are doubled).  Passed via
# --envvars below; the env_special_check rule asserts it round-trips intact.
export TORTURE_QUOTE_VAR='pre"mid"post'

# Env var with an embedded single quote and NO whitespace -- the regression
# case for the executor's single-quote escaping (a value like this was silently
# corrupted to "its" before the fix). env_special_check asserts it round-trips.
export TORTURE_SQUOTE_VAR="it's"

# Env var that is deliberately NOT passed via --envvars.  The getenv_check rule
# uses getenv=True, so the only way this reaches the EP is HTCondor's getenv
# capture of the submit-side (AP) environment.
export GETENV_TEST_VAR="visible_via_getenv"

# Pre-create the shared-FS test directory, world-writable.  The CI Execution
# Point runs each job as a distinct per-slot account (slot1_N).  Without a
# pre-created world-writable directory, the first sample's shared_fs_write job
# would create ${SHARED_FS_TEST_DIR} owned by its slot user (mode 0755) and the
# second sample's job — a *different* slot user — could not write into it.  On a
# real, single-user staging area (e.g. CHTC's /staging/<user>) this never
# happens; this just makes the multi-slot CI pool deterministic.  The AP shares
# the same /staging mount, so submituser can create it here.
mkdir -p "$SHARED_FS_TEST_DIR"
chmod 1777 "$SHARED_FS_TEST_DIR"

echo "=== Starting torture test workflow ==="
echo ""

snakemake \
    --jobs 10 \
    --executor htcondor \
    --htcondor-jobdir logs \
    --htcondor-held-timeout 120 \
    --shared-fs-usage none \
    --htcondor-shared-fs-prefixes "$SHARED_FS_PREFIX" \
    --verbose \
    --envvars TORTURE_TEST_VAR TORTURE_QUOTE_VAR TORTURE_SQUOTE_VAR

echo ""
echo "=== Workflow completed successfully ==="

# -----------------------------------------------------------------------------
# Post-run verification 1: Re-run idempotency
# -----------------------------------------------------------------------------
# A dry-run immediately after a successful run should report "Nothing to be
# done."  If the executor stomped mtimes during output transfer (the pre-fix
# bug), Snakemake will want to re-run rules — which means the fix is broken.
#
# We use --rerun-triggers mtime to scope the check to what the executor can
# actually break: mtime-based dependency ordering.  Without this flag,
# Snakemake's provenance tracking ("Code has changed since last execution")
# would fire whenever the Snakefile is edited between runs — that's a core
# Snakemake feature, not an executor concern.
#
# This is a *local* dry-run (no --executor htcondor / --shared-fs-usage none):
# the mtime invariant from issue #48 is purely an Access-Point-side filesystem
# property — did transferring outputs back stomp the mtimes of sibling files on
# the AP? A local dry-run reading the AP files answers that directly. It also
# avoids a Snakemake 9.x requirement that --shared-fs-usage none be paired with
# a --default-storage-provider, which does not apply when we only want to
# inspect the already-materialized AP files.
echo ""
echo "=== Re-run idempotency check ==="

DRYRUN_OUTPUT=$(snakemake \
    --rerun-triggers mtime \
    --dryrun 2>&1) || true

if echo "$DRYRUN_OUTPUT" | grep -qi "nothing to be done"; then
    echo "PASS: dry-run reports nothing to be done"
else
    echo "FAIL: dry-run wants to re-run rules:"
    echo "$DRYRUN_OUTPUT"
    exit 1
fi

# -----------------------------------------------------------------------------
# Post-run verification 2: htcondor_transfer_output_files sidecar check
# -----------------------------------------------------------------------------
# The extra_output_check rule writes a sidecar file that is NOT a declared
# Snakemake output but IS declared via htcondor_transfer_output_files.
# Verify that HTCondor transferred it back to the AP.
echo ""
echo "=== htcondor_transfer_output_files sidecar check ==="

SIDECAR_PASS=true
for sample in sample1 sample2; do
    sidecar="output/${sample}_extra_sidecar.txt"
    if [ -f "$sidecar" ]; then
        echo "PASS: $sidecar exists"
    else
        echo "FAIL: $sidecar missing — htcondor_transfer_output_files may be broken"
        SIDECAR_PASS=false
    fi
done

if [ "$SIDECAR_PASS" = false ]; then
    echo ""
    echo "htcondor_transfer_output_files test FAILED"
    exit 1
fi

# -----------------------------------------------------------------------------
# Post-run verification 3: Shared filesystem output check
# -----------------------------------------------------------------------------
# The shared_fs_write rule writes output directly to /staging/torture-test/
# (the shared mount).  Since the executor excludes shared-FS paths from
# transfer_output_files, the file must have been written directly by the EP.
# Verify it exists on the AP (which can see the same shared mount).
echo ""
echo "=== Shared filesystem output check ==="

SHARED_FS_PASS=true
for sample in sample1 sample2; do
    shared_file="${SHARED_FS_TEST_DIR}/${sample}_shared_data.txt"
    if [ -f "$shared_file" ]; then
        echo "PASS: $shared_file exists on shared FS"
    else
        echo "FAIL: $shared_file missing — shared-FS output may have been lost"
        SHARED_FS_PASS=false
    fi
done

if [ "$SHARED_FS_PASS" = false ]; then
    echo ""
    echo "Shared filesystem output test FAILED"
    exit 1
fi

# -----------------------------------------------------------------------------
# Post-run verification 4: Negative test — a deliberately failing job
# -----------------------------------------------------------------------------
# Runs the always_fail rule (NOT part of `rule all`) as a separate, targeted
# invocation and asserts that the workflow exits non-zero AND surfaces the
# failure — catching regressions where a failed job hangs or is silently
# swallowed.  --htcondor-held-timeout 0 makes a held job fail immediately so the
# test never stalls.  We run it for a single sample to keep it quick.
echo ""
echo "=== Negative test: deliberate failure is reported (not hung/swallowed) ==="

# Note: the target is placed BEFORE --envvars on purpose — --envvars takes one
# or more values (nargs='+'), so a positional target after it would be parsed
# as an env-var name.
NEG_RC=0
NEG_OUTPUT=$(snakemake \
    "${SHARED_FS_TEST_DIR}/sample1_never.txt" \
    --jobs 10 \
    --executor htcondor \
    --htcondor-jobdir logs \
    --htcondor-held-timeout 0 \
    --shared-fs-usage none \
    --htcondor-shared-fs-prefixes "$SHARED_FS_PREFIX" \
    --verbose \
    --envvars TORTURE_TEST_VAR 2>&1) || NEG_RC=$?

if [ "$NEG_RC" -ne 0 ]; then
    echo "PASS: deliberately-failing workflow exited non-zero (rc=$NEG_RC)"
else
    echo "FAIL: deliberately-failing workflow exited 0 (failure not detected)"
    echo "$NEG_OUTPUT" | tail -30
    exit 1
fi

if echo "$NEG_OUTPUT" | grep -qiE "error in rule always_fail|at least one job did not complete"; then
    echo "PASS: failure was surfaced in Snakemake output"
else
    echo "FAIL: failure was not surfaced as expected"
    echo "$NEG_OUTPUT" | tail -30
    exit 1
fi

echo ""
echo "=== All checks passed ==="
