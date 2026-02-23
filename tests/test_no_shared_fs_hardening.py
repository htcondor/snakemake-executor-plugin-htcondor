"""
Tests for file-transfer hardening when running without a globally shared filesystem.

Covers the two main fixes introduced in response to GitHub issue #48:

1. Absolute-path warnings
   When --shared-fs-usage is 'none' and a job declares an input or output file
   with an absolute path that is not under any configured shared-fs prefix, the
   executor should log a stern warning (but still attempt the submission).

2. Explicit output-file transfer + transfer_output_remaps (mtime fix)
   Previously the executor transferred only the *top-level directory* of each
   output file (e.g. "output/" for "output/result.csv"), which meant that if the
   same directory was used for both inputs *and* outputs of successive rules,
   the whole directory was pushed back to the AP after the second rule, touching
   the input files and invalidating Snakemake's mtime-based dependency tracking.

   The fix is to declare each individual output file explicitly in
   transfer_output_files and add a matching transfer_output_remaps entry so that
   HTCondor places the file at its correct absolute location on the AP.
"""

from os.path import normpath, join
from unittest.mock import Mock
from snakemake_executor_plugin_htcondor import Executor


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_executor(workdir="/ap/workdir", shared_prefixes=None):
    """Return a mock Executor with the methods under test bound to it."""
    executor = Mock(spec=Executor)
    executor.logger = Mock()
    executor.shared_fs_prefixes = shared_prefixes or []
    executor.workflow = Mock()
    executor.workflow.configfiles = []
    executor.workflow.workdir_init = workdir
    executor.get_snakefile = Mock(return_value="Snakefile")

    executor._get_files_for_transfer = Executor._get_files_for_transfer.__get__(
        executor, Executor
    )
    executor._add_file_if_transferable = Executor._add_file_if_transferable.__get__(
        executor, Executor
    )
    executor._parse_file_list = Executor._parse_file_list.__get__(executor, Executor)
    return executor


def _make_job(input_files=None, output_files=None, resources=None):
    """Return a minimal mock individual job."""
    job = Mock()
    job.is_group = Mock(return_value=False)
    job.name = "test_rule"
    job.input = input_files or []
    job.output = output_files or []
    job.wildcards = {}
    job.params = {}
    job.rule = Mock()
    job.rule.script = None
    job.rule.notebook = None
    job.format_wildcards = Mock(side_effect=lambda s, **kw: s)

    _resources = resources or {}

    def resource_get(key, default=None):
        return _resources.get(key, default)

    job.resources = Mock()
    job.resources.get = Mock(side_effect=resource_get)
    return job


# ---------------------------------------------------------------------------
# 1. Absolute-path warnings
# ---------------------------------------------------------------------------


class TestAbsolutePathWarnings:
    """Absolute paths in job.input/output with no shared-fs prefix → warning."""

    def setup_method(self):
        self.executor = _make_executor()

    # --- input files --------------------------------------------------------

    def test_absolute_input_warns(self):
        """Absolute input path not on shared FS should trigger a warning."""
        job = _make_job(input_files=["/abs/data/input.csv"])
        self.executor._get_files_for_transfer(job)

        warning_calls = [str(c) for c in self.executor.logger.warning.call_args_list]
        assert any(
            "/abs/data/input.csv" in msg for msg in warning_calls
        ), "Expected a warning about the absolute input path"

    def test_absolute_input_still_added_to_transfer(self):
        """Even with the warning, the absolute input is still queued for transfer
        so the submission proceeds (warn-but-don't-block policy)."""
        job = _make_job(input_files=["/abs/data/input.csv"])
        transfer_input, *_ = self.executor._get_files_for_transfer(job)

        assert "/abs/data/input.csv" in transfer_input

    def test_relative_input_no_warning(self):
        """Relative input paths must not trigger the absolute-path warning."""
        job = _make_job(input_files=["data/input.csv"])
        self.executor._get_files_for_transfer(job)

        abs_path_warnings = [
            str(c)
            for c in self.executor.logger.warning.call_args_list
            if "absolute path" in str(c)
        ]
        assert (
            abs_path_warnings == []
        ), f"Unexpected absolute-path warning for a relative file: {abs_path_warnings}"

    def test_absolute_input_on_shared_fs_no_warning(self):
        """Absolute input that lives under a shared-fs prefix must NOT warn."""
        executor = _make_executor(shared_prefixes=["/staging/"])
        job = _make_job(input_files=["/staging/project/input.csv"])
        executor._get_files_for_transfer(job)

        abs_path_warnings = [
            str(c)
            for c in executor.logger.warning.call_args_list
            if "/staging/project/input.csv" in str(c)
        ]
        assert (
            abs_path_warnings == []
        ), "Expected no absolute-path warning for a shared-FS input file"

    # --- output files -------------------------------------------------------

    def test_absolute_output_warns(self):
        """Absolute output path not on shared FS should trigger a warning."""
        job = _make_job(output_files=["/abs/results/output.csv"])
        self.executor._get_files_for_transfer(job)

        warning_calls = [str(c) for c in self.executor.logger.warning.call_args_list]
        assert any(
            "/abs/results/output.csv" in msg for msg in warning_calls
        ), "Expected a warning about the absolute output path"

    def test_absolute_output_added_to_transfer_with_identity_remap(self):
        """Absolute output paths that are not on shared FS are still queued for
        transfer, together with an identity remap so HTCondor places the file at
        the correct absolute location on the AP (not just the basename in IWD).
        The job may well fail on the EP trying to write there, but it is strictly
        better to *attempt* the transfer than to guarantee data loss."""
        job = _make_job(output_files=["/abs/results/output.csv"])
        _, transfer_output, remaps = self.executor._get_files_for_transfer(job)

        assert "/abs/results/output.csv" in transfer_output
        assert any("/abs/results/output.csv" in r for r in remaps)
        # The remap must be an identity remap (same path on both sides)
        matching = [r for r in remaps if "/abs/results/output.csv" in r]
        assert len(matching) == 1
        ep_side, ap_side = matching[0].split(" = ", 1)
        assert ep_side.strip() == "/abs/results/output.csv"
        assert ap_side.strip() == "/abs/results/output.csv"

    def test_absolute_output_on_shared_fs_no_warning_and_not_transferred(self):
        """Absolute output that is on a shared-fs prefix must not warn and
        must not appear in the transfer list (no transfer needed)."""
        executor = _make_executor(shared_prefixes=["/staging/"])
        job = _make_job(output_files=["/staging/results/output.csv"])
        _, transfer_output, transfer_remaps = executor._get_files_for_transfer(job)

        abs_path_warnings = [
            str(c)
            for c in executor.logger.warning.call_args_list
            if "/staging/results/output.csv" in str(c)
        ]
        assert (
            abs_path_warnings == []
        ), "Expected no absolute-path warning for a shared-FS output file"
        assert "/staging/results/output.csv" not in transfer_output
        assert len(transfer_remaps) == 0

    def test_mixed_absolute_and_relative_outputs(self):
        """Mixed output list: relative files use a workdir remap, absolute ones
        are warned about but still transferred with an identity remap."""
        job = _make_job(
            output_files=[
                "output/good.csv",  # relative → workdir remap
                "/abs/bad.csv",  # absolute, not shared → warned + identity remap
            ]
        )
        _, transfer_output, remaps = self.executor._get_files_for_transfer(job)

        assert "output/good.csv" in transfer_output
        assert "/abs/bad.csv" in transfer_output

        # Relative output → absolute AP destination remap
        assert any("output/good.csv" in r and "/ap/workdir" in r for r in remaps)
        # Absolute output → identity remap
        assert any("/abs/bad.csv = /abs/bad.csv" in r for r in remaps)

        warning_calls = [str(c) for c in self.executor.logger.warning.call_args_list]
        assert any("/abs/bad.csv" in msg for msg in warning_calls)

    # --- htcondor_transfer_input_files resource ------------------------------

    def test_absolute_custom_input_resource_warns(self):
        """htcondor_transfer_input_files with an absolute path should also warn."""
        job = _make_job(resources={"htcondor_transfer_input_files": "/abs/helper.py"})
        job.format_wildcards = Mock(side_effect=lambda s, **kw: s)
        self.executor._get_files_for_transfer(job)

        warning_calls = [str(c) for c in self.executor.logger.warning.call_args_list]
        assert any("/abs/helper.py" in msg for msg in warning_calls)


# ---------------------------------------------------------------------------
# 2. Explicit output-file transfer (mtime fix)
# ---------------------------------------------------------------------------


class TestExplicitOutputTransfer:
    """Output files are transferred explicitly, not as top-level directories."""

    def setup_method(self):
        self.executor = _make_executor()

    def test_single_relative_output_transferred_explicitly(self):
        """A single relative output file must appear as-is in transfer list."""
        job = _make_job(output_files=["output/result.csv"])
        _, transfer_output, _ = self.executor._get_files_for_transfer(job)

        assert "output/result.csv" in transfer_output
        # The parent directory must NOT be in the list
        assert "output" not in transfer_output

    def test_nested_relative_output_transferred_explicitly(self):
        """Deeply nested relative outputs are listed verbatim."""
        job = _make_job(output_files=["a/b/c/deep.txt"])
        _, transfer_output, _ = self.executor._get_files_for_transfer(job)

        assert "a/b/c/deep.txt" in transfer_output
        assert "a" not in transfer_output

    def test_multiple_outputs_same_directory_all_listed(self):
        """All explicit output files are listed even when they share a directory."""
        job = _make_job(
            output_files=["output/file1.csv", "output/file2.csv", "output/file3.csv"]
        )
        _, transfer_output, _ = self.executor._get_files_for_transfer(job)

        assert "output/file1.csv" in transfer_output
        assert "output/file2.csv" in transfer_output
        assert "output/file3.csv" in transfer_output
        assert "output" not in transfer_output

    def test_overlapping_input_output_directory_mtime_regression(self):
        """Key regression test for the mtime bug.

        When rule 2's inputs live in the same directory as rule 2's outputs
        (because that directory was rule 1's output), the old executor would
        transfer the whole directory back to the AP, touching rule 2's inputs
        and breaking Snakemake's mtime check.

        With the fix, only rule 2's explicit output files are declared for
        transfer, so rule 2's inputs (which also live in output/) are untouched
        on the AP.
        """
        # Simulate rule 2: reads output/step1.csv (rule 1's output),
        # writes output/step2.csv (rule 2's output).  Both live in output/.
        job = _make_job(
            input_files=["output/step1.csv"],
            output_files=["output/step2.csv"],
        )
        _, transfer_output, _ = self.executor._get_files_for_transfer(job)

        # Only the explicit output should be declared for back-transfer
        assert "output/step2.csv" in transfer_output
        # The parent directory must NOT appear — that would touch step1.csv too
        assert "output" not in transfer_output

    def test_output_deduplication(self):
        """Duplicate output paths in job.output are only transferred once."""
        job = _make_job(output_files=["output/result.csv", "output/result.csv"])
        _, transfer_output, _ = self.executor._get_files_for_transfer(job)

        assert transfer_output.count("output/result.csv") == 1


# ---------------------------------------------------------------------------
# 3. transfer_output_remaps generation
# ---------------------------------------------------------------------------


class TestOutputRemaps:
    """transfer_output_remaps maps each EP output path to its AP absolute path."""

    def setup_method(self):
        self.workdir = "/ap/workdir"
        self.executor = _make_executor(workdir=self.workdir)

    def test_remap_generated_for_relative_output(self):
        """Every relative output file must have a corresponding remap entry."""
        job = _make_job(output_files=["output/result.csv"])
        _, _, remaps = self.executor._get_files_for_transfer(job)

        assert len(remaps) == 1
        ep_side, ap_side = remaps[0].split(" = ")
        assert ep_side.strip() == "output/result.csv"
        assert ap_side.strip() == normpath(join(self.workdir, "output/result.csv"))

    def test_remap_destination_is_absolute(self):
        """The AP-side remap destination must always be an absolute path."""
        job = _make_job(output_files=["results/model.pkl"])
        _, _, remaps = self.executor._get_files_for_transfer(job)

        for remap in remaps:
            _, dest = remap.split(" = ", 1)
            assert dest.strip().startswith(
                "/"
            ), f"Remap AP destination should be absolute: {remap}"

    def test_remap_count_matches_output_file_count(self):
        """One remap per output file (excluding shared-FS and absolute outputs)."""
        job = _make_job(
            output_files=[
                "out/a.txt",
                "out/b.txt",
                "out/c.txt",
            ]
        )
        _, transfer_output, remaps = self.executor._get_files_for_transfer(job)

        assert len(remaps) == len(transfer_output)

    def test_no_remap_for_shared_fs_output(self):
        """Outputs on a shared FS are skipped entirely — no remap generated."""
        executor = _make_executor(shared_prefixes=["/staging/"])
        job = _make_job(output_files=["/staging/results/out.csv"])
        _, transfer_output, remaps = executor._get_files_for_transfer(job)

        assert len(transfer_output) == 0
        assert len(remaps) == 0

    def test_identity_remap_for_absolute_output(self):
        """Absolute outputs not on shared FS get an identity remap
        ('/abs/path = /abs/path') so HTCondor returns the file to the correct
        absolute location on the AP instead of the basename in IWD."""
        job = _make_job(output_files=["/abs/output/file.csv"])
        _, transfer_output, remaps = self.executor._get_files_for_transfer(job)

        assert "/abs/output/file.csv" in transfer_output
        assert len(remaps) == 1
        ep_side, ap_side = remaps[0].split(" = ", 1)
        assert ep_side.strip() == "/abs/output/file.csv"
        assert ap_side.strip() == "/abs/output/file.csv"

    def test_remap_uses_normpath(self):
        """The AP-side destination is normalised (no trailing slashes, etc.)."""
        job = _make_job(output_files=["output/../output/result.csv"])
        _, _, remaps = self.executor._get_files_for_transfer(job)

        assert len(remaps) == 1
        ep_side, ap_side = remaps[0].split(" = ", 1)
        # normpath should collapse the ..
        assert ".." not in ep_side.strip()
        assert ".." not in ap_side.strip()

    def test_remap_for_custom_output_resource(self):
        """htcondor_transfer_output_files relative paths also get remaps."""
        job = _make_job(resources={"htcondor_transfer_output_files": "logs/run.log"})
        job.format_wildcards = Mock(side_effect=lambda s, **kw: s)
        _, transfer_output, remaps = self.executor._get_files_for_transfer(job)

        assert "logs/run.log" in transfer_output
        assert any("logs/run.log" in r for r in remaps)
