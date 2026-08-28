"""
Unit tests for metadata file creation and functionalities for htcondor snake commands

These tests aim to:
- Verify that the job is local universe management job before proceeding further
with creating metadata file. Otherwise, the mock_executor should run normally.
- Metadata is created in the right format for both grouped jobs and non-grouped jobs
- Metadata is updated at the time it is expected to in the job life cycle

Current Gap: at the start we use `_CONDOR_JOB_AD` to retrieve that cluster_id, which is
the first step to anything related to the metadata. Here, we supply the mgmt_id rather than
retrieving it because otherwise we need heavyweight executor setup for this.
For now, we'll skip this first, but I have tested manually that it works.
"""

import glob
import json
import os
import time

import pytest
from unittest.mock import Mock

from snakemake_executor_plugin_htcondor import JobStatus

from conftest import (
    create_mock_metadata_executor,
    mock_htcondor_submission,
)


def make_job(name="rule_a", jobid=1, threads=1, resources=None):
    """Plain Mock job suitable for run_job (not the transfer-focused helpers)."""
    job = Mock()
    job.name = name
    job.jobid = jobid
    job.threads = threads
    job.resources = resources or {}
    job.is_group = Mock(return_value=False)
    return job


def make_group_job(individual_jobs, name="test_group_job", jobid=100):
    """Plain Mock group job; job.jobs holds the individual node jobs."""
    job = Mock()
    job.name = name
    job.jobid = jobid
    job.threads = 1
    job.resources = {}
    job.is_group = Mock(return_value=True)
    job.jobs = individual_jobs
    return job


class TestMetadata:
    """Verify different cases as stated in the intro docstrings"""

    @pytest.fixture
    def mock_executor(self, tmp_path):
        return create_mock_metadata_executor(tmp_path)

    def test_not_mgmt_no_metadata(self, tmp_path):
        # No metadata should be present when this is not a local universe management job
        executor = create_mock_metadata_executor(tmp_path, mgmt_id=None)
        job = make_job()

        captured_submit_dict = {}
        with mock_htcondor_submission(captured_submit_dict):
            executor.run_job(job)

        assert executor._mgmt_id is None
        assert executor._metadata == {}
        assert executor._metadata_file is None
        assert (
            glob.glob(os.path.join(executor.jobDir, "snakemake-metadata-*.json")) == []
        )

    def test_all_keys_present_non_grouped(self, mock_executor):
        # Under normal working condition, all keys and associated values should be
        # correct (non-grouped jobs)
        job = make_job(name="rule_a", jobid=7)

        captured_submit_dict = {}
        with mock_htcondor_submission(captured_submit_dict):
            mock_executor.run_job(job)

        job_key = "12345:7"
        entry = mock_executor._metadata["jobs"][job_key]
        assert entry["cluster_id"] == 12345
        assert entry["rule_name"] == "rule_a"
        assert entry["jobid"] == 7
        assert entry["display_name"] == "rule_a-7"
        assert entry["grouped"] is False
        assert entry["status"] == JobStatus.IDLE.value
        assert "submitted_at" in entry
        assert "last_updated" in entry

        # The on-disk file must match what's in memory, proving _write_metadata
        # actually ran (and left no dangling .tmp file behind).
        with open(mock_executor._metadata_file) as f:
            on_disk = json.load(f)
        assert on_disk == mock_executor._metadata
        assert not os.path.exists(f"{mock_executor._metadata_file}.tmp")

    def test_all_keys_present_grouped(self, mock_executor):
        # Under normal working condition, all keys and associated values should be
        # correct (grouped jobs)
        individual_jobs = [
            make_job(name="rule_a", jobid=1),
            make_job(name="rule_b", jobid=2),
        ]
        group_job = make_group_job(individual_jobs, jobid=99)

        captured_submit_dict = {}
        with mock_htcondor_submission(captured_submit_dict):
            mock_executor.run_job(group_job)

        # One metadata entry per node in the group, not one for the whole group.
        assert len(mock_executor._metadata["jobs"]) == len(individual_jobs)
        for j in individual_jobs:
            entry = mock_executor._metadata["jobs"][f"12345:{j.jobid}"]
            assert entry["cluster_id"] == 12345
            assert entry["rule_name"] == j.name
            assert entry["jobid"] == j.jobid
            assert entry["display_name"] == f"{j.name}-{j.jobid}"
            assert entry["grouped"] is True
            assert entry["status"] == JobStatus.IDLE.value

    def test_local_job_not_present(self, tmp_path):
        # Local jobs will not be submitted to the EP and should not be included
        # in the `executable_nodes` key.
        executor = create_mock_metadata_executor(tmp_path, mgmt_id=None)

        local_job = Mock(is_local=True)
        remote_job = Mock(is_local=False)
        executor.workflow.dag = Mock()
        executor.workflow.dag.jobs = [local_job, remote_job]
        executor.workflow.dag.needrun_jobs = Mock(return_value=[local_job, remote_job])

        executor._metadata_file = os.path.join(
            executor.jobDir, "snakemake-metadata-555.json"
        )
        executor._initialize_metadata(555)

        assert executor._metadata["total_nodes"] == 2
        assert executor._metadata["executable_nodes"] == 1

    def test_update_job_status(self, mock_executor):
        # Metadata's status and timestamp are updated when new status differs
        # from the current recorded status. Only touching the particular
        # cluster_id, not all entries.
        job = make_job(name="rule_a", jobid=1)

        captured_submit_dict = {}
        with mock_htcondor_submission(captured_submit_dict):
            mock_executor.run_job(job)

        job_key = "12345:1"
        initial_last_updated = mock_executor._metadata["jobs"][job_key]["last_updated"]

        # Same status: no-op, no write flagged, timestamp untouched.
        mock_executor._update_job_status_in_metadata(12345, JobStatus.IDLE)
        assert mock_executor._metadata_dirty is False
        assert (
            mock_executor._metadata["jobs"][job_key]["last_updated"]
            == initial_last_updated
        )

        # Status and timestamp update, dirty flag set.
        before = time.time()
        mock_executor._update_job_status_in_metadata(12345, JobStatus.RUNNING)
        assert mock_executor._metadata_dirty is True
        assert (
            mock_executor._metadata["jobs"][job_key]["status"]
            == JobStatus.RUNNING.value
        )
        assert mock_executor._metadata["jobs"][job_key]["last_updated"] >= before

        # A status update for an unrelated cluster_id must not touch this entry.
        mock_executor._metadata_dirty = False
        mock_executor._update_job_status_in_metadata(99999, JobStatus.COMPLETED)
        assert mock_executor._metadata_dirty is False
        assert (
            mock_executor._metadata["jobs"][job_key]["status"]
            == JobStatus.RUNNING.value
        )

    def test_flush_only_writes_when_dirty(self, mock_executor):
        # _flush_metadata_if_dirty should only persist to disk (and clear the
        # dirty flag) when something actually changed since the last flush.
        job = make_job(name="rule_a", jobid=1)

        captured_submit_dict = {}
        with mock_htcondor_submission(captured_submit_dict):
            mock_executor.run_job(job)

        mock_executor._metadata_dirty = False
        mock_executor._write_metadata = Mock(wraps=mock_executor._write_metadata)

        # Not dirty
        mock_executor._flush_metadata_if_dirty()
        assert mock_executor._write_metadata.call_count == 0

        # Dirty
        mock_executor._update_job_status_in_metadata(12345, JobStatus.RUNNING)
        mock_executor._flush_metadata_if_dirty()
        assert mock_executor._write_metadata.call_count == 1
        assert mock_executor._metadata_dirty is False
